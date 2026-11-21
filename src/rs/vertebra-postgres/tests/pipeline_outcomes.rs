//! Integration tests for ordered single-`Sync` pipeline outcome demultiplexing (`Result`, `Error`,
//! `Skipped`), phase-accurate `Parse`/`Bind`/`Execute` failure attribution, `ParameterStatus` updates,
//! and trailing `ReadyForQuery` recovery (`ch15:t04`).

extern crate _postgres as vertebra_postgres;

use std::collections::BTreeMap;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use vertebra_postgres::codecs::{INT4_OID, TEXT_OID, TIMESTAMPTZ_OID};
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::Parameter;
use vertebra_postgres::pipeline::{PipelineBatch, PipelineOperation, PipelineOutcome};
use vertebra_postgres::result::{PgValue, TransactionStatus};
use vertebra_postgres::session::PostgresConnection;

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn param_desc(oids: &[u32]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(oids.len() as i16);
    for &oid in oids {
        body.put_u32(oid);
    }
    frame(b't', &body)
}

fn row_desc(cols: &[(&str, u32, i16)]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for &(name, oid, fmt) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(4);
        body.put_i32(-1);
        body.put_i16(fmt);
    }
    frame(b'T', &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cells.len() as i16);
    for cell in cells {
        match cell {
            None => body.put_i32(-1),
            Some(b) => {
                body.put_i32(b.len() as i32);
                body.put_slice(b);
            }
        }
    }
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn err_response(code: &str, msg: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"SERROR\0VERROR\0C");
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

fn make_conn(initial_tz: &str) -> (PostgresConnection, DuplexStream) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.sslmode = "disable".to_string();
    let cfg = PostgresConfig::validate(raw).unwrap();
    let mut params = BTreeMap::new();
    params.insert("DateStyle".to_string(), "Postgres, MDY".to_string());
    params.insert("TimeZone".to_string(), initial_tz.to_string());
    let conn = PostgresConnection::from_stream(client_io, &cfg, params, None);
    (conn, server_io)
}

async fn drain_until_sync_or_query(srv: &mut DuplexStream) -> Vec<u8> {
    let mut tags = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        srv.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        srv.read_exact(&mut body).await.unwrap();
        tags.push(tag);
        if tag == b'S' || tag == b'Q' {
            return tags;
        }
    }
}

#[tokio::test]
async fn demultiplexes_heterogeneous_4op_result_batch_in_open_transaction() {
    let (mut conn, mut srv) = make_conn("UTC");
    // Prime SELECT op in statement cache first so Op 1 uses mixed binary/text portal columns!
    let server = tokio::spawn(async move {
        // Warmup query for SELECT id, custom_label
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut warm = Vec::new();
        warm.extend_from_slice(&frame(b'1', b""));
        warm.extend_from_slice(&param_desc(&[]));
        warm.extend_from_slice(&row_desc(&[("id", INT4_OID, 0), ("lbl", 99999, 0)]));
        warm.extend_from_slice(&frame(b'2', b""));
        warm.extend_from_slice(&row_desc(&[("id", INT4_OID, 0), ("lbl", 99999, 0)]));
        warm.extend_from_slice(&cmd_complete("SELECT 0"));
        warm.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&warm).await.unwrap();

        // BEGIN
        let _ = drain_until_sync_or_query(&mut srv).await;
        srv.write_all(&[cmd_complete("BEGIN"), frame(b'Z', b"T")].concat())
            .await
            .unwrap();

        // 4-op pipeline: [INSERT (affected 2), warm SELECT (mixed binary+text 2 rows), CREATE TEMP TABLE (NoData), "" (EmptyQueryResponse)]
        let tags = drain_until_sync_or_query(&mut srv).await;
        assert_eq!(tags.iter().filter(|&&t| t == b'S').count(), 1);

        let mut resp = Vec::new();
        // Op 0 (cold INSERT): 1, t, n, 2, n, C("INSERT 0 2")
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[TEXT_OID]));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&cmd_complete("INSERT 0 2"));

        // Op 1 (warm SELECT): 2, D, D, C("SELECT 2")
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&data_row(&[Some(&10_i32.to_be_bytes()), Some(b"alpha")]));
        resp.extend_from_slice(&data_row(&[Some(&20_i32.to_be_bytes()), None]));
        resp.extend_from_slice(&cmd_complete("SELECT 2"));

        // Op 2 (cold CREATE TEMP TABLE): 1, t, n, 2, n, C("CREATE TABLE")
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&cmd_complete("CREATE TABLE"));

        // Op 3 (cold empty SQL ""): 1, t, n, 2, n, I (EmptyQueryResponse)
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'I', b""));

        resp.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&resp).await.unwrap();
    });

    conn.pipeline_results(&PipelineBatch::new(vec![PipelineOperation::new(
        "SELECT id, lbl FROM items",
    )]))
    .await
    .unwrap();
    conn.begin().await.unwrap();

    let outcomes = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::with_parameters(
                "INSERT INTO items(name) VALUES ($1)",
                vec![Parameter::new(PgValue::Text("two".to_string()))],
            ),
            PipelineOperation::new("SELECT id, lbl FROM items"),
            PipelineOperation::new("CREATE TEMP TABLE scratch(id int)"),
            PipelineOperation::new(""),
        ]))
        .await
        .expect("4-op heterogeneous batch succeeds");

    assert_eq!(outcomes.len(), 4);
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);

    match &outcomes[0] {
        PipelineOutcome::Result(r) => {
            assert_eq!(r.affected_rows, 2);
            assert_eq!(r.command_tag, "INSERT 0 2");
            assert_eq!(r.transaction_status, TransactionStatus::InTransaction);
        }
        other => panic!("unexpected op 0: {other:?}"),
    }
    match &outcomes[1] {
        PipelineOutcome::Result(r) => {
            assert_eq!(r.affected_rows, 2);
            assert_eq!(
                r.rows,
                vec![
                    vec![PgValue::Int4(10), PgValue::Text("alpha".to_string())],
                    vec![PgValue::Int4(20), PgValue::Null],
                ]
            );
            assert_eq!(r.transaction_status, TransactionStatus::InTransaction);
        }
        other => panic!("unexpected op 1: {other:?}"),
    }
    match &outcomes[2] {
        PipelineOutcome::Result(r) => {
            assert_eq!(r.affected_rows, 0);
            assert_eq!(r.command_tag, "CREATE TABLE");
            assert_eq!(r.transaction_status, TransactionStatus::InTransaction);
        }
        other => panic!("unexpected op 2: {other:?}"),
    }
    match &outcomes[3] {
        PipelineOutcome::Result(r) => {
            assert_eq!(r.affected_rows, 0);
            assert_eq!(r.command_tag, "");
            assert!(r.rows.is_empty());
            assert_eq!(r.transaction_status, TransactionStatus::InTransaction);
        }
        other => panic!("unexpected op 3: {other:?}"),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn middle_execute_error_marks_subsequent_skipped_and_remains_reusable_after_rollback() {
    let (mut conn, mut srv) = make_conn("UTC");

    let server = tokio::spawn(async move {
        let _ = drain_until_sync_or_query(&mut srv).await;
        srv.write_all(&[cmd_complete("BEGIN"), frame(b'Z', b"T")].concat())
            .await
            .unwrap();

        // [SELECT 1, SELECT 1/0, SELECT 3]
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut resp = Vec::new();
        // Op 0 succeeds
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"1")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        // Op 1 fails in Execute -> ErrorResponse("22012"); Op 2 discarded by server until Sync -> Z('E')
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp.extend_from_slice(&err_response("22012", "division by zero"));
        resp.extend_from_slice(&frame(b'Z', b"E"));
        srv.write_all(&resp).await.unwrap();

        // ROLLBACK restores clean Idle session
        let _ = drain_until_sync_or_query(&mut srv).await;
        srv.write_all(&[cmd_complete("ROLLBACK"), frame(b'Z', b"I")].concat())
            .await
            .unwrap();

        // Follow-up simple query succeeds
        let _ = drain_until_sync_or_query(&mut srv).await;
        srv.write_all(
            &[
                row_desc(&[("ok", INT4_OID, 0)]),
                data_row(&[Some(b"9")]),
                cmd_complete("SELECT 1"),
                frame(b'Z', b"I"),
            ]
            .concat(),
        )
        .await
        .unwrap();
    });

    conn.begin().await.unwrap();
    let fail = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::new("SELECT 1"),
            PipelineOperation::new("SELECT 1 / 0"),
            PipelineOperation::new("SELECT 3"),
        ]))
        .await
        .expect_err("middle division by zero fails batch");

    assert_eq!(fail.failing_index, 1);
    assert_eq!(fail.first_error.sqlstate(), Some("22012"));
    assert_eq!(fail.transaction_status, TransactionStatus::Failed);
    assert_eq!(conn.transaction_status(), TransactionStatus::Failed);
    assert_eq!(fail.outcomes.len(), 3);
    assert!(matches!(
        &fail.outcomes[0],
        PipelineOutcome::Result(r)
            if r.rows == vec![vec![PgValue::Int4(1)]]
                && r.transaction_status == TransactionStatus::Failed
    ));
    assert!(matches!(
        &fail.outcomes[1],
        PipelineOutcome::Error(srv_err) if srv_err.sqlstate == "22012"
    ));
    assert_eq!(
        fail.outcomes[2],
        PipelineOutcome::Skipped { failed_index: 1 }
    );

    conn.rollback().await.unwrap();
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    let recovered = conn.simple_query("SELECT 9 AS ok").await.unwrap();
    assert_eq!(recovered[0].rows, vec![vec![PgValue::Int4(9)]]);

    server.await.unwrap();
}

#[tokio::test]
async fn attributes_parse_and_bind_errors_to_exact_operation_before_execute() {
    let (mut conn, mut srv) = make_conn("UTC");

    let server = tokio::spawn(async move {
        // Case 1: Op 1 fails during Parse ("42601")
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut r1 = Vec::new();
        r1.extend_from_slice(&frame(b'1', b""));
        r1.extend_from_slice(&param_desc(&[]));
        r1.extend_from_slice(&frame(b'n', b""));
        r1.extend_from_slice(&frame(b'2', b""));
        r1.extend_from_slice(&frame(b'n', b""));
        r1.extend_from_slice(&cmd_complete("SELECT 0"));
        // Op 1 Parse fails right away
        r1.extend_from_slice(&err_response("42601", "syntax error at or near \"FROMM\""));
        r1.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r1).await.unwrap();

        // Case 2: aborted Case 1 did not promote Op 0 into cache; Op 1 fails during Bind ("22P02")
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut r2 = Vec::new();
        r2.extend_from_slice(&frame(b'1', b""));
        r2.extend_from_slice(&param_desc(&[]));
        r2.extend_from_slice(&frame(b'n', b""));
        r2.extend_from_slice(&frame(b'2', b""));
        r2.extend_from_slice(&frame(b'n', b""));
        r2.extend_from_slice(&cmd_complete("SELECT 0"));
        // Op 1 Parse + Describe(S) succeed, Bind fails with 22P02
        r2.extend_from_slice(&frame(b'1', b""));
        r2.extend_from_slice(&param_desc(&[INT4_OID]));
        r2.extend_from_slice(&row_desc(&[("x", INT4_OID, 0)]));
        r2.extend_from_slice(&err_response(
            "22P02",
            "invalid input syntax for type integer: \"nope\"",
        ));
        r2.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r2).await.unwrap();
    });

    let parse_fail = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::new("SELECT WHERE false"),
            PipelineOperation::new("SELECT FROMM broken"),
            PipelineOperation::new("SELECT 3"),
        ]))
        .await
        .unwrap_err();
    assert_eq!(parse_fail.failing_index, 1);
    assert!(matches!(
        &parse_fail.outcomes[0],
        PipelineOutcome::Result(_)
    ));
    assert!(matches!(
        &parse_fail.outcomes[1],
        PipelineOutcome::Error(e) if e.sqlstate == "42601"
    ));
    assert_eq!(
        parse_fail.outcomes[2],
        PipelineOutcome::Skipped { failed_index: 1 }
    );

    let bind_fail = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::new("SELECT WHERE false"),
            PipelineOperation::with_parameters(
                "SELECT $1::int4 AS x",
                vec![Parameter::new(PgValue::Text("nope".to_string()))],
            ),
            PipelineOperation::new("SELECT 3"),
        ]))
        .await
        .unwrap_err();
    assert_eq!(bind_fail.failing_index, 1);
    assert!(matches!(&bind_fail.outcomes[0], PipelineOutcome::Result(_)));
    assert!(matches!(
        &bind_fail.outcomes[1],
        PipelineOutcome::Error(e) if e.sqlstate == "22P02"
    ));
    assert_eq!(
        bind_fail.outcomes[2],
        PipelineOutcome::Skipped { failed_index: 1 }
    );

    server.await.unwrap();
}

#[tokio::test]
async fn interleaved_parameter_status_and_notice_update_context_without_shifting_indices() {
    let (mut conn, mut srv) = make_conn("Asia/Shanghai");

    let server = tokio::spawn(async move {
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut resp = Vec::new();
        // Op 0: SET LOCAL TimeZone = 'Australia/Lord_Howe' -> emits ParameterStatus + NoticeResponse
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&frame(b'n', b""));
        resp.extend_from_slice(&frame(b'N', b"SNOTICE\0C00000\0Mtz changed\0\0"));
        resp.extend_from_slice(&frame(b'S', b"TimeZone\0Australia/Lord_Howe\0"));
        resp.extend_from_slice(&cmd_complete("SET"));

        // Op 1: cold non-ISO ambiguous abbreviation timestamptz -> preserves updated TimeZone!
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("ts", TIMESTAMPTZ_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("ts", TIMESTAMPTZ_OID, 0)]));
        resp.extend_from_slice(&frame(b'N', b"SNOTICE\0C00000\0Mmid row notice\0\0"));
        resp.extend_from_slice(&data_row(&[Some(b"Wed Jul 01 12:00:00 2026 CST")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&resp).await.unwrap();
    });

    let outcomes = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::new("SET LOCAL TimeZone = 'Australia/Lord_Howe'"),
            PipelineOperation::new("SELECT '2026-07-01 12:00:00+08'::timestamptz AS ts"),
        ]))
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    match &outcomes[1] {
        PipelineOutcome::Result(r) => match &r.rows[0][0] {
            PgValue::TemporalText {
                wire_text,
                time_zone,
                ..
            } => {
                assert_eq!(wire_text, "Wed Jul 01 12:00:00 2026 CST");
                assert_eq!(time_zone, "Australia/Lord_Howe");
            }
            other => panic!("expected TemporalText with updated TimeZone, got {other:?}"),
        },
        other => panic!("unexpected op 1 outcome: {other:?}"),
    }

    server.await.unwrap();
}

#[tokio::test]
async fn client_decode_error_retains_data_error_drains_ready_for_query_without_fake_sqlstate() {
    let (mut conn, mut srv) = make_conn("UTC");

    let server = tokio::spawn(async move {
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut resp = Vec::new();
        // Op 0 succeeds
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("x", INT4_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("x", INT4_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"10")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        // Op 1 sends bogus non-integer bytes in INT4 column
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("bad", INT4_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("bad", INT4_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"not-an-int4")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        // Op 2 still ran on server before ReadyForQuery
        resp.extend_from_slice(&frame(b'1', b""));
        resp.extend_from_slice(&param_desc(&[]));
        resp.extend_from_slice(&row_desc(&[("z", INT4_OID, 0)]));
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("z", INT4_OID, 0)]));
        resp.extend_from_slice(&data_row(&[Some(b"30")]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&resp).await.unwrap();
    });

    let fail = conn
        .pipeline_results(&PipelineBatch::new(vec![
            PipelineOperation::new("SELECT 10 AS x"),
            PipelineOperation::new("SELECT bad FROM t"),
            PipelineOperation::new("SELECT 30 AS z"),
        ]))
        .await
        .unwrap_err();

    assert_eq!(fail.failing_index, 1);
    assert!(matches!(fail.first_error, PostgresError::Data(_)));
    assert_eq!(fail.first_error.sqlstate(), None);
    assert_eq!(fail.outcomes.len(), 1);
    assert!(matches!(&fail.outcomes[0], PipelineOutcome::Result(_)));
    assert!(!conn.is_broken());

    server.await.unwrap();
}

#[tokio::test]
async fn cached_pipeline_op_rejects_unrequested_row_description() {
    let (mut conn, mut srv) = make_conn("UTC");

    let server = tokio::spawn(async move {
        let _ = drain_until_sync_or_query(&mut srv).await;
        let mut warm = Vec::new();
        warm.extend_from_slice(&frame(b'1', b""));
        warm.extend_from_slice(&param_desc(&[]));
        warm.extend_from_slice(&row_desc(&[("id", INT4_OID, 0)]));
        warm.extend_from_slice(&frame(b'2', b""));
        warm.extend_from_slice(&row_desc(&[("id", INT4_OID, 0)]));
        warm.extend_from_slice(&cmd_complete("SELECT 0"));
        warm.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&warm).await.unwrap();

        let tags = drain_until_sync_or_query(&mut srv).await;
        assert_eq!(tags, vec![b'B', b'E', b'S']);
        let mut resp = Vec::new();
        resp.extend_from_slice(&frame(b'2', b""));
        resp.extend_from_slice(&row_desc(&[("id", INT4_OID, 1)]));
        resp.extend_from_slice(&data_row(&[Some(&10_i32.to_be_bytes())]));
        resp.extend_from_slice(&cmd_complete("SELECT 1"));
        resp.extend_from_slice(&frame(b'Z', b"I"));
        let _ = srv.write_all(&resp).await;
    });

    conn.pipeline_results(&PipelineBatch::new(vec![PipelineOperation::new(
        "SELECT id FROM items",
    )]))
    .await
    .unwrap();

    let fail = conn
        .pipeline_results(&PipelineBatch::new(vec![PipelineOperation::new(
            "SELECT id FROM items",
        )]))
        .await
        .unwrap_err();
    assert!(matches!(fail.first_error, PostgresError::Protocol(_)));
    assert!(conn.is_broken());

    server.await.unwrap();
}
