//! Integration tests for cold-query pre-`Parse` parameter discovery and post-`ReadyForQuery` owned result normalization (`ch12:t02`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::CodecContext;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::Parameter;
use vertebra_postgres::metadata::{
    OwnedTextField, OwnedTextResult, normalize_materialized_result, resolve_parameter_metadata,
};
use vertebra_postgres::result::{Column, Format, PgValue, TransactionStatus, VectorKind};
use vertebra_postgres::session::SessionHandle;

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn catalog_row_desc() -> Vec<u8> {
    let cols = [
        ("oid", 26u32),
        ("nspname", 19),
        ("typname", 19),
        ("typtype", 25),
        ("typdelim", 25),
        ("typelem", 26),
        ("typbasetype", 26),
        ("extname", 19),
        ("attname", 19),
        ("atttypid", 26),
        ("atttypmod", 23),
        ("attnum", 21),
        ("attisdropped", 16),
    ];
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for (name, oid) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(Format::Text.code());
    }
    frame(b'T', &body)
}

fn text_row(cells: &[Option<&str>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cells.len() as i16);
    for cell in cells {
        match cell {
            None => body.put_i32(-1),
            Some(s) => {
                body.put_i32(s.len() as i32);
                body.put_slice(s.as_bytes());
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
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_query_frame(sock: &mut tokio::net::TcpStream) -> String {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0], b'Q');
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
    assert_eq!(body.pop(), Some(0));
    String::from_utf8(body).unwrap()
}

fn loopback_handle(port: u16) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[tokio::test]
async fn builtin_params_and_results_emit_zero_queries_and_pre_parse_vector_resolves() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let q1 = read_query_frame(&mut sock).await;
        assert!(q1.contains("'halfvec'"));
        let mut resp1 = Vec::new();
        resp1.extend_from_slice(&catalog_row_desc());
        resp1.extend_from_slice(&text_row(&[
            Some("42002"),
            Some("public"),
            Some("halfvec"),
            Some("b"),
            Some(","),
            Some("0"),
            Some("0"),
            Some("vector"),
            None,
            None,
            None,
            None,
            None,
        ]));
        resp1.extend_from_slice(&cmd_complete("SELECT 1"));
        resp1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp1).await.unwrap();

        let q2 = read_query_frame(&mut sock).await;
        assert!(q2.contains("'vector'"));
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&catalog_row_desc());
        resp2.extend_from_slice(&cmd_complete("SELECT 0"));
        resp2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp2).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();

    let builtin_res = resolve_parameter_metadata(
        conn,
        &[
            Parameter::new(PgValue::Int4(10)),
            Parameter::new(PgValue::Text("hi".to_string())),
        ],
        None,
    )
    .await
    .unwrap();
    assert_eq!(builtin_res.parameter_oids, vec![23, 25]);
    assert!(builtin_res.all_formats_known);

    let half_res = resolve_parameter_metadata(
        conn,
        &[Parameter::new(PgValue::Vector {
            values: vec![1.0, 2.0],
            kind: VectorKind::HalfVec,
        })],
        None,
    )
    .await
    .unwrap();
    assert_eq!(half_res.parameter_oids, vec![42002]);
    assert!(half_res.all_formats_known);

    let missing_vec = resolve_parameter_metadata(
        conn,
        &[Parameter::new(PgValue::Vector {
            values: vec![1.0],
            kind: VectorKind::Vector,
        })],
        None,
    )
    .await;
    assert!(matches!(missing_vec, Err(PostgresError::Data(_))));

    srv.await.unwrap();
}

#[tokio::test]
async fn post_completion_enrichment_guc_snapshot_ambient_savepoint_and_after_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let q1 = read_query_frame(&mut sock).await;
        assert!(q1.contains("41090"));
        let mut resp1 = Vec::new();
        resp1.extend_from_slice(&frame(b'S', b"DateStyle\0German, DMY\0"));
        resp1.extend_from_slice(&catalog_row_desc());
        resp1.extend_from_slice(&text_row(&[
            Some("41090"),
            Some("public"),
            Some("pair_t"),
            Some("c"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            Some("k"),
            Some("23"),
            Some("-1"),
            Some("1"),
            Some("f"),
        ]));
        resp1.extend_from_slice(&text_row(&[
            Some("41090"),
            Some("public"),
            Some("pair_t"),
            Some("c"),
            Some(","),
            Some("0"),
            Some("0"),
            None,
            Some("v"),
            Some("25"),
            Some("-1"),
            Some("2"),
            Some("f"),
        ]));
        resp1.extend_from_slice(&cmd_complete("SELECT 2"));
        resp1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp1).await.unwrap();

        let sp_cmd = read_query_frame(&mut sock).await;
        assert_eq!(sp_cmd, "SAVEPOINT _vtb_meta");
        let mut sp_resp = Vec::new();
        sp_resp.extend_from_slice(&cmd_complete("SAVEPOINT"));
        sp_resp.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&sp_resp).await.unwrap();

        let q2 = read_query_frame(&mut sock).await;
        assert!(q2.contains("41091"));
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&err_response("42501", "permission denied for pg_type"));
        resp2.extend_from_slice(&frame(b'Z', b"E"));
        sock.write_all(&resp2).await.unwrap();

        let rb_cmd = read_query_frame(&mut sock).await;
        assert_eq!(
            rb_cmd,
            "ROLLBACK TO SAVEPOINT _vtb_meta; RELEASE SAVEPOINT _vtb_meta"
        );
        let mut rb_resp = Vec::new();
        rb_resp.extend_from_slice(&cmd_complete("ROLLBACK"));
        rb_resp.extend_from_slice(&cmd_complete("RELEASE"));
        rb_resp.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&rb_resp).await.unwrap();

        let q3 = read_query_frame(&mut sock).await;
        assert!(q3.contains("41092"));
        drop(sock);
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();

    let mut uncompleted = OwnedTextResult::new(
        vec![Column::try_new("x", 23, Format::Text, -1, 0, 0).unwrap()],
        vec![],
        0,
        "SELECT 0",
        Arc::new(CodecContext::default()),
    );
    assert!(matches!(
        normalize_materialized_result(conn, uncompleted.clone()).await,
        Err(PostgresError::Interface(_))
    ));
    uncompleted.mark_completed(TransactionStatus::Idle);
    assert_eq!(
        normalize_materialized_result(conn, uncompleted)
            .await
            .unwrap()
            .rows
            .len(),
        0
    );

    let iso_ctx = Arc::new(CodecContext::from_parameters(conn.parameters()));
    let mut cold1 = OwnedTextResult::new(
        vec![
            Column::try_new("comp", 41090, Format::Text, -1, 0, 0).unwrap(),
            Column::try_new("d", 1082, Format::Text, -1, 0, 0).unwrap(),
        ],
        vec![vec![
            OwnedTextField {
                bytes: Some(b"(7,hello)".to_vec()),
            },
            OwnedTextField {
                bytes: Some(b"2026-07-04".to_vec()),
            },
        ]],
        1,
        "INSERT 0 1",
        iso_ctx.clone(),
    );
    cold1.mark_completed(TransactionStatus::Idle);
    let res1 = normalize_materialized_result(conn, cold1).await.unwrap();
    assert_eq!(
        res1.rows[0][0],
        PgValue::Composite {
            type_oid: 41090,
            field_names: vec!["k".to_string(), "v".to_string()],
            field_oids: vec![23, 25],
            values: vec![PgValue::Int4(7), PgValue::Text("hello".to_string())],
        }
    );
    assert_eq!(res1.rows[0][1], PgValue::Date(9681));

    let mut cold_tx = OwnedTextResult::new(
        vec![Column::try_new("denied_col", 41091, Format::Text, -1, 0, 0).unwrap()],
        vec![vec![OwnedTextField {
            bytes: Some(b"(secret)".to_vec()),
        }]],
        1,
        "SELECT 1",
        iso_ctx.clone(),
    );
    cold_tx.mark_completed(TransactionStatus::InTransaction);
    let res_tx = normalize_materialized_result(conn, cold_tx).await.unwrap();
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);
    assert_eq!(res_tx.rows[0][0], PgValue::Text("(secret)".to_string()));
    assert_eq!(res_tx.columns[0].type_oid, 41091);

    let mut cold_drop = OwnedTextResult::new(
        vec![Column::try_new("secret_col", 41092, Format::Text, -1, 0, 0).unwrap()],
        vec![vec![OwnedTextField {
            bytes: Some(b"sensitive-row-payload".to_vec()),
        }]],
        1,
        "INSERT 0 1",
        iso_ctx,
    );
    cold_drop.mark_completed(TransactionStatus::Idle);
    let err = normalize_materialized_result(conn, cold_drop)
        .await
        .unwrap_err();
    assert!(conn.is_closed());
    assert!(!err.to_string().contains("sensitive-row-payload"));
    match err {
        PostgresError::AfterCompletion {
            completed_result, ..
        } => {
            assert_eq!(completed_result.affected_rows, 1);
            assert_eq!(completed_result.command_tag, "INSERT 0 1");
            assert_eq!(
                completed_result.rows,
                vec![vec![PgValue::Text("sensitive-row-payload".to_string())]]
            );
        }
        other => panic!("expected AfterCompletion, got {other:?}"),
    }

    srv.await.unwrap();
}
