//! Integration tests for retained `SessionHandle` pipeline transaction/savepoint scopes,
//! same-owner raw session exclusivity (`SessionAccess::Pipeline`), and rollback invariants (`ch16:t01`).

extern crate _postgres as vertebra_postgres;

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use vertebra_postgres::codecs::INT4_OID;
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::Parameter;
use vertebra_postgres::pipeline::{PipelineBatch, PipelineOperation, PipelineOutcome};
use vertebra_postgres::result::PgValue;
use vertebra_postgres::session::statements::PoolerMode;
use vertebra_postgres::session::{
    BackendKey, PipelineScopeToken, PostgresConnection, SessionHandle,
};
use vertebra_postgres::transaction::TransactionOwner;

#[derive(Debug, Default)]
struct WireStats {
    raw_written: Mutex<Vec<u8>>,
    flushes: AtomicUsize,
}

impl WireStats {
    fn clear(&self) {
        self.raw_written.lock().unwrap().clear();
        self.flushes.store(0, Ordering::SeqCst);
    }

    fn written_len(&self) -> usize {
        self.raw_written.lock().unwrap().len()
    }
}

#[derive(Debug)]
struct InstrumentedDuplex {
    inner: DuplexStream,
    stats: Arc<WireStats>,
}

impl AsyncRead for InstrumentedDuplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for InstrumentedDuplex {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    self.stats
                        .raw_written
                        .lock()
                        .unwrap()
                        .extend_from_slice(&buf[..n]);
                }
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {
                self.stats.flushes.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

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

fn row_desc_int4(name: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    body.put_u32(0);
    body.put_i16(0);
    body.put_u32(INT4_OID);
    body.put_i16(4);
    body.put_i32(-1);
    body.put_i16(0);
    frame(b'T', &body)
}

fn data_row_text(val: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(val.len() as i32);
    body.put_slice(val);
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

fn make_session(mode: &str) -> (Arc<SessionHandle>, DuplexStream, Arc<WireStats>) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let stats = Arc::new(WireStats::default());
    let instrumented = InstrumentedDuplex {
        inner: client_io,
        stats: Arc::clone(&stats),
    };
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.sslmode = "disable".to_string();
    raw.pooler_mode = mode.to_string();
    let cfg = PostgresConfig::validate(raw).unwrap();
    let mut params = BTreeMap::new();
    params.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    params.insert("TimeZone".to_string(), "UTC".to_string());
    let conn = PostgresConnection::from_stream(
        instrumented,
        &cfg,
        params,
        Some(BackendKey {
            process_id: 9101,
            secret_key: 0x0102_0304,
        }),
    );
    (
        Arc::new(SessionHandle::from_direct(cfg, conn)),
        server_io,
        stats,
    )
}

async fn read_simple_query(sock: &mut DuplexStream) -> String {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0], b'Q', "expected simple Query ('Q') frame");
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
    assert_eq!(body.last().copied(), Some(0));
    String::from_utf8(body[..body.len() - 1].to_vec()).unwrap()
}

async fn read_until_sync(sock: &mut DuplexStream) -> Vec<u8> {
    let mut tags = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        tags.push(tag);
        if tag == b'S' {
            return tags;
        }
    }
}

#[tokio::test]
async fn post_results_caller_failure_keeps_tx_open_and_rolls_back_insert_to_idle() {
    let (session, mut srv, _stats) = make_session("session");

    let srv_task = tokio::spawn(async move {
        assert_eq!(read_simple_query(&mut srv).await, "BEGIN");
        let mut r_begin = cmd_complete("BEGIN");
        r_begin.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_begin).await.unwrap();

        let batch_tags = read_until_sync(&mut srv).await;
        assert_eq!(batch_tags.iter().filter(|&&t| t == b'S').count(), 1);
        assert!(!batch_tags.contains(&b'Q'));
        let mut r_batch = Vec::new();
        r_batch.extend_from_slice(&frame(b'1', b""));
        r_batch.extend_from_slice(&param_desc(&[INT4_OID]));
        r_batch.extend_from_slice(&frame(b'n', b""));
        r_batch.extend_from_slice(&frame(b'2', b""));
        r_batch.extend_from_slice(&frame(b'n', b""));
        r_batch.extend_from_slice(&cmd_complete("INSERT 0 1"));
        r_batch.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_batch).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "ROLLBACK");
        let mut r_rb = cmd_complete("ROLLBACK");
        r_rb.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r_rb).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "SELECT 42");
        let mut r_sel = row_desc_int4("v");
        r_sel.extend_from_slice(&data_row_text(b"42"));
        r_sel.extend_from_slice(&cmd_complete("SELECT 1"));
        r_sel.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r_sel).await.unwrap();
    });

    let token = session.pipeline_begin().await.unwrap();
    let batch = PipelineBatch::new(vec![PipelineOperation {
        sql: "INSERT INTO t VALUES ($1)".to_string(),
        parameters: vec![Parameter::new(PgValue::Int4(10))],
        parameter_oids: None,
    }]);
    let outcomes = session.pipeline_results(token, batch).await.unwrap();
    assert_eq!(outcomes.len(), 1);
    match &outcomes[0] {
        PipelineOutcome::Result(result) => assert_eq!(result.affected_rows, 1),
        other => panic!("expected Result outcome, got {other:?}"),
    }

    session.pipeline_finish(token, false).await.unwrap();
    let check = session.simple_query_shared("SELECT 42").await.unwrap();
    assert_eq!(check[0].rows[0][0], PgValue::Int4(42));
    srv_task.await.unwrap();
}

#[tokio::test]
async fn ambient_transaction_pipeline_uses_savepoint_and_survives_server_error() {
    let (session, mut srv, _stats) = make_session("session");

    let srv_task = tokio::spawn(async move {
        assert_eq!(read_simple_query(&mut srv).await, "BEGIN");
        let mut out = cmd_complete("BEGIN");
        out.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&out).await.unwrap();

        let outer_ins = read_until_sync(&mut srv).await;
        assert_eq!(outer_ins.last().copied(), Some(b'S'));
        let mut out_ins = Vec::new();
        out_ins.extend_from_slice(&frame(b'1', b""));
        out_ins.extend_from_slice(&param_desc(&[]));
        out_ins.extend_from_slice(&frame(b'n', b""));
        out_ins.extend_from_slice(&frame(b'2', b""));
        out_ins.extend_from_slice(&frame(b'n', b""));
        out_ins.extend_from_slice(&cmd_complete("INSERT 0 1"));
        out_ins.extend_from_slice(&frame(b'3', b""));
        out_ins.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&out_ins).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "SAVEPOINT vtb_sp_1");
        let mut r_sp = cmd_complete("SAVEPOINT");
        r_sp.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_sp).await.unwrap();

        let pipe_tags = read_until_sync(&mut srv).await;
        assert_eq!(pipe_tags.iter().filter(|&&t| t == b'S').count(), 1);
        let mut r_pipe = Vec::new();
        r_pipe.extend_from_slice(&frame(b'1', b""));
        r_pipe.extend_from_slice(&param_desc(&[]));
        r_pipe.extend_from_slice(&frame(b'n', b""));
        r_pipe.extend_from_slice(&frame(b'2', b""));
        r_pipe.extend_from_slice(&frame(b'n', b""));
        r_pipe.extend_from_slice(&cmd_complete("INSERT 0 1"));
        r_pipe.extend_from_slice(&frame(b'1', b""));
        r_pipe.extend_from_slice(&param_desc(&[]));
        r_pipe.extend_from_slice(&row_desc_int4("bad"));
        r_pipe.extend_from_slice(&frame(b'2', b""));
        r_pipe.extend_from_slice(&row_desc_int4("bad"));
        r_pipe.extend_from_slice(&error_response("22012", "division by zero"));
        r_pipe.extend_from_slice(&frame(b'Z', b"E"));
        srv.write_all(&r_pipe).await.unwrap();

        assert_eq!(
            read_simple_query(&mut srv).await,
            "ROLLBACK TO SAVEPOINT vtb_sp_1"
        );
        let mut r_rb = cmd_complete("ROLLBACK");
        r_rb.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_rb).await.unwrap();

        assert_eq!(
            read_simple_query(&mut srv).await,
            "RELEASE SAVEPOINT vtb_sp_1"
        );
        let mut r_rel = cmd_complete("RELEASE");
        r_rel.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_rel).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "COMMIT");
        let mut r_com = cmd_complete("COMMIT");
        r_com.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r_com).await.unwrap();
    });

    let outer_owner = TransactionOwner::new(7, session.generation());
    session.begin_as(outer_owner).await.unwrap();
    session
        .query_as(
            Some(outer_owner),
            "INSERT INTO outer_items VALUES (1)",
            &[],
            None,
        )
        .await
        .unwrap();

    assert!(matches!(
        session.pipeline_begin().await,
        Err(PostgresError::Interface(_))
    ));
    let token = session.pipeline_begin_as(Some(outer_owner)).await.unwrap();
    let batch = PipelineBatch::new(vec![
        PipelineOperation {
            sql: "INSERT INTO outer_items VALUES (2)".to_string(),
            parameters: vec![],
            parameter_oids: None,
        },
        PipelineOperation {
            sql: "SELECT 1 / 0 AS bad".to_string(),
            parameters: vec![],
            parameter_oids: None,
        },
    ]);
    let err = session.pipeline_results(token, batch).await.unwrap_err();
    match err {
        PostgresError::PipelineAborted(failure) => {
            assert_eq!(failure.failing_index, 1);
            assert_eq!(failure.outcomes.len(), 2);
            assert!(matches!(failure.outcomes[0], PipelineOutcome::Result(_)));
            assert!(matches!(failure.outcomes[1], PipelineOutcome::Error(_)));
        }
        other => panic!("expected PipelineAborted, got {other:?}"),
    }

    session.pipeline_finish(token, false).await.unwrap();
    session.commit_as(outer_owner).await.unwrap();
    srv_task.await.unwrap();
}

#[tokio::test]
async fn active_pipeline_scope_rejects_raw_session_methods_before_and_after_results_with_zero_bytes()
 {
    let (session, mut srv, stats) = make_session("session");

    let srv_task = tokio::spawn(async move {
        assert_eq!(read_simple_query(&mut srv).await, "BEGIN");
        let mut r_begin = cmd_complete("BEGIN");
        r_begin.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_begin).await.unwrap();

        let batch_tags = read_until_sync(&mut srv).await;
        assert_eq!(batch_tags.last().copied(), Some(b'S'));
        let mut r_sel = Vec::new();
        r_sel.extend_from_slice(&frame(b'1', b""));
        r_sel.extend_from_slice(&param_desc(&[]));
        r_sel.extend_from_slice(&row_desc_int4("one"));
        r_sel.extend_from_slice(&frame(b'2', b""));
        r_sel.extend_from_slice(&row_desc_int4("one"));
        r_sel.extend_from_slice(&data_row_text(b"1"));
        r_sel.extend_from_slice(&cmd_complete("SELECT 1"));
        r_sel.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_sel).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "COMMIT");
        let mut r_com = cmd_complete("COMMIT");
        r_com.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r_com).await.unwrap();
    });

    let token = session.pipeline_begin().await.unwrap();
    let owner = TransactionOwner::new(11, session.generation());
    stats.clear();

    assert!(matches!(
        session.query_shared("SELECT 1", &[], None).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.query_as(Some(owner), "SELECT 1", &[], None).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.simple_query_shared("SELECT 1").await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.begin_as(owner).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.commit_as(owner).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.rollback_as(owner).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.rollback_client().await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.savepoint_as(owner, 1).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.open_portal("SELECT 1", &[], None, 10).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.begin_close(),
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session
            .with_connection(|_c, _m: PoolerMode| async { Ok(()) })
            .await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.pipeline_begin().await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session
            .pipeline_results(PipelineScopeToken(999), PipelineBatch::new(vec![]))
            .await,
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(stats.written_len(), 0);

    let batch = PipelineBatch::new(vec![PipelineOperation {
        sql: "SELECT 1 AS one".to_string(),
        parameters: vec![],
        parameter_oids: None,
    }]);
    let outcomes = session.pipeline_results(token, batch).await.unwrap();
    assert_eq!(outcomes.len(), 1);
    stats.clear();

    assert!(matches!(
        session
            .pipeline_results(token, PipelineBatch::new(vec![]))
            .await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.commit_as(owner).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.query_shared("SELECT 1", &[], None).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.begin_close(),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(stats.written_len(), 0);

    session.pipeline_finish(token, true).await.unwrap();
    srv_task.await.unwrap();
}

#[tokio::test]
async fn caught_preflight_guard_rolls_back_even_when_finish_requests_commit_and_statement_mode_rejects()
 {
    let (stmt_session, _stmt_srv, stmt_stats) = make_session("statement");
    assert!(matches!(
        stmt_session.pipeline_begin().await,
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(stmt_stats.written_len(), 0);

    let (session, mut srv, stats) = make_session("session");
    let srv_task = tokio::spawn(async move {
        assert_eq!(read_simple_query(&mut srv).await, "BEGIN");
        let mut r_begin = cmd_complete("BEGIN");
        r_begin.extend_from_slice(&frame(b'Z', b"T"));
        srv.write_all(&r_begin).await.unwrap();

        assert_eq!(read_simple_query(&mut srv).await, "ROLLBACK");
        let mut r_rb = cmd_complete("ROLLBACK");
        r_rb.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&r_rb).await.unwrap();
    });

    let token = session.pipeline_begin().await.unwrap();
    stats.clear();
    let bad_batch = PipelineBatch::new(vec![
        PipelineOperation {
            sql: "INSERT INTO t VALUES (1)".to_string(),
            parameters: vec![],
            parameter_oids: None,
        },
        PipelineOperation {
            sql: "; /* comment */ COMMIT".to_string(),
            parameters: vec![],
            parameter_oids: None,
        },
    ]);
    let preflight_err = session
        .pipeline_results(token, bad_batch)
        .await
        .unwrap_err();
    match preflight_err {
        PostgresError::PipelineValidation { failing_index, .. } => assert_eq!(failing_index, 1),
        other => panic!("expected PipelineValidation, got {other:?}"),
    }
    assert_eq!(stats.written_len(), 0);

    let finish_err = session.pipeline_finish(token, true).await.unwrap_err();
    match finish_err {
        PostgresError::PipelineValidation { failing_index, .. } => assert_eq!(failing_index, 1),
        other => panic!("expected preserved PipelineValidation on finish(true), got {other:?}"),
    }
    srv_task.await.unwrap();
}
