//! Wire-level integration tests for single-`Sync` outbound pipeline batching, SQL-head preflight,
//! `AsyncWrite::poll_flush` counting, duplex backpressure, and partial-send retirement (`ch15:t03`).

extern crate _postgres as vertebra_postgres;

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};
use vertebra_postgres::codecs::{INT4_OID, TEXT_OID};
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::Parameter;
use vertebra_postgres::pipeline::{
    PipelineBatch, PipelineLimits, PipelineOperation, PipelineOutcome,
};
use vertebra_postgres::result::PgValue;
use vertebra_postgres::session::PostgresConnection;

#[derive(Debug, Default)]
struct WireStats {
    raw_written: Mutex<Vec<u8>>,
    poll_flush_completions: AtomicUsize,
}

impl WireStats {
    fn parsed_frontend_tags(&self) -> Vec<u8> {
        let buf = self.raw_written.lock().unwrap();
        let mut pos = 0usize;
        let mut tags = Vec::new();
        while pos + 5 <= buf.len() {
            let tag = buf[pos];
            let len = u32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
                as usize;
            if len < 4 || pos + 1 + len > buf.len() {
                break;
            }
            tags.push(tag);
            pos += 1 + len;
        }
        tags
    }

    fn clear(&self) {
        self.raw_written.lock().unwrap().clear();
        self.poll_flush_completions.store(0, Ordering::SeqCst);
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
                self.stats
                    .poll_flush_completions
                    .fetch_add(1, Ordering::SeqCst);
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

fn data_row_single(bytes: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(bytes.len() as i32);
    body.put_slice(bytes);
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn make_conn(buf_size: usize) -> (PostgresConnection, DuplexStream, Arc<WireStats>) {
    let (client_io, server_io) = tokio::io::duplex(buf_size);
    let stats = Arc::new(WireStats::default());
    let instrumented = InstrumentedDuplex {
        inner: client_io,
        stats: Arc::clone(&stats),
    };
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.sslmode = "disable".to_string();
    let cfg = PostgresConfig::validate(raw).unwrap();
    let mut params = BTreeMap::new();
    params.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    params.insert("TimeZone".to_string(), "UTC".to_string());
    let conn = PostgresConnection::from_stream(instrumented, &cfg, params, None);
    (conn, server_io, stats)
}

async fn read_frontend_until_sync(sock: &mut DuplexStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        let done = tag == b'S';
        frames.push((tag, body));
        if done {
            return frames;
        }
    }
}

#[tokio::test]
async fn empty_pipeline_batch_emits_zero_bytes_zero_syncs_and_zero_flushes() {
    let (mut conn, _srv, stats) = make_conn(4096);
    let sub = conn
        .submit_pipeline(&PipelineBatch::new(vec![]))
        .await
        .expect("empty pipeline submits cleanly");
    assert_eq!(sub.operation_count(), 0);
    assert!(stats.raw_written.lock().unwrap().is_empty());
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 0);
    let outcomes = conn.collect_submitted_pipeline(sub).await.unwrap();
    assert!(outcomes.is_empty());
    assert!(!conn.is_broken());
}

#[tokio::test]
async fn cold_ddl_and_warm_batches_emit_single_sync_and_single_flush_without_catalog_queries() {
    let (mut conn, mut srv, stats) = make_conn(65536);

    let server_task = tokio::spawn(async move {
        // Batch 1: 3 cold ops (CREATE TEMP TABLE, INSERT with typed NULL, SELECT)
        let f1 = read_frontend_until_sync(&mut srv).await;
        let tags1: Vec<u8> = f1.iter().map(|(t, _)| *t).collect();
        assert!(!tags1.contains(&b'Q'), "must emit 0 catalog Query frames");
        assert!(
            !tags1.contains(&b'H'),
            "must emit 0 intermediate Flush frames"
        );
        assert_eq!(tags1.iter().filter(|&&t| t == b'S').count(), 1);
        assert_eq!(
            tags1,
            vec![
                b'P', b'D', b'B', b'D', b'E', b'P', b'D', b'B', b'D', b'E', b'P', b'D', b'B', b'D',
                b'E', b'S'
            ]
        );

        let mut resp1 = Vec::new();
        // Op 0: CREATE TEMP TABLE p(x int)
        resp1.extend_from_slice(&frame(b'1', b""));
        resp1.extend_from_slice(&param_desc(&[]));
        resp1.extend_from_slice(&frame(b'n', b""));
        resp1.extend_from_slice(&frame(b'2', b""));
        resp1.extend_from_slice(&frame(b'n', b""));
        resp1.extend_from_slice(&cmd_complete("CREATE TABLE"));
        // Op 1: INSERT INTO p VALUES ($1) with typed NULL INT4
        resp1.extend_from_slice(&frame(b'1', b""));
        resp1.extend_from_slice(&param_desc(&[INT4_OID]));
        resp1.extend_from_slice(&frame(b'n', b""));
        resp1.extend_from_slice(&frame(b'2', b""));
        resp1.extend_from_slice(&frame(b'n', b""));
        resp1.extend_from_slice(&cmd_complete("INSERT 0 1"));
        // Op 2: SELECT x FROM p
        resp1.extend_from_slice(&frame(b'1', b""));
        resp1.extend_from_slice(&param_desc(&[]));
        resp1.extend_from_slice(&row_desc(&[("x", INT4_OID, 0)]));
        resp1.extend_from_slice(&frame(b'2', b""));
        resp1.extend_from_slice(&row_desc(&[("x", INT4_OID, 0)]));
        resp1.extend_from_slice(&data_row_single(b"77"));
        resp1.extend_from_slice(&cmd_complete("SELECT 1"));
        resp1.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&resp1).await.unwrap();

        // Batch 2 (cold pure-DML/SELECT batch to populate StatementCache without DDL invalidation)
        let f2 = read_frontend_until_sync(&mut srv).await;
        assert_eq!(f2.iter().filter(|(t, _)| *t == b'S').count(), 1);
        assert!(!f2.iter().any(|(t, _)| *t == b'H'));
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&frame(b'1', b""));
        resp2.extend_from_slice(&param_desc(&[INT4_OID]));
        resp2.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp2.extend_from_slice(&frame(b'2', b""));
        resp2.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        resp2.extend_from_slice(&data_row_single(b"11"));
        resp2.extend_from_slice(&cmd_complete("SELECT 1"));
        resp2.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&resp2).await.unwrap();

        // Batch 3 (2 warm cached invocations of SELECT $1::int4 -> Bind + Execute x 2 + Sync!)
        let f3 = read_frontend_until_sync(&mut srv).await;
        let tags3: Vec<u8> = f3.iter().map(|(t, _)| *t).collect();
        assert_eq!(tags3, vec![b'B', b'E', b'B', b'E', b'S']);
        // Inspect first Bind body to confirm binary format (1) for param & result!
        let bind_body = &f3[0].1;
        // portal "" (1 byte \0) + stmt "vtb_ps_..."\0 -> find format codes
        let stmt_end = 1 + bind_body[1..].iter().position(|&b| b == 0).unwrap() + 1;
        let param_fmt_count = i16::from_be_bytes([bind_body[stmt_end], bind_body[stmt_end + 1]]);
        let first_param_fmt =
            i16::from_be_bytes([bind_body[stmt_end + 2], bind_body[stmt_end + 3]]);
        assert_eq!(param_fmt_count, 1);
        assert_eq!(first_param_fmt, 1, "warm Bind must select binary format");

        let mut resp3 = Vec::new();
        for val in [21_i32, 22_i32] {
            resp3.extend_from_slice(&frame(b'2', b""));
            resp3.extend_from_slice(&data_row_single(&val.to_be_bytes()));
            resp3.extend_from_slice(&cmd_complete("SELECT 1"));
        }
        resp3.extend_from_slice(&frame(b'Z', b"I"));
        srv.write_all(&resp3).await.unwrap();
    });

    let batch1 = PipelineBatch::new(vec![
        PipelineOperation::new("CREATE TEMP TABLE p(x int)"),
        PipelineOperation::with_parameters(
            "INSERT INTO p VALUES ($1)",
            vec![Parameter::with_oid(PgValue::Null, INT4_OID).unwrap()],
        ),
        PipelineOperation::new("SELECT x FROM p"),
    ]);
    let sub1 = conn.submit_pipeline(&batch1).await.unwrap();
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 1);
    let out1 = conn.collect_submitted_pipeline(sub1).await.unwrap();
    assert_eq!(out1.len(), 3);
    assert!(matches!(
        &out1[2],
        PipelineOutcome::Result(r) if r.rows == vec![vec![PgValue::Int4(77)]]
    ));

    stats.clear();
    let batch2 = PipelineBatch::new(vec![PipelineOperation::with_parameters(
        "SELECT $1::int4 AS v",
        vec![Parameter::new(PgValue::Int4(11))],
    )]);
    let out2 = conn.pipeline_results(&batch2).await.unwrap();
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 1);
    assert_eq!(out2.len(), 1);

    stats.clear();
    let batch3 = PipelineBatch::new(vec![
        PipelineOperation::with_parameters(
            "SELECT $1::int4 AS v",
            vec![Parameter::new(PgValue::Int4(21))],
        ),
        PipelineOperation::with_parameters(
            "SELECT $1::int4 AS v",
            vec![Parameter::new(PgValue::Int4(22))],
        ),
    ]);
    let out3 = conn.pipeline_results(&batch3).await.unwrap();
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 1);
    assert_eq!(
        stats.parsed_frontend_tags(),
        vec![b'B', b'E', b'B', b'E', b'S']
    );
    assert!(matches!(
        &out3[0],
        PipelineOutcome::Result(r) if r.rows == vec![vec![PgValue::Int4(21)]]
    ));
    assert!(matches!(
        &out3[1],
        PipelineOutcome::Result(r) if r.rows == vec![vec![PgValue::Int4(22)]]
    ));

    server_task.await.unwrap();
}

#[tokio::test]
async fn later_forbidden_command_blocks_whole_batch_with_zero_wire_bytes_and_flushes() {
    let (mut conn, _srv, stats) = make_conn(4096);

    let commit_batch = PipelineBatch::new(vec![
        PipelineOperation::with_parameters(
            "INSERT INTO t VALUES ($1)",
            vec![Parameter::new(PgValue::Int4(1))],
        ),
        PipelineOperation::new(";/*c*/COMMIT"),
    ]);
    let err1 = conn
        .submit_pipeline(&commit_batch)
        .await
        .expect_err("COMMIT must fail whole-batch preflight");
    match err1 {
        PostgresError::PipelineValidation {
            source,
            failing_index,
        } => {
            assert_eq!(failing_index, 1);
            assert!(matches!(*source, PostgresError::Interface(_)));
        }
        other => panic!("expected PipelineValidation, got {other:?}"),
    }
    assert!(stats.raw_written.lock().unwrap().is_empty());
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 0);
    assert!(!conn.is_broken());

    let copy_batch = PipelineBatch::new(vec![
        PipelineOperation::with_parameters(
            "INSERT INTO t VALUES ($1)",
            vec![Parameter::new(PgValue::Int4(1))],
        ),
        PipelineOperation::new("--x\nCOPY t TO STDOUT"),
    ]);
    let fail2 = conn
        .pipeline_results(&copy_batch)
        .await
        .expect_err("COPY must fail whole-batch preflight");
    assert_eq!(fail2.failing_index, 1);
    assert!(fail2.outcomes.is_empty());
    assert!(matches!(
        fail2.first_error,
        PostgresError::UnsupportedFeature(_)
    ));
    assert!(stats.raw_written.lock().unwrap().is_empty());
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 0);
    assert!(!conn.is_broken());
}

#[tokio::test]
async fn duplex_backpressure_under_1kib_socket_buffer_completes_without_deadlock_in_one_flush() {
    let op_count = 200usize;
    let row_payload = "x".repeat(2622);
    let (mut conn, mut srv, stats) = make_conn(1024);
    let payload_expected = row_payload.clone();
    let server_task = tokio::spawn(async move {
        let mut buf = BytesMut::with_capacity(4096);
        let mut execs = 0usize;
        loop {
            while buf.len() >= 5 {
                let tag = buf[0];
                let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                if buf.len() < 1 + len {
                    break;
                }
                let raw = buf.split_to(1 + len);
                match tag {
                    b'P' => srv.write_all(&frame(b'1', b"")).await.unwrap(),
                    b'D' => {
                        let kind = raw[5];
                        if kind == b'S' {
                            srv.write_all(&param_desc(&[TEXT_OID])).await.unwrap();
                            srv.write_all(&row_desc(&[("p", TEXT_OID, 0)]))
                                .await
                                .unwrap();
                        } else {
                            srv.write_all(&row_desc(&[("p", TEXT_OID, 0)]))
                                .await
                                .unwrap();
                        }
                    }
                    b'B' => srv.write_all(&frame(b'2', b"")).await.unwrap(),
                    b'E' => {
                        execs += 1;
                        srv.write_all(&data_row_single(row_payload.as_bytes()))
                            .await
                            .unwrap();
                        srv.write_all(&cmd_complete("SELECT 1")).await.unwrap();
                    }
                    b'S' => {
                        srv.write_all(&frame(b'Z', b"I")).await.unwrap();
                        srv.flush().await.unwrap();
                        assert_eq!(execs, op_count);
                        return;
                    }
                    other => panic!("unexpected frame {}", other as char),
                }
            }
            let n = srv.read_buf(&mut buf).await.unwrap();
            assert!(n > 0);
        }
    });

    let mut ops = Vec::with_capacity(op_count);
    let big_arg = "a".repeat(256);
    for i in 0..op_count {
        ops.push(PipelineOperation::with_parameters(
            format!("SELECT $1::text AS p /* {i} */"),
            vec![Parameter::new(PgValue::Text(big_arg.clone()))],
        ));
    }
    let batch = PipelineBatch::with_limits(
        ops,
        PipelineLimits {
            max_operations: 512,
            max_sql_bytes: 2 * 1024 * 1024,
            max_parameter_bytes: 2 * 1024 * 1024,
            max_result_bytes: 4 * 1024 * 1024,
        },
    );

    let submitted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        conn.submit_pipeline(&batch),
    )
    .await
    .expect("submit_pipeline must not deadlock under 1024-byte duplex backpressure")
    .expect("submit_pipeline succeeded");

    assert!(
        submitted.prefetched_frame_count() > 0,
        "must drain inbound backend frames while writing large pipeline batch"
    );
    assert_eq!(stats.poll_flush_completions.load(Ordering::SeqCst), 1);
    let tags = stats.parsed_frontend_tags();
    assert_eq!(tags.iter().filter(|&&t| t == b'S').count(), 1);
    assert!(!tags.contains(&b'H'));

    let outcomes = conn
        .collect_submitted_pipeline(submitted)
        .await
        .expect("all 200 outcomes collected");
    assert_eq!(outcomes.len(), op_count);
    assert!(matches!(
        &outcomes[op_count - 1],
        PipelineOutcome::Result(r) if r.rows == vec![vec![PgValue::Text(payload_expected)]]
    ));
    server_task.await.unwrap();
}

#[tokio::test]
async fn partial_send_eof_retires_connection_broken() {
    let (mut conn, mut srv, _stats) = make_conn(1024);

    let dropper = tokio::spawn(async move {
        let mut two_kib = vec![0u8; 2048];
        srv.read_exact(&mut two_kib).await.unwrap();
        drop(srv);
    });

    let mut ops = Vec::new();
    let payload = "z".repeat(512);
    for _ in 0..32 {
        ops.push(PipelineOperation::with_parameters(
            "INSERT INTO big_log VALUES ($1)",
            vec![Parameter::new(PgValue::Text(payload.clone()))],
        ));
    }
    let err = conn
        .submit_pipeline(&PipelineBatch::new(ops))
        .await
        .expect_err("peer EOF after 2 KiB must fail submit_pipeline");
    assert!(matches!(err, PostgresError::Transport(_)));
    assert!(conn.is_broken());
    assert!(conn.is_closed());
    dropper.await.unwrap();
}
