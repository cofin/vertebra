//! Loopback TCP wire integration tests for exclusive `COPY TO STDOUT` streaming (`CopyOut`,
//! `ch18:t02`) and duplex `COPY FROM STDIN` backpressure & `CopyFail` abort recovery
//! (`CopyFromStream`, `ch18:t03`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{Notify, mpsc, oneshot};
use vertebra_postgres::codecs::numeric::decode_numeric_text;
use vertebra_postgres::codecs::{INT4_OID, NUMERIC_OID, TEXT_OID, TIMESTAMPTZ_OID};
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::copy::{
    BinaryCopyDecoder, BinaryCopyEncoder, COPY_DATA_CHUNK_BYTES, MAX_COPY_FAIL_REASON_BYTES,
};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::pipeline::{PipelineBatch, PipelineLimits};
use vertebra_postgres::pool::{PostgresPool, PostgresPoolOptions};
use vertebra_postgres::result::{PgValue, TemporalKind, TransactionStatus};
use vertebra_postgres::session::SessionHandle;
use vertebra_postgres::session::cancel::CANCEL_REQUEST_CODE;

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready(pid: i32, secret: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0_i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"application_name\0vertebra\0"));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0America/New_York\0"));
    let mut k = Vec::with_capacity(8);
    k.extend_from_slice(&pid.to_be_bytes());
    k.extend_from_slice(&secret.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &k));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn copy_response(tag: u8, overall_format: u8, col_formats: &[u16]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(overall_format);
    body.put_i16(col_formats.len() as i16);
    for &fmt in col_formats {
        body.put_u16(fmt);
    }
    frame(tag, &body)
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"SERROR\0VERROR\0C");
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

fn notice_response(message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"SNOTICE\0VNOTICE\0C00000\0M");
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'N', &body)
}

fn command_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn simple_int_select_response(val: i32, tx: u8) -> Vec<u8> {
    let mut col = BytesMut::new();
    col.put_i16(1);
    col.put_slice(b"v\0");
    col.put_u32(0);
    col.put_i16(0);
    col.put_u32(INT4_OID);
    col.put_i16(4);
    col.put_i32(-1);
    col.put_i16(0);

    let s = val.to_string();
    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(s.len() as i32);
    row.put_slice(s.as_bytes());

    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'T', &col));
    out.extend_from_slice(&frame(b'D', &row));
    out.extend_from_slice(&command_complete("SELECT 1"));
    out.extend_from_slice(&frame(b'Z', &[tx]));
    out
}

enum InitialPacket {
    Startup,
    Cancel { pid: i32, secret: i32 },
}

async fn read_initial_packet(sock: &mut TcpStream) -> std::io::Result<InitialPacket> {
    let mut hdr = [0u8; 8];
    sock.read_exact(&mut hdr).await?;
    let total_len = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    let code = i32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    let mut rest = vec![0u8; total_len - 8];
    sock.read_exact(&mut rest).await?;
    if code == CANCEL_REQUEST_CODE {
        let pid = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let secret = i32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]);
        Ok(InitialPacket::Cancel { pid, secret })
    } else {
        Ok(InitialPacket::Startup)
    }
}

async fn read_frontend_frame(sock: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await?;
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await?;
    Ok((hdr[0], body))
}

fn query_sql(body: &[u8]) -> String {
    let nul = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    String::from_utf8_lossy(&body[..nul]).into_owned()
}

fn tcp_config(port: u16, connect_timeout: Duration) -> PostgresConfig {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.channel_binding = "disable".to_string();
    raw.connect_timeout = connect_timeout.as_secs_f64();
    PostgresConfig::validate(raw).expect("valid config")
}

fn size_one_pool(port: u16, connect_timeout: Duration) -> Arc<PostgresPool> {
    let cfg = tcp_config(port, connect_timeout);
    PostgresPool::new(
        cfg,
        PostgresPoolOptions {
            min_size: 0,
            max_size: 1,
            acquire_timeout: Duration::from_secs(5),
            reset_timeout: Duration::from_secs(3),
            close_timeout: Duration::from_secs(3),
            ..Default::default()
        },
    )
    .expect("valid pool")
}

async fn answer_pool_reset_and_followup(sock: &mut TcpStream, pid_val: i32) {
    while let Ok((tag, body)) = read_frontend_frame(sock).await {
        if tag == b'X' {
            break;
        }
        if tag == b'Q' {
            let sql = query_sql(&body);
            if sql.eq_ignore_ascii_case("DISCARD ALL") {
                let mut out = command_complete("DISCARD ALL");
                out.extend_from_slice(&frame(b'Z', b"I"));
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            } else if sql.eq_ignore_ascii_case("BEGIN") {
                let mut out = command_complete("BEGIN");
                out.extend_from_slice(&frame(b'Z', b"T"));
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            } else if sql.eq_ignore_ascii_case("ROLLBACK") {
                let mut out = command_complete("ROLLBACK");
                out.extend_from_slice(&frame(b'Z', b"I"));
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            } else if sql.starts_with("SET ") {
                let mut out = command_complete("SET");
                out.extend_from_slice(&frame(b'Z', b"I"));
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            } else if sql == "SELECT pg_backend_pid()" {
                let out = simple_int_select_response(pid_val, b'I');
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            } else if sql.is_empty() {
                let mut out = frame(b'I', b"");
                out.extend_from_slice(&frame(b'Z', b"I"));
                let _ = sock.write_all(&out).await;
                let _ = sock.flush().await;
            }
        }
    }
}

#[tokio::test]
async fn copy_out_streams_rows_incrementally_before_copy_done_and_reuses_pool_slot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let release_row2 = Arc::new(Notify::new());
    let release_peer = Arc::clone(&release_row2);
    let startups = Arc::new(AtomicUsize::new(0));
    let startups_peer = Arc::clone(&startups);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        assert!(matches!(
            read_initial_packet(&mut sock).await.unwrap(),
            InitialPacket::Startup
        ));
        startups_peer.fetch_add(1, Ordering::SeqCst);
        sock.write_all(&auth_ok_and_ready(4101, 9001))
            .await
            .unwrap();

        let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(tag, b'Q');
        assert_eq!(query_sql(&body), "COPY items TO STDOUT (FORMAT binary)");

        let mut enc = BinaryCopyEncoder::new(2).unwrap();
        let mut r1_wire = BytesMut::new();
        enc.encode_raw_row(&[Some(&7_i32.to_be_bytes()), None], &mut r1_wire)
            .unwrap();
        let mut r2_wire = BytesMut::new();
        enc.encode_raw_row(&[Some(&8_i32.to_be_bytes()), Some(b"ok")], &mut r2_wire)
            .unwrap();
        enc.finish_trailer(&mut r2_wire).unwrap();

        let s1 = 9;
        let s2 = 22;
        let mut first_burst = copy_response(b'H', 1, &[1, 1]);
        first_burst.extend_from_slice(&frame(b'd', &r1_wire[..s1]));
        first_burst.extend_from_slice(&frame(b'd', &r1_wire[s1..s2]));
        first_burst.extend_from_slice(&frame(b'd', &r1_wire[s2..]));
        sock.write_all(&first_burst).await.unwrap();
        sock.flush().await.unwrap();

        release_peer.notified().await;

        let mut finish_burst = frame(b'd', &r2_wire);
        finish_burst.extend_from_slice(&frame(b'c', b""));
        finish_burst.extend_from_slice(&command_complete("COPY 2"));
        finish_burst.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&finish_burst).await.unwrap();
        sock.flush().await.unwrap();

        answer_pool_reset_and_followup(&mut sock, 4101).await;
    });

    let pool = size_one_pool(port, Duration::from_secs(2));
    let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let mut copy_out = session
        .copy_to("COPY items TO STDOUT (FORMAT binary)")
        .await
        .unwrap();
    assert_eq!(copy_out.column_count(), 2);

    assert!(matches!(
        session.simple_query("SELECT 1").await,
        Err(PostgresError::Interface(_))
    ));

    let row1 = tokio::time::timeout(Duration::from_millis(500), copy_out.next_row())
        .await
        .expect("row 1 arrives while barrier before CopyDone is still held")
        .expect("row 1 ok")
        .expect("row 1 some");
    assert_eq!(
        row1,
        vec![Some(Bytes::copy_from_slice(&7_i32.to_be_bytes())), None]
    );
    assert_eq!(copy_out.rows_delivered(), 1);

    release_row2.notify_one();

    let row2 = copy_out.next_row().await.unwrap().unwrap();
    assert_eq!(
        row2,
        vec![
            Some(Bytes::copy_from_slice(&8_i32.to_be_bytes())),
            Some(Bytes::from_static(b"ok"))
        ]
    );
    assert_eq!(copy_out.next_row().await.unwrap(), None);
    assert_eq!(copy_out.rows_delivered(), 2);
    assert!(copy_out.is_exhausted());
    copy_out.close().await.unwrap();
    session.close_shared().await.unwrap();

    let session2 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let pid_res = session2
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap();
    assert_eq!(pid_res[0].rows[0][0], PgValue::Int4(4101));
    assert_eq!(startups.load(Ordering::SeqCst), 1);
    session2.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_out_empty_stream_updates_parameters_and_preserves_ambient_transaction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(4201, 9201))
            .await
            .unwrap();

        let (_, b1) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b1), "BEGIN");
        let mut begin_resp = command_complete("BEGIN");
        begin_resp.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&begin_resp).await.unwrap();

        let (_, b2) = read_frontend_frame(&mut sock).await.unwrap();
        assert!(query_sql(&b2).starts_with("COPY "));

        let mut enc = BinaryCopyEncoder::new(1).unwrap();
        let mut hdr = BytesMut::new();
        enc.write_header(&mut hdr);
        let trailer = (-1_i16).to_be_bytes();

        let mut stream = copy_response(b'H', 1, &[1]);
        stream.extend_from_slice(&notice_response("starting copy"));
        stream.extend_from_slice(&frame(b'd', &hdr));
        stream.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
        stream.extend_from_slice(&frame(b'd', &trailer));
        stream.extend_from_slice(&frame(b'c', b""));
        stream.extend_from_slice(&command_complete("COPY 0"));
        stream.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&stream).await.unwrap();

        let (_, b3) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b3), "ROLLBACK");
        let mut rb = command_complete("ROLLBACK");
        rb.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&rb).await.unwrap();
        let _ = read_frontend_frame(&mut sock).await;
    });

    let cfg = tcp_config(port, Duration::from_secs(2));
    let mut handle = SessionHandle::from_config(cfg);
    handle.connect().await.unwrap();
    let session = Arc::new(handle);

    session.simple_query("BEGIN").await.unwrap();
    assert_eq!(
        session.transaction_status(),
        TransactionStatus::InTransaction
    );

    let mut copy_out = session
        .copy_to("COPY (SELECT 1 WHERE false) TO STDOUT (FORMAT binary)")
        .await
        .unwrap();
    assert_eq!(copy_out.next_row().await.unwrap(), None);
    assert_eq!(copy_out.rows_delivered(), 0);
    assert_eq!(
        session.parameters().get("TimeZone").map(String::as_str),
        Some("UTC")
    );
    assert_eq!(
        session.transaction_status(),
        TransactionStatus::InTransaction
    );

    session.rollback_client().await.unwrap();
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);
    session.close_shared().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_out_rejects_text_format_with_cancel_and_rejects_active_pipeline_before_wire() {
    for (overall, cols) in [(0_u8, vec![0_u16, 0_u16]), (1_u8, vec![1_u16, 0_u16])] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (cancel_tx, cancel_rx) = oneshot::channel::<(i32, i32)>();

        let server = tokio::spawn(async move {
            let (mut sock1, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock1).await.unwrap();
            sock1
                .write_all(&auth_ok_and_ready(4301, 9301))
                .await
                .unwrap();

            let (_, body) = read_frontend_frame(&mut sock1).await.unwrap();
            assert!(query_sql(&body).starts_with("COPY"));
            sock1
                .write_all(&copy_response(b'H', overall, &cols))
                .await
                .unwrap();

            let (mut cancel_sock, _) = listener.accept().await.unwrap();
            match read_initial_packet(&mut cancel_sock).await.unwrap() {
                InitialPacket::Cancel { pid, secret } => {
                    let _ = cancel_tx.send((pid, secret));
                }
                InitialPacket::Startup => panic!("expected CancelRequest, got Startup"),
            }
            drop(cancel_sock);

            let mut aborted = error_response("57014", "canceling statement due to user request");
            aborted.extend_from_slice(&frame(b'Z', b"I"));
            let _ = sock1.write_all(&aborted).await;

            let (mut sock2, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock2).await.unwrap();
            sock2
                .write_all(&auth_ok_and_ready(4302, 9302))
                .await
                .unwrap();
            answer_pool_reset_and_followup(&mut sock2, 4302).await;
        });

        let pool = size_one_pool(port, Duration::from_secs(2));
        let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
        let err = session.copy_to("COPY items TO STDOUT").await.unwrap_err();
        assert!(matches!(err, PostgresError::Data(_)), "got {err:?}");
        assert_eq!(cancel_rx.await.unwrap(), (4301, 9301));
        session.close_shared().await.unwrap();

        let session2 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
        let res = session2
            .simple_query("SELECT pg_backend_pid()")
            .await
            .unwrap();
        assert_eq!(res[0].rows[0][0], PgValue::Int4(4302));
        session2.close_shared().await.unwrap();
        pool.close(Duration::from_secs(2)).await.unwrap();
        server.await.unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let copy_wire_seen = Arc::new(AtomicUsize::new(0));
    let copy_wire_peer = Arc::clone(&copy_wire_seen);
    let pipe_server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(4310, 9310))
            .await
            .unwrap();
        while let Ok((tag, body)) = read_frontend_frame(&mut sock).await {
            if tag == b'X' {
                break;
            }
            if tag == b'Q' {
                let sql = query_sql(&body);
                if sql.contains("COPY") {
                    copy_wire_peer.fetch_add(1, Ordering::SeqCst);
                } else if sql == "BEGIN" {
                    let mut out = command_complete("BEGIN");
                    out.extend_from_slice(&frame(b'Z', b"T"));
                    let _ = sock.write_all(&out).await;
                } else if sql == "ROLLBACK" {
                    let mut out = command_complete("ROLLBACK");
                    out.extend_from_slice(&frame(b'Z', b"I"));
                    let _ = sock.write_all(&out).await;
                }
            }
        }
    });

    let cfg = tcp_config(port, Duration::from_secs(2));
    let mut h = SessionHandle::from_config(cfg);
    h.connect().await.unwrap();
    let session = Arc::new(h);
    let token = session.pipeline_begin().await.unwrap();
    let err = session
        .copy_to("COPY items TO STDOUT (FORMAT binary)")
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        PostgresError::UnsupportedFeature(_) | PostgresError::Interface(_)
    ));
    let multi_err = session
        .copy_to("COPY items TO STDOUT (FORMAT binary); SELECT 1")
        .await
        .unwrap_err();
    assert!(matches!(multi_err, PostgresError::Interface(_)));
    assert_eq!(copy_wire_seen.load(Ordering::SeqCst), 0);
    let empty_batch = PipelineBatch::new(Vec::new());
    let _ = PipelineLimits::default();
    let _ = session.pipeline_finish(token, false).await;
    let _ = empty_batch;
    session.close_shared().await.unwrap();
    pipe_server.await.unwrap();
}

#[tokio::test]
async fn copy_out_surfaces_pre_stream_and_midstream_server_errors_cleanly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(4401, 9401))
            .await
            .unwrap();

        let _ = read_frontend_frame(&mut sock).await.unwrap();
        let mut pre = error_response("22012", "division by zero");
        pre.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&pre).await.unwrap();

        let _ = read_frontend_frame(&mut sock).await.unwrap();
        let mut enc = BinaryCopyEncoder::new(1).unwrap();
        let mut hdr = BytesMut::new();
        enc.write_header(&mut hdr);
        let mut mid = copy_response(b'H', 1, &[1]);
        mid.extend_from_slice(&frame(b'd', &hdr));
        mid.extend_from_slice(&error_response("57014", "canceling statement"));
        mid.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&mid).await.unwrap();

        answer_pool_reset_and_followup(&mut sock, 4401).await;
    });

    let pool = size_one_pool(port, Duration::from_secs(2));
    let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));

    let err1 = session
        .copy_to("COPY (SELECT 1/0) TO STDOUT (FORMAT binary)")
        .await
        .unwrap_err();
    assert_eq!(err1.sqlstate(), Some("22012"));

    let mut copy_out = session
        .copy_to("COPY (SELECT slow()) TO STDOUT (FORMAT binary)")
        .await
        .unwrap();
    let err2 = copy_out.next_row().await.unwrap_err();
    assert_eq!(err2.sqlstate(), Some("57014"));
    session.close_shared().await.unwrap();

    let session2 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let check = session2
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap();
    assert_eq!(check[0].rows[0][0], PgValue::Int4(4401));
    session2.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_out_early_close_and_drop_send_cancel_and_retire_pooled_backend() {
    for explicit_close in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let next_pid = Arc::new(AtomicI32::new(4500));
        let next_pid_peer = Arc::clone(&next_pid);
        let (cancel_tx, mut cancel_rx) = mpsc::unbounded_channel::<(i32, i32)>();

        let server = tokio::spawn(async move {
            let cancel_stop = Arc::new(Notify::new());
            for _ in 0..3 {
                let (mut sock, _) = listener.accept().await.unwrap();
                match read_initial_packet(&mut sock).await.unwrap() {
                    InitialPacket::Cancel { pid, secret } => {
                        let _ = cancel_tx.send((pid, secret));
                        cancel_stop.notify_waiters();
                    }
                    InitialPacket::Startup => {
                        let pid = next_pid_peer.fetch_add(1, Ordering::SeqCst) + 1;
                        let secret = 9000 + pid;
                        let stop = Arc::clone(&cancel_stop);
                        tokio::spawn(async move {
                            sock.write_all(&auth_ok_and_ready(pid, secret))
                                .await
                                .unwrap();
                            if pid == 4501 {
                                let (_, body) = read_frontend_frame(&mut sock).await.unwrap();
                                assert!(query_sql(&body).starts_with("COPY"));
                                let mut enc = BinaryCopyEncoder::new(1).unwrap();
                                let mut row_buf = BytesMut::new();
                                enc.encode_raw_row(&[Some(&1_i32.to_be_bytes())], &mut row_buf)
                                    .unwrap();
                                let mut start = copy_response(b'H', 1, &[1]);
                                start.extend_from_slice(&frame(b'd', &row_buf));
                                let _ = sock.write_all(&start).await;
                                let _ = sock.flush().await;
                                loop {
                                    let mut more = BytesMut::new();
                                    more.put_i16(1);
                                    more.put_i32(4);
                                    more.put_i32(2);
                                    let chunk = frame(b'd', &more);
                                    tokio::select! {
                                        () = stop.notified() => {
                                            let mut end = error_response("57014", "cancelled COPY TO");
                                            end.extend_from_slice(&frame(b'Z', b"I"));
                                            let _ = sock.write_all(&end).await;
                                            let _ = sock.flush().await;
                                            break;
                                        }
                                        () = tokio::time::sleep(Duration::from_millis(10)) => {
                                            if sock.write_all(&chunk).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                }
                            } else {
                                answer_pool_reset_and_followup(&mut sock, pid).await;
                            }
                        });
                    }
                }
            }
        });

        let pool = size_one_pool(port, Duration::from_millis(600));
        {
            let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
            let mut copy_out = session
                .copy_to("COPY infinite TO STDOUT (FORMAT binary)")
                .await
                .unwrap();
            let r1 = copy_out.next_row().await.unwrap().unwrap();
            assert_eq!(r1, vec![Some(Bytes::copy_from_slice(&1_i32.to_be_bytes()))]);
            if explicit_close {
                copy_out.close().await.unwrap();
                copy_out.close().await.unwrap();
                session.close_shared().await.unwrap();
            } else {
                drop(session);
                drop(copy_out);
            }
        }

        let got_cancel = tokio::time::timeout(Duration::from_secs(2), cancel_rx.recv())
            .await
            .expect("CancelRequest received promptly")
            .unwrap();
        assert_eq!(got_cancel, (4501, 13501));

        let session2 = Arc::new(SessionHandle::from_pooled(
            tokio::time::timeout(Duration::from_secs(2), pool.acquire())
                .await
                .expect("pool permit freed after COPY TO retirement")
                .expect("fresh connection acquired"),
        ));
        let pid2 = session2
            .simple_query("SELECT pg_backend_pid()")
            .await
            .unwrap();
        assert_eq!(pid2[0].rows[0][0], PgValue::Int4(4502));
        session2.close_shared().await.unwrap();
        pool.close(Duration::from_secs(2)).await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn copy_out_detects_command_complete_count_mismatch_and_truncated_copy_done() {
    for truncated in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock).await.unwrap();
            sock.write_all(&auth_ok_and_ready(4601, 9601))
                .await
                .unwrap();

            let _ = read_frontend_frame(&mut sock).await.unwrap();
            let mut enc = BinaryCopyEncoder::new(1).unwrap();
            let mut payload = BytesMut::new();
            enc.encode_raw_row(&[Some(&5_i32.to_be_bytes())], &mut payload)
                .unwrap();
            if !truncated {
                enc.finish_trailer(&mut payload).unwrap();
            }
            let mut resp = copy_response(b'H', 1, &[1]);
            resp.extend_from_slice(&frame(b'd', &payload));
            resp.extend_from_slice(&frame(b'c', b""));
            resp.extend_from_slice(&command_complete(if truncated {
                "COPY 1"
            } else {
                "COPY 2"
            }));
            resp.extend_from_slice(&frame(b'Z', b"I"));
            sock.write_all(&resp).await.unwrap();
        });

        let cfg = tcp_config(port, Duration::from_secs(2));
        let mut h = SessionHandle::from_config(cfg);
        h.connect().await.unwrap();
        let session = Arc::new(h);
        let mut copy_out = session
            .copy_to("COPY t TO STDOUT (FORMAT binary)")
            .await
            .unwrap();
        assert!(copy_out.next_row().await.unwrap().is_some());
        let err = copy_out.next_row().await.unwrap_err();
        if truncated {
            assert!(matches!(err, PostgresError::Data(_)), "got {err:?}");
        } else {
            assert!(matches!(err, PostgresError::Protocol(_)), "got {err:?}");
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn copy_in_chunks_multi_64k_frames_commits_owned_tx_and_handles_zero_rows() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(5101, 8101))
            .await
            .unwrap();

        let (_, b1) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b1), "BEGIN");
        let mut r1 = command_complete("BEGIN");
        r1.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&r1).await.unwrap();

        let (_, b2) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b2), "COPY target FROM STDIN (FORMAT binary)");
        sock.write_all(&copy_response(b'G', 1, &[1, 1, 1]))
            .await
            .unwrap();

        let mut dec = BinaryCopyDecoder::new(3);
        let mut decoded_rows = 0usize;
        let mut chunk_count = 0usize;
        let mut max_chunk = 0usize;
        loop {
            let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
            if tag == b'd' {
                assert!(
                    body.len() <= COPY_DATA_CHUNK_BYTES,
                    "CopyData frame {} exceeded 64 KiB",
                    body.len()
                );
                chunk_count += 1;
                max_chunk = max_chunk.max(body.len());
                dec.push(&body).unwrap();
                while dec.next_row().unwrap().is_some() {
                    decoded_rows += 1;
                }
            } else if tag == b'c' {
                break;
            } else {
                panic!("unexpected frontend tag {tag}");
            }
        }
        assert_eq!(decoded_rows, 500);
        assert_eq!(dec.finish().unwrap(), 500);
        assert!(chunk_count >= 3, "expected >=3 chunks, got {chunk_count}");

        let mut copy_done = command_complete("COPY 500");
        copy_done.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&copy_done).await.unwrap();

        let (_, b3) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b3), "COMMIT");
        let mut cm = command_complete("COMMIT");
        cm.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&cm).await.unwrap();

        let (_, b4) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b4), "BEGIN");
        let mut r2 = command_complete("BEGIN");
        r2.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&r2).await.unwrap();

        let _ = read_frontend_frame(&mut sock).await.unwrap();
        sock.write_all(&copy_response(b'G', 1, &[1])).await.unwrap();

        let mut zero_dec = BinaryCopyDecoder::new(1);
        loop {
            let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
            if tag == b'd' {
                zero_dec.push(&body).unwrap();
            } else if tag == b'c' {
                break;
            } else {
                panic!("unexpected tag {tag}");
            }
        }
        assert_eq!(zero_dec.finish().unwrap(), 0);
        let mut z_done = command_complete("COPY 0");
        z_done.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&z_done).await.unwrap();

        let (_, b5) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b5), "COMMIT");
        let mut cm2 = command_complete("COMMIT");
        cm2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&cm2).await.unwrap();

        answer_pool_reset_and_followup(&mut sock, 5101).await;
    });

    let pool = size_one_pool(port, Duration::from_secs(2));
    let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));

    let num = decode_numeric_text("98765.4321").unwrap();
    let payload_text = "x".repeat(300);
    let rows: Vec<Vec<PgValue>> = (0..500_i32)
        .map(|i| {
            vec![
                PgValue::Int4(i),
                if i % 7 == 0 {
                    PgValue::Null
                } else {
                    PgValue::Text(payload_text.clone())
                },
                PgValue::Numeric(num.clone()),
            ]
        })
        .collect();

    let mut copy_in = session
        .copy_from("COPY target FROM STDIN (FORMAT binary)")
        .await
        .unwrap();
    assert_eq!(copy_in.column_count(), 3);
    copy_in
        .write_rows(&[INT4_OID, TEXT_OID, NUMERIC_OID], &rows)
        .await
        .unwrap();
    assert_eq!(copy_in.finish().await.unwrap(), 500);
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);

    let mut copy_zero = session
        .copy_from("COPY target(id) FROM STDIN (FORMAT binary)")
        .await
        .unwrap();
    assert_eq!(copy_zero.finish().await.unwrap(), 0);

    session.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_in_duplex_read_catches_tiny_window_unique_violation_without_deadlock() {
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = sock.listen(16).unwrap();
    let port = listener.local_addr().unwrap().port();
    let hold_first_socket = Arc::new(Notify::new());
    let hold_peer = Arc::clone(&hold_first_socket);

    let server = tokio::spawn(async move {
        let (mut s1, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut s1).await.unwrap();
        s1.write_all(&auth_ok_and_ready(5201, 8201)).await.unwrap();

        let (_, b1) = read_frontend_frame(&mut s1).await.unwrap();
        assert_eq!(query_sql(&b1), "BEGIN");
        let mut r1 = command_complete("BEGIN");
        r1.extend_from_slice(&frame(b'Z', b"T"));
        s1.write_all(&r1).await.unwrap();

        let _ = read_frontend_frame(&mut s1).await.unwrap();
        s1.write_all(&copy_response(b'G', 1, &[1, 1]))
            .await
            .unwrap();
        s1.flush().await.unwrap();

        let mut early_err =
            error_response("23505", "duplicate key value violates unique constraint");
        early_err.extend_from_slice(&frame(b'Z', b"E"));
        s1.write_all(&early_err).await.unwrap();
        s1.flush().await.unwrap();

        let hold_task = tokio::spawn(async move {
            hold_peer.notified().await;
            drop(s1);
        });

        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut s2).await.unwrap();
        s2.write_all(&auth_ok_and_ready(5202, 8202)).await.unwrap();
        answer_pool_reset_and_followup(&mut s2, 5202).await;
        hold_task.await.unwrap();
    });

    let pool = size_one_pool(port, Duration::from_millis(800));
    let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let mut copy_in = session
        .copy_from("COPY uniq_tbl FROM STDIN (FORMAT binary)")
        .await
        .unwrap();

    let blob = vec![0x5A_u8; 4096];
    let big_rows: Vec<Vec<PgValue>> = (0..512_i32)
        .map(|i| vec![PgValue::Int4(i), PgValue::Bytes(blob.clone())])
        .collect();

    let err = tokio::time::timeout(
        Duration::from_secs(1),
        copy_in.write_rows(&[INT4_OID, vertebra_postgres::codecs::BYTEA_OID], &big_rows),
    )
    .await
    .expect("duplex read/write must terminate within 1s without write-all deadlock")
    .expect_err("must surface 23505 server error");

    assert_eq!(err.sqlstate(), Some("23505"));
    session.close_shared().await.unwrap();

    let session2 = Arc::new(SessionHandle::from_pooled(
        tokio::time::timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("pool usable after duplex error")
            .unwrap(),
    ));
    let res = session2
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap();
    assert_eq!(res[0].rows[0][0], PgValue::Int4(5202));
    session2.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    hold_first_socket.notify_one();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_in_abort_sanitizes_nul_and_overlong_utf8_reason_idempotently() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (fail_tx, fail_rx) = oneshot::channel::<Vec<u8>>();
    let rollback_seen = Arc::new(AtomicUsize::new(0));
    let rb_peer = Arc::clone(&rollback_seen);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(5301, 8301))
            .await
            .unwrap();

        let (_, b1) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&b1), "BEGIN");
        let mut r1 = command_complete("BEGIN");
        r1.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&r1).await.unwrap();

        let _ = read_frontend_frame(&mut sock).await.unwrap();
        sock.write_all(&copy_response(b'G', 1, &[1])).await.unwrap();

        loop {
            let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
            if tag == b'd' {
                continue;
            } else if tag == b'f' {
                let _ = fail_tx.send(body);
                let mut err = error_response("57014", "COPY from stdin failed");
                err.extend_from_slice(&frame(b'Z', b"E"));
                sock.write_all(&err).await.unwrap();
                break;
            } else {
                panic!("unexpected tag {tag}");
            }
        }

        let (_, rb) = read_frontend_frame(&mut sock).await.unwrap();
        assert_eq!(query_sql(&rb), "ROLLBACK");
        rb_peer.fetch_add(1, Ordering::SeqCst);
        let mut rb_resp = command_complete("ROLLBACK");
        rb_resp.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&rb_resp).await.unwrap();

        answer_pool_reset_and_followup(&mut sock, 5301).await;
    });

    let pool = size_one_pool(port, Duration::from_secs(2));
    let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let mut copy_in = session
        .copy_from("COPY t FROM STDIN (FORMAT binary)")
        .await
        .unwrap();
    copy_in
        .write_rows(
            &[INT4_OID],
            &[vec![PgValue::Int4(1)], vec![PgValue::Int4(2)]],
        )
        .await
        .unwrap();

    let reason = format!("producer\0failed_{}", "\u{00e9}".repeat(200));
    copy_in.abort(&reason).await.unwrap();
    copy_in.abort("again").await.unwrap();

    let fail_wire = fail_rx.await.unwrap();
    assert_eq!(*fail_wire.last().unwrap(), 0);
    let msg_bytes = &fail_wire[..fail_wire.len() - 1];
    assert!(!msg_bytes.contains(&0));
    assert!(msg_bytes.len() <= MAX_COPY_FAIL_REASON_BYTES);
    let msg_str = std::str::from_utf8(msg_bytes).expect("valid UTF-8 truncation");
    assert!(msg_str.starts_with("producer?failed_"));
    assert_eq!(rollback_seen.load(Ordering::SeqCst), 1);
    assert_eq!(session.transaction_status(), TransactionStatus::Idle);

    session.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn copy_in_preserves_ambient_transaction_in_failed_state_on_abort_and_server_error() {
    for server_side_error in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let _ = read_initial_packet(&mut sock).await.unwrap();
            sock.write_all(&auth_ok_and_ready(5401, 8401))
                .await
                .unwrap();

            let (_, b1) = read_frontend_frame(&mut sock).await.unwrap();
            assert_eq!(query_sql(&b1), "BEGIN");
            let mut r1 = command_complete("BEGIN");
            r1.extend_from_slice(&frame(b'Z', b"T"));
            sock.write_all(&r1).await.unwrap();

            let (_, b2) = read_frontend_frame(&mut sock).await.unwrap();
            assert!(query_sql(&b2).starts_with("COPY"));
            sock.write_all(&copy_response(b'G', 1, &[1])).await.unwrap();

            if server_side_error {
                let (tag, _) = read_frontend_frame(&mut sock).await.unwrap();
                assert_eq!(tag, b'd');
                let mut err = error_response("23505", "unique violation");
                err.extend_from_slice(&frame(b'Z', b"E"));
                sock.write_all(&err).await.unwrap();
            } else {
                let (tag, _) = read_frontend_frame(&mut sock).await.unwrap();
                assert_eq!(tag, b'f');
                let mut err = error_response("57014", "COPY aborted");
                err.extend_from_slice(&frame(b'Z', b"E"));
                sock.write_all(&err).await.unwrap();
            }

            let rb = loop {
                let (ftag, fbody) = read_frontend_frame(&mut sock).await.unwrap();
                if matches!(ftag, b'd' | b'c') {
                    continue;
                }
                assert_eq!(ftag, b'Q', "expected Q ROLLBACK, got tag {}", ftag as char);
                break fbody;
            };
            assert_eq!(
                query_sql(&rb),
                "ROLLBACK",
                "CopyFromStream must not send COMMIT or implicit ROLLBACK before caller rollback()"
            );
            let mut rb_resp = command_complete("ROLLBACK");
            rb_resp.extend_from_slice(&frame(b'Z', b"I"));
            sock.write_all(&rb_resp).await.unwrap();
            let _ = read_frontend_frame(&mut sock).await;
        });

        let cfg = tcp_config(port, Duration::from_secs(2));
        let mut h = SessionHandle::from_config(cfg);
        h.connect().await.unwrap();
        let session = Arc::new(h);

        session.simple_query("BEGIN").await.unwrap();
        let mut copy_in = session
            .copy_from("COPY amb FROM STDIN (FORMAT binary)")
            .await
            .unwrap();

        if server_side_error {
            let err = match copy_in
                .write_rows(&[INT4_OID], &[vec![PgValue::Int4(1)]])
                .await
            {
                Err(e) => e,
                Ok(()) => copy_in.finish().await.unwrap_err(),
            };
            assert_eq!(err.sqlstate(), Some("23505"));
        } else {
            copy_in.abort("bad batch").await.unwrap();
        }

        assert_eq!(session.transaction_status(), TransactionStatus::Failed);
        session.rollback_client().await.unwrap();
        assert_eq!(session.transaction_status(), TransactionStatus::Idle);
        session.close_shared().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn copy_in_encoder_error_and_drop_abort_without_committing_and_reject_pipeline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let commits_seen = Arc::new(AtomicUsize::new(0));
    let commits_peer = Arc::clone(&commits_seen);
    let copy_fails_seen = Arc::new(AtomicUsize::new(0));
    let fails_peer = Arc::clone(&copy_fails_seen);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let _ = read_initial_packet(&mut sock).await.unwrap();
        sock.write_all(&auth_ok_and_ready(5501, 8501))
            .await
            .unwrap();

        for _round in 0..2 {
            loop {
                let (tag, body) = read_frontend_frame(&mut sock).await.unwrap();
                if tag == b'Q' {
                    let sql = query_sql(&body);
                    if sql == "BEGIN" {
                        let mut out = command_complete("BEGIN");
                        out.extend_from_slice(&frame(b'Z', b"T"));
                        sock.write_all(&out).await.unwrap();
                    } else if sql == "COMMIT" {
                        commits_peer.fetch_add(1, Ordering::SeqCst);
                        panic!("unexpected COMMIT on failed/dropped COPY FROM");
                    } else if sql.starts_with("COPY") {
                        sock.write_all(&copy_response(b'G', 1, &[1])).await.unwrap();
                        loop {
                            let (ctag, _) = read_frontend_frame(&mut sock).await.unwrap();
                            if ctag == b'd' {
                                continue;
                            } else if ctag == b'f' {
                                fails_peer.fetch_add(1, Ordering::SeqCst);
                                let mut err = error_response("57014", "COPY aborted");
                                err.extend_from_slice(&frame(b'Z', b"E"));
                                sock.write_all(&err).await.unwrap();
                                break;
                            }
                        }
                        let (_, rb) = read_frontend_frame(&mut sock).await.unwrap();
                        assert_eq!(query_sql(&rb), "ROLLBACK");
                        let mut out = command_complete("ROLLBACK");
                        out.extend_from_slice(&frame(b'Z', b"I"));
                        sock.write_all(&out).await.unwrap();
                        break;
                    } else if sql == "DISCARD ALL" {
                        let mut out = command_complete("DISCARD ALL");
                        out.extend_from_slice(&frame(b'Z', b"I"));
                        sock.write_all(&out).await.unwrap();
                    }
                }
            }
        }

        answer_pool_reset_and_followup(&mut sock, 5501).await;
    });

    let pool = size_one_pool(port, Duration::from_secs(2));
    {
        let session = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
        let mut copy_in = session
            .copy_from("COPY ts_tbl FROM STDIN (FORMAT binary)")
            .await
            .unwrap();
        let bad = vec![vec![PgValue::TemporalText {
            wire_text: "2024-11-03 01:30:00 CST".to_string(),
            kind: TemporalKind::TimestampTz,
            date_style: "ISO, MDY".to_string(),
            time_zone: "America/Chicago".to_string(),
        }]];
        let err = copy_in
            .write_rows(&[TIMESTAMPTZ_OID], &bad)
            .await
            .unwrap_err();
        assert!(
            matches!(err, PostgresError::Data(_)),
            "must return original Data error instead of 57014, got {err:?}"
        );
        assert_eq!(err.sqlstate(), None);

        let mut dropped_copy = session
            .copy_from("COPY ts_tbl FROM STDIN (FORMAT binary)")
            .await
            .unwrap();
        dropped_copy
            .write_rows(
                &[INT4_OID],
                &[
                    vec![PgValue::Int4(1)],
                    vec![PgValue::Int4(2)],
                    vec![PgValue::Int4(3)],
                ],
            )
            .await
            .unwrap();
        drop(dropped_copy);
        session.close_shared().await.unwrap();
    }

    let session2 = Arc::new(SessionHandle::from_pooled(pool.acquire().await.unwrap()));
    let check = session2
        .simple_query("SELECT pg_backend_pid()")
        .await
        .unwrap();
    assert_eq!(check[0].rows[0][0], PgValue::Int4(5501));
    assert_eq!(commits_seen.load(Ordering::SeqCst), 0);
    assert_eq!(copy_fails_seen.load(Ordering::SeqCst), 2);

    let token = session2.pipeline_begin().await.unwrap();
    let pipe_err = session2
        .copy_from("COPY ts_tbl FROM STDIN (FORMAT binary)")
        .await
        .unwrap_err();
    assert!(matches!(
        pipe_err,
        PostgresError::UnsupportedFeature(_) | PostgresError::Interface(_)
    ));
    let _ = session2.pipeline_finish(token, false).await;
    session2.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}
