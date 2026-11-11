//! Integration tests for generation-fenced PostgreSQL `CancelRequest` frames, Protocol 3.2
//! variable-length cancellation keys, delayed-cancel pool safety, ambient tx quarantine,
//! and bounded missing-key retirement (`ch16:t02`).

extern crate _postgres as vertebra_postgres;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use vertebra_postgres::codecs::INT4_OID;
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::pool::{PostgresPool, PostgresPoolOptions};
use vertebra_postgres::result::PgValue;
use vertebra_postgres::session::cancel::{CANCEL_REQUEST_CODE, encode_cancel_request_frame};
use vertebra_postgres::session::{
    CancelDisposition, CancelSecret, PostgresConnection, SessionHandle,
};
use vertebra_postgres::transaction::TransactionDisposition;

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready(pid: i32, secret: i32) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0_i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"application_name\0vertebra\0"));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
    let mut k = Vec::with_capacity(8);
    k.extend_from_slice(&pid.to_be_bytes());
    k.extend_from_slice(&secret.to_be_bytes());
    out.extend_from_slice(&frame(b'K', &k));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
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

fn select_int_response(val: i32, tx: u8, warm: bool) -> Vec<u8> {
    let mut col = BytesMut::new();
    col.put_i16(1);
    col.put_slice(b"v\0");
    col.put_u32(0);
    col.put_i16(0);
    col.put_u32(INT4_OID);
    col.put_i16(4);
    col.put_i32(-1);
    col.put_i16(if warm { 1 } else { 0 });

    let mut row = BytesMut::new();
    row.put_i16(1);
    if warm {
        row.put_i32(4);
        row.put_i32(val);
    } else {
        let s = val.to_string();
        row.put_i32(s.len() as i32);
        row.put_slice(s.as_bytes());
    }

    let mut out = Vec::new();
    if !warm {
        out.extend_from_slice(&frame(b'1', b""));
        out.extend_from_slice(&frame(b't', &0_i16.to_be_bytes()));
        out.extend_from_slice(&frame(b'T', &col));
    }
    out.extend_from_slice(&frame(b'2', b""));
    out.extend_from_slice(&frame(b'T', &col));
    out.extend_from_slice(&frame(b'D', &row));
    out.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
    out.extend_from_slice(&frame(b'3', b""));
    out.extend_from_slice(&frame(b'Z', &[tx]));
    out
}

async fn read_startup(sock: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut rest = vec![0u8; len - 4];
    sock.read_exact(&mut rest).await.unwrap();
    let mut full = len_buf.to_vec();
    full.extend_from_slice(&rest);
    full
}

async fn read_command_group(sock: &mut TcpStream) -> std::io::Result<Vec<(u8, Vec<u8>)>> {
    let mut out = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await?;
        let tag = hdr[0];
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await?;
        out.push((tag, body));
        if matches!(tag, b'S' | b'Q' | b'X') {
            return Ok(out);
        }
    }
}

fn tcp_raw(port: u16, connect_timeout: Duration) -> RawPostgresOptions {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.channel_binding = "disable".to_string();
    raw.connect_timeout = connect_timeout.as_secs_f64();
    raw
}

fn tcp_config(port: u16, connect_timeout: Duration) -> PostgresConfig {
    PostgresConfig::validate(tcp_raw(port, connect_timeout)).unwrap()
}

#[tokio::test]
async fn idle_session_cancel_returns_idle_noop_with_zero_cancel_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_srv = Arc::clone(&accepts);

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        accepts_srv.fetch_add(1, Ordering::SeqCst);
        let _ = read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready(4100, 0x1111_2222))
            .await
            .unwrap();

        let grp = read_command_group(&mut sock).await.unwrap();
        assert_eq!(grp.last().unwrap().0, b'S');
        sock.write_all(&select_int_response(7, b'I', false))
            .await
            .unwrap();

        let term = read_command_group(&mut sock).await.unwrap();
        assert_eq!(term[0].0, b'X');
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_secs(2))).unwrap();
    session.connect().await.unwrap();

    assert_eq!(session.cancel().await, CancelDisposition::IdleNoOp);
    assert_eq!(session.cancel().await, CancelDisposition::IdleNoOp);
    assert!(!session.will_retire());
    assert_eq!(accepts.load(Ordering::SeqCst), 1);

    let res = session
        .query_shared("SELECT 7 AS v", &[], None)
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], PgValue::Int4(7));
    session.close().await.unwrap();
    srv.await.unwrap();
}

#[tokio::test]
async fn active_query_sends_exact_16_byte_cancel_request_drains_57014_and_retires() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (query_seen_tx, query_seen_rx) = oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(4242, 0x1234_5678))
            .await
            .unwrap();

        let grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(grp.last().unwrap().0, b'S');
        let _ = query_seen_tx.send(());

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let mut cancel_pkt = Vec::new();
        cancel_sock.read_to_end(&mut cancel_pkt).await.unwrap();
        let mut expected = Vec::with_capacity(16);
        expected.extend_from_slice(&16_i32.to_be_bytes());
        expected.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        expected.extend_from_slice(&4242_i32.to_be_bytes());
        expected.extend_from_slice(&0x1234_5678_i32.to_be_bytes());
        assert_eq!(cancel_pkt, expected);

        let mut reply = error_response("57014", "canceling statement due to user request");
        reply.extend_from_slice(&frame(b'Z', b"I"));
        primary.write_all(&reply).await.unwrap();
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_secs(2))).unwrap();
    session.connect().await.unwrap();
    let session = Arc::new(session);
    let cancel_handle = session.postgres_cancel_handle();

    let session_q = Arc::clone(&session);
    let q_task = tokio::spawn(async move {
        session_q
            .query_shared("SELECT pg_sleep(10)", &[], None)
            .await
    });

    query_seen_rx.await.unwrap();
    let disp = cancel_handle.cancel_with_disposition().await;
    assert_eq!(disp, CancelDisposition::Sent);
    assert_eq!(
        cancel_handle.cancel_with_disposition().await,
        CancelDisposition::IdleNoOp
    );

    let err = q_task.await.unwrap().unwrap_err();
    match err {
        PostgresError::Server(srv_err) => assert_eq!(srv_err.sqlstate, "57014"),
        other => panic!("expected Server(57014), got {other:?}"),
    }
    assert!(session.will_retire());
    assert!(session.is_closed());
    srv.await.unwrap();
}

#[tokio::test]
async fn variable_length_protocol_32_cancel_secret_encodes_exact_frame_and_redacts_debug() {
    let secret_bytes = [0xAB_u8; 32];
    let secret = CancelSecret::Variable(Box::from(secret_bytes));
    let dbg = format!("{secret:?}");
    assert_eq!(dbg, "CancelSecret::Variable(<redacted 32 bytes>)");

    let mut encoded = BytesMut::new();
    encode_cancel_request_frame(7777, &secret, &mut encoded).unwrap();
    assert_eq!(encoded.len(), 44);
    assert_eq!(&encoded[0..4], &44_i32.to_be_bytes());
    assert_eq!(&encoded[4..8], &CANCEL_REQUEST_CODE.to_be_bytes());
    assert_eq!(&encoded[8..12], &7777_i32.to_be_bytes());
    assert_eq!(&encoded[12..44], &secret_bytes);

    let mut scratch = BytesMut::new();
    assert!(matches!(
        encode_cancel_request_frame(1, &CancelSecret::Variable(Box::from([])), &mut scratch),
        Err(PostgresError::Protocol(_))
    ));
    assert!(matches!(
        encode_cancel_request_frame(
            1,
            &CancelSecret::Variable(vec![1u8; 257].into_boxed_slice()),
            &mut scratch,
        ),
        Err(PostgresError::Protocol(_))
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready_tx, ready_rx) = oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(7777, 1))
            .await
            .unwrap();

        let _ = read_command_group(&mut primary).await.unwrap();
        let _ = ready_tx.send(());

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let mut pkt = Vec::new();
        cancel_sock.read_to_end(&mut pkt).await.unwrap();
        assert_eq!(pkt.len(), 44);
        assert_eq!(&pkt[0..4], &44_i32.to_be_bytes());
        assert_eq!(&pkt[4..8], &CANCEL_REQUEST_CODE.to_be_bytes());
        assert_eq!(&pkt[8..12], &7777_i32.to_be_bytes());
        assert_eq!(&pkt[12..44], &[0xCD_u8; 32]);

        let mut reply = error_response("57014", "cancelled");
        reply.extend_from_slice(&frame(b'Z', b"I"));
        primary.write_all(&reply).await.unwrap();
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_secs(2))).unwrap();
    session.connect().await.unwrap();
    session
        .with_connection(|conn, _| {
            let res = conn.set_variable_cancel_secret(7777, vec![0xCD_u8; 32]);
            std::future::ready(res)
        })
        .await
        .unwrap();
    let session = Arc::new(session);
    let handle = session.postgres_cancel_handle();
    let s2 = Arc::clone(&session);
    let q = tokio::spawn(async move { s2.simple_query_shared("SELECT pg_sleep(10)").await });

    ready_rx.await.unwrap();
    assert_eq!(
        handle.cancel_with_disposition().await,
        CancelDisposition::Sent
    );
    assert!(q.await.unwrap().is_err());
    srv.await.unwrap();
}

#[tokio::test]
async fn delayed_cancel_after_fast_completion_retires_old_backend_and_protects_next_pool_lease() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancelled_pids = Arc::new(std::sync::Mutex::new(Vec::<i32>::new()));
    let cancelled_srv = Arc::clone(&cancelled_pids);
    let (q_seen_tx, q_seen_rx) = oneshot::channel::<()>();
    let (finish_q_tx, finish_q_rx) = oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        let mut q_seen_opt = Some(q_seen_tx);
        let mut finish_q_opt = Some(finish_q_rx);
        let mut next_pid = 5000_i32;

        for _ in 0..3 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let first = read_startup(&mut sock).await;
            if first.len() >= 16
                && i32::from_be_bytes([first[4], first[5], first[6], first[7]])
                    == CANCEL_REQUEST_CODE
            {
                let pid = i32::from_be_bytes([first[8], first[9], first[10], first[11]]);
                cancelled_srv.lock().unwrap().push(pid);
                continue;
            }

            next_pid += 1;
            let pid = next_pid;
            let c_pids = Arc::clone(&cancelled_srv);
            let maybe_seen = q_seen_opt.take();
            let maybe_finish = finish_q_opt.take();
            tokio::spawn(async move {
                let _ = c_pids;
                sock.write_all(&auth_ok_and_ready(pid, 9000 + pid))
                    .await
                    .unwrap();
                let mut seen = maybe_seen;
                let mut finish = maybe_finish;
                while let Ok(grp) = read_command_group(&mut sock).await {
                    let last = grp.last().unwrap().0;
                    if last == b'X' {
                        return;
                    }
                    if last == b'Q' {
                        let mut r = frame(b'C', b"DISCARD ALL\0");
                        r.extend_from_slice(&frame(b'Z', b"I"));
                        if sock.write_all(&r).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    if let Some(tx) = seen.take() {
                        let _ = tx.send(());
                    }
                    if let Some(rx) = finish.take() {
                        let _ = rx.await;
                    }
                    let warm = !grp.iter().any(|(t, _)| *t == b'P');
                    if sock
                        .write_all(&select_int_response(pid, b'I', warm))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });

    let cfg = tcp_config(port, Duration::from_secs(2));
    let opts = PostgresPoolOptions {
        min_size: 0,
        max_size: 1,
        acquire_timeout: Duration::from_secs(2),
        reset_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_secs(2),
        ..PostgresPoolOptions::default()
    };
    let pool = PostgresPool::new(cfg, opts).unwrap();

    let lease1 = pool.acquire().await.unwrap();
    let session1 = Arc::new(SessionHandle::from_pooled(lease1));

    let enter_barrier = Arc::new(std::sync::Barrier::new(2));
    let exit_barrier = Arc::new(std::sync::Barrier::new(2));
    let eb1 = Arc::clone(&enter_barrier);
    let xb1 = Arc::clone(&exit_barrier);
    session1.set_cancel_dial_hook(Some(Arc::new(move || {
        eb1.wait();
        xb1.wait();
    })));

    let cancel1 = session1.postgres_cancel_handle();
    let s1_q = Arc::clone(&session1);
    let q_task = tokio::spawn(async move {
        s1_q.query_shared("SELECT pg_backend_pid() AS v", &[], None)
            .await
    });

    q_seen_rx.await.unwrap();
    let cancel_task = tokio::spawn(async move { cancel1.cancel_with_disposition().await });

    tokio::task::spawn_blocking({
        let eb = Arc::clone(&enter_barrier);
        move || eb.wait()
    })
    .await
    .unwrap();

    let _ = finish_q_tx.send(());
    let fast_res = q_task.await.unwrap().unwrap();
    assert_eq!(fast_res.rows[0][0], PgValue::Int4(5001));

    tokio::task::spawn_blocking(move || exit_barrier.wait())
        .await
        .unwrap();
    assert_eq!(cancel_task.await.unwrap(), CancelDisposition::Sent);
    assert!(session1.will_retire());

    session1.close_shared().await.unwrap();

    let lease2 = pool.acquire().await.unwrap();
    let session2 = SessionHandle::from_pooled(lease2);
    let stale_gen = session2.reserve_operation_generation();
    let res2 = session2
        .query_shared("SELECT pg_backend_pid() AS v", &[], None)
        .await
        .unwrap();
    assert_eq!(res2.rows[0][0], PgValue::Int4(5002));
    assert_eq!(*cancelled_pids.lock().unwrap(), vec![5001]);

    session2.close_shared().await.unwrap();
    let _ = stale_gen;
    pool.close(Duration::from_secs(2)).await.unwrap();
    srv.await.unwrap();
}

#[tokio::test]
async fn ambient_tx_cancel_quarantines_session_until_rollback_client_retires_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (q_seen_tx, q_seen_rx) = oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(6101, 0x2222_3333))
            .await
            .unwrap();

        let begin_grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(begin_grp[0].0, b'Q');
        let mut r_begin = frame(b'C', b"BEGIN\0");
        r_begin.extend_from_slice(&frame(b'Z', b"T"));
        primary.write_all(&r_begin).await.unwrap();

        let slow_grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(slow_grp.last().unwrap().0, b'S');
        let _ = q_seen_tx.send(());

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let mut pkt = Vec::new();
        cancel_sock.read_to_end(&mut pkt).await.unwrap();
        assert_eq!(pkt.len(), 16);

        let mut r_err = error_response("57014", "cancelled in tx");
        r_err.extend_from_slice(&frame(b'Z', b"E"));
        primary.write_all(&r_err).await.unwrap();

        let rb_grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(rb_grp[0].0, b'Q');
        assert_eq!(rb_grp[0].1, b"ROLLBACK\0");
        let mut r_rb = frame(b'C', b"ROLLBACK\0");
        r_rb.extend_from_slice(&frame(b'Z', b"I"));
        primary.write_all(&r_rb).await.unwrap();
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_secs(2))).unwrap();
    session.connect().await.unwrap();
    session.simple_query_shared("BEGIN").await.unwrap();

    let session = Arc::new(session);
    let cancel_handle = session.postgres_cancel_handle();
    let s_q = Arc::clone(&session);
    let q_task =
        tokio::spawn(async move { s_q.query_shared("SELECT pg_sleep(10)", &[], None).await });

    q_seen_rx.await.unwrap();
    assert_eq!(
        cancel_handle.cancel_with_disposition().await,
        CancelDisposition::Sent
    );
    let q_err = q_task.await.unwrap().unwrap_err();
    assert!(matches!(q_err, PostgresError::Server(ref s) if s.sqlstate == "57014"));

    let blocked = session
        .query_shared("SELECT 1", &[], None)
        .await
        .unwrap_err();
    assert!(matches!(blocked, PostgresError::Interface(_)));

    let disp = session.rollback_client().await.unwrap();
    assert_eq!(disp, TransactionDisposition::RolledBack);
    assert!(session.is_closed());
    srv.await.unwrap();
}

#[tokio::test]
async fn missing_backend_key_wakes_drain_and_retires_within_connect_timeout() {
    let (client_io, mut server_io) = tokio::io::duplex(8192);
    let cfg = tcp_config(5432, Duration::from_millis(80));
    let mut params = BTreeMap::new();
    params.insert("DateStyle".to_string(), "ISO, MDY".to_string());
    params.insert("TimeZone".to_string(), "UTC".to_string());
    let conn = PostgresConnection::from_stream(client_io, &cfg, params, None);
    let session = Arc::new(SessionHandle::from_direct(cfg, conn));

    let (seen_tx, seen_rx) = oneshot::channel::<()>();
    let srv = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        let _ = server_io.read(&mut buf).await.unwrap();
        let _ = seen_tx.send(());
        tokio::time::sleep(Duration::from_millis(400)).await;
    });

    let s_q = Arc::clone(&session);
    let start = tokio::time::Instant::now();
    let q_task = tokio::spawn(async move { s_q.simple_query_shared("SELECT pg_sleep(10)").await });

    seen_rx.await.unwrap();
    assert_eq!(session.cancel().await, CancelDisposition::MissingBackendKey);
    let err = q_task.await.unwrap().unwrap_err();
    assert!(matches!(err, PostgresError::Operational(_)));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert!(session.is_closed());
    assert!(session.will_retire());
    srv.abort();
}
