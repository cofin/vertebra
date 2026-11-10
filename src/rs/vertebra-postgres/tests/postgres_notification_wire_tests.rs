//! Wire-protocol integration tests for dedicated PostgreSQL `LISTEN`/`NOTIFY` sessions
//! (`_postgres::notifications::PostgresListener` and `_postgres::session::establish_standalone_connection`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use _postgres::PoolerMode;
use _postgres::config::{PostgresConfig, RawPostgresOptions};
use _postgres::errors::PostgresError;
use _postgres::notifications::{
    DEFAULT_NOTIFICATION_MAX_BYTES, DEFAULT_NOTIFICATION_QUEUE_CAPACITY, ListenerOptions,
    MAX_CHANNEL_BYTES, MAX_RECONNECT_ATTEMPTS, NotificationFrame, PostgresListener,
    validate_and_quote_channel,
};
use _postgres::pool::{PostgresPool, PostgresPoolOptions};
use _postgres::session::establish_standalone_connection;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};

fn build_backend_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let len = (4 + payload.len()) as u32;
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn auth_ok_frame() -> Vec<u8> {
    build_backend_frame(b'R', &0_i32.to_be_bytes())
}

fn parameter_status_frame(name: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(name.len() + value.len() + 2);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    build_backend_frame(b'S', &body)
}

fn backend_key_frame(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret.to_be_bytes());
    build_backend_frame(b'K', &body)
}

fn ready_for_query_frame(status: u8) -> Vec<u8> {
    build_backend_frame(b'Z', &[status])
}

fn command_complete_frame(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    build_backend_frame(b'C', &body)
}

fn notification_frame(pid: i32, channel: &str, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(4 + channel.len() + 1 + payload.len() + 1);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(channel.as_bytes());
    body.push(0);
    body.extend_from_slice(payload);
    body.push(0);
    build_backend_frame(b'A', &body)
}

fn notice_frame(message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"NOTICE\0");
    body.push(b'C');
    body.extend_from_slice(b"00000\0");
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    build_backend_frame(b'N', &body)
}

fn error_response_frame(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    build_backend_frame(b'E', &body)
}

fn single_int4_select_response(val: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rd = Vec::new();
    rd.extend_from_slice(&1_i16.to_be_bytes());
    rd.extend_from_slice(b"?column?\0");
    rd.extend_from_slice(&0_i32.to_be_bytes());
    rd.extend_from_slice(&0_i16.to_be_bytes());
    rd.extend_from_slice(&23_i32.to_be_bytes());
    rd.extend_from_slice(&4_i16.to_be_bytes());
    rd.extend_from_slice(&(-1_i32).to_be_bytes());
    rd.extend_from_slice(&0_i16.to_be_bytes());
    out.extend_from_slice(&build_backend_frame(b'T', &rd));

    let mut dr = Vec::new();
    dr.extend_from_slice(&1_i16.to_be_bytes());
    dr.extend_from_slice(&(val.len() as i32).to_be_bytes());
    dr.extend_from_slice(val.as_bytes());
    out.extend_from_slice(&build_backend_frame(b'D', &dr));
    out.extend_from_slice(&command_complete_frame("SELECT 1"));
    out.extend_from_slice(&ready_for_query_frame(b'I'));
    out
}

async fn read_startup_params(socket: &mut TcpStream) -> BTreeMap<String, String> {
    let mut first8 = [0_u8; 8];
    socket
        .read_exact(&mut first8)
        .await
        .expect("read startup header");
    let mut len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
    let mut code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
    if len == 8 && code == 80_877_103 {
        socket.write_all(b"N").await.expect("reject SSLRequest");
        socket.flush().await.expect("flush SSL N");
        socket
            .read_exact(&mut first8)
            .await
            .expect("read post-SSL startup header");
        len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
        code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
    }
    assert_eq!(code, 0x0003_0000, "expected PostgreSQL Protocol 3.0");
    let mut body = vec![0_u8; len - 8];
    socket
        .read_exact(&mut body)
        .await
        .expect("read startup body");
    assert_eq!(body.last().copied(), Some(0));
    let mut parts = body[..body.len() - 1].split(|&b| b == 0);
    let mut map = BTreeMap::new();
    while let Some(k) = parts.next() {
        if k.is_empty() {
            break;
        }
        let v = parts.next().expect("startup parameter value");
        map.insert(
            String::from_utf8(k.to_vec()).expect("utf8 startup key"),
            String::from_utf8(v.to_vec()).expect("utf8 startup val"),
        );
    }
    map
}

async fn complete_startup_ready(socket: &mut TcpStream, pid: i32) -> BTreeMap<String, String> {
    let params = read_startup_params(socket).await;
    let mut banner = Vec::new();
    banner.extend_from_slice(&auth_ok_frame());
    banner.extend_from_slice(&parameter_status_frame("server_version", "16.4"));
    banner.extend_from_slice(&parameter_status_frame("client_encoding", "UTF8"));
    if let Some(app) = params.get("application_name") {
        banner.extend_from_slice(&parameter_status_frame("application_name", app));
    }
    banner.extend_from_slice(&backend_key_frame(pid, 0x1234_5678));
    banner.extend_from_slice(&ready_for_query_frame(b'I'));
    socket
        .write_all(&banner)
        .await
        .expect("write startup banner");
    socket.flush().await.expect("flush startup banner");
    params
}

#[derive(Debug, PartialEq, Eq)]
enum FrontendMsg {
    Query(String),
    Terminate,
    Other(u8, Vec<u8>),
    Eof,
}

async fn read_frontend_msg(socket: &mut TcpStream) -> FrontendMsg {
    let mut hdr = [0_u8; 5];
    match socket.read_exact(&mut hdr).await {
        Ok(_) => {}
        Err(_) => return FrontendMsg::Eof,
    }
    let tag = hdr[0];
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    assert!(len >= 4, "invalid frontend frame length {len}");
    let mut body = vec![0_u8; len - 4];
    if !body.is_empty() && socket.read_exact(&mut body).await.is_err() {
        return FrontendMsg::Eof;
    }
    match tag {
        b'Q' => {
            assert_eq!(body.last().copied(), Some(0));
            let sql = String::from_utf8(body[..body.len() - 1].to_vec()).expect("utf8 query");
            FrontendMsg::Query(sql)
        }
        b'X' => FrontendMsg::Terminate,
        other => FrontendMsg::Other(other, body),
    }
}

fn direct_config(port: u16) -> PostgresConfig {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.pooler_mode = "direct".to_string();
    raw.connect_timeout = 2.0;
    PostgresConfig::validate(raw).expect("valid PostgresConfig")
}

fn default_listener_options() -> ListenerOptions {
    ListenerOptions {
        queue_capacity: DEFAULT_NOTIFICATION_QUEUE_CAPACITY,
        max_queue_bytes: DEFAULT_NOTIFICATION_MAX_BYTES,
        reconnect: false,
    }
}

#[test]
fn validate_and_quote_channel_enforces_identifier_syntax_and_63_byte_utf8_cap() {
    assert_eq!(DEFAULT_NOTIFICATION_QUEUE_CAPACITY, 1024);
    assert_eq!(DEFAULT_NOTIFICATION_MAX_BYTES, 8 * 1024 * 1024);
    assert_eq!(MAX_CHANNEL_BYTES, 63);
    assert_eq!(MAX_RECONNECT_ATTEMPTS, 8);

    assert_eq!(validate_and_quote_channel("orders").unwrap(), "\"orders\"");
    assert_eq!(
        validate_and_quote_channel("Orders_1").unwrap(),
        "\"Orders_1\""
    );
    assert_eq!(
        validate_and_quote_channel("_audit$2").unwrap(),
        "\"_audit$2\""
    );
    assert_eq!(validate_and_quote_channel("select").unwrap(), "\"select\"");

    let max_ascii = "a".repeat(63);
    assert_eq!(
        validate_and_quote_channel(&max_ascii).unwrap(),
        format!("\"{max_ascii}\"")
    );

    let max_utf8 = "é".repeat(31);
    assert_eq!(max_utf8.len(), 62);
    assert_eq!(
        validate_and_quote_channel(&max_utf8).unwrap(),
        format!("\"{max_utf8}\"")
    );

    let invalid_cases = [
        "",
        &"a".repeat(64),
        &"é".repeat(32),
        "a\0b",
        "bad;drop",
        "schema.orders",
        "bad space",
        " leading",
        "trailing ",
        "1starts_with_digit",
        "bad\"quote",
        "bad'quote",
        "kebab-channel",
    ];
    for bad in invalid_cases {
        let err = validate_and_quote_channel(bad).expect_err(bad);
        assert!(
            matches!(err, PostgresError::Data(_)),
            "expected PostgresError::Data for {bad:?}, got {err:?}"
        );
    }
}

#[tokio::test]
async fn idle_push_delivery_byte_split_framing_interleaved_frames_and_max_size_1_pool_independence()
{
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();
    let (push_now_tx, push_now_rx) = oneshot::channel::<()>();
    let (server_done_tx, server_done_rx) = oneshot::channel::<usize>();

    tokio::spawn(async move {
        let (mut listener_sock, _) = tcp.accept().await.expect("accept listener");
        let listener_params = complete_startup_ready(&mut listener_sock, 4100).await;
        assert!(!listener_params.contains_key("replication"));
        assert_eq!(
            read_frontend_msg(&mut listener_sock).await,
            FrontendMsg::Query("LISTEN \"orders\"".to_string())
        );
        let mut ack = command_complete_frame("LISTEN");
        ack.extend_from_slice(&ready_for_query_frame(b'I'));
        listener_sock.write_all(&ack).await.unwrap();
        listener_sock.flush().await.unwrap();

        let pool_task = tokio::spawn(async move {
            let (mut pool_sock, _) = tcp.accept().await.expect("accept pool conn");
            complete_startup_ready(&mut pool_sock, 4200).await;
            let mut select_count = 0_usize;
            loop {
                match read_frontend_msg(&mut pool_sock).await {
                    FrontendMsg::Query(sql) if sql == "SELECT 1" => {
                        select_count += 1;
                        let resp = single_int4_select_response("1");
                        pool_sock.write_all(&resp).await.unwrap();
                        pool_sock.flush().await.unwrap();
                    }
                    FrontendMsg::Query(sql) if sql == "DISCARD ALL" => {
                        let mut resp = command_complete_frame("DISCARD ALL");
                        resp.extend_from_slice(&ready_for_query_frame(b'I'));
                        pool_sock.write_all(&resp).await.unwrap();
                        pool_sock.flush().await.unwrap();
                    }
                    FrontendMsg::Terminate | FrontendMsg::Eof => break,
                    other => panic!("unexpected pool frontend frame: {other:?}"),
                }
            }
            select_count
        });

        push_now_rx.await.expect("wait for push trigger");
        let mut stream_bytes = Vec::new();
        stream_bytes.extend_from_slice(&notification_frame(4101, "orders", b""));
        stream_bytes.extend_from_slice(&notice_frame("advisory background notice"));
        stream_bytes.extend_from_slice(&notification_frame(
            4102,
            "orders",
            "{\"id\":42,\"note\":\"café\"}".as_bytes(),
        ));
        stream_bytes.extend_from_slice(&parameter_status_frame("application_name", "vtb-live"));
        stream_bytes.extend_from_slice(&notification_frame(4103, "orders", b"dup"));
        stream_bytes.extend_from_slice(&notification_frame(4103, "orders", b"dup"));

        for byte in stream_bytes {
            listener_sock.write_all(&[byte]).await.unwrap();
            listener_sock.flush().await.unwrap();
        }

        let end_msg = read_frontend_msg(&mut listener_sock).await;
        assert!(
            matches!(end_msg, FrontendMsg::Terminate | FrontendMsg::Eof),
            "idle delivery must send zero client queries; got {end_msg:?}"
        );

        let selects = pool_task.await.expect("pool peer");
        let _ = server_done_tx.send(selects);
    });

    let cfg = direct_config(port);
    let listener = PostgresListener::open(
        cfg.clone(),
        vec!["orders".to_string()],
        default_listener_options(),
    )
    .await
    .expect("open listener");
    assert!(!listener.is_closed());

    let pool_opts = PostgresPoolOptions {
        min_size: 0,
        max_size: 1,
        acquire_timeout: Duration::from_secs(2),
        ..PostgresPoolOptions::default()
    };
    let pool = PostgresPool::new(cfg, pool_opts).expect("create max_size=1 pool");

    let mut held_lease = pool.acquire().await.expect("acquire single pool slot");
    let r1 = held_lease.simple_query("SELECT 1").await.expect("select 1");
    assert_eq!(r1.len(), 1);

    push_now_tx.send(()).expect("trigger push");

    let f1 = listener.next().await.expect("frame 1").expect("some 1");
    assert_eq!(
        f1,
        NotificationFrame {
            pid: 4101,
            channel: "orders".to_string(),
            payload: Bytes::new(),
            reconnected_gap: false,
        }
    );
    let f2 = listener.next().await.expect("frame 2").expect("some 2");
    assert_eq!(
        f2,
        NotificationFrame {
            pid: 4102,
            channel: "orders".to_string(),
            payload: Bytes::from_static("{\"id\":42,\"note\":\"café\"}".as_bytes()),
            reconnected_gap: false,
        }
    );

    held_lease.release().await.expect("release lease 1");
    let mut lease2 = pool
        .acquire()
        .await
        .expect("re-acquire max_size=1 slot while listener active");
    let r2 = lease2.simple_query("SELECT 1").await.expect("select 2");
    assert_eq!(r2.len(), 1);
    lease2.release().await.expect("release lease 2");

    let f3 = listener.next().await.expect("frame 3").expect("some 3");
    let f4 = listener.next().await.expect("frame 4").expect("some 4");
    let expected_dup = NotificationFrame {
        pid: 4103,
        channel: "orders".to_string(),
        payload: Bytes::from_static(b"dup"),
        reconnected_gap: false,
    };
    assert_eq!(f3, expected_dup);
    assert_eq!(f4, expected_dup);

    listener.close().await.expect("close listener");
    assert!(listener.is_closed());
    assert_eq!(listener.next().await.expect("eof after close"), None);

    pool.close(Duration::from_secs(2))
        .await
        .expect("close pool");
    let selects = server_done_rx.await.expect("server completed");
    assert_eq!(selects, 2);
}

#[tokio::test]
async fn mid_listen_notification_deduplicated_subscribe_unconsumed_front_slot_and_single_waiter() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();
    let release_ready = Arc::new(Notify::new());
    let release_ready_peer = Arc::clone(&release_ready);
    let saw_mid_listen = Arc::new(Notify::new());
    let saw_mid_listen_peer = Arc::clone(&saw_mid_listen);
    let (push_pair_tx, push_pair_rx) = oneshot::channel::<()>();
    let (push_waiter_tx, push_waiter_rx) = oneshot::channel::<()>();

    let peer = tokio::spawn(async move {
        let (mut sock, _) = tcp.accept().await.expect("accept");
        complete_startup_ready(&mut sock, 5000).await;

        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"a\"".to_string())
        );
        sock.write_all(&notification_frame(5001, "a", b"during-listen"))
            .await
            .unwrap();
        sock.flush().await.unwrap();
        saw_mid_listen_peer.notify_one();

        release_ready_peer.notified().await;
        let mut finish = command_complete_frame("LISTEN");
        finish.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&finish).await.unwrap();
        sock.flush().await.unwrap();

        push_pair_rx.await.unwrap();
        let mut two = notification_frame(5002, "a", b"f1");
        two.extend_from_slice(&notification_frame(5003, "a", b"f2"));
        sock.write_all(&two).await.unwrap();
        sock.flush().await.unwrap();

        push_waiter_rx.await.unwrap();
        sock.write_all(&notification_frame(5004, "a", b"single-waiter-ok"))
            .await
            .unwrap();
        sock.flush().await.unwrap();

        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("UNLISTEN \"a\"".to_string())
        );
        let mut unlisten_reply = notification_frame(5005, "a", b"during-unlisten");
        unlisten_reply.extend_from_slice(&command_complete_frame("UNLISTEN"));
        unlisten_reply.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&unlisten_reply).await.unwrap();
        sock.flush().await.unwrap();

        let end = read_frontend_msg(&mut sock).await;
        assert!(matches!(end, FrontendMsg::Terminate | FrontendMsg::Eof));
    });

    let listener = Arc::new(
        PostgresListener::open(direct_config(port), Vec::new(), default_listener_options())
            .await
            .expect("open with empty channels"),
    );

    let sub_listener = Arc::clone(&listener);
    let sub_task =
        tokio::spawn(async move { sub_listener.subscribe(vec!["a".into(), "a".into()]).await });

    saw_mid_listen.notified().await;
    tokio::task::yield_now().await;
    assert!(
        !sub_task.is_finished(),
        "subscribe must not resolve before backend ReadyForQuery"
    );
    release_ready.notify_one();
    sub_task.await.unwrap().expect("subscribe resolved");

    listener
        .subscribe(vec!["a".into()])
        .await
        .expect("idempotent duplicate subscribe");
    listener
        .subscribe(Vec::new())
        .await
        .expect("empty subscribe no-op");

    let mid = listener.next().await.unwrap().expect("mid-listen frame");
    assert_eq!(mid.pid, 5001);
    assert_eq!(mid.channel, "a");
    assert_eq!(mid.payload, Bytes::from_static(b"during-listen"));
    assert!(!mid.reconnected_gap);

    {
        let mut cancel_fut = Box::pin(listener.next());
        assert!(
            tokio::time::timeout(Duration::from_millis(15), &mut cancel_fut)
                .await
                .is_err()
        );
    }

    let aborted_listener = Arc::clone(&listener);
    let aborted_waiter = tokio::spawn(async move { aborted_listener.next().await });
    tokio::task::yield_now().await;
    aborted_waiter.abort();
    let _ = aborted_waiter.await;

    push_pair_tx.send(()).unwrap();
    let mut popped_f1 = listener.next().await.unwrap().expect("f1");
    assert_eq!(popped_f1.payload, Bytes::from_static(b"f1"));
    popped_f1.reconnected_gap = true;
    listener.push_front_unconsumed(popped_f1.clone());

    let restored = listener.next().await.unwrap().expect("restored f1");
    assert_eq!(restored, popped_f1);
    assert!(restored.reconnected_gap);

    let f2 = listener.next().await.unwrap().expect("f2");
    assert_eq!(f2.payload, Bytes::from_static(b"f2"));
    assert!(!f2.reconnected_gap);

    let waiter1_listener = Arc::clone(&listener);
    let (entered_tx, entered_rx) = oneshot::channel::<()>();
    let waiter1 = tokio::spawn(async move {
        let fut = waiter1_listener.next();
        tokio::pin!(fut);
        let _ = tokio::time::timeout(Duration::from_millis(10), &mut fut).await;
        let _ = entered_tx.send(());
        fut.await
    });
    entered_rx.await.unwrap();

    let dup_err = listener
        .next()
        .await
        .expect_err("concurrent next() must fail with Interface");
    assert!(matches!(dup_err, PostgresError::Interface(_)));

    push_waiter_tx.send(()).unwrap();
    let w1_frame = waiter1
        .await
        .unwrap()
        .expect("first waiter ok")
        .expect("frame");
    assert_eq!(w1_frame.payload, Bytes::from_static(b"single-waiter-ok"));

    listener
        .unsubscribe(vec!["a".into(), "a".into()])
        .await
        .expect("deduplicated unsubscribe");
    listener
        .unsubscribe(vec!["a".into(), "never_subscribed".into()])
        .await
        .expect("idempotent unsubscribe for unsubscribed channels");

    let during_unlisten = listener
        .next()
        .await
        .unwrap()
        .expect("during-unlisten frame");
    assert_eq!(
        during_unlisten.payload,
        Bytes::from_static(b"during-unlisten")
    );

    listener.close().await.expect("close");
    peer.await.expect("peer cleanly joined");
}

#[tokio::test]
async fn control_commands_are_not_starved_by_high_rate_notifications() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();

    let peer = tokio::spawn(async move {
        let (mut sock, _) = tcp.accept().await.expect("accept");
        complete_startup_ready(&mut sock, 5500).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"firehose\"".to_string())
        );
        let mut ack = command_complete_frame("LISTEN");
        ack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&ack).await.unwrap();
        sock.flush().await.unwrap();

        let mut batch = Vec::new();
        for idx in 0..200_i32 {
            batch.extend_from_slice(&notification_frame(
                5500 + idx,
                "firehose",
                b"burst-payload",
            ));
        }
        sock.write_all(&batch).await.unwrap();
        sock.flush().await.unwrap();

        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"ctrl\"".to_string())
        );
        let mut ctrl_ack = command_complete_frame("LISTEN");
        ctrl_ack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&ctrl_ack).await.unwrap();
        sock.flush().await.unwrap();

        let _ = read_frontend_msg(&mut sock).await;
    });

    let listener = PostgresListener::open(
        direct_config(port),
        vec!["firehose".to_string()],
        default_listener_options(),
    )
    .await
    .expect("open firehose");

    let first = listener.next().await.unwrap().expect("first burst frame");
    assert_eq!(first.channel, "firehose");

    tokio::time::timeout(
        Duration::from_secs(1),
        listener.subscribe(vec!["ctrl".to_string()]),
    )
    .await
    .expect("control command must not starve")
    .expect("subscribe ctrl");

    listener.close().await.expect("close");
    peer.await.expect("peer");
}

#[tokio::test]
async fn pooler_mode_and_invalid_channel_batches_rejected_atomically_before_dial_or_sql() {
    let probe = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
    let probe_port = probe.local_addr().unwrap().port();
    let dial_count = Arc::new(AtomicUsize::new(0));
    let dial_counter = Arc::clone(&dial_count);
    let accept_guard = tokio::spawn(async move {
        while probe.accept().await.is_ok() {
            dial_counter.fetch_add(1, Ordering::SeqCst);
        }
    });

    for rejected_mode in ["transaction", "statement", "auto"] {
        let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
        raw.port = i64::from(probe_port);
        raw.sslmode = "disable".to_string();
        raw.pooler_mode = rejected_mode.to_string();
        let cfg = PostgresConfig::validate(raw).expect("valid config");
        let err =
            PostgresListener::open(cfg, vec!["orders".to_string()], default_listener_options())
                .await
                .expect_err("non-direct/session pooler_mode must fail");
        assert!(
            matches!(err, PostgresError::Interface(_)),
            "mode={rejected_mode} got {err:?}"
        );
    }

    for bad_opts in [
        ListenerOptions {
            queue_capacity: 0,
            max_queue_bytes: DEFAULT_NOTIFICATION_MAX_BYTES,
            reconnect: false,
        },
        ListenerOptions {
            queue_capacity: DEFAULT_NOTIFICATION_QUEUE_CAPACITY,
            max_queue_bytes: 0,
            reconnect: false,
        },
    ] {
        let err = PostgresListener::open(
            direct_config(probe_port),
            vec!["orders".to_string()],
            bad_opts,
        )
        .await
        .expect_err("zero capacity/bytes must fail");
        assert!(matches!(err, PostgresError::Interface(_)));
    }

    for bad_batch in [
        vec!["good".to_string(), "bad;drop".to_string()],
        vec!["a".repeat(64)],
        vec!["é".repeat(32)],
    ] {
        let err = PostgresListener::open(
            direct_config(probe_port),
            bad_batch,
            default_listener_options(),
        )
        .await
        .expect_err("invalid initial channel batch must fail");
        assert!(matches!(err, PostgresError::Data(_)));
    }

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        dial_count.load(Ordering::SeqCst),
        0,
        "pre-dial validation must make zero TCP connection attempts"
    );
    accept_guard.abort();

    let session_tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let session_port = session_tcp.local_addr().unwrap().port();
    let session_peer = tokio::spawn(async move {
        let (mut sock, _) = session_tcp.accept().await.expect("accept session listener");
        complete_startup_ready(&mut sock, 5900).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"existing\"".to_string())
        );
        let mut ack = command_complete_frame("LISTEN");
        ack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&ack).await.unwrap();
        sock.flush().await.unwrap();

        let final_msg = read_frontend_msg(&mut sock).await;
        assert!(
            matches!(final_msg, FrontendMsg::Terminate | FrontendMsg::Eof),
            "invalid subscribe/unsubscribe batches must emit zero SQL frames, got {final_msg:?}"
        );
    });

    let mut session_cfg = direct_config(session_port);
    session_cfg.pooler_mode = PoolerMode::Session;
    let listener = PostgresListener::open(
        session_cfg,
        vec!["existing".to_string()],
        default_listener_options(),
    )
    .await
    .expect("PoolerMode::Session allowed");

    let sub_err = listener
        .subscribe(vec!["good".to_string(), "bad;drop".to_string()])
        .await
        .expect_err("mixed subscribe batch must fail atomically");
    assert!(matches!(sub_err, PostgresError::Data(_)));

    let len_err = listener
        .subscribe(vec!["good".to_string(), "a".repeat(64)])
        .await
        .expect_err("64-byte channel in subscribe must fail atomically");
    assert!(matches!(len_err, PostgresError::Data(_)));

    let unsub_err = listener
        .unsubscribe(vec!["existing".to_string(), "bad;drop".to_string()])
        .await
        .expect_err("mixed unsubscribe batch must fail atomically");
    assert!(matches!(unsub_err, PostgresError::Data(_)));

    listener.close().await.expect("close");
    session_peer.await.expect("session peer verified zero SQL");
}

#[tokio::test]
async fn bounded_frame_and_byte_overflow_fail_closed_with_operational_error() {
    let tcp1 = TcpListener::bind("127.0.0.1:0").await.expect("bind 1");
    let port1 = tcp1.local_addr().unwrap().port();
    let accepts1 = Arc::new(AtomicUsize::new(0));
    let accepts1_peer = Arc::clone(&accepts1);

    let peer1 = tokio::spawn(async move {
        let (mut sock, _) = tcp1.accept().await.expect("accept");
        accepts1_peer.fetch_add(1, Ordering::SeqCst);
        complete_startup_ready(&mut sock, 6100).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"orders\"".to_string())
        );
        let mut reply = command_complete_frame("LISTEN");
        reply.extend_from_slice(&ready_for_query_frame(b'I'));
        for idx in 0..4_i32 {
            reply.extend_from_slice(&notification_frame(6100 + idx, "orders", b"x"));
        }
        sock.write_all(&reply).await.unwrap();
        sock.flush().await.unwrap();

        let end = tokio::time::timeout(Duration::from_secs(2), read_frontend_msg(&mut sock))
            .await
            .expect("listener must close socket on frame overflow");
        assert!(matches!(end, FrontendMsg::Terminate | FrontendMsg::Eof));
    });

    let listener1 = PostgresListener::open(
        direct_config(port1),
        vec!["orders".to_string()],
        ListenerOptions {
            queue_capacity: 3,
            max_queue_bytes: DEFAULT_NOTIFICATION_MAX_BYTES,
            reconnect: true,
        },
    )
    .await;

    match listener1 {
        Err(err) => {
            assert!(
                matches!(&err, PostgresError::Operational(msg) if msg.contains("overflow")),
                "expected Operational overflow, got {err:?}"
            );
        }
        Ok(l) => {
            let err = tokio::time::timeout(Duration::from_secs(2), l.next())
                .await
                .expect("next resolved")
                .expect_err("must fail with overflow");
            assert!(
                matches!(&err, PostgresError::Operational(msg) if msg.contains("overflow")),
                "expected Operational overflow, got {err:?}"
            );
            assert!(l.is_closed());
        }
    }
    peer1.await.unwrap();
    assert_eq!(accepts1.load(Ordering::SeqCst), 1);

    let tcp2 = TcpListener::bind("127.0.0.1:0").await.expect("bind 2");
    let port2 = tcp2.local_addr().unwrap().port();
    let (push_bytes_tx, push_bytes_rx) = oneshot::channel::<()>();

    let peer2 = tokio::spawn(async move {
        let (mut sock, _) = tcp2.accept().await.expect("accept");
        complete_startup_ready(&mut sock, 6200).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"orders\"".to_string())
        );
        let mut ack = command_complete_frame("LISTEN");
        ack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&ack).await.unwrap();
        sock.flush().await.unwrap();

        push_bytes_rx.await.unwrap();
        let mut burst = notification_frame(6201, "orders", &[b'a'; 40]);
        burst.extend_from_slice(&notification_frame(6202, "orders", &[b'b'; 40]));
        sock.write_all(&burst).await.unwrap();
        sock.flush().await.unwrap();

        let end = tokio::time::timeout(Duration::from_secs(2), read_frontend_msg(&mut sock))
            .await
            .expect("listener must close socket on byte overflow");
        assert!(matches!(end, FrontendMsg::Terminate | FrontendMsg::Eof));
    });

    let listener2 = PostgresListener::open(
        direct_config(port2),
        vec!["orders".to_string()],
        ListenerOptions {
            queue_capacity: 1024,
            max_queue_bytes: 64,
            reconnect: false,
        },
    )
    .await
    .expect("open listener 2");

    push_bytes_tx.send(()).unwrap();
    peer2.await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while !listener2.is_closed() && Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
    assert!(listener2.is_closed());
    let byte_err = listener2
        .next()
        .await
        .expect_err("byte overflow must surface Operational error");
    assert!(
        matches!(&byte_err, PostgresError::Operational(msg) if msg.contains("overflow")),
        "expected Operational overflow, got {byte_err:?}"
    );
}

#[tokio::test]
async fn establish_standalone_connection_encodes_replication_database_and_skips_session_gucs() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();

    let peer = tokio::spawn(async move {
        let (mut repl_sock, _) = tcp.accept().await.expect("accept replication");
        let repl_params = complete_startup_ready(&mut repl_sock, 7001).await;
        let repl_end = read_frontend_msg(&mut repl_sock).await;
        assert!(
            matches!(repl_end, FrontendMsg::Terminate | FrontendMsg::Eof),
            "replication startup must skip post-auth SET GUCs; got {repl_end:?}"
        );

        let (mut std_sock, _) = tcp.accept().await.expect("accept standard");
        let std_params = complete_startup_ready(&mut std_sock, 7002).await;
        let std_end = read_frontend_msg(&mut std_sock).await;
        assert!(matches!(std_end, FrontendMsg::Terminate | FrontendMsg::Eof));

        (repl_params, std_params)
    });

    let mut raw = RawPostgresOptions::new("127.0.0.1", "repl_user", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.application_name = "vtb-cdc".to_string();
    raw.session_gucs
        .insert("TimeZone".to_string(), "UTC".to_string());
    raw.session_gucs
        .insert("statement_timeout".to_string(), "5000".to_string());
    let cfg = PostgresConfig::validate(raw).expect("valid config");

    let mut repl_conn = establish_standalone_connection(&cfg, true)
        .await
        .expect("establish replication=database connection");
    repl_conn.close().await.expect("close repl_conn");

    let mut std_conn = establish_standalone_connection(&cfg, false)
        .await
        .expect("establish standard standalone connection");
    std_conn.close().await.expect("close std_conn");

    let (repl_params, std_params) = peer.await.expect("peer");
    assert_eq!(
        repl_params.get("user").map(String::as_str),
        Some("repl_user")
    );
    assert_eq!(
        repl_params.get("database").map(String::as_str),
        Some("appdb")
    );
    assert_eq!(
        repl_params.get("client_encoding").map(String::as_str),
        Some("UTF8")
    );
    assert_eq!(
        repl_params.get("replication").map(String::as_str),
        Some("database")
    );
    assert_eq!(
        repl_params.get("application_name").map(String::as_str),
        Some("vtb-cdc")
    );
    assert!(!repl_params.contains_key("TimeZone"));
    assert!(!repl_params.contains_key("statement_timeout"));

    assert!(!std_params.contains_key("replication"));
    assert_eq!(std_params.get("TimeZone").map(String::as_str), Some("UTC"));
    assert_eq!(
        std_params.get("statement_timeout").map(String::as_str),
        Some("5000")
    );
}

#[tokio::test]
async fn reconnect_false_eof_and_non_recoverable_errors_terminate_without_retry() {
    let tcp_eof = TcpListener::bind("127.0.0.1:0").await.expect("bind eof");
    let port_eof = tcp_eof.local_addr().unwrap().port();
    let eof_accepts = Arc::new(AtomicUsize::new(0));
    let eof_accepts_peer = Arc::clone(&eof_accepts);
    let (drop_now_tx, drop_now_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let (mut sock, _) = tcp_eof.accept().await.expect("accept");
        eof_accepts_peer.fetch_add(1, Ordering::SeqCst);
        complete_startup_ready(&mut sock, 8001).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"orders\"".to_string())
        );
        let mut ack = command_complete_frame("LISTEN");
        ack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&ack).await.unwrap();
        sock.flush().await.unwrap();
        drop_now_rx.await.unwrap();
        drop(sock);
    });

    let listener_eof = PostgresListener::open(
        direct_config(port_eof),
        vec!["orders".to_string()],
        default_listener_options(),
    )
    .await
    .expect("open eof listener");
    drop_now_tx.send(()).unwrap();
    let eof_err = listener_eof
        .next()
        .await
        .expect_err("EOF with reconnect=false must surface Transport error");
    assert!(matches!(eof_err, PostgresError::Transport(_)));
    assert!(listener_eof.is_closed());
    assert_eq!(eof_accepts.load(Ordering::SeqCst), 1);

    let tcp_auth = TcpListener::bind("127.0.0.1:0").await.expect("bind auth");
    let port_auth = tcp_auth.local_addr().unwrap().port();
    let auth_accepts = Arc::new(AtomicUsize::new(0));
    let auth_accepts_peer = Arc::clone(&auth_accepts);
    let (drop_auth_tx, drop_auth_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let (mut sock1, _) = tcp_auth.accept().await.expect("accept 1");
        auth_accepts_peer.fetch_add(1, Ordering::SeqCst);
        complete_startup_ready(&mut sock1, 8010).await;
        drop_auth_rx.await.unwrap();
        drop(sock1);

        let (mut sock2, _) = tcp_auth.accept().await.expect("accept 2");
        auth_accepts_peer.fetch_add(1, Ordering::SeqCst);
        read_startup_params(&mut sock2).await;
        let err_frame = error_response_frame("FATAL", "28P01", "password authentication failed");
        sock2.write_all(&err_frame).await.unwrap();
        sock2.flush().await.unwrap();
        drop(sock2);

        while tcp_auth.accept().await.is_ok() {
            auth_accepts_peer.fetch_add(1, Ordering::SeqCst);
        }
    });

    let listener_auth = PostgresListener::open(
        direct_config(port_auth),
        Vec::new(),
        ListenerOptions {
            queue_capacity: 64,
            max_queue_bytes: 65_536,
            reconnect: true,
        },
    )
    .await
    .expect("open auth listener");
    drop_auth_tx.send(()).unwrap();
    let auth_err = listener_auth
        .next()
        .await
        .expect_err("28P01 during reconnect must terminate without retrying");
    match auth_err {
        PostgresError::Server(srv) => assert_eq!(srv.sqlstate, "28P01"),
        other => panic!("expected Server(28P01), got {other:?}"),
    }
    assert!(listener_auth.is_closed());
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(auth_accepts.load(Ordering::SeqCst), 2);

    let tcp_proto = TcpListener::bind("127.0.0.1:0").await.expect("bind proto");
    let port_proto = tcp_proto.local_addr().unwrap().port();
    let proto_accepts = Arc::new(AtomicUsize::new(0));
    let proto_accepts_peer = Arc::clone(&proto_accepts);
    let (send_bad_tx, send_bad_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let (mut sock, _) = tcp_proto.accept().await.expect("accept proto");
        proto_accepts_peer.fetch_add(1, Ordering::SeqCst);
        complete_startup_ready(&mut sock, 8020).await;
        send_bad_rx.await.unwrap();
        sock.write_all(&[b'A', 0, 0, 0, 2]).await.unwrap();
        sock.flush().await.unwrap();
        while tcp_proto.accept().await.is_ok() {
            proto_accepts_peer.fetch_add(1, Ordering::SeqCst);
        }
    });

    let listener_proto = PostgresListener::open(
        direct_config(port_proto),
        Vec::new(),
        ListenerOptions {
            queue_capacity: 64,
            max_queue_bytes: 65_536,
            reconnect: true,
        },
    )
    .await
    .expect("open proto listener");
    send_bad_tx.send(()).unwrap();
    let proto_err = listener_proto
        .next()
        .await
        .expect_err("protocol error must not reconnect");
    assert!(matches!(proto_err, PostgresError::Protocol(_)));
    assert!(listener_proto.is_closed());
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(proto_accepts.load(Ordering::SeqCst), 1);

    let tcp_sql = TcpListener::bind("127.0.0.1:0").await.expect("bind sql");
    let port_sql = tcp_sql.local_addr().unwrap().port();
    let sql_accepts = Arc::new(AtomicUsize::new(0));
    let sql_accepts_peer = Arc::clone(&sql_accepts);

    tokio::spawn(async move {
        let (mut sock, _) = tcp_sql.accept().await.expect("accept sql");
        sql_accepts_peer.fetch_add(1, Ordering::SeqCst);
        complete_startup_ready(&mut sock, 8030).await;
        assert_eq!(
            read_frontend_msg(&mut sock).await,
            FrontendMsg::Query("LISTEN \"restricted\"".to_string())
        );
        let mut resp = error_response_frame("ERROR", "42501", "permission denied for channel");
        resp.extend_from_slice(&ready_for_query_frame(b'I'));
        sock.write_all(&resp).await.unwrap();
        sock.flush().await.unwrap();
        let _ = read_frontend_msg(&mut sock).await;
        while tcp_sql.accept().await.is_ok() {
            sql_accepts_peer.fetch_add(1, Ordering::SeqCst);
        }
    });

    let listener_sql = PostgresListener::open(
        direct_config(port_sql),
        Vec::new(),
        ListenerOptions {
            queue_capacity: 64,
            max_queue_bytes: 65_536,
            reconnect: true,
        },
    )
    .await
    .expect("open sql listener");
    let sql_err = listener_sql
        .subscribe(vec!["restricted".to_string()])
        .await
        .expect_err("SQL ErrorResponse on LISTEN must fail without reconnect");
    match sql_err {
        PostgresError::Server(srv) => assert_eq!(srv.sqlstate, "42501"),
        other => panic!("expected Server(42501), got {other:?}"),
    }
    assert!(listener_sql.is_closed());
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(sql_accepts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reconnect_true_resubscribes_active_snapshot_marks_gap_once_and_handles_mid_subscribe_drop()
{
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();
    let dropped_first = Arc::new(Notify::new());
    let dropped_first_peer = Arc::clone(&dropped_first);
    let allow_second = Arc::new(Notify::new());
    let allow_second_peer = Arc::clone(&allow_second);

    let peer = tokio::spawn(async move {
        let (mut sock1, _) = tcp.accept().await.expect("accept 1");
        complete_startup_ready(&mut sock1, 9001).await;
        for expected in ["LISTEN \"orders\"", "LISTEN \"legacy\""] {
            assert_eq!(
                read_frontend_msg(&mut sock1).await,
                FrontendMsg::Query(expected.to_string())
            );
            let mut ack = command_complete_frame("LISTEN");
            ack.extend_from_slice(&ready_for_query_frame(b'I'));
            sock1.write_all(&ack).await.unwrap();
            sock1.flush().await.unwrap();
        }
        assert_eq!(
            read_frontend_msg(&mut sock1).await,
            FrontendMsg::Query("UNLISTEN \"legacy\"".to_string())
        );
        let mut unack = command_complete_frame("UNLISTEN");
        unack.extend_from_slice(&notification_frame(9001, "orders", b"before-drop"));
        unack.extend_from_slice(&ready_for_query_frame(b'I'));
        sock1.write_all(&unack).await.unwrap();
        sock1.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(sock1);
        dropped_first_peer.notify_one();

        allow_second_peer.notified().await;
        let (mut sock2, _) = tcp.accept().await.expect("accept 2");
        complete_startup_ready(&mut sock2, 9002).await;
        assert_eq!(
            read_frontend_msg(&mut sock2).await,
            FrontendMsg::Query("LISTEN \"orders\"".to_string())
        );
        let mut ack2 = command_complete_frame("LISTEN");
        ack2.extend_from_slice(&ready_for_query_frame(b'I'));
        ack2.extend_from_slice(&notification_frame(
            9002,
            "orders",
            b"after-idle-reconnect-1",
        ));
        ack2.extend_from_slice(&notification_frame(
            9002,
            "orders",
            b"after-idle-reconnect-2",
        ));
        sock2.write_all(&ack2).await.unwrap();
        sock2.flush().await.unwrap();

        assert_eq!(
            read_frontend_msg(&mut sock2).await,
            FrontendMsg::Query("LISTEN \"audit\"".to_string())
        );
        drop(sock2);

        let (mut sock3, _) = tcp.accept().await.expect("accept 3");
        complete_startup_ready(&mut sock3, 9003).await;
        for expected in ["LISTEN \"orders\"", "LISTEN \"audit\""] {
            assert_eq!(
                read_frontend_msg(&mut sock3).await,
                FrontendMsg::Query(expected.to_string())
            );
            let mut ack3 = command_complete_frame("LISTEN");
            ack3.extend_from_slice(&ready_for_query_frame(b'I'));
            sock3.write_all(&ack3).await.unwrap();
            sock3.flush().await.unwrap();
        }
        sock3
            .write_all(&notification_frame(9003, "audit", b"after-sub-reconnect"))
            .await
            .unwrap();
        sock3.flush().await.unwrap();

        let end = read_frontend_msg(&mut sock3).await;
        assert!(matches!(end, FrontendMsg::Terminate | FrontendMsg::Eof));
    });

    let listener = PostgresListener::open(
        direct_config(port),
        vec!["orders".to_string(), "legacy".to_string()],
        ListenerOptions {
            queue_capacity: 128,
            max_queue_bytes: 65_536,
            reconnect: true,
        },
    )
    .await
    .expect("open reconnect listener");

    listener
        .unsubscribe(vec!["legacy".to_string()])
        .await
        .expect("unsubscribe legacy");

    let pre = listener.next().await.unwrap().expect("pre-drop frame");
    assert_eq!(pre.payload, Bytes::from_static(b"before-drop"));
    assert!(!pre.reconnected_gap);

    dropped_first.notified().await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !listener.is_closed(),
        "is_closed() must remain false while in recoverable reconnect backoff"
    );
    allow_second.notify_one();

    let post1 = listener.next().await.unwrap().expect("post1");
    assert_eq!(post1.payload, Bytes::from_static(b"after-idle-reconnect-1"));
    assert!(post1.reconnected_gap);

    let post2 = listener.next().await.unwrap().expect("post2");
    assert_eq!(post2.payload, Bytes::from_static(b"after-idle-reconnect-2"));
    assert!(!post2.reconnected_gap);

    listener
        .subscribe(vec!["audit".to_string()])
        .await
        .expect("subscribe survives mid-LISTEN drop via reconnect");
    assert!(!listener.is_closed());

    let post3 = listener.next().await.unwrap().expect("post3");
    assert_eq!(post3.channel, "audit");
    assert_eq!(post3.payload, Bytes::from_static(b"after-sub-reconnect"));
    assert!(post3.reconnected_gap);

    listener.close().await.expect("close");
    assert!(listener.is_closed());
    peer.await.expect("peer finished cleanly");
}

#[tokio::test]
async fn close_aborts_backoff_within_100ms_wakes_waiter_and_drop_closes_cleanly() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();
    let dropped_notify = Arc::new(Notify::new());
    let dropped_notify_peer = Arc::clone(&dropped_notify);

    let (sock1_task_tx, sock1_task_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let (mut sock1, _) = tcp.accept().await.expect("accept 1");
        complete_startup_ready(&mut sock1, 9501).await;
        let _ = sock1_task_rx.await;
        drop(sock1);
        drop(tcp);
        dropped_notify_peer.notify_one();
    });

    let listener = Arc::new(
        PostgresListener::open(
            direct_config(port),
            Vec::new(),
            ListenerOptions {
                queue_capacity: 64,
                max_queue_bytes: 65_536,
                reconnect: true,
            },
        )
        .await
        .expect("open"),
    );

    let _ = sock1_task_tx.send(());
    dropped_notify.notified().await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!listener.is_closed());

    let waiter_listener = Arc::clone(&listener);
    let waiter = tokio::spawn(async move { waiter_listener.next().await });
    tokio::task::yield_now().await;

    let started = Instant::now();
    listener.close().await.expect("close during backoff");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "close() must abort backoff within 100ms, took {elapsed:?}"
    );
    assert!(listener.is_closed());
    assert_eq!(waiter.await.unwrap().expect("waiter woken cleanly"), None);

    listener
        .close()
        .await
        .expect("second close() must be idempotent");
    assert!(listener.is_closed());

    let tcp_drop = TcpListener::bind("127.0.0.1:0").await.expect("bind drop");
    let port_drop = tcp_drop.local_addr().unwrap().port();
    let (closed_tx, closed_rx) = oneshot::channel::<FrontendMsg>();
    let (queued_tx, queued_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let (mut sock, _) = tcp_drop.accept().await.expect("accept drop listener");
        complete_startup_ready(&mut sock, 9600).await;
        sock.write_all(&notification_frame(9600, "q", b"queued-wire"))
            .await
            .unwrap();
        sock.flush().await.unwrap();
        let _ = queued_tx.send(());
        let end = read_frontend_msg(&mut sock).await;
        let _ = closed_tx.send(end);
    });

    let tcp_clear = TcpListener::bind("127.0.0.1:0").await.expect("bind clear");
    let port_clear = tcp_clear.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut sock, _) = tcp_clear.accept().await.expect("accept clear listener");
        complete_startup_ready(&mut sock, 9599).await;
        sock.write_all(&notification_frame(9599, "q", b"queued-in-vecdeque"))
            .await
            .unwrap();
        sock.flush().await.unwrap();
        let _ = read_frontend_msg(&mut sock).await;
    });
    let clear_listener = PostgresListener::open(
        direct_config(port_clear),
        Vec::new(),
        default_listener_options(),
    )
    .await
    .expect("open clear listener");
    tokio::time::sleep(Duration::from_millis(20)).await;
    clear_listener.push_front_unconsumed(NotificationFrame {
        pid: 1,
        channel: "stale".to_string(),
        payload: Bytes::from_static(b"cleared-on-close"),
        reconnected_gap: true,
    });
    clear_listener.close().await.expect("close clears buffers");
    assert_eq!(clear_listener.next().await.expect("post-close next"), None);

    let drop_listener = PostgresListener::open(
        direct_config(port_drop),
        Vec::new(),
        default_listener_options(),
    )
    .await
    .expect("open drop listener");
    queued_rx.await.unwrap();
    drop(drop_listener);

    let end = tokio::time::timeout(Duration::from_secs(1), closed_rx)
        .await
        .expect("Drop must close backend socket promptly")
        .expect("peer msg");
    assert!(matches!(end, FrontendMsg::Terminate | FrontendMsg::Eof));
}

#[tokio::test(start_paused = true)]
async fn reconnect_exhausts_exact_max_8_attempts_and_transitions_closed_true() {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = tcp.local_addr().unwrap().port();
    let reconnect_dials = Arc::new(AtomicUsize::new(0));
    let reconnect_dials_peer = Arc::clone(&reconnect_dials);

    tokio::spawn(async move {
        let (mut sock0, _) = tcp.accept().await.expect("initial accept");
        complete_startup_ready(&mut sock0, 9900).await;
        drop(sock0);

        while let Ok((mut retry_sock, _)) = tcp.accept().await {
            reconnect_dials_peer.fetch_add(1, Ordering::SeqCst);
            let _ = read_startup_params(&mut retry_sock).await;
            drop(retry_sock);
        }
    });

    let listener = PostgresListener::open(
        direct_config(port),
        Vec::new(),
        ListenerOptions {
            queue_capacity: 64,
            max_queue_bytes: 65_536,
            reconnect: true,
        },
    )
    .await
    .expect("open initial");

    let err = listener
        .next()
        .await
        .expect_err("all 8 reconnect attempts must fail closed");
    assert!(matches!(err, PostgresError::Transport(_)));
    assert!(listener.is_closed());
    assert_eq!(
        reconnect_dials.load(Ordering::SeqCst),
        MAX_RECONNECT_ATTEMPTS as usize
    );
}
