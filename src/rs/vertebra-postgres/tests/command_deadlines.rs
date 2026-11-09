//! Integration tests for non-renewing Rust command I/O deadlines, `OperationalError` precedence
//! over cleanup `57014`, post-`ReadyForQuery` enrichment skip/`AfterCompletion`, and bounded
//! `connect_timeout` cancellation retirement (`ch17:t01`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;
use vertebra_postgres::codecs::INT4_OID;
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::extended::Parameter;
use vertebra_postgres::pool::{PostgresPool, PostgresPoolOptions};
use vertebra_postgres::result::{PgValue, VectorKind};
use vertebra_postgres::session::cancel::CANCEL_REQUEST_CODE;
use vertebra_postgres::session::{CommandBudget, SessionHandle};

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

fn row_desc_single(name: &str, oid: u32) -> Vec<u8> {
    let mut col = BytesMut::new();
    col.put_i16(1);
    col.put_slice(name.as_bytes());
    col.put_u8(0);
    col.put_u32(0);
    col.put_i16(0);
    col.put_u32(oid);
    col.put_i16(-1);
    col.put_i32(-1);
    col.put_i16(0);
    frame(b'T', &col)
}

fn data_row_single_text(text: &str) -> Vec<u8> {
    let mut row = BytesMut::new();
    row.put_i16(1);
    row.put_i32(text.len() as i32);
    row.put_slice(text.as_bytes());
    frame(b'D', &row)
}

fn cold_extended_single_text_response(name: &str, oid: u32, text: &str, tx: u8) -> Vec<u8> {
    let desc = row_desc_single(name, oid);
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'1', b""));
    out.extend_from_slice(&frame(b't', &0_i16.to_be_bytes()));
    out.extend_from_slice(&desc);
    out.extend_from_slice(&frame(b'2', b""));
    out.extend_from_slice(&desc);
    out.extend_from_slice(&data_row_single_text(text));
    out.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
    out.extend_from_slice(&frame(b'3', b""));
    out.extend_from_slice(&frame(b'Z', &[tx]));
    out
}

fn expected_cancel_packet(pid: i32, secret: i32) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(16);
    pkt.extend_from_slice(&16_i32.to_be_bytes());
    pkt.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    pkt.extend_from_slice(&pid.to_be_bytes());
    pkt.extend_from_slice(&secret.to_be_bytes());
    pkt
}

async fn read_startup_or_cancel(sock: &mut TcpStream) -> Vec<u8> {
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

#[tokio::test]
async fn silent_peer_and_ignored_cancel_bounds_cleanup_and_replaces_pool_pid() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel_count = Arc::new(AtomicUsize::new(0));
    let query_count_conn1 = Arc::new(AtomicUsize::new(0));
    let cancel_srv = Arc::clone(&cancel_count);
    let qcount_srv = Arc::clone(&query_count_conn1);

    let srv = tokio::spawn(async move {
        let (mut primary1, _) = listener.accept().await.unwrap();
        let startup1 = read_startup_or_cancel(&mut primary1).await;
        assert!(startup1.len() > 16);
        primary1
            .write_all(&auth_ok_and_ready(5101, 0x1020_3040))
            .await
            .unwrap();

        let grp1 = read_command_group(&mut primary1).await.unwrap();
        assert_eq!(grp1.last().unwrap().0, b'S');
        qcount_srv.fetch_add(1, Ordering::SeqCst);

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let cancel_pkt = read_startup_or_cancel(&mut cancel_sock).await;
        assert_eq!(cancel_pkt, expected_cancel_packet(5101, 0x1020_3040));
        cancel_srv.fetch_add(1, Ordering::SeqCst);

        let primary1_hold = tokio::spawn(async move {
            let mut buf = [0u8; 64];
            while let Ok(n) = primary1.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                assert_eq!(
                    buf[0], b'X',
                    "retired socket must never replay SQL queries on silent peer"
                );
            }
        });

        let (mut primary2, _) = listener.accept().await.unwrap();
        let startup2 = read_startup_or_cancel(&mut primary2).await;
        assert!(startup2.len() > 16);
        primary2
            .write_all(&auth_ok_and_ready(5102, 0x5060_7080))
            .await
            .unwrap();

        let grp2 = read_command_group(&mut primary2).await.unwrap();
        assert_eq!(grp2.last().unwrap().0, b'S');
        primary2
            .write_all(&cold_extended_single_text_response(
                "pid", INT4_OID, "5102", b'I',
            ))
            .await
            .unwrap();

        loop {
            let next = read_command_group(&mut primary2).await.unwrap();
            if next[0].0 == b'X' {
                break;
            }
            assert_eq!(next[0].0, b'Q');
            let mut ok = frame(b'C', b"DISCARD ALL\0");
            ok.extend_from_slice(&frame(b'Z', b"I"));
            primary2.write_all(&ok).await.unwrap();
        }
        primary1_hold.await.unwrap();
    });

    let cfg = PostgresConfig::validate(tcp_raw(port, Duration::from_millis(80))).unwrap();
    let pool_opts = PostgresPoolOptions {
        min_size: 0,
        max_size: 1,
        acquire_timeout: Duration::from_secs(2),
        reset_timeout: Duration::from_millis(200),
        close_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let pool = PostgresPool::new(cfg, pool_opts).unwrap();

    let lease1 = pool.acquire().await.unwrap();
    let s1 = SessionHandle::from_pooled(lease1);

    let dur = CommandBudget::validate_optional_seconds(Some(0.04)).unwrap();
    let budget = CommandBudget::start_from_duration(dur).unwrap();
    let started = Instant::now();
    let err = s1
        .query_with_deadline("SELECT pg_sleep(10)", &[], None, budget)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(
        matches!(&err, PostgresError::Operational(_)),
        "expected OperationalError, got {err:?}"
    );
    assert_eq!(err.sqlstate(), None);
    assert!(
        elapsed < Duration::from_millis(450),
        "silent cancel cleanup exceeded 40ms+80ms bound: {elapsed:?}"
    );
    assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
    assert_eq!(query_count_conn1.load(Ordering::SeqCst), 1);
    s1.close_shared().await.unwrap();

    let lease2 = pool.acquire().await.unwrap();
    let s2 = SessionHandle::from_pooled(lease2);
    let res = s2
        .query("SELECT pg_backend_pid()", &[], None)
        .await
        .unwrap();
    assert_eq!(res.rows[0][0], PgValue::Int4(5102));
    s2.close_shared().await.unwrap();
    pool.close(Duration::from_secs(2)).await.unwrap();
    srv.await.unwrap();
}

#[tokio::test]
async fn dribbled_multi_statement_frames_never_renew_command_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel_count = Arc::new(AtomicUsize::new(0));
    let simple_count = Arc::new(AtomicUsize::new(0));
    let cancel_srv = Arc::clone(&cancel_count);
    let scount_srv = Arc::clone(&simple_count);

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup_or_cancel(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(6001, 0x2222_3333))
            .await
            .unwrap();

        let grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(grp[0].0, b'Q');
        scount_srv.fetch_add(1, Ordering::SeqCst);

        let dribble = tokio::spawn(async move {
            let _ = primary.write_all(&row_desc_single("n", INT4_OID)).await;
            let _ = primary.flush().await;
            for step in 0..5 {
                tokio::time::sleep(Duration::from_millis(25)).await;
                if step == 2 {
                    if primary
                        .write_all(&frame(b'C', b"SELECT 2\0"))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if primary
                        .write_all(&row_desc_single("m", INT4_OID))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if primary
                    .write_all(&data_row_single_text(&step.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }
                let _ = primary.flush().await;
            }
        });

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let pkt = read_startup_or_cancel(&mut cancel_sock).await;
        assert_eq!(pkt, expected_cancel_packet(6001, 0x2222_3333));
        cancel_srv.fetch_add(1, Ordering::SeqCst);
        let _ = dribble.await;
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(80))).unwrap();
    session.connect().await.unwrap();

    let budget = CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(60)));
    let started = Instant::now();
    let err = session
        .simple_query_with_deadline("SELECT 1; SELECT 2", budget)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(matches!(&err, PostgresError::Operational(_)), "got {err:?}");
    assert_eq!(err.sqlstate(), None);
    assert!(
        elapsed < Duration::from_millis(400),
        "dribbled simple_query renewed budget unexpectedly: {elapsed:?}"
    );
    assert_eq!(cancel_count.load(Ordering::SeqCst), 1);
    assert_eq!(simple_count.load(Ordering::SeqCst), 1);
    srv.await.unwrap();
}

#[tokio::test]
async fn cleanup_57014_keeps_operational_precedence_while_early_server_57014_stays_server_error() {
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut primary, _) = listener.accept().await.unwrap();
            let _ = read_startup_or_cancel(&mut primary).await;
            primary
                .write_all(&auth_ok_and_ready(7001, 0x0102_0304))
                .await
                .unwrap();
            let _ = read_command_group(&mut primary).await.unwrap();

            let (mut cancel_sock, _) = listener.accept().await.unwrap();
            let pkt = read_startup_or_cancel(&mut cancel_sock).await;
            assert_eq!(pkt, expected_cancel_packet(7001, 0x0102_0304));

            let mut cleanup_reply =
                error_response("57014", "canceling statement due to user request");
            cleanup_reply.extend_from_slice(&frame(b'Z', b"I"));
            let _ = primary.write_all(&cleanup_reply).await;
        });

        let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(200))).unwrap();
        session.connect().await.unwrap();
        let budget = CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(40)));
        let err = session
            .query_with_deadline("SELECT pg_sleep(5)", &[], None, budget)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PostgresError::Operational(_)),
            "client deadline must override cleanup 57014, got {err:?}"
        );
        assert_eq!(err.sqlstate(), None);
        assert!(session.is_closed());
        srv.await.unwrap();
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let accepts_srv = Arc::clone(&accepts);
        let srv = tokio::spawn(async move {
            let (mut primary, _) = listener.accept().await.unwrap();
            accepts_srv.fetch_add(1, Ordering::SeqCst);
            let _ = read_startup_or_cancel(&mut primary).await;
            primary
                .write_all(&auth_ok_and_ready(7002, 0x0506_0708))
                .await
                .unwrap();
            let _ = read_command_group(&mut primary).await.unwrap();

            tokio::time::sleep(Duration::from_millis(10)).await;
            let mut reply = error_response("57014", "canceling statement due to statement timeout");
            reply.extend_from_slice(&frame(b'Z', b"I"));
            primary.write_all(&reply).await.unwrap();

            let term = read_command_group(&mut primary).await.unwrap();
            assert_eq!(term[0].0, b'X');
        });

        let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(500))).unwrap();
        session.connect().await.unwrap();
        let budget =
            CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(500)));
        let err = session
            .query_with_deadline("SELECT pg_sleep(5)", &[], None, budget)
            .await
            .unwrap_err();
        match &err {
            PostgresError::Server(srv_err) => assert_eq!(srv_err.sqlstate, "57014"),
            other => panic!("expected Server(57014), got {other:?}"),
        }
        assert_eq!(err.sqlstate(), Some("57014"));
        assert_eq!(accepts.load(Ordering::SeqCst), 1);
        session.close().await.unwrap();
        srv.await.unwrap();
    }
}

#[tokio::test]
async fn pre_parse_discovery_timeout_emits_zero_parse_and_no_after_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let saw_parse = Arc::new(AtomicUsize::new(0));
    let saw_parse_srv = Arc::clone(&saw_parse);

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup_or_cancel(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(8001, 0x1122_3344))
            .await
            .unwrap();

        let grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(grp.len(), 1);
        assert_eq!(
            grp[0].0, b'Q',
            "expected pre-Parse TypeCatalog::discover simple_query"
        );
        if grp.iter().any(|(t, _)| *t == b'P') {
            saw_parse_srv.fetch_add(1, Ordering::SeqCst);
        }

        let (mut cancel_sock, _) = listener.accept().await.unwrap();
        let pkt = read_startup_or_cancel(&mut cancel_sock).await;
        assert_eq!(pkt, expected_cancel_packet(8001, 0x1122_3344));

        let mut reply = error_response("57014", "canceling catalog lookup");
        reply.extend_from_slice(&frame(b'Z', b"I"));
        let _ = primary.write_all(&reply).await;
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(200))).unwrap();
    session.connect().await.unwrap();
    let vec_param = Parameter::new(PgValue::Vector {
        values: vec![1.0, 2.0],
        kind: VectorKind::Vector,
    });
    let budget = CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(40)));
    let err = session
        .query_with_deadline("SELECT $1", &[vec_param], None, budget)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, PostgresError::Operational(_)),
        "expected plain Operational without AfterCompletion, got {err:?}"
    );
    assert_eq!(err.sqlstate(), None);
    assert_eq!(saw_parse.load(Ordering::SeqCst), 0);
    srv.await.unwrap();
}

#[tokio::test]
async fn post_ready_for_query_skips_exhausted_enrichment_and_preserves_after_completion_mid_enrichment()
 {
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (query_seen_tx, query_seen_rx) = oneshot::channel::<()>();
        let (allow_resp_tx, allow_resp_rx) = oneshot::channel::<()>();
        let (resp_written_tx, resp_written_rx) = oneshot::channel::<()>();
        let catalog_queries = Arc::new(AtomicUsize::new(0));
        let cat_srv = Arc::clone(&catalog_queries);

        let srv = tokio::spawn(async move {
            let (mut primary, _) = listener.accept().await.unwrap();
            let _ = read_startup_or_cancel(&mut primary).await;
            primary
                .write_all(&auth_ok_and_ready(8101, 0x4455_6677))
                .await
                .unwrap();

            let grp = read_command_group(&mut primary).await.unwrap();
            assert_eq!(grp.last().unwrap().0, b'S');
            let _ = query_seen_tx.send(());
            allow_resp_rx.await.unwrap();

            primary
                .write_all(&cold_extended_single_text_response(
                    "custom_col",
                    91_001,
                    "raw_custom_payload",
                    b'I',
                ))
                .await
                .unwrap();
            primary.flush().await.unwrap();
            let _ = resp_written_tx.send(());

            let next = read_command_group(&mut primary).await.unwrap();
            if next[0].0 == b'Q' {
                cat_srv.fetch_add(1, Ordering::SeqCst);
            } else {
                assert_eq!(next[0].0, b'X');
            }
        });

        let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(300))).unwrap();
        session.connect().await.unwrap();
        let budget = CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(40)));
        let q_fut = session.query_with_deadline("SELECT 'raw_custom_payload'", &[], None, budget);
        tokio::pin!(q_fut);

        tokio::select! {
            res = &mut q_fut => panic!("unexpected early finish before response written: {res:?}"),
            res = query_seen_rx => res.unwrap(),
        }
        allow_resp_tx.send(()).unwrap();
        resp_written_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(60)).await;

        let res = q_fut.await.unwrap();
        assert_eq!(res.columns.len(), 1);
        assert_eq!(res.columns[0].type_oid, 91_001);
        assert_eq!(
            res.rows,
            vec![vec![PgValue::Text("raw_custom_payload".to_string())]]
        );
        assert_eq!(catalog_queries.load(Ordering::SeqCst), 0);
        session.close_shared().await.unwrap();
        srv.await.unwrap();
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let srv = tokio::spawn(async move {
            let (mut primary, _) = listener.accept().await.unwrap();
            let _ = read_startup_or_cancel(&mut primary).await;
            primary
                .write_all(&auth_ok_and_ready(8102, 0x7788_99AA))
                .await
                .unwrap();

            let grp = read_command_group(&mut primary).await.unwrap();
            assert_eq!(grp.last().unwrap().0, b'S');
            primary
                .write_all(&cold_extended_single_text_response(
                    "custom_col",
                    91_002,
                    "committed_val",
                    b'I',
                ))
                .await
                .unwrap();

            let cat_grp = read_command_group(&mut primary).await.unwrap();
            assert_eq!(cat_grp[0].0, b'Q');

            let (mut cancel_sock, _) = listener.accept().await.unwrap();
            let pkt = read_startup_or_cancel(&mut cancel_sock).await;
            assert_eq!(pkt, expected_cancel_packet(8102, 0x7788_99AA));

            let mut reply = error_response("57014", "canceling enrichment catalog query");
            reply.extend_from_slice(&frame(b'Z', b"I"));
            let _ = primary.write_all(&reply).await;
        });

        let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(200))).unwrap();
        session.connect().await.unwrap();
        let budget = CommandBudget::from_deadline(Some(Instant::now() + Duration::from_millis(60)));
        let err = session
            .query_with_deadline(
                "INSERT INTO items VALUES (1) RETURNING c",
                &[],
                None,
                budget,
            )
            .await
            .unwrap_err();
        match err {
            PostgresError::AfterCompletion {
                source,
                completed_result,
            } => {
                assert!(
                    matches!(*source, PostgresError::Operational(_)),
                    "expected AfterCompletion(Operational), got {source:?}"
                );
                assert_eq!(source.sqlstate(), None);
                assert_eq!(completed_result.columns[0].type_oid, 91_002);
                assert_eq!(
                    completed_result.rows,
                    vec![vec![PgValue::Text("committed_val".to_string())]]
                );
            }
            other => panic!("expected AfterCompletion, got {other:?}"),
        }
        assert!(session.is_closed());
        srv.await.unwrap();
    }
}

#[tokio::test]
async fn untimed_query_waits_behind_barrier_and_invalid_budgets_fail_with_interface() {
    for bad in [
        0.0,
        -0.0,
        -1.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        1e300,
    ] {
        let err = CommandBudget::validate_optional_seconds(Some(bad)).unwrap_err();
        assert!(
            matches!(err, PostgresError::Interface(_)),
            "expected Interface for {bad}, got {err:?}"
        );
    }
    assert_eq!(
        CommandBudget::validate_optional_seconds(None).unwrap(),
        None
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let barrier = Arc::new(Notify::new());
    let barrier_srv = Arc::clone(&barrier);
    let (seen_tx, seen_rx) = oneshot::channel::<()>();

    let srv = tokio::spawn(async move {
        let (mut primary, _) = listener.accept().await.unwrap();
        let _ = read_startup_or_cancel(&mut primary).await;
        primary
            .write_all(&auth_ok_and_ready(9001, 0x1ABB_CCDD))
            .await
            .unwrap();

        let grp = read_command_group(&mut primary).await.unwrap();
        assert_eq!(grp.last().unwrap().0, b'S');
        let _ = seen_tx.send(());
        barrier_srv.notified().await;
        primary
            .write_all(&cold_extended_single_text_response(
                "v", INT4_OID, "77", b'I',
            ))
            .await
            .unwrap();

        let term = read_command_group(&mut primary).await.unwrap();
        assert_eq!(term[0].0, b'X');
    });

    let mut session = SessionHandle::new(tcp_raw(port, Duration::from_millis(80))).unwrap();
    session.connect().await.unwrap();
    let session = Arc::new(session);
    let session_q = Arc::clone(&session);

    let q_task = tokio::spawn(async move {
        session_q
            .query_with_deadline("SELECT 77 AS v", &[], None, CommandBudget::untimed())
            .await
    });

    seen_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        !q_task.is_finished(),
        "untimed query must stay pending past connect_timeout"
    );

    barrier.notify_one();
    let res = q_task.await.unwrap().unwrap();
    assert_eq!(res.rows[0][0], PgValue::Int4(77));
    session.close_shared().await.unwrap();
    srv.await.unwrap();
}
