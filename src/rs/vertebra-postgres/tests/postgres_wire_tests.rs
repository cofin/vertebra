mod support;

use std::collections::HashMap;
use std::time::Duration;

use _postgres::config::{
    ChannelBindingPolicy, PostgresConfig, RawPostgresOptions, parse_strict_sslmode,
};
use _postgres::errors::{PostgresError, ServerError};
use _postgres::protocol::TERMINATE_FRAME;
use _postgres::session::{BackendKey, SessionHandle, TransactionStatus};
use tokio::io::AsyncReadExt;
use vertebra_db::connection::TlsMode;

use support::{
    frame_auth_ok, frame_backend_key_data, frame_error_response, frame_notice_response,
    frame_parameter_status, frame_ready_for_query, read_startup_packet, read_tagged_frontend_frame,
    spawn_scripted_server, write_frames,
};

fn base_raw_options() -> RawPostgresOptions {
    RawPostgresOptions {
        user: "app".to_string(),
        database: "prod".to_string(),
        ..RawPostgresOptions::default()
    }
}

#[test]
fn validate_accepts_contract_defaults() {
    let cfg = PostgresConfig::validate(base_raw_options()).expect("valid default config");
    assert_eq!(cfg.user, "app");
    assert_eq!(cfg.database, "prod");
    assert_eq!(cfg.host, "localhost");
    assert_eq!(cfg.port, 5432);
    assert_eq!(cfg.password, None);
    assert_eq!(cfg.unix_socket, None);
    assert_eq!(cfg.sslmode, TlsMode::VerifyFull);
    assert_eq!(cfg.ca_pem, None);
    assert_eq!(cfg.client_cert_pem, None);
    assert_eq!(cfg.client_key_pem, None);
    assert_eq!(cfg.server_name, None);
    assert_eq!(cfg.channel_binding, ChannelBindingPolicy::Prefer);
    assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
    assert_eq!(cfg.application_name, "vertebra");
    assert!(cfg.session_gucs.is_empty());
    assert!(cfg.keepalives);
    assert_eq!(cfg.keepalives_idle, None);
    assert_eq!(cfg.keepalives_interval, None);
    assert_eq!(cfg.keepalives_count, None);
    assert_eq!(cfg.cloud_sql_instance, None);
    assert_eq!(cfg.cloud_sql_ip_type, "PRIVATE");
    assert_eq!(cfg.alloydb_instance_uri, None);
    assert_eq!(cfg.alloydb_ip_type, "PRIVATE");
    assert!(!cfg.enable_iam_auth);
    assert!(cfg.cloud_auth_options.is_empty());
    assert!(cfg.iam_auth_options.is_empty());
}

#[test]
fn validate_rejects_empty_and_nul_identifiers() {
    for (mutate, label) in [
        (
            (|r: &mut RawPostgresOptions| r.user = String::new()) as fn(&mut RawPostgresOptions),
            "empty user",
        ),
        (
            |r: &mut RawPostgresOptions| r.user = "app\0admin".to_string(),
            "nul user",
        ),
        (
            |r: &mut RawPostgresOptions| r.database = String::new(),
            "empty database",
        ),
        (
            |r: &mut RawPostgresOptions| r.database = "db\0x".to_string(),
            "nul database",
        ),
        (
            |r: &mut RawPostgresOptions| r.host = String::new(),
            "empty host",
        ),
        (
            |r: &mut RawPostgresOptions| r.host = "db\0.internal".to_string(),
            "nul host",
        ),
        (
            |r: &mut RawPostgresOptions| r.server_name = Some(String::new()),
            "empty server_name",
        ),
        (
            |r: &mut RawPostgresOptions| r.server_name = Some("sni\0".to_string()),
            "nul server_name",
        ),
        (
            |r: &mut RawPostgresOptions| r.application_name = String::new(),
            "empty application_name",
        ),
        (
            |r: &mut RawPostgresOptions| r.application_name = "vtb\0".to_string(),
            "nul application_name",
        ),
    ] {
        let mut raw = base_raw_options();
        mutate(&mut raw);
        let err = PostgresConfig::validate(raw).expect_err(label);
        assert!(matches!(err, PostgresError::Config(_)), "{label}: {err:?}");
    }
}

#[test]
fn validate_rejects_uri_or_path_in_tcp_host() {
    for invalid_host in ["postgresql://localhost/db", "/var/run/postgresql", "a/b"] {
        let mut raw = base_raw_options();
        raw.host = invalid_host.to_string();
        let err = PostgresConfig::validate(raw).expect_err("URI or slash host must fail");
        assert!(matches!(err, PostgresError::Config(_)));
    }
}

#[test]
fn validate_rejects_invalid_ports_and_timeouts() {
    for invalid_port in [-1, 0, 65536, i64::MIN, i64::MAX] {
        let mut raw = base_raw_options();
        raw.port = invalid_port;
        let err = PostgresConfig::validate(raw).expect_err("out-of-range port must fail");
        assert!(matches!(err, PostgresError::Config(_)));
    }

    for invalid_timeout in [0.0, -5.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut raw = base_raw_options();
        raw.connect_timeout = invalid_timeout;
        let err = PostgresConfig::validate(raw).expect_err("invalid timeout must fail");
        assert!(matches!(err, PostgresError::Config(_)));
    }
}

#[test]
fn validate_enforces_mtls_pem_pairs_and_non_empty_buffers() {
    let mut cert_only = base_raw_options();
    cert_only.client_cert_pem = Some(b"CERT".to_vec());
    let err = PostgresConfig::validate(cert_only).expect_err("cert without key must fail");
    assert!(matches!(err, PostgresError::Config(_)));

    let mut key_only = base_raw_options();
    key_only.client_key_pem = Some(b"PRIVATE-KEY-SECRET".to_vec());
    let err = PostgresConfig::validate(key_only).expect_err("key without cert must fail");
    assert!(!err.to_string().contains("PRIVATE-KEY-SECRET"));

    let mut empty_ca = base_raw_options();
    empty_ca.ca_pem = Some(Vec::new());
    assert!(matches!(
        PostgresConfig::validate(empty_ca),
        Err(PostgresError::Config(_))
    ));
}

#[test]
fn strict_sslmode_and_channel_binding_spellings() {
    assert_eq!(parse_strict_sslmode("disable").unwrap(), TlsMode::Disable);
    assert_eq!(parse_strict_sslmode("prefer").unwrap(), TlsMode::Prefer);
    assert_eq!(parse_strict_sslmode("require").unwrap(), TlsMode::Require);
    assert_eq!(
        parse_strict_sslmode("verify-ca").unwrap(),
        TlsMode::VerifyCa
    );
    assert_eq!(
        parse_strict_sslmode("verify-full").unwrap(),
        TlsMode::VerifyFull
    );

    for invalid_ssl in [
        "allow",
        "true",
        "false",
        "verify_full",
        "verify_ca",
        "PREFER",
        " disable ",
    ] {
        assert!(matches!(
            parse_strict_sslmode(invalid_ssl),
            Err(PostgresError::Config(_))
        ));
    }

    assert_eq!(
        ChannelBindingPolicy::parse("disable").unwrap(),
        ChannelBindingPolicy::Disable
    );
    assert_eq!(
        ChannelBindingPolicy::parse("prefer").unwrap(),
        ChannelBindingPolicy::Prefer
    );
    assert_eq!(
        ChannelBindingPolicy::parse("require").unwrap(),
        ChannelBindingPolicy::Require
    );
    for invalid_cb in ["PREFER", "true", "none", ""] {
        assert!(matches!(
            ChannelBindingPolicy::parse(invalid_cb),
            Err(PostgresError::Config(_))
        ));
    }
}

#[test]
fn debug_and_errors_redact_secrets_and_retain_server_diagnostics() {
    let mut bad_pw = base_raw_options();
    bad_pw.password = Some("s3cr3t-pw\0".to_string());
    let pw_err = PostgresConfig::validate(bad_pw).expect_err("nul password must fail");
    assert!(!pw_err.to_string().contains("s3cr3t-pw"));
    assert!(!format!("{pw_err:?}").contains("s3cr3t-pw"));

    let mut raw = base_raw_options();
    raw.password = Some("super-secret-password".to_string());
    raw.client_cert_pem = Some(b"CERT-DATA".to_vec());
    raw.client_key_pem = Some(b"SUPER-SECRET-PRIVATE-KEY".to_vec());
    raw.cloud_auth_options = HashMap::from([(
        "access_token".to_string(),
        "cloud-admin-secret-token".to_string(),
    )]);
    raw.iam_auth_options = HashMap::from([(
        "access_token".to_string(),
        "iam-login-secret-token".to_string(),
    )]);
    let raw_dbg = format!("{raw:?}");
    assert!(!raw_dbg.contains("super-secret-password"));
    assert!(!raw_dbg.contains("SUPER-SECRET-PRIVATE-KEY"));
    assert!(!raw_dbg.contains("cloud-admin-secret-token"));
    assert!(!raw_dbg.contains("iam-login-secret-token"));

    let cfg = PostgresConfig::validate(raw).expect("valid config with secrets");
    let cfg_dbg = format!("{cfg:?}");
    assert!(!cfg_dbg.contains("super-secret-password"));
    assert!(!cfg_dbg.contains("SUPER-SECRET-PRIVATE-KEY"));
    assert!(!cfg_dbg.contains("cloud-admin-secret-token"));
    assert!(!cfg_dbg.contains("iam-login-secret-token"));
    assert!(cfg_dbg.contains("<redacted>"));

    let srv_err = PostgresError::server(
        "FATAL",
        "28P01",
        "password authentication failed",
        vec![(b'D', "secret detail".to_string())],
    )
    .expect("valid 5-char SQLSTATE");
    assert_eq!(srv_err.sqlstate(), Some("28P01"));
    assert!(!srv_err.to_string().contains("secret detail"));
    match &srv_err {
        PostgresError::Server(ServerError {
            severity,
            sqlstate,
            message,
            diagnostics,
        }) => {
            assert_eq!(severity, "FATAL");
            assert_eq!(sqlstate, "28P01");
            assert_eq!(message, "password authentication failed");
            assert_eq!(diagnostics, &vec![(b'D', "secret detail".to_string())]);
        }
        other => panic!("expected ServerError, got {other:?}"),
    }

    assert!(matches!(
        PostgresError::server("ERROR", "28", "short sqlstate", Vec::new()),
        Err(PostgresError::Protocol(_))
    ));
}

#[tokio::test]
async fn startup_trust_publishes_ready_session_and_closes_once() {
    let (addr, accepts, server_task) = spawn_scripted_server(|mut stream| async move {
        let params = read_startup_packet(&mut stream)
            .await
            .expect("read startup");
        assert_eq!(params.get("user").map(String::as_str), Some("alice"));
        assert_eq!(params.get("database").map(String::as_str), Some("appdb"));
        assert_eq!(
            params.get("client_encoding").map(String::as_str),
            Some("UTF8")
        );
        assert_eq!(
            params.get("application_name").map(String::as_str),
            Some("vtb-test")
        );

        write_frames(
            &mut stream,
            &[
                frame_auth_ok(),
                frame_notice_response("welcome notice"),
                frame_parameter_status("server_version", "16.4"),
                frame_parameter_status("TimeZone", "UTC"),
                frame_backend_key_data(4242, 999_001),
                frame_ready_for_query(b'I'),
            ],
        )
        .await
        .expect("write startup sequence");

        let (tag, payload) = read_tagged_frontend_frame(&mut stream)
            .await
            .expect("read Terminate frame");
        assert_eq!(tag, b'X');
        assert!(payload.is_empty());

        let mut trailing = Vec::new();
        let n = stream
            .read_to_end(&mut trailing)
            .await
            .expect("read EOF after Terminate");
        assert_eq!(n, 0);
    })
    .await;

    let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
    raw.port = i64::from(addr.port());
    raw.sslmode = "disable".to_string();
    raw.application_name = "vtb-test".to_string();

    let mut handle = SessionHandle::new(raw).expect("pure constructor");
    assert_eq!(accepts.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(!handle.is_ready());
    assert_eq!(handle.generation(), 0);

    let conn = handle.connect().await.expect("trust connect");
    assert_eq!(conn.generation(), 1);
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
    assert_eq!(
        conn.backend_key(),
        Some(BackendKey {
            process_id: 4242,
            secret_key: 999_001,
        })
    );
    assert_eq!(conn.parameter("server_version"), Some("16.4"));
    assert_eq!(conn.parameter("TimeZone"), Some("UTC"));
    assert!(!conn.is_tls());
    assert!(handle.is_ready());

    assert!(matches!(
        handle.connect().await,
        Err(PostgresError::Interface(_))
    ));

    handle.close().await.expect("first close");
    assert!(handle.is_closed());
    assert!(!handle.is_ready());
    handle.close().await.expect("idempotent second close");

    server_task.await.expect("server task clean exit");
    assert_eq!(TERMINATE_FRAME, [b'X', 0, 0, 0, 4]);
}

#[tokio::test]
async fn startup_rejects_server_error_and_protocol_order_violations() {
    let (addr1, _, t1) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(
            &mut stream,
            &[frame_error_response(
                "3D000",
                "database \"missing\" does not exist",
                Some("sensitive internal path"),
            )],
        )
        .await
        .unwrap();
    })
    .await;

    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "alice", "missing");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    let mut h1 = SessionHandle::new(raw1).unwrap();
    let err1 = h1.connect().await.expect_err("must fail with 3D000");
    assert_eq!(err1.sqlstate(), Some("3D000"));
    assert!(!err1.to_string().contains("sensitive internal path"));
    assert!(h1.is_closed());
    t1.await.unwrap();

    let (addr2, _, t2) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;

    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "alice", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    let mut h2 = SessionHandle::new(raw2).unwrap();
    let err2 = h2
        .connect()
        .await
        .expect_err("ReadyForQuery before AuthOk must fail");
    assert!(matches!(err2, PostgresError::Protocol(_)));
    assert!(h2.is_closed());
    t2.await.unwrap();

    for bad_status in *b"TE?" {
        let (addr3, _, t3) = spawn_scripted_server(move |mut stream| async move {
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(
                &mut stream,
                &[frame_auth_ok(), frame_ready_for_query(bad_status)],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw3 = RawPostgresOptions::new(addr3.ip().to_string(), "alice", "appdb");
        raw3.port = i64::from(addr3.port());
        raw3.sslmode = "disable".to_string();
        let mut h3 = SessionHandle::new(raw3).unwrap();
        let err3 = h3
            .connect()
            .await
            .expect_err("non-Idle ReadyForQuery must fail");
        assert!(matches!(err3, PostgresError::Protocol(_)));
        assert!(h3.is_closed());
        t3.await.unwrap();
    }
}

#[tokio::test]
async fn startup_cancellation_and_timeout_transition_handle_to_closed() {
    let (addr, _, server_task) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_ok()]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
    })
    .await;

    let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
    raw.port = i64::from(addr.port());
    raw.sslmode = "disable".to_string();
    raw.connect_timeout = 0.08;

    let mut handle = SessionHandle::new(raw).unwrap();
    let err = handle
        .connect()
        .await
        .expect_err("stalled server must time out");
    assert!(matches!(err, PostgresError::Transport(_)));
    assert!(handle.is_closed());
    assert!(!handle.is_ready());
    assert_eq!(handle.generation(), 1);
    server_task.await.unwrap();

    let (addr2, _, server_task2) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;

    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "alice", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    let mut handle2 = SessionHandle::new(raw2).unwrap();
    {
        let attempt = handle2.begin_connect().unwrap();
        let fut = attempt.run();
        tokio::pin!(fut);
        let _ = tokio::time::timeout(Duration::from_millis(25), &mut fut).await;
    }
    assert!(handle2.is_closed());
    assert!(!handle2.is_ready());
    assert_eq!(handle2.generation(), 1);
    server_task2.await.unwrap();
}

#[tokio::test]
async fn startup_session_gucs_and_tcp_keepalive_and_unix_domain_socket() {
    let (addr, _, server_task) = spawn_scripted_server(|mut stream| async move {
        let params = read_startup_packet(&mut stream).await.unwrap();
        assert_eq!(
            params.get("application_name").map(String::as_str),
            Some("custom-app-from-gucs")
        );
        assert_eq!(
            params.get("search_path").map(String::as_str),
            Some("analytics,public")
        );
        assert_eq!(
            params.get("client_encoding").map(String::as_str),
            Some("UTF8")
        );
        write_frames(
            &mut stream,
            &[
                frame_auth_ok(),
                frame_backend_key_data(101, 202),
                frame_ready_for_query(b'I'),
            ],
        )
        .await
        .unwrap();
        let (tag, _) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        assert_eq!(tag, b'X');
    })
    .await;

    let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
    raw.port = i64::from(addr.port());
    raw.sslmode = "disable".to_string();
    raw.application_name = "default-app".to_string();
    raw.session_gucs
        .insert("search_path".to_string(), "analytics,public".to_string());
    raw.session_gucs.insert(
        "application_name".to_string(),
        "custom-app-from-gucs".to_string(),
    );
    raw.keepalives = true;
    raw.keepalives_idle = Some(30);
    raw.keepalives_interval = Some(10);
    raw.keepalives_count = Some(3);

    let mut handle = SessionHandle::new(raw).unwrap();
    let conn = handle
        .connect()
        .await
        .expect("connect with gucs + keepalive");
    assert_eq!(conn.generation(), 1);
    handle.close().await.unwrap();
    server_task.await.unwrap();

    #[cfg(unix)]
    {
        let sock_path = std::env::temp_dir().join(format!(
            "vtb-pg-unix-{}-{}.sock",
            std::process::id(),
            addr.port()
        ));
        let _ = std::fs::remove_file(&sock_path);
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let unix_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let params = read_startup_packet(&mut stream).await.unwrap();
            assert_eq!(params.get("user").map(String::as_str), Some("unix-user"));
            write_frames(
                &mut stream,
                &[
                    frame_auth_ok(),
                    frame_backend_key_data(303, 404),
                    frame_ready_for_query(b'I'),
                ],
            )
            .await
            .unwrap();
            let (tag, _) = read_tagged_frontend_frame(&mut stream).await.unwrap();
            assert_eq!(tag, b'X');
        });

        let mut raw_unix = RawPostgresOptions::new("localhost", "unix-user", "appdb");
        raw_unix.unix_socket = Some(sock_path.to_string_lossy().into_owned());
        raw_unix.sslmode = "disable".to_string();
        raw_unix.keepalives = false;
        let mut h_unix = SessionHandle::new(raw_unix).unwrap();
        let conn_unix = h_unix.connect().await.expect("unix socket connect");
        assert!(!conn_unix.is_tls());
        h_unix.close().await.unwrap();
        unix_task.await.unwrap();
        let _ = std::fs::remove_file(&sock_path);
    }
}
