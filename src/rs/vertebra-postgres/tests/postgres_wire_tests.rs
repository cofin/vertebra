use std::collections::HashMap;
use std::time::Duration;

use _postgres::config::{
    ChannelBindingPolicy, PostgresConfig, RawPostgresOptions, parse_strict_sslmode,
};
use _postgres::errors::{PostgresError, ServerError};
use vertebra_db::connection::TlsMode;

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
