use std::collections::HashMap;
use std::time::Duration;

use _postgres::config::{
    ChannelBindingPolicy, PostgresConfig, RawPostgresOptions, parse_strict_sslmode,
};
use _postgres::errors::{PostgresError, ServerError};
use vertebra_db::connection::TlsMode;

fn options() -> RawPostgresOptions {
    RawPostgresOptions {
        user: "app".into(),
        database: "prod".into(),
        ..Default::default()
    }
}

#[test]
fn config_defaults_are_owned_and_complete() {
    let config = PostgresConfig::validate(options()).unwrap();
    assert_eq!(config.user, "app");
    assert_eq!(config.database, "prod");
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.sslmode, TlsMode::VerifyFull);
    assert_eq!(config.channel_binding, ChannelBindingPolicy::Prefer);
    assert_eq!(config.connect_timeout, Duration::from_secs(10));
    assert_eq!(config.application_name, "vertebra");
    assert!(config.keepalives);
    assert!(!config.enable_iam_auth);
    assert_eq!(config.cloud_sql_ip_type, "PRIVATE");
    assert_eq!(config.alloydb_ip_type, "PRIVATE");
    assert_eq!(config.password, None);
    assert_eq!(config.unix_socket, None);
    assert_eq!(config.ca_pem, None);
    assert_eq!(config.client_cert_pem, None);
    assert_eq!(config.client_key_pem, None);
    assert_eq!(config.server_name, None);
    assert_eq!(config.keepalives_idle, None);
    assert_eq!(config.keepalives_interval, None);
    assert_eq!(config.keepalives_count, None);
    assert_eq!(config.cloud_sql_instance, None);
    assert_eq!(config.alloydb_instance_uri, None);
    assert!(config.session_gucs.is_empty());
    assert!(config.cloud_auth_options.is_empty());
    assert!(config.iam_auth_options.is_empty());
    assert!(matches!(
        PostgresConfig::validate(RawPostgresOptions::default()),
        Err(PostgresError::Config(_))
    ));
}

#[test]
fn invalid_scalar_and_host_inputs_are_rejected_without_echoing_values() {
    for field in ["user", "database", "host", "server_name"] {
        for value in ["", "secret-scalar\0suffix"] {
            let mut raw = options();
            match field {
                "user" => raw.user = value.into(),
                "database" => raw.database = value.into(),
                "host" => raw.host = value.into(),
                _ => raw.server_name = Some(value.into()),
            }
            let err = PostgresConfig::validate(raw).unwrap_err();
            assert!(matches!(err, PostgresError::Config(_)));
            assert!(!err.to_string().contains("secret-scalar"));
        }
    }
    for host in ["postgresql://localhost/db", "/var/run/postgresql"] {
        assert!(matches!(
            PostgresConfig::validate(RawPostgresOptions {
                host: host.into(),
                ..options()
            }),
            Err(PostgresError::Config(_))
        ));
    }
    for host in ["does-not-exist.invalid", "127.0.0.1", "::1"] {
        assert_eq!(
            PostgresConfig::validate(RawPostgresOptions {
                host: host.into(),
                ..options()
            })
            .unwrap()
            .host,
            host
        );
    }
    for raw in [
        RawPostgresOptions {
            password: Some("secret-password\0".into()),
            ..options()
        },
        RawPostgresOptions {
            application_name: "secret-app\0".into(),
            ..options()
        },
    ] {
        let err = PostgresConfig::validate(raw).unwrap_err();
        assert!(matches!(err, PostgresError::Config(_)));
        assert!(!err.to_string().contains("secret-"));
    }
}

#[test]
fn signed_ports_and_timeout_conversion_are_checked_before_conversion() {
    for port in [i64::MIN, -1, 0, 65536, i64::MAX] {
        assert!(matches!(
            PostgresConfig::validate(RawPostgresOptions { port, ..options() }),
            Err(PostgresError::Config(_))
        ));
    }
    for port in [1, 65535] {
        assert_eq!(
            PostgresConfig::validate(RawPostgresOptions { port, ..options() })
                .unwrap()
                .port,
            port as u16
        );
    }
    for connect_timeout in [
        0.0,
        -0.0,
        -5.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::MAX,
    ] {
        assert!(matches!(
            PostgresConfig::validate(RawPostgresOptions {
                connect_timeout,
                ..options()
            }),
            Err(PostgresError::Config(_))
        ));
    }
    assert_eq!(
        PostgresConfig::validate(RawPostgresOptions {
            connect_timeout: 0.25,
            ..options()
        })
        .unwrap()
        .connect_timeout,
        Duration::from_millis(250)
    );
}

#[test]
fn tls_and_binding_spellings_are_strict() {
    for (raw, expected) in [
        ("disable", TlsMode::Disable),
        ("prefer", TlsMode::Prefer),
        ("require", TlsMode::Require),
        ("verify-ca", TlsMode::VerifyCa),
        ("verify-full", TlsMode::VerifyFull),
    ] {
        assert_eq!(parse_strict_sslmode(raw).unwrap(), expected);
        assert_eq!(
            PostgresConfig::validate(RawPostgresOptions {
                sslmode: raw.into(),
                ..options()
            })
            .unwrap()
            .sslmode,
            expected
        );
    }
    for raw in [
        "allow",
        "true",
        "false",
        "off",
        "on",
        "verify_full",
        "verify_ca",
        "PREFER",
        " prefer",
        "prefer ",
        "",
    ] {
        assert!(matches!(
            parse_strict_sslmode(raw),
            Err(PostgresError::Config(_))
        ));
        assert!(matches!(
            PostgresConfig::validate(RawPostgresOptions {
                sslmode: raw.into(),
                ..options()
            }),
            Err(PostgresError::Config(_))
        ));
    }
    assert_eq!(
        ChannelBindingPolicy::default(),
        ChannelBindingPolicy::Prefer
    );
    for (raw, expected) in [
        ("disable", ChannelBindingPolicy::Disable),
        ("prefer", ChannelBindingPolicy::Prefer),
        ("require", ChannelBindingPolicy::Require),
    ] {
        assert_eq!(ChannelBindingPolicy::parse(raw).unwrap(), expected);
        assert_eq!(
            PostgresConfig::validate(RawPostgresOptions {
                channel_binding: raw.into(),
                ..options()
            })
            .unwrap()
            .channel_binding,
            expected
        );
    }
    for raw in ["PREFER", "true", " require", "require ", ""] {
        assert!(matches!(
            ChannelBindingPolicy::parse(raw),
            Err(PostgresError::Config(_))
        ));
        assert!(matches!(
            PostgresConfig::validate(RawPostgresOptions {
                channel_binding: raw.into(),
                ..options()
            }),
            Err(PostgresError::Config(_))
        ));
    }
}

#[test]
fn pem_pairs_and_empty_buffers_fail_without_key_disclosure() {
    for raw in [
        RawPostgresOptions {
            ca_pem: Some(vec![]),
            ..options()
        },
        RawPostgresOptions {
            client_cert_pem: Some(vec![]),
            client_key_pem: Some(b"PRIVATE-KEY-SECRET".to_vec()),
            ..options()
        },
        RawPostgresOptions {
            client_cert_pem: Some(b"CERT".to_vec()),
            client_key_pem: Some(vec![]),
            ..options()
        },
        RawPostgresOptions {
            client_cert_pem: Some(b"CERT".to_vec()),
            ..options()
        },
        RawPostgresOptions {
            client_key_pem: Some(b"PRIVATE-KEY-SECRET".to_vec()),
            ..options()
        },
    ] {
        let err = PostgresConfig::validate(raw).unwrap_err();
        assert!(matches!(err, PostgresError::Config(_)));
        assert!(!err.to_string().contains("PRIVATE-KEY-SECRET"));
    }
}

#[test]
fn explicit_options_survive_validation_and_debug_redacts_secrets() {
    let mut raw = RawPostgresOptions {
        host: "db.invalid".into(),
        port: 6543,
        password: Some("s3cr3t-pw".into()),
        unix_socket: Some("/socket/file".into()),
        sslmode: "disable".into(),
        ca_pem: Some(b"CA".to_vec()),
        client_cert_pem: Some(b"CERT".to_vec()),
        client_key_pem: Some(b"PRIVATE-KEY-SECRET".to_vec()),
        server_name: Some("db.example".into()),
        channel_binding: "require".into(),
        connect_timeout: 1.5,
        application_name: "app-name".into(),
        session_gucs: HashMap::from([("search_path".into(), "app,public".into())]),
        keepalives: false,
        keepalives_idle: Some(42),
        keepalives_interval: Some(12),
        keepalives_count: Some(3),
        cloud_sql_instance: Some("project:region:instance".into()),
        cloud_sql_ip_type: "PSC".into(),
        alloydb_instance_uri: Some("projects/p/locations/l/clusters/c/instances/i".into()),
        alloydb_ip_type: "PUBLIC".into(),
        enable_iam_auth: true,
        cloud_auth_options: HashMap::from([("access_token".into(), "mock-admin-token".into())]),
        iam_auth_options: HashMap::from([("access_token".into(), "mock-login-token".into())]),
        ..options()
    };
    let config = PostgresConfig::validate(raw.clone()).unwrap();
    raw.password.as_mut().unwrap().clear();
    raw.client_key_pem.as_mut().unwrap().clear();
    raw.session_gucs.clear();
    raw.cloud_auth_options.clear();
    raw.iam_auth_options.clear();
    assert_eq!(config.password.as_deref(), Some("s3cr3t-pw"));
    assert_eq!(
        config.client_key_pem.as_deref(),
        Some(b"PRIVATE-KEY-SECRET".as_slice())
    );
    assert_eq!(config.session_gucs["search_path"], "app,public");
    assert_eq!(
        config.cloud_auth_options["access_token"],
        "mock-admin-token"
    );
    assert_eq!(config.iam_auth_options["access_token"], "mock-login-token");
    assert_eq!(config.host, "db.invalid");
    assert_eq!(config.port, 6543);
    assert_eq!(config.unix_socket.as_deref(), Some("/socket/file"));
    assert_eq!(config.sslmode, TlsMode::Disable);
    assert_eq!(config.channel_binding, ChannelBindingPolicy::Require);
    assert_eq!(config.ca_pem.as_deref(), Some(b"CA".as_slice()));
    assert_eq!(config.client_cert_pem.as_deref(), Some(b"CERT".as_slice()));
    assert_eq!(config.server_name.as_deref(), Some("db.example"));
    assert_eq!(config.connect_timeout, Duration::from_millis(1500));
    assert_eq!(config.application_name, "app-name");
    assert!(!config.keepalives);
    assert_eq!(config.keepalives_idle, Some(42));
    assert_eq!(config.keepalives_interval, Some(12));
    assert_eq!(config.keepalives_count, Some(3));
    assert_eq!(
        config.cloud_sql_instance.as_deref(),
        Some("project:region:instance")
    );
    assert_eq!(config.cloud_sql_ip_type, "PSC");
    assert_eq!(
        config.alloydb_instance_uri.as_deref(),
        Some("projects/p/locations/l/clusters/c/instances/i")
    );
    assert_eq!(config.alloydb_ip_type, "PUBLIC");
    assert!(config.enable_iam_auth);
    let debug = format!("{config:?}");
    for secret in [
        "s3cr3t-pw",
        "PRIVATE-KEY-SECRET",
        "mock-admin-token",
        "mock-login-token",
    ] {
        assert!(!debug.contains(secret));
    }
    assert!(debug.contains("<redacted>"));
}

#[test]
fn server_errors_retain_diagnostics_but_display_only_primary_fields() {
    let err = PostgresError::server(
        "FATAL",
        "28P01",
        "password authentication failed",
        vec![
            (b'D', "secret detail".into()),
            (b'H', "secret hint".into()),
            (b'W', "secret context".into()),
        ],
    )
    .unwrap();
    assert_eq!(err.sqlstate(), Some("28P01"));
    assert_eq!(
        err.to_string(),
        "FATAL [28P01]: password authentication failed"
    );
    let PostgresError::Server(server) = err else {
        panic!("expected server error")
    };
    assert_eq!(
        server,
        ServerError {
            severity: "FATAL".into(),
            sqlstate: "28P01".into(),
            message: "password authentication failed".into(),
            diagnostics: vec![
                (b'D', "secret detail".into()),
                (b'H', "secret hint".into()),
                (b'W', "secret context".into())
            ]
        }
    );
    let sanitized =
        PostgresError::server("ERROR\n", "XX000", "bad\0message\rnext", vec![]).unwrap();
    assert_eq!(sanitized.to_string(), "ERROR  [XX000]: bad message next");
    for sqlstate in ["28", "28P001", "28P0!", "é123", "28\0P1", "28P 1"] {
        assert!(matches!(
            PostgresError::server("ERROR", sqlstate, "failure", vec![]),
            Err(PostgresError::Protocol(_))
        ));
    }
    assert_eq!(
        PostgresError::server("ERROR", "abc12", "failure", vec![])
            .unwrap()
            .sqlstate(),
        Some("abc12")
    );
    for error in [
        PostgresError::Config("invalid option".into()),
        PostgresError::Interface("closed".into()),
        PostgresError::Data("invalid value".into()),
        PostgresError::UnsupportedAuth("mechanism".into()),
        PostgresError::UnsupportedFeature("feature".into()),
        PostgresError::Transport("disconnected".into()),
        PostgresError::Protocol("bad frame".into()),
    ] {
        assert_eq!(error.sqlstate(), None);
        assert!(!error.to_string().is_empty());
    }
}
