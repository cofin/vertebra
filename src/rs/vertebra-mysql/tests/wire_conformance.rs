//! Integration tests for `vertebra-mysql` wire protocol, authentication plugins,
//! binary prepared statements, codecs, Arrow SHM round-trips, and Cloud SQL / IAM config.

use _mysql::client::{DecodedCell, MySQLClientCore};
use _mysql::config::{MySQLConnectConfig, MySQLSslMode};
use _mysql::errors::{MySQLErrorKind, classify_mysql_error};
use _mysql::loopback::MySQLWireTestServer;
use _mysql::protocol::{
    compute_caching_sha2_scramble_bytes, compute_mysql_native_scramble_bytes,
    encrypt_caching_sha2_rsa,
};
use mysql_async::{Params, Value};
use vertebra_db::py_async::block_on_db;

fn build_config(dsn: &str, statement_cache_size: usize) -> MySQLConnectConfig {
    MySQLConnectConfig::from_dsn_and_kwargs(
        dsn,
        None,
        None,
        None,
        None,
        None,
        None,
        Some("disable"),
        None,
        None,
        None,
        5_000,
        5_000,
        5_000,
        1,
        2,
        statement_cache_size,
        "utf8mb4",
        true,
        None,
        None,
        "PUBLIC",
        false,
        None,
        None,
        None,
    )
    .expect("valid MySQLConnectConfig")
}

#[test]
fn test_wire_conformance_handshake_auth_plugins_and_rsa_exchange() {
    for (plugin, rsa_full) in [
        ("caching_sha2_password", false),
        ("caching_sha2_password", true),
        ("mysql_native_password", false),
    ] {
        let server = MySQLWireTestServer::start_with_options(plugin, rsa_full, "s3cr3t-pass")
            .expect("loopback server start");
        let core = MySQLClientCore::new(build_config(&server.dsn(), 8));

        block_on_db(async {
            let ok = core.ping().await.expect("ping");
            assert!(ok);
            let rs = core
                .execute_result_set("SELECT 1 AS one".to_string(), Params::Empty)
                .await
                .expect("select 1");
            assert_eq!(rs.rows.len(), 1);
            assert!(matches!(rs.rows[0][0], DecodedCell::Int(1)));
            core.close().await.expect("close");
        });
    }

    let nonce = b"12345678901234567890";
    let sha2 = compute_caching_sha2_scramble_bytes(b"pw", nonce).expect("sha2");
    let sha1 = compute_mysql_native_scramble_bytes(b"pw", nonce).expect("sha1");
    assert_eq!(sha2.len(), 32);
    assert_eq!(sha1.len(), 20);
    assert!(encrypt_caching_sha2_rsa(b"pw", nonce, "invalid-pem").is_err());
}

#[test]
fn test_wire_conformance_config_validation_and_secret_redaction() {
    for bad_dsn in [
        "",
        "memory://",
        "mysql://memory/db",
        "mysql://root@127.0.0.1:3306/:memory:",
        "mysql://root@127.0.0.1:3306/db\0injected",
        "postgres://root@127.0.0.1:5432/db",
    ] {
        let err = MySQLConnectConfig::from_dsn_and_kwargs(
            bad_dsn, None, None, None, None, None, None, None, None, None, None, 5_000, 5_000,
            5_000, 1, 4, 32, "utf8mb4", true, None, None, "PUBLIC", false, None, None, None,
        )
        .expect_err("invalid DSN should fail");
        assert_eq!(err.kind, MySQLErrorKind::ImproperConfiguration);
    }

    let zero_port_err = MySQLConnectConfig::from_dsn_and_kwargs(
        "mysql://root@127.0.0.1/app",
        None,
        Some(0),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        5_000,
        5_000,
        5_000,
        1,
        4,
        32,
        "utf8mb4",
        true,
        None,
        None,
        "PUBLIC",
        false,
        None,
        None,
        None,
    )
    .expect_err("port=0 must fail");
    assert_eq!(zero_port_err.kind, MySQLErrorKind::ImproperConfiguration);

    let bad_pem_err = MySQLConnectConfig::from_dsn_and_kwargs(
        "mysql://root@127.0.0.1:3306/app",
        None,
        None,
        None,
        None,
        None,
        None,
        Some("verify-ca"),
        Some(
            "-----BEGIN CERTIFICATE-----\nnot-valid-base64!!!\n-----END CERTIFICATE-----"
                .to_string(),
        ),
        None,
        None,
        5_000,
        5_000,
        5_000,
        1,
        4,
        32,
        "utf8mb4",
        true,
        None,
        None,
        "PUBLIC",
        false,
        None,
        None,
        None,
    )
    .expect_err("corrupt inline PEM must fail");
    assert_eq!(bad_pem_err.kind, MySQLErrorKind::ImproperConfiguration);

    let cfg = MySQLConnectConfig::from_dsn_and_kwargs(
        "cloudsql://proj:us-central1:inst/prod_db?ssl_mode=verify-full&enable_iam_auth=true",
        None,
        None,
        Some("iam-sa".to_string()),
        Some("super-secret-password-value".to_string()),
        None,
        None,
        None,
        None,
        None,
        None,
        5_000,
        5_000,
        5_000,
        1,
        4,
        32,
        "utf8mb4",
        true,
        None,
        None,
        "PRIVATE",
        false,
        None,
        Some("{\"private_key\":\"secret-jwt-key\"}".to_string()),
        None,
    )
    .expect("valid cloudsql config");
    assert_eq!(cfg.ssl_mode, MySQLSslMode::VerifyFull);
    assert!(cfg.enable_iam_auth);
    let debug_str = format!("{cfg:?}");
    let display_str = format!("{cfg}");
    assert!(!debug_str.contains("super-secret-password-value"));
    assert!(!debug_str.contains("secret-jwt-key"));
    assert!(!display_str.contains("super-secret-password-value"));
    assert!(!display_str.contains("secret-jwt-key"));
}

#[test]
fn test_wire_conformance_binary_protocol_returning_count_and_errors() {
    let server = MySQLWireTestServer::start_default().expect("server");
    let core = MySQLClientCore::new(build_config(&server.dsn(), 4));

    block_on_db(async {
        core.execute_result_set(
            "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64) NOT NULL, payload BLOB)"
                .to_string(),
            Params::Empty,
        )
        .await
        .expect("create table");

        let ret = core
            .execute_result_set(
                "INSERT INTO items (name, payload) VALUES (?, ?) RETURNING id".to_string(),
                Params::Positional(vec![
                    Value::Bytes(b"alpha".to_vec()),
                    Value::Bytes(b"ascii-blob".to_vec()),
                ]),
            )
            .await
            .expect("insert returning");
        assert_eq!(ret.rows.len(), 1);
        assert!(matches!(ret.rows[0][0], DecodedCell::Int(1)));

        let empty_sel = core
            .execute_result_set(
                "SELECT id, name, payload FROM items WHERE id = ?".to_string(),
                Params::Positional(vec![Value::Int(999)]),
            )
            .await
            .expect("empty select");
        assert!(empty_sel.rows.is_empty());
        assert_eq!(empty_sel.columns.len(), 3);
        assert_eq!(empty_sel.columns[0].0, "id");
        assert_eq!(empty_sel.columns[1].0, "name");
        assert_eq!(empty_sel.columns[2].0, "payload");

        let subq_count = core
            .execute_result_set(
                "SELECT COUNT(*) FROM (SELECT id, name FROM items WHERE id = ?) AS _vtb_count_subq"
                    .to_string(),
                Params::Positional(vec![Value::Int(1)]),
            )
            .await
            .expect("subquery count");
        assert_eq!(subq_count.rows.len(), 1);
        assert!(matches!(subq_count.rows[0][0], DecodedCell::Int(1)));

        let blob_sel = core
            .execute_result_set(
                "SELECT payload FROM items WHERE id = ?".to_string(),
                Params::Positional(vec![Value::Int(1)]),
            )
            .await
            .expect("blob select");
        assert!(matches!(
            &blob_sel.rows[0][0],
            DecodedCell::Bytes(b) if b == b"ascii-blob"
        ));

        assert_eq!(
            classify_mysql_error(1062, Some("23000")),
            MySQLErrorKind::IntegrityError
        );
        assert_eq!(
            classify_mysql_error(1213, Some("40001")),
            MySQLErrorKind::SerializationFailure
        );
        assert_eq!(
            classify_mysql_error(1317, Some("70100")),
            MySQLErrorKind::QueryCancelled
        );

        core.close().await.expect("close");
    });
}
