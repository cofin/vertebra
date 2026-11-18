//! Golden TDS 7.4 loopback wire tests for `vertebra-mssql`:
//! - `PRELOGIN` (`0x12`), `LOGIN7` (`0x10`), and `ENVCHANGE` routing (`0xE3` type `20`) redirects
//! - `sp_executesql` RPC (`0x03`) with `@P1` positional and `@name` named parameter rewriting
//! - `execute`, `execute_many`, `fetch_all`, `fetch_one`, `fetch_optional`, and `select_value`
//! - Transaction Manager (`0x0E`) `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVE` and savepoint rollback
//! - Out-of-band `ATTENTION` (`0x06`) query cancellation and timeout handling
//! - Server `ERROR` token (`0xAA`) classification into `vertebra._vertebra.DatabaseError` subclasses

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use _mssql::client::MssqlClient;
use _mssql::config::{MssqlClientConfig, MssqlEncryptionMode};
use _mssql::errors::{MssqlErrorKind, mssql_error_to_py};
use _mssql::params::MssqlParam;
use pyo3::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const PKT_SQL_BATCH: u8 = 0x01;
const PKT_RPC: u8 = 0x03;
const PKT_TABULAR_RESULT: u8 = 0x04;
const PKT_ATTENTION: u8 = 0x06;
const PKT_BULK_LOAD: u8 = 0x07;
const PKT_TRANS_MGR: u8 = 0x0E;
const PKT_LOGIN7: u8 = 0x10;
const PKT_PRELOGIN: u8 = 0x12;

fn wrap_tds_packet(pkt_type: u8, payload: &[u8]) -> Vec<u8> {
    let total_len = (8 + payload.len()) as u16;
    let mut out = Vec::with_capacity(total_len as usize);
    out.push(pkt_type);
    out.push(0x01); // EOM
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes()); // SPID
    out.push(1); // PacketID
    out.push(0); // Window
    out.extend_from_slice(payload);
    out
}

async fn read_tds_message(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut full_payload = Vec::new();
    let mut msg_type;
    loop {
        let mut hdr = [0u8; 8];
        stream.read_exact(&mut hdr).await?;
        msg_type = hdr[0];
        let status = hdr[1];
        let total_len = u16::from_be_bytes([hdr[2], hdr[3]]) as usize;
        let body_len = total_len.saturating_sub(8);
        let mut chunk = vec![0u8; body_len];
        if body_len > 0 {
            stream.read_exact(&mut chunk).await?;
        }
        full_payload.extend_from_slice(&chunk);
        if (status & 0x01) != 0 {
            break;
        }
    }
    Ok((msg_type, full_payload))
}

fn build_prelogin_response_not_supported() -> Vec<u8> {
    // Option table:
    // VERSION (0): offset 16, len 6
    // ENCRYPTION (1): offset 22, len 1 (2 = ENCRYPT_NOT_SUP)
    // INSTOPT (2): offset 23, len 1 (0x00)
    // TERMINATOR (0xFF)
    let body = vec![
        0x00, 0x00, 0x10, 0x00, 0x06, // VERSION
        0x01, 0x00, 0x16, 0x00, 0x01, // ENCRYPTION
        0x02, 0x00, 0x17, 0x00, 0x01, // INSTOPT
        0xFF, // TERMINATOR
        16, 0, 0x03, 0xE8, 0, 0,    // version 16.0.1000.0
        0x02, // EncryptionLevel::NotSupported
        0x00, // INSTOPT valid
    ];
    wrap_tds_packet(PKT_TABULAR_RESULT, &body)
}

fn put_b_varchar(buf: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().collect();
    buf.push(units.len() as u8);
    for u in units {
        buf.extend_from_slice(&u.to_le_bytes());
    }
}

fn put_us_varchar(buf: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().collect();
    buf.extend_from_slice(&(units.len() as u16).to_le_bytes());
    for u in units {
        buf.extend_from_slice(&u.to_le_bytes());
    }
}

fn append_loginack_token(buf: &mut Vec<u8>) {
    let mut ack = Vec::new();
    ack.push(1); // SQL_TSQL interface
    ack.extend_from_slice(&0x7400_0004u32.to_be_bytes()); // TDS 7.4 (`FeatureLevel::SqlServerN`)
    put_b_varchar(&mut ack, "Microsoft SQL Server");
    ack.extend_from_slice(&0x1000_03E8u32.to_le_bytes());

    buf.push(0xAD); // LOGINACK
    buf.extend_from_slice(&(ack.len() as u16).to_le_bytes());
    buf.extend_from_slice(&ack);
}

fn append_done_token(buf: &mut Vec<u8>, token_byte: u8, status: u16, cur_cmd: u16, rows: u64) {
    buf.push(token_byte);
    buf.extend_from_slice(&status.to_le_bytes());
    buf.extend_from_slice(&cur_cmd.to_le_bytes());
    buf.extend_from_slice(&rows.to_le_bytes());
}

fn append_routing_envchange(buf: &mut Vec<u8>, host: &str, port: u16) {
    let host_utf16: Vec<u16> = host.encode_utf16().collect();
    let mut routing_val = Vec::new();
    routing_val.push(0); // protocol = TCP (0)
    routing_val.extend_from_slice(&port.to_le_bytes());
    routing_val.extend_from_slice(&(host_utf16.len() as u16).to_le_bytes());
    for u in host_utf16 {
        routing_val.extend_from_slice(&u.to_le_bytes());
    }

    let mut env_body = Vec::new();
    env_body.push(20); // EnvChangeTy::Routing
    env_body.extend_from_slice(&(routing_val.len() as u16).to_le_bytes());
    env_body.extend_from_slice(&routing_val);
    env_body.extend_from_slice(&0u16.to_le_bytes()); // old value length = 0

    buf.push(0xE3); // ENVCHANGE
    buf.extend_from_slice(&(env_body.len() as u16).to_le_bytes());
    buf.extend_from_slice(&env_body);
}

fn append_error_token(buf: &mut Vec<u8>, number: u32, state: u8, class: u8, message: &str) {
    let mut err_body = Vec::new();
    err_body.extend_from_slice(&number.to_le_bytes());
    err_body.push(state);
    err_body.push(class);
    put_us_varchar(&mut err_body, message);
    put_b_varchar(&mut err_body, "loopback-sql");
    put_b_varchar(&mut err_body, "sp_executesql");
    err_body.extend_from_slice(&1u32.to_le_bytes()); // line 1

    buf.push(0xAA); // ERROR token
    buf.extend_from_slice(&(err_body.len() as u16).to_le_bytes());
    buf.extend_from_slice(&err_body);
}

fn append_two_col_resultset(buf: &mut Vec<u8>, rows: &[(i32, &str)]) {
    // COLMETADATA (0x81) for 2 columns:
    // Col 0: `id` FixedLenType::Int4 (0x38)
    // Col 1: `name` VarLenType::NVarchar (0xE7), max_len = 256, collation = [0x09, 0x04, 0xD0, 0x00, 0x34]
    buf.push(0x81);
    buf.extend_from_slice(&2u16.to_le_bytes());

    // Column 0: `id`
    buf.extend_from_slice(&0u32.to_le_bytes()); // UserType
    buf.extend_from_slice(&0x0008u16.to_le_bytes()); // Flags (Updateable)
    buf.push(0x38); // FixedLenType::Int4
    put_b_varchar(buf, "id");

    // Column 1: `name`
    buf.extend_from_slice(&0u32.to_le_bytes()); // UserType
    buf.extend_from_slice(&0x0009u16.to_le_bytes()); // Flags (Nullable | Updateable)
    buf.push(0xE7); // VarLenType::NVarchar
    buf.extend_from_slice(&256u16.to_le_bytes()); // max byte len
    buf.extend_from_slice(&[0x09, 0x04, 0xD0, 0x00, 0x34]); // collation
    put_b_varchar(buf, "name");

    for &(id, name) in rows {
        buf.push(0xD1); // ROW token
        buf.extend_from_slice(&id.to_le_bytes());
        let utf16: Vec<u16> = name.encode_utf16().collect();
        let byte_len = (utf16.len() * 2) as u16;
        buf.extend_from_slice(&byte_len.to_le_bytes());
        for u in utf16 {
            buf.extend_from_slice(&u.to_le_bytes());
        }
    }
}

fn decode_utf16le_ascii_lossy(payload: &[u8]) -> String {
    let mut out = String::new();
    for &pair in payload.as_chunks::<2>().0 {
        let u = u16::from_le_bytes(pair);
        if (0x20..=0x7E).contains(&u) {
            out.push(char::from_u32(u as u32).unwrap_or(' '));
        }
    }
    out
}

async fn spawn_mock_tds_server(
    redirect_to: Option<(String, u16)>,
    attention_counter: Arc<AtomicUsize>,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let redirect_clone = redirect_to.clone();
            let attn_clone = Arc::clone(&attention_counter);
            tokio::spawn(async move {
                // 1. PRELOGIN
                let Ok((pkt_ty, _)) = read_tds_message(&mut stream).await else {
                    return;
                };
                assert_eq!(pkt_ty, PKT_PRELOGIN);
                let _ = stream
                    .write_all(&build_prelogin_response_not_supported())
                    .await;

                // 2. LOGIN7
                let Ok((pkt_ty, _)) = read_tds_message(&mut stream).await else {
                    return;
                };
                assert_eq!(pkt_ty, PKT_LOGIN7);

                let mut login_resp = Vec::new();
                if let Some((r_host, r_port)) = redirect_clone {
                    append_routing_envchange(&mut login_resp, &r_host, r_port);
                    append_loginack_token(&mut login_resp);
                    append_done_token(&mut login_resp, 0xFD, 0, 0, 0);
                    let _ = stream
                        .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &login_resp))
                        .await;
                    return;
                }

                append_loginack_token(&mut login_resp);
                append_done_token(&mut login_resp, 0xFD, 0, 0, 0);
                if stream
                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &login_resp))
                    .await
                    .is_err()
                {
                    return;
                }

                // 3. Command loop
                while let Ok((cmd_ty, payload)) = read_tds_message(&mut stream).await {
                    match cmd_ty {
                        PKT_SQL_BATCH => {
                            let ascii_text = decode_utf16le_ascii_lossy(&payload);
                            let mut resp = Vec::new();
                            if ascii_text.contains("SELECT TOP 0") {
                                // Emit COLMETADATA for bulk insert target: `id` BIGINT (Int8 0x7F) + `name` NVARCHAR(256)
                                resp.push(0x81);
                                resp.extend_from_slice(&2u16.to_le_bytes());

                                // Col 0: `id` BIGINT (`FixedLenType::Int8 = 0x7F`, Updateable)
                                resp.extend_from_slice(&0u32.to_le_bytes());
                                resp.extend_from_slice(&0x0008u16.to_le_bytes());
                                resp.push(0x7F);
                                put_b_varchar(&mut resp, "id");

                                // Col 1: `name` NVARCHAR(256) (`0xE7`, Nullable | Updateable)
                                resp.extend_from_slice(&0u32.to_le_bytes());
                                resp.extend_from_slice(&0x0009u16.to_le_bytes());
                                resp.push(0xE7);
                                resp.extend_from_slice(&256u16.to_le_bytes());
                                resp.extend_from_slice(&[0x09, 0x04, 0xD0, 0x00, 0x34]);
                                put_b_varchar(&mut resp, "name");
                            }
                            append_done_token(&mut resp, 0xFD, 0, 0, 0);
                            let _ = stream
                                .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                .await;
                        }
                        PKT_BULK_LOAD => {
                            let mut resp = Vec::new();
                            append_done_token(&mut resp, 0xFD, 0x0010, 0xF0, 2);
                            let _ = stream
                                .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                .await;
                        }
                        PKT_TRANS_MGR => {
                            // ALL_HEADERS is 22 bytes, followed by u16 LE request_type
                            let req_type = if payload.len() >= 24 {
                                u16::from_le_bytes([payload[22], payload[23]])
                            } else {
                                0
                            };
                            let mut resp = Vec::new();
                            match req_type {
                                5 => {
                                    // TM_BEGIN_XACT -> ENVCHANGE BeginTransaction (8)
                                    resp.push(0xE3);
                                    resp.extend_from_slice(&11u16.to_le_bytes());
                                    resp.push(8);
                                    resp.push(8);
                                    resp.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
                                    resp.push(0);
                                }
                                7 => {
                                    // TM_COMMIT_XACT -> ENVCHANGE CommitTransaction (9)
                                    resp.push(0xE3);
                                    resp.extend_from_slice(&11u16.to_le_bytes());
                                    resp.push(9);
                                    resp.push(0);
                                    resp.push(8);
                                    resp.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
                                }
                                8 => {
                                    // TM_ROLLBACK_XACT -> ENVCHANGE RollbackTransaction (10)
                                    resp.push(0xE3);
                                    resp.extend_from_slice(&11u16.to_le_bytes());
                                    resp.push(10);
                                    resp.push(0);
                                    resp.push(8);
                                    resp.extend_from_slice(&0x1122_3344_5566_7788u64.to_le_bytes());
                                }
                                _ => {
                                    // TM_SAVE_XACT (9)
                                }
                            }
                            append_done_token(&mut resp, 0xFD, 0, 0, 0);
                            let _ = stream
                                .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                .await;
                        }
                        PKT_RPC => {
                            let ascii_text = decode_utf16le_ascii_lossy(&payload);
                            if ascii_text.contains("WAITFOR DELAY") {
                                // Wait for PKT_ATTENTION (0x06) from client
                                if let Ok((attn_ty, _)) = read_tds_message(&mut stream).await
                                    && attn_ty == PKT_ATTENTION
                                {
                                    attn_clone.fetch_add(1, Ordering::SeqCst);
                                    let mut resp = Vec::new();
                                    // DONE with DONE_ATTN (0x0020)
                                    append_done_token(&mut resp, 0xFD, 0x0020, 0, 0);
                                    let _ = stream
                                        .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                        .await;
                                }
                            } else if let Some(idx) = ascii_text.find("RAISE_ERROR_") {
                                let digits: String = ascii_text[idx + "RAISE_ERROR_".len()..]
                                    .chars()
                                    .take_while(char::is_ascii_digit)
                                    .collect();
                                let code: u32 = digits.parse().unwrap_or(50000);
                                let mut resp = Vec::new();
                                append_error_token(
                                    &mut resp,
                                    code,
                                    1,
                                    16,
                                    &format!("Synthetic SQL Server error {code}"),
                                );
                                append_done_token(&mut resp, 0xFD, 0x0002, 0, 0);
                                let _ = stream
                                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                    .await;
                            } else if ascii_text.contains("ZERO_ROWS") {
                                let mut resp = Vec::new();
                                append_two_col_resultset(&mut resp, &[]);
                                append_done_token(&mut resp, 0xFF, 0x0010, 0xC1, 0);
                                append_done_token(&mut resp, 0xFE, 0, 0, 0);
                                let _ = stream
                                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                    .await;
                            } else if ascii_text.contains("SELECT") {
                                let mut resp = Vec::new();
                                append_two_col_resultset(&mut resp, &[(42, "Ada"), (99, "Grace")]);
                                append_done_token(&mut resp, 0xFF, 0x0010, 0xC1, 2);
                                append_done_token(&mut resp, 0xFE, 0, 0, 0);
                                let _ = stream
                                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                    .await;
                            } else {
                                let mut resp = Vec::new();
                                append_done_token(&mut resp, 0xFF, 0x0010, 0xC3, 1);
                                append_done_token(&mut resp, 0xFE, 0, 0, 0);
                                let _ = stream
                                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                    .await;
                            }
                        }
                        PKT_ATTENTION => {
                            attn_clone.fetch_add(1, Ordering::SeqCst);
                            let mut resp = Vec::new();
                            append_done_token(&mut resp, 0xFD, 0x0020, 0, 0);
                            let _ = stream
                                .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                                .await;
                        }
                        _ => break,
                    }
                }
            });
        }
    });

    addr
}

fn loopback_config(addr: std::net::SocketAddr) -> MssqlClientConfig {
    let dsn = format!(
        "mssql://sa:Secret123!@127.0.0.1:{}/tempdb?encrypt=not_supported",
        addr.port()
    );
    let mut cfg = MssqlClientConfig::from_dsn(&dsn).expect("valid DSN");
    cfg.encryption = MssqlEncryptionMode::Disabled;
    cfg.query_timeout = Duration::from_secs(5);
    cfg
}

#[tokio::test]
async fn tds_prelogin_login7_envchange_routing_and_rpc_lifecycle() {
    let attn_counter = Arc::new(AtomicUsize::new(0));
    let target_addr = spawn_mock_tds_server(None, Arc::clone(&attn_counter)).await;
    let router_addr = spawn_mock_tds_server(
        Some(("127.0.0.1".to_string(), target_addr.port())),
        Arc::clone(&attn_counter),
    )
    .await;

    let client = MssqlClient::new(loopback_config(router_addr)).expect("client");

    assert!(client.ping().await.expect("ping across ENVCHANGE routing"));

    let affected = client
        .execute(
            "INSERT INTO users (id, name) VALUES (?, ?)".to_string(),
            vec![MssqlParam::I64(42), MssqlParam::String("Ada".to_string())],
        )
        .await
        .expect("execute DML");
    assert_eq!(affected, 1);

    let batch_affected = client
        .execute_many(vec![
            (
                "INSERT INTO users (id, name) VALUES (@P1, @P2)".to_string(),
                vec![MssqlParam::I64(1), MssqlParam::String("One".to_string())],
            ),
            (
                "INSERT INTO users (id, name) VALUES (@P1, @P2)".to_string(),
                vec![MssqlParam::I64(2), MssqlParam::String("Two".to_string())],
            ),
        ])
        .await
        .expect("execute_many");
    assert_eq!(batch_affected, 2);

    let res = client
        .query(
            "SELECT id, name FROM users WHERE id = ?".to_string(),
            vec![MssqlParam::I64(42)],
        )
        .await
        .expect("query");
    assert_eq!(res.columns.len(), 2);
    assert_eq!(res.rows.len(), 2);

    let one = client
        .fetch_one("SELECT id, name FROM users".to_string(), vec![])
        .await
        .expect("fetch_one");
    assert_eq!(one.rows.len(), 1);

    let missing_err = client
        .fetch_one("SELECT ZERO_ROWS FROM users".to_string(), vec![])
        .await
        .expect_err("fetch_one on empty set must fail with NotFound");
    assert_eq!(missing_err.kind, MssqlErrorKind::NotFound);

    let opt_empty = client
        .fetch_optional("SELECT ZERO_ROWS FROM users".to_string(), vec![])
        .await
        .expect("fetch_optional on empty set");
    assert!(opt_empty.rows.is_empty());

    let missing_val_err = client
        .select_value("SELECT ZERO_ROWS FROM users".to_string(), vec![])
        .await
        .expect_err("select_value on empty set must fail with NotFound");
    assert_eq!(missing_val_err.kind, MssqlErrorKind::NotFound);

    // Transaction Manager lifecycle: BEGIN, SAVEPOINT, ROLLBACK TO SAVEPOINT, RELEASE, COMMIT, ROLLBACK
    client
        .begin_transaction(Some("READ_COMMITTED".to_string()))
        .await
        .expect("begin_transaction");
    client
        .savepoint("vtb_sp1".to_string())
        .await
        .expect("savepoint");
    client
        .rollback_to_savepoint("vtb_sp1".to_string())
        .await
        .expect("rollback_to_savepoint");
    client
        .release_savepoint("vtb_sp1".to_string())
        .await
        .expect("release_savepoint");
    client
        .commit_transaction()
        .await
        .expect("commit_transaction");

    client
        .begin_transaction(Some("SNAPSHOT".to_string()))
        .await
        .expect("begin_transaction snapshot");
    client
        .rollback_transaction()
        .await
        .expect("rollback_transaction");

    client.close().await.expect("close");
    assert!(client.is_closed());
}

#[tokio::test]
async fn tds_attention_cancellation_and_server_error_hierarchy_mapping() {
    let attn_counter = Arc::new(AtomicUsize::new(0));
    let addr = spawn_mock_tds_server(None, Arc::clone(&attn_counter)).await;
    let mut cfg = loopback_config(addr);
    cfg.query_timeout = Duration::from_millis(120);
    let client = Arc::new(MssqlClient::new(cfg).expect("client"));

    // 1. Timeout triggers out-of-band TDS ATTENTION (0x06) packet and returns QueryCancelled
    let timeout_err = client
        .execute("WAITFOR DELAY '00:00:10'".to_string(), vec![])
        .await
        .expect_err("WAITFOR DELAY must time out and send ATTENTION");
    assert_eq!(timeout_err.kind, MssqlErrorKind::QueryCancelled);
    assert!(attn_counter.load(Ordering::SeqCst) >= 1);

    // 2. Connection remains reusable after ATTENTION drain!
    let error_cases = [
        (2627u32, MssqlErrorKind::Integrity, "IntegrityError"),
        (
            1205u32,
            MssqlErrorKind::SerializationFailure,
            "SerializationFailure",
        ),
        (208u32, MssqlErrorKind::Programming, "ProgrammingError"),
        (8115u32, MssqlErrorKind::Data, "DataError"),
        (
            1222u32,
            MssqlErrorKind::QueryCancelled,
            "QueryCancelledError",
        ),
    ];

    Python::initialize();
    for (code, expected_kind, expected_py_class) in error_cases {
        let err = client
            .execute(format!("SELECT RAISE_ERROR_{code}"), vec![])
            .await
            .expect_err("expected server TokenError");
        assert_eq!(err.kind, expected_kind);
        assert_eq!(err.vendor_code, Some(code as i64));

        Python::attach(|py| {
            let py_err = mssql_error_to_py(py, err);
            let exc_val = py_err.value(py);
            let cls_name = exc_val.get_type().name().expect("type name").to_string();
            assert_eq!(cls_name, expected_py_class);
        });
    }
}

#[tokio::test]
async fn tds_vtbarrow_shm_export_and_bulk_load_roundtrip() {
    use _mssql::broker::{DIALECT_TAG_MSSQL, MssqlBrokerBackend};
    use _mssql::bulk_load::decode_shm_arrow_batches;

    let attn_counter = Arc::new(AtomicUsize::new(0));
    let addr = spawn_mock_tds_server(None, Arc::clone(&attn_counter)).await;
    let client = MssqlClient::new(loopback_config(addr)).expect("client");

    // Verify MssqlBrokerBackend dialect contract
    let backend: &MssqlBrokerBackend = client.broker();
    assert_eq!(backend.dialect_tag(), DIALECT_TAG_MSSQL);
    assert_eq!(backend.dialect(), "mssql");

    // 1. select_to_arrow exports result set into a VTBARROW POSIX SHM segment
    let (shm_name, byte_len, row_count, region) = client
        .select_to_arrow("SELECT id, name FROM users".to_string(), vec![])
        .await
        .expect("select_to_arrow");
    assert_eq!(row_count, 2);
    assert!(byte_len > 64);

    // 2. load_from_arrow_shm reads the VTBARROW SHM segment and streams it over TDS 0x07 BulkLoad
    let decoded_batches = decode_shm_arrow_batches(&shm_name).expect("decode_shm_arrow_batches");
    assert_eq!(decoded_batches.len(), 1);
    assert_eq!(decoded_batches[0].num_rows(), 2);

    let loaded = client
        .load_from_arrow_shm("bulk_target".to_string(), shm_name.clone(), None)
        .await
        .expect("load_from_arrow_shm");
    assert_eq!(loaded, 2);
    drop(region);

    // 3. stream_arrow_batches chunks rows into VTBARROW SHM segments
    let chunks = client
        .stream_arrow_batches("SELECT id, name FROM users".to_string(), vec![], 1)
        .await
        .expect("stream_arrow_batches");
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].2, 1);
    assert_eq!(chunks[1].2, 1);
}

#[tokio::test]
async fn tds_fedauth_aad_token_login7_wire_and_set_aad_token_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let captured_logins = Arc::new(tokio::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let captured_clone = Arc::clone(&captured_logins);

    let server_task = tokio::spawn(async move {
        for conn_idx in 0..2 {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let (pkt_ty, _prelogin_payload) =
                read_tds_message(&mut stream).await.expect("read prelogin");
            assert_eq!(pkt_ty, PKT_PRELOGIN);
            stream
                .write_all(&build_prelogin_response_not_supported())
                .await
                .expect("write prelogin resp");

            let (pkt_ty, login_payload) = read_tds_message(&mut stream).await.expect("read login7");
            assert_eq!(pkt_ty, PKT_LOGIN7);
            captured_clone.lock().await.push(login_payload);

            let mut login_resp = Vec::new();
            append_loginack_token(&mut login_resp);
            append_done_token(&mut login_resp, 0xFD, 0, 0, 0);
            stream
                .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &login_resp))
                .await
                .expect("write loginack");

            if let Ok((cmd_ty, _)) = read_tds_message(&mut stream).await {
                assert_eq!(cmd_ty, PKT_RPC);
                let mut resp = Vec::new();
                append_two_col_resultset(&mut resp, &[(1, "ok")]);
                append_done_token(&mut resp, 0xFF, 0x0010, 0xC1, 1);
                append_done_token(&mut resp, 0xFE, 0, 0, 0);
                let _ = stream
                    .write_all(&wrap_tds_packet(PKT_TABULAR_RESULT, &resp))
                    .await;
            }
            if conn_idx == 0 {
                drop(stream);
            }
        }
    });

    let mut cfg = loopback_config(addr);
    cfg.password = "SqlServerSecretShouldNotBeSent".to_string();
    cfg.aad_token = Some("test-token-placeholder-aad-1".to_string());
    cfg.validate().expect("valid aad_token config");

    let dbg_str = format!("{cfg:?}");
    assert!(dbg_str.contains("[REDACTED]"));
    assert!(!dbg_str.contains("test-token-placeholder-aad-1"));
    assert!(!dbg_str.contains("SqlServerSecretShouldNotBeSent"));

    let tib_dbg = format!(
        "{:?}",
        cfg.to_tiberius_config("127.0.0.1", addr.port(), false)
    );
    assert!(tib_dbg.contains("AADToken"));
    assert!(!tib_dbg.contains("test-token-placeholder-aad-1"));

    let mut blank_cfg = cfg.clone();
    blank_cfg.aad_token = Some("   ".to_string());
    assert!(blank_cfg.validate().is_err());

    let client = MssqlClient::new(cfg).expect("client");
    let rows = client
        .query("SELECT 1".to_string(), vec![])
        .await
        .expect("first query with aad_token_1");
    assert_eq!(rows.rows.len(), 1);

    client
        .config()
        .set_aad_token(Some("   ".to_string()))
        .expect_err("blank aad_token must be rejected");
    client
        .config()
        .set_aad_token(Some("test-token-placeholder-aad-2".to_string()))
        .expect("set_aad_token updates stored token");

    let _ = client.query("SELECT 1".to_string(), vec![]).await;
    let _ = client.query("SELECT 1".to_string(), vec![]).await;

    let logins = captured_logins.lock().await;
    assert_eq!(logins.len(), 2);
    let first_utf16: Vec<u8> = "test-token-placeholder-aad-1"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    let second_utf16: Vec<u8> = "test-token-placeholder-aad-2"
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect();
    assert!(
        logins[0]
            .windows(first_utf16.len())
            .any(|w| w == first_utf16.as_slice()),
        "first LOGIN7 must carry FEDAUTH AAD token 1"
    );
    assert!(
        logins[1]
            .windows(second_utf16.len())
            .any(|w| w == second_utf16.as_slice()),
        "second LOGIN7 after set_aad_token must carry FEDAUTH AAD token 2"
    );

    client
        .config()
        .set_aad_token(None)
        .expect("clearing aad_token restores SQL auth");
    let sql_tib_dbg = format!(
        "{:?}",
        client
            .config()
            .to_tiberius_config("127.0.0.1", addr.port(), false)
    );
    assert!(sql_tib_dbg.contains("SqlServer"));
    assert!(!sql_tib_dbg.contains("AADToken"));

    server_task.abort();
}
