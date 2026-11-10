//! In-process MySQL v10 TCP wire server (`MySQLWireTestServer`) for loopback protocol,
//! prepared-statement (`COM_STMT_PREPARE`/`EXECUTE`/`CLOSE`), binary codec, transaction/savepoint,
//! `KILL QUERY` cancellation, and driver conformance testing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;

use mysql_async::consts::{CapabilityFlags, ColumnFlags, ColumnType, StatusFlags};
use pyo3::prelude::*;
use rsa::Oaep;
use rsa::RsaPrivateKey;
use rsa::pkcs8::{EncodePublicKey, LineEnding};
use sha1::Sha1;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

use crate::client::DecodedCell;
use crate::codecs::{
    decode_com_stmt_execute, encode_binary_row_packet, encode_column_definition_41,
    encode_lenenc_int, encode_text_row_packet,
};
use crate::errors::MySQLError;
use crate::protocol::{
    HandshakeV10, compute_caching_sha2_scramble_bytes, compute_mysql_native_scramble_bytes,
    encode_eof_packet, encode_err_packet, encode_mysql_packet, encode_ok_packet,
    split_sql_statements,
};

struct Mutex<T>(std::sync::Mutex<T>);

impl<T> Mutex<T> {
    fn new(val: T) -> Self {
        Self(std::sync::Mutex::new(val))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, T> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }
}

const SERVER_CAPABILITIES: u32 = CapabilityFlags::CLIENT_LONG_PASSWORD.bits()
    | CapabilityFlags::CLIENT_FOUND_ROWS.bits()
    | CapabilityFlags::CLIENT_LONG_FLAG.bits()
    | CapabilityFlags::CLIENT_CONNECT_WITH_DB.bits()
    | CapabilityFlags::CLIENT_PROTOCOL_41.bits()
    | CapabilityFlags::CLIENT_TRANSACTIONS.bits()
    | CapabilityFlags::CLIENT_SECURE_CONNECTION.bits()
    | CapabilityFlags::CLIENT_MULTI_STATEMENTS.bits()
    | CapabilityFlags::CLIENT_MULTI_RESULTS.bits()
    | CapabilityFlags::CLIENT_PS_MULTI_RESULTS.bits()
    | CapabilityFlags::CLIENT_PLUGIN_AUTH.bits()
    | CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.bits();

#[derive(Debug, Clone)]
struct ColumnDef {
    name: String,
    col_type: u8,
    col_flags: u16,
    not_null: bool,
    is_primary_key: bool,
    is_auto_increment: bool,
    is_boolean: bool,
}

#[derive(Debug, Clone)]
struct TableDef {
    name: String,
    columns: Vec<ColumnDef>,
    rows: Vec<Vec<DecodedCell>>,
    next_auto_inc: u64,
}

#[derive(Debug, Clone, Default)]
struct DatabaseState {
    tables: HashMap<String, TableDef>,
}

#[derive(Debug, Clone)]
struct PreparedStmtEntry {
    sql: String,
    num_params: usize,
    bound_types: Vec<(u8, bool)>,
}

struct ServerSharedState {
    db: Mutex<DatabaseState>,
    active_cancels: Mutex<HashMap<u32, Arc<Notify>>>,
    next_conn_id: AtomicU32,
    prepare_count: AtomicUsize,
    execute_count: AtomicUsize,
    close_stmt_count: AtomicUsize,
    kill_query_count: AtomicUsize,
    query_count: AtomicUsize,
    auth_plugin: String,
    require_rsa_full_auth: bool,
    expected_password: String,
    rsa_key: Option<Arc<RsaPrivateKey>>,
    rsa_pub_pem: Option<String>,
    shutdown: AtomicBool,
    shutdown_notify: Notify,
}

/// Real TCP MySQL v10 wire server speaking `HandshakeV10`, `caching_sha2_password` /
/// `mysql_native_password`, `COM_QUERY`, `COM_STMT_PREPARE` / `COM_STMT_EXECUTE` /
/// `COM_STMT_CLOSE`, binary result sets, transactions/savepoints, and `KILL QUERY <conn_id>`.
#[pyclass(module = "vertebra._mysql", frozen, immutable_type)]
pub struct MySQLWireTestServer {
    addr: std::net::SocketAddr,
    shared: Arc<ServerSharedState>,
}

impl MySQLWireTestServer {
    pub fn start_default() -> Result<Self, MySQLError> {
        Self::start_with_options("caching_sha2_password", false, "secret")
    }

    pub fn start_with_options(
        auth_plugin: &str,
        require_rsa_full_auth: bool,
        expected_password: &str,
    ) -> Result<Self, MySQLError> {
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| {
            MySQLError::connection(format!("Failed to bind loopback TCP port: {e}"))
        })?;
        std_listener.set_nonblocking(true).map_err(|e| {
            MySQLError::connection(format!("Failed to set nonblocking listener: {e}"))
        })?;
        let addr = std_listener
            .local_addr()
            .map_err(|e| MySQLError::connection(format!("Failed to read local_addr: {e}")))?;

        let (rsa_key, rsa_pub_pem) = if require_rsa_full_auth {
            let mut rng = rsa::rand_core::OsRng;
            let priv_key = RsaPrivateKey::new(&mut rng, 2048)
                .map_err(|e| MySQLError::connection(format!("RSA keygen failed: {e}")))?;
            let pub_pem = priv_key
                .to_public_key()
                .to_public_key_pem(LineEnding::LF)
                .map_err(|e| MySQLError::connection(format!("RSA PEM encode failed: {e}")))?;
            (Some(Arc::new(priv_key)), Some(pub_pem))
        } else {
            (None, None)
        };

        let shared = Arc::new(ServerSharedState {
            db: Mutex::new(DatabaseState::default()),
            active_cancels: Mutex::new(HashMap::new()),
            next_conn_id: AtomicU32::new(100),
            prepare_count: AtomicUsize::new(0),
            execute_count: AtomicUsize::new(0),
            close_stmt_count: AtomicUsize::new(0),
            kill_query_count: AtomicUsize::new(0),
            query_count: AtomicUsize::new(0),
            auth_plugin: auth_plugin.to_string(),
            require_rsa_full_auth,
            expected_password: expected_password.to_string(),
            rsa_key,
            rsa_pub_pem,
            shutdown: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
        });

        let shared_bg = Arc::clone(&shared);
        let rt = vertebra_db::py_async::db_runtime_handle();
        rt.spawn(async move {
            let Ok(listener) = TcpListener::from_std(std_listener) else {
                return;
            };
            loop {
                tokio::select! {
                    _ = shared_bg.shutdown_notify.notified() => {
                        break;
                    }
                    accept_res = listener.accept() => {
                        let Ok((stream, _)) = accept_res else {
                            break;
                        };
                        if shared_bg.shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        let conn_shared = Arc::clone(&shared_bg);
                        tokio::spawn(async move {
                            let _ = handle_connection(stream, conn_shared).await;
                        });
                    }
                }
            }
        });

        Ok(Self { addr, shared })
    }
}

#[pymethods]
impl MySQLWireTestServer {
    #[new]
    #[pyo3(signature = (
        auth_plugin = "caching_sha2_password",
        require_rsa_full_auth = false,
        expected_password = "secret",
    ))]
    pub fn py_new(
        py: Python<'_>,
        auth_plugin: &str,
        require_rsa_full_auth: bool,
        expected_password: &str,
    ) -> PyResult<Self> {
        let server = py.detach(|| {
            Self::start_with_options(auth_plugin, require_rsa_full_auth, expected_password)
        })?;
        Ok(server)
    }

    #[getter]
    pub fn host(&self) -> &str {
        "127.0.0.1"
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    #[getter]
    pub fn dsn(&self) -> String {
        format!(
            "mysql://vtb:{}@127.0.0.1:{}/vtb?ssl_mode=disable",
            self.shared.expected_password,
            self.addr.port()
        )
    }

    #[getter]
    pub fn prepare_count(&self) -> usize {
        self.shared.prepare_count.load(Ordering::Acquire)
    }

    #[getter]
    pub fn execute_count(&self) -> usize {
        self.shared.execute_count.load(Ordering::Acquire)
    }

    #[getter]
    pub fn close_stmt_count(&self) -> usize {
        self.shared.close_stmt_count.load(Ordering::Acquire)
    }

    #[getter]
    pub fn kill_query_count(&self) -> usize {
        self.shared.kill_query_count.load(Ordering::Acquire)
    }

    #[getter]
    pub fn query_count(&self) -> usize {
        self.shared.query_count.load(Ordering::Acquire)
    }

    pub fn close(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        self.shared.shutdown_notify.notify_waiters();
    }
}

impl Drop for MySQLWireTestServer {
    fn drop(&mut self) {
        self.close();
    }
}

async fn read_packet(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), std::io::Error> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;
    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
    let seq = header[3];
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((seq, payload))
}

async fn write_packet(
    stream: &mut TcpStream,
    seq: u8,
    payload: &[u8],
) -> Result<(), std::io::Error> {
    let pkt = encode_mysql_packet(seq, payload);
    stream.write_all(&pkt).await?;
    stream.flush().await
}

async fn handle_connection(
    mut stream: TcpStream,
    shared: Arc<ServerSharedState>,
) -> Result<(), std::io::Error> {
    let _ = stream.set_nodelay(true);
    let conn_id = shared.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let nonce: [u8; 20] = *b"12345678901234567890";

    let handshake = HandshakeV10 {
        protocol_version: 10,
        server_version: "8.4.0-Vertebra".to_string(),
        connection_id: conn_id,
        nonce,
        capability_flags: SERVER_CAPABILITIES,
        character_set: 45,
        status_flags: StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits(),
        auth_plugin_name: shared.auth_plugin.clone(),
    };
    write_packet(&mut stream, 0, &handshake.encode_payload()).await?;

    let (mut seq, resp) = read_packet(&mut stream).await?;
    if resp.len() < 32 {
        return Ok(());
    }
    let client_caps = u32::from_le_bytes([resp[0], resp[1], resp[2], resp[3]]);
    let mut pos = 32;
    let user_end = resp[pos..]
        .iter()
        .position(|&b| b == 0)
        .map(|i| pos + i)
        .unwrap_or(resp.len());
    pos = (user_end + 1).min(resp.len());

    let auth_data =
        if (client_caps & CapabilityFlags::CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.bits()) != 0 {
            let mut slice = &resp[pos..];
            let v = crate::codecs::decode_lenenc_bytes(&mut slice).unwrap_or_default();
            pos = resp.len() - slice.len();
            v
        } else if (client_caps & CapabilityFlags::CLIENT_SECURE_CONNECTION.bits()) != 0 {
            if pos < resp.len() {
                let len = resp[pos] as usize;
                pos += 1;
                let end = (pos + len).min(resp.len());
                let v = resp[pos..end].to_vec();
                pos = end;
                v
            } else {
                Vec::new()
            }
        } else {
            let end = resp[pos..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| pos + i)
                .unwrap_or(resp.len());
            let v = resp[pos..end].to_vec();
            pos = (end + 1).min(resp.len());
            v
        };

    if (client_caps & CapabilityFlags::CLIENT_CONNECT_WITH_DB.bits()) != 0
        && pos < resp.len()
        && let Some(db_end) = resp[pos..].iter().position(|&b| b == 0)
    {
        pos += db_end + 1;
    }
    let client_plugin =
        if (client_caps & CapabilityFlags::CLIENT_PLUGIN_AUTH.bits()) != 0 && pos < resp.len() {
            let end = resp[pos..]
                .iter()
                .position(|&b| b == 0)
                .map(|i| pos + i)
                .unwrap_or(resp.len());
            String::from_utf8_lossy(&resp[pos..end]).into_owned()
        } else {
            shared.auth_plugin.clone()
        };

    let expected_pw = shared.expected_password.as_bytes();
    if shared.auth_plugin == "mysql_native_password" {
        let final_auth = if client_plugin != "mysql_native_password" {
            seq = seq.wrapping_add(1);
            let mut switch_pkt = Vec::with_capacity(32 + nonce.len());
            switch_pkt.push(0xFE);
            switch_pkt.extend_from_slice(b"mysql_native_password\0");
            switch_pkt.extend_from_slice(&nonce);
            switch_pkt.push(0);
            write_packet(&mut stream, seq, &switch_pkt).await?;
            let (s_next, switch_resp) = read_packet(&mut stream).await?;
            seq = s_next;
            switch_resp
        } else {
            auth_data
        };
        let expected_scramble =
            compute_mysql_native_scramble_bytes(expected_pw, &nonce).unwrap_or([0u8; 20]);
        let ok = (expected_pw.is_empty() && final_auth.is_empty())
            || final_auth.as_slice() == expected_scramble.as_slice();
        seq = seq.wrapping_add(1);
        if !ok {
            let err = encode_err_packet(1045, "28000", "Access denied for user");
            write_packet(&mut stream, seq, &err).await?;
            return Ok(());
        }
        let ok_pkt = encode_ok_packet(0, 0, StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits(), 0);
        write_packet(&mut stream, seq, &ok_pkt).await?;
    } else if shared.require_rsa_full_auth {
        seq = seq.wrapping_add(1);
        write_packet(&mut stream, seq, &[0x01, 0x04]).await?;
        let (s_req, req_pkt) = read_packet(&mut stream).await?;
        seq = s_req;
        let encrypted_pw = if req_pkt == [0x02] {
            seq = seq.wrapping_add(1);
            let pem = shared.rsa_pub_pem.as_deref().unwrap_or("");
            let mut key_pkt = Vec::with_capacity(1 + pem.len());
            key_pkt.push(0x01);
            key_pkt.extend_from_slice(pem.as_bytes());
            write_packet(&mut stream, seq, &key_pkt).await?;
            let (s_enc, enc_pkt) = read_packet(&mut stream).await?;
            seq = s_enc;
            enc_pkt
        } else {
            req_pkt
        };
        let mut auth_ok = false;
        if let Some(ref priv_key) = shared.rsa_key
            && let Ok(mut decrypted) = priv_key.decrypt(Oaep::new::<Sha1>(), &encrypted_pw)
        {
            for (i, b) in decrypted.iter_mut().enumerate() {
                *b ^= nonce[i % nonce.len()];
            }
            if let Some(nul_pos) = decrypted.iter().position(|&b| b == 0) {
                decrypted.truncate(nul_pos);
            }
            auth_ok = decrypted.as_slice() == expected_pw;
        }
        seq = seq.wrapping_add(1);
        if !auth_ok {
            let err = encode_err_packet(1045, "28000", "RSA full authentication failed");
            write_packet(&mut stream, seq, &err).await?;
            return Ok(());
        }
        let ok_pkt = encode_ok_packet(0, 0, StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits(), 0);
        write_packet(&mut stream, seq, &ok_pkt).await?;
    } else {
        let expected_scramble =
            compute_caching_sha2_scramble_bytes(expected_pw, &nonce).unwrap_or([0u8; 32]);
        let ok = (expected_pw.is_empty() && auth_data.is_empty())
            || auth_data.as_slice() == expected_scramble.as_slice();
        if !expected_pw.is_empty() {
            seq = seq.wrapping_add(1);
            write_packet(&mut stream, seq, &[0x01, 0x03]).await?;
        }
        seq = seq.wrapping_add(1);
        if !ok {
            let err = encode_err_packet(1045, "28000", "Access denied for user");
            write_packet(&mut stream, seq, &err).await?;
            return Ok(());
        }
        let ok_pkt = encode_ok_packet(0, 0, StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits(), 0);
        write_packet(&mut stream, seq, &ok_pkt).await?;
    }

    let cancel_notify = Arc::new(Notify::new());
    shared
        .active_cancels
        .lock()
        .insert(conn_id, Arc::clone(&cancel_notify));

    let mut conn_state = ConnSessionState {
        conn_id,
        in_tx: false,
        tx_db: None,
        savepoints: Vec::new(),
        next_stmt_id: 1,
        prepared_stmts: HashMap::new(),
    };

    loop {
        let Ok((cmd_seq, payload)) = read_packet(&mut stream).await else {
            break;
        };
        if payload.is_empty() {
            break;
        }
        let mut out_seq = cmd_seq.wrapping_add(1);
        let cmd = payload[0];

        match cmd {
            0x01 => {
                // COM_QUIT
                break;
            }
            0x02 | 0x0E | 0x1A | 0x1F => {
                // COM_INIT_DB, COM_PING, COM_STMT_RESET, COM_RESET_CONNECTION
                if cmd == 0x1F {
                    conn_state.in_tx = false;
                    conn_state.tx_db = None;
                    conn_state.savepoints.clear();
                    conn_state.prepared_stmts.clear();
                }
                let ok_pkt = encode_ok_packet(0, 0, conn_state.status_flags(), 0);
                write_packet(&mut stream, out_seq, &ok_pkt).await?;
            }
            0x19 => {
                // COM_STMT_CLOSE (no response packet)
                if payload.len() >= 5 {
                    let stmt_id =
                        u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                    conn_state.prepared_stmts.remove(&stmt_id);
                    shared.close_stmt_count.fetch_add(1, Ordering::Release);
                }
            }
            0x16 => {
                // COM_STMT_PREPARE
                let sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                shared.prepare_count.fetch_add(1, Ordering::Release);
                match prepare_sql_statement(&sql, &shared, &conn_state) {
                    Ok((num_params, columns)) => {
                        let stmt_id = conn_state.next_stmt_id;
                        conn_state.next_stmt_id += 1;
                        conn_state.prepared_stmts.insert(
                            stmt_id,
                            PreparedStmtEntry {
                                sql,
                                num_params,
                                bound_types: Vec::new(),
                            },
                        );

                        let mut prep_ok = Vec::with_capacity(12);
                        prep_ok.push(0x00);
                        prep_ok.extend_from_slice(&stmt_id.to_le_bytes());
                        prep_ok.extend_from_slice(&(columns.len() as u16).to_le_bytes());
                        prep_ok.extend_from_slice(&(num_params as u16).to_le_bytes());
                        prep_ok.push(0x00);
                        prep_ok.extend_from_slice(&0u16.to_le_bytes());
                        write_packet(&mut stream, out_seq, &prep_ok).await?;
                        out_seq = out_seq.wrapping_add(1);

                        if num_params > 0 {
                            for _ in 0..num_params {
                                let col_pkt = encode_column_definition_41(
                                    "vtb",
                                    "",
                                    "?",
                                    ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                                    0,
                                    0,
                                );
                                write_packet(&mut stream, out_seq, &col_pkt).await?;
                                out_seq = out_seq.wrapping_add(1);
                            }
                            let eof = encode_eof_packet(0, conn_state.status_flags());
                            write_packet(&mut stream, out_seq, &eof).await?;
                            out_seq = out_seq.wrapping_add(1);
                        }

                        if !columns.is_empty() {
                            for (name, col_type, col_flags) in &columns {
                                let col_pkt = encode_column_definition_41(
                                    "vtb", "vtb", name, *col_type, *col_flags, 0,
                                );
                                write_packet(&mut stream, out_seq, &col_pkt).await?;
                                out_seq = out_seq.wrapping_add(1);
                            }
                            let eof = encode_eof_packet(0, conn_state.status_flags());
                            write_packet(&mut stream, out_seq, &eof).await?;
                        }
                    }
                    Err((errno, sqlstate, msg)) => {
                        let err = encode_err_packet(errno, &sqlstate, &msg);
                        write_packet(&mut stream, out_seq, &err).await?;
                    }
                }
            }
            0x17 => {
                // COM_STMT_EXECUTE
                shared.execute_count.fetch_add(1, Ordering::Release);
                if payload.len() < 10 {
                    let err = encode_err_packet(1210, "HY000", "Malformed COM_STMT_EXECUTE packet");
                    write_packet(&mut stream, out_seq, &err).await?;
                    continue;
                }
                let stmt_id = u32::from_le_bytes([payload[1], payload[2], payload[3], payload[4]]);
                let Some(stmt_entry) = conn_state.prepared_stmts.get_mut(&stmt_id) else {
                    let err =
                        encode_err_packet(1243, "HY000", "Unknown prepared statement handler");
                    write_packet(&mut stream, out_seq, &err).await?;
                    continue;
                };
                let num_params = stmt_entry.num_params;
                let sql = stmt_entry.sql.clone();
                let decoded_res =
                    decode_com_stmt_execute(&payload, num_params, &mut stmt_entry.bound_types);
                let (_, params) = match decoded_res {
                    Ok(v) => v,
                    Err(e) => {
                        let err = encode_err_packet(1210, "HY000", &e.message);
                        write_packet(&mut stream, out_seq, &err).await?;
                        continue;
                    }
                };

                match execute_sql_statement(&sql, &params, &shared, &mut conn_state, &cancel_notify)
                    .await
                {
                    Ok(mut result_sets) => {
                        if result_sets.is_empty() {
                            let ok_pkt = encode_ok_packet(0, 0, conn_state.status_flags(), 0);
                            write_packet(&mut stream, out_seq, &ok_pkt).await?;
                        } else {
                            let total_sets = result_sets.len();
                            for (set_idx, set) in result_sets.drain(..).enumerate() {
                                let more_flag = if set_idx + 1 < total_sets {
                                    StatusFlags::SERVER_MORE_RESULTS_EXISTS.bits()
                                } else {
                                    0
                                };
                                let status = conn_state.status_flags() | more_flag;
                                match set {
                                    ExecOutcome::Ok {
                                        affected_rows,
                                        last_insert_id,
                                    } => {
                                        let ok_pkt = encode_ok_packet(
                                            affected_rows,
                                            last_insert_id,
                                            status,
                                            0,
                                        );
                                        write_packet(&mut stream, out_seq, &ok_pkt).await?;
                                        out_seq = out_seq.wrapping_add(1);
                                    }
                                    ExecOutcome::Rows { columns, rows } => {
                                        let mut col_cnt = Vec::new();
                                        encode_lenenc_int(columns.len() as u64, &mut col_cnt);
                                        write_packet(&mut stream, out_seq, &col_cnt).await?;
                                        out_seq = out_seq.wrapping_add(1);

                                        let mut col_types = Vec::with_capacity(columns.len());
                                        for (name, ct, cf) in &columns {
                                            col_types.push((name.clone(), *ct));
                                            let col_pkt = encode_column_definition_41(
                                                "vtb", "vtb", name, *ct, *cf, 0,
                                            );
                                            write_packet(&mut stream, out_seq, &col_pkt).await?;
                                            out_seq = out_seq.wrapping_add(1);
                                        }
                                        let eof1 = encode_eof_packet(0, conn_state.status_flags());
                                        write_packet(&mut stream, out_seq, &eof1).await?;
                                        out_seq = out_seq.wrapping_add(1);

                                        for row in &rows {
                                            let row_pkt = encode_binary_row_packet(row, &col_types);
                                            write_packet(&mut stream, out_seq, &row_pkt).await?;
                                            out_seq = out_seq.wrapping_add(1);
                                        }
                                        let eof2 = encode_eof_packet(0, status);
                                        write_packet(&mut stream, out_seq, &eof2).await?;
                                        out_seq = out_seq.wrapping_add(1);
                                    }
                                }
                            }
                        }
                    }
                    Err((errno, sqlstate, msg)) => {
                        let err = encode_err_packet(errno, &sqlstate, &msg);
                        write_packet(&mut stream, out_seq, &err).await?;
                    }
                }
            }
            0x03 => {
                // COM_QUERY
                let raw_sql = String::from_utf8_lossy(&payload[1..]).into_owned();
                shared.query_count.fetch_add(1, Ordering::Release);
                let mut stmts = split_sql_statements(&raw_sql);
                if stmts.is_empty() {
                    stmts.push(raw_sql);
                }

                let mut all_outcomes = Vec::new();
                let mut err_packet: Option<(u16, String, String)> = None;
                for stmt in &stmts {
                    match execute_sql_statement(stmt, &[], &shared, &mut conn_state, &cancel_notify)
                        .await
                    {
                        Ok(outcomes) => all_outcomes.extend(outcomes),
                        Err(err) => {
                            err_packet = Some(err);
                            break;
                        }
                    }
                }

                if let Some((errno, sqlstate, msg)) = err_packet {
                    let err = encode_err_packet(errno, &sqlstate, &msg);
                    write_packet(&mut stream, out_seq, &err).await?;
                } else if all_outcomes.is_empty() {
                    let ok_pkt = encode_ok_packet(0, 0, conn_state.status_flags(), 0);
                    write_packet(&mut stream, out_seq, &ok_pkt).await?;
                } else {
                    let total_sets = all_outcomes.len();
                    for (set_idx, outcome) in all_outcomes.into_iter().enumerate() {
                        let more_flag = if set_idx + 1 < total_sets {
                            StatusFlags::SERVER_MORE_RESULTS_EXISTS.bits()
                        } else {
                            0
                        };
                        let status = conn_state.status_flags() | more_flag;
                        match outcome {
                            ExecOutcome::Ok {
                                affected_rows,
                                last_insert_id,
                            } => {
                                let ok_pkt =
                                    encode_ok_packet(affected_rows, last_insert_id, status, 0);
                                write_packet(&mut stream, out_seq, &ok_pkt).await?;
                                out_seq = out_seq.wrapping_add(1);
                            }
                            ExecOutcome::Rows { columns, rows } => {
                                let mut col_cnt = Vec::new();
                                encode_lenenc_int(columns.len() as u64, &mut col_cnt);
                                write_packet(&mut stream, out_seq, &col_cnt).await?;
                                out_seq = out_seq.wrapping_add(1);

                                for (name, ct, cf) in &columns {
                                    let col_pkt = encode_column_definition_41(
                                        "vtb", "vtb", name, *ct, *cf, 0,
                                    );
                                    write_packet(&mut stream, out_seq, &col_pkt).await?;
                                    out_seq = out_seq.wrapping_add(1);
                                }
                                let eof1 = encode_eof_packet(0, conn_state.status_flags());
                                write_packet(&mut stream, out_seq, &eof1).await?;
                                out_seq = out_seq.wrapping_add(1);

                                for row in &rows {
                                    let row_pkt = encode_text_row_packet(row);
                                    write_packet(&mut stream, out_seq, &row_pkt).await?;
                                    out_seq = out_seq.wrapping_add(1);
                                }
                                let eof2 = encode_eof_packet(0, status);
                                write_packet(&mut stream, out_seq, &eof2).await?;
                                out_seq = out_seq.wrapping_add(1);
                            }
                        }
                    }
                }
            }
            _ => {
                let err = encode_err_packet(1047, "08S01", "Unknown command");
                write_packet(&mut stream, out_seq, &err).await?;
            }
        }
    }

    shared.active_cancels.lock().remove(&conn_id);
    Ok(())
}

struct ConnSessionState {
    conn_id: u32,
    in_tx: bool,
    tx_db: Option<DatabaseState>,
    savepoints: Vec<(String, DatabaseState)>,
    next_stmt_id: u32,
    prepared_stmts: HashMap<u32, PreparedStmtEntry>,
}

impl ConnSessionState {
    fn status_flags(&self) -> u16 {
        let mut flags = StatusFlags::SERVER_STATUS_AUTOCOMMIT.bits();
        if self.in_tx {
            flags |= StatusFlags::SERVER_STATUS_IN_TRANS.bits();
        }
        flags
    }
}

enum ExecOutcome {
    Ok {
        affected_rows: u64,
        last_insert_id: u64,
    },
    Rows {
        columns: Vec<(String, u8, u16)>,
        rows: Vec<Vec<DecodedCell>>,
    },
}

type WireColumnMeta = (String, u8, u16);
type WireErrorTuple = (u16, String, String);
type SelectResultPair = (Vec<WireColumnMeta>, Vec<Vec<DecodedCell>>);

fn strip_sql_comments(sql: &str) -> String {
    let mut s = sql.trim();
    if let Some(start) = s.find("/*")
        && let Some(end) = s[start + 2..].find("*/")
    {
        let mut combined = String::with_capacity(s.len());
        combined.push_str(&s[..start]);
        combined.push(' ');
        combined.push_str(&s[start + 2 + end + 2..]);
        return strip_sql_comments(&combined);
    }
    s = s.trim();
    if let Some(stripped) = s.strip_suffix(';') {
        stripped.trim().to_string()
    } else {
        s.to_string()
    }
}

fn count_placeholders(sql: &str) -> usize {
    let bytes = sql.as_bytes();
    let mut idx = 0;
    let mut count = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'\'' || b == b'"' || b == b'`' {
            let quote = b;
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'\\' && quote != b'`' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if bytes[idx] == quote {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == quote {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            continue;
        }
        if b == b'?' {
            count += 1;
        }
        idx += 1;
    }
    count
}

fn check_synthetic_error(upper: &str) -> Option<(u16, String, String)> {
    if upper.contains("ERROR_1062") {
        return Some((
            1062,
            "23000".to_string(),
            "Duplicate entry '1' for key 'PRIMARY'".to_string(),
        ));
    }
    if upper.contains("ERROR_1452") {
        return Some((
            1452,
            "23000".to_string(),
            "Cannot add or update a child row: a foreign key constraint fails".to_string(),
        ));
    }
    if upper.contains("ERROR_1213") {
        return Some((
            1213,
            "40001".to_string(),
            "Deadlock found when trying to get lock; try restarting transaction".to_string(),
        ));
    }
    if upper.contains("ERROR_1205") {
        return Some((
            1205,
            "HY000".to_string(),
            "Lock wait timeout exceeded; try restarting transaction".to_string(),
        ));
    }
    if upper.contains("ERROR_1317") {
        return Some((
            1317,
            "70100".to_string(),
            "Query execution was interrupted".to_string(),
        ));
    }
    if upper.contains("ERROR_3024") {
        return Some((
            3024,
            "HY000".to_string(),
            "Query execution was interrupted, maximum statement execution time exceeded"
                .to_string(),
        ));
    }
    if upper.contains("ERROR_1406") {
        return Some((
            1406,
            "22001".to_string(),
            "Data too long for column 'val' at row 1".to_string(),
        ));
    }
    if upper.contains("ERROR_1264") {
        return Some((
            1264,
            "22003".to_string(),
            "Out of range value for column 'val' at row 1".to_string(),
        ));
    }
    if upper.contains("ERROR_1064") {
        return Some((
            1064,
            "42000".to_string(),
            "You have an error in your SQL syntax".to_string(),
        ));
    }
    None
}

fn prepare_sql_statement(
    raw_sql: &str,
    shared: &ServerSharedState,
    conn_state: &ConnSessionState,
) -> Result<(usize, Vec<WireColumnMeta>), WireErrorTuple> {
    let cleaned = strip_sql_comments(raw_sql);
    let upper = cleaned.to_ascii_uppercase();
    if let Some(err) = check_synthetic_error(&upper) {
        return Err(err);
    }
    let num_params = count_placeholders(&cleaned);

    if upper.starts_with("SELECT ") {
        let cols = infer_select_columns(&cleaned, shared, conn_state)?;
        return Ok((num_params, cols));
    }
    if upper.starts_with("INSERT INTO ") && find_top_level_keyword(&upper, " RETURNING ").is_some()
    {
        return Ok((
            num_params,
            vec![("id".to_string(), ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
        ));
    }
    Ok((num_params, Vec::new()))
}

fn infer_select_columns(
    cleaned_sql: &str,
    shared: &ServerSharedState,
    conn_state: &ConnSessionState,
) -> Result<Vec<WireColumnMeta>, WireErrorTuple> {
    let upper = cleaned_sql.to_ascii_uppercase();
    if upper == "SELECT 1" || upper.starts_with("SELECT 1 ") {
        return Ok(vec![(
            "1".to_string(),
            ColumnType::MYSQL_TYPE_LONGLONG as u8,
            0,
        )]);
    }
    if upper.starts_with("SELECT CONNECTION_ID()") {
        return Ok(vec![(
            "CONNECTION_ID()".to_string(),
            ColumnType::MYSQL_TYPE_LONGLONG as u8,
            ColumnFlags::UNSIGNED_FLAG.bits(),
        )]);
    }
    if upper.starts_with("SELECT SLEEP(") {
        return Ok(vec![(
            "SLEEP".to_string(),
            ColumnType::MYSQL_TYPE_LONGLONG as u8,
            0,
        )]);
    }
    if let Some(from_idx) = find_top_level_keyword(&upper, " FROM ") {
        let proj_str = cleaned_sql[7..from_idx].trim();
        let after_from = cleaned_sql[from_idx + 6..].trim();
        if after_from.starts_with('(') {
            let alias = extract_select_alias(proj_str);
            return Ok(vec![(alias, ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)]);
        }
        let table_token = after_from
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches(|c| c == '`' || c == '"' || c == ';');
        let table_key = table_token
            .rsplit('.')
            .next()
            .unwrap_or(table_token)
            .to_ascii_lowercase();

        if table_key == "tables" || table_key == "columns" {
            return Ok(vec![(
                "name".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                0,
            )]);
        }

        let guard = shared.db.lock();
        let active_db = conn_state.tx_db.as_ref().unwrap_or(&*guard);
        let Some(table) = active_db.tables.get(&table_key) else {
            return Err((
                1146,
                "42S02".to_string(),
                format!("Table 'vtb.{table_token}' doesn't exist"),
            ));
        };

        if proj_str == "*" {
            return Ok(table
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.col_type, c.col_flags))
                .collect());
        }

        let mut cols = Vec::new();
        for item in split_comma_top_level(proj_str) {
            let trimmed = item.trim();
            let alias = extract_select_alias(trimmed);
            let expr_upper = trimmed.to_ascii_uppercase();
            if expr_upper.starts_with("COUNT(") {
                cols.push((alias, ColumnType::MYSQL_TYPE_LONGLONG as u8, 0));
            } else {
                let col_lookup =
                    strip_ident_quotes(trimmed.split_whitespace().next().unwrap_or(trimmed))
                        .to_ascii_lowercase();
                if let Some(col_def) = table
                    .columns
                    .iter()
                    .find(|c| c.name.to_ascii_lowercase() == col_lookup)
                {
                    cols.push((alias, col_def.col_type, col_def.col_flags));
                } else {
                    cols.push((alias, ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0));
                }
            }
        }
        return Ok(cols);
    }

    let proj_str = cleaned_sql[7..].trim();
    let mut cols = Vec::new();
    for item in split_comma_top_level(proj_str) {
        let alias = extract_select_alias(item.trim());
        cols.push((alias, ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0));
    }
    Ok(cols)
}

async fn execute_sql_statement(
    raw_sql: &str,
    params: &[DecodedCell],
    shared: &ServerSharedState,
    conn_state: &mut ConnSessionState,
    cancel_notify: &Arc<Notify>,
) -> Result<Vec<ExecOutcome>, (u16, String, String)> {
    let cleaned = strip_sql_comments(raw_sql);
    if cleaned.is_empty() {
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }
    let upper = cleaned.to_ascii_uppercase();
    if let Some(err) = check_synthetic_error(&upper) {
        return Err(err);
    }

    if upper.starts_with("KILL ") {
        let tokens: Vec<&str> = cleaned.split_whitespace().collect();
        let id_str = tokens.last().copied().unwrap_or("0");
        let target_id = id_str
            .parse::<u32>()
            .ok()
            .or_else(|| match params.first() {
                Some(DecodedCell::Int(i)) => Some(*i as u32),
                Some(DecodedCell::UInt(u)) => Some(*u as u32),
                _ => None,
            })
            .unwrap_or(0);
        shared.kill_query_count.fetch_add(1, Ordering::Release);
        if let Some(notify) = shared.active_cancels.lock().get(&target_id).cloned() {
            notify.notify_waiters();
        }
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("SELECT SLEEP(") {
        let secs = parse_sleep_seconds(&cleaned, params);
        let sleep_dur = Duration::from_secs_f64(secs.max(0.0));
        tokio::select! {
            _ = cancel_notify.notified() => {
                return Err((
                    1317,
                    "70100".to_string(),
                    "Query execution was interrupted".to_string(),
                ));
            }
            _ = tokio::time::sleep(sleep_dur) => {
                return Ok(vec![ExecOutcome::Rows {
                    columns: vec![("SLEEP".to_string(), ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
                    rows: vec![vec![DecodedCell::Int(0)]],
                }]);
            }
        }
    }

    if upper.starts_with("CALL MULTI_RESULT_DEMO") || upper == "SELECT_MULTI_SETS" {
        return Ok(vec![
            ExecOutcome::Rows {
                columns: vec![("a".to_string(), ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
                rows: vec![vec![DecodedCell::Int(1)], vec![DecodedCell::Int(2)]],
            },
            ExecOutcome::Rows {
                columns: vec![("b".to_string(), ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0)],
                rows: vec![vec![DecodedCell::Text("second_set".to_string())]],
            },
        ]);
    }

    if upper.starts_with("SET ") || upper.starts_with("USE ") {
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("START TRANSACTION") || upper == "BEGIN" || upper.starts_with("BEGIN ") {
        let snapshot = shared.db.lock().clone();
        conn_state.in_tx = true;
        conn_state.tx_db = Some(snapshot);
        conn_state.savepoints.clear();
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper == "COMMIT" || upper.starts_with("COMMIT ") {
        if let Some(tx_db) = conn_state.tx_db.take() {
            *shared.db.lock() = tx_db;
        }
        conn_state.in_tx = false;
        conn_state.savepoints.clear();
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("ROLLBACK TO ") {
        let rest = cleaned[12..].trim();
        let sp_raw = if rest.to_ascii_uppercase().starts_with("SAVEPOINT ") {
            rest[10..].trim()
        } else {
            rest
        };
        let sp_name = strip_ident_quotes(sp_raw);
        if let Some(pos) = conn_state
            .savepoints
            .iter()
            .rposition(|(n, _)| n.eq_ignore_ascii_case(&sp_name))
        {
            let restored = conn_state.savepoints[pos].1.clone();
            conn_state.tx_db = Some(restored);
            conn_state.savepoints.truncate(pos + 1);
            return Ok(vec![ExecOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            }]);
        }
        return Err((
            1305,
            "42000".to_string(),
            format!("SAVEPOINT {sp_name} does not exist"),
        ));
    }

    if upper == "ROLLBACK" || upper.starts_with("ROLLBACK ") {
        conn_state.tx_db = None;
        conn_state.in_tx = false;
        conn_state.savepoints.clear();
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("SAVEPOINT ") {
        let sp_name = strip_ident_quotes(cleaned[10..].trim());
        if !conn_state.in_tx {
            conn_state.in_tx = true;
            conn_state.tx_db = Some(shared.db.lock().clone());
        }
        if let Some(ref tx_db) = conn_state.tx_db {
            conn_state.savepoints.push((sp_name, tx_db.clone()));
        }
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("RELEASE SAVEPOINT ") {
        let sp_name = strip_ident_quotes(cleaned[18..].trim());
        if let Some(pos) = conn_state
            .savepoints
            .iter()
            .rposition(|(n, _)| n.eq_ignore_ascii_case(&sp_name))
        {
            conn_state.savepoints.remove(pos);
            return Ok(vec![ExecOutcome::Ok {
                affected_rows: 0,
                last_insert_id: 0,
            }]);
        }
        return Err((
            1305,
            "42000".to_string(),
            format!("SAVEPOINT {sp_name} does not exist"),
        ));
    }

    if upper.starts_with("DROP TABLE") {
        let tokens: Vec<&str> = cleaned.split_whitespace().collect();
        let if_exists = upper.contains("IF EXISTS");
        let raw_table = tokens.last().copied().unwrap_or("");
        let table_key = normalize_table_key(raw_table);
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        if db.tables.remove(&table_key).is_none() && !if_exists {
            return Err((
                1051,
                "42S02".to_string(),
                format!("Unknown table 'vtb.{raw_table}'"),
            ));
        }
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("TRUNCATE ") {
        let tokens: Vec<&str> = cleaned.split_whitespace().collect();
        let raw_table = tokens.last().copied().unwrap_or("");
        let table_key = normalize_table_key(raw_table);
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        if let Some(table) = db.tables.get_mut(&table_key) {
            table.rows.clear();
            table.next_auto_inc = 1;
        }
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("CREATE TABLE") || upper.starts_with("CREATE TEMPORARY TABLE") {
        let table_def = parse_create_table(&cleaned)?;
        let key = table_def.name.to_ascii_lowercase();
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        db.tables.insert(key, table_def);
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: 0,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("INSERT INTO ") {
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        let outcome = execute_insert(&cleaned, params, db)?;
        return Ok(vec![outcome]);
    }

    if upper.starts_with("DELETE FROM ") {
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        let affected = execute_delete(&cleaned, params, db)?;
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: affected,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("UPDATE ") {
        let mut guard = shared.db.lock();
        let db = conn_state.tx_db.as_mut().unwrap_or(&mut *guard);
        let affected = execute_update(&cleaned, params, db)?;
        return Ok(vec![ExecOutcome::Ok {
            affected_rows: affected,
            last_insert_id: 0,
        }]);
    }

    if upper.starts_with("SELECT ") || upper.starts_with("SHOW ") {
        let guard = shared.db.lock();
        let db = conn_state.tx_db.as_ref().unwrap_or(&*guard);
        let (cols, rows) = execute_select(&cleaned, params, db, conn_state.conn_id)?;
        return Ok(vec![ExecOutcome::Rows {
            columns: cols,
            rows,
        }]);
    }

    Err((
        1064,
        "42000".to_string(),
        format!("You have an error in your SQL syntax near '{cleaned}'"),
    ))
}

fn parse_sleep_seconds(sql: &str, params: &[DecodedCell]) -> f64 {
    if let Some(first) = params.first() {
        return match first {
            DecodedCell::Int(i) => *i as f64,
            DecodedCell::UInt(u) => *u as f64,
            DecodedCell::Float(f) => *f,
            DecodedCell::Decimal(s) | DecodedCell::Text(s) => s.trim().parse().unwrap_or(0.1),
            _ => 0.1,
        };
    }
    if let Some(l) = sql.find('(')
        && let Some(r) = sql[l + 1..].find(')')
    {
        return sql[l + 1..l + 1 + r].trim().parse::<f64>().unwrap_or(0.1);
    }
    0.1
}

fn strip_ident_quotes(s: &str) -> String {
    s.trim()
        .trim_matches(|c| c == '`' || c == '"' || c == '\'')
        .to_string()
}

fn normalize_table_key(raw: &str) -> String {
    let unquoted = strip_ident_quotes(raw);
    unquoted
        .rsplit('.')
        .next()
        .unwrap_or(&unquoted)
        .trim_matches(|c| c == '`' || c == '"')
        .to_ascii_lowercase()
}

fn split_comma_top_level(s: &str) -> Vec<&str> {
    let bytes = s.as_bytes();
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut idx = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'\'' || b == b'"' || b == b'`' {
            let q = b;
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'\\' && q != b'`' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if bytes[idx] == q {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == q {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth = depth.saturating_sub(1);
        } else if b == b',' && depth == 0 {
            parts.push(s[start..idx].trim());
            start = idx + 1;
        }
        idx += 1;
    }
    if start <= s.len() {
        let tail = s[start..].trim();
        if !tail.is_empty() {
            parts.push(tail);
        }
    }
    parts
}

fn find_top_level_keyword(upper_sql: &str, keyword: &str) -> Option<usize> {
    let bytes = upper_sql.as_bytes();
    let kw_bytes = keyword.as_bytes();
    if bytes.len() < kw_bytes.len() {
        return None;
    }
    let mut depth = 0usize;
    let mut idx = 0;
    while idx + kw_bytes.len() <= bytes.len() {
        let b = bytes[idx];
        if b == b'\'' || b == b'"' || b == b'`' {
            let q = b;
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'\\' && q != b'`' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if bytes[idx] == q {
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            continue;
        }
        if b == b'(' {
            depth += 1;
        } else if b == b')' {
            depth = depth.saturating_sub(1);
        } else if depth == 0 && &bytes[idx..idx + kw_bytes.len()] == kw_bytes {
            return Some(idx);
        }
        idx += 1;
    }
    None
}

fn extract_select_alias(expr: &str) -> String {
    let upper = expr.to_ascii_uppercase();
    if let Some(as_idx) = upper.rfind(" AS ") {
        return strip_ident_quotes(&expr[as_idx + 4..]);
    }
    let clean = strip_ident_quotes(expr);
    clean.rsplit('.').next().unwrap_or(&clean).to_string()
}

fn parse_create_table(sql: &str) -> Result<TableDef, (u16, String, String)> {
    let open_paren = sql.find('(').ok_or_else(|| {
        (
            1064,
            "42000".to_string(),
            "Missing '(' in CREATE TABLE".to_string(),
        )
    })?;
    let close_paren = sql.rfind(')').ok_or_else(|| {
        (
            1064,
            "42000".to_string(),
            "Missing ')' in CREATE TABLE".to_string(),
        )
    })?;
    let header = sql[..open_paren].trim();
    let raw_table_name = header.split_whitespace().last().unwrap_or("table");
    let table_name =
        strip_ident_quotes(raw_table_name.rsplit('.').next().unwrap_or(raw_table_name));

    let body = &sql[open_paren + 1..close_paren];
    let mut columns = Vec::new();
    let mut table_pk_cols = Vec::new();

    for item in split_comma_top_level(body) {
        let item_upper = item.to_ascii_uppercase();
        if item_upper.starts_with("PRIMARY KEY")
            && let Some(l) = item.find('(')
            && let Some(r) = item.rfind(')')
        {
            for pk_col in item[l + 1..r].split(',') {
                table_pk_cols.push(strip_ident_quotes(pk_col).to_ascii_lowercase());
            }
            continue;
        }
        if item_upper.starts_with("CONSTRAINT ")
            || item_upper.starts_with("UNIQUE ")
            || item_upper.starts_with("KEY ")
            || item_upper.starts_with("INDEX ")
        {
            continue;
        }

        let mut parts = item.split_whitespace();
        let Some(raw_col_name) = parts.next() else {
            continue;
        };
        let col_name = strip_ident_quotes(raw_col_name);
        let col_name_lower = col_name.to_ascii_lowercase();
        let rest_upper = item[raw_col_name.len()..].trim().to_ascii_uppercase();

        let is_unsigned = rest_upper.contains("UNSIGNED");
        let is_pk = rest_upper.contains("PRIMARY KEY");
        let not_null = is_pk || rest_upper.contains("NOT NULL");
        let is_auto_inc = rest_upper.contains("AUTO_INCREMENT");

        let (col_type, mut col_flags, is_boolean) = infer_column_type(&col_name_lower, &rest_upper);
        if is_unsigned {
            col_flags |= ColumnFlags::UNSIGNED_FLAG.bits();
        }
        columns.push(ColumnDef {
            name: col_name,
            col_type,
            col_flags,
            not_null,
            is_primary_key: is_pk,
            is_auto_increment: is_auto_inc,
            is_boolean,
        });
    }

    for pk_name in table_pk_cols {
        for col in &mut columns {
            if col.name.to_ascii_lowercase() == pk_name {
                col.is_primary_key = true;
                col.not_null = true;
            }
        }
    }

    Ok(TableDef {
        name: table_name,
        columns,
        rows: Vec::new(),
        next_auto_inc: 1,
    })
}

fn infer_column_type(col_name_lower: &str, type_spec_upper: &str) -> (u8, u16, bool) {
    if col_name_lower == "val_decimal"
        || type_spec_upper.starts_with("DECIMAL")
        || type_spec_upper.starts_with("NUMERIC")
        || type_spec_upper.starts_with("DEC")
    {
        return (ColumnType::MYSQL_TYPE_NEWDECIMAL as u8, 0, false);
    }
    if col_name_lower == "val_bool"
        || type_spec_upper.starts_with("BOOL")
        || type_spec_upper.starts_with("TINYINT(1)")
    {
        return (ColumnType::MYSQL_TYPE_TINY as u8, 0, true);
    }
    if col_name_lower == "val_ts"
        || type_spec_upper.starts_with("DATETIME")
        || type_spec_upper.starts_with("TIMESTAMP")
    {
        return (ColumnType::MYSQL_TYPE_DATETIME as u8, 0, false);
    }
    if col_name_lower == "val_json" || type_spec_upper.starts_with("JSON") {
        return (ColumnType::MYSQL_TYPE_JSON as u8, 0, false);
    }
    if col_name_lower == "val_bytes"
        || type_spec_upper.contains("BLOB")
        || type_spec_upper.starts_with("BINARY")
        || type_spec_upper.starts_with("VARBINARY")
    {
        return (
            ColumnType::MYSQL_TYPE_BLOB as u8,
            ColumnFlags::BINARY_FLAG.bits(),
            false,
        );
    }
    if type_spec_upper.starts_with("VECTOR") {
        return (
            ColumnType::MYSQL_TYPE_VECTOR as u8,
            ColumnFlags::BINARY_FLAG.bits(),
            false,
        );
    }
    if type_spec_upper.starts_with("BIT") {
        return (
            ColumnType::MYSQL_TYPE_BIT as u8,
            ColumnFlags::BINARY_FLAG.bits(),
            false,
        );
    }
    if type_spec_upper.starts_with("TINYINT") {
        return (ColumnType::MYSQL_TYPE_TINY as u8, 0, false);
    }
    if type_spec_upper.starts_with("SMALLINT") {
        return (ColumnType::MYSQL_TYPE_SHORT as u8, 0, false);
    }
    if type_spec_upper.starts_with("MEDIUMINT") {
        return (ColumnType::MYSQL_TYPE_INT24 as u8, 0, false);
    }
    if type_spec_upper.starts_with("BIGINT") || type_spec_upper.starts_with("INT64") {
        return (ColumnType::MYSQL_TYPE_LONGLONG as u8, 0, false);
    }
    if type_spec_upper.starts_with("INT") || type_spec_upper.starts_with("SERIAL") {
        return (ColumnType::MYSQL_TYPE_LONG as u8, 0, false);
    }
    if type_spec_upper.starts_with("FLOAT") {
        return (ColumnType::MYSQL_TYPE_FLOAT as u8, 0, false);
    }
    if type_spec_upper.starts_with("DOUBLE") || type_spec_upper.starts_with("REAL") {
        return (ColumnType::MYSQL_TYPE_DOUBLE as u8, 0, false);
    }
    if type_spec_upper.starts_with("DATE") {
        return (ColumnType::MYSQL_TYPE_DATE as u8, 0, false);
    }
    if type_spec_upper.starts_with("TIME") {
        return (ColumnType::MYSQL_TYPE_TIME as u8, 0, false);
    }
    if type_spec_upper.starts_with("YEAR") {
        return (ColumnType::MYSQL_TYPE_YEAR as u8, 0, false);
    }
    (ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0, false)
}

fn coerce_cell_for_column(cell: DecodedCell, col: &ColumnDef) -> DecodedCell {
    if matches!(cell, DecodedCell::Null) {
        return DecodedCell::Null;
    }
    let ct = col.col_type;
    if ct == ColumnType::MYSQL_TYPE_NEWDECIMAL as u8 || ct == ColumnType::MYSQL_TYPE_DECIMAL as u8 {
        return match cell {
            DecodedCell::Decimal(s) | DecodedCell::Text(s) => DecodedCell::Decimal(s),
            DecodedCell::Bytes(b) => DecodedCell::Decimal(String::from_utf8_lossy(&b).into_owned()),
            DecodedCell::Int(i) => DecodedCell::Decimal(i.to_string()),
            DecodedCell::UInt(u) => DecodedCell::Decimal(u.to_string()),
            DecodedCell::Float(f) => DecodedCell::Decimal(f.to_string()),
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_JSON as u8 {
        return match cell {
            DecodedCell::Json(s) | DecodedCell::Text(s) => DecodedCell::Json(s),
            DecodedCell::Bytes(b) => DecodedCell::Json(String::from_utf8_lossy(&b).into_owned()),
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_BLOB as u8
        || ct == ColumnType::MYSQL_TYPE_VECTOR as u8
        || ct == ColumnType::MYSQL_TYPE_BIT as u8
    {
        return match cell {
            DecodedCell::Bytes(b) => DecodedCell::Bytes(b),
            DecodedCell::Text(s) => DecodedCell::Bytes(s.into_bytes()),
            DecodedCell::Int(i) if ct == ColumnType::MYSQL_TYPE_BIT as u8 => {
                DecodedCell::Bytes(vec![i as u8])
            }
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_DATETIME as u8 || ct == ColumnType::MYSQL_TYPE_TIMESTAMP as u8 {
        return match cell {
            DecodedCell::DateTime(..) => cell,
            DecodedCell::Date(y, m, d) => DecodedCell::DateTime(y, m, d, 0, 0, 0, 0),
            DecodedCell::Text(ref s) => {
                if let Some((y, m, d, hh, mm, ss, us)) = parse_sql_datetime(s) {
                    DecodedCell::DateTime(y, m, d, hh, mm, ss, us)
                } else {
                    cell
                }
            }
            DecodedCell::Bytes(ref b) => {
                if let Ok(s) = std::str::from_utf8(b)
                    && let Some((y, m, d, hh, mm, ss, us)) = parse_sql_datetime(s)
                {
                    DecodedCell::DateTime(y, m, d, hh, mm, ss, us)
                } else {
                    cell
                }
            }
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_DATE as u8 {
        return match cell {
            DecodedCell::Date(..) => cell,
            DecodedCell::DateTime(y, m, d, ..) => DecodedCell::Date(y, m, d),
            DecodedCell::Text(ref s) => {
                if let Some((y, m, d)) = parse_sql_date(s) {
                    DecodedCell::Date(y, m, d)
                } else {
                    cell
                }
            }
            DecodedCell::Bytes(ref b) => {
                if let Ok(s) = std::str::from_utf8(b)
                    && let Some((y, m, d)) = parse_sql_date(s)
                {
                    DecodedCell::Date(y, m, d)
                } else {
                    cell
                }
            }
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_TIME as u8 {
        return match cell {
            DecodedCell::Time(..) => cell,
            DecodedCell::Text(ref s) => {
                if let Some((neg, days, hh, mm, ss, us)) = parse_sql_time(s) {
                    DecodedCell::Time(neg, days, hh, mm, ss, us)
                } else {
                    cell
                }
            }
            DecodedCell::Bytes(ref b) => {
                if let Ok(s) = std::str::from_utf8(b)
                    && let Some((neg, days, hh, mm, ss, us)) = parse_sql_time(s)
                {
                    DecodedCell::Time(neg, days, hh, mm, ss, us)
                } else {
                    cell
                }
            }
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_TINY as u8
        || ct == ColumnType::MYSQL_TYPE_SHORT as u8
        || ct == ColumnType::MYSQL_TYPE_INT24 as u8
        || ct == ColumnType::MYSQL_TYPE_LONG as u8
        || ct == ColumnType::MYSQL_TYPE_LONGLONG as u8
        || ct == ColumnType::MYSQL_TYPE_YEAR as u8
    {
        let is_unsigned = (col.col_flags & ColumnFlags::UNSIGNED_FLAG.bits()) != 0;
        return match cell {
            DecodedCell::Bool(b) => DecodedCell::Int(if b { 1 } else { 0 }),
            DecodedCell::Int(i) => {
                if is_unsigned && i >= 0 {
                    DecodedCell::UInt(i as u64)
                } else {
                    DecodedCell::Int(i)
                }
            }
            DecodedCell::UInt(u) => {
                if is_unsigned || u > (i64::MAX as u64) {
                    DecodedCell::UInt(u)
                } else {
                    DecodedCell::Int(u as i64)
                }
            }
            DecodedCell::Text(ref s) => {
                if is_unsigned {
                    s.trim()
                        .parse::<u64>()
                        .map(DecodedCell::UInt)
                        .unwrap_or(cell)
                } else {
                    s.trim()
                        .parse::<i64>()
                        .map(DecodedCell::Int)
                        .unwrap_or(cell)
                }
            }
            DecodedCell::Bytes(ref b) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    if is_unsigned {
                        s.trim()
                            .parse::<u64>()
                            .map(DecodedCell::UInt)
                            .unwrap_or(cell)
                    } else {
                        s.trim()
                            .parse::<i64>()
                            .map(DecodedCell::Int)
                            .unwrap_or(cell)
                    }
                } else {
                    cell
                }
            }
            other => other,
        };
    }
    if ct == ColumnType::MYSQL_TYPE_FLOAT as u8 || ct == ColumnType::MYSQL_TYPE_DOUBLE as u8 {
        return match cell {
            DecodedCell::Float(f) => DecodedCell::Float(f),
            DecodedCell::Int(i) => DecodedCell::Float(i as f64),
            DecodedCell::UInt(u) => DecodedCell::Float(u as f64),
            DecodedCell::Text(ref s) => s
                .trim()
                .parse::<f64>()
                .map(DecodedCell::Float)
                .unwrap_or(cell),
            DecodedCell::Bytes(ref b) => std::str::from_utf8(b)
                .ok()
                .and_then(|s| s.trim().parse::<f64>().ok())
                .map(DecodedCell::Float)
                .unwrap_or(cell),
            other => other,
        };
    }
    match cell {
        DecodedCell::Bytes(b) => match String::from_utf8(b) {
            Ok(s) => DecodedCell::Text(s),
            Err(e) => DecodedCell::Bytes(e.into_bytes()),
        },
        other => other,
    }
}

fn parse_sql_date(s: &str) -> Option<(i32, u32, u32)> {
    let mut parts = s.trim().split('-');
    let y = parts.next()?.parse::<i32>().ok()?;
    let m = parts.next()?.parse::<u32>().ok()?;
    let d = parts.next()?.parse::<u32>().ok()?;
    Some((y, m, d))
}

fn parse_sql_datetime(s: &str) -> Option<(i32, u32, u32, u32, u32, u32, u32)> {
    let trimmed = s.trim().trim_end_matches('Z');
    let stripped = if let Some(plus_idx) = trimmed[10.min(trimmed.len())..].find('+') {
        &trimmed[..10 + plus_idx]
    } else {
        trimmed
    };
    let (date_part, time_part) = stripped
        .split_once(' ')
        .or_else(|| stripped.split_once('T'))?;
    let (y, m, d) = parse_sql_date(date_part)?;
    let (_, _, hh, mm, ss, us) = parse_sql_time(time_part)?;
    Some((y, m, d, hh, mm, ss, us))
}

fn parse_sql_time(s: &str) -> Option<(bool, u32, u32, u32, u32, u32)> {
    let trimmed = s.trim();
    let (is_neg, rest) = if let Some(r) = trimmed.strip_prefix('-') {
        (true, r)
    } else {
        (false, trimmed)
    };
    let mut parts = rest.split(':');
    let total_hh = parts.next()?.parse::<u32>().ok()?;
    let mm = parts.next()?.parse::<u32>().ok()?;
    let sec_part = parts.next()?;
    let (ss, us) = if let Some((s_str, frac_str)) = sec_part.split_once('.') {
        let sec = s_str.parse::<u32>().ok()?;
        let mut padded = frac_str.chars().take(6).collect::<String>();
        while padded.len() < 6 {
            padded.push('0');
        }
        let micros = padded.parse::<u32>().ok()?;
        (sec, micros)
    } else {
        (sec_part.parse::<u32>().ok()?, 0)
    };
    let days = total_hh / 24;
    let hh = total_hh % 24;
    Some((is_neg, days, hh, mm, ss, us))
}

fn eval_sql_expr(expr: &str, param_iter: &mut std::slice::Iter<'_, DecodedCell>) -> DecodedCell {
    let trimmed = expr.trim();
    if trimmed == "?" {
        return param_iter.next().cloned().unwrap_or(DecodedCell::Null);
    }
    let upper = trimmed.to_ascii_uppercase();
    if upper == "NULL" {
        return DecodedCell::Null;
    }
    if upper == "TRUE" {
        return DecodedCell::Bool(true);
    }
    if upper == "FALSE" {
        return DecodedCell::Bool(false);
    }
    if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
    {
        let inner = &trimmed[1..trimmed.len() - 1];
        return DecodedCell::Text(inner.replace("''", "'").replace("\\'", "'"));
    }
    if let Some(hex_str) = upper.strip_prefix("X'").and_then(|s| s.strip_suffix('\'')) {
        let mut bytes = Vec::with_capacity(hex_str.len() / 2);
        let mut idx = 0;
        while idx + 2 <= hex_str.len() {
            if let Ok(b) = u8::from_str_radix(&hex_str[idx..idx + 2], 16) {
                bytes.push(b);
            }
            idx += 2;
        }
        return DecodedCell::Bytes(bytes);
    }
    if let Ok(i) = trimmed.parse::<i64>() {
        return DecodedCell::Int(i);
    }
    if let Ok(u) = trimmed.parse::<u64>() {
        return DecodedCell::UInt(u);
    }
    if let Ok(f) = trimmed.parse::<f64>() {
        return DecodedCell::Float(f);
    }
    DecodedCell::Text(trimmed.to_string())
}

fn execute_insert(
    sql: &str,
    params: &[DecodedCell],
    db: &mut DatabaseState,
) -> Result<ExecOutcome, (u16, String, String)> {
    let upper = sql.to_ascii_uppercase();
    let returning_idx = find_top_level_keyword(&upper, " RETURNING ");
    let base_sql = match returning_idx {
        Some(idx) => sql[..idx].trim(),
        None => sql,
    };
    let returning_cols: Option<Vec<String>> = returning_idx.map(|idx| {
        sql[idx + 11..]
            .split(',')
            .map(|c| strip_ident_quotes(c).to_ascii_lowercase())
            .collect()
    });
    let base_upper = base_sql.to_ascii_uppercase();
    let values_idx = find_top_level_keyword(&base_upper, " VALUES")
        .or_else(|| find_top_level_keyword(&base_upper, ")VALUES"))
        .map(|i| {
            if base_upper.as_bytes()[i] == b')' {
                i + 1
            } else {
                i
            }
        })
        .ok_or_else(|| {
            (
                1064,
                "42000".to_string(),
                "Missing VALUES in INSERT".to_string(),
            )
        })?;

    let prefix = base_sql[12..values_idx].trim();
    let (raw_table, explicit_cols) = if let Some(open_p) = prefix.find('(') {
        let close_p = prefix.rfind(')').unwrap_or(prefix.len());
        let cols: Vec<String> = prefix[open_p + 1..close_p]
            .split(',')
            .map(|c| strip_ident_quotes(c).to_ascii_lowercase())
            .collect();
        (prefix[..open_p].trim(), Some(cols))
    } else {
        (prefix, None)
    };

    let table_key = normalize_table_key(raw_table);
    let Some(table) = db.tables.get_mut(&table_key) else {
        return Err((
            1146,
            "42S02".to_string(),
            format!("Table 'vtb.{raw_table}' doesn't exist"),
        ));
    };

    let values_part = base_sql[values_idx..]
        .trim()
        .strip_prefix("VALUES")
        .or_else(|| base_sql[values_idx..].trim().strip_prefix("values"))
        .unwrap_or(&base_sql[values_idx + 6..])
        .trim();

    let tuple_strs = split_value_tuples(values_part);
    let mut param_iter = params.iter();
    let mut inserted_count = 0u64;
    let mut last_id = 0u64;
    let mut inserted_rows = Vec::new();

    for tup_str in tuple_strs {
        let exprs = split_comma_top_level(tup_str);
        let mut row = vec![DecodedCell::Null; table.columns.len()];

        if let Some(ref col_names) = explicit_cols {
            for (col_name, expr) in col_names.iter().zip(exprs.iter()) {
                if let Some((col_idx, col_def)) = table
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name.to_ascii_lowercase() == *col_name)
                {
                    let raw_val = eval_sql_expr(expr, &mut param_iter);
                    row[col_idx] = coerce_cell_for_column(raw_val, col_def);
                }
            }
        } else {
            for (col_idx, expr) in exprs.iter().enumerate() {
                if let Some(col_def) = table.columns.get(col_idx) {
                    let raw_val = eval_sql_expr(expr, &mut param_iter);
                    row[col_idx] = coerce_cell_for_column(raw_val, col_def);
                }
            }
        }

        for (col_idx, col_def) in table.columns.iter().enumerate() {
            if col_def.is_auto_increment && matches!(row[col_idx], DecodedCell::Null) {
                let id = table.next_auto_inc;
                table.next_auto_inc += 1;
                row[col_idx] = DecodedCell::Int(id as i64);
                last_id = id;
            } else if col_def.is_auto_increment
                && let DecodedCell::Int(i) = row[col_idx]
                && i > 0
            {
                let u = i as u64;
                if u >= table.next_auto_inc {
                    table.next_auto_inc = u + 1;
                }
                last_id = u;
            }
            if col_def.not_null && matches!(row[col_idx], DecodedCell::Null) {
                return Err((
                    1048,
                    "23000".to_string(),
                    format!("Column '{}' cannot be null", col_def.name),
                ));
            }
            if col_def.is_primary_key {
                let candidate = &row[col_idx];
                if table
                    .rows
                    .iter()
                    .any(|existing| cells_equal(&existing[col_idx], candidate))
                {
                    return Err((
                        1062,
                        "23000".to_string(),
                        format!(
                            "Duplicate entry '{}' for key '{}.PRIMARY'",
                            cell_display(candidate),
                            table.name
                        ),
                    ));
                }
            }
        }

        inserted_rows.push(row.clone());
        table.rows.push(row);
        inserted_count += 1;
    }

    if let Some(ret_cols) = returning_cols {
        let mut out_cols = Vec::new();
        let mut col_indices = Vec::new();
        for rc in &ret_cols {
            if let Some((idx, col_def)) = table
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.to_ascii_lowercase() == *rc)
            {
                out_cols.push((col_def.name.clone(), col_def.col_type, col_def.col_flags));
                col_indices.push(idx);
            }
        }
        let mut out_rows = Vec::new();
        for r in inserted_rows {
            let projected = col_indices.iter().map(|&idx| r[idx].clone()).collect();
            out_rows.push(projected);
        }
        return Ok(ExecOutcome::Rows {
            columns: out_cols,
            rows: out_rows,
        });
    }

    Ok(ExecOutcome::Ok {
        affected_rows: inserted_count,
        last_insert_id: last_id,
    })
}

fn split_value_tuples(values_str: &str) -> Vec<&str> {
    let bytes = values_str.as_bytes();
    let mut tuples = Vec::new();
    let mut depth = 0usize;
    let mut tup_start = None;
    let mut idx = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'\'' || b == b'"' || b == b'`' {
            let q = b;
            idx += 1;
            while idx < bytes.len() {
                if bytes[idx] == b'\\' && q != b'`' && idx + 1 < bytes.len() {
                    idx += 2;
                    continue;
                }
                if bytes[idx] == q {
                    if idx + 1 < bytes.len() && bytes[idx + 1] == q {
                        idx += 2;
                        continue;
                    }
                    idx += 1;
                    break;
                }
                idx += 1;
            }
            continue;
        }
        if b == b'(' {
            if depth == 0 {
                tup_start = Some(idx + 1);
            }
            depth += 1;
        } else if b == b')' {
            depth = depth.saturating_sub(1);
            if depth == 0
                && let Some(start) = tup_start.take()
            {
                tuples.push(values_str[start..idx].trim());
            }
        }
        idx += 1;
    }
    tuples
}

fn execute_delete(
    sql: &str,
    params: &[DecodedCell],
    db: &mut DatabaseState,
) -> Result<u64, (u16, String, String)> {
    let upper = sql.to_ascii_uppercase();
    let rest = sql[12..].trim();
    let rest_upper = &upper[12..].trim();
    let (raw_table, where_clause) =
        if let Some(w_idx) = find_top_level_keyword(rest_upper, " WHERE ") {
            (rest[..w_idx].trim(), Some(rest[w_idx + 7..].trim()))
        } else {
            (rest, None)
        };
    let table_key = normalize_table_key(raw_table);
    let Some(table) = db.tables.get_mut(&table_key) else {
        return Err((
            1146,
            "42S02".to_string(),
            format!("Table 'vtb.{raw_table}' doesn't exist"),
        ));
    };

    let before = table.rows.len();
    let cols = table.columns.clone();
    table.rows.retain(|row| {
        let mut param_iter = params.iter();
        !where_clause
            .map(|w| eval_where_clause(w, row, &cols, &mut param_iter))
            .unwrap_or(true)
    });
    let removed = (before - table.rows.len()) as u64;
    Ok(removed)
}

fn execute_update(
    sql: &str,
    params: &[DecodedCell],
    db: &mut DatabaseState,
) -> Result<u64, (u16, String, String)> {
    let upper = sql.to_ascii_uppercase();
    let set_idx = find_top_level_keyword(&upper, " SET ").ok_or_else(|| {
        (
            1064,
            "42000".to_string(),
            "Missing SET in UPDATE".to_string(),
        )
    })?;
    let raw_table = sql[7..set_idx].trim();
    let after_set = &sql[set_idx + 5..];
    let after_set_upper = &upper[set_idx + 5..];
    let (set_part, where_clause) =
        if let Some(w_idx) = find_top_level_keyword(after_set_upper, " WHERE ") {
            (
                after_set[..w_idx].trim(),
                Some(after_set[w_idx + 7..].trim()),
            )
        } else {
            (after_set.trim(), None)
        };

    let table_key = normalize_table_key(raw_table);
    let Some(table) = db.tables.get_mut(&table_key) else {
        return Err((
            1146,
            "42S02".to_string(),
            format!("Table 'vtb.{raw_table}' doesn't exist"),
        ));
    };

    let assignments: Vec<(&str, &str)> = split_comma_top_level(set_part)
        .into_iter()
        .filter_map(|s| s.split_once('='))
        .collect();
    let set_param_count: usize = assignments
        .iter()
        .map(|(_, expr)| count_placeholders(expr))
        .sum();
    let set_params = &params[..set_param_count.min(params.len())];
    let where_params = &params[set_param_count.min(params.len())..];

    let cols = table.columns.clone();
    let mut updated = 0u64;
    for row in &mut table.rows {
        let mut w_iter = where_params.iter();
        let matches = where_clause
            .map(|w| eval_where_clause(w, row, &cols, &mut w_iter))
            .unwrap_or(true);
        if matches {
            let mut s_iter = set_params.iter();
            for (col_raw, expr_raw) in &assignments {
                let col_name = strip_ident_quotes(col_raw).to_ascii_lowercase();
                if let Some((idx, col_def)) = cols
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name.to_ascii_lowercase() == col_name)
                {
                    let val = eval_sql_expr(expr_raw, &mut s_iter);
                    row[idx] = coerce_cell_for_column(val, col_def);
                }
            }
            updated += 1;
        }
    }
    Ok(updated)
}

fn execute_select(
    sql: &str,
    params: &[DecodedCell],
    db: &DatabaseState,
    conn_id: u32,
) -> Result<SelectResultPair, WireErrorTuple> {
    let upper = sql.to_ascii_uppercase();
    if upper.starts_with("SHOW TABLES") {
        let mut rows: Vec<Vec<DecodedCell>> = db
            .tables
            .values()
            .map(|t| vec![DecodedCell::Text(t.name.clone())])
            .collect();
        rows.sort_by(|a, b| cell_cmp(&a[0], &b[0]));
        return Ok((
            vec![(
                "Tables_in_vtb".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                0,
            )],
            rows,
        ));
    }

    if upper == "SELECT 1" || upper.starts_with("SELECT 1 ") && !upper.contains(" FROM ") {
        let alias = extract_select_alias(sql[7..].trim());
        return Ok((
            vec![(alias, ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
            vec![vec![DecodedCell::Int(1)]],
        ));
    }

    if upper.starts_with("SELECT CONNECTION_ID()") {
        return Ok((
            vec![(
                "CONNECTION_ID()".to_string(),
                ColumnType::MYSQL_TYPE_LONGLONG as u8,
                ColumnFlags::UNSIGNED_FLAG.bits(),
            )],
            vec![vec![DecodedCell::UInt(conn_id as u64)]],
        ));
    }

    if upper.contains("@@MAX_ALLOWED_PACKET") {
        return Ok((
            vec![(
                "@@max_allowed_packet".to_string(),
                ColumnType::MYSQL_TYPE_LONGLONG as u8,
                ColumnFlags::UNSIGNED_FLAG.bits(),
            )],
            vec![vec![DecodedCell::UInt(67_108_864)]],
        ));
    }

    if upper.starts_with("SELECT @@") || upper.starts_with("SELECT VERSION()") {
        return Ok((
            vec![(
                "version".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                0,
            )],
            vec![vec![DecodedCell::Text("8.4.0-Vertebra".to_string())]],
        ));
    }

    let Some(from_idx) = find_top_level_keyword(&upper, " FROM ") else {
        let proj_str = sql[7..].trim();
        let mut param_iter = params.iter();
        let mut cols = Vec::new();
        let mut row = Vec::new();
        for item in split_comma_top_level(proj_str) {
            let trimmed = item.trim();
            let expr_part = if let Some(as_idx) = trimmed.to_ascii_uppercase().rfind(" AS ") {
                &trimmed[..as_idx]
            } else {
                trimmed
            };
            let alias = extract_select_alias(trimmed);
            let val = eval_sql_expr(expr_part, &mut param_iter);
            let ct = match &val {
                DecodedCell::Int(_) | DecodedCell::UInt(_) | DecodedCell::Bool(_) => {
                    ColumnType::MYSQL_TYPE_LONGLONG as u8
                }
                DecodedCell::Float(_) => ColumnType::MYSQL_TYPE_DOUBLE as u8,
                DecodedCell::Decimal(_) => ColumnType::MYSQL_TYPE_NEWDECIMAL as u8,
                DecodedCell::Bytes(_) => ColumnType::MYSQL_TYPE_BLOB as u8,
                DecodedCell::Date(..) => ColumnType::MYSQL_TYPE_DATE as u8,
                DecodedCell::DateTime(..) => ColumnType::MYSQL_TYPE_DATETIME as u8,
                DecodedCell::Time(..) => ColumnType::MYSQL_TYPE_TIME as u8,
                _ => ColumnType::MYSQL_TYPE_VAR_STRING as u8,
            };
            cols.push((alias, ct, 0));
            row.push(val);
        }
        return Ok((cols, vec![row]));
    };

    let proj_str = sql[7..from_idx].trim();
    let after_from = &sql[from_idx + 6..];
    let after_from_trimmed = after_from.trim();
    if let Some(sub_rest) = after_from_trimmed.strip_prefix('(')
        && let Some(close_idx) = sub_rest.rfind(')')
    {
        let inner_sql = sub_rest[..close_idx].trim();
        let (_inner_cols, inner_rows) = execute_select(inner_sql, params, db, conn_id)?;
        let alias = extract_select_alias(proj_str);
        return Ok((
            vec![(alias, ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
            vec![vec![DecodedCell::Int(inner_rows.len() as i64)]],
        ));
    }
    let after_from_upper = &upper[from_idx + 6..];

    let where_idx = find_top_level_keyword(after_from_upper, " WHERE ");
    let order_idx = find_top_level_keyword(after_from_upper, " ORDER BY ");
    let limit_idx = find_top_level_keyword(after_from_upper, " LIMIT ");

    let table_end = [where_idx, order_idx, limit_idx]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after_from.len());
    let raw_table = after_from[..table_end]
        .split_whitespace()
        .next()
        .unwrap_or("");
    let table_key = normalize_table_key(raw_table);

    if table_key == "tables" && after_from_upper.contains("INFORMATION_SCHEMA") {
        let mut rows: Vec<Vec<DecodedCell>> = db
            .tables
            .values()
            .map(|t| vec![DecodedCell::Text(t.name.clone())])
            .collect();
        rows.sort_by(|a, b| cell_cmp(&a[0], &b[0]));
        return Ok((
            vec![(
                "table_name".to_string(),
                ColumnType::MYSQL_TYPE_VAR_STRING as u8,
                0,
            )],
            rows,
        ));
    }

    let Some(table) = db.tables.get(&table_key) else {
        return Err((
            1146,
            "42S02".to_string(),
            format!("Table 'vtb.{raw_table}' doesn't exist"),
        ));
    };

    let where_clause = where_idx.map(|w| {
        let end = [order_idx, limit_idx]
            .into_iter()
            .flatten()
            .filter(|&i| i > w)
            .min()
            .unwrap_or(after_from.len());
        after_from[w + 7..end].trim()
    });

    let order_clause = order_idx.map(|o| {
        let end = limit_idx.filter(|&i| i > o).unwrap_or(after_from.len());
        after_from[o + 10..end].trim()
    });

    let limit_clause = limit_idx.map(|l| after_from[l + 7..].trim());

    let mut filtered_rows: Vec<&Vec<DecodedCell>> = table
        .rows
        .iter()
        .filter(|row| {
            let mut param_iter = params.iter();
            where_clause
                .map(|w| eval_where_clause(w, row, &table.columns, &mut param_iter))
                .unwrap_or(true)
        })
        .collect();

    if let Some(order_str) = order_clause {
        let mut order_specs = Vec::new();
        for part in split_comma_top_level(order_str) {
            let mut tokens = part.split_whitespace();
            if let Some(col_tok) = tokens.next() {
                let col_name = strip_ident_quotes(col_tok).to_ascii_lowercase();
                let desc = tokens
                    .next()
                    .map(|d| d.eq_ignore_ascii_case("DESC"))
                    .unwrap_or(false);
                if let Some(col_idx) = table
                    .columns
                    .iter()
                    .position(|c| c.name.to_ascii_lowercase() == col_name)
                {
                    order_specs.push((col_idx, desc));
                }
            }
        }
        filtered_rows.sort_by(|a, b| {
            for &(col_idx, desc) in &order_specs {
                let cmp = cell_cmp(&a[col_idx], &b[col_idx]);
                if cmp != std::cmp::Ordering::Equal {
                    return if desc { cmp.reverse() } else { cmp };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    if let Some(lim_str) = limit_clause {
        let lim_upper = lim_str.to_ascii_uppercase();
        let (limit_n, offset_n) = if let Some((l_part, o_part)) = lim_upper.split_once(" OFFSET ") {
            (
                l_part.trim().parse::<usize>().unwrap_or(usize::MAX),
                o_part.trim().parse::<usize>().unwrap_or(0),
            )
        } else {
            (lim_str.trim().parse::<usize>().unwrap_or(usize::MAX), 0)
        };
        filtered_rows = filtered_rows
            .into_iter()
            .skip(offset_n)
            .take(limit_n)
            .collect();
    }

    let proj_upper = proj_str.to_ascii_uppercase();
    if proj_upper.starts_with("COUNT(") {
        let alias = extract_select_alias(proj_str);
        return Ok((
            vec![(alias, ColumnType::MYSQL_TYPE_LONGLONG as u8, 0)],
            vec![vec![DecodedCell::Int(filtered_rows.len() as i64)]],
        ));
    }

    if proj_str == "*" {
        let out_cols: Vec<(String, u8, u16)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.col_type, c.col_flags))
            .collect();
        let out_rows: Vec<Vec<DecodedCell>> =
            filtered_rows.into_iter().map(|r| (*r).clone()).collect();
        return Ok((out_cols, out_rows));
    }

    let proj_items = split_comma_top_level(proj_str);
    let mut out_cols = Vec::with_capacity(proj_items.len());
    let mut col_indices = Vec::with_capacity(proj_items.len());

    for item in &proj_items {
        let trimmed = item.trim();
        let alias = extract_select_alias(trimmed);
        let expr_part = if let Some(as_idx) = trimmed.to_ascii_uppercase().rfind(" AS ") {
            trimmed[..as_idx].trim()
        } else {
            trimmed
        };
        let lookup = strip_ident_quotes(expr_part.rsplit('.').next().unwrap_or(expr_part))
            .to_ascii_lowercase();

        if let Some((idx, col_def)) = table
            .columns
            .iter()
            .enumerate()
            .find(|(_, c)| c.name.to_ascii_lowercase() == lookup)
        {
            out_cols.push((alias, col_def.col_type, col_def.col_flags));
            col_indices.push((Some(idx), col_def.is_boolean));
        } else {
            out_cols.push((alias, ColumnType::MYSQL_TYPE_VAR_STRING as u8, 0));
            col_indices.push((None, false));
        }
    }

    let mut out_rows = Vec::with_capacity(filtered_rows.len());
    for row in filtered_rows {
        let mut projected = Vec::with_capacity(col_indices.len());
        for &(maybe_idx, is_bool) in &col_indices {
            if let Some(idx) = maybe_idx {
                let val = row[idx].clone();
                if is_bool && let DecodedCell::Int(i) = val {
                    projected.push(DecodedCell::Bool(i != 0));
                } else {
                    projected.push(val);
                }
            } else {
                projected.push(DecodedCell::Null);
            }
        }
        out_rows.push(projected);
    }

    Ok((out_cols, out_rows))
}

fn eval_where_clause(
    where_str: &str,
    row: &[DecodedCell],
    cols: &[ColumnDef],
    param_iter: &mut std::slice::Iter<'_, DecodedCell>,
) -> bool {
    let trimmed = where_str.trim();
    if trimmed.is_empty() || trimmed == "1=1" || trimmed.eq_ignore_ascii_case("TRUE") {
        return true;
    }
    if trimmed == "1=0" || trimmed.eq_ignore_ascii_case("FALSE") {
        return false;
    }

    let upper = trimmed.to_ascii_uppercase();
    if let Some(and_idx) = find_top_level_keyword(&upper, " AND ") {
        let left = eval_where_clause(&trimmed[..and_idx], row, cols, param_iter);
        let right = eval_where_clause(&trimmed[and_idx + 5..], row, cols, param_iter);
        return left && right;
    }

    if let Some(is_not_null) = upper.strip_suffix(" IS NOT NULL") {
        let col_name = strip_ident_quotes(is_not_null).to_ascii_lowercase();
        if let Some(idx) = cols
            .iter()
            .position(|c| c.name.to_ascii_lowercase() == col_name)
        {
            return !matches!(row[idx], DecodedCell::Null);
        }
    }
    if let Some(is_null) = upper.strip_suffix(" IS NULL") {
        let col_name = strip_ident_quotes(is_null).to_ascii_lowercase();
        if let Some(idx) = cols
            .iter()
            .position(|c| c.name.to_ascii_lowercase() == col_name)
        {
            return matches!(row[idx], DecodedCell::Null);
        }
    }

    for op in ["<=", ">=", "!=", "<>", "=", "<", ">"] {
        if let Some((lhs_raw, rhs_raw)) = trimmed.split_once(op) {
            let col_name =
                strip_ident_quotes(lhs_raw.trim().rsplit('.').next().unwrap_or(lhs_raw.trim()))
                    .to_ascii_lowercase();
            let rhs_val = eval_sql_expr(rhs_raw, param_iter);
            if let Some((idx, col_def)) = cols
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.to_ascii_lowercase() == col_name)
            {
                let coerced_rhs = coerce_cell_for_column(rhs_val, col_def);
                let cell = &row[idx];
                if matches!(cell, DecodedCell::Null) || matches!(coerced_rhs, DecodedCell::Null) {
                    return false;
                }
                let ord = cell_cmp(cell, &coerced_rhs);
                return match op {
                    "=" => ord == std::cmp::Ordering::Equal,
                    "!=" | "<>" => ord != std::cmp::Ordering::Equal,
                    "<" => ord == std::cmp::Ordering::Less,
                    "<=" => ord != std::cmp::Ordering::Greater,
                    ">" => ord == std::cmp::Ordering::Greater,
                    ">=" => ord != std::cmp::Ordering::Less,
                    _ => false,
                };
            }
        }
    }
    true
}

fn cells_equal(a: &DecodedCell, b: &DecodedCell) -> bool {
    cell_cmp(a, b) == std::cmp::Ordering::Equal
}

fn cell_cmp(a: &DecodedCell, b: &DecodedCell) -> std::cmp::Ordering {
    match (a, b) {
        (DecodedCell::Null, DecodedCell::Null) => std::cmp::Ordering::Equal,
        (DecodedCell::Null, _) => std::cmp::Ordering::Less,
        (_, DecodedCell::Null) => std::cmp::Ordering::Greater,
        (DecodedCell::Int(x), DecodedCell::Int(y)) => x.cmp(y),
        (DecodedCell::UInt(x), DecodedCell::UInt(y)) => x.cmp(y),
        (DecodedCell::Int(x), DecodedCell::UInt(y)) => {
            if *x < 0 {
                std::cmp::Ordering::Less
            } else {
                (*x as u64).cmp(y)
            }
        }
        (DecodedCell::UInt(x), DecodedCell::Int(y)) => {
            if *y < 0 {
                std::cmp::Ordering::Greater
            } else {
                x.cmp(&(*y as u64))
            }
        }
        (DecodedCell::Float(x), DecodedCell::Float(y)) => {
            x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
        }
        (DecodedCell::Text(x), DecodedCell::Text(y)) => x.cmp(y),
        (DecodedCell::Decimal(x), DecodedCell::Decimal(y)) => x.cmp(y),
        (DecodedCell::Bytes(x), DecodedCell::Bytes(y)) => x.cmp(y),
        _ => cell_display(a).cmp(&cell_display(b)),
    }
}

fn cell_display(cell: &DecodedCell) -> String {
    match cell {
        DecodedCell::Null => "NULL".to_string(),
        DecodedCell::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        DecodedCell::Int(i) => i.to_string(),
        DecodedCell::UInt(u) => u.to_string(),
        DecodedCell::Float(f) => f.to_string(),
        DecodedCell::Decimal(s) | DecodedCell::Json(s) | DecodedCell::Text(s) => s.clone(),
        DecodedCell::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        DecodedCell::Date(y, m, d) => format!("{y:04}-{m:02}-{d:02}"),
        DecodedCell::DateTime(y, m, d, hh, mm, ss, us) => {
            format!("{y:04}-{m:02}-{d:02} {hh:02}:{mm:02}:{ss:02}.{us:06}")
        }
        DecodedCell::Time(neg, days, hh, mm, ss, us) => {
            let sign = if *neg { "-" } else { "" };
            format!("{sign}{}:{mm:02}:{ss:02}.{us:06}", days * 24 + hh)
        }
    }
}
