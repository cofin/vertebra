//! Dedicated non-pooled PostgreSQL `CopyBoth` logical replication session handshake, catalog
//! preflight, `IDENTIFY_SYSTEM` verification, and clean `CopyDone` shutdown.

use std::collections::BTreeMap;
use std::time::Duration;

use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use crate::codecs::TEXT_OID;
use crate::config::PostgresConfig;
use crate::errors::PostgresError;
use crate::extended::Parameter;
use crate::protocol::{
    MAX_BACKEND_FRAME_LENGTH, ReplicationBackendMessage, TERMINATE_FRAME, parse_error_fields,
    read_replication_message,
};
use crate::result::{PgValue, TransactionStatus};
use crate::session::statements::PoolerMode;
use crate::session::{
    CommandBudget, ExchangeState, PostgresConnection, establish_standalone_connection,
};

/// Maximum UTF-8 byte length of a PostgreSQL replication slot or publication identifier (`NAMEDATALEN - 1`).
pub const MAX_REPLICATION_IDENTIFIER_BYTES: usize = 63;

const SLOT_PREFLIGHT_SQL: &str = "SELECT pg_catalog.current_database(), s.database, s.slot_type, s.plugin, s.active, s.restart_lsn::pg_catalog.text, s.confirmed_flush_lsn::pg_catalog.text, s.wal_status FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = $1";

const PUBLICATION_PREFLIGHT_PG14_SQL: &str = "SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, pt.schemaname, pt.tablename FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables pt ON pt.pubname = p.pubname WHERE p.pubname = $1 ORDER BY pt.schemaname ASC NULLS FIRST, pt.tablename ASC NULLS FIRST";

const PUBLICATION_PREFLIGHT_PG15_SQL: &str = "SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, pt.schemaname, pt.tablename, pt.attnames, pt.rowfilter FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables pt ON pt.pubname = p.pubname WHERE p.pubname = $1 ORDER BY pt.schemaname ASC NULLS FIRST, pt.tablename ASC NULLS FIRST";

const FINGERPRINT_DOMAIN: &[u8] = b"vertebra-postgres:publication_fingerprint:v1\0";
const SOCKET_DISCARD_GRACE: Duration = Duration::from_millis(50);

/// Verified server, slot, and publication identity captured from catalog preflight and `IDENTIFY_SYSTEM`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedReplicationIdentity {
    /// Cluster `systemid` reported by `IDENTIFY_SYSTEM`.
    pub system_identifier: String,
    /// Current timeline ID reported by `IDENTIFY_SYSTEM`.
    pub timeline: i32,
    /// Verified database name matching both `pg_replication_slots.database` and `IDENTIFY_SYSTEM`.
    pub database: String,
    /// Caller-provisioned logical `pgoutput` replication slot name.
    pub slot: String,
    /// Deterministic lowercase SHA-256 hex digest over publication flags and member definitions.
    pub publication_fingerprint: String,
    /// Slot `confirmed_flush_lsn` observed during preflight.
    pub confirmed_flush_lsn: u64,
    /// Slot `restart_lsn` observed during preflight.
    pub restart_lsn: u64,
}

/// Format a 64-bit PostgreSQL WAL LSN as uppercase `"X/Y"` hexadecimal text.
pub fn format_pg_lsn(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{hi:X}/{lo:X}")
}

/// Parse canonical PostgreSQL `"X/Y"` hexadecimal LSN text into a 64-bit byte offset.
///
/// Requires strictly one `'/'` separator with `1..=8` ASCII hexadecimal digits on each side and
/// rejects whitespace, signs, underscores, or `0x` prefixes.
pub fn parse_pg_lsn(raw: &str) -> Result<u64, PostgresError> {
    let Some((hi_str, lo_str)) = raw.split_once('/') else {
        return Err(PostgresError::Data(format!(
            "invalid PostgreSQL LSN {raw:?}: expected `X/Y` hexadecimal format"
        )));
    };
    if lo_str.contains('/') {
        return Err(PostgresError::Data(format!(
            "invalid PostgreSQL LSN {raw:?}: expected a single `/` separator"
        )));
    }
    let hi = parse_lsn_half(hi_str, raw)?;
    let lo = parse_lsn_half(lo_str, raw)?;
    Ok((u64::from(hi) << 32) | u64::from(lo))
}

/// Dedicated non-pooled PostgreSQL logical replication (`replication=database`) connection in active
/// `CopyBoth` mode.
#[derive(Debug)]
pub struct PostgresReplicationConnection {
    conn: PostgresConnection,
    identity: VerifiedReplicationIdentity,
}

impl PostgresReplicationConnection {
    /// Validate a caller-provisioned logical `pgoutput` slot and publication alongside `IDENTIFY_SYSTEM`
    /// without starting replication or borrowing a query-pool connection.
    pub async fn probe_identity(
        config: &PostgresConfig,
        slot: &str,
        publication: &str,
    ) -> Result<VerifiedReplicationIdentity, PostgresError> {
        ensure_session_capable_pooler(config)?;
        validate_replication_identifier(slot, "slot")?;
        validate_replication_identifier(publication, "publication")?;

        let preflight = run_catalog_preflight(config, slot, publication).await?;
        let mut repl_conn = establish_standalone_connection(config, true).await?;
        let sys_res = run_identify_system(&mut repl_conn, config.connect_timeout).await;
        close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
        let system = sys_res?;
        verify_system_database(&system, &preflight.database)?;

        Ok(build_verified_identity(slot, preflight, system))
    }

    /// Establish a dedicated `replication=database` logical `pgoutput` `CopyBoth` session after
    /// verifying catalog slot/publication metadata and `IDENTIFY_SYSTEM`.
    pub async fn connect_logical(
        config: &PostgresConfig,
        slot: &str,
        publication: &str,
        start_lsn: Option<u64>,
    ) -> Result<Self, PostgresError> {
        ensure_session_capable_pooler(config)?;
        validate_replication_identifier(slot, "slot")?;
        validate_replication_identifier(publication, "publication")?;

        let preflight = run_catalog_preflight(config, slot, publication).await?;
        if let Some(requested_lsn) = start_lsn {
            validate_start_lsn_floor(slot, requested_lsn, &preflight)?;
        }

        let mut repl_conn = establish_standalone_connection(config, true).await?;
        let system = match run_identify_system(&mut repl_conn, config.connect_timeout).await {
            Ok(sys) => sys,
            Err(err) => {
                close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
                return Err(err);
            }
        };
        if let Err(err) = verify_system_database(&system, &preflight.database) {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(err);
        }
        if let Some(requested_lsn) = start_lsn
            && requested_lsn > system.xlogpos
        {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(PostgresError::Operational(format!(
                "requested start_lsn {} exceeds server current WAL flush position {} on slot {slot:?}",
                format_pg_lsn(requested_lsn),
                format_pg_lsn(system.xlogpos),
            )));
        }

        let start_sql = build_start_replication_command(slot, publication, start_lsn);
        let start_deadline = Instant::now() + config.connect_timeout;
        enter_copy_both(&mut repl_conn, &start_sql, start_deadline).await?;

        let identity = build_verified_identity(slot, preflight, system);
        Ok(Self {
            conn: repl_conn,
            identity,
        })
    }

    /// Borrow the verified server, slot, and publication identity captured during session startup.
    pub const fn identity(&self) -> &VerifiedReplicationIdentity {
        &self.identity
    }

    /// Return `true` once this replication connection has closed or discarded its physical socket.
    pub const fn is_closed(&self) -> bool {
        self.conn.is_closed()
    }

    /// Lookup a backend `ParameterStatus` value reported on the replication socket.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.conn.parameter(name)
    }

    /// Return the number of unread backend bytes preserved in `read_buf` (including post-`'W'` frames).
    pub fn buffered_read_len(&self) -> usize {
        self.conn.buffered_read_len()
    }

    /// Send frontend `CopyDone` and drain backend `CopyDone`, `CommandComplete`, and `ReadyForQuery`
    /// within `deadline`, closing and discarding the socket on completion, error, or timeout.
    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), PostgresError> {
        if self.conn.is_closed() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            self.discard_socket().await;
            return Err(PostgresError::Operational(
                "PostgreSQL replication shutdown deadline expired before sending CopyDone"
                    .to_string(),
            ));
        }

        self.conn.write_buf.clear();
        frontend::copy_done(&mut self.conn.write_buf);
        let write_res = tokio::time::timeout_at(deadline, async {
            self.conn.stream.write_all(&self.conn.write_buf).await?;
            self.conn.stream.flush().await
        })
        .await;
        self.conn.write_buf.clear();

        match write_res {
            Ok(Ok(())) => {}
            Ok(Err(io_err)) => {
                self.discard_socket().await;
                return Err(PostgresError::Transport(format!(
                    "PostgreSQL replication CopyDone write failed: {io_err}"
                )));
            }
            Err(_) => {
                self.discard_socket().await;
                return Err(PostgresError::Operational(
                    "PostgreSQL replication shutdown timed out while sending CopyDone".to_string(),
                ));
            }
        }

        let mut saw_copy_done = false;
        let mut saw_command_complete = false;
        let mut backend_err: Option<PostgresError> = None;

        loop {
            let msg = match read_replication_message(
                &mut self.conn.stream,
                &mut self.conn.read_buf,
                MAX_BACKEND_FRAME_LENGTH,
                Some(deadline),
            )
            .await
            {
                Ok(m) => m,
                Err(err) => {
                    let timed_out =
                        matches!(&err, PostgresError::Transport(m) if m.contains("timed out"));
                    self.discard_socket().await;
                    if timed_out {
                        return Err(PostgresError::Operational(
                            "PostgreSQL replication shutdown timed out awaiting ReadyForQuery"
                                .to_string(),
                        ));
                    }
                    return Err(err);
                }
            };

            match msg {
                ReplicationBackendMessage::Standard(Message::CopyData(_)) if !saw_copy_done => {}
                ReplicationBackendMessage::Standard(Message::CopyDone) if !saw_copy_done => {
                    saw_copy_done = true;
                }
                ReplicationBackendMessage::Standard(Message::RowDescription(_))
                | ReplicationBackendMessage::Standard(Message::DataRow(_))
                    if saw_copy_done && !saw_command_complete => {}
                ReplicationBackendMessage::Standard(Message::CommandComplete(_)) => {
                    saw_command_complete = true;
                }
                ReplicationBackendMessage::Standard(Message::NoticeResponse(_)) => {}
                ReplicationBackendMessage::Standard(Message::ParameterStatus(body)) => {
                    if let Err(err) = self.conn.handle_parameter_status_frame(&body) {
                        self.discard_socket().await;
                        return Err(err);
                    }
                }
                ReplicationBackendMessage::Standard(Message::NotificationResponse(body)) => {
                    if let Err(err) = self.conn.handle_notification_frame(&body) {
                        self.discard_socket().await;
                        return Err(err);
                    }
                }
                ReplicationBackendMessage::Standard(Message::ErrorResponse(body)) => {
                    match parse_error_fields(body.fields()) {
                        Ok(srv_err) => {
                            if backend_err.is_none() {
                                backend_err = Some(srv_err);
                            }
                        }
                        Err(proto_err) => {
                            self.discard_socket().await;
                            return Err(proto_err);
                        }
                    }
                }
                ReplicationBackendMessage::Standard(Message::ReadyForQuery(body)) => {
                    let status_res = TransactionStatus::from_wire_tag(body.status());
                    self.terminate_at_deadline(deadline).await;
                    status_res?;
                    if let Some(err) = backend_err {
                        return Err(err);
                    }
                    if !saw_copy_done || !saw_command_complete {
                        return Err(PostgresError::Protocol(format!(
                            "PostgreSQL replication CopyBoth shutdown reached ReadyForQuery with copy_done={saw_copy_done} command_complete={saw_command_complete}"
                        )));
                    }
                    return Ok(());
                }
                _ => {
                    self.discard_socket().await;
                    return Err(PostgresError::Protocol(
                        "unexpected backend frame during PostgreSQL replication CopyDone shutdown"
                            .to_string(),
                    ));
                }
            }
        }
    }

    async fn terminate_at_deadline(&mut self, deadline: Instant) {
        if self.conn.is_closed() {
            return;
        }
        self.conn.mark_closed();
        self.conn.read_buf.clear();
        self.conn.write_buf.clear();
        let terminate_deadline = if Instant::now() < deadline {
            deadline
        } else {
            Instant::now() + SOCKET_DISCARD_GRACE
        };
        let _ = tokio::time::timeout_at(terminate_deadline, async {
            let _ = self.conn.stream.write_all(&TERMINATE_FRAME).await;
            let _ = self.conn.stream.flush().await;
            let _ = self.conn.stream.shutdown().await;
        })
        .await;
    }

    async fn discard_socket(&mut self) {
        if self.conn.is_closed() {
            return;
        }
        self.conn.mark_closed();
        self.conn.read_buf.clear();
        self.conn.write_buf.clear();
        let _ = tokio::time::timeout(SOCKET_DISCARD_GRACE, self.conn.stream.shutdown()).await;
    }
}

impl Drop for PostgresReplicationConnection {
    fn drop(&mut self) {
        if !self.conn.is_closed() {
            self.conn.mark_closed();
        }
    }
}

pub(crate) fn register_pymodule(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}

struct VerifiedPreflight {
    database: String,
    restart_lsn: u64,
    confirmed_flush_lsn: u64,
    publication_fingerprint: String,
}

struct SystemIdentification {
    system_identifier: String,
    timeline: i32,
    xlogpos: u64,
    dbname: Option<String>,
}

fn parse_lsn_half(part: &str, full: &str) -> Result<u32, PostgresError> {
    if part.is_empty() || part.len() > 8 || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PostgresError::Data(format!(
            "invalid PostgreSQL LSN {full:?}: each half must be 1..=8 hexadecimal digits"
        )));
    }
    u32::from_str_radix(part, 16).map_err(|e| {
        PostgresError::Data(format!(
            "invalid PostgreSQL LSN component {part:?} in {full:?}: {e}"
        ))
    })
}

fn ensure_session_capable_pooler(config: &PostgresConfig) -> Result<(), PostgresError> {
    if !matches!(config.pooler_mode, PoolerMode::Direct | PoolerMode::Session) {
        return Err(PostgresError::Interface(format!(
            "PostgreSQL logical replication requires explicit pooler_mode `direct` or `session`, got `{}`",
            config.pooler_mode.as_str()
        )));
    }
    Ok(())
}

fn validate_replication_identifier(name: &str, kind: &str) -> Result<(), PostgresError> {
    if name.is_empty() {
        return Err(PostgresError::Data(format!(
            "PostgreSQL replication {kind} identifier cannot be empty"
        )));
    }
    let byte_len = name.len();
    if byte_len > MAX_REPLICATION_IDENTIFIER_BYTES {
        return Err(PostgresError::Data(format!(
            "PostgreSQL replication {kind} identifier {name:?} is {byte_len} UTF-8 bytes (exceeds maximum {MAX_REPLICATION_IDENTIFIER_BYTES})"
        )));
    }
    if name.as_bytes().contains(&0) {
        return Err(PostgresError::Data(format!(
            "PostgreSQL replication {kind} identifier cannot contain embedded NUL byte"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(PostgresError::Data(format!(
            "PostgreSQL replication {kind} identifier {name:?} cannot contain control characters"
        )));
    }
    Ok(())
}

/// Quote a validated replication slot name as a `repl_scanner.l` double-quoted identifier (`"..."`).
fn quote_replication_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for ch in name.chars() {
        if ch == '"' {
            out.push_str("\"\"");
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

/// Format a validated single publication identifier for `START_REPLICATION ... (publication_names '...')`.
///
/// PostgreSQL parses option values in two stages:
/// 1. `src/backend/replication/repl_scanner.l` scans standard `'...'` strings where `''` escapes
///    single quotes and backslashes remain literal (`xqinside = [^\']+`, with no `E'...'` prefix).
/// 2. `pgoutput.c` passes the decoded string into `SplitIdentifierString(..., ',')`, which folds
///    unquoted identifiers to lowercase and splits on unquoted commas/whitespace while preserving
///    case, commas, spaces, quotes (`""`), and backslashes inside `"..."`.
///
/// Simple lowercase ASCII SQL identifiers emit bare `'pub_name'`; any identifier containing
/// uppercase letters, commas, whitespace, quotes, backslashes, or other punctuation wraps inside
/// double quotes (`""` escaping `"`) before outer single-quote doubling (`''`), preventing option
/// or multi-publication injection while matching catalog `pubname` identically.
fn quote_publication_names_option(publication: &str) -> String {
    let ident_token = if is_simple_lowercase_sql_identifier(publication) {
        publication.to_owned()
    } else {
        quote_replication_identifier(publication)
    };
    let mut out = String::with_capacity(ident_token.len() + 2);
    out.push('\'');
    for ch in ident_token.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn is_simple_lowercase_sql_identifier(name: &str) -> bool {
    let bytes = name.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    let valid_start = first.is_ascii_lowercase() || first == b'_';
    valid_start
        && bytes[1..]
            .iter()
            .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn build_start_replication_command(
    slot: &str,
    publication: &str,
    start_lsn: Option<u64>,
) -> String {
    let quoted_slot = quote_replication_identifier(slot);
    let lsn_str = start_lsn.map_or_else(|| "0/0".to_string(), format_pg_lsn);
    let quoted_pub = quote_publication_names_option(publication);
    format!(
        "START_REPLICATION SLOT {quoted_slot} LOGICAL {lsn_str} (proto_version '1', publication_names {quoted_pub}, binary 'false', streaming 'false', messages 'true')"
    )
}

async fn run_catalog_preflight(
    config: &PostgresConfig,
    slot: &str,
    publication: &str,
) -> Result<VerifiedPreflight, PostgresError> {
    let mut meta_conn = establish_standalone_connection(config, false).await?;
    let outcome =
        inspect_slot_and_publication(&mut meta_conn, config.connect_timeout, slot, publication)
            .await;
    close_connection_bounded(&mut meta_conn, config.connect_timeout).await;
    outcome
}

async fn inspect_slot_and_publication(
    meta_conn: &mut PostgresConnection,
    timeout: Duration,
    slot: &str,
    publication: &str,
) -> Result<VerifiedPreflight, PostgresError> {
    let slot_params = [Parameter::new(PgValue::Text(slot.to_owned()))];
    let text_oids = [TEXT_OID];
    let slot_res = meta_conn
        .query_with_deadline(
            SLOT_PREFLIGHT_SQL,
            &slot_params,
            Some(&text_oids),
            CommandBudget::from_deadline(Some(Instant::now() + timeout)),
        )
        .await?;

    if slot_res.rows.is_empty() {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL logical replication slot {slot:?} does not exist"
        )));
    }
    if slot_res.rows.len() > 1 {
        return Err(PostgresError::Protocol(format!(
            "expected 1 pg_replication_slots row for {slot:?}, got {}",
            slot_res.rows.len()
        )));
    }
    let row = &slot_res.rows[0];
    if row.len() != 8 {
        return Err(PostgresError::Protocol(format!(
            "expected 8 columns from pg_replication_slots preflight, got {}",
            row.len()
        )));
    }

    let current_db = expect_non_null_text(&row[0], "current_database")?;
    let slot_type = expect_non_null_text(&row[2], "pg_replication_slots.slot_type")?;
    if slot_type != "logical" {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL replication slot {slot:?} has slot_type {slot_type:?}; logical `pgoutput` slot required"
        )));
    }

    let slot_db = match &row[1] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(PostgresError::Operational(format!(
                "PostgreSQL logical replication slot {slot:?} has NULL database"
            )));
        }
        other => {
            return Err(PostgresError::Protocol(format!(
                "unexpected pg_replication_slots.database value: {other:?}"
            )));
        }
    };
    if slot_db != current_db {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL logical replication slot {slot:?} belongs to database {slot_db:?}, connected to {current_db:?}"
        )));
    }

    let plugin = match &row[3] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(PostgresError::Operational(format!(
                "PostgreSQL logical replication slot {slot:?} has NULL output plugin; `pgoutput` required"
            )));
        }
        other => {
            return Err(PostgresError::Protocol(format!(
                "unexpected pg_replication_slots.plugin value: {other:?}"
            )));
        }
    };
    if plugin != "pgoutput" {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL logical replication slot {slot:?} uses output plugin {plugin:?}; `pgoutput` is required"
        )));
    }

    let active = expect_bool(&row[4], "pg_replication_slots.active")?;
    if active {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL logical replication slot {slot:?} is already active"
        )));
    }

    if let PgValue::Text(wal_status) = &row[7]
        && wal_status.eq_ignore_ascii_case("lost")
    {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL logical replication slot {slot:?} WAL is invalidated (wal_unavailable: wal_status={wal_status:?})"
        )));
    }

    let restart_lsn_raw = match &row[5] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(PostgresError::Operational(format!(
                "PostgreSQL logical replication slot {slot:?} has NULL restart_lsn (wal_unavailable)"
            )));
        }
        other => {
            return Err(PostgresError::Protocol(format!(
                "unexpected pg_replication_slots.restart_lsn value: {other:?}"
            )));
        }
    };
    let restart_lsn = parse_pg_lsn(restart_lsn_raw).map_err(|_| {
        PostgresError::Protocol(format!(
            "malformed pg_replication_slots.restart_lsn {restart_lsn_raw:?} for slot {slot:?}"
        ))
    })?;

    let confirmed_raw = match &row[6] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(PostgresError::Operational(format!(
                "PostgreSQL logical replication slot {slot:?} has NULL confirmed_flush_lsn"
            )));
        }
        other => {
            return Err(PostgresError::Protocol(format!(
                "unexpected pg_replication_slots.confirmed_flush_lsn value: {other:?}"
            )));
        }
    };
    let confirmed_flush_lsn = parse_pg_lsn(confirmed_raw).map_err(|_| {
        PostgresError::Protocol(format!(
            "malformed pg_replication_slots.confirmed_flush_lsn {confirmed_raw:?} for slot {slot:?}"
        ))
    })?;

    let server_version_num = parse_server_version_num(meta_conn.parameters());
    let include_pg15_cols = server_version_num >= 150_000;
    let pub_sql = if include_pg15_cols {
        PUBLICATION_PREFLIGHT_PG15_SQL
    } else {
        PUBLICATION_PREFLIGHT_PG14_SQL
    };
    let pub_params = [Parameter::new(PgValue::Text(publication.to_owned()))];
    let pub_res = meta_conn
        .query_with_deadline(
            pub_sql,
            &pub_params,
            Some(&text_oids),
            CommandBudget::from_deadline(Some(Instant::now() + timeout)),
        )
        .await?;

    if pub_res.rows.is_empty() {
        return Err(PostgresError::Operational(format!(
            "PostgreSQL publication {publication:?} does not exist"
        )));
    }
    let publication_fingerprint =
        hash_publication_rows(publication, &pub_res.rows, include_pg15_cols)?;

    Ok(VerifiedPreflight {
        database: current_db.to_owned(),
        restart_lsn,
        confirmed_flush_lsn,
        publication_fingerprint,
    })
}

fn validate_start_lsn_floor(
    slot: &str,
    requested_lsn: u64,
    preflight: &VerifiedPreflight,
) -> Result<(), PostgresError> {
    if requested_lsn < preflight.restart_lsn {
        return Err(PostgresError::Operational(format!(
            "requested start_lsn {} is behind slot {slot:?} restart_lsn {} (wal_unavailable)",
            format_pg_lsn(requested_lsn),
            format_pg_lsn(preflight.restart_lsn),
        )));
    }
    if requested_lsn < preflight.confirmed_flush_lsn {
        return Err(PostgresError::Operational(format!(
            "requested start_lsn {} rewinds behind slot {slot:?} confirmed_flush_lsn {} (stale checkpoint rewind; wal_unavailable)",
            format_pg_lsn(requested_lsn),
            format_pg_lsn(preflight.confirmed_flush_lsn),
        )));
    }
    Ok(())
}

fn parse_server_version_num(parameters: &BTreeMap<String, String>) -> u32 {
    if let Some(raw_num) = parameters.get("server_version_num")
        && let Ok(parsed) = raw_num.trim().parse::<u32>()
    {
        return parsed;
    }
    let Some(raw_ver) = parameters.get("server_version") else {
        return 0;
    };
    let token = raw_ver.split_whitespace().next().unwrap_or("");
    let mut parts = token.split('.');
    let Some(major) = parts.next().and_then(parse_leading_u32) else {
        return 0;
    };
    let minor = parts.next().and_then(parse_leading_u32).unwrap_or(0);
    if major >= 10 {
        major.saturating_mul(10_000).saturating_add(minor)
    } else {
        let patch = parts.next().and_then(parse_leading_u32).unwrap_or(0);
        major
            .saturating_mul(10_000)
            .saturating_add(minor.saturating_mul(100))
            .saturating_add(patch)
    }
}

fn parse_leading_u32(segment: &str) -> Option<u32> {
    let digits_len = segment.bytes().take_while(u8::is_ascii_digit).count();
    if digits_len == 0 {
        return None;
    }
    segment[..digits_len].parse::<u32>().ok()
}

fn hash_publication_rows(
    expected_publication: &str,
    rows: &[Vec<PgValue>],
    include_pg15_cols: bool,
) -> Result<String, PostgresError> {
    let expected_cols = if include_pg15_cols { 11 } else { 9 };
    let mut hasher = Sha256::new();
    hasher.update(FINGERPRINT_DOMAIN);
    hasher.update([u8::from(include_pg15_cols)]);
    hasher.update((rows.len() as u64).to_be_bytes());

    for (row_idx, row) in rows.iter().enumerate() {
        if row.len() != expected_cols {
            return Err(PostgresError::Protocol(format!(
                "expected {expected_cols} columns from pg_publication preflight row {row_idx}, got {}",
                row.len()
            )));
        }
        hasher.update((row_idx as u64).to_be_bytes());
        let pubname = expect_non_null_text(&row[0], "pg_publication.pubname")?;
        if pubname != expected_publication {
            return Err(PostgresError::Protocol(format!(
                "pg_publication returned {pubname:?} while querying {expected_publication:?}"
            )));
        }
        hash_scalar_text(&mut hasher, pubname);
        for (col_idx, label) in [
            (1, "puballtables"),
            (2, "pubinsert"),
            (3, "pubupdate"),
            (4, "pubdelete"),
            (5, "pubtruncate"),
            (6, "pubviaroot"),
        ] {
            let flag = expect_bool(&row[col_idx], label)?;
            hasher.update([0x02, u8::from(flag)]);
        }
        hash_nullable_text(&mut hasher, &row[7], "pg_publication_tables.schemaname")?;
        hash_nullable_text(&mut hasher, &row[8], "pg_publication_tables.tablename")?;
        if include_pg15_cols {
            hash_attnames_value(&mut hasher, &row[9])?;
            hash_nullable_text(&mut hasher, &row[10], "pg_publication_tables.rowfilter")?;
        }
    }

    let digest = hasher.finalize();
    Ok(encode_hex_lower(&digest))
}

fn hash_scalar_text(hasher: &mut Sha256, val: &str) {
    hasher.update([0x01]);
    hasher.update((val.len() as u64).to_be_bytes());
    hasher.update(val.as_bytes());
}

fn hash_nullable_text(
    hasher: &mut Sha256,
    val: &PgValue,
    label: &str,
) -> Result<(), PostgresError> {
    match val {
        PgValue::Null => {
            hasher.update([0x00]);
            Ok(())
        }
        PgValue::Text(s) => {
            hash_scalar_text(hasher, s);
            Ok(())
        }
        other => Err(PostgresError::Protocol(format!(
            "unexpected {label} value in publication preflight: {other:?}"
        ))),
    }
}

fn hash_attnames_value(hasher: &mut Sha256, val: &PgValue) -> Result<(), PostgresError> {
    match val {
        PgValue::Null => {
            hasher.update([0x00]);
            Ok(())
        }
        PgValue::Text(s) => {
            hash_scalar_text(hasher, s);
            Ok(())
        }
        PgValue::Array {
            dimensions, values, ..
        } => {
            hasher.update([0x03]);
            hasher.update((dimensions.len() as u64).to_be_bytes());
            for &dim in dimensions {
                hasher.update((dim as u64).to_be_bytes());
            }
            hasher.update((values.len() as u64).to_be_bytes());
            for elem in values {
                hash_attnames_value(hasher, elem)?;
            }
            Ok(())
        }
        other => Err(PostgresError::Protocol(format!(
            "unexpected pg_publication_tables.attnames value: {other:?}"
        ))),
    }
}

fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0F) as usize] as char);
    }
    out
}

fn expect_non_null_text<'a>(val: &'a PgValue, label: &str) -> Result<&'a str, PostgresError> {
    match val {
        PgValue::Text(s) => Ok(s.as_str()),
        other => Err(PostgresError::Protocol(format!(
            "expected non-null text for {label}, got {other:?}"
        ))),
    }
}

fn expect_bool(val: &PgValue, label: &str) -> Result<bool, PostgresError> {
    match val {
        PgValue::Bool(b) => Ok(*b),
        other => Err(PostgresError::Protocol(format!(
            "expected boolean for {label}, got {other:?}"
        ))),
    }
}

async fn run_identify_system(
    repl_conn: &mut PostgresConnection,
    timeout: Duration,
) -> Result<SystemIdentification, PostgresError> {
    let mut results = repl_conn
        .simple_query_with_deadline(
            "IDENTIFY_SYSTEM",
            CommandBudget::from_deadline(Some(Instant::now() + timeout)),
        )
        .await?;
    if results.len() != 1 {
        return Err(PostgresError::Protocol(format!(
            "expected 1 result set from IDENTIFY_SYSTEM, got {}",
            results.len()
        )));
    }
    let res = results.remove(0);
    if res.rows.len() != 1 {
        return Err(PostgresError::Protocol(format!(
            "expected 1 row from IDENTIFY_SYSTEM, got {}",
            res.rows.len()
        )));
    }
    let row = &res.rows[0];
    if row.len() < 3 {
        return Err(PostgresError::Protocol(format!(
            "expected at least 3 columns from IDENTIFY_SYSTEM, got {}",
            row.len()
        )));
    }

    let system_identifier = expect_non_null_text(&row[0], "IDENTIFY_SYSTEM.systemid")?.to_owned();
    if system_identifier.is_empty() {
        return Err(PostgresError::Protocol(
            "IDENTIFY_SYSTEM returned an empty systemid".to_string(),
        ));
    }

    let timeline = match &row[1] {
        PgValue::Int4(v) => *v,
        PgValue::Int2(v) => i32::from(*v),
        PgValue::Int8(v) => i32::try_from(*v).map_err(|_| {
            PostgresError::Protocol(format!("IDENTIFY_SYSTEM timeline {v} overflows i32"))
        })?,
        PgValue::Text(s) => s.parse::<i32>().map_err(|e| {
            PostgresError::Protocol(format!("invalid IDENTIFY_SYSTEM timeline {s:?}: {e}"))
        })?,
        other => {
            return Err(PostgresError::Protocol(format!(
                "unexpected IDENTIFY_SYSTEM.timeline value: {other:?}"
            )));
        }
    };
    if timeline < 1 {
        return Err(PostgresError::Protocol(format!(
            "IDENTIFY_SYSTEM returned non-positive timeline {timeline}"
        )));
    }

    let xlogpos_raw = expect_non_null_text(&row[2], "IDENTIFY_SYSTEM.xlogpos")?;
    let xlogpos = parse_pg_lsn(xlogpos_raw).map_err(|_| {
        PostgresError::Protocol(format!("malformed IDENTIFY_SYSTEM.xlogpos {xlogpos_raw:?}"))
    })?;

    let dbname = match row.get(3) {
        None | Some(PgValue::Null) => None,
        Some(PgValue::Text(s)) => Some(s.clone()),
        Some(other) => {
            return Err(PostgresError::Protocol(format!(
                "unexpected IDENTIFY_SYSTEM.dbname value: {other:?}"
            )));
        }
    };

    Ok(SystemIdentification {
        system_identifier,
        timeline,
        xlogpos,
        dbname,
    })
}

fn verify_system_database(
    system: &SystemIdentification,
    expected_database: &str,
) -> Result<(), PostgresError> {
    match system.dbname.as_deref() {
        Some(db) if db == expected_database => Ok(()),
        Some(db) => Err(PostgresError::Operational(format!(
            "IDENTIFY_SYSTEM reported database {db:?}, expected {expected_database:?}"
        ))),
        None => Err(PostgresError::Operational(format!(
            "IDENTIFY_SYSTEM returned NULL dbname on database-scoped replication session for {expected_database:?}"
        ))),
    }
}

fn build_verified_identity(
    slot: &str,
    preflight: VerifiedPreflight,
    system: SystemIdentification,
) -> VerifiedReplicationIdentity {
    VerifiedReplicationIdentity {
        system_identifier: system.system_identifier,
        timeline: system.timeline,
        database: preflight.database,
        slot: slot.to_owned(),
        publication_fingerprint: preflight.publication_fingerprint,
        confirmed_flush_lsn: preflight.confirmed_flush_lsn,
        restart_lsn: preflight.restart_lsn,
    }
}

async fn enter_copy_both(
    repl_conn: &mut PostgresConnection,
    start_sql: &str,
    deadline: Instant,
) -> Result<(), PostgresError> {
    if let Err(err) = repl_conn
        .send_simple_command(start_sql, Some(deadline))
        .await
    {
        discard_raw_connection(repl_conn).await;
        return Err(err);
    }
    repl_conn.set_exchange_state(ExchangeState::CopyActive);

    loop {
        let msg = match read_replication_message(
            &mut repl_conn.stream,
            &mut repl_conn.read_buf,
            MAX_BACKEND_FRAME_LENGTH,
            Some(deadline),
        )
        .await
        {
            Ok(m) => m,
            Err(err) => {
                discard_raw_connection(repl_conn).await;
                return Err(err);
            }
        };

        match msg {
            ReplicationBackendMessage::CopyBothResponse {
                format: 0,
                column_formats,
            } if column_formats.is_empty() => {
                return Ok(());
            }
            ReplicationBackendMessage::CopyBothResponse {
                format,
                column_formats,
            } => {
                discard_raw_connection(repl_conn).await;
                return Err(PostgresError::Protocol(format!(
                    "logical CopyBoth requires format=0 and 0 columns, got format={format} columns={}",
                    column_formats.len()
                )));
            }
            ReplicationBackendMessage::Standard(Message::NoticeResponse(_)) => {}
            ReplicationBackendMessage::Standard(Message::ParameterStatus(body)) => {
                if let Err(err) = repl_conn.handle_parameter_status_frame(&body) {
                    discard_raw_connection(repl_conn).await;
                    return Err(err);
                }
            }
            ReplicationBackendMessage::Standard(Message::NotificationResponse(body)) => {
                if let Err(err) = repl_conn.handle_notification_frame(&body) {
                    discard_raw_connection(repl_conn).await;
                    return Err(err);
                }
            }
            ReplicationBackendMessage::Standard(Message::ErrorResponse(body)) => {
                let err = match parse_error_fields(body.fields()) {
                    Ok(srv_err) => srv_err,
                    Err(proto_err) => {
                        discard_raw_connection(repl_conn).await;
                        return Err(proto_err);
                    }
                };
                drain_after_start_error(repl_conn, deadline).await;
                close_connection_until(repl_conn, deadline).await;
                return Err(err);
            }
            ReplicationBackendMessage::Standard(_) => {
                discard_raw_connection(repl_conn).await;
                return Err(PostgresError::Protocol(
                    "unexpected backend frame while awaiting CopyBothResponse ('W')".to_string(),
                ));
            }
        }
    }
}

async fn drain_after_start_error(repl_conn: &mut PostgresConnection, deadline: Instant) {
    loop {
        match read_replication_message(
            &mut repl_conn.stream,
            &mut repl_conn.read_buf,
            MAX_BACKEND_FRAME_LENGTH,
            Some(deadline),
        )
        .await
        {
            Ok(ReplicationBackendMessage::Standard(Message::ReadyForQuery(body))) => {
                repl_conn.set_exchange_state(ExchangeState::Idle);
                if let Ok(status) = TransactionStatus::from_wire_tag(body.status()) {
                    repl_conn.set_transaction_status_from_wire(status);
                }
                return;
            }
            Ok(ReplicationBackendMessage::Standard(Message::NoticeResponse(_)))
            | Ok(ReplicationBackendMessage::Standard(Message::ErrorResponse(_))) => {}
            Ok(ReplicationBackendMessage::Standard(Message::ParameterStatus(body))) => {
                if repl_conn.handle_parameter_status_frame(&body).is_err() {
                    return;
                }
            }
            Ok(_) | Err(_) => {
                discard_raw_connection(repl_conn).await;
                return;
            }
        }
    }
}

async fn close_connection_bounded(conn: &mut PostgresConnection, timeout: Duration) {
    if conn.is_closed() {
        return;
    }
    if tokio::time::timeout(timeout, conn.close()).await.is_err() {
        discard_raw_connection(conn).await;
    }
}

async fn close_connection_until(conn: &mut PostgresConnection, deadline: Instant) {
    if conn.is_closed() {
        return;
    }
    let effective = if Instant::now() < deadline {
        deadline
    } else {
        Instant::now() + SOCKET_DISCARD_GRACE
    };
    if tokio::time::timeout_at(effective, conn.close())
        .await
        .is_err()
    {
        discard_raw_connection(conn).await;
    }
}

async fn discard_raw_connection(conn: &mut PostgresConnection) {
    if !conn.is_closed() {
        conn.mark_closed();
    }
    conn.read_buf.clear();
    conn.write_buf.clear();
    let _ = tokio::time::timeout(SOCKET_DISCARD_GRACE, conn.stream.shutdown()).await;
}
