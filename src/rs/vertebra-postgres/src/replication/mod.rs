//! Dedicated non-pooled PostgreSQL `CopyBoth` logical replication session handshake, catalog
//! preflight, publication custom-type preloading, `IDENTIFY_SYSTEM` verification, `pgoutput`
//! tuple hydration, and clean `CopyDone` shutdown.

pub mod feedback;
pub mod pgoutput;
pub mod stream;
pub mod supervisor;
pub use stream::{ReplicationDelivery, ReplicationSession};
pub use supervisor::{
    MAX_RAW_RECONNECT_ATTEMPTS, PostgresReplicationResume, ReplicationEventDelivery,
    ReplicationStreamEvent, ReplicationSupervisor, raw_reconnect_delay,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use pyo3::exceptions::{PyStopAsyncIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::Notify;
use tokio::time::Instant;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, spawn_rust_db_task_with_attr, spawn_rust_db_task_with_attr_and_cancel,
    spawn_rust_db_task_with_attr_and_future,
};

use crate::async_client::POSTGRES_COMPLETION_DRAINER_ATTR;
use crate::codecs::{CodecContext, OID_OID, TEXT_OID};
use crate::config::PostgresConfig;
use crate::errors::{PostgresError, postgres_error_to_py};
use crate::extended::Parameter;
use crate::protocol::{
    MAX_BACKEND_FRAME_LENGTH, ReplicationBackendMessage, TERMINATE_FRAME, parse_error_fields,
    read_replication_message,
};
use crate::python_values::pg_value_to_python;
use crate::result::{PgValue, QueryResult, TransactionStatus};
use crate::session::statements::PoolerMode;
use crate::session::{
    CommandBudget, ExchangeState, PostgresConnection, establish_standalone_connection,
};
use crate::types::{CompositeField, MAX_DISCOVERY_ROUNDS, TypeCatalog, TypeInfo, TypeKind};

pub use pgoutput::{
    ActiveLogicalTx, BoundedReplicationQueue, DEFAULT_MAX_QUEUE_BYTES, DEFAULT_MAX_QUEUE_RECORDS,
    DEFAULT_MAX_RECORD_BYTES, DEFAULT_MAX_RELATIONS, OldTupleKind, PgOutputColumn, PgOutputDecoder,
    PgOutputFrame, PgOutputRelationSchema, PgOutputTupleValue, ReplicationDedupKey,
    ReplicationRecord,
};

/// Maximum UTF-8 byte length of a PostgreSQL replication slot or publication identifier (`NAMEDATALEN - 1`).
pub const MAX_REPLICATION_IDENTIFIER_BYTES: usize = 63;

const SLOT_PREFLIGHT_SQL: &str = "SELECT pg_catalog.current_database(), s.database, s.slot_type, s.plugin, s.active, s.restart_lsn::pg_catalog.text, s.confirmed_flush_lsn::pg_catalog.text, s.wal_status FROM pg_catalog.pg_replication_slots s WHERE s.slot_name = $1";

const PUBLICATION_PREFLIGHT_PG14_SQL: &str = "SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, pt.schemaname, pt.tablename FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables pt ON pt.pubname = p.pubname WHERE p.pubname = $1 ORDER BY pt.schemaname ASC NULLS FIRST, pt.tablename ASC NULLS FIRST";

const PUBLICATION_PREFLIGHT_PG15_SQL: &str = "SELECT p.pubname, p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate, p.pubviaroot, pt.schemaname, pt.tablename, pt.attnames, pt.rowfilter FROM pg_catalog.pg_publication p LEFT JOIN pg_catalog.pg_publication_tables pt ON pt.pubname = p.pubname WHERE p.pubname = $1 ORDER BY pt.schemaname ASC NULLS FIRST, pt.tablename ASC NULLS FIRST";

const PUBLICATION_TYPES_PRELOAD_SQL: &str = "SELECT DISTINCT t.oid, tn.nspname, t.typname, t.typtype::pg_catalog.text, t.typcategory::pg_catalog.text, t.typdelim::pg_catalog.text, t.typelem, t.typbasetype, rng.rngsubtype, mrng.rngtypid, mrng.rngsubtype, ext.extname, ca.attname, ca.atttypid, ca.atttypmod, ca.attnum FROM pg_catalog.pg_publication_tables pt JOIN pg_catalog.pg_namespace rn ON rn.nspname = pt.schemaname JOIN pg_catalog.pg_class c ON c.relnamespace = rn.oid AND c.relname = pt.tablename JOIN pg_catalog.pg_attribute col ON col.attrelid = c.oid AND col.attnum > 0 AND NOT col.attisdropped JOIN pg_catalog.pg_type t ON t.oid = col.atttypid JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace LEFT JOIN pg_catalog.pg_range rng ON rng.rngtypid = t.oid LEFT JOIN pg_catalog.pg_range mrng ON mrng.rngmultitypid = t.oid LEFT JOIN pg_catalog.pg_depend dep ON dep.classid = 1247 AND dep.objid = t.oid AND dep.deptype = 'e' LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = dep.refobjid LEFT JOIN pg_catalog.pg_attribute ca ON t.typtype = 'c' AND ca.attrelid = t.typrelid AND ca.attnum > 0 AND NOT ca.attisdropped WHERE pt.pubname = $1 ORDER BY t.oid ASC, ca.attnum ASC NULLS FIRST";

const MAX_PRELOAD_DISCOVERY_ROWS: usize = 4096;
const MAX_PRELOAD_OID_BATCH: usize = 128;

const FINGERPRINT_DOMAIN: &[u8] = b"vertebra-postgres:publication_fingerprint:v1\0";
const SOCKET_DISCARD_GRACE: Duration = Duration::from_millis(50);

struct CachedReplicationPyTypes {
    pg_unchanged_toast: Py<PyAny>,
    replication_identity: Py<PyAny>,
    replication_record: Py<PyAny>,
}

static REPLICATION_PY_TYPES: PyOnceLock<CachedReplicationPyTypes> = PyOnceLock::new();

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
    type_catalog: TypeCatalog,
    type_identities: HashMap<u32, (String, String)>,
    codec_ctx: CodecContext,
    effective_start_lsn: u64,
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
    /// verifying catalog slot/publication metadata, preloading published custom types, and running `IDENTIFY_SYSTEM`.
    pub async fn connect_logical(
        config: &PostgresConfig,
        slot: &str,
        publication: &str,
        start_lsn: Option<u64>,
    ) -> Result<Self, PostgresError> {
        ensure_session_capable_pooler(config)?;
        validate_replication_identifier(slot, "slot")?;
        validate_replication_identifier(publication, "publication")?;

        let (preflight, type_catalog, type_identities) =
            run_connect_preflight(config, slot, publication, start_lsn).await?;

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

        let codec_ctx = CodecContext::from_parameters(repl_conn.parameters());
        let effective_start_lsn = start_lsn.unwrap_or(preflight.confirmed_flush_lsn);
        let identity = build_verified_identity(slot, preflight, system);
        Ok(Self {
            conn: repl_conn,
            identity,
            type_catalog,
            type_identities,
            codec_ctx,
            effective_start_lsn,
        })
    }

    /// Re-establish a dedicated `replication=database` logical `pgoutput` `CopyBoth` session after
    /// verifying that server `system_identifier`, database, slot, and publication fingerprint match
    /// `expected` without timeline regression, resuming monotonically from `max(start_lsn, confirmed_flush_lsn)`.
    pub async fn connect_logical_verified(
        config: &PostgresConfig,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        expected: &VerifiedReplicationIdentity,
    ) -> Result<Self, PostgresError> {
        Self::connect_logical_verified_outcome(config, slot, publication, start_lsn, expected)
            .await
            .map_err(VerifiedConnectFailure::into_error)
    }

    pub(super) async fn connect_logical_verified_outcome(
        config: &PostgresConfig,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        expected: &VerifiedReplicationIdentity,
    ) -> Result<Self, VerifiedConnectFailure> {
        ensure_session_capable_pooler(config).map_err(VerifiedConnectFailure::Other)?;
        validate_replication_identifier(slot, "slot").map_err(VerifiedConnectFailure::Other)?;
        validate_replication_identifier(publication, "publication")
            .map_err(VerifiedConnectFailure::Other)?;
        if slot != expected.slot {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "PostgreSQL logical replication slot mismatch on resume: requested {slot:?}, expected {:?} (identity_mismatch)",
                    expected.slot
                ),
            )));
        }

        let (preflight, resume_lsn, type_catalog, type_identities) =
            run_verified_connect_preflight(config, slot, publication, start_lsn, expected).await?;

        let mut repl_conn = establish_standalone_connection(config, true)
            .await
            .map_err(VerifiedConnectFailure::Other)?;
        let system = match run_identify_system(&mut repl_conn, config.connect_timeout).await {
            Ok(sys) => sys,
            Err(err) => {
                close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
                return Err(VerifiedConnectFailure::Other(err));
            }
        };
        if let Err(err) = verify_system_database(&system, &preflight.database) {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(VerifiedConnectFailure::Other(err));
        }
        if system.system_identifier != expected.system_identifier {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "IDENTIFY_SYSTEM systemid mismatch on resume: got {:?}, expected {:?} (identity_mismatch)",
                    system.system_identifier, expected.system_identifier
                ),
            )));
        }
        if system.timeline < expected.timeline {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "IDENTIFY_SYSTEM timeline regressed on resume: got {}, expected >= {} (identity_mismatch)",
                    system.timeline, expected.timeline
                ),
            )));
        }
        if resume_lsn > system.xlogpos {
            close_connection_bounded(&mut repl_conn, config.connect_timeout).await;
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "requested resume LSN {} exceeds server current WAL flush position {} on slot {slot:?} (wal_unavailable)",
                    format_pg_lsn(resume_lsn),
                    format_pg_lsn(system.xlogpos),
                ),
            )));
        }

        let start_sql = build_start_replication_command(slot, publication, Some(resume_lsn));
        let start_deadline = Instant::now() + config.connect_timeout;
        enter_copy_both(&mut repl_conn, &start_sql, start_deadline)
            .await
            .map_err(VerifiedConnectFailure::Other)?;

        let codec_ctx = CodecContext::from_parameters(repl_conn.parameters());
        let identity = build_verified_identity(slot, preflight, system);
        Ok(Self {
            conn: repl_conn,
            identity,
            type_catalog,
            type_identities,
            codec_ctx,
            effective_start_lsn: resume_lsn,
        })
    }

    pub(super) const fn effective_start_lsn(&self) -> u64 {
        self.effective_start_lsn
    }

    /// Borrow the verified server, slot, and publication identity captured during session startup.
    pub const fn identity(&self) -> &VerifiedReplicationIdentity {
        &self.identity
    }

    /// Borrow the preloaded session [`TypeCatalog`] populated on the metadata socket before `CopyBoth`.
    pub const fn type_catalog(&self) -> &TypeCatalog {
        &self.type_catalog
    }

    /// Mutably borrow the session [`TypeCatalog`] for streaming `pgoutput` `'Y'` mismatch invalidation.
    pub fn type_catalog_mut(&mut self) -> &mut TypeCatalog {
        &mut self.type_catalog
    }

    /// Borrow the preloaded `(namespace, name)` identity map keyed by custom `pg_type.oid`.
    pub const fn type_identities(&self) -> &HashMap<u32, (String, String)> {
        &self.type_identities
    }

    /// Borrow the session [`CodecContext`] captured from replication backend `ParameterStatus` frames.
    pub const fn codec_context(&self) -> &CodecContext {
        &self.codec_ctx
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
                    self.codec_ctx = CodecContext::from_parameters(self.conn.parameters());
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

/// Project a single replication [`PgOutputTupleValue`] into Python without leaking `UnchangedToast`
/// into SQL [`PgValue`] or parameter binding codecs.
pub fn pgoutput_tuple_value_to_python(
    py: Python<'_>,
    value: &PgOutputTupleValue,
) -> PyResult<Py<PyAny>> {
    match value {
        PgOutputTupleValue::Null => Ok(py.None()),
        PgOutputTupleValue::UnchangedToast => {
            let py_types = resolve_replication_py_types(py)?;
            Ok(py_types.pg_unchanged_toast.clone_ref(py))
        }
        PgOutputTupleValue::Value(val) => pg_value_to_python(py, val),
    }
}

/// Project a decoded `pgoutput` tuple aligned with `relation` into an ordered Python `dict[str, Any]`.
pub fn pgoutput_tuple_to_python_dict(
    py: Python<'_>,
    relation: &PgOutputRelationSchema,
    tuple: &[PgOutputTupleValue],
) -> PyResult<Py<PyDict>> {
    if relation.columns.len() != tuple.len() {
        return Err(crate::errors::postgres_error_to_py(
            py,
            PostgresError::Protocol(format!(
                "pgoutput tuple width {} does not match relation {}.{} width {}",
                tuple.len(),
                relation.namespace,
                relation.name,
                relation.columns.len()
            )),
        ));
    }
    let out = PyDict::new(py);
    for (col, cell) in relation.columns.iter().zip(tuple.iter()) {
        let py_cell = pgoutput_tuple_value_to_python(py, cell)?;
        out.set_item(col.name.as_str(), py_cell)?;
    }
    Ok(out.unbind())
}

/// Project a [`VerifiedReplicationIdentity`] into `vertebra.db.adapters.postgres.types.PostgresReplicationIdentity`.
pub fn verified_identity_to_python(
    py: Python<'_>,
    identity: &VerifiedReplicationIdentity,
) -> PyResult<Py<PyAny>> {
    let py_types = resolve_replication_py_types(py)?;
    Ok(py_types
        .replication_identity
        .bind(py)
        .call1((
            identity.system_identifier.as_str(),
            identity.database.as_str(),
            identity.slot.as_str(),
            identity.publication_fingerprint.as_str(),
        ))?
        .unbind())
}

fn old_tuple_kind_str(kind: OldTupleKind) -> &'static str {
    match kind {
        OldTupleKind::Key => "key",
        OldTupleKind::Full => "full",
    }
}

fn dedup_key_to_python<'py>(
    py: Python<'py>,
    dedup: &ReplicationDedupKey,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("system_identifier", dedup.system_identifier.as_str())?;
    out.set_item("slot", dedup.slot.as_str())?;
    out.set_item("begin_final_lsn", dedup.begin_final_lsn)?;
    out.set_item("transaction_ordinal", dedup.transaction_ordinal)?;
    Ok(out)
}

fn relation_schema_to_python<'py>(
    py: Python<'py>,
    relation: &PgOutputRelationSchema,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item("relation_oid", relation.relation_oid)?;
    out.set_item("namespace", relation.namespace.as_str())?;
    out.set_item("name", relation.name.as_str())?;
    out.set_item(
        "replica_identity",
        char::from(relation.replica_identity).to_string(),
    )?;
    let cols = PyList::empty(py);
    for col in relation.columns.iter() {
        let col_dict = PyDict::new(py);
        col_dict.set_item("is_key", col.is_key)?;
        col_dict.set_item("name", col.name.as_str())?;
        col_dict.set_item("type_oid", col.type_oid)?;
        col_dict.set_item("type_modifier", col.type_modifier)?;
        cols.append(col_dict)?;
    }
    out.set_item("columns", cols)?;
    Ok(out)
}

/// Losslessly convert a [`ReplicationStreamEvent`] into `vertebra.db.adapters.postgres.types.PostgresReplicationRecord`.
pub fn replication_stream_event_to_python(
    py: Python<'_>,
    event: &ReplicationStreamEvent,
) -> PyResult<Py<PyAny>> {
    let py_types = resolve_replication_py_types(py)?;
    match event {
        ReplicationStreamEvent::Control {
            identity,
            acknowledged_lsn,
            generation,
        } => {
            let payload = PyDict::new(py);
            payload.set_item("identity", verified_identity_to_python(py, identity)?)?;
            payload.set_item("acknowledged_lsn", *acknowledged_lsn)?;
            Ok(py_types
                .replication_record
                .bind(py)
                .call1(("control", payload, py.None(), *generation))?
                .unbind())
        }
        ReplicationStreamEvent::Record { record, generation } => {
            let payload = PyDict::new(py);
            let (kind, commit_end_lsn): (&str, Option<u64>) = match record {
                ReplicationRecord::Begin {
                    dedup,
                    xid,
                    begin_final_lsn,
                    commit_time_pg_us,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("begin_final_lsn", *begin_final_lsn)?;
                    payload.set_item("commit_time_pg_us", *commit_time_pg_us)?;
                    ("begin", None)
                }
                ReplicationRecord::Insert {
                    dedup,
                    xid,
                    relation,
                    new_tuple,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("relation", relation_schema_to_python(py, relation)?)?;
                    payload.set_item(
                        "new_tuple",
                        pgoutput_tuple_to_python_dict(py, relation, new_tuple)?,
                    )?;
                    ("insert", None)
                }
                ReplicationRecord::Update {
                    dedup,
                    xid,
                    relation,
                    old_kind,
                    old_tuple,
                    new_tuple,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("relation", relation_schema_to_python(py, relation)?)?;
                    payload.set_item("old_kind", old_kind.map(old_tuple_kind_str))?;
                    let old_py = match old_tuple {
                        Some(tuple) => Some(pgoutput_tuple_to_python_dict(py, relation, tuple)?),
                        None => None,
                    };
                    payload.set_item("old_tuple", old_py)?;
                    payload.set_item(
                        "new_tuple",
                        pgoutput_tuple_to_python_dict(py, relation, new_tuple)?,
                    )?;
                    ("update", None)
                }
                ReplicationRecord::Delete {
                    dedup,
                    xid,
                    relation,
                    old_kind,
                    old_tuple,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("relation", relation_schema_to_python(py, relation)?)?;
                    payload.set_item("old_kind", old_tuple_kind_str(*old_kind))?;
                    payload.set_item(
                        "old_tuple",
                        pgoutput_tuple_to_python_dict(py, relation, old_tuple)?,
                    )?;
                    ("delete", None)
                }
                ReplicationRecord::Truncate {
                    dedup,
                    xid,
                    relations,
                    cascade,
                    restart_identity,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    let rel_list = PyList::empty(py);
                    for rel in relations {
                        rel_list.append(relation_schema_to_python(py, rel)?)?;
                    }
                    payload.set_item("relations", rel_list)?;
                    payload.set_item("cascade", *cascade)?;
                    payload.set_item("restart_identity", *restart_identity)?;
                    ("truncate", None)
                }
                ReplicationRecord::Message {
                    dedup,
                    xid,
                    transactional,
                    message_lsn,
                    prefix,
                    content,
                } => {
                    let dedup_py = match dedup {
                        Some(key) => Some(dedup_key_to_python(py, key)?),
                        None => None,
                    };
                    payload.set_item("dedup", dedup_py)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("transactional", *transactional)?;
                    payload.set_item("message_lsn", *message_lsn)?;
                    payload.set_item("prefix", prefix.as_str())?;
                    payload.set_item("content", PyBytes::new(py, content.as_ref()))?;
                    ("message", None)
                }
                ReplicationRecord::Commit {
                    dedup,
                    xid,
                    flags,
                    commit_lsn,
                    commit_end_lsn,
                    commit_time_pg_us,
                    origin_name,
                    origin_lsn,
                } => {
                    payload.set_item("dedup", dedup_key_to_python(py, dedup)?)?;
                    payload.set_item("xid", *xid)?;
                    payload.set_item("flags", *flags)?;
                    payload.set_item("commit_lsn", *commit_lsn)?;
                    payload.set_item("commit_end_lsn", *commit_end_lsn)?;
                    payload.set_item("commit_time_pg_us", *commit_time_pg_us)?;
                    payload.set_item("origin_name", origin_name.as_deref())?;
                    payload.set_item("origin_lsn", *origin_lsn)?;
                    ("commit", Some(*commit_end_lsn))
                }
            };
            Ok(py_types
                .replication_record
                .bind(py)
                .call1((kind, payload, commit_end_lsn, *generation))?
                .unbind())
        }
    }
}

pub(crate) fn extract_strict_lsn(obj: &Bound<'_, PyAny>, field_name: &str) -> PyResult<u64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "expected int {field_name}, not bool"
        )));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err(format!("expected int {field_name}")))?;
    u64::try_from(val).map_err(|_| {
        PyValueError::new_err(format!(
            "{field_name} must be in 0..={}, got {val}",
            u64::MAX
        ))
    })
}

struct ReplicationNextState {
    next_id: u64,
    claimed: Option<(u64, Py<PyAny>)>,
    in_flight: HashSet<u64>,
}

struct ReplicationNextControl {
    state: Mutex<ReplicationNextState>,
    retire_done: Notify,
}

impl ReplicationNextControl {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ReplicationNextState {
                next_id: 0,
                claimed: None,
                in_flight: HashSet::new(),
            }),
            retire_done: Notify::new(),
        })
    }

    fn try_claim(self: &Arc<Self>, py: Python<'_>) -> PyResult<(u64, ReplicationInFlightGuard)> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, ref fut)) = st.claimed {
            let done = fut
                .bind(py)
                .call_method0("done")
                .and_then(|v| v.extract::<bool>())
                .unwrap_or(false);
            if !done {
                return Err(postgres_error_to_py(
                    py,
                    PostgresError::Interface(
                        "concurrent AsyncPostgresReplication.next() calls are not permitted"
                            .to_string(),
                    ),
                ));
            }
            st.claimed = None;
        }
        st.next_id = st.next_id.wrapping_add(1);
        let id = st.next_id;
        st.in_flight.insert(id);
        Ok((
            id,
            ReplicationInFlightGuard {
                ctl: Arc::clone(self),
                id,
            },
        ))
    }

    fn attach_future(&self, id: u64, future: Py<PyAny>) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.in_flight.contains(&id) {
            st.claimed = Some((id, future));
        }
    }

    async fn wait_prior_retired(&self, my_id: u64) {
        loop {
            let notified = self.retire_done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let st = self.state.lock().unwrap_or_else(|e| e.into_inner());
                if !st.in_flight.iter().any(|&other| other != my_id) {
                    return;
                }
            }
            notified.await;
        }
    }
}

struct ReplicationInFlightGuard {
    ctl: Arc<ReplicationNextControl>,
    id: u64,
}

impl Drop for ReplicationInFlightGuard {
    fn drop(&mut self) {
        {
            let mut st = self.ctl.state.lock().unwrap_or_else(|e| e.into_inner());
            st.in_flight.remove(&self.id);
            if st
                .claimed
                .as_ref()
                .is_some_and(|(claimed_id, _)| *claimed_id == self.id)
            {
                st.claimed = None;
            }
        }
        self.ctl.retire_done.notify_waiters();
    }
}

struct PendingReplicationOutcome {
    delivery: Option<ReplicationEventDelivery>,
    _guard: ReplicationInFlightGuard,
}

struct PendingReplicationError {
    err: PostgresError,
    _guard: ReplicationInFlightGuard,
}

/// Dedicated non-pooled asynchronous PostgreSQL logical replication stream
/// (`vertebra._postgres.AsyncPostgresReplication`).
#[pyclass(
    name = "AsyncPostgresReplication",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresReplication {
    supervisor: Arc<ReplicationSupervisor>,
    next_ctl: Arc<ReplicationNextControl>,
    published_generation: Arc<AtomicU64>,
}

impl PyAsyncPostgresReplication {
    /// Wrap an open [`ReplicationSupervisor`] in the frozen Python stream handle.
    pub(crate) fn from_supervisor(supervisor: ReplicationSupervisor) -> Self {
        Self {
            supervisor: Arc::new(supervisor),
            next_ctl: ReplicationNextControl::new(),
            published_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    fn spawn_next_inner<'py>(
        &self,
        py: Python<'py>,
        stop_on_none: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = py.import("asyncio")?.call_method0("get_running_loop")?;
        let (call_id, guard) = self.next_ctl.try_claim(py)?;
        let ctl_worker = Arc::clone(&self.next_ctl);
        let supervisor = Arc::clone(&self.supervisor);
        let published_gen = Arc::clone(&self.published_generation);

        let awaitable = spawn_rust_db_task_with_attr_and_future(
            py,
            async move {
                ctl_worker.wait_prior_retired(call_id).await;
                match supervisor.next_event().await {
                    Ok(opt_delivery) => Ok(PendingReplicationOutcome {
                        delivery: opt_delivery,
                        _guard: guard,
                    }),
                    Err(err) => Err(PendingReplicationError { err, _guard: guard }),
                }
            },
            move |py, py_fut, mut outcome| {
                let Some(delivery) = outcome.delivery.take() else {
                    drop(outcome);
                    return if stop_on_none {
                        Err(PyStopAsyncIteration::new_err(()))
                    } else {
                        Ok(py.None())
                    };
                };
                if py_fut.call_method0("cancelled")?.extract::<bool>()? {
                    drop(delivery);
                    drop(outcome);
                    return Err(pyo3::exceptions::asyncio::CancelledError::new_err(
                        "AsyncPostgresReplication.next() was cancelled",
                    ));
                }
                let py_record = replication_stream_event_to_python(py, delivery.event())?;
                if py_fut.call_method0("cancelled")?.extract::<bool>()? {
                    drop(delivery);
                    drop(outcome);
                    return Err(pyo3::exceptions::asyncio::CancelledError::new_err(
                        "AsyncPostgresReplication.next() was cancelled",
                    ));
                }
                let event = match delivery.publish() {
                    Ok(ev) => ev,
                    Err(err) => {
                        drop(outcome);
                        return Err(postgres_error_to_py(py, err));
                    }
                };
                let generation = match event {
                    ReplicationStreamEvent::Control { generation, .. }
                    | ReplicationStreamEvent::Record { generation, .. } => generation,
                };
                published_gen.store(generation, Ordering::Release);
                drop(outcome);
                Ok(py_record)
            },
            |py, pending_err| postgres_error_to_py(py, pending_err.err),
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        self.next_ctl
            .attach_future(call_id, awaitable.future_handle(py));
        Bound::new(py, awaitable)
    }

    fn spawn_aclose_inner<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let supervisor = Arc::clone(&self.supervisor);
        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move { supervisor.shutdown().await },
            |py, ()| Ok(py.None()),
            postgres_error_to_py,
            || {},
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl PyAsyncPostgresReplication {
    /// Return a read-only snapshot indicating whether the supervisor is permanently closed (`False` during reconnect backoff).
    #[getter]
    pub fn closed(&self) -> bool {
        self.supervisor.is_closed()
    }

    /// Await the next `PostgresReplicationRecord` (`control`, `begin`, `insert`, `update`, `delete`, `truncate`, `message`, `commit`) or `None` once closed.
    pub fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_next_inner(py, false)
    }

    /// Return the replication stream itself as an asynchronous iterator.
    pub fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Await the next `PostgresReplicationRecord` or raise `StopAsyncIteration` once closed.
    pub fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_next_inner(py, true)
    }

    /// Acknowledge a published commit boundary `commit_end_lsn` on the active published generation.
    pub fn acknowledge(&self, py: Python<'_>, commit_end_lsn: &Bound<'_, PyAny>) -> PyResult<u64> {
        let lsn = extract_strict_lsn(commit_end_lsn, "commit_end_lsn")?;
        let generation = self.published_generation.load(Ordering::Acquire);
        self.supervisor
            .acknowledge(generation, lsn)
            .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Idempotently shut down the replication supervisor and terminate the active WAL stream cleanly.
    pub fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_aclose_inner(py)
    }

    /// Enter an `async with` block resolving to this `AsyncPostgresReplication`.
    pub fn __aenter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let py_self: Py<Self> = slf.into_pyobject(py)?.unbind();
        let awaitable = spawn_rust_db_task_with_attr(
            py,
            async { Ok::<(), PostgresError>(()) },
            move |_py, ()| Ok(py_self.into_any()),
            postgres_error_to_py,
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )?;
        Bound::new(py, awaitable)
    }

    /// Idempotently close the replication stream on `async with` exit without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        exc_val: Option<&Bound<'py, PyAny>>,
        exc_tb: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let _ = (exc_type, exc_val, exc_tb);
        self.spawn_aclose_inner(py)
    }

    fn __repr__(&self) -> String {
        let resume = self.supervisor.resume();
        format!(
            "AsyncPostgresReplication(generation={}, acknowledged_lsn={}, closed={})",
            resume.generation,
            resume.acknowledged_lsn,
            self.supervisor.is_closed(),
        )
    }
}

pub(crate) fn register_pymodule(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncPostgresReplication>()
}

struct PendingOpenReplicationGuard {
    supervisor: Option<ReplicationSupervisor>,
}

impl PendingOpenReplicationGuard {
    fn new(supervisor: ReplicationSupervisor) -> Self {
        Self {
            supervisor: Some(supervisor),
        }
    }

    fn take_into_py_replication(&mut self) -> PyAsyncPostgresReplication {
        let supervisor = self
            .supervisor
            .take()
            .expect("PendingOpenReplicationGuard consumed exactly once");
        PyAsyncPostgresReplication::from_supervisor(supervisor)
    }
}

impl Drop for PendingOpenReplicationGuard {
    fn drop(&mut self) {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.cancel();
        }
    }
}

pub(crate) fn extract_opt_lsn(obj: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    if obj.is_none() {
        return Ok(None);
    }
    extract_strict_lsn(obj, "start_lsn").map(Some)
}

pub(crate) fn extract_replication_queue_capacity(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "expected int queue_capacity, not bool",
        ));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int queue_capacity"))?;
    if !(1..=1024).contains(&val) {
        return Err(PyValueError::new_err(format!(
            "replication queue_capacity must be between 1 and 1024, got {val}"
        )));
    }
    Ok(val as usize)
}

fn validate_replication_client_and_identifiers(
    py: Python<'_>,
    session: &crate::session::SessionHandle,
    method: &str,
    slot: &str,
    publication: &str,
) -> PyResult<PostgresConfig> {
    if session.is_closed() {
        return Err(postgres_error_to_py(
            py,
            PostgresError::Interface(format!(
                "cannot call {method}() on a closed PostgreSQL client"
            )),
        ));
    }
    let config = session.config().clone();
    if matches!(
        config.pooler_mode,
        PoolerMode::Transaction | PoolerMode::Statement
    ) {
        return Err(postgres_error_to_py(
            py,
            PostgresError::UnsupportedFeature(format!(
                "PostgreSQL logical replication does not support pooler_mode `{}`",
                config.pooler_mode.as_str()
            )),
        ));
    }
    ensure_session_capable_pooler(&config).map_err(|e| postgres_error_to_py(py, e))?;
    validate_replication_identifier(slot, "slot").map_err(|e| postgres_error_to_py(py, e))?;
    validate_replication_identifier(publication, "publication")
        .map_err(|e| postgres_error_to_py(py, e))?;
    Ok(config)
}

pub(crate) fn spawn_replication_identity<'py>(
    py: Python<'py>,
    session: &Arc<crate::session::SessionHandle>,
    slot: String,
    publication: String,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
    let config = validate_replication_client_and_identifiers(
        py,
        session,
        "replication_identity",
        &slot,
        &publication,
    )?;
    let awaitable = spawn_rust_db_task_with_attr(
        py,
        async move { PostgresReplicationConnection::probe_identity(&config, &slot, &publication).await },
        |py, identity| verified_identity_to_python(py, &identity),
        postgres_error_to_py,
        POSTGRES_COMPLETION_DRAINER_ATTR,
    )?;
    Bound::new(py, awaitable)
}

pub(crate) fn spawn_open_replication<'py>(
    py: Python<'py>,
    session: &Arc<crate::session::SessionHandle>,
    slot: String,
    publication: String,
    start_lsn: Option<u64>,
    queue_capacity: usize,
    reconnect: bool,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
    let config = validate_replication_client_and_identifiers(
        py,
        session,
        "open_replication",
        &slot,
        &publication,
    )?;
    let awaitable = spawn_rust_db_task_with_attr(
        py,
        async move {
            let supervisor = ReplicationSupervisor::open(
                config,
                slot,
                publication,
                start_lsn,
                queue_capacity,
                reconnect,
            )
            .await?;
            Ok::<PendingOpenReplicationGuard, PostgresError>(PendingOpenReplicationGuard::new(
                supervisor,
            ))
        },
        |py, mut open_guard| {
            let py_repl = open_guard.take_into_py_replication();
            Ok(Py::new(py, py_repl)?.into_any())
        },
        postgres_error_to_py,
        POSTGRES_COMPLETION_DRAINER_ATTR,
    )?;
    Bound::new(py, awaitable)
}

fn resolve_replication_py_types(py: Python<'_>) -> PyResult<&CachedReplicationPyTypes> {
    REPLICATION_PY_TYPES.get_or_try_init(py, || {
        let types_mod = py.import("vertebra.db.adapters.postgres.types")?;
        Ok(CachedReplicationPyTypes {
            pg_unchanged_toast: types_mod.getattr("PG_UNCHANGED_TOAST")?.unbind(),
            replication_identity: types_mod.getattr("PostgresReplicationIdentity")?.unbind(),
            replication_record: types_mod.getattr("PostgresReplicationRecord")?.unbind(),
        })
    })
}

struct VerifiedPreflight {
    database: String,
    restart_lsn: u64,
    confirmed_flush_lsn: u64,
    publication_fingerprint: String,
    has_published_tables: bool,
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

pub(super) enum VerifiedConnectFailure {
    SlotActive(PostgresError),
    Other(PostgresError),
}

impl VerifiedConnectFailure {
    pub(super) fn into_error(self) -> PostgresError {
        match self {
            Self::SlotActive(err) | Self::Other(err) => err,
        }
    }
}

pub(super) fn ensure_session_capable_pooler(config: &PostgresConfig) -> Result<(), PostgresError> {
    if !matches!(config.pooler_mode, PoolerMode::Direct | PoolerMode::Session) {
        return Err(PostgresError::Interface(format!(
            "PostgreSQL logical replication requires explicit pooler_mode `direct` or `session`, got `{}`",
            config.pooler_mode.as_str()
        )));
    }
    Ok(())
}

pub(super) fn validate_replication_identifier(name: &str, kind: &str) -> Result<(), PostgresError> {
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

async fn run_connect_preflight(
    config: &PostgresConfig,
    slot: &str,
    publication: &str,
    start_lsn: Option<u64>,
) -> Result<
    (
        VerifiedPreflight,
        TypeCatalog,
        HashMap<u32, (String, String)>,
    ),
    PostgresError,
> {
    let mut meta_conn = establish_standalone_connection(config, false).await?;
    let outcome = async {
        let preflight =
            inspect_slot_and_publication(&mut meta_conn, config.connect_timeout, slot, publication)
                .await?;
        if let Some(requested_lsn) = start_lsn {
            validate_start_lsn_floor(slot, requested_lsn, &preflight)?;
        }
        let (catalog, identities) = if preflight.has_published_tables {
            preload_publication_types(&mut meta_conn, config.connect_timeout, publication).await?
        } else {
            (TypeCatalog::new_with_builtins(), HashMap::new())
        };
        Ok((preflight, catalog, identities))
    }
    .await;
    close_connection_bounded(&mut meta_conn, config.connect_timeout).await;
    outcome
}

async fn run_verified_connect_preflight(
    config: &PostgresConfig,
    slot: &str,
    publication: &str,
    start_lsn: u64,
    expected: &VerifiedReplicationIdentity,
) -> Result<
    (
        VerifiedPreflight,
        u64,
        TypeCatalog,
        HashMap<u32, (String, String)>,
    ),
    VerifiedConnectFailure,
> {
    let mut meta_conn = establish_standalone_connection(config, false)
        .await
        .map_err(VerifiedConnectFailure::Other)?;
    let outcome = async {
        let preflight = inspect_slot_and_publication_inner(
            &mut meta_conn,
            config.connect_timeout,
            slot,
            publication,
            Some(&expected.database),
        )
        .await?;
        if preflight.publication_fingerprint != expected.publication_fingerprint {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "PostgreSQL publication {publication:?} fingerprint changed across resume: got {}, expected {} (identity_mismatch)",
                    preflight.publication_fingerprint, expected.publication_fingerprint
                ),
            )));
        }
        let resume_lsn = start_lsn.max(preflight.confirmed_flush_lsn);
        if resume_lsn < preflight.restart_lsn {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "resume LSN {} is behind slot {slot:?} restart_lsn {} (wal_unavailable)",
                    format_pg_lsn(resume_lsn),
                    format_pg_lsn(preflight.restart_lsn),
                ),
            )));
        }
        let (catalog, identities) = if preflight.has_published_tables {
            preload_publication_types(&mut meta_conn, config.connect_timeout, publication)
                .await
                .map_err(VerifiedConnectFailure::Other)?
        } else {
            (TypeCatalog::new_with_builtins(), HashMap::new())
        };
        Ok((preflight, resume_lsn, catalog, identities))
    }
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
    inspect_slot_and_publication_inner(meta_conn, timeout, slot, publication, None)
        .await
        .map_err(VerifiedConnectFailure::into_error)
}

async fn inspect_slot_and_publication_inner(
    meta_conn: &mut PostgresConnection,
    timeout: Duration,
    slot: &str,
    publication: &str,
    expected_database: Option<&str>,
) -> Result<VerifiedPreflight, VerifiedConnectFailure> {
    let slot_params = [Parameter::new(PgValue::Text(slot.to_owned()))];
    let text_oids = [TEXT_OID];
    let slot_res = meta_conn
        .query_with_deadline(
            SLOT_PREFLIGHT_SQL,
            &slot_params,
            Some(&text_oids),
            CommandBudget::from_deadline(Some(Instant::now() + timeout)),
        )
        .await
        .map_err(VerifiedConnectFailure::Other)?;

    if slot_res.rows.is_empty() {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!("PostgreSQL logical replication slot {slot:?} does not exist"),
        )));
    }
    if slot_res.rows.len() > 1 {
        return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
            format!(
                "expected 1 pg_replication_slots row for {slot:?}, got {}",
                slot_res.rows.len()
            ),
        )));
    }
    let row = &slot_res.rows[0];
    if row.len() != 8 {
        return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
            format!(
                "expected 8 columns from pg_replication_slots preflight, got {}",
                row.len()
            ),
        )));
    }

    let current_db =
        expect_non_null_text(&row[0], "current_database").map_err(VerifiedConnectFailure::Other)?;
    let slot_type = expect_non_null_text(&row[2], "pg_replication_slots.slot_type")
        .map_err(VerifiedConnectFailure::Other)?;
    if slot_type != "logical" {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!(
                "PostgreSQL replication slot {slot:?} has slot_type {slot_type:?}; logical `pgoutput` slot required"
            ),
        )));
    }

    let slot_db = match &row[1] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!("PostgreSQL logical replication slot {slot:?} has NULL database"),
            )));
        }
        other => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
                format!("unexpected pg_replication_slots.database value: {other:?}"),
            )));
        }
    };
    if slot_db != current_db {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!(
                "PostgreSQL logical replication slot {slot:?} belongs to database {slot_db:?}, connected to {current_db:?}"
            ),
        )));
    }
    if let Some(exp_db) = expected_database
        && current_db != exp_db
    {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!(
                "PostgreSQL logical replication database mismatch on resume: connected to {current_db:?}, expected {exp_db:?} (identity_mismatch)"
            ),
        )));
    }

    let plugin = match &row[3] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "PostgreSQL logical replication slot {slot:?} has NULL output plugin; `pgoutput` required"
                ),
            )));
        }
        other => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
                format!("unexpected pg_replication_slots.plugin value: {other:?}"),
            )));
        }
    };
    if plugin != "pgoutput" {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!(
                "PostgreSQL logical replication slot {slot:?} uses output plugin {plugin:?}; `pgoutput` is required"
            ),
        )));
    }

    let active = expect_bool(&row[4], "pg_replication_slots.active")
        .map_err(VerifiedConnectFailure::Other)?;
    if active {
        return Err(VerifiedConnectFailure::SlotActive(
            PostgresError::Operational(format!(
                "PostgreSQL logical replication slot {slot:?} is already active"
            )),
        ));
    }

    if let PgValue::Text(wal_status) = &row[7]
        && wal_status.eq_ignore_ascii_case("lost")
    {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!(
                "PostgreSQL logical replication slot {slot:?} WAL is invalidated (wal_unavailable: wal_status={wal_status:?})"
            ),
        )));
    }

    let restart_lsn_raw = match &row[5] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "PostgreSQL logical replication slot {slot:?} has NULL restart_lsn (wal_unavailable)"
                ),
            )));
        }
        other => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
                format!("unexpected pg_replication_slots.restart_lsn value: {other:?}"),
            )));
        }
    };
    let restart_lsn = parse_pg_lsn(restart_lsn_raw).map_err(|_| {
        VerifiedConnectFailure::Other(PostgresError::Protocol(format!(
            "malformed pg_replication_slots.restart_lsn {restart_lsn_raw:?} for slot {slot:?}"
        )))
    })?;

    let confirmed_raw = match &row[6] {
        PgValue::Text(s) => s.as_str(),
        PgValue::Null => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
                format!(
                    "PostgreSQL logical replication slot {slot:?} has NULL confirmed_flush_lsn"
                ),
            )));
        }
        other => {
            return Err(VerifiedConnectFailure::Other(PostgresError::Protocol(
                format!("unexpected pg_replication_slots.confirmed_flush_lsn value: {other:?}"),
            )));
        }
    };
    let confirmed_flush_lsn = parse_pg_lsn(confirmed_raw).map_err(|_| {
        VerifiedConnectFailure::Other(PostgresError::Protocol(format!(
            "malformed pg_replication_slots.confirmed_flush_lsn {confirmed_raw:?} for slot {slot:?}"
        )))
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
        .await
        .map_err(VerifiedConnectFailure::Other)?;

    if pub_res.rows.is_empty() {
        return Err(VerifiedConnectFailure::Other(PostgresError::Operational(
            format!("PostgreSQL publication {publication:?} does not exist"),
        )));
    }
    let publication_fingerprint =
        hash_publication_rows(publication, &pub_res.rows, include_pg15_cols)
            .map_err(VerifiedConnectFailure::Other)?;
    let has_published_tables = pub_res.rows.iter().any(|r| {
        matches!(
            (r.get(7), r.get(8)),
            (Some(PgValue::Text(s)), Some(PgValue::Text(t))) if !s.is_empty() && !t.is_empty()
        )
    });

    Ok(VerifiedPreflight {
        database: current_db.to_owned(),
        restart_lsn,
        confirmed_flush_lsn,
        publication_fingerprint,
        has_published_tables,
    })
}

async fn preload_publication_types(
    meta_conn: &mut PostgresConnection,
    timeout: Duration,
    publication: &str,
) -> Result<(TypeCatalog, HashMap<u32, (String, String)>), PostgresError> {
    let mut catalog = TypeCatalog::new_with_builtins();
    let mut identities: HashMap<u32, (String, String)> = HashMap::new();
    let mut queried_oids: HashSet<u32> = HashSet::new();
    let mut total_rows = 0usize;

    let pub_params = [Parameter::new(PgValue::Text(publication.to_owned()))];
    let text_oids = [TEXT_OID];
    let root_res = meta_conn
        .query_with_deadline(
            PUBLICATION_TYPES_PRELOAD_SQL,
            &pub_params,
            Some(&text_oids),
            CommandBudget::from_deadline(Some(Instant::now() + timeout)),
        )
        .await?;

    total_rows = total_rows.saturating_add(root_res.rows.len());
    if total_rows > MAX_PRELOAD_DISCOVERY_ROWS {
        return Err(PostgresError::Operational(format!(
            "publication custom-type preload exceeded maximum catalog row cap ({MAX_PRELOAD_DISCOVERY_ROWS})"
        )));
    }

    let mut next_candidates = Vec::new();
    ingest_preloaded_type_rows(
        &mut catalog,
        &mut identities,
        &mut queried_oids,
        &root_res,
        &mut next_candidates,
    )?;

    let mut pending_oids = collect_unseen_child_oids(&catalog, &mut queried_oids, next_candidates);
    let mut round = 0usize;

    while !pending_oids.is_empty() {
        if round >= MAX_DISCOVERY_ROUNDS {
            return Err(PostgresError::Data(format!(
                "publication custom-type preload exceeded maximum transitive rounds ({MAX_DISCOVERY_ROUNDS})"
            )));
        }
        round += 1;

        let current_batch = std::mem::take(&mut pending_oids);
        let mut round_candidates = Vec::new();

        for chunk in current_batch.chunks(MAX_PRELOAD_OID_BATCH) {
            let sql = build_oid_preload_query(chunk.len());
            let params: Vec<Parameter> = chunk
                .iter()
                .map(|&oid| Parameter::new(PgValue::Int8(i64::from(oid))))
                .collect();
            let param_oids = vec![OID_OID; chunk.len()];
            let res = meta_conn
                .query_with_deadline(
                    &sql,
                    &params,
                    Some(&param_oids),
                    CommandBudget::from_deadline(Some(Instant::now() + timeout)),
                )
                .await?;
            total_rows = total_rows.saturating_add(res.rows.len());
            if total_rows > MAX_PRELOAD_DISCOVERY_ROWS {
                return Err(PostgresError::Operational(format!(
                    "publication custom-type preload exceeded maximum catalog row cap ({MAX_PRELOAD_DISCOVERY_ROWS})"
                )));
            }
            ingest_preloaded_type_rows(
                &mut catalog,
                &mut identities,
                &mut queried_oids,
                &res,
                &mut round_candidates,
            )?;
        }

        pending_oids = collect_unseen_child_oids(&catalog, &mut queried_oids, round_candidates);
    }

    let custom_oids: Vec<u32> = identities.keys().copied().collect();
    for oid in custom_oids {
        let _ = catalog.resolve_domain_base(oid)?;
    }

    Ok((catalog, identities))
}

fn collect_unseen_child_oids(
    catalog: &TypeCatalog,
    queried_oids: &mut HashSet<u32>,
    candidates: Vec<u32>,
) -> Vec<u32> {
    let mut pending = Vec::new();
    for oid in candidates {
        if oid != 0 && catalog.type_info(oid).is_none() && queried_oids.insert(oid) {
            pending.push(oid);
        }
    }
    pending
}

fn build_oid_preload_query(param_count: usize) -> String {
    let mut placeholders = String::with_capacity(param_count * 5);
    for idx in 1..=param_count {
        if idx > 1 {
            placeholders.push_str(", ");
        }
        placeholders.push('$');
        placeholders.push_str(&idx.to_string());
    }
    format!(
        "SELECT DISTINCT t.oid, tn.nspname, t.typname, t.typtype::pg_catalog.text, t.typcategory::pg_catalog.text, t.typdelim::pg_catalog.text, t.typelem, t.typbasetype, rng.rngsubtype, mrng.rngtypid, mrng.rngsubtype, ext.extname, ca.attname, ca.atttypid, ca.atttypmod, ca.attnum FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace LEFT JOIN pg_catalog.pg_range rng ON rng.rngtypid = t.oid LEFT JOIN pg_catalog.pg_range mrng ON mrng.rngmultitypid = t.oid LEFT JOIN pg_catalog.pg_depend dep ON dep.classid = 1247 AND dep.objid = t.oid AND dep.deptype = 'e' LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = dep.refobjid LEFT JOIN pg_catalog.pg_attribute ca ON t.typtype = 'c' AND ca.attrelid = t.typrelid AND ca.attnum > 0 AND NOT ca.attisdropped WHERE t.oid IN ({placeholders}) ORDER BY t.oid ASC, ca.attnum ASC NULLS FIRST"
    )
}

fn ingest_preloaded_type_rows(
    catalog: &mut TypeCatalog,
    identities: &mut HashMap<u32, (String, String)>,
    queried_oids: &mut HashSet<u32>,
    result: &QueryResult,
    next_candidates: &mut Vec<u32>,
) -> Result<(), PostgresError> {
    let mut composite_headers: HashMap<u32, (String, String, u8)> = HashMap::new();
    let mut composite_fields: HashMap<u32, Vec<(i32, CompositeField)>> = HashMap::new();

    for (row_idx, row) in result.rows.iter().enumerate() {
        if row.len() < 16 {
            return Err(PostgresError::Protocol(format!(
                "expected 16 columns from publication custom-type preload row {row_idx}, got {}",
                row.len()
            )));
        }

        let oid = preload_cell_u32(row.first())?;
        if oid == 0 {
            return Err(PostgresError::Protocol(format!(
                "publication custom-type preload row {row_idx} returned invalid type oid 0"
            )));
        }
        queried_oids.insert(oid);

        let is_seeded_builtin = catalog.type_info(oid).is_some() && !identities.contains_key(&oid);
        if is_seeded_builtin {
            continue;
        }

        let schema = preload_cell_opt_str(row.get(1))
            .ok_or_else(|| {
                PostgresError::Protocol(format!(
                    "publication custom-type preload OID {oid} missing nspname"
                ))
            })?
            .to_owned();
        let name = preload_cell_opt_str(row.get(2))
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                PostgresError::Protocol(format!(
                    "publication custom-type preload OID {oid} missing typname"
                ))
            })?
            .to_owned();
        let typtype = preload_cell_opt_str(row.get(3)).unwrap_or("b");
        let typcategory = preload_cell_opt_str(row.get(4)).unwrap_or("");
        let delimiter = preload_cell_opt_str(row.get(5))
            .and_then(|s| s.as_bytes().first().copied())
            .unwrap_or(b',');
        let typelem = preload_cell_u32(row.get(6))?;
        let typbasetype = preload_cell_u32(row.get(7))?;
        let rngsubtype = preload_cell_u32(row.get(8))?;
        let mrngtypid = preload_cell_u32(row.get(9))?;
        let mrngsubtype = preload_cell_u32(row.get(10))?;
        let extname = preload_cell_opt_str(row.get(11)).filter(|s| !s.is_empty());

        identities.insert(oid, (schema.clone(), name.clone()));

        match typtype {
            "c" => {
                composite_headers
                    .entry(oid)
                    .or_insert_with(|| (schema, name, delimiter));
                let entry = composite_fields.entry(oid).or_default();
                let attnum = preload_cell_i32(row.get(15), 0)?;
                if attnum > 0
                    && let Some(att_name) =
                        preload_cell_opt_str(row.get(12)).filter(|s| !s.is_empty())
                {
                    let atttypid = preload_cell_u32(row.get(13))?;
                    if atttypid == 0 {
                        return Err(PostgresError::Data(format!(
                            "composite OID {oid} attribute {att_name:?} has invalid atttypid 0"
                        )));
                    }
                    let atttypmod = preload_cell_i32(row.get(14), -1)?;
                    next_candidates.push(atttypid);
                    entry.push((
                        attnum,
                        CompositeField {
                            name: att_name.to_owned(),
                            type_oid: atttypid,
                            type_modifier: atttypmod,
                        },
                    ));
                }
            }
            "d" => {
                if typbasetype == 0 {
                    return Err(PostgresError::Data(format!(
                        "domain OID {oid} ({name:?}) has invalid typbasetype 0"
                    )));
                }
                next_candidates.push(typbasetype);
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Domain {
                        base_oid: typbasetype,
                    },
                    delimiter,
                });
            }
            "e" => {
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Enum,
                    delimiter,
                });
            }
            "r" => {
                if rngsubtype != 0 {
                    next_candidates.push(rngsubtype);
                }
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Range,
                    delimiter,
                });
            }
            "m" => {
                if mrngtypid != 0 {
                    next_candidates.push(mrngtypid);
                }
                if mrngsubtype != 0 {
                    next_candidates.push(mrngsubtype);
                }
                catalog.insert_custom_type(TypeInfo {
                    oid,
                    schema,
                    name,
                    kind: TypeKind::Range,
                    delimiter,
                });
            }
            _ => {
                let is_array = typelem != 0
                    && (typcategory == "A" || (name.starts_with('_') && extname.is_none()));
                if is_array {
                    next_candidates.push(typelem);
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema,
                        name,
                        kind: TypeKind::Array {
                            element_oid: typelem,
                            delimiter,
                        },
                        delimiter,
                    });
                } else if extname.is_some() || matches!(name.as_str(), "vector" | "halfvec") {
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema: schema.clone(),
                        name: name.clone(),
                        kind: TypeKind::Extension { schema, name },
                        delimiter,
                    });
                } else {
                    catalog.insert_custom_type(TypeInfo {
                        oid,
                        schema,
                        name,
                        kind: TypeKind::Scalar,
                        delimiter,
                    });
                }
            }
        }
    }

    for (oid, (schema, name, delimiter)) in composite_headers {
        let mut numbered = composite_fields.remove(&oid).unwrap_or_default();
        numbered.sort_by_key(|(attnum, _)| *attnum);
        numbered.dedup_by_key(|(attnum, _)| *attnum);
        let fields = numbered.into_iter().map(|(_, field)| field).collect();
        catalog.insert_custom_type(TypeInfo {
            oid,
            schema,
            name,
            kind: TypeKind::Composite { fields },
            delimiter,
        });
    }

    Ok(())
}

fn preload_cell_u32(val: Option<&PgValue>) -> Result<u32, PostgresError> {
    match val {
        None | Some(PgValue::Null) => Ok(0),
        Some(PgValue::Int2(v)) => u32::try_from(*v)
            .map_err(|_| PostgresError::Protocol(format!("negative catalog OID {v}"))),
        Some(PgValue::Int4(v)) => u32::try_from(*v)
            .map_err(|_| PostgresError::Protocol(format!("negative catalog OID {v}"))),
        Some(PgValue::Int8(v)) => u32::try_from(*v)
            .map_err(|_| PostgresError::Protocol(format!("out-of-range catalog OID {v}"))),
        Some(PgValue::Text(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(0)
            } else {
                trimmed.parse::<u32>().map_err(|_| {
                    PostgresError::Protocol(format!("invalid catalog u32 literal {trimmed:?}"))
                })
            }
        }
        other => Err(PostgresError::Protocol(format!(
            "unexpected catalog u32 cell value {other:?}"
        ))),
    }
}

fn preload_cell_i32(val: Option<&PgValue>, default: i32) -> Result<i32, PostgresError> {
    match val {
        None | Some(PgValue::Null) => Ok(default),
        Some(PgValue::Int2(v)) => Ok(i32::from(*v)),
        Some(PgValue::Int4(v)) => Ok(*v),
        Some(PgValue::Int8(v)) => i32::try_from(*v)
            .map_err(|_| PostgresError::Protocol(format!("out-of-range catalog i32 {v}"))),
        Some(PgValue::Text(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(default)
            } else {
                trimmed.parse::<i32>().map_err(|_| {
                    PostgresError::Protocol(format!("invalid catalog i32 literal {trimmed:?}"))
                })
            }
        }
        other => Err(PostgresError::Protocol(format!(
            "unexpected catalog i32 cell value {other:?}"
        ))),
    }
}

fn preload_cell_opt_str(val: Option<&PgValue>) -> Option<&str> {
    match val {
        Some(PgValue::Text(s)) => Some(s.as_str()),
        _ => None,
    }
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

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use pyo3::types::{PyCFunction, PyDict, PyTuple};

    use super::*;

    fn sample_relation() -> Arc<PgOutputRelationSchema> {
        Arc::new(PgOutputRelationSchema {
            relation_oid: 16384,
            namespace: "public".to_string(),
            name: "events".to_string(),
            replica_identity: b'f',
            columns: Arc::from(vec![
                PgOutputColumn {
                    is_key: true,
                    name: "id".to_string(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                PgOutputColumn {
                    is_key: false,
                    name: "payload".to_string(),
                    type_oid: 17,
                    type_modifier: -1,
                },
                PgOutputColumn {
                    is_key: false,
                    name: "note".to_string(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ]),
        })
    }

    fn sample_dedup(ordinal: u64) -> ReplicationDedupKey {
        ReplicationDedupKey {
            system_identifier: "7399123456789".to_string(),
            slot: "vtb_slot".to_string(),
            begin_final_lsn: 0x1_0000_0040,
            transaction_ordinal: ordinal,
        }
    }

    #[test]
    fn lossless_stream_event_conversion_preserves_toast_bytes_and_schema() {
        Python::initialize();
        Python::attach(|py| {
            let rel = sample_relation();
            let toast_sentinel = resolve_replication_py_types(py)
                .unwrap()
                .pg_unchanged_toast
                .clone_ref(py);

            let events = [
                ReplicationStreamEvent::Control {
                    identity: VerifiedReplicationIdentity {
                        system_identifier: "7399123456789".to_string(),
                        timeline: 1,
                        database: "appdb".to_string(),
                        slot: "vtb_slot".to_string(),
                        publication_fingerprint: "abcd1234".to_string(),
                        confirmed_flush_lsn: 0x100,
                        restart_lsn: 0x80,
                    },
                    acknowledged_lsn: 0x100,
                    generation: 1,
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Begin {
                        dedup: sample_dedup(0),
                        xid: 42,
                        begin_final_lsn: 0x1_0000_0040,
                        commit_time_pg_us: 123_456,
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Insert {
                        dedup: sample_dedup(1),
                        xid: 42,
                        relation: Arc::clone(&rel),
                        new_tuple: vec![
                            PgOutputTupleValue::Value(PgValue::Int4(7)),
                            PgOutputTupleValue::Value(PgValue::Bytes(b"\x00\xffraw".to_vec())),
                            PgOutputTupleValue::Null,
                        ],
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Update {
                        dedup: sample_dedup(2),
                        xid: 42,
                        relation: Arc::clone(&rel),
                        old_kind: Some(OldTupleKind::Key),
                        old_tuple: Some(vec![
                            PgOutputTupleValue::Value(PgValue::Int4(7)),
                            PgOutputTupleValue::UnchangedToast,
                            PgOutputTupleValue::Null,
                        ]),
                        new_tuple: vec![
                            PgOutputTupleValue::Value(PgValue::Int4(7)),
                            PgOutputTupleValue::UnchangedToast,
                            PgOutputTupleValue::Value(PgValue::Text("updated".to_string())),
                        ],
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Delete {
                        dedup: sample_dedup(3),
                        xid: 42,
                        relation: Arc::clone(&rel),
                        old_kind: OldTupleKind::Full,
                        old_tuple: vec![
                            PgOutputTupleValue::Value(PgValue::Int4(7)),
                            PgOutputTupleValue::Null,
                            PgOutputTupleValue::Value(PgValue::Text("bye".to_string())),
                        ],
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Truncate {
                        dedup: sample_dedup(4),
                        xid: 42,
                        relations: vec![Arc::clone(&rel)],
                        cascade: true,
                        restart_identity: false,
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Message {
                        dedup: Some(sample_dedup(5)),
                        xid: Some(42),
                        transactional: true,
                        message_lsn: 0x1_0000_0050,
                        prefix: "cdc".to_string(),
                        content: Bytes::from_static(b"\x01\x02\xfe\xff"),
                    },
                },
                ReplicationStreamEvent::Record {
                    generation: 1,
                    record: ReplicationRecord::Commit {
                        dedup: sample_dedup(6),
                        xid: 42,
                        flags: 0,
                        commit_lsn: 0x1_0000_0060,
                        commit_end_lsn: 0x1_0000_0080,
                        commit_time_pg_us: 123_999,
                        origin_name: Some("origin_a".to_string()),
                        origin_lsn: Some(0x900),
                    },
                },
            ];

            let py_records: Vec<Py<PyAny>> = events
                .iter()
                .map(|ev| replication_stream_event_to_python(py, ev).unwrap())
                .collect();

            let locals = PyDict::new(py);
            locals
                .set_item("records", PyList::new(py, py_records).unwrap())
                .unwrap();
            locals.set_item("toast_sentinel", toast_sentinel).unwrap();

            let script = CString::new(concat!(
                "kinds = [r.kind for r in records]\n",
                "assert kinds == ['control', 'begin', 'insert', 'update', 'delete', 'truncate', 'message', 'commit']\n",
                "assert records[0].payload['identity'].slot == 'vtb_slot'\n",
                "assert records[0].commit_end_lsn is None and records[0].generation == 1\n",
                "assert records[2].payload['new_tuple']['payload'] == b'\\x00\\xffraw'\n",
                "assert records[2].payload['relation']['columns'][1]['type_oid'] == 17\n",
                "assert records[3].payload['old_kind'] == 'key'\n",
                "assert records[3].payload['old_tuple']['payload'] is toast_sentinel\n",
                "assert records[3].payload['new_tuple']['payload'] is toast_sentinel\n",
                "assert records[4].payload['old_kind'] == 'full'\n",
                "assert records[5].payload['cascade'] is True\n",
                "assert records[6].payload['content'] == b'\\x01\\x02\\xfe\\xff'\n",
                "assert records[7].commit_end_lsn == 0x1_0000_0080\n",
                "assert records[7].payload['origin_name'] == 'origin_a'\n",
            ))
            .unwrap();
            py.run(&script, None, Some(&locals)).unwrap();
        });
    }

    #[test]
    fn replication_next_control_retires_cancelled_future_before_next_claim() {
        Python::initialize();
        let released = Arc::new(AtomicBool::new(false));
        let released_bg = Arc::clone(&released);

        Python::attach(|py| {
            let ctl = ReplicationNextControl::new();
            let ctl_spawn = Arc::clone(&ctl);
            let spawn_cancelled =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let (id, guard) = ctl_spawn.try_claim(args.py())?;
                    let flag = Arc::clone(&released_bg);
                    let aw = spawn_rust_db_task_with_attr_and_future(
                        args.py(),
                        async move {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            Ok::<(u64, ReplicationInFlightGuard), PostgresError>((id, guard))
                        },
                        move |py, _fut, (done_id, guard)| {
                            flag.store(true, Ordering::SeqCst);
                            drop(guard);
                            Ok(done_id.into_pyobject(py)?.into_any().unbind())
                        },
                        postgres_error_to_py,
                        POSTGRES_COMPLETION_DRAINER_ATTR,
                    )?;
                    ctl_spawn.attach_future(id, aw.future_handle(args.py()));
                    Bound::new(args.py(), aw).map(Bound::unbind)
                })
                .unwrap();

            let ctl_check = Arc::clone(&ctl);
            let check_overlap =
                PyCFunction::new_closure(py, None, None, move |args: &Bound<'_, PyTuple>, _| {
                    let _ = ctl_check.try_claim(args.py())?;
                    PyResult::Ok(())
                })
                .unwrap();

            let globals = PyDict::new(py);
            globals.set_item("spawn_next", spawn_cancelled).unwrap();
            globals.set_item("check_overlap", check_overlap).unwrap();
            let script = CString::new(concat!(
                "import asyncio\n",
                "async def main():\n",
                "    aw = spawn_next()\n",
                "    overlap_failed = False\n",
                "    try:\n",
                "        check_overlap()\n",
                "    except Exception:\n",
                "        overlap_failed = True\n",
                "    assert overlap_failed\n",
                "    del aw\n",
                "    check_overlap()\n",
                "asyncio.run(main())\n",
            ))
            .unwrap();
            py.run(&script, Some(&globals), None).unwrap();

            py.detach(|| {
                let deadline = Instant::now() + Duration::from_secs(1);
                while !released.load(Ordering::SeqCst)
                    && ctl.state.lock().unwrap().in_flight.contains(&1)
                    && Instant::now() < deadline
                {
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            assert!(ctl.state.lock().unwrap().in_flight.is_empty());
        });
    }
}
