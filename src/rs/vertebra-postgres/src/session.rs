//! PostgreSQL session state machine (`SessionHandle`), cancellation-safe `PendingSession` guard,
//! and authenticated `PostgresConnection` publication.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{Message, NotificationResponseBody, ParameterStatusBody};
use postgres_protocol::message::frontend;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
#[cfg(unix)]
use vertebra_db::connection::ConnectOptions;
use vertebra_db::connection::TransportStream;

use crate::auth::Authentication;
use crate::cloud;
use crate::cloud::material::{Clock, CloudConnector, SystemClock};
use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::credentials::{ScopedLoginSnapshot, ScopedTokenProvider};
use crate::errors::{PostgresError, ServerError};
use crate::portal::Portal;
use crate::protocol::{TERMINATE_FRAME, encode_startup_frame, parse_error_fields, read_message};
use crate::tls;
use crate::transaction::{
    TransactionDisposition, TransactionOwner, TransactionScope, TransactionScopeKind,
    TransactionState,
};

/// Pooler-aware prepared statement LRU cache and capability attestation (`crate::session::statements`).
#[path = "statements.rs"]
pub mod statements;

use self::statements::{PoolerCapabilities, PoolerMode, StatementCache, StatementKey};

/// Default maximum materialized result / portal chunk byte budget (`64 MiB`).
pub const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// Backend transaction status indicator reported by `ReadyForQuery` (`'Z'`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TransactionStatus {
    /// `'I'` — idle (not in a transaction block).
    #[default]
    Idle,
    /// `'T'` — inside an open transaction block.
    InTransaction,
    /// `'E'` — inside a failed transaction block awaiting `ROLLBACK`.
    Failed,
}

impl TransactionStatus {
    /// Alias matching `Failed` for callers referencing `FailedTransaction`.
    #[allow(non_upper_case_globals)]
    pub const FailedTransaction: Self = Self::Failed;

    /// Parse the 1-byte `ReadyForQuery` transaction indicator (`b'I'`, `b'T'`, `b'E'`).
    pub fn from_wire_tag(tag: u8) -> Result<Self, PostgresError> {
        match tag {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            other => Err(PostgresError::Protocol(format!(
                "invalid ReadyForQuery transaction status indicator 0x{other:02x}"
            ))),
        }
    }
}

/// Backend process ID and cancellation secret key delivered by `BackendKeyData` (`'K'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendKey {
    pub process_id: i32,
    pub secret_key: i32,
}

/// Asynchronous notification delivered via `NotificationResponse` (`'A'`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationEvent {
    pub process_id: i32,
    pub channel: String,
    pub payload: String,
}

/// Active protocol exchange state on a [`PostgresConnection`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExchangeState {
    /// Connection is at a clean `ReadyForQuery` command boundary.
    Idle,
    /// Extended query (`Parse`..`ReadyForQuery`) is currently active.
    InExtendedExchange,
    /// Suspended portal is currently open on the connection.
    PortalActive,
    /// `COPY` (`CopyIn`, `CopyOut`, or `CopyBoth`) is currently active.
    CopyActive,
    /// Single-`Sync` pipeline batch is currently active.
    PipelineActive,
}

/// Async byte stream trait enabling instrumented duplex streams in deterministic wire tests.
pub trait AsyncConnectionIo:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + std::fmt::Debug
{
}

impl<T> AsyncConnectionIo for T where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + std::fmt::Debug
{
}

/// Underlying session byte stream (`TransportStream` on all production dials, or custom `AsyncConnectionIo` in wire harnesses).
#[derive(Debug)]
pub(crate) enum ConnectionStream {
    Transport(TransportStream),
    Custom(Box<dyn AsyncConnectionIo>),
}

impl tokio::io::AsyncRead for ConnectionStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Transport(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            Self::Custom(s) => std::pin::Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl tokio::io::AsyncWrite for ConnectionStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Transport(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            Self::Custom(s) => std::pin::Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Transport(s) => std::pin::Pin::new(s).poll_flush(cx),
            Self::Custom(s) => std::pin::Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Transport(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            Self::Custom(s) => std::pin::Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Established, authenticated PostgreSQL wire connection at `ReadyForQuery(Idle)`.
#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) stream: ConnectionStream,
    pub(crate) read_buf: BytesMut,
    pub(crate) write_buf: BytesMut,
    pub(crate) parameters: BTreeMap<String, String>,
    pub(crate) startup_parameters: BTreeMap<String, String>,
    pub(crate) configured_session_gucs: BTreeMap<String, String>,
    pub(crate) baseline_application_name: String,
    pub(crate) backend_key: Option<BackendKey>,
    pub(crate) transaction_status: TransactionStatus,
    pub(crate) transaction_state: TransactionState,
    pub(crate) generation: u64,
    pub(crate) tls_negotiated: bool,
    pub(crate) tls_server_end_point: Option<Vec<u8>>,
    pub(crate) unnamed_statement_valid: bool,
    pub(crate) type_catalog: crate::types::TypeCatalog,
    pub(crate) exchange_state: ExchangeState,
    pub(crate) statement_descriptions:
        std::collections::HashMap<String, crate::extended::PreparedDescription>,
    pub(crate) pooler_capabilities: PoolerCapabilities,
    pub(crate) statement_cache: StatementCache,
    pub(crate) configured_pooler_mode: PoolerMode,
    pub(crate) configured_statement_cache_size: usize,
    pub(crate) configured_protocol_level_pooler_cache: bool,
    pub(crate) next_savepoint_token: u64,
    pub(crate) next_portal_id: u64,
    pub(crate) open_portals: Vec<String>,
    pub(crate) notifications: Vec<NotificationEvent>,
    pub(crate) closed: bool,
}

impl PostgresConnection {
    /// Monotonic connection generation assigned by the parent [`SessionHandle`].
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Current backend transaction status reported by the most recent `ReadyForQuery`.
    pub const fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Borrow the synchronized [`TransactionState`] (including active savepoint tokens).
    pub const fn transaction_state(&self) -> &TransactionState {
        &self.transaction_state
    }

    /// Optional `(process_id, secret_key)` pair received during startup.
    pub const fn backend_key(&self) -> Option<BackendKey> {
        self.backend_key
    }

    /// Return `true` when the underlying transport negotiated TLS.
    pub const fn is_tls(&self) -> bool {
        self.tls_negotiated
    }

    /// Construct an authenticated [`PostgresConnection`] over a custom async duplex stream (used by protocol wire harnesses).
    pub fn from_stream<S: AsyncConnectionIo + 'static>(
        stream: S,
        config: &PostgresConfig,
        parameters: BTreeMap<String, String>,
        backend_key: Option<BackendKey>,
    ) -> Self {
        let pooler_capabilities = PoolerCapabilities::resolve(
            config.pooler_mode,
            config.statement_cache_size,
            config.protocol_level_pooler_cache,
            &parameters,
        );
        let statement_cache = StatementCache::new(pooler_capabilities.effective_capacity);
        let startup_parameters = parameters.clone();
        let configured_session_gucs: BTreeMap<String, String> = config
            .session_gucs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Self {
            stream: ConnectionStream::Custom(Box::new(stream)),
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(1024),
            parameters,
            startup_parameters,
            configured_session_gucs,
            baseline_application_name: config.application_name.clone(),
            backend_key,
            transaction_status: TransactionStatus::Idle,
            transaction_state: TransactionState::new(),
            generation: 1,
            tls_negotiated: false,
            tls_server_end_point: None,
            unnamed_statement_valid: false,
            type_catalog: crate::types::TypeCatalog::new_with_builtins(),
            exchange_state: ExchangeState::Idle,
            statement_descriptions: std::collections::HashMap::new(),
            pooler_capabilities,
            statement_cache,
            configured_pooler_mode: config.pooler_mode,
            configured_statement_cache_size: config.statement_cache_size,
            configured_protocol_level_pooler_cache: config.protocol_level_pooler_cache,
            next_savepoint_token: 0,
            next_portal_id: 0,
            open_portals: Vec::new(),
            notifications: Vec::new(),
            closed: false,
        }
    }

    /// Borrow the underlying [`TransportStream`] when backed by TCP, TLS, or Unix transport.
    pub fn stream(&self) -> &TransportStream {
        match &self.stream {
            ConnectionStream::Transport(s) => s,
            ConnectionStream::Custom(_) => {
                panic!("PostgresConnection::stream() called on custom test duplex stream")
            }
        }
    }

    /// Verified peer leaf `tls-server-end-point` digest when available on a TLS connection.
    pub fn tls_server_end_point(&self) -> Option<&[u8]> {
        self.tls_server_end_point.as_deref()
    }

    /// Lookup a session `ParameterStatus` value reported by the backend.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(String::as_str)
    }

    /// Borrow all retained `ParameterStatus` key-value pairs.
    pub const fn parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }

    /// Return `true` once [`PostgresConnection::close`] has completed or the socket is closed.
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Return `true` when the connection is closed or left in an unrecoverable mid-exchange state.
    pub const fn is_broken(&self) -> bool {
        self.closed || !matches!(self.exchange_state, ExchangeState::Idle)
    }

    /// Mark the connection closed and reset `exchange_state` to `Idle`.
    pub fn mark_closed(&mut self) {
        self.closed = true;
        self.exchange_state = ExchangeState::Idle;
    }

    /// Synchronize both `transaction_status` and `transaction_state` from a `ReadyForQuery` status byte.
    pub fn set_transaction_status_from_wire(&mut self, status: TransactionStatus) {
        self.transaction_status = status;
        self.transaction_state.sync_from_wire(status);
    }

    /// Return the resolved [`PoolerCapabilities`] for this session.
    pub const fn pooler_capabilities(&self) -> &PoolerCapabilities {
        &self.pooler_capabilities
    }

    /// Borrow the per-session [`StatementCache`].
    pub const fn statement_cache(&self) -> &StatementCache {
        &self.statement_cache
    }

    /// Mutably borrow the per-session [`StatementCache`].
    pub fn statement_cache_mut(&mut self) -> &mut StatementCache {
        &mut self.statement_cache
    }

    /// Allocate the next monotonic numeric savepoint token (`>= 1`).
    pub fn next_savepoint_token(&mut self) -> u64 {
        self.next_savepoint_token = self.next_savepoint_token.saturating_add(1);
        self.next_savepoint_token
    }

    /// Allocate the next unique server portal name (`vtb_p_<id>`).
    pub fn next_portal_name(&mut self) -> String {
        self.next_portal_id = self.next_portal_id.saturating_add(1);
        let name = format!("vtb_p_{}", self.next_portal_id);
        self.open_portals.push(name.clone());
        name
    }

    /// Unregister a closed portal name from `open_portals`.
    pub fn unregister_portal_name(&mut self, portal_name: &str) {
        self.open_portals.retain(|p| p != portal_name);
    }

    /// Return the number of unconsumed backend bytes currently buffered.
    pub fn buffered_read_len(&self) -> usize {
        self.read_buf.len()
    }

    /// Return the number of unflushed frontend bytes currently buffered.
    pub fn buffered_write_len(&self) -> usize {
        self.write_buf.len()
    }

    /// Return `true` if a previously prepared unnamed statement (`""`) remains valid on the wire.
    pub const fn is_unnamed_statement_valid(&self) -> bool {
        self.unnamed_statement_valid
    }

    /// Return `true` if a previously prepared unnamed statement (`""`) remains valid on the wire.
    pub const fn unnamed_statement_valid(&self) -> bool {
        self.unnamed_statement_valid
    }

    /// Update the local validity state of the unnamed prepared statement (`""`).
    pub fn set_unnamed_statement_valid(&mut self, valid: bool) {
        self.unnamed_statement_valid = valid;
    }

    /// Borrow the session-local [`crate::types::TypeCatalog`].
    pub const fn type_catalog(&self) -> &crate::types::TypeCatalog {
        &self.type_catalog
    }

    /// Mutably borrow the session-local [`crate::types::TypeCatalog`].
    pub fn type_catalog_mut(&mut self) -> &mut crate::types::TypeCatalog {
        &mut self.type_catalog
    }

    /// Return the current protocol [`ExchangeState`].
    pub const fn exchange_state(&self) -> ExchangeState {
        self.exchange_state
    }

    /// Update the protocol [`ExchangeState`] guard.
    pub fn set_exchange_state(&mut self, state: ExchangeState) {
        self.exchange_state = state;
    }

    /// Register or update a warm [`crate::extended::PreparedDescription`] for `sql`.
    pub fn register_prepared_description(
        &mut self,
        sql: impl Into<String>,
        description: crate::extended::PreparedDescription,
    ) {
        self.statement_descriptions.insert(sql.into(), description);
    }

    /// Central hook invoked whenever a `NotificationResponse` frame is observed on the wire.
    pub fn dispatch_notification(&mut self, pid: i32, channel: String, payload: String) {
        self.notifications.push(NotificationEvent {
            process_id: pid,
            channel,
            payload,
        });
    }

    /// Drain all buffered asynchronous `NotificationResponse` events in arrival order.
    pub fn drain_notifications(&mut self) -> Vec<NotificationEvent> {
        std::mem::take(&mut self.notifications)
    }

    /// Handle an asynchronous `ParameterStatus` frame, updating `TypeCatalog` and `PoolerCapabilities` when applicable.
    pub fn handle_parameter_status_frame(
        &mut self,
        body: &ParameterStatusBody,
    ) -> Result<(), PostgresError> {
        let name = body
            .name()
            .map_err(|e| {
                self.mark_closed();
                PostgresError::Protocol(format!("invalid ParameterStatus name UTF-8: {e}"))
            })?
            .to_owned();
        let val = body
            .value()
            .map_err(|e| {
                self.mark_closed();
                PostgresError::Protocol(format!("invalid ParameterStatus value UTF-8: {e}"))
            })?
            .to_owned();
        if name.eq_ignore_ascii_case("search_path") {
            self.type_catalog.invalidate_extension_names();
            self.statement_descriptions.clear();
            self.statement_cache.invalidate_all();
        }
        let is_pooler_key = statements::is_pooler_banner_parameter(&name);
        self.parameters.insert(name, val);
        if is_pooler_key {
            self.pooler_capabilities = PoolerCapabilities::resolve(
                self.configured_pooler_mode,
                self.configured_statement_cache_size,
                self.configured_protocol_level_pooler_cache,
                &self.parameters,
            );
            self.statement_cache
                .set_capacity(self.pooler_capabilities.effective_capacity);
        }
        Ok(())
    }

    /// Handle an asynchronous `NotificationResponse` frame.
    pub fn handle_notification_frame(
        &mut self,
        body: &NotificationResponseBody,
    ) -> Result<(), PostgresError> {
        let channel = body
            .channel()
            .map_err(|e| {
                self.mark_closed();
                PostgresError::Protocol(format!("invalid NotificationResponse channel UTF-8: {e}"))
            })?
            .to_owned();
        let payload = body
            .message()
            .map_err(|e| {
                self.mark_closed();
                PostgresError::Protocol(format!("invalid NotificationResponse payload UTF-8: {e}"))
            })?
            .to_owned();
        self.dispatch_notification(body.process_id(), channel, payload);
        Ok(())
    }

    /// Start an explicit transaction (`BEGIN`), verifying server transition to `'T'`.
    pub async fn begin(&mut self) -> Result<TransactionDisposition, PostgresError> {
        if !self.pooler_capabilities.supports_scoped_state() {
            return Err(PostgresError::Interface(
                "cannot start explicit transaction in PoolerMode::Statement".to_string(),
            ));
        }
        let sql = self.transaction_state.validate_begin()?;
        let results = self.simple_query(sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if self.transaction_status != TransactionStatus::InTransaction || tag != "BEGIN" {
            return Err(PostgresError::Protocol(format!(
                "expected BEGIN -> InTransaction ('T'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        Ok(TransactionDisposition::Begun)
    }

    /// Commit the current transaction (`COMMIT`), rejecting silent `ROLLBACK` completion in aborted state `'E'`.
    pub async fn commit(&mut self) -> Result<TransactionDisposition, PostgresError> {
        let prior_status = self.transaction_status;
        let sql = self.transaction_state.validate_commit()?;
        let results = self.simple_query(sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if prior_status == TransactionStatus::Failed || tag.eq_ignore_ascii_case("ROLLBACK") {
            return Err(PostgresError::Operational(
                "COMMIT on aborted transaction ('E') resulted in server ROLLBACK".to_string(),
            ));
        }
        if self.transaction_status != TransactionStatus::Idle || tag != "COMMIT" {
            return Err(PostgresError::Protocol(format!(
                "expected COMMIT -> Idle ('I'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        Ok(TransactionDisposition::Committed)
    }

    /// Roll back the current transaction (`ROLLBACK`), verifying restoration to `'I'`.
    pub async fn rollback(&mut self) -> Result<TransactionDisposition, PostgresError> {
        let sql = self.transaction_state.validate_rollback()?;
        let results = self.simple_query(sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if self.transaction_status != TransactionStatus::Idle || tag != "ROLLBACK" {
            return Err(PostgresError::Protocol(format!(
                "expected ROLLBACK -> Idle ('I'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        Ok(TransactionDisposition::RolledBack)
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`) inside an active `'T'` transaction.
    pub async fn savepoint(&mut self, token: u64) -> Result<TransactionDisposition, PostgresError> {
        let sql = self.transaction_state.validate_savepoint(token)?;
        let results = self.simple_query(&sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if self.transaction_status != TransactionStatus::InTransaction || tag != "SAVEPOINT" {
            return Err(PostgresError::Protocol(format!(
                "expected SAVEPOINT -> InTransaction ('T'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        self.transaction_state
            .record_savepoint(token, self.transaction_status);
        Ok(TransactionDisposition::SavepointCreated(token))
    }

    /// Release an active numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`).
    pub async fn release_savepoint(
        &mut self,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let (idx, sql) = self.transaction_state.validate_release_savepoint(token)?;
        let results = self.simple_query(&sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if self.transaction_status != TransactionStatus::InTransaction || tag != "RELEASE" {
            return Err(PostgresError::Protocol(format!(
                "expected RELEASE -> InTransaction ('T'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        self.transaction_state
            .apply_release_savepoint(idx, self.transaction_status);
        Ok(TransactionDisposition::SavepointReleased(token))
    }

    /// Roll back to an active numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`), restoring `'T'`.
    pub async fn rollback_to_savepoint(
        &mut self,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let (idx, sql) = self
            .transaction_state
            .validate_rollback_to_savepoint(token)?;
        let results = self.simple_query(&sql).await?;
        let tag = results
            .last()
            .map(|r| r.command_tag.as_str())
            .unwrap_or_default();
        if self.transaction_status != TransactionStatus::InTransaction || tag != "ROLLBACK" {
            return Err(PostgresError::Protocol(format!(
                "expected ROLLBACK TO SAVEPOINT -> InTransaction ('T'), got tag={tag:?} status={:?}",
                self.transaction_status
            )));
        }
        self.transaction_state
            .apply_rollback_to_savepoint(idx, self.transaction_status);
        Ok(TransactionDisposition::RolledBackToSavepoint(token))
    }

    /// Execute the conservative pool-return reset sequence (`ROLLBACK` + portal close + `DISCARD ALL` + baseline GUC restore + cache invalidation).
    pub async fn reset_for_pool_return(&mut self) -> Result<(), PostgresError> {
        if self.closed {
            return Err(PostgresError::Transport(
                "cannot reset a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state == ExchangeState::CopyActive {
            self.mark_closed();
            return Err(PostgresError::Interface(
                "cannot reset connection with active COPY stream".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            self.drain_to_ready_for_query().await?;
        }

        let remaining_portals = std::mem::take(&mut self.open_portals);
        if !remaining_portals.is_empty()
            && self.transaction_status == TransactionStatus::InTransaction
        {
            for portal_name in &remaining_portals {
                self.write_buf.clear();
                frontend::close(b'P', portal_name, &mut self.write_buf).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode Close(P): {e}"))
                })?;
                frontend::sync(&mut self.write_buf);
                self.exchange_state = ExchangeState::InExtendedExchange;
                if let Err(err) = self.stream.write_all(&self.write_buf).await {
                    self.write_buf.clear();
                    self.mark_closed();
                    return Err(PostgresError::Transport(format!(
                        "portal reset Close(P) write failed: {err}"
                    )));
                }
                self.write_buf.clear();
                if let Err(err) = self.stream.flush().await {
                    self.mark_closed();
                    return Err(PostgresError::Transport(format!(
                        "portal reset Close(P) flush failed: {err}"
                    )));
                }
                self.drain_to_ready_for_query().await?;
            }
        }

        if self.transaction_status != TransactionStatus::Idle {
            self.simple_query("ROLLBACK").await?;
            if self.transaction_status != TransactionStatus::Idle {
                self.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "pool reset ROLLBACK did not restore Idle ('I'), got {:?}",
                    self.transaction_status
                )));
            }
        }

        self.statement_cache.clear_for_reset();
        self.statement_descriptions.clear();
        self.type_catalog.invalidate_all();
        self.unnamed_statement_valid = false;
        self.notifications.clear();

        if matches!(
            self.pooler_capabilities.effective_mode,
            PoolerMode::Direct | PoolerMode::Session
        ) {
            for (k, v) in &self.startup_parameters {
                self.parameters.insert(k.clone(), v.clone());
            }
            self.simple_query("DISCARD ALL").await?;
            if self.transaction_status != TransactionStatus::Idle {
                self.mark_closed();
                return Err(PostgresError::Protocol(format!(
                    "pool reset DISCARD ALL did not finish in Idle ('I'), got {:?}",
                    self.transaction_status
                )));
            }

            let mut restore_stmts = Vec::new();
            if self
                .parameters
                .get("application_name")
                .is_some_and(|cur| cur != &self.baseline_application_name)
            {
                restore_stmts.push(format_set_guc_sql(
                    "application_name",
                    &self.baseline_application_name,
                ));
            }
            for (k, v) in &self.configured_session_gucs {
                if k.eq_ignore_ascii_case("client_encoding") {
                    continue;
                }
                restore_stmts.push(format_set_guc_sql(k, v));
            }
            if !restore_stmts.is_empty() {
                let batch_sql = restore_stmts.join("; ");
                self.simple_query(&batch_sql).await?;
                for (k, v) in &self.configured_session_gucs {
                    if !k.eq_ignore_ascii_case("client_encoding") {
                        self.parameters.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn drain_to_ready_for_query(&mut self) -> Result<(), PostgresError> {
        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };
            match msg {
                Message::DataRow(_)
                | Message::PortalSuspended
                | Message::CommandComplete(_)
                | Message::EmptyQueryResponse
                | Message::CloseComplete
                | Message::ParseComplete
                | Message::BindComplete
                | Message::NoData
                | Message::ParameterDescription(_)
                | Message::RowDescription(_)
                | Message::ErrorResponse(_)
                | Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    return Ok(());
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend frame while draining connection to ReadyForQuery"
                            .to_string(),
                    ));
                }
            }
        }
    }

    /// Execute a PostgreSQL Protocol 3.0 simple `Query` (`'Q'`) and drain all frames through `ReadyForQuery`.
    pub async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute simple_query on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute simple_query while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }

        self.unnamed_statement_valid = false;
        self.write_buf.clear();
        postgres_protocol::message::frontend::query(sql, &mut self.write_buf).map_err(|e| {
            PostgresError::Protocol(format!("failed to encode simple Query frame: {e}"))
        })?;

        if let Err(err) = self.stream.write_all(&self.write_buf).await {
            self.write_buf.clear();
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "simple_query write failed: {err}"
            )));
        }
        self.write_buf.clear();
        if let Err(err) = self.stream.flush().await {
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "simple_query flush failed: {err}"
            )));
        }

        let mut collector = crate::query::SimpleQueryCollector::with_catalog(
            self.type_catalog.snapshot_for_batch(),
        );
        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };

            match msg {
                Message::RowDescription(body) => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_row_description(&body, &self.parameters)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::DataRow(body) => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_data_row(&body)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!("invalid CommandComplete tag UTF-8: {e}"))
                    })?;
                    if tag.starts_with("ALTER ")
                        || tag.starts_with("DROP ")
                        || tag.starts_with("CREATE ")
                        || tag.eq_ignore_ascii_case("DISCARD ALL")
                        || tag.starts_with("DEALLOCATE")
                    {
                        self.statement_descriptions.clear();
                        self.statement_cache.invalidate_all();
                    }
                    if let Err(err) = collector.handle_command_complete(tag) {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::EmptyQueryResponse => {
                    if let Err(err) = collector.handle_empty_query() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(server_err) => collector.handle_error_response(server_err),
                    Err(err) => {
                        self.mark_closed();
                        return Err(err);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    match collector.finish(status) {
                        Ok(results) => return Ok(results),
                        Err(err) => {
                            if matches!(
                                err,
                                PostgresError::Protocol(_) | PostgresError::Transport(_)
                            ) {
                                self.mark_closed();
                            }
                            return Err(err);
                        }
                    }
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during simple_query".to_string(),
                    ));
                }
            }
        }
    }

    /// Execute a single-statement extended query and stream each `DataRow` frame into `on_row` without materializing `Vec<Vec<PgValue>>`.
    pub async fn query_with_row_sink<F>(
        &mut self,
        sql: &str,
        params: &[crate::extended::Parameter],
        result_formats: &[crate::result::Format],
        on_columns: impl FnOnce(&[crate::result::Column]) -> Result<(), PostgresError>,
        mut on_row: F,
    ) -> Result<String, PostgresError>
    where
        F: FnMut(&crate::extended::RowBody) -> Result<(), PostgresError>,
    {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute query_with_row_sink on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute query_with_row_sink while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }

        let resolved = crate::metadata::resolve_parameter_metadata(self, params, None).await?;
        let param_formats: Vec<crate::result::Format> = resolved
            .parameter_oids
            .iter()
            .map(|&oid| {
                if !result_formats.is_empty() && oid != 0 {
                    crate::codecs::format_for_oid_with_catalog(oid, &self.type_catalog)
                } else {
                    crate::result::Format::Text
                }
            })
            .collect();
        let req_result_formats: Vec<crate::result::Format> = if result_formats.is_empty() {
            if let Some(desc) = self.statement_descriptions.get(sql) {
                desc.result_columns
                    .iter()
                    .map(|c| {
                        crate::codecs::format_for_oid_with_catalog(c.type_oid, &self.type_catalog)
                    })
                    .collect()
            } else {
                vec![crate::result::Format::Text]
            }
        } else {
            result_formats.to_vec()
        };

        let ctx = crate::codecs::CodecContext::from_parameters(&self.parameters);
        self.write_buf.clear();
        postgres_protocol::message::frontend::parse(
            "",
            sql,
            resolved.parameter_oids.iter().copied(),
            &mut self.write_buf,
        )
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Parse frame: {e}")))?;
        postgres_protocol::message::frontend::describe(b'S', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(S): {e}")))?;
        crate::extended::encode_bind(
            "",
            "",
            &resolved.parameters,
            &resolved.parameter_oids,
            &param_formats,
            &req_result_formats,
            &ctx,
            &self.type_catalog,
            &mut self.write_buf,
        )?;
        postgres_protocol::message::frontend::describe(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(P): {e}")))?;
        postgres_protocol::message::frontend::execute("", 0, &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
        postgres_protocol::message::frontend::close(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Close(P): {e}")))?;
        postgres_protocol::message::frontend::sync(&mut self.write_buf);

        self.unnamed_statement_valid = true;
        self.exchange_state = ExchangeState::InExtendedExchange;

        if let Err(err) = self.stream.write_all(&self.write_buf).await {
            self.write_buf.clear();
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "extended query write failed: {err}"
            )));
        }
        self.write_buf.clear();
        if let Err(err) = self.stream.flush().await {
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "extended query flush failed: {err}"
            )));
        }

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            req_result_formats,
            true,
            true,
        );
        let mut on_columns_opt = Some(on_columns);
        let mut command_tag: Option<String> = None;

        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };

            match msg {
                Message::ParseComplete => {
                    if let Err(err) = collector.handle_parse_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::ParameterDescription(body) => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_parameter_description(&body)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::BindComplete => {
                    if let Err(err) = collector.handle_bind_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::RowDescription(body) => {
                    if !collector.has_error() {
                        if let Err(err) = collector.handle_row_description(&body) {
                            self.mark_closed();
                            return Err(err);
                        }
                        if !collector.portal_columns().is_empty()
                            && let Some(cb) = on_columns_opt.take()
                            && let Err(err) = cb(collector.portal_columns())
                        {
                            collector.handle_error(err);
                        }
                    }
                }
                Message::NoData => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_no_data()
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::DataRow(body) => {
                    if !collector.has_error() {
                        if let Some(cb) = on_columns_opt.take()
                            && let Err(err) = cb(collector.portal_columns())
                        {
                            collector.handle_error(err);
                        }
                        if !collector.has_error()
                            && let Err(err) = on_row(&body)
                        {
                            collector.handle_error(err);
                        }
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!("invalid CommandComplete tag UTF-8: {e}"))
                    })?;
                    if !collector.has_error()
                        && let Some(cb) = on_columns_opt.take()
                        && let Err(err) = cb(collector.portal_columns())
                    {
                        collector.handle_error(err);
                    }
                    if let Err(err) = collector.handle_command_complete(tag) {
                        self.mark_closed();
                        return Err(err);
                    }
                    command_tag = Some(tag.to_owned());
                }
                Message::EmptyQueryResponse => {
                    if !collector.has_error()
                        && let Some(cb) = on_columns_opt.take()
                        && let Err(err) = cb(&[])
                    {
                        collector.handle_error(err);
                    }
                    if let Err(err) = collector.handle_empty_query() {
                        self.mark_closed();
                        return Err(err);
                    }
                    command_tag = Some(String::new());
                }
                Message::CloseComplete => {}
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(server_err) => collector.handle_error_response(server_err),
                    Err(err) => {
                        self.mark_closed();
                        return Err(err);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    let desc = collector.prepared_description();
                    match collector.finish(status) {
                        Ok(_) => {
                            self.statement_descriptions.insert(sql.to_owned(), desc);
                            return Ok(command_tag.unwrap_or_default());
                        }
                        Err(err) => {
                            if matches!(
                                err,
                                PostgresError::Protocol(_) | PostgresError::Transport(_)
                            ) {
                                self.mark_closed();
                            }
                            return Err(err);
                        }
                    }
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during extended query".to_string(),
                    ));
                }
            }
        }
    }

    /// Execute a single-statement PostgreSQL Protocol 3.0 extended query (`Parse`/`Describe`/`Bind`/`Execute`/`Close`/`Sync` or warm named `Bind`).
    pub async fn query(
        &mut self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute query on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute query while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }

        let resolved =
            crate::metadata::resolve_parameter_metadata(self, parameters, parameter_oids).await?;
        let cache_enabled = self.statement_cache.capacity() > 0;
        let stmt_key = StatementKey::new(
            sql.to_owned(),
            resolved.parameter_oids.clone(),
            self.statement_cache.schema_generation(),
        );

        if cache_enabled && let Some(cached) = self.statement_cache.get_by_key(&stmt_key) {
            return self
                .execute_cached_named_statement(&cached.name, &cached.description, &resolved)
                .await;
        }

        let stmt_name = if cache_enabled {
            self.statement_cache.next_statement_name()
        } else {
            self.unnamed_statement_valid = true;
            String::new()
        };

        let evicted = self.statement_cache.take_evicted_names();
        let param_formats = vec![crate::result::Format::Text; resolved.parameters.len()];
        let result_formats = vec![crate::result::Format::Text];
        let captured_ctx = Arc::new(crate::codecs::CodecContext::from_parameters(
            &self.parameters,
        ));

        self.write_buf.clear();
        for ev_name in &evicted {
            frontend::close(b'S', ev_name, &mut self.write_buf).map_err(|e| {
                PostgresError::Protocol(format!("failed to encode Close(S) frame: {e}"))
            })?;
        }
        frontend::parse(
            &stmt_name,
            sql,
            resolved.parameter_oids.iter().copied(),
            &mut self.write_buf,
        )
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Parse frame: {e}")))?;
        frontend::describe(b'S', &stmt_name, &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(S): {e}")))?;
        crate::extended::encode_bind(
            "",
            &stmt_name,
            &resolved.parameters,
            &resolved.parameter_oids,
            &param_formats,
            &result_formats,
            &captured_ctx,
            &self.type_catalog,
            &mut self.write_buf,
        )?;
        frontend::describe(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(P): {e}")))?;
        frontend::execute("", 0, &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
        frontend::close(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Close(P): {e}")))?;
        frontend::sync(&mut self.write_buf);

        self.exchange_state = ExchangeState::InExtendedExchange;

        if let Err(err) = self.stream.write_all(&self.write_buf).await {
            self.write_buf.clear();
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "extended query write failed: {err}"
            )));
        }
        self.write_buf.clear();
        if let Err(err) = self.stream.flush().await {
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "extended query flush failed: {err}"
            )));
        }

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            result_formats,
            true,
            true,
        );
        let mut raw_rows: Vec<Vec<crate::metadata::OwnedTextField>> = Vec::new();
        let mut row_ctx = Arc::clone(&captured_ctx);
        let mut command_tag: Option<String> = None;
        let mut affected_rows = 0u64;

        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };

            match msg {
                Message::ParseComplete => {
                    if let Err(err) = collector.handle_parse_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::ParameterDescription(body) => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_parameter_description(&body)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::BindComplete => {
                    if let Err(err) = collector.handle_bind_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::RowDescription(body) => {
                    if !collector.has_error() {
                        row_ctx = Arc::new(crate::codecs::CodecContext::from_parameters(
                            &self.parameters,
                        ));
                        if let Err(err) = collector.handle_row_description(&body) {
                            self.mark_closed();
                            return Err(err);
                        }
                    }
                }
                Message::NoData => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_no_data()
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::DataRow(body) => {
                    if !collector.has_error() {
                        let cols = collector.portal_columns();
                        if cols.is_empty() {
                            self.mark_closed();
                            return Err(PostgresError::Protocol(
                                "received DataRow without preceding RowDescription".to_string(),
                            ));
                        }
                        let buffer = body.buffer();
                        let mut ranges = body.ranges();
                        let mut owned_row = Vec::with_capacity(cols.len());
                        while let Some(range_opt) = ranges.next().map_err(|e| {
                            self.mark_closed();
                            PostgresError::Protocol(format!("malformed DataRow field range: {e}"))
                        })? {
                            owned_row.push(crate::metadata::OwnedTextField {
                                bytes: range_opt.map(|r| buffer[r].to_vec()),
                            });
                        }
                        if owned_row.len() != cols.len() {
                            self.mark_closed();
                            return Err(PostgresError::Protocol(format!(
                                "DataRow field count {} does not match column count {}",
                                owned_row.len(),
                                cols.len()
                            )));
                        }
                        raw_rows.push(owned_row);
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!("invalid CommandComplete tag UTF-8: {e}"))
                    })?;
                    affected_rows = match crate::query::parse_command_tag_affected_rows(tag) {
                        Ok(n) => n,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    if let Err(err) = collector.handle_command_complete(tag) {
                        self.mark_closed();
                        return Err(err);
                    }
                    command_tag = Some(tag.to_owned());
                }
                Message::EmptyQueryResponse => {
                    if let Err(err) = collector.handle_empty_query() {
                        self.mark_closed();
                        return Err(err);
                    }
                    affected_rows = 0;
                    command_tag = Some(String::new());
                }
                Message::CloseComplete => {}
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(server_err) => {
                        if statements::is_statement_invalidation_sqlstate(&server_err.sqlstate) {
                            self.statement_cache.invalidate_all();
                            self.statement_descriptions.clear();
                        }
                        collector.handle_error_response(server_err);
                    }
                    Err(err) => {
                        self.mark_closed();
                        return Err(err);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    let desc = collector.prepared_description();
                    let portal_cols = collector.portal_columns().to_vec();
                    match collector.finish(status) {
                        Ok(_) => {
                            let tag = command_tag.unwrap_or_default();
                            if tag.starts_with("ALTER ")
                                || tag.starts_with("DROP ")
                                || tag.starts_with("CREATE ")
                                || tag.eq_ignore_ascii_case("DISCARD ALL")
                                || tag.starts_with("DEALLOCATE")
                            {
                                self.statement_descriptions.clear();
                                self.statement_cache.invalidate_all();
                            } else {
                                self.statement_descriptions
                                    .insert(sql.to_owned(), desc.clone());
                                if cache_enabled {
                                    self.statement_cache.insert_named(
                                        stmt_name,
                                        stmt_key.sql,
                                        stmt_key.param_oids,
                                        desc,
                                    );
                                }
                            }
                            let mut owned = crate::metadata::OwnedTextResult::new(
                                portal_cols,
                                raw_rows,
                                affected_rows,
                                tag,
                                row_ctx,
                            );
                            owned.mark_completed(status);
                            return crate::metadata::normalize_materialized_result(self, owned)
                                .await;
                        }
                        Err(err) => {
                            if matches!(
                                err,
                                PostgresError::Protocol(_) | PostgresError::Transport(_)
                            ) {
                                self.mark_closed();
                            }
                            return Err(err);
                        }
                    }
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during extended query".to_string(),
                    ));
                }
            }
        }
    }

    async fn execute_cached_named_statement(
        &mut self,
        stmt_name: &str,
        description: &crate::extended::PreparedDescription,
        resolved: &crate::metadata::ResolvedParameters,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let effective_param_oids: Vec<u32> = resolved
            .parameter_oids
            .iter()
            .enumerate()
            .map(|(i, &oid)| {
                if oid != 0 {
                    oid
                } else {
                    description.parameter_oids.get(i).copied().unwrap_or(0)
                }
            })
            .collect();
        let param_formats: Vec<crate::result::Format> = effective_param_oids
            .iter()
            .map(|&oid| {
                if oid != 0 {
                    crate::codecs::format_for_oid_with_catalog(oid, &self.type_catalog)
                } else {
                    crate::result::Format::Text
                }
            })
            .collect();
        let result_formats: Vec<crate::result::Format> = description
            .result_columns
            .iter()
            .map(|col| crate::codecs::format_for_oid_with_catalog(col.type_oid, &self.type_catalog))
            .collect();

        let evicted = self.statement_cache.take_evicted_names();
        let ctx = crate::codecs::CodecContext::from_parameters(&self.parameters);
        self.write_buf.clear();
        for ev_name in &evicted {
            frontend::close(b'S', ev_name, &mut self.write_buf).map_err(|e| {
                PostgresError::Protocol(format!("failed to encode Close(S) frame: {e}"))
            })?;
        }
        crate::extended::encode_bind(
            "",
            stmt_name,
            &resolved.parameters,
            &effective_param_oids,
            &param_formats,
            &result_formats,
            &ctx,
            &self.type_catalog,
            &mut self.write_buf,
        )?;
        frontend::describe(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(P): {e}")))?;
        frontend::execute("", 0, &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
        frontend::close(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Close(P): {e}")))?;
        frontend::sync(&mut self.write_buf);

        self.exchange_state = ExchangeState::InExtendedExchange;

        if let Err(err) = self.stream.write_all(&self.write_buf).await {
            self.write_buf.clear();
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "cached extended query write failed: {err}"
            )));
        }
        self.write_buf.clear();
        if let Err(err) = self.stream.flush().await {
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "cached extended query flush failed: {err}"
            )));
        }

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            result_formats,
            false,
            true,
        );
        let mut row_ctx = ctx;

        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };

            match msg {
                Message::BindComplete => {
                    if let Err(err) = collector.handle_bind_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::RowDescription(body) => {
                    if !collector.has_error() {
                        row_ctx = crate::codecs::CodecContext::from_parameters(&self.parameters);
                        if let Err(err) = collector.handle_row_description(&body) {
                            self.mark_closed();
                            return Err(err);
                        }
                    }
                }
                Message::NoData => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_no_data()
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::DataRow(body) => {
                    if !collector.has_error()
                        && let Err(err) =
                            collector.handle_data_row(&body, &row_ctx, &self.type_catalog)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!("invalid CommandComplete tag UTF-8: {e}"))
                    })?;
                    if tag.starts_with("ALTER ")
                        || tag.starts_with("DROP ")
                        || tag.starts_with("CREATE ")
                        || tag.eq_ignore_ascii_case("DISCARD ALL")
                        || tag.starts_with("DEALLOCATE")
                    {
                        self.statement_descriptions.clear();
                        self.statement_cache.invalidate_all();
                    }
                    if let Err(err) = collector.handle_command_complete(tag) {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::EmptyQueryResponse => {
                    if let Err(err) = collector.handle_empty_query() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::CloseComplete => {}
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(server_err) => {
                        if statements::is_statement_invalidation_sqlstate(&server_err.sqlstate) {
                            self.statement_cache.invalidate_all();
                            self.statement_descriptions.clear();
                        }
                        collector.handle_error_response(server_err);
                    }
                    Err(err) => {
                        self.mark_closed();
                        return Err(err);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    match collector.finish(status) {
                        Ok(res) => return Ok(res),
                        Err(err) => {
                            if matches!(
                                err,
                                PostgresError::Protocol(_) | PostgresError::Transport(_)
                            ) {
                                self.mark_closed();
                            }
                            return Err(err);
                        }
                    }
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during cached extended query".to_string(),
                    ));
                }
            }
        }
    }

    /// Execute a warm extended query using known [`crate::extended::PreparedDescription`] metadata to negotiate mixed binary/text formats.
    pub async fn query_with_description(
        &mut self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        description: &crate::extended::PreparedDescription,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute query_with_description on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute query_with_description while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }

        let resolved =
            crate::metadata::resolve_parameter_metadata(self, parameters, parameter_oids).await?;
        let effective_param_oids: Vec<u32> = resolved
            .parameter_oids
            .iter()
            .enumerate()
            .map(|(i, &oid)| {
                if oid != 0 {
                    oid
                } else {
                    description.parameter_oids.get(i).copied().unwrap_or(0)
                }
            })
            .collect();
        let param_formats: Vec<crate::result::Format> = effective_param_oids
            .iter()
            .map(|&oid| {
                if oid != 0 {
                    crate::codecs::format_for_oid_with_catalog(oid, &self.type_catalog)
                } else {
                    crate::result::Format::Text
                }
            })
            .collect();
        let result_formats: Vec<crate::result::Format> = description
            .result_columns
            .iter()
            .map(|col| crate::codecs::format_for_oid_with_catalog(col.type_oid, &self.type_catalog))
            .collect();

        let ctx = crate::codecs::CodecContext::from_parameters(&self.parameters);
        self.write_buf.clear();
        postgres_protocol::message::frontend::parse(
            "",
            sql,
            effective_param_oids.iter().copied(),
            &mut self.write_buf,
        )
        .map_err(|e| PostgresError::Protocol(format!("failed to encode Parse frame: {e}")))?;
        postgres_protocol::message::frontend::describe(b'S', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(S): {e}")))?;
        crate::extended::encode_bind(
            "",
            "",
            &resolved.parameters,
            &effective_param_oids,
            &param_formats,
            &result_formats,
            &ctx,
            &self.type_catalog,
            &mut self.write_buf,
        )?;
        postgres_protocol::message::frontend::describe(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Describe(P): {e}")))?;
        postgres_protocol::message::frontend::execute("", 0, &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Execute frame: {e}")))?;
        postgres_protocol::message::frontend::close(b'P', "", &mut self.write_buf)
            .map_err(|e| PostgresError::Protocol(format!("failed to encode Close(P): {e}")))?;
        postgres_protocol::message::frontend::sync(&mut self.write_buf);

        self.unnamed_statement_valid = true;
        self.exchange_state = ExchangeState::InExtendedExchange;

        if let Err(err) = self.stream.write_all(&self.write_buf).await {
            self.write_buf.clear();
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "warm extended query write failed: {err}"
            )));
        }
        self.write_buf.clear();
        if let Err(err) = self.stream.flush().await {
            self.mark_closed();
            return Err(PostgresError::Transport(format!(
                "warm extended query flush failed: {err}"
            )));
        }

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            result_formats,
            true,
            true,
        );
        let mut row_ctx = ctx;

        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, None).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };

            match msg {
                Message::ParseComplete => {
                    if let Err(err) = collector.handle_parse_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::ParameterDescription(body) => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_parameter_description(&body)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::BindComplete => {
                    if let Err(err) = collector.handle_bind_complete() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::RowDescription(body) => {
                    if !collector.has_error() {
                        row_ctx = crate::codecs::CodecContext::from_parameters(&self.parameters);
                        if let Err(err) = collector.handle_row_description(&body) {
                            self.mark_closed();
                            return Err(err);
                        }
                    }
                }
                Message::NoData => {
                    if !collector.has_error()
                        && let Err(err) = collector.handle_no_data()
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::DataRow(body) => {
                    if !collector.has_error()
                        && let Err(err) =
                            collector.handle_data_row(&body, &row_ctx, &self.type_catalog)
                    {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!("invalid CommandComplete tag UTF-8: {e}"))
                    })?;
                    if let Err(err) = collector.handle_command_complete(tag) {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::EmptyQueryResponse => {
                    if let Err(err) = collector.handle_empty_query() {
                        self.mark_closed();
                        return Err(err);
                    }
                }
                Message::CloseComplete => {}
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(server_err) => collector.handle_error_response(server_err),
                    Err(err) => {
                        self.mark_closed();
                        return Err(err);
                    }
                },
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.exchange_state = ExchangeState::Idle;
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(err) => {
                            self.mark_closed();
                            return Err(err);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    match collector.finish(status) {
                        Ok(res) => return Ok(res),
                        Err(err) => {
                            if matches!(
                                err,
                                PostgresError::Protocol(_) | PostgresError::Transport(_)
                            ) {
                                self.mark_closed();
                            }
                            return Err(err);
                        }
                    }
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during warm extended query".to_string(),
                    ));
                }
            }
        }
    }

    /// Send a single 5-byte `Terminate` (`'X'`) frame and shut down the socket once.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.exchange_state = ExchangeState::Idle;
        let _ = self.stream.write_all(&TERMINATE_FRAME).await;
        let _ = self.stream.flush().await;
        let _ = self.stream.shutdown().await;
        Ok(())
    }
}

/// Physical connection backing a [`SessionHandle`] (either direct socket ownership or a pooled lease).
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SessionBacking {
    /// Direct socket that terminates with `'X'` on `close()`.
    Direct(PostgresConnection),
    /// Borrowed pool lease that resets and returns to [`crate::pool::PostgresPool`] on `close()`.
    Pooled(crate::pool::PooledConnection),
}

impl SessionBacking {
    /// Close or return the underlying connection.
    pub async fn close(self) -> Result<(), PostgresError> {
        match self {
            Self::Direct(mut conn) => conn.close().await,
            Self::Pooled(lease) => lease.release().await,
        }
    }
}

impl Deref for SessionBacking {
    type Target = PostgresConnection;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Direct(c) => c,
            Self::Pooled(p) => p,
        }
    }
}

impl DerefMut for SessionBacking {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Direct(c) => c,
            Self::Pooled(p) => p,
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum SessionState {
    Disconnected,
    Connecting { generation: u64 },
    Ready(SessionBacking),
    InFlight { generation: u64 },
    Closed { generation: u64 },
}

#[derive(Debug)]
struct SessionInner {
    state: SessionState,
    next_generation: u64,
    parameters: BTreeMap<String, String>,
    last_transaction_status: TransactionStatus,
    pinned_owner: Option<TransactionOwner>,
    active_portal: Option<(TransactionOwner, String)>,
}

/// Lock-free operation guard checking out [`SessionBacking`] without holding a `Mutex` across `.await`, Python conversion, or GIL reacquisition.
pub(crate) struct SessionOperationGuard<'a> {
    handle: &'a SessionHandle,
    backing: Option<SessionBacking>,
    portal_mode: bool,
}

impl SessionOperationGuard<'_> {
    /// Mutably borrow the checked-out [`PostgresConnection`].
    pub fn connection_mut(&mut self) -> &mut PostgresConnection {
        self.backing.as_mut().expect("SessionOperationGuard active")
    }
}

impl Drop for SessionOperationGuard<'_> {
    fn drop(&mut self) {
        let Some(mut backing) = self.backing.take() else {
            return;
        };
        if !self.portal_mode && backing.exchange_state != ExchangeState::Idle {
            backing.mark_closed();
        }
        let mut guard = self
            .handle
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.parameters = backing.parameters.clone();
        guard.last_transaction_status = backing.transaction_status();
        if backing.transaction_status() == TransactionStatus::Idle && guard.active_portal.is_none()
        {
            guard.pinned_owner = None;
        }
        if matches!(guard.state, SessionState::InFlight { .. }) {
            guard.state = SessionState::Ready(backing);
        }
    }
}

/// Explicit PostgreSQL session lifecycle handle (`Disconnected -> Connecting -> Ready -> Closed`).
pub struct SessionHandle {
    config: PostgresConfig,
    cloud_connector: Arc<CloudConnector>,
    token_provider: ScopedTokenProvider,
    clock: Arc<dyn Clock>,
    inner: std::sync::Mutex<SessionInner>,
}

/// Type alias uniting [`ClientSession`] and [`SessionHandle`].
pub type ClientSession = SessionHandle;

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("config", &self.config)
            .field("is_ready", &self.is_ready())
            .field("is_closed", &self.is_closed())
            .field("generation", &self.generation())
            .finish()
    }
}

/// Outcome of reserving a physical connection start on [`SessionHandle`].
pub enum ClientBeginConnect {
    Attempt(ClientConnectAttempt),
    AlreadyReady,
}

impl SessionHandle {
    /// Validate `raw` options without performing any network, DNS, or filesystem I/O.
    pub fn new(raw: RawPostgresOptions) -> Result<Self, PostgresError> {
        let config = PostgresConfig::validate(raw)?;
        Ok(Self::from_config(config))
    }

    /// Create a disconnected [`SessionHandle`] from a pre-validated [`PostgresConfig`].
    pub fn from_config(config: PostgresConfig) -> Self {
        Self::with_clock(config, Arc::new(SystemClock))
    }

    /// Create a disconnected [`SessionHandle`] with an injected [`Clock`].
    pub fn with_clock(config: PostgresConfig, clock: Arc<dyn Clock>) -> Self {
        let cloud_connector = Arc::new(CloudConnector::with_clock(Arc::clone(&clock)));
        let token_provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
            Arc::clone(&clock),
        );
        Self {
            config,
            cloud_connector,
            token_provider,
            clock,
            inner: std::sync::Mutex::new(SessionInner {
                state: SessionState::Disconnected,
                next_generation: 0,
                parameters: BTreeMap::new(),
                last_transaction_status: TransactionStatus::Idle,
                pinned_owner: None,
                active_portal: None,
            }),
        }
    }

    /// Construct a ready [`SessionHandle`] backed by a checked-out [`crate::pool::PooledConnection`] lease.
    pub fn from_pooled(lease: crate::pool::PooledConnection) -> Self {
        let config = lease.pool().config().clone();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let cloud_connector = Arc::new(CloudConnector::with_clock(Arc::clone(&clock)));
        let token_provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
            Arc::clone(&clock),
        );
        let generation = lease.generation();
        let parameters = lease.parameters.clone();
        let status = lease.transaction_status();
        Self {
            config,
            cloud_connector,
            token_provider,
            clock,
            inner: std::sync::Mutex::new(SessionInner {
                state: SessionState::Ready(SessionBacking::Pooled(lease)),
                next_generation: generation,
                parameters,
                last_transaction_status: status,
                pinned_owner: None,
                active_portal: None,
            }),
        }
    }

    /// Borrow the validated [`PostgresConfig`].
    pub const fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// Return `true` only when an authenticated [`PostgresConnection`] has been published.
    pub fn is_ready(&self) -> bool {
        let guard = self.inner.lock().expect("SessionHandle lock poisoned");
        matches!(
            guard.state,
            SessionState::Ready(ref b) if !b.is_closed()
        ) || matches!(guard.state, SessionState::InFlight { .. })
    }

    /// Return `true` when the session is not currently active (`Disconnected`, `Connecting`, or `Closed`), matching Python `client.closed`.
    pub fn is_client_closed(&self) -> bool {
        !self.is_ready()
    }

    /// Return `true` when the session is in the terminal or post-failure `Closed` state.
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Closed { .. } => true,
            SessionState::Ready(b) => b.is_closed(),
            _ => false,
        }
    }

    /// Return the current or most recent connection generation (`0` before the first connect attempt).
    pub fn generation(&self) -> u64 {
        let guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Disconnected => guard.next_generation,
            SessionState::Connecting { generation }
            | SessionState::InFlight { generation }
            | SessionState::Closed { generation } => *generation,
            SessionState::Ready(conn) => conn.generation(),
        }
    }

    /// Return the most recently synchronized [`TransactionStatus`].
    pub fn transaction_status(&self) -> TransactionStatus {
        let guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Ready(conn) => conn.transaction_status(),
            _ => guard.last_transaction_status,
        }
    }

    /// Return a cloned snapshot of retained `ParameterStatus` entries.
    pub fn parameters(&self) -> BTreeMap<String, String> {
        let guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Ready(conn) => conn.parameters.clone(),
            _ => guard.parameters.clone(),
        }
    }

    /// Mutably borrow the established [`PostgresConnection`] if `Ready`.
    pub fn connection_mut(&mut self) -> Option<&mut PostgresConnection> {
        let inner = self.inner.get_mut().expect("SessionHandle lock poisoned");
        match &mut inner.state {
            SessionState::Ready(backing) => Some(&mut **backing),
            _ => None,
        }
    }

    /// Begin a new connection attempt, returning a drop-safe [`ConnectAttempt`].
    pub fn begin_connect(&mut self) -> Result<ConnectAttempt<'_>, PostgresError> {
        let inner = self.inner.get_mut().expect("SessionHandle lock poisoned");
        if matches!(
            inner.state,
            SessionState::Ready(_)
                | SessionState::InFlight { .. }
                | SessionState::Connecting { .. }
        ) {
            return Err(PostgresError::Interface(
                "cannot call connect on an already connected or connecting SessionHandle"
                    .to_string(),
            ));
        }
        inner.next_generation = inner.next_generation.saturating_add(1);
        let generation = inner.next_generation;
        inner.state = SessionState::Connecting { generation };
        let deadline = Instant::now() + self.config.connect_timeout;
        Ok(ConnectAttempt {
            pending: PendingSession {
                handle: Some(self),
                generation,
            },
            deadline,
        })
    }

    /// Establish a session under the single `connect_timeout` deadline.
    pub async fn connect(&mut self) -> Result<&mut PostgresConnection, PostgresError> {
        let attempt = self.begin_connect()?;
        attempt.run().await
    }

    /// Close the active connection (sending `Terminate` once if direct `Ready`, or releasing if pooled) and mark `Closed`.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if let Some(backing) = self.begin_close()? {
            backing.close().await?;
        }
        Ok(())
    }

    /// Reserve a startup transition (`Disconnected -> Connecting`) on an `Arc<SessionHandle>`, return `AlreadyReady` if `Ready`, or reject `Connecting`/`Closed`.
    pub fn begin_client_connect(self: &Arc<Self>) -> Result<ClientBeginConnect, PostgresError> {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match guard.state {
            SessionState::Disconnected => {
                guard.next_generation = guard.next_generation.saturating_add(1);
                let generation = guard.next_generation;
                guard.state = SessionState::Connecting { generation };
                let deadline = Instant::now() + self.config.connect_timeout;
                Ok(ClientBeginConnect::Attempt(ClientConnectAttempt {
                    session: Some(Arc::clone(self)),
                    generation,
                    deadline,
                }))
            }
            SessionState::Ready(_) | SessionState::InFlight { .. } => {
                Ok(ClientBeginConnect::AlreadyReady)
            }
            SessionState::Connecting { .. } => Err(PostgresError::Interface(
                "connection attempt is already in progress".to_string(),
            )),
            SessionState::Closed { .. } => Err(PostgresError::Interface(
                "cannot connect a closed client".to_string(),
            )),
        }
    }

    /// Synchronously transition to `Closed` and extract any live [`SessionBacking`] for shutdown or pool return.
    pub fn begin_close(&self) -> Result<Option<SessionBacking>, PostgresError> {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        if guard.active_portal.is_some() {
            return Err(PostgresError::Interface(
                "cannot close session while a Portal is active".to_string(),
            ));
        }
        match guard.state {
            SessionState::Connecting { .. } | SessionState::InFlight { .. } => {
                Err(PostgresError::Interface(
                    "cannot close client while an operation or connect is in progress".to_string(),
                ))
            }
            SessionState::Disconnected => {
                let generation = guard.next_generation;
                guard.state = SessionState::Closed { generation };
                Ok(None)
            }
            SessionState::Closed { .. } => Ok(None),
            SessionState::Ready(_) => {
                let next_gen = guard.next_generation;
                let prev = std::mem::replace(
                    &mut guard.state,
                    SessionState::Closed {
                        generation: next_gen,
                    },
                );
                guard.pinned_owner = None;
                if let SessionState::Ready(backing) = prev {
                    guard.state = SessionState::Closed {
                        generation: backing.generation(),
                    };
                    Ok(Some(backing))
                } else {
                    unreachable!("checked Ready")
                }
            }
        }
    }

    /// Reserve exclusive operation ownership without holding a `Mutex` across `.await`.
    pub(crate) async fn acquire_operation(
        &self,
    ) -> Result<SessionOperationGuard<'_>, PostgresError> {
        self.acquire_operation_for(None).await
    }

    /// Reserve exclusive operation ownership for a specific [`TransactionOwner`] (or default unpinned caller).
    pub(crate) async fn acquire_operation_for(
        &self,
        owner: Option<TransactionOwner>,
    ) -> Result<SessionOperationGuard<'_>, PostgresError> {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        if guard.active_portal.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute operation while a Portal is active on this session".to_string(),
            ));
        }
        if let Some(pinned) = guard.pinned_owner {
            match owner {
                Some(req) if req == pinned => {}
                Some(_) => {
                    return Err(PostgresError::Interface(
                        "session is pinned by another TransactionOwner".to_string(),
                    ));
                }
                None if pinned.id != 0 => {
                    return Err(PostgresError::Interface(
                        "session is pinned by an explicit TransactionOwner".to_string(),
                    ));
                }
                None => {}
            }
        } else if let Some(req) = owner
            && let SessionState::Ready(ref b) = guard.state
            && req.generation != b.generation()
        {
            return Err(PostgresError::Interface(format!(
                "TransactionOwner generation {} does not match session generation {}",
                req.generation,
                b.generation()
            )));
        }

        match guard.state {
            SessionState::Ready(_) => {
                let generation = guard.next_generation;
                let prev =
                    std::mem::replace(&mut guard.state, SessionState::InFlight { generation });
                if let SessionState::Ready(backing) = prev {
                    if backing.is_closed() {
                        guard.state = SessionState::Closed {
                            generation: backing.generation(),
                        };
                        return Err(PostgresError::Interface(
                            "session connection is closed".to_string(),
                        ));
                    }
                    guard.state = SessionState::InFlight {
                        generation: backing.generation(),
                    };
                    Ok(SessionOperationGuard {
                        handle: self,
                        backing: Some(backing),
                        portal_mode: false,
                    })
                } else {
                    unreachable!("checked Ready")
                }
            }
            SessionState::InFlight { .. } => Err(PostgresError::Interface(
                "another operation is already in flight on this session".to_string(),
            )),
            SessionState::Connecting { .. } => Err(PostgresError::Interface(
                "session connection is still in progress".to_string(),
            )),
            SessionState::Disconnected | SessionState::Closed { .. } => Err(
                PostgresError::Interface("session is not connected".to_string()),
            ),
        }
    }

    /// Reserve exclusive operation ownership for the active [`Portal`] identified by `(owner, portal_name)`.
    pub(crate) async fn acquire_portal_operation(
        &self,
        owner: TransactionOwner,
        portal_name: &str,
    ) -> Result<SessionOperationGuard<'_>, PostgresError> {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        match &guard.active_portal {
            Some((active_owner, active_name))
                if *active_owner == owner && active_name == portal_name => {}
            _ => {
                return Err(PostgresError::Interface(format!(
                    "portal `{portal_name}` is not the active pinned portal on this session"
                )));
            }
        }
        match guard.state {
            SessionState::Ready(_) => {
                let generation = guard.next_generation;
                let prev =
                    std::mem::replace(&mut guard.state, SessionState::InFlight { generation });
                if let SessionState::Ready(backing) = prev {
                    if backing.is_closed() {
                        guard.state = SessionState::Closed {
                            generation: backing.generation(),
                        };
                        return Err(PostgresError::Interface(
                            "session connection is closed".to_string(),
                        ));
                    }
                    guard.state = SessionState::InFlight {
                        generation: backing.generation(),
                    };
                    Ok(SessionOperationGuard {
                        handle: self,
                        backing: Some(backing),
                        portal_mode: true,
                    })
                } else {
                    unreachable!("checked Ready")
                }
            }
            SessionState::InFlight { .. } => Err(PostgresError::Interface(
                "another operation is already in flight on this session".to_string(),
            )),
            _ => Err(PostgresError::Interface(
                "session is not connected".to_string(),
            )),
        }
    }

    /// Unpin an active [`Portal`] when it completes or closes.
    pub(crate) fn unpin_portal(&self, portal_name: &str, owner: TransactionOwner) {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        if let SessionState::Ready(ref mut backing) = guard.state {
            backing.unregister_portal_name(portal_name);
            guard.last_transaction_status = backing.transaction_status();
        }
        if matches!(
            &guard.active_portal,
            Some((active_owner, active_name)) if *active_owner == owner && active_name == portal_name
        ) {
            guard.active_portal = None;
            if guard.last_transaction_status == TransactionStatus::Idle {
                guard.pinned_owner = None;
            } else if let Some(pinned) = guard.pinned_owner
                && pinned == owner
            {
                guard.pinned_owner = None;
            }
        }
    }

    /// Execute an async closure with exclusive access to `(&mut PostgresConnection, PoolerMode)`.
    pub async fn with_connection<F, Fut, T>(&self, f: F) -> Result<T, PostgresError>
    where
        F: FnOnce(&mut PostgresConnection, PoolerMode) -> Fut,
        Fut: std::future::Future<Output = Result<T, PostgresError>>,
    {
        let mut op = self.acquire_operation().await?;
        let conn = op.connection_mut();
        let mode = conn.pooler_capabilities.effective_mode;
        f(conn, mode).await
    }

    /// Start an explicit transaction (`BEGIN`) pinned to `owner`.
    pub async fn begin_as(
        &self,
        owner: TransactionOwner,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        let disp = op.connection_mut().begin().await?;
        drop(op);
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        guard.pinned_owner = Some(owner);
        Ok(disp)
    }

    /// Commit an active transaction (`COMMIT`) pinned to `owner`.
    pub async fn commit_as(
        &self,
        owner: TransactionOwner,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        op.connection_mut().commit().await
    }

    /// Roll back an active transaction (`ROLLBACK`) pinned to `owner`.
    pub async fn rollback_as(
        &self,
        owner: TransactionOwner,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        op.connection_mut().rollback().await
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        op.connection_mut().savepoint(token).await
    }

    /// Release a numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn release_savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        op.connection_mut().release_savepoint(token).await
    }

    /// Roll back to a numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn rollback_to_savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let mut op = self.acquire_operation_for(Some(owner)).await?;
        op.connection_mut().rollback_to_savepoint(token).await
    }

    /// Execute an extended query under `owner`.
    pub async fn query_as(
        &self,
        owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let mut op = self.acquire_operation_for(owner).await?;
        op.connection_mut()
            .query(sql, parameters, parameter_oids)
            .await
    }

    /// Execute an extended query on the shared session without holding a `Mutex` across `.await`.
    pub async fn query_shared(
        &self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        self.query_as(None, sql, parameters, parameter_oids).await
    }

    /// Execute a simple query (`'Q'`) on the shared session without holding a `Mutex` across `.await`.
    pub async fn simple_query_shared(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        let mut op = self.acquire_operation().await?;
        op.connection_mut().simple_query(sql).await
    }

    /// Open a session-retaining named [`Portal`] inside an owned `BEGIN` or nested savepoint [`TransactionScope`].
    pub async fn open_portal(
        self: &Arc<Self>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
    ) -> Result<Portal, PostgresError> {
        if batch_size <= 0 {
            return Err(PostgresError::Interface(
                "open_portal requires batch_size >= 1 (batch_size <= 0 is forbidden)".to_string(),
            ));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }

        let mut op = self.acquire_operation().await?;
        let conn = op.connection_mut();
        if !conn.pooler_capabilities.supports_scoped_state() {
            return Err(PostgresError::Interface(
                "cannot open_portal in PoolerMode::Statement".to_string(),
            ));
        }
        if conn.transaction_status() == TransactionStatus::Failed {
            return Err(PostgresError::Interface(
                "cannot open_portal while transaction is aborted ('E')".to_string(),
            ));
        }

        let resolved =
            crate::metadata::resolve_parameter_metadata(conn, parameters, parameter_oids).await?;

        let portal_name = conn.next_portal_name();
        let owner = TransactionOwner::new(
            0x8000_0000_0000_0000 | conn.next_portal_id,
            conn.generation(),
        );
        let scope = match conn.transaction_status() {
            TransactionStatus::Idle => {
                conn.begin().await?;
                TransactionScope::new(owner, TransactionScopeKind::ExplicitTransaction)
            }
            TransactionStatus::InTransaction => {
                let token = conn.next_savepoint_token();
                conn.savepoint(token).await?;
                TransactionScope::new(owner, TransactionScopeKind::NestedSavepoint { token })
            }
            TransactionStatus::Failed => {
                conn.unregister_portal_name(&portal_name);
                return Err(PostgresError::Interface(
                    "cannot open_portal while transaction is aborted ('E')".to_string(),
                ));
            }
        };

        let setup_res = setup_named_portal_on_conn(conn, &portal_name, sql, &resolved).await;
        match setup_res {
            Ok((columns, codec_ctx, catalog_snapshot)) => {
                let generation = conn.generation();
                drop(op);
                {
                    let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
                    guard.active_portal = Some((owner, portal_name.clone()));
                    if guard.pinned_owner.is_none() {
                        guard.pinned_owner = Some(owner);
                    }
                }
                Ok(Portal::new(
                    Arc::clone(self),
                    portal_name,
                    generation,
                    scope,
                    columns,
                    codec_ctx,
                    catalog_snapshot,
                    DEFAULT_MAX_RESULT_BYTES,
                ))
            }
            Err(err) => {
                conn.unregister_portal_name(&portal_name);
                let _ = crate::portal::close_and_rollback_portal_on_conn(
                    conn,
                    &portal_name,
                    &scope,
                    false,
                )
                .await;
                Err(err)
            }
        }
    }

    fn rollback_connecting(&self, generation: u64) {
        let mut guard = self.inner.lock().expect("SessionHandle lock poisoned");
        if matches!(
            guard.state,
            SessionState::Connecting { generation: g } if g == generation
        ) {
            guard.state = SessionState::Closed { generation };
        }
    }
}

async fn setup_named_portal_on_conn(
    conn: &mut PostgresConnection,
    portal_name: &str,
    sql: &str,
    resolved: &crate::metadata::ResolvedParameters,
) -> Result<
    (
        Vec<crate::result::Column>,
        Arc<crate::codecs::CodecContext>,
        crate::types::TypeCatalog,
    ),
    PostgresError,
> {
    let param_formats: Vec<crate::result::Format> = resolved
        .parameter_oids
        .iter()
        .map(|&oid| {
            if oid != 0 {
                crate::codecs::format_for_oid_with_catalog(oid, &conn.type_catalog)
            } else {
                crate::result::Format::Text
            }
        })
        .collect();
    let result_formats = vec![crate::result::Format::Text];
    let ctx = crate::codecs::CodecContext::from_parameters(&conn.parameters);

    conn.write_buf.clear();
    frontend::parse(
        "",
        sql,
        resolved.parameter_oids.iter().copied(),
        &mut conn.write_buf,
    )
    .map_err(|e| PostgresError::Protocol(format!("failed to encode Portal Parse: {e}")))?;
    frontend::describe(b'S', "", &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode Portal Describe(S): {e}"))
    })?;
    crate::extended::encode_bind(
        portal_name,
        "",
        &resolved.parameters,
        &resolved.parameter_oids,
        &param_formats,
        &result_formats,
        &ctx,
        &conn.type_catalog,
        &mut conn.write_buf,
    )?;
    frontend::describe(b'P', portal_name, &mut conn.write_buf).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode Portal Describe(P): {e}"))
    })?;
    frontend::sync(&mut conn.write_buf);

    conn.unnamed_statement_valid = true;
    conn.exchange_state = ExchangeState::InExtendedExchange;

    if let Err(err) = conn.stream.write_all(&conn.write_buf).await {
        conn.write_buf.clear();
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "open_portal write failed: {err}"
        )));
    }
    conn.write_buf.clear();
    if let Err(err) = conn.stream.flush().await {
        conn.mark_closed();
        return Err(PostgresError::Transport(format!(
            "open_portal flush failed: {err}"
        )));
    }

    let mut collector = crate::extended::ExtendedQueryCollector::new(
        resolved.parameters.len(),
        result_formats,
        true,
        true,
    );
    let mut portal_err: Option<PostgresError> = None;
    let mut custom_oids_to_discover: Vec<u32> = Vec::new();

    loop {
        let msg = match read_message(&mut conn.stream, &mut conn.read_buf, None).await {
            Ok(m) => m,
            Err(err) => {
                conn.mark_closed();
                return Err(err);
            }
        };

        match msg {
            Message::ParseComplete => {
                if let Err(err) = collector.handle_parse_complete() {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::ParameterDescription(body) => {
                if !collector.has_error()
                    && let Err(err) = collector.handle_parameter_description(&body)
                {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::BindComplete => {
                if let Err(err) = collector.handle_bind_complete() {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::RowDescription(body) => {
                if !collector.has_error()
                    && let Err(err) = collector.handle_row_description(&body)
                {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::NoData => {
                if !collector.has_error()
                    && let Err(err) = collector.handle_no_data()
                {
                    conn.mark_closed();
                    return Err(err);
                }
            }
            Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                Ok(server_err) => {
                    if portal_err.is_none() {
                        portal_err = Some(PostgresError::Server(server_err.clone()));
                    }
                    collector.handle_error_response(server_err);
                }
                Err(err) => {
                    conn.mark_closed();
                    return Err(err);
                }
            },
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                conn.handle_parameter_status_frame(&body)?;
            }
            Message::NotificationResponse(body) => {
                conn.handle_notification_frame(&body)?;
            }
            Message::ReadyForQuery(body) => {
                conn.exchange_state = ExchangeState::Idle;
                let status = match TransactionStatus::from_wire_tag(body.status()) {
                    Ok(s) => s,
                    Err(err) => {
                        conn.mark_closed();
                        return Err(err);
                    }
                };
                conn.set_transaction_status_from_wire(status);
                if let Some(err) = portal_err {
                    return Err(err);
                }
                let cols = collector.portal_columns().to_vec();
                for col in &cols {
                    if conn.type_catalog.type_info(col.type_oid).is_none() {
                        custom_oids_to_discover.push(col.type_oid);
                    }
                }
                if !custom_oids_to_discover.is_empty() {
                    let mut cat = conn.type_catalog.clone();
                    let _ = cat.discover(conn, &custom_oids_to_discover, &[]).await;
                }
                let codec_ctx = Arc::new(crate::codecs::CodecContext::from_parameters(
                    &conn.parameters,
                ));
                let catalog_snapshot = conn.type_catalog.clone();
                return Ok((cols, codec_ctx, catalog_snapshot));
            }
            _ => {
                conn.mark_closed();
                return Err(PostgresError::Protocol(
                    "unexpected backend frame during open_portal setup".to_string(),
                ));
            }
        }
    }
}

/// Drop guard ensuring a cancelled or failed startup attempt transitions [`SessionHandle`] to `Closed`.
pub struct PendingSession<'a> {
    handle: Option<&'a mut SessionHandle>,
    generation: u64,
}

impl<'a> PendingSession<'a> {
    fn config(&self) -> &PostgresConfig {
        &self.handle.as_ref().expect("PendingSession armed").config
    }

    fn cloud_connector(&self) -> Arc<CloudConnector> {
        Arc::clone(
            &self
                .handle
                .as_ref()
                .expect("PendingSession armed")
                .cloud_connector,
        )
    }

    fn token_provider(&self) -> ScopedTokenProvider {
        self.handle
            .as_ref()
            .expect("PendingSession armed")
            .token_provider
            .clone()
    }

    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.handle.as_ref().expect("PendingSession armed").clock)
    }

    fn commit(mut self, conn: PostgresConnection) -> &'a mut PostgresConnection {
        let handle = self.handle.take().expect("PendingSession armed");
        let inner = handle.inner.get_mut().expect("SessionHandle lock poisoned");
        inner.parameters = conn.parameters.clone();
        inner.last_transaction_status = conn.transaction_status();
        inner.state = SessionState::Ready(SessionBacking::Direct(conn));
        match &mut inner.state {
            SessionState::Ready(backing) => backing,
            _ => unreachable!("SessionState::Ready was just assigned"),
        }
    }
}

impl Drop for PendingSession<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let inner = handle.inner.get_mut().expect("SessionHandle lock poisoned");
            inner.state = SessionState::Closed {
                generation: self.generation,
            };
        }
    }
}

/// Single-deadline startup driver created by [`SessionHandle::begin_connect`].
pub struct ConnectAttempt<'a> {
    pending: PendingSession<'a>,
    deadline: Instant,
}

impl<'a> ConnectAttempt<'a> {
    /// Execute TCP/TLS/Unix/Cloud dial, TCP keepalive setup, `StartupMessage`, authentication, and `ReadyForQuery(Idle)` verification.
    pub async fn run(self) -> Result<&'a mut PostgresConnection, PostgresError> {
        let Self { pending, deadline } = self;
        let config = pending.config().clone();
        let connector = pending.cloud_connector();
        let token_provider = pending.token_provider();
        let clock = pending.clock();
        let generation = pending.generation;

        let conn = establish_postgres_connection(
            &config,
            &connector,
            &token_provider,
            clock.as_ref(),
            generation,
            deadline,
        )
        .await?;
        Ok(pending.commit(conn))
    }
}

pub(crate) async fn establish_postgres_connection(
    config: &PostgresConfig,
    connector: &CloudConnector,
    token_provider: &ScopedTokenProvider,
    clock: &dyn Clock,
    generation: u64,
    deadline: Instant,
) -> Result<PostgresConnection, PostgresError> {
    let (mut stream, tls_negotiated, tls_server_end_point, login_snapshot) =
        if config.cloud_sql_instance.is_some() {
            let dialed = cloud::dial_cloudsql(config, connector, deadline).await?;
            (
                dialed.stream,
                true,
                dialed.tls_server_end_point,
                dialed.login_snapshot,
            )
        } else if config.alloydb_instance_uri.is_some() {
            let dialed = cloud::dial_alloydb(config, connector, deadline).await?;
            (
                dialed.stream,
                true,
                dialed.tls_server_end_point,
                dialed.login_snapshot,
            )
        } else if let Some(ref unix_path) = config.unix_socket {
            let direct_login = if config.enable_iam_auth {
                let scopes = config.direct_iam_default_scopes();
                Some(
                    token_provider
                        .resolve_direct_login(&scopes, deadline)
                        .await?,
                )
            } else {
                None
            };
            let stream = dial_unix_socket(unix_path, deadline).await?;
            (stream, false, None, direct_login)
        } else {
            let direct_login = if config.enable_iam_auth {
                let scopes = config.direct_iam_default_scopes();
                Some(
                    token_provider
                        .resolve_direct_login(&scopes, deadline)
                        .await?,
                )
            } else {
                None
            };
            let dialed = tls::connect(config, deadline).await?;
            (
                dialed.stream,
                dialed.tls_negotiated,
                dialed.tls_server_end_point,
                direct_login,
            )
        };

    if !matches!(stream, TransportStream::Unix(_)) {
        stream
            .set_tcp_keepalive(config.tcp_keepalive())
            .map_err(|e| PostgresError::Transport(e.to_string()))?;
    }

    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(1024);
    encode_startup_frame(config, &mut write_buf)?;

    tokio::time::timeout_at(deadline, async {
        stream.write_all(&write_buf).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| PostgresError::Transport("StartupMessage write timed out".to_string()))?
    .map_err(|e| PostgresError::Transport(format!("StartupMessage write failed: {e}")))?;
    write_buf.clear();

    let mut auth = Authentication::new();
    let mut parameters = BTreeMap::new();
    let mut backend_key = None;

    loop {
        let msg = read_message(&mut stream, &mut read_buf, Some(deadline)).await?;
        match msg {
            Message::ErrorResponse(body) => {
                let err = parse_error_fields(body.fields())?;
                if let PostgresError::Server(ref srv) = err
                    && srv.sqlstate.starts_with("28")
                    && let Some(ref snap) = login_snapshot
                {
                    snap.invalidate_generation();
                }
                return Err(redact_session_error(
                    err,
                    config.password.as_deref(),
                    login_snapshot.as_ref(),
                ));
            }
            Message::NoticeResponse(_) => {}
            Message::ParameterStatus(body) => {
                if !auth.is_complete() {
                    return Err(PostgresError::Protocol(
                        "received ParameterStatus before AuthenticationOk".to_string(),
                    ));
                }
                let name = body
                    .name()
                    .map_err(|e| {
                        PostgresError::Protocol(format!("invalid ParameterStatus name: {e}"))
                    })?
                    .to_string();
                let val = body
                    .value()
                    .map_err(|e| {
                        PostgresError::Protocol(format!("invalid ParameterStatus value: {e}"))
                    })?
                    .to_string();
                parameters.insert(name, val);
            }
            Message::BackendKeyData(body) => {
                if !auth.is_complete() {
                    return Err(PostgresError::Protocol(
                        "received BackendKeyData before AuthenticationOk".to_string(),
                    ));
                }
                backend_key = Some(BackendKey {
                    process_id: body.process_id(),
                    secret_key: body.secret_key(),
                });
            }
            Message::ReadyForQuery(body) => {
                if !auth.is_complete() {
                    return Err(PostgresError::Protocol(
                        "received ReadyForQuery before AuthenticationOk".to_string(),
                    ));
                }
                let status = TransactionStatus::from_wire_tag(body.status())?;
                if status != TransactionStatus::Idle {
                    return Err(PostgresError::Protocol(format!(
                        "startup ReadyForQuery reported non-Idle status {status:?}"
                    )));
                }
                let pooler_capabilities = PoolerCapabilities::resolve(
                    config.pooler_mode,
                    config.statement_cache_size,
                    config.protocol_level_pooler_cache,
                    &parameters,
                );
                let statement_cache = StatementCache::new(pooler_capabilities.effective_capacity);
                let startup_parameters = parameters.clone();
                let configured_session_gucs: BTreeMap<String, String> = config
                    .session_gucs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                return Ok(PostgresConnection {
                    stream: ConnectionStream::Transport(stream),
                    read_buf,
                    write_buf,
                    parameters,
                    startup_parameters,
                    configured_session_gucs,
                    baseline_application_name: config.application_name.clone(),
                    backend_key,
                    transaction_status: status,
                    transaction_state: TransactionState::new(),
                    generation,
                    tls_negotiated,
                    tls_server_end_point,
                    unnamed_statement_valid: false,
                    type_catalog: crate::types::TypeCatalog::new_with_builtins(),
                    exchange_state: ExchangeState::Idle,
                    statement_descriptions: std::collections::HashMap::new(),
                    pooler_capabilities,
                    statement_cache,
                    configured_pooler_mode: config.pooler_mode,
                    configured_statement_cache_size: config.statement_cache_size,
                    configured_protocol_level_pooler_cache: config.protocol_level_pooler_cache,
                    next_savepoint_token: 0,
                    next_portal_id: 0,
                    open_portals: Vec::new(),
                    notifications: Vec::new(),
                    closed: false,
                });
            }
            other => {
                let completed = auth
                    .handle_message_with_login(
                        &other,
                        config,
                        tls_server_end_point.as_deref(),
                        login_snapshot.as_ref(),
                        clock,
                        &mut write_buf,
                    )
                    .map_err(|e| {
                        redact_session_error(e, config.password.as_deref(), login_snapshot.as_ref())
                    })?;
                if !write_buf.is_empty() {
                    if Instant::now() >= deadline {
                        return Err(PostgresError::Transport(
                            "authentication deadline exceeded before response flush".to_string(),
                        ));
                    }
                    tokio::time::timeout_at(deadline, async {
                        stream.write_all(&write_buf).await?;
                        stream.flush().await
                    })
                    .await
                    .map_err(|_| {
                        PostgresError::Transport(
                            "authentication response write timed out".to_string(),
                        )
                    })?
                    .map_err(|e| {
                        PostgresError::Transport(format!(
                            "authentication response write failed: {e}"
                        ))
                    })?;
                    write_buf.clear();
                }
                let _ = completed;
            }
        }
    }
}

/// Owned, `'static`-compatible startup attempt that rolls back [`SessionHandle`] to `Closed` on drop.
pub struct ClientConnectAttempt {
    session: Option<Arc<SessionHandle>>,
    generation: u64,
    deadline: Instant,
}

impl Drop for ClientConnectAttempt {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            session.rollback_connecting(self.generation);
        }
    }
}

impl ClientConnectAttempt {
    /// Execute startup and return an uncommitted [`ClientPendingSession`] guard.
    pub async fn run(mut self) -> Result<ClientPendingSession, PostgresError> {
        let session = self.session.take().expect("ClientConnectAttempt armed");
        let generation = self.generation;
        let deadline = self.deadline;
        let mut guard = ClientConnectAttempt {
            session: Some(Arc::clone(&session)),
            generation,
            deadline,
        };
        let conn = establish_postgres_connection(
            &session.config,
            &session.cloud_connector,
            &session.token_provider,
            session.clock.as_ref(),
            generation,
            deadline,
        )
        .await?;
        let session = guard.session.take().expect("guard armed");
        Ok(ClientPendingSession {
            session: Some(session),
            conn: Some(conn),
            generation,
        })
    }
}

/// Cancellation-safe publication guard holding an authenticated [`PostgresConnection`] until [`ClientPendingSession::commit`] runs on the caller thread.
pub struct ClientPendingSession {
    session: Option<Arc<SessionHandle>>,
    conn: Option<PostgresConnection>,
    generation: u64,
}

impl ClientPendingSession {
    /// Publish the authenticated [`PostgresConnection`] into [`SessionHandle`] (`Connecting -> Ready`).
    pub fn commit(mut self) -> Result<(), PostgresError> {
        let session = self.session.take().expect("ClientPendingSession armed");
        let conn = self.conn.take().expect("ClientPendingSession connection");
        let mut guard = session.inner.lock().expect("SessionHandle lock poisoned");
        match guard.state {
            SessionState::Connecting { generation } if generation == self.generation => {
                guard.parameters = conn.parameters.clone();
                guard.last_transaction_status = conn.transaction_status();
                guard.state = SessionState::Ready(SessionBacking::Direct(conn));
                Ok(())
            }
            _ => Err(PostgresError::Interface(
                "client was closed before connection completed".to_string(),
            )),
        }
    }
}

impl Drop for ClientPendingSession {
    fn drop(&mut self) {
        let _ = self.conn.take();
        if let Some(session) = self.session.take() {
            session.rollback_connecting(self.generation);
        }
    }
}

fn format_set_guc_sql(key: &str, value: &str) -> String {
    let escaped_ident = key.replace('"', "\"\"");
    let escaped_val = value.replace('\'', "''");
    format!("SET \"{escaped_ident}\" = '{escaped_val}'")
}

#[cfg(unix)]
async fn dial_unix_socket(path: &str, deadline: Instant) -> Result<TransportStream, PostgresError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(PostgresError::Transport(
            "Unix domain socket connection deadline exceeded".to_string(),
        ));
    }
    let remaining = deadline
        .saturating_duration_since(now)
        .max(Duration::from_millis(1));
    let opts = ConnectOptions::unix(path).with_connect_timeout(remaining);
    tokio::time::timeout_at(deadline, TransportStream::connect(&opts))
        .await
        .map_err(|_| {
            PostgresError::Transport("Unix domain socket connection timed out".to_string())
        })?
        .map_err(|e| PostgresError::Transport(e.to_string()))
}

#[cfg(not(unix))]
async fn dial_unix_socket(
    _path: &str,
    _deadline: Instant,
) -> Result<TransportStream, PostgresError> {
    Err(PostgresError::UnsupportedFeature(
        "Unix domain sockets are not supported on this platform".to_string(),
    ))
}

fn redact_session_error(
    err: PostgresError,
    password: Option<&str>,
    login_snapshot: Option<&ScopedLoginSnapshot>,
) -> PostgresError {
    let sanitize = |mut s: String| -> String {
        if let Some(pw) = password.filter(|p| !p.is_empty()) {
            s = s.replace(pw, "<redacted>");
        }
        if let Some(snap) = login_snapshot {
            let tok = snap.access_token();
            if !tok.is_empty() {
                s = s.replace(tok, "<redacted>");
            }
        }
        s
    };
    match err {
        PostgresError::Config(m) => PostgresError::Config(sanitize(m)),
        PostgresError::Interface(m) => PostgresError::Interface(sanitize(m)),
        PostgresError::Data(m) => PostgresError::Data(sanitize(m)),
        PostgresError::Operational(m) => PostgresError::Operational(sanitize(m)),
        PostgresError::PoolExhausted(m) => PostgresError::PoolExhausted(sanitize(m)),
        PostgresError::Auth(m) => PostgresError::Auth(sanitize(m)),
        PostgresError::UnsupportedAuth(m) => PostgresError::UnsupportedAuth(sanitize(m)),
        PostgresError::UnsupportedFeature(m) => PostgresError::UnsupportedFeature(sanitize(m)),
        PostgresError::Transport(m) => PostgresError::Transport(sanitize(m)),
        PostgresError::Protocol(m) => PostgresError::Protocol(sanitize(m)),
        PostgresError::Server(srv) => PostgresError::Server(ServerError {
            severity: srv.severity,
            sqlstate: srv.sqlstate,
            message: sanitize(srv.message),
            diagnostics: srv
                .diagnostics
                .into_iter()
                .map(|(k, v)| (k, sanitize(v)))
                .collect(),
        }),
        PostgresError::AfterCompletion {
            source,
            completed_result,
        } => PostgresError::AfterCompletion {
            source: Box::new(redact_session_error(*source, password, login_snapshot)),
            completed_result,
        },
        PostgresError::PipelineValidation {
            source,
            failing_index,
        } => PostgresError::PipelineValidation {
            source: Box::new(redact_session_error(*source, password, login_snapshot)),
            failing_index,
        },
        PostgresError::PipelineAborted(mut failure) => {
            failure.first_error =
                redact_session_error(failure.first_error, password, login_snapshot);
            PostgresError::PipelineAborted(failure)
        }
    }
}
