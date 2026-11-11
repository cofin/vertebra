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

/// Retained pipeline transaction/savepoint scopes (`crate::session::pipeline_scope`).
#[path = "pipeline_scope.rs"]
pub mod pipeline_scope;
pub use self::pipeline_scope::PipelineScopeToken;

/// Generation-fenced PostgreSQL `CancelRequest` engine (`crate::session::cancel`).
#[path = "cancel.rs"]
pub mod cancel;
pub use self::cancel::{
    CancelDisposition, CancelSecret, OperationGeneration, PostgresCancelHandle,
};

use self::cancel::{CancelKeyData, StandaloneCancelState};
use self::pipeline_scope::ActivePipelineScope;
use self::statements::{PoolerCapabilities, PoolerMode, StatementCache, StatementKey};

/// Default maximum materialized result / portal chunk byte budget (`64 MiB`).
pub const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// Non-renewing single-`Instant` command I/O budget computed at native operation reservation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CommandBudget(Option<Instant>);

impl CommandBudget {
    /// Construct an untimed [`CommandBudget`] (`None`).
    pub const fn untimed() -> Self {
        Self(None)
    }

    /// Wrap an explicit optional absolute [`Instant`].
    pub const fn from_deadline(deadline: Option<Instant>) -> Self {
        Self(deadline)
    }

    /// Validate an optional timeout in seconds before reserving session state or emitting wire bytes.
    ///
    /// Rejects `<= 0.0`, `NaN`, `+-infinity`, sub-nanosecond values that round to zero, and values
    /// overflowing [`Duration`] or [`Instant::checked_add`] with [`PostgresError::Interface`].
    pub fn validate_optional_seconds(
        timeout: Option<f64>,
    ) -> Result<Option<Duration>, PostgresError> {
        let Some(secs) = timeout else {
            return Ok(None);
        };
        if !secs.is_finite() || secs <= 0.0 {
            return Err(PostgresError::Interface(format!(
                "timeout must be a finite positive float (> 0.0), got {secs}"
            )));
        }
        let duration = Duration::try_from_secs_f64(secs).map_err(|_| {
            PostgresError::Interface(format!("timeout {secs} overflowed Duration bounds"))
        })?;
        if duration.is_zero() {
            return Err(PostgresError::Interface(format!(
                "timeout {secs} rounded down to a zero Duration"
            )));
        }
        if Instant::now().checked_add(duration).is_none() {
            return Err(PostgresError::Interface(format!(
                "timeout {secs} overflowed Instant deadline bounds"
            )));
        }
        Ok(Some(duration))
    }

    /// Compute one absolute non-renewing [`Instant`] once native operation reservation succeeds.
    pub fn start_from_duration(duration: Option<Duration>) -> Result<Self, PostgresError> {
        match duration {
            None => Ok(Self(None)),
            Some(dur) => Instant::now()
                .checked_add(dur)
                .map(|dl| Self(Some(dl)))
                .ok_or_else(|| {
                    PostgresError::Interface("timeout overflowed Instant deadline bounds".into())
                }),
        }
    }

    /// Return the underlying optional absolute [`Instant`].
    pub const fn deadline(&self) -> Option<Instant> {
        self.0
    }

    /// Return `true` when an explicit deadline was set and `Instant::now() >= deadline`.
    pub fn is_expired(&self) -> bool {
        self.0.is_some_and(|dl| Instant::now() >= dl)
    }
}

/// RAII guard scoping `PostgresConnection::active_deadline` to a single timed command and clearing
/// it automatically on return, error, panic, or task drop so subsequent calls and cancel cleanup
/// never inherit a stale deadline.
pub struct CommandDeadlineGuard<'a> {
    conn: &'a mut PostgresConnection,
    prev_deadline: Option<Instant>,
    prev_defer_read_close: bool,
}

impl CommandDeadlineGuard<'_> {
    pub(crate) const fn wire_started(&self) -> bool {
        self.conn.deadline_wire_started
    }

    pub(crate) const fn timed_out(&self) -> bool {
        self.conn.deadline_timed_out
    }

    pub(crate) const fn write_uncertain(&self) -> bool {
        self.conn.deadline_write_uncertain
    }
}

impl Deref for CommandDeadlineGuard<'_> {
    type Target = PostgresConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
    }
}

impl DerefMut for CommandDeadlineGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
    }
}

impl Drop for CommandDeadlineGuard<'_> {
    fn drop(&mut self) {
        self.conn.active_deadline = self.prev_deadline;
        self.conn.defer_deadline_read_close = self.prev_defer_read_close;
    }
}

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
    pub(crate) cancel_key: Option<CancelKeyData>,
    pub(crate) standalone_cancel: Arc<StandaloneCancelState>,
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
    pub(crate) active_deadline: Option<Instant>,
    pub(crate) deadline_wire_started: bool,
    pub(crate) deadline_timed_out: bool,
    pub(crate) deadline_write_uncertain: bool,
    pub(crate) defer_deadline_read_close: bool,
    pub(crate) copy_in_owned_tx: bool,
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
        let cancel_key = backend_key.map(CancelKeyData::from);
        let standalone_cancel = Arc::new(StandaloneCancelState {
            config: config.clone(),
            cloud_connector: Arc::new(CloudConnector::new()),
            key: cancel_key.clone(),
            in_exchange: std::sync::atomic::AtomicBool::new(false),
            cancel_sent: std::sync::atomic::AtomicBool::new(false),
        });
        Self {
            stream: ConnectionStream::Custom(Box::new(stream)),
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(1024),
            parameters,
            startup_parameters,
            configured_session_gucs,
            baseline_application_name: config.application_name.clone(),
            backend_key,
            cancel_key,
            standalone_cancel,
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
            active_deadline: None,
            deadline_wire_started: false,
            deadline_timed_out: false,
            deadline_write_uncertain: false,
            defer_deadline_read_close: false,
            copy_in_owned_tx: false,
            closed: false,
        }
    }

    /// Return the currently active per-command I/O deadline (`None` for untimed operations).
    pub const fn active_deadline(&self) -> Option<Instant> {
        self.active_deadline
    }

    /// Install an RAII-scoped command deadline (`active_deadline`) on this connection.
    pub fn enter_command_deadline(
        &mut self,
        deadline: Option<Instant>,
    ) -> CommandDeadlineGuard<'_> {
        let prev_deadline = self.active_deadline;
        let prev_defer_read_close = self.defer_deadline_read_close;
        self.active_deadline = deadline;
        self.deadline_wire_started = false;
        self.deadline_timed_out = false;
        self.deadline_write_uncertain = false;
        self.defer_deadline_read_close = false;
        CommandDeadlineGuard {
            conn: self,
            prev_deadline,
            prev_defer_read_close,
        }
    }

    pub(crate) fn enter_session_command_deadline(
        &mut self,
        deadline: Option<Instant>,
    ) -> CommandDeadlineGuard<'_> {
        let prev_deadline = self.active_deadline;
        let prev_defer_read_close = self.defer_deadline_read_close;
        self.active_deadline = deadline;
        self.deadline_wire_started = false;
        self.deadline_timed_out = false;
        self.deadline_write_uncertain = false;
        self.defer_deadline_read_close = deadline.is_some();
        CommandDeadlineGuard {
            conn: self,
            prev_deadline,
            prev_defer_read_close,
        }
    }

    /// Execute `f` with `active_deadline` installed via RAII [`CommandDeadlineGuard`], ensuring
    /// `active_deadline` resets to its previous value on return, error, or cancellation.
    pub async fn with_command_deadline<F, T>(
        &mut self,
        deadline: Option<Instant>,
        f: F,
    ) -> Result<T, PostgresError>
    where
        F: for<'c> FnOnce(
            &'c mut PostgresConnection,
        )
            -> vertebra_db::connection::BoxFuture<'c, Result<T, PostgresError>>,
    {
        let mut guard = self.enter_command_deadline(deadline);
        f(&mut guard).await
    }

    /// Execute an extended query under an RAII [`CommandBudget`].
    pub async fn query_with_deadline(
        &mut self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        budget: CommandBudget,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let mut guard = self.enter_command_deadline(budget.deadline());
        guard.query(sql, parameters, parameter_oids).await
    }

    /// Execute a simple query (`'Q'`) under an RAII [`CommandBudget`].
    pub async fn simple_query_with_deadline(
        &mut self,
        sql: &str,
        budget: CommandBudget,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        let mut guard = self.enter_command_deadline(budget.deadline());
        guard.simple_query(sql).await
    }

    pub(crate) async fn write_command_buf(
        &mut self,
        op_label: &str,
        was_unnamed_pre_marked: bool,
    ) -> Result<(), PostgresError> {
        let mut started_now = false;
        let res = crate::query::write_command_buffer(
            &mut self.stream,
            &mut self.write_buf,
            self.active_deadline,
            &mut started_now,
            op_label,
        )
        .await;
        if started_now {
            self.deadline_wire_started = true;
        }
        match res {
            Ok(_) => Ok(()),
            Err(crate::query::CommandIoOutcome::DeadlineExceeded) => {
                self.deadline_timed_out = true;
                if started_now {
                    self.deadline_write_uncertain = true;
                    self.mark_closed();
                } else {
                    if was_unnamed_pre_marked {
                        self.unnamed_statement_valid = false;
                    }
                    self.set_exchange_state(ExchangeState::Idle);
                    self.standalone_cancel
                        .in_exchange
                        .store(false, std::sync::atomic::Ordering::Release);
                }
                Err(PostgresError::Operational(
                    crate::query::COMMAND_IO_DEADLINE_EXCEEDED_MSG.to_string(),
                ))
            }
            Err(crate::query::CommandIoOutcome::Other(err)) => {
                self.mark_closed();
                Err(err)
            }
        }
    }

    pub(crate) async fn read_command_frame(&mut self) -> Result<Message, PostgresError> {
        self.deadline_wire_started = true;
        match crate::query::read_command_message(
            &mut self.stream,
            &mut self.read_buf,
            self.active_deadline,
        )
        .await
        {
            Ok(msg) => Ok(msg),
            Err(crate::query::CommandIoOutcome::DeadlineExceeded) => {
                self.deadline_timed_out = true;
                if !self.defer_deadline_read_close {
                    self.mark_closed();
                }
                Err(PostgresError::Operational(
                    crate::query::COMMAND_IO_DEADLINE_EXCEEDED_MSG.to_string(),
                ))
            }
            Err(crate::query::CommandIoOutcome::Other(err)) => {
                self.mark_closed();
                Err(err)
            }
        }
    }

    /// Read and decode the next raw PostgreSQL backend [`Message`] frame from the dedicated socket.
    ///
    /// Safe to cancel at `deadline == None` inside a `tokio::select!` pump loop because partial
    /// frame bytes remain buffered inside `self.read_buf` until a complete bounded frame arrives.
    pub(crate) async fn read_raw_message(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<Message, PostgresError> {
        if self.closed {
            return Err(PostgresError::Transport(
                "PostgreSQL connection is closed".to_string(),
            ));
        }
        match read_message(&mut self.stream, &mut self.read_buf, deadline).await {
            Ok(msg) => Ok(msg),
            Err(err) => {
                self.mark_closed();
                Err(err)
            }
        }
    }

    /// Encode and flush a single simple `Query` (`'Q'`) command frame without consuming backend replies,
    /// allowing a dedicated notification or replication pump to drive response framing directly.
    pub(crate) async fn send_simple_command(
        &mut self,
        sql: &str,
        deadline: Option<Instant>,
    ) -> Result<(), PostgresError> {
        if self.closed {
            return Err(PostgresError::Transport(
                "PostgreSQL connection is closed".to_string(),
            ));
        }
        if sql.as_bytes().contains(&0) {
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }
        self.unnamed_statement_valid = false;
        self.write_buf.clear();
        if let Err(err) = frontend::query(sql, &mut self.write_buf) {
            self.write_buf.clear();
            return Err(PostgresError::Protocol(format!(
                "failed to encode simple Query frame: {err}"
            )));
        }
        let write_res = match deadline {
            Some(dl) => {
                if Instant::now() >= dl {
                    self.write_buf.clear();
                    self.mark_closed();
                    return Err(PostgresError::Transport(
                        "PostgreSQL simple command write timed out".to_string(),
                    ));
                }
                tokio::time::timeout_at(dl, async {
                    self.stream.write_all(&self.write_buf).await?;
                    self.stream.flush().await
                })
                .await
                .map_err(|_| {
                    PostgresError::Transport(
                        "PostgreSQL simple command write timed out".to_string(),
                    )
                })
                .and_then(|res| {
                    res.map_err(|e| {
                        PostgresError::Transport(format!(
                            "PostgreSQL simple command write failed: {e}"
                        ))
                    })
                })
            }
            None => async {
                self.stream.write_all(&self.write_buf).await?;
                self.stream.flush().await
            }
            .await
            .map_err(|e| {
                PostgresError::Transport(format!("PostgreSQL simple command write failed: {e}"))
            }),
        };
        self.write_buf.clear();
        if let Err(err) = write_res {
            self.mark_closed();
            return Err(err);
        }
        Ok(())
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
        self.copy_in_owned_tx = false;
        self.exchange_state = ExchangeState::Idle;
        self.standalone_cancel
            .in_exchange
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Return a typed [`PostgresCancelHandle`] targeting this physical connection.
    pub fn postgres_cancel_handle(&self) -> PostgresCancelHandle {
        PostgresCancelHandle::from_standalone(Arc::clone(&self.standalone_cancel))
    }

    /// Return `true` when a standalone [`PostgresCancelHandle`] sent a `CancelRequest` for this connection.
    pub fn has_cancel_sent(&self) -> bool {
        self.standalone_cancel
            .cancel_sent
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Override the connection's Rust-private cancellation key with a variable-length Protocol 3.2
    /// secret (`1..=256` bytes) without exposing `process_id` or key bytes to Python.
    pub fn set_variable_cancel_secret(
        &mut self,
        process_id: i32,
        secret: impl Into<Box<[u8]>>,
    ) -> Result<(), PostgresError> {
        let secret_box = secret.into();
        if secret_box.is_empty()
            || secret_box.len() > self::cancel::MAX_VARIABLE_CANCEL_SECRET_BYTES
        {
            return Err(PostgresError::Protocol(format!(
                "variable-length cancellation key must be 1..={} bytes",
                self::cancel::MAX_VARIABLE_CANCEL_SECRET_BYTES
            )));
        }
        let key = CancelKeyData {
            process_id,
            secret: CancelSecret::Variable(secret_box),
        };
        let new_standalone = Arc::new(StandaloneCancelState {
            config: self.standalone_cancel.config.clone(),
            cloud_connector: Arc::clone(&self.standalone_cancel.cloud_connector),
            key: Some(key.clone()),
            in_exchange: std::sync::atomic::AtomicBool::new(
                !matches!(self.exchange_state, ExchangeState::Idle) && !self.closed,
            ),
            cancel_sent: std::sync::atomic::AtomicBool::new(self.has_cancel_sent()),
        });
        self.cancel_key = Some(key);
        self.standalone_cancel = new_standalone;
        Ok(())
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
        self.standalone_cancel.in_exchange.store(
            !matches!(state, ExchangeState::Idle) && !self.closed,
            std::sync::atomic::Ordering::Release,
        );
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
        self.apply_parameter_status_kv(name, val);
        Ok(())
    }

    pub(crate) fn apply_parameter_status_kv(&mut self, name: String, val: String) {
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
                self.set_exchange_state(ExchangeState::InExtendedExchange);
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
        self.copy_in_owned_tx = false;
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
        self.drain_to_ready_for_query_until(None).await
    }

    pub(crate) async fn drain_to_ready_for_query_until(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<(), PostgresError> {
        loop {
            let msg = match read_message(&mut self.stream, &mut self.read_buf, deadline).await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };
            match msg {
                Message::DataRow(_)
                | Message::CopyData(_)
                | Message::CopyDone
                | Message::CopyOutResponse(_)
                | Message::CopyInResponse(_)
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
                    self.set_exchange_state(ExchangeState::Idle);
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

        let prev_unnamed = self.unnamed_statement_valid;
        self.unnamed_statement_valid = false;
        self.write_buf.clear();
        postgres_protocol::message::frontend::query(sql, &mut self.write_buf).map_err(|e| {
            PostgresError::Protocol(format!("failed to encode simple Query frame: {e}"))
        })?;

        self.standalone_cancel
            .in_exchange
            .store(true, std::sync::atomic::Ordering::Release);
        if let Err(err) = self.write_command_buf("simple_query", false).await {
            if !self.deadline_write_uncertain && !self.closed {
                self.unnamed_statement_valid = prev_unnamed;
            }
            return Err(err);
        }

        let mut collector = crate::query::SimpleQueryCollector::with_catalog(
            self.type_catalog.snapshot_for_batch(),
        );
        loop {
            let msg = self.read_command_frame().await?;

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
                    self.standalone_cancel
                        .in_exchange
                        .store(false, std::sync::atomic::Ordering::Release);
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

        let prev_unnamed = self.unnamed_statement_valid;
        self.unnamed_statement_valid = true;
        self.set_exchange_state(ExchangeState::InExtendedExchange);
        self.write_command_buf("extended query", !prev_unnamed)
            .await?;

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            req_result_formats,
            true,
            true,
        );
        let mut on_columns_opt = Some(on_columns);
        let mut command_tag: Option<String> = None;

        loop {
            let msg = self.read_command_frame().await?;

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
                    self.set_exchange_state(ExchangeState::Idle);
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

        let prev_unnamed = self.unnamed_statement_valid;
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

        self.set_exchange_state(ExchangeState::InExtendedExchange);
        self.write_command_buf("extended query", !cache_enabled && !prev_unnamed)
            .await?;

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
            let msg = self.read_command_frame().await?;

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
                    self.set_exchange_state(ExchangeState::Idle);
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
                            self.deadline_wire_started = false;
                            if self.active_deadline.is_some_and(|dl| Instant::now() >= dl) {
                                return crate::extended::decode_completed_text_result_without_enrichment(
                                    owned,
                                    &self.type_catalog,
                                );
                            }
                            return match crate::metadata::normalize_materialized_result(self, owned)
                                .await
                            {
                                Err(PostgresError::AfterCompletion {
                                    completed_result, ..
                                }) if self.deadline_timed_out && !self.deadline_wire_started => {
                                    self.closed = false;
                                    self.deadline_timed_out = false;
                                    Ok(completed_result)
                                }
                                other => other,
                            };
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

        self.set_exchange_state(ExchangeState::InExtendedExchange);
        self.write_command_buf("cached extended query", false)
            .await?;

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            result_formats,
            false,
            true,
        );
        let mut row_ctx = ctx;

        loop {
            let msg = self.read_command_frame().await?;

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
                    self.set_exchange_state(ExchangeState::Idle);
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

        let prev_unnamed = self.unnamed_statement_valid;
        self.unnamed_statement_valid = true;
        self.set_exchange_state(ExchangeState::InExtendedExchange);
        self.write_command_buf("warm extended query", !prev_unnamed)
            .await?;

        let mut collector = crate::extended::ExtendedQueryCollector::new(
            resolved.parameters.len(),
            result_formats,
            true,
            true,
        );
        let mut row_ctx = ctx;

        loop {
            let msg = self.read_command_frame().await?;

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
                    self.set_exchange_state(ExchangeState::Idle);
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

    /// Initiate `COPY ... TO STDOUT (FORMAT binary)`, validating single-statement SQL, active
    /// exchange/pipeline guards, and binary `CopyOutResponse` overall + per-column format codes.
    pub async fn start_copy_out(&mut self, sql: &str) -> Result<usize, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute COPY TO on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state == ExchangeState::PipelineActive {
            return Err(PostgresError::UnsupportedFeature(
                "COPY TO is not supported while a PostgreSQL pipeline scope is active".to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute COPY TO while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        validate_single_copy_sql(sql)?;

        self.unnamed_statement_valid = false;
        self.write_buf.clear();
        frontend::query(sql, &mut self.write_buf).map_err(|e| {
            PostgresError::Protocol(format!("failed to encode COPY TO Query frame: {e}"))
        })?;
        self.set_exchange_state(ExchangeState::CopyActive);
        self.write_command_buf("start_copy_out", false).await?;

        let mut pre_err: Option<PostgresError> = None;
        let mut unexpected_non_copy = false;
        loop {
            let msg = self.read_command_frame().await?;
            match msg {
                Message::CopyOutResponse(body) => {
                    let overall = body.format();
                    let mut col_formats = Vec::new();
                    let mut iter = body.column_formats();
                    while let Some(fmt) = iter.next().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!(
                            "malformed CopyOutResponse column_formats: {e}"
                        ))
                    })? {
                        col_formats.push(fmt);
                    }
                    if overall != 1 || col_formats.iter().any(|&f| f != 1) {
                        let cancel = self.postgres_cancel_handle();
                        let deadline =
                            Instant::now() + self.standalone_cancel.config.connect_timeout;
                        let _ = cancel.cancel_until(deadline).await;
                        let _ = self.drain_to_ready_for_query_until(Some(deadline)).await;
                        self.mark_closed();
                        return Err(PostgresError::Data(
                            "COPY TO requires FORMAT binary (received text CopyOutResponse)"
                                .to_string(),
                        ));
                    }
                    return Ok(col_formats.len());
                }
                Message::CopyInResponse(_) => {
                    let deadline = Instant::now() + self.standalone_cancel.config.connect_timeout;
                    let _ = crate::copy::abort_copy_in_on_connection(
                        self,
                        "copy_to expected COPY TO STDOUT, got COPY FROM STDIN",
                        deadline,
                    )
                    .await;
                    return Err(PostgresError::Interface(
                        "copy_to statement entered COPY FROM STDIN mode; use copy_from instead"
                            .to_string(),
                    ));
                }
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(srv) => {
                        if pre_err.is_none() {
                            pre_err = Some(PostgresError::Server(srv));
                        }
                    }
                    Err(e) => {
                        self.mark_closed();
                        return Err(e);
                    }
                },
                Message::RowDescription(_)
                | Message::DataRow(_)
                | Message::CommandComplete(_)
                | Message::EmptyQueryResponse => {
                    unexpected_non_copy = true;
                }
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.set_exchange_state(ExchangeState::Idle);
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(e) => {
                            self.mark_closed();
                            return Err(e);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    if let Some(err) = pre_err {
                        return Err(err);
                    }
                    if unexpected_non_copy {
                        return Err(PostgresError::Interface(
                            "statement passed to copy_to did not enter COPY TO STDOUT mode"
                                .to_string(),
                        ));
                    }
                    return Err(PostgresError::Protocol(
                        "received ReadyForQuery without CopyOutResponse or ErrorResponse"
                            .to_string(),
                    ));
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during start_copy_out".to_string(),
                    ));
                }
            }
        }
    }

    /// Initiate `COPY ... FROM STDIN (FORMAT binary)`, opening a COPY-owned `BEGIN` when idle (`'I'`),
    /// preserving caller ownership when already in `'T'`, and validating binary `CopyInResponse` formats.
    pub async fn start_copy_in(&mut self, sql: &str) -> Result<usize, PostgresError> {
        if self.closed {
            return Err(PostgresError::Interface(
                "cannot execute COPY FROM on a closed PostgresConnection".to_string(),
            ));
        }
        if self.exchange_state == ExchangeState::PipelineActive {
            return Err(PostgresError::UnsupportedFeature(
                "COPY FROM is not supported while a PostgreSQL pipeline scope is active"
                    .to_string(),
            ));
        }
        if self.exchange_state != ExchangeState::Idle {
            return Err(PostgresError::Interface(format!(
                "cannot execute COPY FROM while exchange_state is {:?}",
                self.exchange_state
            )));
        }
        validate_single_copy_sql(sql)?;
        if self.transaction_status == TransactionStatus::Failed {
            return Err(PostgresError::Interface(
                "cannot start COPY FROM while transaction is aborted ('E')".to_string(),
            ));
        }

        let started_owned_tx = if self.transaction_status == TransactionStatus::Idle {
            self.begin().await?;
            true
        } else {
            false
        };

        self.unnamed_statement_valid = false;
        self.copy_in_owned_tx = false;
        self.write_buf.clear();
        frontend::query(sql, &mut self.write_buf).map_err(|e| {
            PostgresError::Protocol(format!("failed to encode COPY FROM Query frame: {e}"))
        })?;
        self.set_exchange_state(ExchangeState::CopyActive);
        if let Err(err) = self.write_command_buf("start_copy_in", false).await {
            if started_owned_tx
                && !self.closed
                && self.transaction_status != TransactionStatus::Idle
            {
                let _ = self.rollback().await;
            }
            return Err(err);
        }

        let mut pre_err: Option<PostgresError> = None;
        let mut unexpected_non_copy = false;
        loop {
            let msg = match self.read_command_frame().await {
                Ok(m) => m,
                Err(err) => {
                    self.mark_closed();
                    return Err(err);
                }
            };
            match msg {
                Message::CopyInResponse(body) => {
                    let overall = body.format();
                    let mut col_formats = Vec::new();
                    let mut iter = body.column_formats();
                    while let Some(fmt) = iter.next().map_err(|e| {
                        self.mark_closed();
                        PostgresError::Protocol(format!(
                            "malformed CopyInResponse column_formats: {e}"
                        ))
                    })? {
                        col_formats.push(fmt);
                    }
                    if overall != 1 || col_formats.iter().any(|&f| f != 1) {
                        let deadline =
                            Instant::now() + self.standalone_cancel.config.connect_timeout;
                        let _ = crate::copy::abort_copy_in_on_connection(
                            self,
                            "COPY FROM requires FORMAT binary",
                            deadline,
                        )
                        .await;
                        if started_owned_tx
                            && !self.closed
                            && self.transaction_status != TransactionStatus::Idle
                        {
                            let _ = self.rollback().await;
                        }
                        return Err(PostgresError::Data(
                            "COPY FROM requires FORMAT binary (received text CopyInResponse)"
                                .to_string(),
                        ));
                    }
                    self.copy_in_owned_tx = started_owned_tx;
                    return Ok(col_formats.len());
                }
                Message::CopyOutResponse(_) => {
                    let cancel = self.postgres_cancel_handle();
                    let deadline = Instant::now() + self.standalone_cancel.config.connect_timeout;
                    let _ = cancel.cancel_until(deadline).await;
                    let _ = self.drain_to_ready_for_query_until(Some(deadline)).await;
                    self.mark_closed();
                    return Err(PostgresError::Interface(
                        "copy_from statement entered COPY TO STDOUT mode; use copy_to instead"
                            .to_string(),
                    ));
                }
                Message::ErrorResponse(body) => match crate::query::parse_error_response(&body) {
                    Ok(srv) => {
                        if pre_err.is_none() {
                            pre_err = Some(PostgresError::Server(srv));
                        }
                    }
                    Err(e) => {
                        self.mark_closed();
                        return Err(e);
                    }
                },
                Message::RowDescription(_)
                | Message::DataRow(_)
                | Message::CommandComplete(_)
                | Message::EmptyQueryResponse => {
                    unexpected_non_copy = true;
                }
                Message::NoticeResponse(_) => {}
                Message::ParameterStatus(body) => {
                    self.handle_parameter_status_frame(&body)?;
                }
                Message::NotificationResponse(body) => {
                    self.handle_notification_frame(&body)?;
                }
                Message::ReadyForQuery(body) => {
                    self.set_exchange_state(ExchangeState::Idle);
                    let status = match TransactionStatus::from_wire_tag(body.status()) {
                        Ok(s) => s,
                        Err(e) => {
                            self.mark_closed();
                            return Err(e);
                        }
                    };
                    self.set_transaction_status_from_wire(status);
                    if started_owned_tx && status != TransactionStatus::Idle {
                        let _ = self.rollback().await;
                    }
                    if let Some(err) = pre_err {
                        return Err(err);
                    }
                    if unexpected_non_copy {
                        return Err(PostgresError::Interface(
                            "statement passed to copy_from did not enter COPY FROM STDIN mode"
                                .to_string(),
                        ));
                    }
                    return Err(PostgresError::Protocol(
                        "received ReadyForQuery without CopyInResponse or ErrorResponse"
                            .to_string(),
                    ));
                }
                _ => {
                    self.mark_closed();
                    return Err(PostgresError::Protocol(
                        "unexpected backend message frame during start_copy_in".to_string(),
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
        self.mark_closed();
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

    pub(crate) fn mark_retire(&mut self) {
        if let Self::Pooled(lease) = self {
            lease.mark_retire();
        }
    }

    pub(crate) async fn retire_physical_socket(&mut self) {
        match self {
            Self::Direct(conn) => {
                let _ = conn.close().await;
            }
            Self::Pooled(lease) => {
                lease.mark_retire();
                let _ = PostgresConnection::close(lease).await;
            }
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

/// Centralized session access discriminant preventing ordinary raw escapes during active
/// pipeline scopes or failed-by-cancellation ambient transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum SessionAccess {
    Ordinary,
    Pipeline(PipelineScopeToken),
    Portal,
    RollbackAllowed,
    Cancel,
    ForcedCleanup,
}

/// Optional synchronous test hook executed on a blocking thread immediately before dialing a
/// fresh `CancelRequest` socket (used by deterministic delayed-cancel barrier tests).
pub(crate) type CancelDialHook = Arc<dyn Fn() + Send + Sync>;

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
struct ActiveOpState {
    op_gen: OperationGeneration,
    key: Option<CancelKeyData>,
    had_ambient_tx: bool,
    cancel_claimed: bool,
    retire_on_finish: bool,
    cancel_notify: Arc<tokio::sync::Notify>,
}

#[derive(Debug)]
pub(crate) struct OperationMeta {
    pub(crate) next_pipeline_token: u64,
    pub(crate) pre_pinned_owner: Option<TransactionOwner>,
    pub(crate) install_pipeline_scope: Option<ActivePipelineScope>,
    pub(crate) pipeline_already_submitted: bool,
    pub(crate) pipeline_already_failed: bool,
    pub(crate) mark_pipeline_submitted: bool,
    pub(crate) mark_pipeline_failed: Option<PostgresError>,
    pub(crate) finish_pipeline_scope: Option<ActivePipelineScope>,
    pub(crate) clear_pipeline_scope: Option<(bool, Option<TransactionOwner>)>,
    pub(crate) retire_pending: bool,
    pub(crate) ambient_cancel_failed: bool,
    pub(crate) force_retire_now: bool,
    pub(crate) cleanup_deadline: Option<Instant>,
    pub(crate) clear_ambient_cancel_failed: bool,
    pub(crate) pin_owner_on_ok: Option<TransactionOwner>,
    pub(crate) install_portal_on_ok: Option<(TransactionOwner, String)>,
    pub(crate) rereserve_op_gen_on_ok: bool,
}

#[derive(Debug)]
pub(crate) struct SessionInner {
    state: SessionState,
    next_generation: u64,
    next_op_generation: u64,
    reserved_generations: std::collections::BTreeSet<u64>,
    pre_cancelled_generations: std::collections::BTreeSet<u64>,
    active_op: Option<ActiveOpState>,
    closing_inflight: bool,
    close_after_inflight: bool,
    portal_cleanup_inflight: bool,
    copy_cleanup_inflight: bool,
    copy_cleanup_tasks: usize,
    active_copy: bool,
    parameters: BTreeMap<String, String>,
    last_transaction_status: TransactionStatus,
    pinned_owner: Option<TransactionOwner>,
    active_portal: Option<(TransactionOwner, String)>,
    next_pipeline_token: u64,
    pub(crate) active_pipeline: Option<ActivePipelineScope>,
    retire_pending: bool,
    ambient_cancel_failed: bool,
}

impl SessionInner {
    fn new_disconnected() -> Self {
        Self {
            state: SessionState::Disconnected,
            next_generation: 0,
            next_op_generation: 0,
            reserved_generations: std::collections::BTreeSet::new(),
            pre_cancelled_generations: std::collections::BTreeSet::new(),
            active_op: None,
            closing_inflight: false,
            close_after_inflight: false,
            portal_cleanup_inflight: false,
            copy_cleanup_inflight: false,
            copy_cleanup_tasks: 0,
            active_copy: false,
            parameters: BTreeMap::new(),
            last_transaction_status: TransactionStatus::Idle,
            pinned_owner: None,
            active_portal: None,
            next_pipeline_token: 0,
            active_pipeline: None,
            retire_pending: false,
            ambient_cancel_failed: false,
        }
    }

    fn new_ready(backing: SessionBacking) -> Self {
        let generation = backing.generation();
        let parameters = backing.parameters.clone();
        let status = backing.transaction_status();
        Self {
            state: SessionState::Ready(backing),
            next_generation: generation,
            next_op_generation: 0,
            reserved_generations: std::collections::BTreeSet::new(),
            pre_cancelled_generations: std::collections::BTreeSet::new(),
            active_op: None,
            closing_inflight: false,
            close_after_inflight: false,
            portal_cleanup_inflight: false,
            copy_cleanup_inflight: false,
            copy_cleanup_tasks: 0,
            active_copy: false,
            parameters,
            last_transaction_status: status,
            pinned_owner: None,
            active_portal: None,
            next_pipeline_token: 0,
            active_pipeline: None,
            retire_pending: false,
            ambient_cancel_failed: false,
        }
    }

    fn prune_generation_sets(&mut self) {
        let floor = self.next_op_generation.saturating_sub(4096);
        self.reserved_generations.retain(|&g| g >= floor);
        self.pre_cancelled_generations.retain(|&g| g >= floor);
    }

    const fn has_copy_cleanup(&self) -> bool {
        self.copy_cleanup_inflight || self.copy_cleanup_tasks > 0
    }
}

pub(crate) struct SessionShared {
    pub(crate) config: PostgresConfig,
    pub(crate) cloud_connector: Arc<CloudConnector>,
    pub(crate) inner: std::sync::Mutex<SessionInner>,
    pub(crate) cancel_dial_hook: std::sync::Mutex<Option<CancelDialHook>>,
    pub(crate) inflight_done_notify: Arc<tokio::sync::Notify>,
}

impl Drop for SessionShared {
    fn drop(&mut self) {
        let inner = match self.inner.get_mut() {
            Ok(g) => g,
            Err(e) => e.into_inner(),
        };
        let active_pipe = inner.active_pipeline.take();
        let retire_pending = inner.retire_pending || inner.ambient_cancel_failed;
        if active_pipe.is_none() && !retire_pending {
            return;
        }
        let prev = std::mem::replace(
            &mut inner.state,
            SessionState::Closed {
                generation: inner.next_generation,
            },
        );
        let SessionState::Ready(mut backing) = prev else {
            return;
        };
        let _access = SessionAccess::ForcedCleanup;
        if retire_pending {
            backing.mark_retire();
        }
        let timeout = self.config.connect_timeout;
        let cleanup = async move {
            if let Some(pipe) = active_pipe {
                let rb = tokio::time::timeout(
                    timeout,
                    self::pipeline_scope::rollback_pipeline_scope(&mut backing, &pipe.scope),
                )
                .await;
                if !matches!(rb, Ok(Ok(()))) || retire_pending {
                    backing.retire_physical_socket().await;
                }
            } else if retire_pending {
                backing.retire_physical_socket().await;
            }
            let _ = tokio::time::timeout(timeout, backing.close()).await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(cleanup);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum CloseAction {
    Immediate(Option<SessionBacking>),
    WaitInFlight(std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>),
}

struct InFlightDropGuard<'a> {
    shared: &'a Arc<SessionShared>,
    backing: Option<SessionBacking>,
    disarmed: bool,
}

impl Drop for InFlightDropGuard<'_> {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        if let Some(mut backing) = self.backing.take() {
            backing.mark_closed();
            backing.mark_retire();
            let generation = backing.generation();
            let timeout = self.shared.config.connect_timeout;
            {
                let mut guard = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
                guard.active_op = None;
                guard.closing_inflight = false;
                guard.close_after_inflight = false;
                guard.active_pipeline = None;
                guard.pinned_owner = None;
                guard.state = SessionState::Closed { generation };
            }
            self.shared.inflight_done_notify.notify_waiters();
            let cleanup = async move {
                let _ = tokio::time::timeout(timeout, backing.close()).await;
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(cleanup);
            } else {
                vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
            }
        }
    }
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

    /// Retire the checked-out physical socket boundedly by `connect_timeout` while keeping any
    /// pooled lease slot owned by the session until the client closes.
    pub(crate) async fn retire_socket_bounded(&mut self) {
        if let Some(ref mut backing) = self.backing {
            backing.mark_retire();
            let timeout = self.handle.shared.config.connect_timeout;
            let _ = tokio::time::timeout(timeout, backing.retire_physical_socket()).await;
        }
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
        let close_now = {
            let mut guard = self
                .handle
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.active_op = None;
            guard.closing_inflight = false;
            let close_requested = guard.close_after_inflight && !guard.portal_cleanup_inflight;
            if close_requested {
                guard.close_after_inflight = false;
            }
            guard.parameters = backing.parameters.clone();
            guard.last_transaction_status = backing.transaction_status();
            if backing.transaction_status() == TransactionStatus::Idle
                && guard.active_portal.is_none()
                && guard.active_pipeline.is_none()
                && !guard.ambient_cancel_failed
            {
                guard.pinned_owner = None;
            }
            if guard.retire_pending {
                backing.mark_retire();
            }
            if close_requested {
                guard.active_portal = None;
                guard.pinned_owner = None;
                guard.ambient_cancel_failed = false;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                Some(backing)
            } else {
                if matches!(guard.state, SessionState::InFlight { .. }) {
                    guard.state = SessionState::Ready(backing);
                }
                None
            }
        };
        if let Some(b) = close_now {
            let timeout = self.handle.shared.config.connect_timeout;
            let notify = Arc::clone(&self.handle.shared.inflight_done_notify);
            let task = async move {
                let _ = tokio::time::timeout(timeout, b.close()).await;
                notify.notify_waiters();
            };
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                h.spawn(task);
            } else {
                vertebra_db::py_async::db_runtime_handle().spawn(task);
            }
        } else if !self
            .handle
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned")
            .portal_cleanup_inflight
        {
            self.handle.shared.inflight_done_notify.notify_waiters();
        }
    }
}

/// Owned `'static` exclusive session lease held across multi-step `CopyOut` / `CopyFromStream` lifecycles.
pub(crate) struct CopySessionLease {
    session: Arc<SessionHandle>,
    backing: Option<SessionBacking>,
    op_gen: OperationGeneration,
    cancel_notify: Arc<tokio::sync::Notify>,
    ambient_failed_on_retire: bool,
}

impl CopySessionLease {
    pub(crate) fn cancel_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.cancel_notify)
    }

    pub(crate) fn connect_timeout(&self) -> Duration {
        self.session.shared.config.connect_timeout
    }

    pub(crate) fn connection_mut(&mut self) -> &mut PostgresConnection {
        self.backing.as_mut().expect("CopySessionLease active")
    }

    pub(crate) fn cancel_claimed(&self) -> bool {
        self.session.did_op_claim_cancel(self.op_gen)
    }

    pub(crate) fn mark_ambient_failed_on_retire(&mut self) {
        self.ambient_failed_on_retire = true;
    }

    pub(crate) fn begin_drop_cleanup(&self) {
        let mut guard = self
            .session
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.copy_cleanup_inflight = true;
    }

    pub(crate) async fn finish_clean(mut self) {
        let Some(mut backing) = self.backing.take() else {
            return;
        };
        if backing.exchange_state != ExchangeState::Idle {
            backing.mark_closed();
        }
        let sole_owner = Arc::strong_count(&self.session) == 1;
        let (close_backing, drop_direct) = {
            let mut guard = self
                .session
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            let op_state = guard.active_op.take();
            let op_retire = op_state
                .as_ref()
                .is_some_and(|op| op.cancel_claimed || op.retire_on_finish);
            guard.active_copy = false;
            guard.copy_cleanup_inflight = false;
            guard.parameters = backing.parameters.clone();
            guard.last_transaction_status = backing.transaction_status();
            if backing.transaction_status() == TransactionStatus::Idle
                && guard.active_portal.is_none()
                && guard.active_pipeline.is_none()
                && !guard.ambient_cancel_failed
            {
                guard.pinned_owner = None;
            }
            if guard.retire_pending || op_retire {
                guard.retire_pending = true;
                backing.mark_retire();
            }
            let close_now = guard.close_after_inflight || sole_owner;
            guard.close_after_inflight = false;
            if close_now {
                guard.closing_inflight = true;
                guard.pinned_owner = None;
                guard.ambient_cancel_failed = false;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (Some(backing), None)
            } else if matches!(backing, SessionBacking::Direct(ref c) if c.is_closed())
                && !guard.ambient_cancel_failed
            {
                guard.closing_inflight = false;
                guard.pinned_owner = None;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (None, Some(backing))
            } else {
                guard.closing_inflight = false;
                guard.state = SessionState::Ready(backing);
                (None, None)
            }
        };
        drop(drop_direct);
        let timeout = self.session.shared.config.connect_timeout;
        if let Some(b) = close_backing {
            let _ = tokio::time::timeout(timeout, b.close()).await;
            let mut guard = self
                .session
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.closing_inflight = false;
        }
        self.session.shared.inflight_done_notify.notify_waiters();
    }

    pub(crate) async fn finish_retired(mut self) {
        let Some(mut backing) = self.backing.take() else {
            return;
        };
        backing.mark_retire();
        let timeout = self.session.shared.config.connect_timeout;
        let _ = tokio::time::timeout(timeout, backing.retire_physical_socket()).await;
        let sole_owner = Arc::strong_count(&self.session) == 1;
        let (close_backing, drop_direct) = {
            let mut guard = self
                .session
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.active_op = None;
            guard.active_copy = false;
            guard.copy_cleanup_inflight = false;
            guard.retire_pending = true;
            guard.parameters = backing.parameters.clone();
            guard.last_transaction_status = backing.transaction_status();
            if self.ambient_failed_on_retire {
                guard.ambient_cancel_failed = true;
            }
            let close_now = guard.close_after_inflight || sole_owner;
            guard.close_after_inflight = false;
            if close_now {
                guard.closing_inflight = true;
                guard.pinned_owner = None;
                guard.ambient_cancel_failed = false;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (Some(backing), None)
            } else if matches!(backing, SessionBacking::Direct(_)) && !guard.ambient_cancel_failed {
                guard.closing_inflight = false;
                guard.pinned_owner = None;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (None, Some(backing))
            } else {
                guard.closing_inflight = false;
                guard.state = SessionState::Ready(backing);
                (None, None)
            }
        };
        drop(drop_direct);
        if let Some(b) = close_backing {
            let _ = tokio::time::timeout(timeout, b.close()).await;
            let mut guard = self
                .session
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.closing_inflight = false;
        }
        self.session.shared.inflight_done_notify.notify_waiters();
    }

    pub(crate) async fn cancel_drain_and_retire(mut self) {
        let deadline = Instant::now() + self.connect_timeout();
        let cancel_handle = self.session.fenced_cancel_handle(self.op_gen);
        let _ = cancel_handle.cancel_until(deadline).await;
        if let Some(ref mut backing) = self.backing
            && !backing.is_closed()
            && Instant::now() < deadline
        {
            let _ = backing.drain_to_ready_for_query_until(Some(deadline)).await;
        }
        self.finish_retired().await;
    }

    pub(crate) fn spawn_cancel_and_retire_on_drop(self) {
        self.begin_drop_cleanup();
        let task = async move {
            self.cancel_drain_and_retire().await;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(task);
        } else {
            vertebra_db::py_async::db_runtime_handle().spawn(task);
        }
    }
}

impl Drop for CopySessionLease {
    fn drop(&mut self) {
        let Some(mut backing) = self.backing.take() else {
            return;
        };
        backing.mark_closed();
        backing.mark_retire();
        let sole_owner = Arc::strong_count(&self.session) == 1;
        let timeout = self.session.shared.config.connect_timeout;
        let (close_backing, drop_direct) = {
            let mut guard = self
                .session
                .shared
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.active_op = None;
            guard.active_copy = false;
            guard.copy_cleanup_inflight = false;
            guard.retire_pending = true;
            guard.parameters = backing.parameters.clone();
            guard.last_transaction_status = backing.transaction_status();
            if self.ambient_failed_on_retire {
                guard.ambient_cancel_failed = true;
            }
            let close_now = guard.close_after_inflight || sole_owner;
            guard.close_after_inflight = false;
            if close_now {
                guard.closing_inflight = true;
                guard.pinned_owner = None;
                guard.ambient_cancel_failed = false;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (Some(backing), None)
            } else if matches!(backing, SessionBacking::Direct(_)) && !guard.ambient_cancel_failed {
                guard.closing_inflight = false;
                guard.pinned_owner = None;
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                (None, Some(backing))
            } else {
                guard.closing_inflight = false;
                guard.state = SessionState::Ready(backing);
                (None, None)
            }
        };
        drop(drop_direct);
        if let Some(b) = close_backing {
            let shared = Arc::clone(&self.session.shared);
            let cleanup = async move {
                let _ = tokio::time::timeout(timeout, b.close()).await;
                {
                    let mut guard = shared.inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.closing_inflight = false;
                }
                shared.inflight_done_notify.notify_waiters();
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(cleanup);
            } else {
                vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
            }
        } else {
            self.session.shared.inflight_done_notify.notify_waiters();
        }
    }
}

/// Generation-fenced RAII tracker keeping [`SessionHandle`] cleanup/close barriers engaged while a
/// non-aborted background COPY close/abort/cancelled step finishes draining or retiring its lease.
pub(crate) struct CopyTaskCleanupGuard {
    session: Arc<SessionHandle>,
    armed: bool,
}

impl CopyTaskCleanupGuard {
    pub(crate) async fn finish(mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.session.finish_copy_task_cleanup_async().await;
    }
}

impl Drop for CopyTaskCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.session.finish_copy_task_cleanup_sync();
    }
}

/// Explicit PostgreSQL session lifecycle handle (`Disconnected -> Connecting -> Ready -> Closed`).
pub struct SessionHandle {
    pub(crate) shared: Arc<SessionShared>,
    token_provider: ScopedTokenProvider,
    clock: Arc<dyn Clock>,
}

/// Type alias uniting [`ClientSession`] and [`SessionHandle`].
pub type ClientSession = SessionHandle;

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("config", &self.shared.config)
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
        let shared = Arc::new(SessionShared {
            config,
            cloud_connector,
            inner: std::sync::Mutex::new(SessionInner::new_disconnected()),
            cancel_dial_hook: std::sync::Mutex::new(None),
            inflight_done_notify: Arc::new(tokio::sync::Notify::new()),
        });
        Self {
            shared,
            token_provider,
            clock,
        }
    }

    /// Construct a ready [`SessionHandle`] directly wrapping an established [`PostgresConnection`] (used by deterministic protocol wire harnesses).
    pub fn from_direct(config: PostgresConfig, conn: PostgresConnection) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let cloud_connector = Arc::new(CloudConnector::with_clock(Arc::clone(&clock)));
        let token_provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
            Arc::clone(&clock),
        );
        let shared = Arc::new(SessionShared {
            config,
            cloud_connector,
            inner: std::sync::Mutex::new(SessionInner::new_ready(SessionBacking::Direct(conn))),
            cancel_dial_hook: std::sync::Mutex::new(None),
            inflight_done_notify: Arc::new(tokio::sync::Notify::new()),
        });
        Self {
            shared,
            token_provider,
            clock,
        }
    }

    /// Construct a ready [`SessionHandle`] backed by a checked-out [`crate::pool::PooledConnection`] lease.
    pub fn from_pooled(lease: crate::pool::PooledConnection) -> Self {
        let config = lease.pool().config().clone();
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let cloud_connector = Arc::clone(lease.pool().cloud_connector());
        let token_provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
            Arc::clone(&clock),
        );
        let shared = Arc::new(SessionShared {
            config,
            cloud_connector,
            inner: std::sync::Mutex::new(SessionInner::new_ready(SessionBacking::Pooled(lease))),
            cancel_dial_hook: std::sync::Mutex::new(None),
            inflight_done_notify: Arc::new(tokio::sync::Notify::new()),
        });
        Self {
            shared,
            token_provider,
            clock,
        }
    }

    /// Install an optional pre-dial barrier/hook invoked before opening any fresh `CancelRequest` socket on this session.
    pub fn set_cancel_dial_hook(&self, hook: Option<Arc<dyn Fn() + Send + Sync>>) {
        let mut guard = self
            .shared
            .cancel_dial_hook
            .lock()
            .expect("cancel_dial_hook poisoned");
        *guard = hook;
    }

    /// Borrow the validated [`PostgresConfig`].
    pub fn config(&self) -> &PostgresConfig {
        &self.shared.config
    }

    /// Return `true` when this session has claimed a `CancelRequest` and is scheduled to retire its physical socket.
    pub fn will_retire(&self) -> bool {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.retire_pending
    }

    /// Return an out-of-band generation-fenced [`PostgresCancelHandle`] targeting this session's active operation.
    pub fn postgres_cancel_handle(&self) -> PostgresCancelHandle {
        PostgresCancelHandle::from_session(Arc::clone(&self.shared), None)
    }

    /// Return a [`PostgresCancelHandle`] fenced to one specific [`OperationGeneration`].
    pub fn fenced_cancel_handle(&self, op_gen: OperationGeneration) -> PostgresCancelHandle {
        PostgresCancelHandle::from_session(Arc::clone(&self.shared), Some(op_gen))
    }

    /// Cancel the currently active operation (if any) out-of-band without acquiring the session execution lock.
    pub async fn cancel(&self) -> CancelDisposition {
        self.postgres_cancel_handle()
            .cancel_with_disposition()
            .await
    }

    /// Reserve a monotonic [`OperationGeneration`] before scheduling or running a session operation.
    pub fn reserve_operation_generation(&self) -> OperationGeneration {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.next_op_generation = guard.next_op_generation.saturating_add(1);
        let id = guard.next_op_generation;
        guard.prune_generation_sets();
        guard.reserved_generations.insert(id);
        OperationGeneration(id)
    }

    pub(crate) fn unreserve_operation_generation(&self, op_gen: OperationGeneration) {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.reserved_generations.remove(&op_gen.0);
        guard.pre_cancelled_generations.remove(&op_gen.0);
    }

    pub(crate) fn claim_cancel_on_shared(
        shared: &Arc<SessionShared>,
        fenced_generation: Option<OperationGeneration>,
    ) -> self::cancel::SessionCancelClaim {
        let hook = shared
            .cancel_dial_hook
            .lock()
            .expect("cancel_dial_hook poisoned")
            .clone();
        let (key_opt, notify) = {
            let mut guard = shared.inner.lock().expect("SessionHandle lock poisoned");
            if let Some(target) = fenced_generation {
                if guard.reserved_generations.remove(&target.0) {
                    guard.pre_cancelled_generations.insert(target.0);
                    return self::cancel::SessionCancelClaim::CancelledBeforeSend;
                }
                if guard.pre_cancelled_generations.contains(&target.0) {
                    return self::cancel::SessionCancelClaim::IdleNoOp;
                }
            }
            let Some(ref mut active) = guard.active_op else {
                return self::cancel::SessionCancelClaim::IdleNoOp;
            };
            if let Some(target) = fenced_generation
                && active.op_gen != target
            {
                return self::cancel::SessionCancelClaim::IdleNoOp;
            }
            if active.cancel_claimed {
                return self::cancel::SessionCancelClaim::IdleNoOp;
            }
            active.cancel_claimed = true;
            active.retire_on_finish = true;
            let had_ambient = active.had_ambient_tx;
            let key_opt = active.key.clone();
            let notify = Arc::clone(&active.cancel_notify);
            guard.retire_pending = true;
            if had_ambient {
                guard.ambient_cancel_failed = true;
            }
            if let Some(ref mut pipe) = guard.active_pipeline {
                pipe.failed = true;
            }
            (key_opt, notify)
        };
        notify.notify_one();
        match key_opt {
            Some(key) => self::cancel::SessionCancelClaim::Send { key, hook },
            None => self::cancel::SessionCancelClaim::MissingBackendKey,
        }
    }

    pub(crate) fn claim_deadline_cancel_on_shared(
        shared: &Arc<SessionShared>,
        op_gen: OperationGeneration,
        wire_started: bool,
    ) -> self::cancel::SessionCancelClaim {
        let hook = shared
            .cancel_dial_hook
            .lock()
            .expect("cancel_dial_hook poisoned")
            .clone();
        let mut guard = shared.inner.lock().expect("SessionHandle lock poisoned");
        let Some(ref mut active) = guard.active_op else {
            return self::cancel::SessionCancelClaim::IdleNoOp;
        };
        if active.op_gen != op_gen || active.cancel_claimed {
            return self::cancel::SessionCancelClaim::IdleNoOp;
        }
        active.cancel_claimed = true;
        if !wire_started {
            return self::cancel::SessionCancelClaim::CancelledBeforeSend;
        }
        active.retire_on_finish = true;
        let had_ambient = active.had_ambient_tx;
        let key_opt = active.key.clone();
        guard.retire_pending = true;
        if had_ambient {
            guard.ambient_cancel_failed = true;
        }
        match key_opt {
            Some(key) => self::cancel::SessionCancelClaim::Send { key, hook },
            None => self::cancel::SessionCancelClaim::MissingBackendKey,
        }
    }

    pub(crate) fn did_op_claim_cancel(&self, op_gen: OperationGeneration) -> bool {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard
            .active_op
            .as_ref()
            .is_some_and(|op| op.op_gen == op_gen && op.cancel_claimed)
    }

    fn cleanup_wait_budget(&self) -> Duration {
        let base = self.shared.config.connect_timeout;
        base.saturating_mul(2)
            .max(base.saturating_add(Duration::from_millis(250)))
    }

    pub(crate) fn begin_copy_task_cleanup(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
    ) -> Option<CopyTaskCleanupGuard> {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        let matches_active_copy = guard.active_copy
            && guard
                .active_op
                .as_ref()
                .is_some_and(|op| op.op_gen == op_gen);
        if !matches_active_copy {
            return None;
        }
        guard.copy_cleanup_tasks = guard.copy_cleanup_tasks.saturating_add(1);
        Some(CopyTaskCleanupGuard {
            session: Arc::clone(self),
            armed: true,
        })
    }

    fn take_close_backing_on_last_copy_cleanup(&self) -> (bool, Option<SessionBacking>) {
        let mut guard = self.shared.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.copy_cleanup_tasks = guard.copy_cleanup_tasks.saturating_sub(1);
        if guard.has_copy_cleanup() {
            return (false, None);
        }
        if guard.close_after_inflight
            && !guard.active_copy
            && !guard.portal_cleanup_inflight
            && !guard.closing_inflight
            && matches!(guard.state, SessionState::Ready(_))
        {
            guard.close_after_inflight = false;
            guard.closing_inflight = true;
            guard.pinned_owner = None;
            guard.ambient_cancel_failed = false;
            let next_gen = guard.next_generation;
            let prev = std::mem::replace(
                &mut guard.state,
                SessionState::Closed {
                    generation: next_gen,
                },
            );
            if let SessionState::Ready(mut backing) = prev {
                if guard.retire_pending {
                    backing.mark_retire();
                }
                guard.state = SessionState::Closed {
                    generation: backing.generation(),
                };
                return (true, Some(backing));
            }
        }
        (true, None)
    }

    async fn finish_copy_task_cleanup_async(&self) {
        let (last_done, close_backing) = self.take_close_backing_on_last_copy_cleanup();
        if let Some(b) = close_backing {
            let _ = tokio::time::timeout(self.shared.config.connect_timeout, b.close()).await;
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.closing_inflight = false;
        }
        if last_done {
            self.shared.inflight_done_notify.notify_waiters();
        }
    }

    fn finish_copy_task_cleanup_sync(&self) {
        let (last_done, close_backing) = self.take_close_backing_on_last_copy_cleanup();
        if let Some(b) = close_backing {
            let timeout = self.shared.config.connect_timeout;
            let shared = Arc::clone(&self.shared);
            let cleanup = async move {
                let _ = tokio::time::timeout(timeout, b.close()).await;
                {
                    let mut guard = shared.inner.lock().unwrap_or_else(|e| e.into_inner());
                    guard.closing_inflight = false;
                }
                shared.inflight_done_notify.notify_waiters();
            };
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(cleanup);
            } else {
                vertebra_db::py_async::db_runtime_handle().spawn(cleanup);
            }
        } else if last_done {
            self.shared.inflight_done_notify.notify_waiters();
        }
    }

    async fn wait_portal_drop_cleanup_if_needed(&self) {
        let deadline = Instant::now() + self.cleanup_wait_budget();
        loop {
            if Instant::now() >= deadline {
                break;
            }
            let wait_fut = {
                let notify_arc = Arc::clone(&self.shared.inflight_done_notify);
                let guard = self
                    .shared
                    .inner
                    .lock()
                    .expect("SessionHandle lock poisoned");
                if !guard.portal_cleanup_inflight
                    && !guard.has_copy_cleanup()
                    && !guard.closing_inflight
                {
                    None
                } else {
                    let notified = notify_arc.notified_owned();
                    let mut pinned = Box::pin(notified);
                    pinned.as_mut().enable();
                    Some(pinned)
                }
            };
            let Some(pinned) = wait_fut else {
                break;
            };
            if tokio::time::timeout_at(deadline, pinned).await.is_err() {
                break;
            }
        }
    }

    pub(crate) async fn wait_close_inflight(
        &self,
        mut pending: std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>,
    ) {
        let deadline = Instant::now() + self.cleanup_wait_budget();
        loop {
            if Instant::now() >= deadline {
                break;
            }
            if tokio::time::timeout_at(deadline, &mut pending)
                .await
                .is_err()
            {
                break;
            }
            let next_step = {
                let notify_arc = Arc::clone(&self.shared.inflight_done_notify);
                let mut guard = self
                    .shared
                    .inner
                    .lock()
                    .expect("SessionHandle lock poisoned");
                let still_busy = guard.portal_cleanup_inflight
                    || guard.has_copy_cleanup()
                    || guard.closing_inflight
                    || matches!(guard.state, SessionState::InFlight { .. });
                if still_busy {
                    let notified = notify_arc.notified_owned();
                    let mut pinned = Box::pin(notified);
                    pinned.as_mut().enable();
                    Ok(pinned)
                } else if matches!(guard.state, SessionState::Ready(_)) {
                    guard.close_after_inflight = false;
                    guard.closing_inflight = true;
                    guard.pinned_owner = None;
                    guard.ambient_cancel_failed = false;
                    let next_gen = guard.next_generation;
                    let prev = std::mem::replace(
                        &mut guard.state,
                        SessionState::Closed {
                            generation: next_gen,
                        },
                    );
                    if let SessionState::Ready(mut backing) = prev {
                        if guard.retire_pending {
                            backing.mark_retire();
                        }
                        guard.state = SessionState::Closed {
                            generation: backing.generation(),
                        };
                        Err(Some(backing))
                    } else {
                        guard.closing_inflight = false;
                        Err(None)
                    }
                } else {
                    guard.close_after_inflight = false;
                    Err(None)
                }
            };
            match next_step {
                Ok(next_pinned) => {
                    pending = next_pinned;
                }
                Err(Some(backing)) => {
                    let _ =
                        tokio::time::timeout(self.shared.config.connect_timeout, backing.close())
                            .await;
                    {
                        let mut guard = self
                            .shared
                            .inner
                            .lock()
                            .expect("SessionHandle lock poisoned");
                        guard.closing_inflight = false;
                    }
                    self.shared.inflight_done_notify.notify_waiters();
                    break;
                }
                Err(None) => break,
            }
        }
    }

    /// Synchronously validate that ordinary operation reservation can proceed without an active
    /// portal or pipeline scope (used to reject invalid arguments before starting reservation state).
    pub fn ensure_ordinary_reservable(&self) -> Result<(), PostgresError> {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if guard.active_portal.is_some() && !guard.portal_cleanup_inflight {
            return Err(PostgresError::Interface(
                "cannot execute operation while a Portal is active on this session".to_string(),
            ));
        }
        if guard.active_copy && !guard.has_copy_cleanup() {
            return Err(PostgresError::Interface(
                "cannot execute operation while a COPY stream is active on this session"
                    .to_string(),
            ));
        }
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute ordinary session operation while a pipeline scope is active"
                    .to_string(),
            ));
        }
        Ok(())
    }

    /// Synchronously claim `op_gen` inside a Python `asyncio.Future` cancellation callback and spawn
    /// a fresh-socket `CancelRequest` only when `op_gen` is currently in flight on the wire.
    pub fn trigger_async_cancel(&self, op_gen: OperationGeneration) {
        if let self::cancel::SessionCancelClaim::Send { key, hook } =
            Self::claim_cancel_on_shared(&self.shared, Some(op_gen))
        {
            let config = self.shared.config.clone();
            let connector = Arc::clone(&self.shared.cloud_connector);
            let deadline = Instant::now() + config.connect_timeout;
            vertebra_db::py_async::db_runtime_handle().spawn(async move {
                let _ = self::cancel::send_cancel_request_until(
                    &config, &connector, &key, deadline, hook,
                )
                .await;
            });
        }
    }

    /// Return `true` only when an authenticated [`PostgresConnection`] has been published.
    pub fn is_ready(&self) -> bool {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        matches!(
            guard.state,
            SessionState::Ready(ref b) if !b.is_closed()
        ) || (matches!(guard.state, SessionState::InFlight { .. }) && !guard.close_after_inflight)
    }

    /// Return `true` when the session is not currently active (`Disconnected`, `Connecting`, or `Closed`), matching Python `client.closed`.
    pub fn is_client_closed(&self) -> bool {
        !self.is_ready()
    }

    /// Return `true` when the session is in the terminal or post-failure `Closed` state.
    pub fn is_closed(&self) -> bool {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Closed { .. } => true,
            SessionState::Ready(b) => b.is_closed(),
            SessionState::InFlight { .. } => guard.close_after_inflight,
            _ => false,
        }
    }

    /// Return the current or most recent connection generation (`0` before the first connect attempt).
    pub fn generation(&self) -> u64 {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
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
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Ready(conn) => conn.transaction_status(),
            _ => guard.last_transaction_status,
        }
    }

    /// Return a cloned snapshot of retained `ParameterStatus` entries.
    pub fn parameters(&self) -> BTreeMap<String, String> {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        match &guard.state {
            SessionState::Ready(conn) => conn.parameters.clone(),
            _ => guard.parameters.clone(),
        }
    }

    /// Mutably borrow the established [`PostgresConnection`] if `Ready` and uniquely owned.
    pub fn connection_mut(&mut self) -> Option<&mut PostgresConnection> {
        let shared = Arc::get_mut(&mut self.shared)?;
        let inner = shared.inner.get_mut().expect("SessionHandle lock poisoned");
        if inner.active_pipeline.is_some() || inner.active_portal.is_some() || inner.active_copy {
            return None;
        }
        match &mut inner.state {
            SessionState::Ready(backing) => Some(&mut **backing),
            _ => None,
        }
    }

    /// Begin a new connection attempt, returning a drop-safe [`ConnectAttempt`].
    pub fn begin_connect(&mut self) -> Result<ConnectAttempt<'_>, PostgresError> {
        let generation = {
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            if matches!(
                guard.state,
                SessionState::Ready(_)
                    | SessionState::InFlight { .. }
                    | SessionState::Connecting { .. }
            ) {
                return Err(PostgresError::Interface(
                    "cannot call connect on an already connected or connecting SessionHandle"
                        .to_string(),
                ));
            }
            guard.next_generation = guard.next_generation.saturating_add(1);
            let generation = guard.next_generation;
            guard.state = SessionState::Connecting { generation };
            guard.retire_pending = false;
            guard.ambient_cancel_failed = false;
            guard.active_pipeline = None;
            generation
        };
        let deadline = Instant::now() + self.shared.config.connect_timeout;
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
        self.close_shared().await
    }

    /// Close the session on a shared handle, awaiting bounded in-flight drain/retirement when a cancelled task is cleaning up.
    pub async fn close_shared(&self) -> Result<(), PostgresError> {
        match self.begin_close_action()? {
            CloseAction::Immediate(Some(backing)) => backing.close().await,
            CloseAction::Immediate(None) => Ok(()),
            CloseAction::WaitInFlight(notified) => {
                self.wait_close_inflight(notified).await;
                Ok(())
            }
        }
    }

    /// Reserve a startup transition (`Disconnected -> Connecting`) on an `Arc<SessionHandle>`, return `AlreadyReady` if `Ready`, or reject `Connecting`/`Closed`.
    pub fn begin_client_connect(self: &Arc<Self>) -> Result<ClientBeginConnect, PostgresError> {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        match guard.state {
            SessionState::Disconnected => {
                guard.next_generation = guard.next_generation.saturating_add(1);
                let generation = guard.next_generation;
                guard.state = SessionState::Connecting { generation };
                let deadline = Instant::now() + self.shared.config.connect_timeout;
                Ok(ClientBeginConnect::Attempt(ClientConnectAttempt {
                    session: Some(Arc::clone(self)),
                    generation,
                    deadline,
                }))
            }
            SessionState::Ready(ref b) if !b.is_closed() => Ok(ClientBeginConnect::AlreadyReady),
            SessionState::InFlight { .. } if !guard.close_after_inflight => {
                Ok(ClientBeginConnect::AlreadyReady)
            }
            SessionState::Connecting { .. } => Err(PostgresError::Interface(
                "connection attempt is already in progress".to_string(),
            )),
            SessionState::Ready(_)
            | SessionState::InFlight { .. }
            | SessionState::Closed { .. } => Err(PostgresError::Interface(
                "cannot connect a closed client".to_string(),
            )),
        }
    }

    pub(crate) fn begin_close_action(&self) -> Result<CloseAction, PostgresError> {
        let notify_arc = Arc::clone(&self.shared.inflight_done_notify);
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if guard.portal_cleanup_inflight || guard.has_copy_cleanup() || guard.closing_inflight {
            guard.close_after_inflight = true;
            let notified = notify_arc.notified_owned();
            let mut pinned = Box::pin(notified);
            pinned.as_mut().enable();
            return Ok(CloseAction::WaitInFlight(pinned));
        }
        if guard.active_copy && !guard.retire_pending {
            return Err(PostgresError::Interface(
                "cannot close session while a COPY stream is active".to_string(),
            ));
        }
        if guard.active_portal.is_some() && !guard.retire_pending {
            return Err(PostgresError::Interface(
                "cannot close session while a Portal is active".to_string(),
            ));
        }
        if guard.active_pipeline.is_some() && !guard.retire_pending {
            return Err(PostgresError::Interface(
                "cannot close session while a pipeline scope is active".to_string(),
            ));
        }
        match guard.state {
            SessionState::Connecting { .. } => Err(PostgresError::Interface(
                "cannot close client while an operation or connect is in progress".to_string(),
            )),
            SessionState::InFlight { .. } => {
                let cancelling = guard.retire_pending
                    || guard.closing_inflight
                    || guard.active_op.as_ref().is_some_and(|op| op.cancel_claimed);
                if !cancelling {
                    return Err(PostgresError::Interface(
                        "cannot close client while an operation or connect is in progress"
                            .to_string(),
                    ));
                }
                guard.close_after_inflight = true;
                let notified = notify_arc.notified_owned();
                let mut pinned = Box::pin(notified);
                pinned.as_mut().enable();
                Ok(CloseAction::WaitInFlight(pinned))
            }
            SessionState::Disconnected => {
                let generation = guard.next_generation;
                guard.state = SessionState::Closed { generation };
                Ok(CloseAction::Immediate(None))
            }
            SessionState::Closed { .. } => Ok(CloseAction::Immediate(None)),
            SessionState::Ready(_) => {
                if guard.active_pipeline.is_some() {
                    return Err(PostgresError::Interface(
                        "cannot close session while a pipeline scope is active".to_string(),
                    ));
                }
                let next_gen = guard.next_generation;
                let prev = std::mem::replace(
                    &mut guard.state,
                    SessionState::Closed {
                        generation: next_gen,
                    },
                );
                guard.pinned_owner = None;
                guard.ambient_cancel_failed = false;
                if let SessionState::Ready(mut backing) = prev {
                    if guard.retire_pending {
                        backing.mark_retire();
                    }
                    guard.state = SessionState::Closed {
                        generation: backing.generation(),
                    };
                    Ok(CloseAction::Immediate(Some(backing)))
                } else {
                    unreachable!("checked Ready")
                }
            }
        }
    }

    /// Synchronously transition to `Closed` and extract any live [`SessionBacking`] for shutdown or pool return.
    pub fn begin_close(&self) -> Result<Option<SessionBacking>, PostgresError> {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if guard.active_portal.is_some() {
            return Err(PostgresError::Interface(
                "cannot close session while a Portal is active".to_string(),
            ));
        }
        if guard.active_copy {
            return Err(PostgresError::Interface(
                "cannot close session while a COPY stream is active".to_string(),
            ));
        }
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::Interface(
                "cannot close session while a pipeline scope is active".to_string(),
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
                guard.ambient_cancel_failed = false;
                if let SessionState::Ready(mut backing) = prev {
                    if guard.retire_pending {
                        backing.mark_retire();
                    }
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

    fn validate_pinned_owner(
        guard: &SessionInner,
        owner: Option<TransactionOwner>,
    ) -> Result<(), PostgresError> {
        if let Some(pinned) = guard.pinned_owner {
            match owner {
                Some(req) if req == pinned => Ok(()),
                Some(_) => Err(PostgresError::Interface(
                    "session is pinned by another TransactionOwner".to_string(),
                )),
                None if pinned.id != 0 => Err(PostgresError::Interface(
                    "session is pinned by an explicit TransactionOwner".to_string(),
                )),
                None => Ok(()),
            }
        } else if let Some(req) = owner
            && let SessionState::Ready(ref b) = guard.state
            && req.generation != b.generation()
        {
            Err(PostgresError::Interface(format!(
                "TransactionOwner generation {} does not match session generation {}",
                req.generation,
                b.generation()
            )))
        } else {
            Ok(())
        }
    }

    pub(crate) async fn run_cancellable_operation<T, F>(
        &self,
        op_gen: OperationGeneration,
        access: SessionAccess,
        owner: Option<TransactionOwner>,
        f: F,
    ) -> Result<T, PostgresError>
    where
        F: for<'c> FnOnce(
            &'c mut PostgresConnection,
            &'c mut OperationMeta,
        )
            -> vertebra_db::connection::BoxFuture<'c, Result<T, PostgresError>>,
    {
        self.wait_portal_drop_cleanup_if_needed().await;
        let (backing, mut meta, cancel_notify) = {
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.reserved_generations.remove(&op_gen.0);
            if guard.pre_cancelled_generations.remove(&op_gen.0) {
                return Err(PostgresError::Interface(
                    "operation was cancelled before wire submission".to_string(),
                ));
            }
            if guard.active_portal.is_some() {
                return Err(PostgresError::Interface(
                    "cannot execute operation while a Portal is active on this session".to_string(),
                ));
            }
            if guard.active_copy {
                return Err(PostgresError::Interface(
                    "cannot execute operation while a COPY stream is active on this session"
                        .to_string(),
                ));
            }
            match access {
                SessionAccess::Ordinary => {
                    if guard.active_pipeline.is_some() {
                        return Err(PostgresError::Interface(
                            "cannot execute ordinary session operation while a pipeline scope is active"
                                .to_string(),
                        ));
                    }
                    if guard.ambient_cancel_failed {
                        return Err(PostgresError::Interface(
                            "ambient transaction failed due to cancellation; call rollback() first"
                                .to_string(),
                        ));
                    }
                    Self::validate_pinned_owner(&guard, owner)?;
                }
                SessionAccess::RollbackAllowed => {
                    if guard.active_pipeline.is_some() {
                        return Err(PostgresError::Interface(
                            "cannot execute raw rollback while a pipeline scope is active"
                                .to_string(),
                        ));
                    }
                    Self::validate_pinned_owner(&guard, owner)?;
                }
                SessionAccess::Pipeline(token) => {
                    let Some(ref active) = guard.active_pipeline else {
                        return Err(PostgresError::Interface(
                            "no active pipeline scope on this session".to_string(),
                        ));
                    };
                    if active.token != token {
                        return Err(PostgresError::Interface(format!(
                            "pipeline scope token {} does not match active scope {}",
                            token.0, active.token.0
                        )));
                    }
                }
                SessionAccess::Portal | SessionAccess::Cancel | SessionAccess::ForcedCleanup => {
                    return Err(PostgresError::Interface(
                        "invalid direct SessionAccess entry".to_string(),
                    ));
                }
            }

            let allow_closed_cleanup = matches!(access, SessionAccess::Pipeline(_))
                || (matches!(access, SessionAccess::RollbackAllowed)
                    && (guard.ambient_cancel_failed || guard.retire_pending));

            match guard.state {
                SessionState::Ready(_) => {
                    let next_gen = guard.next_generation;
                    let prev = std::mem::replace(
                        &mut guard.state,
                        SessionState::InFlight {
                            generation: next_gen,
                        },
                    );
                    let SessionState::Ready(backing) = prev else {
                        unreachable!("checked Ready")
                    };
                    if backing.is_closed() && !allow_closed_cleanup {
                        if matches!(backing, SessionBacking::Pooled(_)) {
                            guard.state = SessionState::Ready(backing);
                        } else {
                            guard.state = SessionState::Closed {
                                generation: backing.generation(),
                            };
                        }
                        return Err(PostgresError::Interface(
                            "session connection is closed".to_string(),
                        ));
                    }
                    let conn_gen = backing.generation();
                    let had_ambient_tx = if let Some(ref pipe) = guard.active_pipeline {
                        pipe.had_ambient_tx
                    } else if matches!(access, SessionAccess::RollbackAllowed) {
                        false
                    } else {
                        backing.transaction_status() != TransactionStatus::Idle
                    };
                    let meta = OperationMeta {
                        next_pipeline_token: guard.next_pipeline_token.saturating_add(1),
                        pre_pinned_owner: guard.pinned_owner,
                        install_pipeline_scope: None,
                        pipeline_already_submitted: guard
                            .active_pipeline
                            .as_ref()
                            .is_some_and(|p| p.submitted),
                        pipeline_already_failed: guard
                            .active_pipeline
                            .as_ref()
                            .is_some_and(|p| p.failed),
                        mark_pipeline_submitted: false,
                        mark_pipeline_failed: None,
                        finish_pipeline_scope: if matches!(access, SessionAccess::Pipeline(_)) {
                            guard.active_pipeline.clone()
                        } else {
                            None
                        },
                        clear_pipeline_scope: None,
                        retire_pending: guard.retire_pending,
                        ambient_cancel_failed: guard.ambient_cancel_failed,
                        force_retire_now: false,
                        cleanup_deadline: None,
                        clear_ambient_cancel_failed: matches!(
                            access,
                            SessionAccess::RollbackAllowed
                        ),
                        pin_owner_on_ok: None,
                        install_portal_on_ok: None,
                        rereserve_op_gen_on_ok: false,
                    };
                    let cancel_notify = Arc::new(tokio::sync::Notify::new());
                    guard.active_op = Some(ActiveOpState {
                        op_gen,
                        key: backing.cancel_key.clone(),
                        had_ambient_tx,
                        cancel_claimed: false,
                        retire_on_finish: guard.retire_pending,
                        cancel_notify: Arc::clone(&cancel_notify),
                    });
                    guard.state = SessionState::InFlight {
                        generation: conn_gen,
                    };
                    (backing, meta, cancel_notify)
                }
                SessionState::InFlight { .. } => {
                    return Err(PostgresError::Interface(
                        "another operation is already in flight on this session".to_string(),
                    ));
                }
                SessionState::Connecting { .. } => {
                    return Err(PostgresError::Interface(
                        "session connection is still in progress".to_string(),
                    ));
                }
                SessionState::Disconnected | SessionState::Closed { .. } => {
                    return Err(PostgresError::Interface(
                        "session is not connected".to_string(),
                    ));
                }
            }
        };

        let mut drop_guard = InFlightDropGuard {
            shared: &self.shared,
            backing: Some(backing),
            disarmed: false,
        };
        let connect_timeout = self.shared.config.connect_timeout;

        let timed_res = {
            let conn: &mut PostgresConnection = drop_guard
                .backing
                .as_mut()
                .expect("InFlightDropGuard holds backing");
            let mut op_fut = f(conn, &mut meta);
            let cancel_drain_timeout = async {
                cancel_notify.notified().await;
                tokio::time::sleep(connect_timeout).await;
            };
            tokio::pin!(cancel_drain_timeout);
            tokio::select! {
                biased;
                res = &mut op_fut => Ok(res),
                () = &mut cancel_drain_timeout => Err(()),
            }
        };

        drop_guard.disarmed = true;
        let mut backing = drop_guard
            .backing
            .take()
            .expect("InFlightDropGuard disarmed with backing");

        let mut result = match timed_res {
            Ok(res) => res,
            Err(()) => {
                backing.mark_closed();
                Err(PostgresError::Operational(
                    "cancelled PostgreSQL operation timed out while draining wire response"
                        .to_string(),
                ))
            }
        };

        if backing.exchange_state != ExchangeState::Idle {
            backing.mark_closed();
        }

        let (retire_socket_now, close_after_first) = {
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            let op_state = guard.active_op.take();
            let op_cancel_claimed = op_state.as_ref().is_some_and(|op| op.cancel_claimed);
            let op_retire = op_state.as_ref().is_some_and(|op| op.retire_on_finish);

            guard.parameters = backing.parameters.clone();
            guard.last_transaction_status = backing.transaction_status();

            if meta.clear_ambient_cancel_failed {
                guard.ambient_cancel_failed = false;
            }

            if let Some((_had_ambient, prev_owner)) = meta.clear_pipeline_scope.take() {
                guard.active_pipeline = None;
                if backing.transaction_status() == TransactionStatus::Idle
                    && !guard.ambient_cancel_failed
                {
                    guard.pinned_owner = None;
                } else {
                    guard.pinned_owner = prev_owner;
                }
            } else if result.is_ok()
                && let Some(mut installed) = meta.install_pipeline_scope.take()
            {
                if op_cancel_claimed || guard.retire_pending {
                    installed.failed = true;
                }
                guard.next_pipeline_token = installed.token.0;
                guard.pinned_owner = Some(installed.scope.owner);
                guard.active_pipeline = Some(installed);
            } else if let Some(ref mut active) = guard.active_pipeline {
                if meta.mark_pipeline_submitted {
                    active.submitted = true;
                }
                if let Some(err) = meta.mark_pipeline_failed.take() {
                    active.failed = true;
                    if active.primary_error.is_none() {
                        active.primary_error = Some(err);
                    }
                }
                if op_cancel_claimed {
                    active.failed = true;
                }
            }

            if result.is_ok()
                && let Some((portal_owner, portal_name)) = meta.install_portal_on_ok.take()
            {
                if !op_cancel_claimed && !guard.retire_pending {
                    guard.active_portal = Some((portal_owner, portal_name));
                    if guard.pinned_owner.is_none() {
                        guard.pinned_owner = Some(portal_owner);
                    }
                }
            } else if result.is_ok()
                && let Some(pin) = meta.pin_owner_on_ok
            {
                guard.pinned_owner = Some(pin);
            } else if backing.transaction_status() == TransactionStatus::Idle
                && guard.active_portal.is_none()
                && guard.active_pipeline.is_none()
                && !guard.ambient_cancel_failed
            {
                guard.pinned_owner = None;
            }

            let retire_requested = guard.retire_pending || op_retire || meta.force_retire_now;
            if retire_requested {
                guard.retire_pending = true;
                backing.mark_retire();
            }
            if meta.rereserve_op_gen_on_ok && result.is_ok() {
                if op_cancel_claimed
                    || retire_requested
                    || guard.close_after_inflight
                    || backing.is_closed()
                {
                    result = Err(PostgresError::Operational(
                        "COPY FROM Arrow was cancelled during target catalog discovery".to_string(),
                    ));
                } else {
                    guard.reserved_generations.insert(op_gen.0);
                }
            }

            let still_in_pipeline = guard.active_pipeline.is_some();
            let still_ambient_failed = guard.ambient_cancel_failed;
            let retire_socket_now = retire_requested
                && !still_in_pipeline
                && (!still_ambient_failed
                    || backing.is_closed()
                    || meta.cleanup_deadline.is_some());

            if retire_socket_now || guard.close_after_inflight {
                guard.closing_inflight = true;
            }
            (retire_socket_now, guard.close_after_inflight)
        };

        if retire_socket_now {
            if let Some(cleanup_dl) = meta.cleanup_deadline {
                if Instant::now() < cleanup_dl {
                    let _ =
                        tokio::time::timeout_at(cleanup_dl, backing.retire_physical_socket()).await;
                } else {
                    let _ = tokio::time::timeout(
                        Duration::from_millis(5),
                        backing.retire_physical_socket(),
                    )
                    .await;
                }
            } else {
                let _ =
                    tokio::time::timeout(connect_timeout, backing.retire_physical_socket()).await;
            }
        }

        let close_lease_now = close_after_first || {
            let guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.close_after_inflight
        };

        let mut remaining_backing = Some(backing);
        if close_lease_now && let Some(b) = remaining_backing.take() {
            let _ = tokio::time::timeout(connect_timeout, b.close()).await;
        }

        let late_close_backing = {
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.closing_inflight = false;
            let final_close = guard.close_after_inflight || remaining_backing.is_none();
            guard.close_after_inflight = false;

            let late_res = match remaining_backing.take() {
                None => {
                    guard.pinned_owner = None;
                    guard.active_pipeline = None;
                    guard.ambient_cancel_failed = false;
                    let generation = guard.next_generation;
                    guard.state = SessionState::Closed { generation };
                    None
                }
                Some(b) if final_close => {
                    let generation = b.generation();
                    guard.pinned_owner = None;
                    guard.active_pipeline = None;
                    guard.ambient_cancel_failed = false;
                    guard.state = SessionState::Closed { generation };
                    Some(b)
                }
                Some(SessionBacking::Direct(c))
                    if c.is_closed()
                        && !guard.ambient_cancel_failed
                        && guard.active_pipeline.is_none() =>
                {
                    let generation = c.generation();
                    guard.pinned_owner = None;
                    guard.state = SessionState::Closed { generation };
                    None
                }
                Some(b) => {
                    guard.state = SessionState::Ready(b);
                    None
                }
            };
            if meta.rereserve_op_gen_on_ok && !matches!(guard.state, SessionState::Ready(_)) {
                guard.reserved_generations.remove(&op_gen.0);
                guard.pre_cancelled_generations.remove(&op_gen.0);
                if result.is_ok() {
                    result = Err(PostgresError::Interface(
                        "session connection is closed".to_string(),
                    ));
                }
            }
            late_res
        };
        if let Some(b) = late_close_backing {
            let _ = tokio::time::timeout(connect_timeout, b.close()).await;
        }
        self.shared.inflight_done_notify.notify_waiters();
        result
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
        self.wait_portal_drop_cleanup_if_needed().await;
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if guard.active_portal.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute operation while a Portal is active on this session".to_string(),
            ));
        }
        if guard.active_copy {
            return Err(PostgresError::Interface(
                "cannot execute operation while a COPY stream is active on this session"
                    .to_string(),
            ));
        }
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute ordinary session operation while a pipeline scope is active"
                    .to_string(),
            ));
        }
        if guard.ambient_cancel_failed {
            return Err(PostgresError::Interface(
                "ambient transaction failed due to cancellation; call rollback() first".to_string(),
            ));
        }
        Self::validate_pinned_owner(&guard, owner)?;

        match guard.state {
            SessionState::Ready(_) => {
                let generation = guard.next_generation;
                let prev =
                    std::mem::replace(&mut guard.state, SessionState::InFlight { generation });
                if let SessionState::Ready(backing) = prev {
                    if backing.is_closed() {
                        if matches!(backing, SessionBacking::Pooled(_)) {
                            guard.state = SessionState::Ready(backing);
                        } else {
                            guard.state = SessionState::Closed {
                                generation: backing.generation(),
                            };
                        }
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
        self.acquire_portal_operation_with_generation(None, owner, portal_name, false)
            .await
            .map(|(g, _)| g)
    }

    pub(crate) async fn acquire_portal_operation_with_generation(
        &self,
        fenced_op_gen: Option<OperationGeneration>,
        owner: TransactionOwner,
        portal_name: &str,
        had_caller_ambient_tx: bool,
    ) -> Result<(SessionOperationGuard<'_>, Arc<tokio::sync::Notify>), PostgresError> {
        let _access = SessionAccess::Portal;
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if let Some(op_gen) = fenced_op_gen {
            guard.reserved_generations.remove(&op_gen.0);
            if guard.pre_cancelled_generations.remove(&op_gen.0) {
                return Err(PostgresError::Interface(
                    "portal operation was cancelled before wire submission".to_string(),
                ));
            }
        }
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute portal operation while a pipeline scope is active".to_string(),
            ));
        }
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
                        if matches!(backing, SessionBacking::Pooled(_)) {
                            guard.state = SessionState::Ready(backing);
                        } else {
                            guard.state = SessionState::Closed {
                                generation: backing.generation(),
                            };
                        }
                        return Err(PostgresError::Interface(
                            "session connection is closed".to_string(),
                        ));
                    }
                    let op_gen = match fenced_op_gen {
                        Some(g) => g,
                        None => {
                            guard.next_op_generation = guard.next_op_generation.saturating_add(1);
                            OperationGeneration(guard.next_op_generation)
                        }
                    };
                    let notify = Arc::new(tokio::sync::Notify::new());
                    guard.active_op = Some(ActiveOpState {
                        op_gen,
                        key: backing.cancel_key.clone(),
                        had_ambient_tx: had_ambient_for_portal(had_caller_ambient_tx),
                        cancel_claimed: false,
                        retire_on_finish: guard.retire_pending,
                        cancel_notify: Arc::clone(&notify),
                    });
                    guard.state = SessionState::InFlight {
                        generation: backing.generation(),
                    };
                    Ok((
                        SessionOperationGuard {
                            handle: self,
                            backing: Some(backing),
                            portal_mode: true,
                        },
                        notify,
                    ))
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

    pub(crate) fn begin_portal_drop_cleanup(&self, portal_name: &str, owner: TransactionOwner) {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if matches!(
            &guard.active_portal,
            Some((active_owner, active_name)) if *active_owner == owner && active_name == portal_name
        ) {
            guard.portal_cleanup_inflight = true;
        }
    }

    pub(crate) async fn finish_portal_drop_cleanup(
        &self,
        portal_name: &str,
        owner: TransactionOwner,
    ) {
        let close_backing = {
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            if let SessionState::Ready(ref mut backing) = guard.state {
                backing.unregister_portal_name(portal_name);
                guard.last_transaction_status = backing.transaction_status();
            }
            if matches!(
                &guard.active_portal,
                Some((active_owner, active_name)) if *active_owner == owner && active_name == portal_name
            ) {
                guard.active_portal = None;
                if guard.last_transaction_status == TransactionStatus::Idle
                    && !guard.ambient_cancel_failed
                {
                    guard.pinned_owner = None;
                } else if let Some(pinned) = guard.pinned_owner
                    && pinned == owner
                {
                    guard.pinned_owner = None;
                }
            }
            guard.portal_cleanup_inflight = false;
            if guard.close_after_inflight {
                guard.close_after_inflight = false;
                let next_gen = guard.next_generation;
                let prev = std::mem::replace(
                    &mut guard.state,
                    SessionState::Closed {
                        generation: next_gen,
                    },
                );
                if let SessionState::Ready(mut backing) = prev {
                    if guard.retire_pending {
                        backing.mark_retire();
                    }
                    guard.closing_inflight = true;
                    guard.pinned_owner = None;
                    guard.ambient_cancel_failed = false;
                    guard.state = SessionState::Closed {
                        generation: backing.generation(),
                    };
                    Some(backing)
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(b) = close_backing {
            let _ = tokio::time::timeout(self.shared.config.connect_timeout, b.close()).await;
            let mut guard = self
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.closing_inflight = false;
        }
        self.shared.inflight_done_notify.notify_waiters();
    }

    /// Unpin an active [`Portal`] when it completes or closes.
    pub(crate) fn unpin_portal(&self, portal_name: &str, owner: TransactionOwner) {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if let SessionState::Ready(ref mut backing) = guard.state {
            backing.unregister_portal_name(portal_name);
            guard.last_transaction_status = backing.transaction_status();
        }
        if matches!(
            &guard.active_portal,
            Some((active_owner, active_name)) if *active_owner == owner && active_name == portal_name
        ) {
            guard.active_portal = None;
            if guard.last_transaction_status == TransactionStatus::Idle
                && !guard.ambient_cancel_failed
            {
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
        let op_gen = self.reserve_operation_generation();
        self.begin_as_with_generation(op_gen, Some(owner)).await
    }

    pub(crate) async fn begin_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, move |conn, meta| {
            Box::pin(async move {
                let disp = conn.begin().await?;
                if let Some(pin) = owner {
                    meta.pin_owner_on_ok = Some(pin);
                }
                Ok(disp)
            })
        })
        .await
    }

    /// Commit an active transaction (`COMMIT`) pinned to `owner`.
    pub async fn commit_as(
        &self,
        owner: TransactionOwner,
    ) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.commit_as_with_generation(op_gen, Some(owner)).await
    }

    pub(crate) async fn commit_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, |conn, _meta| {
            Box::pin(async move { conn.commit().await })
        })
        .await
    }

    /// Roll back an active transaction (`ROLLBACK`) pinned to `owner`.
    pub async fn rollback_as(
        &self,
        owner: TransactionOwner,
    ) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.rollback_as_with_generation(op_gen, Some(owner)).await
    }

    /// Roll back the unpinned client transaction (`ROLLBACK`), clearing any ambient cancellation failure and retiring cancel-sent sockets.
    pub async fn rollback_client(&self) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.rollback_as_with_generation(op_gen, None).await
    }

    pub(crate) async fn rollback_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::RollbackAllowed,
            owner,
            |conn, meta| {
                Box::pin(async move {
                    if conn.is_closed() && (meta.ambient_cancel_failed || meta.retire_pending) {
                        meta.force_retire_now = true;
                        return Ok(TransactionDisposition::RolledBack);
                    }
                    if (meta.ambient_cancel_failed || meta.retire_pending)
                        && conn.transaction_status() == TransactionStatus::Idle
                    {
                        meta.force_retire_now = true;
                        return Ok(TransactionDisposition::RolledBack);
                    }
                    let res = conn.rollback().await;
                    if meta.ambient_cancel_failed || meta.retire_pending {
                        meta.force_retire_now = true;
                        if res.is_err() && meta.retire_pending {
                            conn.mark_closed();
                            return Ok(TransactionDisposition::RolledBack);
                        }
                    }
                    res
                })
            },
        )
        .await
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.savepoint_as_with_generation(op_gen, Some(owner), token)
            .await
    }

    pub(crate) async fn savepoint_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Ordinary,
            owner,
            move |conn, _meta| Box::pin(async move { conn.savepoint(token).await }),
        )
        .await
    }

    /// Release a numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn release_savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.release_savepoint_as_with_generation(op_gen, Some(owner), token)
            .await
    }

    pub(crate) async fn release_savepoint_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Ordinary,
            owner,
            move |conn, _meta| Box::pin(async move { conn.release_savepoint(token).await }),
        )
        .await
    }

    /// Roll back to a numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`) under `owner`.
    pub async fn rollback_to_savepoint_as(
        &self,
        owner: TransactionOwner,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.rollback_to_savepoint_as_with_generation(op_gen, Some(owner), token)
            .await
    }

    pub(crate) async fn rollback_to_savepoint_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
        token: u64,
    ) -> Result<TransactionDisposition, PostgresError> {
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Ordinary,
            owner,
            move |conn, _meta| Box::pin(async move { conn.rollback_to_savepoint(token).await }),
        )
        .await
    }

    pub(crate) async fn cleanup_deadline_expired_command(
        shared: &Arc<SessionShared>,
        op_gen: OperationGeneration,
        wire_started: bool,
        write_uncertain: bool,
        conn: &mut PostgresConnection,
        meta: &mut OperationMeta,
    ) {
        let claim = Self::claim_deadline_cancel_on_shared(shared, op_gen, wire_started);
        if !wire_started {
            return;
        }
        let cleanup_dl = Instant::now() + shared.config.connect_timeout;
        meta.force_retire_now = true;
        meta.cleanup_deadline = Some(cleanup_dl);

        if let self::cancel::SessionCancelClaim::Send { key, hook } = claim {
            let _ = self::cancel::send_cancel_request_until(
                &shared.config,
                &shared.cloud_connector,
                &key,
                cleanup_dl,
                hook,
            )
            .await;
        }

        if write_uncertain {
            conn.mark_closed();
            return;
        }

        if Instant::now() < cleanup_dl {
            let _ = conn.drain_to_ready_for_query_until(Some(cleanup_dl)).await;
        }
        conn.mark_closed();
    }

    /// Execute an untimed extended query (`CommandBudget::untimed()`).
    pub async fn query(
        &self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        self.query_with_deadline(sql, parameters, parameter_oids, CommandBudget::untimed())
            .await
    }

    /// Execute an extended query bounded by one non-renewing [`CommandBudget`].
    pub async fn query_with_deadline(
        &self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        budget: CommandBudget,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.query_as_with_generation_and_budget(
            op_gen,
            None,
            sql,
            parameters,
            parameter_oids,
            budget,
        )
        .await
    }

    /// Execute an extended query under `owner`.
    pub async fn query_as(
        &self,
        owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.query_as_with_generation(op_gen, owner, sql, parameters, parameter_oids)
            .await
    }

    pub(crate) async fn query_as_with_generation(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        self.query_as_with_generation_and_budget(
            op_gen,
            owner,
            sql,
            parameters,
            parameter_oids,
            CommandBudget::untimed(),
        )
        .await
    }

    pub(crate) async fn query_as_with_generation_and_budget(
        &self,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        budget: CommandBudget,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        let sql_owned = sql.to_owned();
        let params_owned = parameters.to_vec();
        let oids_owned = parameter_oids.map(ToOwned::to_owned);
        let shared = Arc::clone(&self.shared);
        self.run_cancellable_operation(op_gen, SessionAccess::Ordinary, owner, move |conn, meta| {
            Box::pin(async move {
                let (res, timed_out, wire_started, write_uncertain) = {
                    let mut dl_guard = conn.enter_session_command_deadline(budget.deadline());
                    let res = dl_guard
                        .query(&sql_owned, &params_owned, oids_owned.as_deref())
                        .await;
                    (
                        res,
                        dl_guard.timed_out(),
                        dl_guard.wire_started(),
                        dl_guard.write_uncertain(),
                    )
                };
                if timed_out {
                    Self::cleanup_deadline_expired_command(
                        &shared,
                        op_gen,
                        wire_started,
                        write_uncertain,
                        conn,
                        meta,
                    )
                    .await;
                }
                res
            })
        })
        .await
    }

    /// Execute an extended query on the shared session without holding a `Mutex` across `.await`.
    pub async fn query_shared(
        &self,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
    ) -> Result<crate::result::QueryResult, PostgresError> {
        self.query_with_deadline(sql, parameters, parameter_oids, CommandBudget::untimed())
            .await
    }

    /// Execute an untimed simple query (`'Q'`, `CommandBudget::untimed()`).
    pub async fn simple_query(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        self.simple_query_with_deadline(sql, CommandBudget::untimed())
            .await
    }

    /// Execute a simple query (`'Q'`) bounded by one non-renewing [`CommandBudget`].
    pub async fn simple_query_with_deadline(
        &self,
        sql: &str,
        budget: CommandBudget,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        let op_gen = self.reserve_operation_generation();
        self.simple_query_with_generation_and_budget(op_gen, sql, budget)
            .await
    }

    /// Execute a simple query (`'Q'`) on the shared session without holding a `Mutex` across `.await`.
    pub async fn simple_query_shared(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        self.simple_query_with_deadline(sql, CommandBudget::untimed())
            .await
    }

    pub(crate) async fn simple_query_with_generation_and_budget(
        &self,
        op_gen: OperationGeneration,
        sql: &str,
        budget: CommandBudget,
    ) -> Result<Vec<crate::result::QueryResult>, PostgresError> {
        let sql_owned = sql.to_owned();
        let shared = Arc::clone(&self.shared);
        self.run_cancellable_operation(op_gen, SessionAccess::Ordinary, None, move |conn, meta| {
            Box::pin(async move {
                let (res, timed_out, wire_started, write_uncertain) = {
                    let mut dl_guard = conn.enter_session_command_deadline(budget.deadline());
                    let res = dl_guard.simple_query(&sql_owned).await;
                    (
                        res,
                        dl_guard.timed_out(),
                        dl_guard.wire_started(),
                        dl_guard.write_uncertain(),
                    )
                };
                if timed_out {
                    Self::cleanup_deadline_expired_command(
                        &shared,
                        op_gen,
                        wire_started,
                        write_uncertain,
                        conn,
                        meta,
                    )
                    .await;
                }
                res
            })
        })
        .await
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
        let op_gen = self.reserve_operation_generation();
        self.open_portal_with_generation(op_gen, sql, parameters, parameter_oids, batch_size)
            .await
    }

    pub(crate) async fn open_portal_with_generation(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
    ) -> Result<Portal, PostgresError> {
        self.open_portal_as_with_generation(
            op_gen,
            None,
            sql,
            parameters,
            parameter_oids,
            batch_size,
        )
        .await
    }

    pub(crate) async fn open_portal_as_with_generation(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        caller_owner: Option<TransactionOwner>,
        sql: &str,
        parameters: &[crate::extended::Parameter],
        parameter_oids: Option<&[u32]>,
        batch_size: i32,
    ) -> Result<Portal, PostgresError> {
        if batch_size <= 0 {
            self.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Interface(
                "open_portal requires batch_size >= 1 (batch_size <= 0 is forbidden)".to_string(),
            ));
        }
        if sql.as_bytes().contains(&0) {
            self.unreserve_operation_generation(op_gen);
            return Err(PostgresError::Data(
                "SQL query string cannot contain embedded NUL byte".to_string(),
            ));
        }
        let sql_owned = sql.to_owned();
        let params_owned = parameters.to_vec();
        let oids_owned = parameter_oids.map(ToOwned::to_owned);
        let session_arc = Arc::clone(self);
        let _ = batch_size;
        self.run_cancellable_operation(
            op_gen,
            SessionAccess::Ordinary,
            caller_owner,
            move |conn, meta| {
                Box::pin(async move {
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

                    let resolved = crate::metadata::resolve_parameter_metadata(
                        conn,
                        &params_owned,
                        oids_owned.as_deref(),
                    )
                    .await?;

                    let portal_name = conn.next_portal_name();
                    let owner = TransactionOwner::new(
                        0x8000_0000_0000_0000 | conn.next_portal_id,
                        conn.generation(),
                    );
                    let scope = match conn.transaction_status() {
                        TransactionStatus::Idle => {
                            if let Err(err) = conn.begin().await {
                                conn.unregister_portal_name(&portal_name);
                                return Err(err);
                            }
                            TransactionScope::new(owner, TransactionScopeKind::ExplicitTransaction)
                        }
                        TransactionStatus::InTransaction => {
                            let token = conn.next_savepoint_token();
                            if let Err(err) = conn.savepoint(token).await {
                                conn.unregister_portal_name(&portal_name);
                                return Err(err);
                            }
                            TransactionScope::new(
                                owner,
                                TransactionScopeKind::NestedSavepoint { token },
                            )
                        }
                        TransactionStatus::Failed => {
                            conn.unregister_portal_name(&portal_name);
                            return Err(PostgresError::Interface(
                                "cannot open_portal while transaction is aborted ('E')".to_string(),
                            ));
                        }
                    };

                    let setup_res =
                        setup_named_portal_on_conn(conn, &portal_name, &sql_owned, &resolved).await;
                    match setup_res {
                        Ok((columns, codec_ctx, catalog_snapshot)) => {
                            let generation = conn.generation();
                            meta.install_portal_on_ok = Some((owner, portal_name.clone()));
                            Ok(Portal::new(
                                session_arc,
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
                })
            },
        )
        .await
    }

    /// Acquire exclusive session ownership and open a streaming binary `COPY ... TO STDOUT` handle.
    pub async fn copy_to(
        self: &Arc<Self>,
        sql: &str,
    ) -> Result<crate::copy::CopyOut, PostgresError> {
        validate_single_copy_sql(sql)?;
        let op_gen = self.reserve_operation_generation();
        self.copy_to_with_generation_as(op_gen, sql, None).await
    }

    /// Open a streaming binary `COPY ... TO STDOUT` handle inside an optional explicit [`TransactionOwner`].
    pub async fn copy_to_as(
        self: &Arc<Self>,
        sql: &str,
        owner: Option<TransactionOwner>,
    ) -> Result<crate::copy::CopyOut, PostgresError> {
        validate_single_copy_sql(sql)?;
        let op_gen = self.reserve_operation_generation();
        self.copy_to_with_generation_as(op_gen, sql, owner).await
    }

    pub(crate) async fn copy_to_with_generation_as(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        sql: &str,
        owner: Option<TransactionOwner>,
    ) -> Result<crate::copy::CopyOut, PostgresError> {
        if let Err(err) = validate_single_copy_sql(sql) {
            self.unreserve_operation_generation(op_gen);
            return Err(err);
        }
        let mut lease = self.checkout_for_copy(op_gen, owner).await?;
        let start_res = {
            let already_cancelled = lease.cancel_claimed();
            let cancel_notify = lease.cancel_notify();
            let timeout = lease.connect_timeout();
            let conn = lease.connection_mut();
            let timed = {
                let start_fut = conn.start_copy_out(sql);
                tokio::pin!(start_fut);
                let cancel_timeout = async move {
                    if !already_cancelled {
                        cancel_notify.notified().await;
                    }
                    tokio::time::sleep(timeout).await;
                };
                tokio::pin!(cancel_timeout);
                tokio::select! {
                    biased;
                    res = &mut start_fut => Some(res),
                    () = &mut cancel_timeout => None,
                }
            };
            match timed {
                Some(res) => res,
                None => {
                    conn.mark_closed();
                    Err(PostgresError::Operational(
                        "cancelled COPY TO start timed out while draining wire response"
                            .to_string(),
                    ))
                }
            }
        };
        match start_res {
            Ok(cols) => {
                if lease.cancel_claimed() {
                    lease.cancel_drain_and_retire().await;
                    return Err(PostgresError::Operational(
                        "COPY TO was cancelled during start".to_string(),
                    ));
                }
                Ok(crate::copy::CopyOut::new(lease, cols))
            }
            Err(err) => {
                if lease.connection_mut().is_closed() || lease.cancel_claimed() {
                    lease.finish_retired().await;
                } else {
                    lease.finish_clean().await;
                }
                Err(err)
            }
        }
    }

    /// Acquire exclusive session ownership and open a duplex binary `COPY ... FROM STDIN` handle.
    pub async fn copy_from(
        self: &Arc<Self>,
        sql: &str,
    ) -> Result<crate::copy::CopyFromStream, PostgresError> {
        validate_single_copy_sql(sql)?;
        let op_gen = self.reserve_operation_generation();
        self.copy_from_with_generation_as(op_gen, sql, None).await
    }

    /// Open a duplex binary `COPY ... FROM STDIN` handle inside an optional explicit [`TransactionOwner`].
    pub async fn copy_from_as(
        self: &Arc<Self>,
        sql: &str,
        owner: Option<TransactionOwner>,
    ) -> Result<crate::copy::CopyFromStream, PostgresError> {
        validate_single_copy_sql(sql)?;
        let op_gen = self.reserve_operation_generation();
        self.copy_from_with_generation_as(op_gen, sql, owner).await
    }

    pub(crate) async fn copy_from_with_generation_as(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        sql: &str,
        owner: Option<TransactionOwner>,
    ) -> Result<crate::copy::CopyFromStream, PostgresError> {
        if let Err(err) = validate_single_copy_sql(sql) {
            self.unreserve_operation_generation(op_gen);
            return Err(err);
        }
        let mut lease = self.checkout_for_copy(op_gen, owner).await?;
        let start_res = {
            let already_cancelled = lease.cancel_claimed();
            let cancel_notify = lease.cancel_notify();
            let timeout = lease.connect_timeout();
            let conn = lease.connection_mut();
            let timed = {
                let start_fut = conn.start_copy_in(sql);
                tokio::pin!(start_fut);
                let cancel_timeout = async move {
                    if !already_cancelled {
                        cancel_notify.notified().await;
                    }
                    tokio::time::sleep(timeout).await;
                };
                tokio::pin!(cancel_timeout);
                tokio::select! {
                    biased;
                    res = &mut start_fut => Some(res),
                    () = &mut cancel_timeout => None,
                }
            };
            match timed {
                Some(res) => res,
                None => {
                    conn.mark_closed();
                    Err(PostgresError::Operational(
                        "cancelled COPY FROM start timed out while draining wire response"
                            .to_string(),
                    ))
                }
            }
        };
        match start_res {
            Ok(cols) => {
                let (owns_tx, ctx, catalog) = {
                    let conn = lease.connection_mut();
                    let owns = conn.copy_in_owned_tx;
                    conn.copy_in_owned_tx = false;
                    let ctx = crate::codecs::CodecContext::from_parameters(&conn.parameters);
                    let cat = conn.type_catalog.clone();
                    (owns, ctx, cat)
                };
                if lease.cancel_claimed() {
                    crate::copy::abort_copy_in_lease(lease, owns_tx, "COPY FROM cancelled").await;
                    return Err(PostgresError::Operational(
                        "COPY FROM was cancelled during start".to_string(),
                    ));
                }
                match crate::copy::CopyFromStream::new(lease, cols, owns_tx, ctx, catalog) {
                    Ok(stream) => Ok(stream),
                    Err(err) => Err(err),
                }
            }
            Err(err) => {
                if lease.connection_mut().is_closed() || lease.cancel_claimed() {
                    lease.finish_retired().await;
                } else {
                    lease.finish_clean().await;
                }
                Err(err)
            }
        }
    }

    pub(crate) fn ensure_not_in_pipeline_for_copy(&self) -> Result<(), PostgresError> {
        let guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::UnsupportedFeature(
                "COPY is not supported while a PostgreSQL pipeline scope is active".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn checkout_for_copy(
        self: &Arc<Self>,
        op_gen: OperationGeneration,
        owner: Option<TransactionOwner>,
    ) -> Result<CopySessionLease, PostgresError> {
        self.wait_portal_drop_cleanup_if_needed().await;
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        guard.reserved_generations.remove(&op_gen.0);
        if guard.pre_cancelled_generations.remove(&op_gen.0) {
            return Err(PostgresError::Interface(
                "COPY operation was cancelled before wire submission".to_string(),
            ));
        }
        if guard.active_pipeline.is_some() {
            return Err(PostgresError::UnsupportedFeature(
                "COPY is not supported while a PostgreSQL pipeline scope is active".to_string(),
            ));
        }
        if guard.active_portal.is_some() {
            return Err(PostgresError::Interface(
                "cannot execute COPY while a Portal is active on this session".to_string(),
            ));
        }
        if guard.active_copy {
            return Err(PostgresError::Interface(
                "another COPY stream is already active on this session".to_string(),
            ));
        }
        if guard.ambient_cancel_failed {
            return Err(PostgresError::Interface(
                "ambient transaction failed due to cancellation; call rollback() first".to_string(),
            ));
        }
        Self::validate_pinned_owner(&guard, owner)?;

        match guard.state {
            SessionState::Ready(_) => {
                let next_gen = guard.next_generation;
                let prev = std::mem::replace(
                    &mut guard.state,
                    SessionState::InFlight {
                        generation: next_gen,
                    },
                );
                let SessionState::Ready(backing) = prev else {
                    unreachable!("checked Ready")
                };
                if backing.is_closed() {
                    if matches!(backing, SessionBacking::Pooled(_)) {
                        guard.state = SessionState::Ready(backing);
                    } else {
                        guard.state = SessionState::Closed {
                            generation: backing.generation(),
                        };
                    }
                    return Err(PostgresError::Interface(
                        "session connection is closed".to_string(),
                    ));
                }
                let conn_gen = backing.generation();
                let had_ambient_tx = backing.transaction_status() != TransactionStatus::Idle;
                let cancel_notify = Arc::new(tokio::sync::Notify::new());
                guard.active_op = Some(ActiveOpState {
                    op_gen,
                    key: backing.cancel_key.clone(),
                    had_ambient_tx,
                    cancel_claimed: false,
                    retire_on_finish: guard.retire_pending,
                    cancel_notify: Arc::clone(&cancel_notify),
                });
                guard.active_copy = true;
                guard.state = SessionState::InFlight {
                    generation: conn_gen,
                };
                Ok(CopySessionLease {
                    session: Arc::clone(self),
                    backing: Some(backing),
                    op_gen,
                    cancel_notify,
                    ambient_failed_on_retire: false,
                })
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

    fn rollback_connecting(&self, generation: u64) {
        let mut guard = self
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
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
    conn.set_exchange_state(ExchangeState::InExtendedExchange);

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
                conn.set_exchange_state(ExchangeState::Idle);
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
        &self
            .handle
            .as_ref()
            .expect("PendingSession armed")
            .shared
            .config
    }

    fn cloud_connector(&self) -> Arc<CloudConnector> {
        Arc::clone(
            &self
                .handle
                .as_ref()
                .expect("PendingSession armed")
                .shared
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
        let shared = Arc::get_mut(&mut handle.shared)
            .expect("SessionHandle::connect requires unique ownership before commit");
        let inner = shared.inner.get_mut().expect("SessionHandle lock poisoned");
        inner.parameters = conn.parameters.clone();
        inner.last_transaction_status = conn.transaction_status();
        inner.retire_pending = false;
        inner.ambient_cancel_failed = false;
        inner.active_pipeline = None;
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
            let mut guard = handle
                .shared
                .inner
                .lock()
                .expect("SessionHandle lock poisoned");
            guard.state = SessionState::Closed {
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

/// Dial and authenticate an unpooled standalone [`PostgresConnection`] reusing the full Cloud SQL,
/// AlloyDB, direct-IAM token refresh, TLS, SCRAM-SHA-256-PLUS, Unix socket, and `connect_timeout`
/// startup path.
///
/// When `replication_database` is `true`, the `StartupMessage` includes `"replication" => "database"`
/// and omits arbitrary session GUC parameters and post-authentication `SET` GUC commands that are
/// incompatible with PostgreSQL walsender startup.
pub async fn establish_standalone_connection(
    config: &PostgresConfig,
    replication_database: bool,
) -> Result<PostgresConnection, PostgresError> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let connector = Arc::new(CloudConnector::with_clock(Arc::clone(&clock)));
    let token_provider = ScopedTokenProvider::new(
        Some(config.cloud_auth_options.clone()),
        (!config.iam_auth_options.is_empty()).then(|| config.iam_auth_options.clone()),
        Arc::clone(&clock),
    );
    let deadline = Instant::now() + config.connect_timeout;
    establish_postgres_connection_with_mode(
        config,
        &connector,
        &token_provider,
        clock.as_ref(),
        1,
        deadline,
        replication_database,
    )
    .await
}

fn encode_replication_startup_frame(
    config: &PostgresConfig,
    out: &mut BytesMut,
) -> Result<(), PostgresError> {
    let app_name = config
        .session_gucs
        .iter()
        .find_map(|(k, v)| {
            k.eq_ignore_ascii_case("application_name")
                .then_some(v.as_str())
        })
        .unwrap_or(config.application_name.as_str());
    let mut params: Vec<(&str, &str)> = Vec::with_capacity(5);
    params.push(("user", config.user.as_str()));
    params.push(("database", config.database.as_str()));
    params.push(("client_encoding", "UTF8"));
    params.push(("replication", "database"));
    if !app_name.is_empty() {
        params.push(("application_name", app_name));
    }
    frontend::startup_message(params, out).map_err(|e| {
        PostgresError::Protocol(format!("failed to encode replication StartupMessage: {e}"))
    })
}

pub(crate) async fn establish_postgres_connection(
    config: &PostgresConfig,
    connector: &Arc<CloudConnector>,
    token_provider: &ScopedTokenProvider,
    clock: &dyn Clock,
    generation: u64,
    deadline: Instant,
) -> Result<PostgresConnection, PostgresError> {
    establish_postgres_connection_with_mode(
        config,
        connector,
        token_provider,
        clock,
        generation,
        deadline,
        false,
    )
    .await
}

async fn establish_postgres_connection_with_mode(
    config: &PostgresConfig,
    connector: &Arc<CloudConnector>,
    token_provider: &ScopedTokenProvider,
    clock: &dyn Clock,
    generation: u64,
    deadline: Instant,
    replication_database: bool,
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
    if replication_database {
        encode_replication_startup_frame(config, &mut write_buf)?;
    } else {
        encode_startup_frame(config, &mut write_buf)?;
    }

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
                let configured_session_gucs: BTreeMap<String, String> = if replication_database {
                    BTreeMap::new()
                } else {
                    config
                        .session_gucs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                };
                let cancel_key = backend_key.map(CancelKeyData::from);
                let standalone_cancel = Arc::new(StandaloneCancelState {
                    config: config.clone(),
                    cloud_connector: Arc::clone(connector),
                    key: cancel_key.clone(),
                    in_exchange: std::sync::atomic::AtomicBool::new(false),
                    cancel_sent: std::sync::atomic::AtomicBool::new(false),
                });
                return Ok(PostgresConnection {
                    stream: ConnectionStream::Transport(stream),
                    read_buf,
                    write_buf,
                    parameters,
                    startup_parameters,
                    configured_session_gucs,
                    baseline_application_name: config.application_name.clone(),
                    backend_key,
                    cancel_key,
                    standalone_cancel,
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
                    active_deadline: None,
                    deadline_wire_started: false,
                    deadline_timed_out: false,
                    deadline_write_uncertain: false,
                    defer_deadline_read_close: false,
                    copy_in_owned_tx: false,
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
            &session.shared.config,
            &session.shared.cloud_connector,
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
        let mut guard = session
            .shared
            .inner
            .lock()
            .expect("SessionHandle lock poisoned");
        match guard.state {
            SessionState::Connecting { generation } if generation == self.generation => {
                guard.parameters = conn.parameters.clone();
                guard.last_transaction_status = conn.transaction_status();
                guard.retire_pending = false;
                guard.ambient_cancel_failed = false;
                guard.active_pipeline = None;
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

const fn had_ambient_for_portal(had_caller_ambient_tx: bool) -> bool {
    had_caller_ambient_tx
}

fn format_set_guc_sql(key: &str, value: &str) -> String {
    let escaped_ident = key.replace('"', "\"\"");
    let escaped_val = value.replace('\'', "''");
    format!("SET \"{escaped_ident}\" = '{escaped_val}'")
}

/// Validate that `sql` contains zero NUL bytes and represents exactly one SQL statement
/// (permitting semicolons inside quotes/comments/dollar-quotes and optional trailing `;` + trivia).
pub(crate) fn validate_single_copy_sql(sql: &str) -> Result<(), PostgresError> {
    if sql.as_bytes().contains(&0) {
        return Err(PostgresError::Data(
            "SQL query string cannot contain embedded NUL byte".to_string(),
        ));
    }
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut pos = 0usize;
    let mut saw_token = false;
    let mut saw_top_semicolon = false;

    while pos < len {
        let b = bytes[pos];
        if b.is_ascii_whitespace() {
            pos += 1;
            continue;
        }
        if b == b'-' && pos + 1 < len && bytes[pos + 1] == b'-' {
            pos += 2;
            while pos < len && bytes[pos] != b'\n' && bytes[pos] != b'\r' {
                pos += 1;
            }
            continue;
        }
        if b == b'/' && pos + 1 < len && bytes[pos + 1] == b'*' {
            pos += 2;
            let mut depth = 1usize;
            while pos < len && depth > 0 {
                if pos + 1 < len && bytes[pos] == b'/' && bytes[pos + 1] == b'*' {
                    depth += 1;
                    pos += 2;
                } else if pos + 1 < len && bytes[pos] == b'*' && bytes[pos + 1] == b'/' {
                    depth -= 1;
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            continue;
        }
        if saw_top_semicolon {
            return Err(PostgresError::Interface(
                "COPY statement cannot contain multiple SQL commands".to_string(),
            ));
        }
        if b == b';' {
            if !saw_token {
                return Err(PostgresError::Interface(
                    "COPY statement cannot start with an empty `;` command".to_string(),
                ));
            }
            saw_top_semicolon = true;
            pos += 1;
            continue;
        }
        saw_token = true;
        if b == b'\'' {
            let escaped_string = pos > 0
                && (bytes[pos - 1] == b'E' || bytes[pos - 1] == b'e')
                && (pos < 2 || {
                    let prev = bytes[pos - 2];
                    !(prev.is_ascii_alphanumeric() || prev == b'_' || prev == b'$' || prev >= 0x80)
                });
            pos += 1;
            while pos < len {
                if escaped_string && bytes[pos] == b'\\' {
                    pos = (pos + 2).min(len);
                    continue;
                }
                if bytes[pos] == b'\'' {
                    pos += 1;
                    if pos < len && bytes[pos] == b'\'' {
                        pos += 1;
                        continue;
                    }
                    break;
                }
                pos += 1;
            }
            continue;
        }
        if b == b'"' {
            pos += 1;
            while pos < len {
                if bytes[pos] == b'"' {
                    pos += 1;
                    if pos < len && bytes[pos] == b'"' {
                        pos += 1;
                        continue;
                    }
                    break;
                }
                pos += 1;
            }
            continue;
        }
        if b == b'$'
            && let Some(tag_end) = parse_dollar_quote_tag(bytes, pos)
        {
            let tag = &bytes[pos..tag_end];
            pos = tag_end;
            while pos + tag.len() <= len {
                if &bytes[pos..pos + tag.len()] == tag {
                    pos += tag.len();
                    break;
                }
                pos += 1;
            }
            if pos + tag.len() > len && !bytes[..len].ends_with(tag) {
                pos = len;
            }
            continue;
        }
        pos += 1;
    }

    if !saw_token {
        return Err(PostgresError::Interface(
            "COPY SQL statement cannot be empty".to_string(),
        ));
    }
    Ok(())
}

fn parse_dollar_quote_tag(bytes: &[u8], start: usize) -> Option<usize> {
    debug_assert_eq!(bytes[start], b'$');
    let mut idx = start + 1;
    if idx < bytes.len() && bytes[idx] == b'$' {
        return Some(idx + 1);
    }
    let first = *bytes.get(idx)?;
    if !(first.is_ascii_alphabetic() || first == b'_' || first >= 0x80) {
        return None;
    }
    idx += 1;
    while idx < bytes.len() {
        let c = bytes[idx];
        if c == b'$' {
            return Some(idx + 1);
        }
        if !(c.is_ascii_alphanumeric() || c == b'_' || c >= 0x80) {
            return None;
        }
        idx += 1;
    }
    None
}

#[cfg(unix)]
pub(crate) async fn dial_unix_socket(
    path: &str,
    deadline: Instant,
) -> Result<TransportStream, PostgresError> {
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
pub(crate) async fn dial_unix_socket(
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
