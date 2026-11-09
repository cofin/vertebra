//! PostgreSQL session state machine (`SessionHandle`), cancellation-safe `PendingSession` guard,
//! and authenticated `PostgresConnection` publication.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
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
use crate::protocol::{TERMINATE_FRAME, encode_startup_frame, parse_error_fields, read_message};
use crate::tls;

/// Backend transaction status indicator reported by `ReadyForQuery` (`'Z'`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    /// `'I'` — idle (not in a transaction block).
    Idle,
    /// `'T'` — inside an open transaction block.
    InTransaction,
    /// `'E'` — inside a failed transaction block awaiting `ROLLBACK`.
    Failed,
}

impl TransactionStatus {
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

/// Established, authenticated PostgreSQL wire connection at `ReadyForQuery(Idle)`.
#[derive(Debug)]
pub struct PostgresConnection {
    pub(crate) stream: TransportStream,
    pub(crate) read_buf: BytesMut,
    pub(crate) write_buf: BytesMut,
    pub(crate) parameters: BTreeMap<String, String>,
    pub(crate) backend_key: Option<BackendKey>,
    pub(crate) transaction_status: TransactionStatus,
    pub(crate) generation: u64,
    pub(crate) tls_negotiated: bool,
    pub(crate) tls_server_end_point: Option<Vec<u8>>,
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

    /// Optional `(process_id, secret_key)` pair received during startup.
    pub const fn backend_key(&self) -> Option<BackendKey> {
        self.backend_key
    }

    /// Return `true` when the underlying transport negotiated TLS.
    pub const fn is_tls(&self) -> bool {
        self.tls_negotiated
    }

    /// Borrow the underlying [`TransportStream`].
    pub const fn stream(&self) -> &TransportStream {
        &self.stream
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

    /// Return the number of unconsumed backend bytes currently buffered.
    pub fn buffered_read_len(&self) -> usize {
        self.read_buf.len()
    }

    /// Return the number of unflushed frontend bytes currently buffered.
    pub fn buffered_write_len(&self) -> usize {
        self.write_buf.len()
    }

    /// Send a single 5-byte `Terminate` (`'X'`) frame and shut down the socket once.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let _ = self.stream.write_all(&TERMINATE_FRAME).await;
        let _ = self.stream.flush().await;
        let _ = self.stream.shutdown().await;
        Ok(())
    }
}

#[derive(Debug)]
enum SessionState {
    Disconnected,
    Connecting { generation: u64 },
    Ready(PostgresConnection),
    Closed { generation: u64 },
}

/// Explicit PostgreSQL session lifecycle handle (`Disconnected -> Connecting -> Ready -> Closed`).
pub struct SessionHandle {
    config: PostgresConfig,
    state: SessionState,
    next_generation: u64,
    cloud_connector: Arc<CloudConnector>,
    token_provider: ScopedTokenProvider,
    clock: Arc<dyn Clock>,
}

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("config", &self.config)
            .field("state", &self.state)
            .field("next_generation", &self.next_generation)
            .finish()
    }
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
            state: SessionState::Disconnected,
            next_generation: 0,
            cloud_connector,
            token_provider,
            clock,
        }
    }

    /// Borrow the validated [`PostgresConfig`].
    pub const fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// Return `true` only when an authenticated [`PostgresConnection`] has been published.
    pub const fn is_ready(&self) -> bool {
        matches!(self.state, SessionState::Ready(_))
    }

    /// Return `true` when the session is in the terminal or post-failure `Closed` state.
    pub const fn is_closed(&self) -> bool {
        matches!(self.state, SessionState::Closed { .. })
    }

    /// Return the current or most recent connection generation (`0` before the first connect attempt).
    pub const fn generation(&self) -> u64 {
        match &self.state {
            SessionState::Disconnected => self.next_generation,
            SessionState::Connecting { generation } | SessionState::Closed { generation } => {
                *generation
            }
            SessionState::Ready(conn) => conn.generation(),
        }
    }

    /// Borrow the established [`PostgresConnection`] if `Ready`.
    pub const fn connection(&self) -> Option<&PostgresConnection> {
        match &self.state {
            SessionState::Ready(conn) => Some(conn),
            _ => None,
        }
    }

    /// Mutably borrow the established [`PostgresConnection`] if `Ready`.
    pub fn connection_mut(&mut self) -> Option<&mut PostgresConnection> {
        match &mut self.state {
            SessionState::Ready(conn) => Some(conn),
            _ => None,
        }
    }

    /// Begin a new connection attempt, returning a drop-safe [`ConnectAttempt`].
    ///
    /// If the returned [`ConnectAttempt`] is dropped or cancelled before `ReadyForQuery(Idle)`
    /// commits, [`PendingSession`] transitions this handle to `Closed { generation }`.
    pub fn begin_connect(&mut self) -> Result<ConnectAttempt<'_>, PostgresError> {
        if matches!(
            self.state,
            SessionState::Ready(_) | SessionState::Connecting { .. }
        ) {
            return Err(PostgresError::Interface(
                "cannot call connect on an already connected or connecting SessionHandle"
                    .to_string(),
            ));
        }
        self.next_generation = self.next_generation.saturating_add(1);
        let generation = self.next_generation;
        self.state = SessionState::Connecting { generation };
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

    /// Close the active connection (sending `Terminate` once if `Ready`) and mark `Closed`.
    pub async fn close(&mut self) -> Result<(), PostgresError> {
        let generation = self.generation();
        let prev = std::mem::replace(&mut self.state, SessionState::Closed { generation });
        if let SessionState::Ready(mut conn) = prev {
            self.state = SessionState::Closed {
                generation: conn.generation(),
            };
            conn.close().await?;
        }
        Ok(())
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
        handle.state = SessionState::Ready(conn);
        match &mut handle.state {
            SessionState::Ready(c) => c,
            _ => unreachable!("SessionState::Ready was just assigned"),
        }
    }
}

impl Drop for PendingSession<'_> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.state = SessionState::Closed {
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
                return Ok(PostgresConnection {
                    stream,
                    read_buf,
                    write_buf,
                    parameters,
                    backend_key,
                    transaction_status: status,
                    generation,
                    tls_negotiated,
                    tls_server_end_point,
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
                        redact_session_error(
                            e,
                            config.password.as_deref(),
                            login_snapshot.as_ref(),
                        )
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

#[derive(Debug)]
enum ClientSessionState {
    Configured,
    Connecting { generation: u64 },
    Ready(PostgresConnection),
    Closed { generation: u64 },
}

#[derive(Debug)]
struct ClientSessionInner {
    state: ClientSessionState,
    next_generation: u64,
    parameters: BTreeMap<String, String>,
}

/// Thread-safe physical session handle shared by `SyncPostgresClient` and `AsyncPostgresClient`.
pub struct ClientSession {
    config: PostgresConfig,
    cloud_connector: Arc<CloudConnector>,
    token_provider: ScopedTokenProvider,
    clock: Arc<dyn Clock>,
    inner: std::sync::Mutex<ClientSessionInner>,
}

impl std::fmt::Debug for ClientSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientSession")
            .field("config", &self.config)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Outcome of reserving a physical connection start on [`ClientSession`].
pub enum ClientBeginConnect {
    Attempt(ClientConnectAttempt),
    AlreadyReady,
}

impl ClientSession {
    /// Construct a disconnected [`ClientSession`] in `Configured` state (`closed == true`, `parameters == {}`).
    pub fn new(config: PostgresConfig) -> Self {
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
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
            inner: std::sync::Mutex::new(ClientSessionInner {
                state: ClientSessionState::Configured,
                next_generation: 0,
                parameters: BTreeMap::new(),
            }),
        }
    }

    /// Borrow the validated [`PostgresConfig`].
    pub const fn config(&self) -> &PostgresConfig {
        &self.config
    }

    /// Return `false` only when the session has published an authenticated `Ready` connection.
    pub fn is_closed(&self) -> bool {
        let guard = self.inner.lock().expect("ClientSession lock poisoned");
        !matches!(guard.state, ClientSessionState::Ready(_))
    }

    /// Return the current or most recent connection generation (`0` before the first connect attempt).
    pub fn generation(&self) -> u64 {
        let guard = self.inner.lock().expect("ClientSession lock poisoned");
        match &guard.state {
            ClientSessionState::Configured => guard.next_generation,
            ClientSessionState::Connecting { generation }
            | ClientSessionState::Closed { generation } => *generation,
            ClientSessionState::Ready(conn) => conn.generation(),
        }
    }

    /// Return a cloned snapshot of retained `ParameterStatus` entries.
    pub fn parameters(&self) -> BTreeMap<String, String> {
        let guard = self.inner.lock().expect("ClientSession lock poisoned");
        guard.parameters.clone()
    }

    /// Reserve a startup transition (`Configured -> Connecting`), return `AlreadyReady` if `Ready`, or reject `Connecting`/`Closed`.
    pub fn begin_connect(self: &Arc<Self>) -> Result<ClientBeginConnect, PostgresError> {
        let mut guard = self.inner.lock().expect("ClientSession lock poisoned");
        match guard.state {
            ClientSessionState::Configured => {
                guard.next_generation = guard.next_generation.saturating_add(1);
                let generation = guard.next_generation;
                guard.state = ClientSessionState::Connecting { generation };
                let deadline = Instant::now() + self.config.connect_timeout;
                Ok(ClientBeginConnect::Attempt(ClientConnectAttempt {
                    session: Some(Arc::clone(self)),
                    generation,
                    deadline,
                }))
            }
            ClientSessionState::Ready(_) => Ok(ClientBeginConnect::AlreadyReady),
            ClientSessionState::Connecting { .. } => Err(PostgresError::Interface(
                "connection attempt is already in progress".to_string(),
            )),
            ClientSessionState::Closed { .. } => Err(PostgresError::Interface(
                "cannot connect a closed client".to_string(),
            )),
        }
    }

    /// Synchronously transition to `Closed` and extract any live [`PostgresConnection`] for `Terminate` shutdown.
    pub fn begin_close(&self) -> Result<Option<PostgresConnection>, PostgresError> {
        let mut guard = self.inner.lock().expect("ClientSession lock poisoned");
        match guard.state {
            ClientSessionState::Connecting { .. } => Err(PostgresError::Interface(
                "cannot close client while connect is in progress".to_string(),
            )),
            ClientSessionState::Configured => {
                let generation = guard.next_generation;
                guard.state = ClientSessionState::Closed { generation };
                Ok(None)
            }
            ClientSessionState::Closed { .. } => Ok(None),
            ClientSessionState::Ready(_) => {
                let next_gen = guard.next_generation;
                let prev = std::mem::replace(
                    &mut guard.state,
                    ClientSessionState::Closed {
                        generation: next_gen,
                    },
                );
                if let ClientSessionState::Ready(conn) = prev {
                    guard.state = ClientSessionState::Closed {
                        generation: conn.generation(),
                    };
                    Ok(Some(conn))
                } else {
                    unreachable!("checked Ready")
                }
            }
        }
    }

    fn rollback_connecting(&self, generation: u64) {
        let mut guard = self.inner.lock().expect("ClientSession lock poisoned");
        if matches!(
            guard.state,
            ClientSessionState::Connecting { generation: g } if g == generation
        ) {
            guard.state = ClientSessionState::Closed { generation };
        }
    }
}

/// Owned, `'static`-compatible startup attempt that rolls back [`ClientSession`] to `Closed` on drop.
pub struct ClientConnectAttempt {
    session: Option<Arc<ClientSession>>,
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
    session: Option<Arc<ClientSession>>,
    conn: Option<PostgresConnection>,
    generation: u64,
}

impl ClientPendingSession {
    /// Publish the authenticated [`PostgresConnection`] into [`ClientSession`] (`Connecting -> Ready`).
    pub fn commit(mut self) -> Result<(), PostgresError> {
        let session = self.session.take().expect("ClientPendingSession armed");
        let conn = self.conn.take().expect("ClientPendingSession connection");
        let mut guard = session.inner.lock().expect("ClientSession lock poisoned");
        match guard.state {
            ClientSessionState::Connecting { generation } if generation == self.generation => {
                guard.parameters = conn.parameters.clone();
                guard.state = ClientSessionState::Ready(conn);
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

#[cfg(unix)]
async fn dial_unix_socket(
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
        .map_err(|_| PostgresError::Transport("Unix domain socket connection timed out".to_string()))?
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
    }
}
