//! PostgreSQL session state machine (`SessionHandle`), cancellation-safe `PendingSession` guard,
//! and authenticated `PostgresConnection` publication.

use std::collections::BTreeMap;

use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use vertebra_db::connection::TransportStream;

use crate::auth::Authentication;
use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::errors::PostgresError;
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
#[derive(Debug)]
pub struct SessionHandle {
    config: PostgresConfig,
    state: SessionState,
    next_generation: u64,
}

impl SessionHandle {
    /// Validate `raw` options without performing any network, DNS, or filesystem I/O.
    pub fn new(raw: RawPostgresOptions) -> Result<Self, PostgresError> {
        let config = PostgresConfig::validate(raw)?;
        Ok(Self::from_config(config))
    }

    /// Create a disconnected [`SessionHandle`] from a pre-validated [`PostgresConfig`].
    pub fn from_config(config: PostgresConfig) -> Self {
        Self {
            config,
            state: SessionState::Disconnected,
            next_generation: 0,
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
    /// Execute TCP/TLS dial, `StartupMessage`, authentication, and `ReadyForQuery(Idle)` verification.
    pub async fn run(self) -> Result<&'a mut PostgresConnection, PostgresError> {
        let Self { pending, deadline } = self;
        let config = pending.config().clone();
        let generation = pending.generation;

        let dialed = tls::connect(&config, deadline).await?;
        let mut stream = dialed.stream;
        let tls_negotiated = dialed.tls_negotiated;
        let tls_server_end_point = dialed.tls_server_end_point;

        let mut read_buf = BytesMut::with_capacity(8192);
        let mut write_buf = BytesMut::with_capacity(1024);
        encode_startup_frame(&config, &mut write_buf)?;

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
                    return Err(parse_error_fields(body.fields())?);
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
                    let conn = PostgresConnection {
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
                    };
                    return Ok(pending.commit(conn));
                }
                other => {
                    let completed = auth.handle_message(&other, &config, &mut write_buf)?;
                    if !write_buf.is_empty() {
                        if Instant::now() >= deadline {
                            return Err(PostgresError::Transport(
                                "authentication deadline exceeded before response flush"
                                    .to_string(),
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
}
