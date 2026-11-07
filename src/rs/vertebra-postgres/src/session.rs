//! Owned, cancellation-safe PostgreSQL session startup and publication.

use crate::{auth::Authentication, config::PostgresConfig, errors::PostgresError, protocol, tls};
use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, time::Instant};
use vertebra_db::connection::TransportStream;

/// PostgreSQL transaction state carried by ReadyForQuery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}
impl TransactionStatus {
    /// Validate a backend transaction status byte.
    pub fn from_wire_byte(byte: u8) -> Result<Self, PostgresError> {
        match byte {
            b'I' => Ok(Self::Idle),
            b'T' => Ok(Self::InTransaction),
            b'E' => Ok(Self::Failed),
            _ => Err(PostgresError::Protocol("invalid transaction status".into())),
        }
    }
    /// Return the protocol status byte.
    pub const fn as_wire_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

/// Backend cancellation credentials, retained exclusively in Rust.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackendKey {
    pub process_id: i32,
    pub secret_key: i32,
}

/// One physical socket and its authenticated startup metadata.
pub struct PostgresConnection {
    stream: TransportStream,
    parameters: HashMap<String, String>,
    backend_key: Option<BackendKey>,
    transaction_status: TransactionStatus,
    tls_server_end_point: Option<Vec<u8>>,
}
impl PostgresConnection {
    /// Return the last value of each startup parameter.
    pub fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }
    /// Return server-issued cancellation credentials.
    pub fn backend_key(&self) -> Option<BackendKey> {
        self.backend_key
    }
    /// Return the current transaction state.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }
    /// Return verified TLS peer binding material when available.
    pub fn tls_server_end_point(&self) -> Option<&[u8]> {
        self.tls_server_end_point.as_deref()
    }
}

enum SessionState {
    Configured,
    Connecting,
    Ready {
        conn: PostgresConnection,
        params_snapshot: HashMap<String, String>,
    },
    Closed {
        params_snapshot: HashMap<String, String>,
    },
}

/// Shared session ownership; only committed connections are visible as ready.
#[derive(Clone)]
pub struct SessionHandle {
    config: Arc<PostgresConfig>,
    state: Arc<Mutex<SessionState>>,
}
impl SessionHandle {
    /// Create a configured handle without opening a socket.
    pub fn new(config: PostgresConfig) -> Self {
        Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(SessionState::Configured)),
        }
    }
    /// Return the owned constructor configuration.
    pub fn config(&self) -> &PostgresConfig {
        &self.config
    }
    /// Return true until publication or after terminal close.
    pub fn is_closed(&self) -> bool {
        !matches!(*self.state.lock().unwrap(), SessionState::Ready { .. })
    }
    /// Snapshot committed parameters, including after close.
    pub fn parameters(&self) -> HashMap<String, String> {
        match &*self.state.lock().unwrap() {
            SessionState::Ready {
                params_snapshot, ..
            }
            | SessionState::Closed { params_snapshot } => params_snapshot.clone(),
            _ => HashMap::new(),
        }
    }
    /// Reserve one connection attempt and its total deadline.
    pub fn begin_connect(&self) -> Result<BeginConnect, PostgresError> {
        let mut state = self.state.lock().unwrap();
        match &*state {
            SessionState::Configured => {
                let deadline = Instant::now()
                    .checked_add(self.config.connect_timeout)
                    .ok_or_else(|| {
                        PostgresError::Config("connect_timeout exceeds clock range".into())
                    })?;
                *state = SessionState::Connecting;
                Ok(BeginConnect::Attempt(ConnectAttempt {
                    handle: self.clone(),
                    deadline,
                    armed: true,
                }))
            }
            SessionState::Ready { .. } => Ok(BeginConnect::AlreadyReady),
            _ => Err(PostgresError::Interface(
                "session is connecting or closed".into(),
            )),
        }
    }
    /// Publish closed before a best-effort, bounded Terminate exchange.
    pub async fn close(&self) -> Result<(), PostgresError> {
        let conn = {
            let mut state = self.state.lock().unwrap();
            if matches!(*state, SessionState::Connecting) {
                return Err(PostgresError::Interface("session is connecting".into()));
            }
            match std::mem::replace(
                &mut *state,
                SessionState::Closed {
                    params_snapshot: HashMap::new(),
                },
            ) {
                SessionState::Ready {
                    conn,
                    params_snapshot,
                } => {
                    *state = SessionState::Closed { params_snapshot };
                    Some(conn)
                }
                SessionState::Closed { params_snapshot } => {
                    *state = SessionState::Closed { params_snapshot };
                    None
                }
                _ => None,
            }
        };
        if let Some(mut conn) = conn {
            let _ = tokio::time::timeout(Duration::from_millis(100), async {
                conn.stream.write_all(&protocol::TERMINATE_FRAME).await?;
                conn.stream.flush().await?;
                conn.stream.shutdown().await
            })
            .await;
        }
        Ok(())
    }
    fn rollback(&self) {
        *self.state.lock().unwrap() = SessionState::Closed {
            params_snapshot: HashMap::new(),
        };
    }
}

/// A fresh owned attempt or an already published session.
pub enum BeginConnect {
    Attempt(ConnectAttempt),
    AlreadyReady,
}

/// An armed reservation that closes terminally when abandoned.
pub struct ConnectAttempt {
    handle: SessionHandle,
    deadline: Instant,
    armed: bool,
}
impl ConnectAttempt {
    /// Dial and authenticate within the reservation's original deadline.
    pub async fn run(mut self) -> Result<PendingSession, PostgresError> {
        let conn =
            tokio::time::timeout_at(self.deadline, startup(self.handle.config(), self.deadline))
                .await
                .map_err(|_| PostgresError::Transport("connection deadline elapsed".into()))??;
        let pending = PendingSession {
            handle: self.handle.clone(),
            deadline: self.deadline,
            armed: true,
            conn: Some(conn),
        };
        self.armed = false;
        Ok(pending)
    }
}
impl Drop for ConnectAttempt {
    fn drop(&mut self) {
        if self.armed {
            self.handle.rollback();
        }
    }
}

/// Authenticated socket awaiting explicit adoption by its caller.
pub struct PendingSession {
    handle: SessionHandle,
    deadline: Instant,
    armed: bool,
    conn: Option<PostgresConnection>,
}
impl PendingSession {
    /// Inspect authenticated metadata before publication.
    pub fn connection(&self) -> &PostgresConnection {
        self.conn.as_ref().unwrap()
    }
    /// Atomically publish a session while the original deadline remains valid.
    pub fn commit(mut self) -> Result<(), PostgresError> {
        let mut state = self.handle.state.lock().unwrap();
        if Instant::now() > self.deadline {
            return Err(PostgresError::Transport(
                "connection deadline elapsed before publication".into(),
            ));
        }
        if !matches!(*state, SessionState::Connecting) {
            return Err(PostgresError::Interface(
                "session reservation is closed".into(),
            ));
        }
        let conn = self.conn.take().unwrap();
        let params_snapshot = conn.parameters.clone();
        *state = SessionState::Ready {
            conn,
            params_snapshot,
        };
        self.armed = false;
        Ok(())
    }
}
impl Drop for PendingSession {
    fn drop(&mut self) {
        if self.armed {
            self.handle.rollback();
        }
    }
}

async fn startup(
    config: &PostgresConfig,
    deadline: Instant,
) -> Result<PostgresConnection, PostgresError> {
    if config.unix_socket.is_some()
        || config.cloud_sql_instance.is_some()
        || config.alloydb_instance_uri.is_some()
        || config.enable_iam_auth
    {
        return Err(PostgresError::UnsupportedFeature(
            "only direct TCP is available".into(),
        ));
    }
    let tls::DialedTransport {
        mut stream,
        tls_server_end_point,
    } = tls::connect(config, deadline).await?;
    stream
        .write_all(&protocol::encode_startup_frame(config)?)
        .await
        .map_err(|error| PostgresError::Transport(error.to_string()))?;
    stream
        .flush()
        .await
        .map_err(|error| PostgresError::Transport(error.to_string()))?;
    let mut conn = PostgresConnection {
        stream,
        parameters: HashMap::new(),
        backend_key: None,
        transaction_status: TransactionStatus::Idle,
        tls_server_end_point,
    };
    let mut auth = Authentication::new();
    let mut out = BytesMut::new();
    loop {
        let message = protocol::read_message(&mut conn.stream).await?;
        if auth.handle_message(&message, config, &mut out)? {
            if !out.is_empty() {
                conn.stream
                    .write_all(&out)
                    .await
                    .map_err(|error| PostgresError::Transport(error.to_string()))?;
                conn.stream
                    .flush()
                    .await
                    .map_err(|error| PostgresError::Transport(error.to_string()))?;
                out.clear();
            }
            continue;
        }
        match message {
            Message::ParameterStatus(body) if auth.is_complete() => {
                let name = body
                    .name()
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                let value = body
                    .value()
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                conn.parameters.insert(name.into(), value.into());
            }
            Message::BackendKeyData(body) if auth.is_complete() && conn.backend_key.is_none() => {
                conn.backend_key = Some(BackendKey {
                    process_id: body.process_id(),
                    secret_key: body.secret_key(),
                });
            }
            Message::ReadyForQuery(body) if auth.is_complete() => {
                conn.transaction_status = TransactionStatus::from_wire_byte(body.status())?;
                if conn.transaction_status != TransactionStatus::Idle
                    || conn.parameters.get("client_encoding").map(String::as_str) != Some("UTF8")
                {
                    return Err(PostgresError::Protocol(
                        "startup requires UTF8 and idle transaction status".into(),
                    ));
                }
                return Ok(conn);
            }
            Message::NoticeResponse(_) => {}
            Message::ErrorResponse(body) => return Err(server_error(body)?),
            _ => {
                return Err(PostgresError::Protocol(
                    "unexpected startup message or order".into(),
                ));
            }
        }
    }
}

fn server_error(
    body: postgres_protocol::message::backend::ErrorResponseBody,
) -> Result<PostgresError, PostgresError> {
    use fallible_iterator::FallibleIterator;
    let fields = body
        .fields()
        .map(|field| {
            Ok((
                field.type_(),
                std::str::from_utf8(field.value_bytes())
                    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
                    .to_owned(),
            ))
        })
        .collect::<Vec<_>>()
        .map_err(|error| PostgresError::Protocol(error.to_string()))?;
    let field = |tag| {
        fields
            .iter()
            .find(|(key, _)| *key == tag)
            .map(|(_, value)| value.as_str())
    };
    let severity = field(b'V')
        .or_else(|| field(b'S'))
        .ok_or_else(|| PostgresError::Protocol("missing server severity".into()))?
        .to_owned();
    let sqlstate = field(b'C')
        .ok_or_else(|| PostgresError::Protocol("missing server SQLSTATE".into()))?
        .to_owned();
    let message = field(b'M')
        .ok_or_else(|| PostgresError::Protocol("missing server message".into()))?
        .to_owned();
    PostgresError::server(severity, &sqlstate, message, fields)
}
