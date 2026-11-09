//! Generation-fenced PostgreSQL `CancelRequest` engine, owned 32-bit / variable-length
//! cancellation secrets, and mandatory cancel-sent connection retirement.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{BufMut, BytesMut};
use postgres_protocol::message::frontend;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;
use vertebra_db::connection::{BoxFuture, CancelHandle, Connection, TransportError};

use crate::cloud;
use crate::cloud::material::CloudConnector;
use crate::config::PostgresConfig;
use crate::errors::PostgresError;
use crate::session::{
    BackendKey, CancelDialHook, PostgresConnection, SessionHandle, SessionShared, dial_unix_socket,
};
use crate::tls;

/// PostgreSQL `CancelRequest` magic request code (`1234 << 16 | 5678 == 80877102 == 0x04d2_162e`).
pub const CANCEL_REQUEST_CODE: i32 = 80_877_102;

/// Maximum variable-length cancellation secret length permitted by PostgreSQL Protocol 3.2 (`256` bytes).
pub const MAX_VARIABLE_CANCEL_SECRET_BYTES: usize = 256;

/// Monotonic per-session operation generation fencing out-of-band and `asyncio` cancellation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationGeneration(pub u64);

/// Rust-private PostgreSQL cancellation secret supporting Protocol 3.0 `32`-bit keys and
/// Protocol 3.2 variable-length (up to `256`-byte / `256`-bit+) keys without Python exposure.
#[derive(Clone, PartialEq, Eq)]
pub enum CancelSecret {
    /// Canonical Protocol 3.0 4-byte `BackendKeyData` secret key.
    V30(i32),
    /// Variable-length Protocol 3.2 `BackendKeyData` secret bytes (`1..=256` bytes).
    Variable(Box<[u8]>),
}

impl std::fmt::Debug for CancelSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V30(_) => f.write_str("CancelSecret::V30(<redacted>)"),
            Self::Variable(bytes) => {
                write!(
                    f,
                    "CancelSecret::Variable(<redacted {} bytes>)",
                    bytes.len()
                )
            }
        }
    }
}

/// Rust-private `(process_id, secret)` pair retained on an authenticated connection.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CancelKeyData {
    pub(crate) process_id: i32,
    pub(crate) secret: CancelSecret,
}

impl std::fmt::Debug for CancelKeyData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelKeyData")
            .field("process_id", &self.process_id)
            .field("secret", &self.secret)
            .finish()
    }
}

impl From<BackendKey> for CancelKeyData {
    fn from(key: BackendKey) -> Self {
        Self {
            process_id: key.process_id,
            secret: CancelSecret::V30(key.secret_key),
        }
    }
}

/// Encode a Protocol 3.0 (`16`-byte) or Protocol 3.2 variable-length (`12 + secret.len()`)
/// `CancelRequest` packet into `buf` with zero `StartupMessage` or password bytes.
pub fn encode_cancel_request_frame(
    process_id: i32,
    secret: &CancelSecret,
    buf: &mut BytesMut,
) -> Result<(), PostgresError> {
    match secret {
        CancelSecret::V30(key) => {
            frontend::cancel_request(process_id, *key, buf);
            Ok(())
        }
        CancelSecret::Variable(bytes) => {
            if bytes.is_empty() || bytes.len() > MAX_VARIABLE_CANCEL_SECRET_BYTES {
                return Err(PostgresError::Protocol(format!(
                    "variable-length cancellation key must be 1..={MAX_VARIABLE_CANCEL_SECRET_BYTES} bytes, got {}",
                    bytes.len()
                )));
            }
            let frame_len = 12_i32
                .checked_add(i32::try_from(bytes.len()).map_err(|_| {
                    PostgresError::Protocol("cancellation key length overflow".to_string())
                })?)
                .ok_or_else(|| {
                    PostgresError::Protocol("CancelRequest frame length overflow".to_string())
                })?;
            buf.reserve(4 + 4 + 4 + bytes.len());
            buf.put_i32(frame_len);
            buf.put_i32(CANCEL_REQUEST_CODE);
            buf.put_i32(process_id);
            buf.put_slice(bytes);
            Ok(())
        }
    }
}

/// Outcome of an out-of-band or `asyncio`-triggered cancellation request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelDisposition {
    /// Session was idle, closed, or the target operation generation had already completed.
    IdleNoOp,
    /// Reserved operation generation was cancelled before touching the wire (`0` packets sent).
    CancelledBeforeSend,
    /// Active operation was marked for drain/retirement, but the backend omitted `BackendKeyData`.
    MissingBackendKey,
    /// `CancelRequest` packet was sent and flushed over a fresh transport connection.
    Sent,
    /// `CancelRequest` was sent and the original query stream drained cleanly to `ReadyForQuery`.
    SentAndDrained,
    /// `CancelRequest` was sent and the session retired after a drain/transport error.
    SentAndRetiredOnError,
    /// Fresh cancel transport dial or write failed; original session still retires boundedly.
    TransportError,
}

#[derive(Debug)]
pub(crate) struct StandaloneCancelState {
    pub(crate) config: PostgresConfig,
    pub(crate) cloud_connector: Arc<CloudConnector>,
    pub(crate) key: Option<CancelKeyData>,
    pub(crate) in_exchange: AtomicBool,
    pub(crate) cancel_sent: AtomicBool,
}

#[derive(Clone)]
enum CancelTarget {
    Session {
        shared: Arc<SessionShared>,
        fenced_generation: Option<OperationGeneration>,
    },
    Standalone(Arc<StandaloneCancelState>),
}

/// Thread-safe, generation-fenced PostgreSQL `CancelRequest` handle implementing [`CancelHandle`].
#[derive(Clone)]
pub struct PostgresCancelHandle {
    target: CancelTarget,
}

impl std::fmt::Debug for PostgresCancelHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.target {
            CancelTarget::Session {
                fenced_generation, ..
            } => f
                .debug_struct("PostgresCancelHandle")
                .field("kind", &"session")
                .field("fenced_generation", fenced_generation)
                .finish(),
            CancelTarget::Standalone(_) => f
                .debug_struct("PostgresCancelHandle")
                .field("kind", &"standalone")
                .finish(),
        }
    }
}

impl PostgresCancelHandle {
    pub(crate) fn from_session(
        shared: Arc<SessionShared>,
        fenced_generation: Option<OperationGeneration>,
    ) -> Self {
        Self {
            target: CancelTarget::Session {
                shared,
                fenced_generation,
            },
        }
    }

    pub(crate) fn from_standalone(state: Arc<StandaloneCancelState>) -> Self {
        Self {
            target: CancelTarget::Standalone(state),
        }
    }

    /// Claim cancellation for the active (or fenced) operation generation and dial a fresh
    /// endpoint/TLS/cloud transport to send `CancelRequest` under `connect_timeout`.
    pub async fn cancel_with_disposition(&self) -> CancelDisposition {
        let deadline = match &self.target {
            CancelTarget::Session { shared, .. } => Instant::now() + shared.config.connect_timeout,
            CancelTarget::Standalone(st) => Instant::now() + st.config.connect_timeout,
        };
        self.cancel_until(deadline).await
    }

    pub(crate) async fn cancel_until(&self, deadline: Instant) -> CancelDisposition {
        match &self.target {
            CancelTarget::Session {
                shared,
                fenced_generation,
            } => cancel_session_until(shared, *fenced_generation, deadline).await,
            CancelTarget::Standalone(state) => {
                if !state.in_exchange.load(Ordering::Acquire) {
                    return CancelDisposition::IdleNoOp;
                }
                if state.cancel_sent.swap(true, Ordering::AcqRel) {
                    return CancelDisposition::IdleNoOp;
                }
                let Some(ref key) = state.key else {
                    return CancelDisposition::MissingBackendKey;
                };
                match send_cancel_request_until(
                    &state.config,
                    &state.cloud_connector,
                    key,
                    deadline,
                    None,
                )
                .await
                {
                    Ok(()) => CancelDisposition::Sent,
                    Err(_) => CancelDisposition::TransportError,
                }
            }
        }
    }
}

impl CancelHandle for PostgresCancelHandle {
    fn cancel(&self) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            match self.cancel_with_disposition().await {
                CancelDisposition::TransportError => Err(TransportError::Io(std::io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "PostgreSQL CancelRequest transport failed",
                ))),
                _ => Ok(()),
            }
        })
    }
}

pub(crate) async fn cancel_session_until(
    shared: &Arc<SessionShared>,
    fenced_generation: Option<OperationGeneration>,
    deadline: Instant,
) -> CancelDisposition {
    let claimed = SessionHandle::claim_cancel_on_shared(shared, fenced_generation);
    match claimed {
        SessionCancelClaim::IdleNoOp => CancelDisposition::IdleNoOp,
        SessionCancelClaim::CancelledBeforeSend => CancelDisposition::CancelledBeforeSend,
        SessionCancelClaim::MissingBackendKey => CancelDisposition::MissingBackendKey,
        SessionCancelClaim::Send { key, hook } => {
            match send_cancel_request_until(
                &shared.config,
                &shared.cloud_connector,
                &key,
                deadline,
                hook,
            )
            .await
            {
                Ok(()) => CancelDisposition::Sent,
                Err(_) => CancelDisposition::TransportError,
            }
        }
    }
}

pub(crate) enum SessionCancelClaim {
    IdleNoOp,
    CancelledBeforeSend,
    MissingBackendKey,
    Send {
        key: CancelKeyData,
        hook: Option<CancelDialHook>,
    },
}

/// Dial a fresh transport to the exact validated original endpoint (`TCP`/`TLS`, Unix socket,
/// Cloud SQL `:3307`, or AlloyDB `:5433` + `MetadataExchange`) with zero `StartupMessage` or
/// password/direct-IAM token generation, write `CancelRequest`, flush, and close under `deadline`.
pub(crate) async fn send_cancel_request_until(
    config: &PostgresConfig,
    connector: &CloudConnector,
    key: &CancelKeyData,
    deadline: Instant,
    pre_dial_hook: Option<CancelDialHook>,
) -> Result<(), PostgresError> {
    let mut frame = BytesMut::with_capacity(64);
    encode_cancel_request_frame(key.process_id, &key.secret, &mut frame)?;

    if let Some(hook) = pre_dial_hook {
        let _ = tokio::task::spawn_blocking(move || hook()).await;
    }

    if Instant::now() >= deadline {
        return Err(PostgresError::Transport(
            "CancelRequest deadline exceeded before dial".to_string(),
        ));
    }

    let mut stream = if config.cloud_sql_instance.is_some() {
        cloud::dial_cloudsql(config, connector, deadline)
            .await?
            .stream
    } else if config.alloydb_instance_uri.is_some() {
        cloud::dial_alloydb(config, connector, deadline)
            .await?
            .stream
    } else if let Some(ref unix_path) = config.unix_socket {
        dial_unix_socket(unix_path, deadline).await?
    } else {
        tls::connect(config, deadline).await?.stream
    };

    if Instant::now() >= deadline {
        return Err(PostgresError::Transport(
            "CancelRequest deadline exceeded before write".to_string(),
        ));
    }

    tokio::time::timeout_at(deadline, async {
        stream.write_all(&frame).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| PostgresError::Transport("CancelRequest write timed out".to_string()))?
    .map_err(|e| PostgresError::Transport(format!("CancelRequest write failed: {e}")))?;

    let _ = tokio::time::timeout_at(deadline, stream.shutdown()).await;
    Ok(())
}

fn pg_err_to_transport(err: PostgresError) -> TransportError {
    match err {
        PostgresError::Server(srv) if srv.sqlstate == "57014" => TransportError::Cancelled,
        PostgresError::Config(msg) | PostgresError::Interface(msg) | PostgresError::Data(msg) => {
            TransportError::InvalidConfig(msg)
        }
        other => TransportError::Io(std::io::Error::other(other.to_string())),
    }
}

impl Connection for PostgresConnection {
    fn ping(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            self.simple_query("")
                .await
                .map(|_| ())
                .map_err(pg_err_to_transport)
        })
    }

    fn is_broken(&self) -> bool {
        PostgresConnection::is_broken(self) || self.has_cancel_sent()
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(self.postgres_cancel_handle())
    }

    fn close(mut self) -> BoxFuture<'static, Result<(), TransportError>> {
        Box::pin(async move {
            PostgresConnection::close(&mut self)
                .await
                .map_err(pg_err_to_transport)
        })
    }
}

impl Connection for SessionHandle {
    fn ping(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
        Box::pin(async move {
            self.simple_query_shared("")
                .await
                .map(|_| ())
                .map_err(pg_err_to_transport)
        })
    }

    fn is_broken(&self) -> bool {
        self.is_closed() || self.will_retire()
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(self.postgres_cancel_handle())
    }

    fn close(mut self) -> BoxFuture<'static, Result<(), TransportError>> {
        Box::pin(async move {
            SessionHandle::close(&mut self)
                .await
                .map_err(pg_err_to_transport)
        })
    }
}
