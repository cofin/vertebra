//! Shared async transport stream (`TCP`, `tokio-rustls`, Unix socket), Cloud SQL (`:3307`) and
//! AlloyDB (`:5433`) mTLS handshakes, and the [`Connection`] / [`CancelHandle`] traits.

use std::future::Future;
use std::io::{self, BufReader, ErrorKind};
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::dialer::{
    AlloyDbClusterUri, AuthType, CloudSqlInstanceUri, exchange_alloydb_metadata_over_stream,
};

/// Boxed `Send` future alias used by [`CancelHandle`] and [`Connection`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Install `rustls`' `ring` `CryptoProvider` as the process default if none is installed yet.
pub fn install_rustls_ring_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Transport-layer TLS negotiation and certificate verification policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TlsMode {
    /// Never negotiate TLS; transmit plaintext frames over the socket.
    Disable,
    /// Prefer TLS when negotiated by the wire protocol, falling back to plaintext otherwise.
    #[default]
    Prefer,
    /// Require an encrypted TLS channel; verify the CA chain when `ca_pem` is supplied.
    Require,
    /// Require TLS and verify the server certificate chain against trusted root CAs without hostname matching.
    VerifyCa,
    /// Require TLS and verify both the server certificate chain and the target server hostname/IP.
    VerifyFull,
}

impl TlsMode {
    /// Parse a TLS mode string (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`).
    pub fn parse(raw: &str) -> Result<Self, TransportError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disable" | "false" | "off" => Ok(Self::Disable),
            "prefer" | "allow" => Ok(Self::Prefer),
            "require" | "true" | "on" => Ok(Self::Require),
            "verify-ca" | "verify_ca" => Ok(Self::VerifyCa),
            "verify-full" | "verify_full" => Ok(Self::VerifyFull),
            other => Err(TransportError::InvalidConfig(format!(
                "unsupported TLS mode `{other}`"
            ))),
        }
    }

    /// Return `true` when [`TransportStream::connect`] performs an immediate TLS handshake.
    pub const fn requires_immediate_tls(self) -> bool {
        matches!(self, Self::Require | Self::VerifyCa | Self::VerifyFull)
    }
}

/// Transport error classified by transience and SQLSTATE (`08xxx` / `57014`).
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("I/O transport error: {0}")]
    Io(#[from] io::Error),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("connection timed out after {0:?}")]
    Timeout(Duration),
    #[error("cloud dialer handshake failed: {0}")]
    Dialer(String),
    #[error("invalid connection configuration: {0}")]
    InvalidConfig(String),
    #[error("operation cancelled")]
    Cancelled,
    #[error("connection closed")]
    Closed,
}

impl TransportError {
    /// Return `true` when retrying on a fresh connection may succeed without caller changes.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Timeout(_) | Self::Closed => true,
            Self::Io(err) => matches!(
                err.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::TimedOut
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::WouldBlock
                    | ErrorKind::Interrupted
            ),
            Self::Tls(_) | Self::Dialer(_) | Self::InvalidConfig(_) | Self::Cancelled => false,
        }
    }

    /// Return the canonical 5-character SQLSTATE code associated with this transport failure.
    pub const fn sqlstate(&self) -> Option<&'static str> {
        match self {
            Self::Io(_) | Self::Timeout(_) | Self::Tls(_) | Self::Dialer(_) => Some("08001"),
            Self::Closed => Some("08006"),
            Self::Cancelled => Some("57014"),
            Self::InvalidConfig(_) => None,
        }
    }
}

/// Expected server certificate identity for Cloud SQL (`:3307`) or AlloyDB (`:5433`) TLS 1.3 mTLS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudPeerIdentity {
    /// Cloud SQL `:3307`: try SAN verification against `server_name` when `Some`, falling back to Subject `CN == expected_cn`.
    CloudSql {
        server_name: Option<String>,
        expected_cn: String,
    },
    /// AlloyDB `:5433`: require SAN verification against `expected_san` (`instanceIp` or `pscDnsName`), never falling back to Subject `CN` or CA-only.
    AlloyDb { expected_san: String },
}

/// TCP keepalive parameters applied to a socket before protocol startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TcpKeepaliveConfig {
    pub enabled: bool,
    pub idle: Option<Duration>,
    pub interval: Option<Duration>,
    pub retries: Option<u32>,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            idle: None,
            interval: None,
            retries: None,
        }
    }
}

/// Network or IPC endpoint targeted by [`ConnectOptions`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointTarget {
    /// Direct TCP `host:port` endpoint.
    Tcp { host: String, port: u16 },
    /// Local Unix domain socket path.
    Unix { path: PathBuf },
    /// Google Cloud SQL `:3307` mTLS proxy endpoint verifying server SAN or Subject `CN`.
    CloudSql {
        instance: CloudSqlInstanceUri,
        addr: SocketAddr,
        server_ca_pem: Vec<u8>,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
        peer_identity: CloudPeerIdentity,
    },
    /// Google Cloud AlloyDB `:5433` mTLS + `MetadataExchangeRequest` endpoint.
    AlloyDb {
        cluster: AlloyDbClusterUri,
        addr: SocketAddr,
        server_ca_pem: Vec<u8>,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
        user_agent: String,
        oauth2_token: String,
        auth_type: AuthType,
        peer_identity: CloudPeerIdentity,
    },
}

/// Connection dial and TLS parameters consumed by [`TransportStream::connect`].
#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub target: EndpointTarget,
    pub tls_mode: TlsMode,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub server_name: Option<String>,
    pub connect_timeout: Duration,
    pub tcp_nodelay: bool,
    pub application_name: Option<String>,
}

impl ConnectOptions {
    /// Construct default TCP connection options (`TlsMode::Disable`, `tcp_nodelay = true`, `10s` timeout).
    pub fn tcp(host: impl Into<String>, port: u16) -> Self {
        Self {
            target: EndpointTarget::Tcp {
                host: host.into(),
                port,
            },
            tls_mode: TlsMode::Disable,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tcp_nodelay: true,
            application_name: None,
        }
    }

    /// Construct Unix domain socket connection options.
    pub fn unix(path: impl Into<PathBuf>) -> Self {
        Self {
            target: EndpointTarget::Unix { path: path.into() },
            tls_mode: TlsMode::Disable,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tcp_nodelay: false,
            application_name: None,
        }
    }

    /// Set the transport [`TlsMode`].
    pub fn with_tls_mode(mut self, mode: TlsMode) -> Self {
        self.tls_mode = mode;
        self
    }

    /// Configure a PEM-encoded root CA bundle for server certificate verification.
    pub fn with_ca_pem(mut self, ca_pem: impl Into<Vec<u8>>) -> Self {
        self.ca_pem = Some(ca_pem.into());
        self
    }

    /// Configure a PEM-encoded client certificate chain and private key for mTLS.
    pub fn with_client_identity(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert_pem = Some(cert_pem.into());
        self.client_key_pem = Some(key_pem.into());
        self
    }

    /// Override the SNI / hostname verified during `VerifyFull` TLS handshakes.
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }

    /// Override the connection establishment timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }
}

/// Unified async byte stream over TCP, `tokio-rustls` TLS, and Unix domain sockets.
#[derive(Debug)]
pub enum TransportStream {
    PlainTcp(TcpStream),
    TlsTcp(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

impl TransportStream {
    /// Return `true` when this stream is encrypted with `tokio-rustls`.
    pub const fn is_tls(&self) -> bool {
        matches!(self, Self::TlsTcp(_))
    }

    /// Return the raw Unix file descriptor when backed by a plain TCP or Unix socket.
    #[cfg(unix)]
    pub fn raw_fd_if_plain(&self) -> Option<RawFd> {
        match self {
            Self::PlainTcp(tcp) => Some(tcp.as_raw_fd()),
            Self::Unix(unix) => Some(unix.as_raw_fd()),
            Self::TlsTcp(_) => None,
        }
    }

    /// Perform a non-blocking write on a plain TCP or Unix socket without entering Tokio.
    ///
    /// Returns `None` for TLS streams, which stay on the async Tokio path.
    pub fn try_write_plain(&self, buf: &[u8]) -> Option<io::Result<usize>> {
        match self {
            Self::PlainTcp(tcp) => Some(tcp.try_write(buf)),
            #[cfg(unix)]
            Self::Unix(unix) => Some(unix.try_write(buf)),
            Self::TlsTcp(_) => None,
        }
    }

    /// Perform a non-blocking read into `buf` on a plain TCP or Unix socket without entering Tokio.
    ///
    /// Returns `None` for TLS streams, which stay on the async Tokio path.
    pub fn try_read_buf_plain<B: bytes::BufMut>(&self, buf: &mut B) -> Option<io::Result<usize>> {
        match self {
            Self::PlainTcp(tcp) => Some(tcp.try_read_buf(buf)),
            #[cfg(unix)]
            Self::Unix(unix) => Some(unix.try_read_buf(buf)),
            Self::TlsTcp(_) => None,
        }
    }

    /// Return the remote TCP `SocketAddr` when backed by TCP or TLS-over-TCP.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::PlainTcp(tcp) => tcp.peer_addr().ok(),
            Self::TlsTcp(tls) => tls.get_ref().0.peer_addr().ok(),
            #[cfg(unix)]
            Self::Unix(_) => None,
        }
    }

    /// Return the raw DER bytes of the verified peer leaf certificate when connected over TLS.
    pub fn peer_leaf_der(&self) -> Option<&[u8]> {
        match self {
            Self::TlsTcp(tls) => tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .map(CertificateDer::as_ref),
            Self::PlainTcp(_) => None,
            #[cfg(unix)]
            Self::Unix(_) => None,
        }
    }

    /// Apply TCP keepalive socket options (`SO_KEEPALIVE`, `TCP_KEEPIDLE`/`TCP_KEEPALIVE`,
    /// `TCP_KEEPINTVL`, `TCP_KEEPCNT`) to the underlying TCP socket.
    pub fn set_tcp_keepalive(&self, config: TcpKeepaliveConfig) -> Result<(), TransportError> {
        if !config.enabled
            && (config.idle.is_some() || config.interval.is_some() || config.retries.is_some())
        {
            return Err(TransportError::InvalidConfig(
                "keepalive idle/interval/retries cannot be set when keepalives are disabled"
                    .to_string(),
            ));
        }
        if config.idle.is_some_and(|d| d.is_zero()) {
            return Err(TransportError::InvalidConfig(
                "keepalives_idle must be greater than zero".to_string(),
            ));
        }
        if config.interval.is_some_and(|d| d.is_zero()) {
            return Err(TransportError::InvalidConfig(
                "keepalives_interval must be greater than zero".to_string(),
            ));
        }
        if config.retries == Some(0) {
            return Err(TransportError::InvalidConfig(
                "keepalives_count must be greater than zero".to_string(),
            ));
        }

        let tcp = match self {
            Self::PlainTcp(tcp) => tcp,
            Self::TlsTcp(tls) => tls.get_ref().0,
            #[cfg(unix)]
            Self::Unix(_) => {
                return Err(TransportError::InvalidConfig(
                    "TCP keepalives cannot be applied to a Unix domain socket".to_string(),
                ));
            }
        };

        let sock_ref = socket2::SockRef::from(tcp);
        sock_ref.set_keepalive(config.enabled)?;
        if config.enabled
            && (config.idle.is_some() || config.interval.is_some() || config.retries.is_some())
        {
            let mut ka = socket2::TcpKeepalive::new();
            if let Some(idle) = config.idle {
                ka = ka.with_time(idle);
            }
            if let Some(interval) = config.interval {
                ka = ka.with_interval(interval);
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "tvos",
                target_os = "watchos",
            ))]
            if let Some(retries) = config.retries {
                ka = ka.with_retries(retries);
            }
            sock_ref.set_tcp_keepalive(&ka)?;
        }
        Ok(())
    }

    /// Establish a transport connection (including TLS and Cloud SQL / AlloyDB handshakes when configured).
    pub async fn connect(options: &ConnectOptions) -> Result<Self, TransportError> {
        let timeout = options.connect_timeout;
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::time::timeout_at(deadline, Self::connect_inner(options, deadline))
            .await
            .map_err(|_| TransportError::Timeout(timeout))?
    }

    /// Establish only the TCP + TLS 1.3 mTLS channel for an [`EndpointTarget::AlloyDb`] target
    /// without sending `MetadataExchangeRequest`, allowing callers to validate or refresh login
    /// credentials before calling [`finish_alloydb_metadata`].
    pub async fn connect_alloydb_tls(options: &ConnectOptions) -> Result<Self, TransportError> {
        let timeout = options.connect_timeout;
        tokio::time::timeout(timeout, Self::connect_alloydb_tls_inner(options))
            .await
            .map_err(|_| TransportError::Timeout(timeout))?
    }

    async fn connect_alloydb_tls_inner(options: &ConnectOptions) -> Result<Self, TransportError> {
        let EndpointTarget::AlloyDb {
            addr,
            server_ca_pem,
            client_cert_pem,
            client_key_pem,
            peer_identity,
            ..
        } = &options.target
        else {
            return Err(TransportError::InvalidConfig(
                "connect_alloydb_tls requires EndpointTarget::AlloyDb".to_string(),
            ));
        };
        let tcp = TcpStream::connect(*addr).await?;
        if options.tcp_nodelay {
            tcp.set_nodelay(true)?;
        }
        let client_cfg = build_cloud_mtls_config(
            server_ca_pem,
            client_cert_pem,
            client_key_pem,
            peer_identity.clone(),
        )?;
        let server_name = cloud_sni_server_name(peer_identity, *addr);
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        Ok(Self::TlsTcp(Box::new(tls_stream)))
    }

    /// Upgrade an existing plain [`TcpStream`] to TLS using `options` (for protocol `SSLRequest` / `STARTTLS`).
    pub async fn upgrade_tls(
        stream: TcpStream,
        default_host: &str,
        options: &ConnectOptions,
    ) -> Result<Self, TransportError> {
        let effective_mode = match options.tls_mode {
            TlsMode::Disable => {
                return Err(TransportError::InvalidConfig(
                    "cannot upgrade to TLS when TlsMode::Disable is set".to_string(),
                ));
            }
            TlsMode::Prefer => TlsMode::Require,
            other => other,
        };
        let sni_host = options.server_name.as_deref().unwrap_or(default_host);
        let client_cfg = build_standard_tls_config(
            effective_mode,
            options.ca_pem.as_deref(),
            options.client_cert_pem.as_deref(),
            options.client_key_pem.as_deref(),
        )?;
        let server_name = parse_server_name(sni_host)?;
        let connector = TlsConnector::from(Arc::new(client_cfg));
        let tls_stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| TransportError::Tls(e.to_string()))?;
        Ok(Self::TlsTcp(Box::new(tls_stream)))
    }

    async fn connect_inner(
        options: &ConnectOptions,
        absolute_deadline: tokio::time::Instant,
    ) -> Result<Self, TransportError> {
        match &options.target {
            EndpointTarget::Tcp { host, port } => {
                let tcp = TcpStream::connect((host.as_str(), *port)).await?;
                if options.tcp_nodelay {
                    tcp.set_nodelay(true)?;
                }
                if options.tls_mode.requires_immediate_tls() {
                    Self::upgrade_tls(tcp, host, options).await
                } else {
                    Ok(Self::PlainTcp(tcp))
                }
            }
            EndpointTarget::Unix { path } => {
                #[cfg(unix)]
                {
                    let stream = tokio::net::UnixStream::connect(path).await?;
                    Ok(Self::Unix(stream))
                }
                #[cfg(not(unix))]
                {
                    let _ = path;
                    Err(TransportError::InvalidConfig(
                        "Unix domain sockets are not supported on this platform".to_string(),
                    ))
                }
            }
            EndpointTarget::CloudSql {
                instance: _,
                addr,
                server_ca_pem,
                client_cert_pem,
                client_key_pem,
                peer_identity,
            } => {
                let tcp = TcpStream::connect(*addr).await?;
                if options.tcp_nodelay {
                    tcp.set_nodelay(true)?;
                }
                let client_cfg = build_cloud_mtls_config(
                    server_ca_pem,
                    client_cert_pem,
                    client_key_pem,
                    peer_identity.clone(),
                )?;
                let server_name = cloud_sni_server_name(peer_identity, *addr);
                let connector = TlsConnector::from(Arc::new(client_cfg));
                let tls_stream = connector
                    .connect(server_name, tcp)
                    .await
                    .map_err(|e| TransportError::Tls(e.to_string()))?;
                Ok(Self::TlsTcp(Box::new(tls_stream)))
            }
            EndpointTarget::AlloyDb { .. } => {
                let mut stream = Self::connect_alloydb_tls_inner(options).await?;
                finish_alloydb_metadata(&mut stream, options, absolute_deadline).await?;
                Ok(stream)
            }
        }
    }
}

/// Complete the AlloyDB `:5433` `MetadataExchangeRequest` / `MetadataExchangeResponse` frame
/// exchange over an established [`TransportStream`] (after [`TransportStream::connect_alloydb_tls`])
/// within `absolute_deadline`.
pub async fn finish_alloydb_metadata(
    stream: &mut TransportStream,
    options: &ConnectOptions,
    absolute_deadline: tokio::time::Instant,
) -> Result<(), TransportError> {
    let EndpointTarget::AlloyDb {
        user_agent,
        oauth2_token,
        auth_type,
        ..
    } = &options.target
    else {
        return Err(TransportError::InvalidConfig(
            "finish_alloydb_metadata requires EndpointTarget::AlloyDb".to_string(),
        ));
    };
    let TransportStream::TlsTcp(tls_stream) = stream else {
        return Err(TransportError::InvalidConfig(
            "finish_alloydb_metadata requires an established TlsTcp stream".to_string(),
        ));
    };
    if tokio::time::Instant::now() >= absolute_deadline {
        return Err(TransportError::Timeout(options.connect_timeout));
    }
    tokio::time::timeout_at(
        absolute_deadline,
        exchange_alloydb_metadata_over_stream(
            tls_stream.as_mut(),
            user_agent,
            oauth2_token,
            *auth_type,
        ),
    )
    .await
    .map_err(|_| TransportError::Timeout(options.connect_timeout))?
    .map_err(TransportError::Dialer)?;
    Ok(())
}

fn cloud_sni_server_name(
    peer_identity: &CloudPeerIdentity,
    addr: SocketAddr,
) -> ServerName<'static> {
    let candidate = match peer_identity {
        CloudPeerIdentity::CloudSql { server_name, .. } => server_name.as_deref(),
        CloudPeerIdentity::AlloyDb { expected_san } => Some(expected_san.as_str()),
    };
    if let Some(raw) = candidate {
        let trimmed = raw.trim().trim_end_matches('.');
        if !trimmed.is_empty()
            && let Ok(sn) = parse_server_name(trimmed)
        {
            return sn;
        }
    }
    ServerName::IpAddress(addr.ip().into())
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::PlainTcp(s) => Pin::new(s).poll_read(cx, buf),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::PlainTcp(s) => Pin::new(s).poll_write(cx, buf),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::PlainTcp(s) => Pin::new(s).poll_flush(cx),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_flush(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::PlainTcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::PlainTcp(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Self::TlsTcp(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
            #[cfg(unix)]
            Self::Unix(s) => Pin::new(s).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::PlainTcp(s) => s.is_write_vectored(),
            Self::TlsTcp(s) => s.is_write_vectored(),
            #[cfg(unix)]
            Self::Unix(s) => s.is_write_vectored(),
        }
    }
}

/// Out-of-band query cancellation handle safe to invoke concurrently while a [`Connection`] is borrowed.
pub trait CancelHandle: Send + Sync {
    /// Signal cancellation for the active operation on the parent connection.
    fn cancel(&self) -> BoxFuture<'_, Result<(), TransportError>>;
}

/// Shared lifecycle contract implemented by native Rust database wire connections.
pub trait Connection: Send {
    /// Execute a lightweight wire-level liveness check.
    fn ping(&mut self) -> BoxFuture<'_, Result<(), TransportError>>;

    /// Return `true` when the transport or protocol state is broken and must not be returned to a pool.
    fn is_broken(&self) -> bool;

    /// Return a thread-safe out-of-band cancellation handle for this connection.
    fn cancel_handle(&self) -> Arc<dyn CancelHandle>;

    /// Gracefully terminate the wire session and close the underlying transport.
    fn close(self) -> BoxFuture<'static, Result<(), TransportError>>
    where
        Self: Sized;
}

fn parse_server_name(host: &str) -> Result<ServerName<'static>, TransportError> {
    let trimmed = host.trim().trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(trimmed.to_string()).map_err(|e| {
        TransportError::InvalidConfig(format!("invalid TLS server name `{host}`: {e}"))
    })
}

fn load_root_store(ca_pem: Option<&[u8]>) -> Result<RootCertStore, TransportError> {
    let mut roots = RootCertStore::empty();
    if let Some(pem) = ca_pem {
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| TransportError::InvalidConfig(format!("invalid CA PEM bundle: {e}")))?;
        if certs.is_empty() {
            return Err(TransportError::InvalidConfig(
                "CA PEM bundle contained zero certificates".to_string(),
            ));
        }
        let (added, _) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(TransportError::InvalidConfig(
                "failed to parse any valid X.509 CA certificate from bundle".to_string(),
            ));
        }
    } else {
        roots.add_parsable_certificates(webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().cloned());
    }
    Ok(roots)
}

fn parse_client_identity(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TransportError> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(cert_pem))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TransportError::InvalidConfig(format!("invalid client cert PEM: {e}")))?;
    if certs.is_empty() {
        return Err(TransportError::InvalidConfig(
            "client certificate PEM contained zero certificates".to_string(),
        ));
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_pem))
        .map_err(|e| TransportError::InvalidConfig(format!("invalid client key PEM: {e}")))?
        .ok_or_else(|| {
            TransportError::InvalidConfig("no private key found in client key PEM".to_string())
        })?;
    Ok((certs, key))
}

fn build_standard_tls_config(
    mode: TlsMode,
    ca_pem: Option<&[u8]>,
    client_cert_pem: Option<&[u8]>,
    client_key_pem: Option<&[u8]>,
) -> Result<ClientConfig, TransportError> {
    install_rustls_ring_provider();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| TransportError::Tls(e.to_string()))?;

    let client_identity = match (client_cert_pem, client_key_pem) {
        (Some(cert), Some(key)) => Some(parse_client_identity(cert, key)?),
        (None, None) => None,
        _ => {
            return Err(TransportError::InvalidConfig(
                "both client_cert_pem and client_key_pem must be provided for mTLS".to_string(),
            ));
        }
    };

    match mode {
        TlsMode::VerifyFull => {
            let roots = load_root_store(ca_pem)?;
            let want_auth = builder.with_root_certificates(roots);
            match client_identity {
                Some((certs, key)) => want_auth
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TransportError::InvalidConfig(format!("invalid mTLS key: {e}"))),
                None => Ok(want_auth.with_no_client_auth()),
            }
        }
        TlsMode::VerifyCa => {
            let roots = Arc::new(load_root_store(ca_pem)?);
            let inner = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
                .build()
                .map_err(|e| TransportError::Tls(e.to_string()))?;
            let verifier = Arc::new(CaAndOptionalCnVerifier {
                inner,
                expected_cn: None,
            });
            let want_auth = builder
                .dangerous()
                .with_custom_certificate_verifier(verifier);
            match client_identity {
                Some((certs, key)) => want_auth
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TransportError::InvalidConfig(format!("invalid mTLS key: {e}"))),
                None => Ok(want_auth.with_no_client_auth()),
            }
        }
        TlsMode::Require | TlsMode::Prefer => {
            if ca_pem.is_some() {
                return build_standard_tls_config(
                    TlsMode::VerifyCa,
                    ca_pem,
                    client_cert_pem,
                    client_key_pem,
                );
            }
            let verifier = Arc::new(EncryptOnlyServerVerifier { provider });
            let want_auth = builder
                .dangerous()
                .with_custom_certificate_verifier(verifier);
            match client_identity {
                Some((certs, key)) => want_auth
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TransportError::InvalidConfig(format!("invalid mTLS key: {e}"))),
                None => Ok(want_auth.with_no_client_auth()),
            }
        }
        TlsMode::Disable => Err(TransportError::InvalidConfig(
            "TLS configuration requested with TlsMode::Disable".to_string(),
        )),
    }
}

fn build_cloud_mtls_config(
    server_ca_pem: &[u8],
    client_cert_pem: &[u8],
    client_key_pem: &[u8],
    peer_identity: CloudPeerIdentity,
) -> Result<ClientConfig, TransportError> {
    install_rustls_ring_provider();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = Arc::new(load_root_store(Some(server_ca_pem))?);
    let inner = WebPkiServerVerifier::builder_with_provider(roots, Arc::clone(&provider))
        .build()
        .map_err(|e| TransportError::Tls(e.to_string()))?;
    let verifier = Arc::new(CloudPeerVerifier {
        inner,
        peer_identity,
    });
    let (certs, key) = parse_client_identity(client_cert_pem, client_key_pem)?;
    ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TransportError::Tls(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_auth_cert(certs, key)
        .map_err(|e| TransportError::InvalidConfig(format!("invalid cloud mTLS identity: {e}")))
}

#[derive(Debug)]
struct CaAndOptionalCnVerifier {
    inner: Arc<WebPkiServerVerifier>,
    expected_cn: Option<String>,
}

impl ServerCertVerifier for CaAndOptionalCnVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(_) => {}
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => {}
            Err(other) => return Err(other),
        }

        if let Some(ref expected) = self.expected_cn {
            let actual = extract_der_common_name(end_entity.as_ref());
            if actual.as_deref() != Some(expected.as_str()) {
                return Err(rustls::Error::InvalidCertificate(
                    CertificateError::ApplicationVerificationFailure,
                ));
            }
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[derive(Debug)]
struct CloudPeerVerifier {
    inner: Arc<WebPkiServerVerifier>,
    peer_identity: CloudPeerIdentity,
}

impl ServerCertVerifier for CloudPeerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(_) => {}
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => {}
            Err(other) => return Err(other),
        }

        match &self.peer_identity {
            CloudPeerIdentity::AlloyDb { expected_san } => {
                let trimmed = expected_san.trim().trim_end_matches('.');
                if trimmed.is_empty() {
                    return Err(rustls::Error::InvalidCertificate(
                        CertificateError::NotValidForName,
                    ));
                }
                let sn = parse_server_name(trimmed).map_err(|_| {
                    rustls::Error::InvalidCertificate(CertificateError::NotValidForName)
                })?;
                self.inner
                    .verify_server_cert(end_entity, intermediates, &sn, ocsp_response, now)
            }
            CloudPeerIdentity::CloudSql {
                server_name: sni_opt,
                expected_cn,
            } => {
                if let Some(raw_name) = sni_opt.as_deref().map(str::trim).filter(|s| !s.is_empty())
                {
                    if let Ok(sn) = parse_server_name(raw_name)
                        && self
                            .inner
                            .verify_server_cert(end_entity, intermediates, &sn, ocsp_response, now)
                            .is_ok()
                    {
                        return Ok(ServerCertVerified::assertion());
                    }
                    let stripped = raw_name.trim_end_matches('.');
                    if !stripped.is_empty()
                        && stripped != raw_name
                        && let Ok(sn) = parse_server_name(stripped)
                        && self
                            .inner
                            .verify_server_cert(end_entity, intermediates, &sn, ocsp_response, now)
                            .is_ok()
                    {
                        return Ok(ServerCertVerified::assertion());
                    }
                }
                if !expected_cn.is_empty()
                    && extract_der_common_name(end_entity.as_ref()).as_deref()
                        == Some(expected_cn.as_str())
                {
                    return Ok(ServerCertVerified::assertion());
                }
                Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidForName,
                ))
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[derive(Debug)]
struct EncryptOnlyServerVerifier {
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for EncryptOnlyServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn read_der_length(buf: &[u8], pos: usize) -> Option<(usize, usize)> {
    let first = *buf.get(pos)?;
    if (first & 0x80) == 0 {
        return Some((first as usize, pos + 1));
    }
    let num_octets = (first & 0x7f) as usize;
    if num_octets == 0 || num_octets > 4 || pos + 1 + num_octets > buf.len() {
        return None;
    }
    let mut len = 0usize;
    for &b in &buf[pos + 1..pos + 1 + num_octets] {
        len = (len << 8) | (b as usize);
    }
    Some((len, pos + 1 + num_octets))
}

fn extract_der_common_name(der: &[u8]) -> Option<String> {
    extract_tbs_subject_slice(der).and_then(scan_last_cn_in_rdn)
}

fn read_der_tlv(buf: &[u8], pos: usize) -> Option<(u8, &[u8], usize)> {
    let tag = *buf.get(pos)?;
    let (len, val_start) = read_der_length(buf, pos + 1)?;
    let end = val_start.checked_add(len)?;
    if end > buf.len() {
        return None;
    }
    Some((tag, &buf[val_start..end], end))
}

fn extract_tbs_subject_slice(der: &[u8]) -> Option<&[u8]> {
    let (cert_tag, cert_body, cert_end) = read_der_tlv(der, 0)?;
    if cert_tag != 0x30 || cert_end != der.len() {
        return None;
    }
    let (tbs_tag, tbs_body, _) = read_der_tlv(cert_body, 0)?;
    if tbs_tag != 0x30 {
        return None;
    }
    let mut pos = 0usize;
    let (first_tag, _, next_pos) = read_der_tlv(tbs_body, pos)?;
    if first_tag == 0xA0 {
        pos = next_pos;
    }
    // Skip serialNumber, signature, issuer, validity -> arrive at subject.
    for _ in 0..4 {
        let (_, _, after) = read_der_tlv(tbs_body, pos)?;
        pos = after;
    }
    let (subj_tag, subj_body, _) = read_der_tlv(tbs_body, pos)?;
    if subj_tag != 0x30 {
        return None;
    }
    Some(subj_body)
}

fn scan_last_cn_in_rdn(rdn_seq: &[u8]) -> Option<String> {
    const OID_COMMON_NAME_BODY: [u8; 3] = [0x55, 0x04, 0x03];

    let mut last_cn = None;
    let mut rdn_pos = 0usize;
    while rdn_pos < rdn_seq.len() {
        let (set_tag, set_body, next_rdn_pos) = read_der_tlv(rdn_seq, rdn_pos)?;
        if set_tag != 0x31 || set_body.is_empty() {
            return None;
        }
        rdn_pos = next_rdn_pos;

        let mut atv_pos = 0usize;
        while atv_pos < set_body.len() {
            let (seq_tag, seq_body, next_atv_pos) = read_der_tlv(set_body, atv_pos)?;
            if seq_tag != 0x30 {
                return None;
            }
            atv_pos = next_atv_pos;

            let (oid_tag, oid_bytes, val_tlv_pos) = read_der_tlv(seq_body, 0)?;
            if oid_tag != 0x06 {
                return None;
            }
            let (val_tag, val_bytes, atv_end) = read_der_tlv(seq_body, val_tlv_pos)?;
            if atv_end != seq_body.len() {
                return None;
            }
            if oid_bytes == OID_COMMON_NAME_BODY {
                if !matches!(val_tag, 0x0C | 0x13 | 0x16) {
                    return None;
                }
                let cn = std::str::from_utf8(val_bytes).ok()?;
                last_cn = Some(cn.to_string());
            }
        }
    }
    last_cn
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    };
    use rustls::ServerConfig;
    use rustls::server::WebPkiClientVerifier;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    use super::*;
    use crate::dialer::encode_alloydb_metadata_response;

    struct TestPki {
        ca_pem: Vec<u8>,
        server_cert_pem: Vec<u8>,
        server_key_pem: Vec<u8>,
        client_cert_pem: Vec<u8>,
        client_key_pem: Vec<u8>,
    }

    impl TestPki {
        fn generate(sans: &[&str], server_cn: &str) -> Self {
            let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
            ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            ca_params
                .distinguished_name
                .push(DnType::CommonName, "vertebra-test-root-ca");
            let ca_key = KeyPair::generate().unwrap();
            let ca_cert = ca_params.self_signed(&ca_key).unwrap();
            let ca_issuer = Issuer::new(ca_params, ca_key);

            let mut srv_params =
                CertificateParams::new(sans.iter().map(|s| (*s).to_string()).collect::<Vec<_>>())
                    .unwrap();
            srv_params
                .distinguished_name
                .push(DnType::CommonName, server_cn);
            let srv_key = KeyPair::generate().unwrap();
            let srv_cert = srv_params.signed_by(&srv_key, &ca_issuer).unwrap();

            let mut cli_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            cli_params
                .distinguished_name
                .push(DnType::CommonName, "vertebra-test-client");
            let cli_key = KeyPair::generate().unwrap();
            let cli_cert = cli_params.signed_by(&cli_key, &ca_issuer).unwrap();

            Self {
                ca_pem: ca_cert.pem().into_bytes(),
                server_cert_pem: srv_cert.pem().into_bytes(),
                server_key_pem: srv_key.serialize_pem().into_bytes(),
                client_cert_pem: cli_cert.pem().into_bytes(),
                client_key_pem: cli_key.serialize_pem().into_bytes(),
            }
        }

        fn server_acceptor(&self, require_client_cert: bool) -> TlsAcceptor {
            install_rustls_ring_provider();
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let (srv_certs, srv_key) =
                parse_client_identity(&self.server_cert_pem, &self.server_key_pem).unwrap();
            let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
                .with_safe_default_protocol_versions()
                .unwrap();
            let cfg = if require_client_cert {
                let roots = Arc::new(load_root_store(Some(&self.ca_pem)).unwrap());
                let client_verifier = WebPkiClientVerifier::builder_with_provider(roots, provider)
                    .build()
                    .unwrap();
                builder
                    .with_client_cert_verifier(client_verifier)
                    .with_single_cert(srv_certs, srv_key)
                    .unwrap()
            } else {
                builder
                    .with_no_client_auth()
                    .with_single_cert(srv_certs, srv_key)
                    .unwrap()
            };
            TlsAcceptor::from(Arc::new(cfg))
        }
    }

    struct DummyCancelHandle {
        cancelled: Arc<AtomicBool>,
    }

    impl CancelHandle for DummyCancelHandle {
        fn cancel(&self) -> BoxFuture<'_, Result<(), TransportError>> {
            Box::pin(async {
                self.cancelled.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    struct DummyConnection {
        stream: TransportStream,
        broken: bool,
        cancelled: Arc<AtomicBool>,
    }

    impl Connection for DummyConnection {
        fn ping(&mut self) -> BoxFuture<'_, Result<(), TransportError>> {
            Box::pin(async {
                self.stream.write_all(b"PING").await?;
                let mut pong = [0u8; 4];
                self.stream.read_exact(&mut pong).await?;
                if &pong == b"PONG" {
                    Ok(())
                } else {
                    self.broken = true;
                    Err(TransportError::Closed)
                }
            })
        }

        fn is_broken(&self) -> bool {
            self.broken
        }

        fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
            Arc::new(DummyCancelHandle {
                cancelled: Arc::clone(&self.cancelled),
            })
        }

        fn close(mut self) -> BoxFuture<'static, Result<(), TransportError>> {
            Box::pin(async move {
                self.stream.shutdown().await?;
                Ok(())
            })
        }
    }

    #[test]
    fn transport_error_transience_and_sqlstate_mapping() {
        let refused = TransportError::Io(io::Error::new(ErrorKind::ConnectionRefused, "refused"));
        assert!(refused.is_transient());
        assert_eq!(refused.sqlstate(), Some("08001"));

        let timeout = TransportError::Timeout(Duration::from_millis(250));
        assert!(timeout.is_transient());
        assert_eq!(timeout.sqlstate(), Some("08001"));

        let closed = TransportError::Closed;
        assert!(closed.is_transient());
        assert_eq!(closed.sqlstate(), Some("08006"));

        let tls_err = TransportError::Tls("bad cert".to_string());
        assert!(!tls_err.is_transient());
        assert_eq!(tls_err.sqlstate(), Some("08001"));

        let cancelled = TransportError::Cancelled;
        assert!(!cancelled.is_transient());
        assert_eq!(cancelled.sqlstate(), Some("57014"));

        let invalid = TransportError::InvalidConfig("bad mode".to_string());
        assert!(!invalid.is_transient());
        assert_eq!(invalid.sqlstate(), None);
    }

    #[tokio::test]
    async fn loopback_tcp_connect_and_connection_trait_lifecycle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            sock.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"PING");
            sock.write_all(b"PONG").await.unwrap();
        });

        let opts = ConnectOptions::tcp("127.0.0.1", addr.port());
        let stream = TransportStream::connect(&opts).await.unwrap();
        assert!(!stream.is_tls());
        assert_eq!(stream.peer_addr().unwrap().port(), addr.port());

        let mut conn = DummyConnection {
            stream,
            broken: false,
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        assert!(!conn.is_broken());
        conn.ping().await.unwrap();
        let cancel = conn.cancel_handle();
        cancel.cancel().await.unwrap();
        assert!(conn.cancelled.load(Ordering::SeqCst));
        conn.close().await.unwrap();
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_domain_socket_connect_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("vtb-db.sock");
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut srv, _) = listener.accept().await.unwrap();
            let mut msg = [0u8; 5];
            srv.read_exact(&mut msg).await.unwrap();
            assert_eq!(&msg, b"HELLO");
            srv.write_all(b"WORLD").await.unwrap();
        });

        let opts = ConnectOptions::unix(&sock_path);
        let mut client = TransportStream::connect(&opts).await.unwrap();
        client.write_all(b"HELLO").await.unwrap();
        let mut reply = [0u8; 5];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"WORLD");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tls_verify_full_accepts_matching_san_and_rejects_hostname_mismatch() {
        let pki = TestPki::generate(&["db.internal.example"], "db.internal.example");
        let acceptor = pki.server_acceptor(false);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor_ok = acceptor.clone();
        let server_ok = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut tls = acceptor_ok.accept(sock).await.unwrap();
            let mut buf = [0u8; 2];
            tls.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, b"OK");
            tls.write_all(b"HI").await.unwrap();
        });

        let opts_ok = ConnectOptions::tcp("127.0.0.1", addr.port())
            .with_tls_mode(TlsMode::VerifyFull)
            .with_ca_pem(pki.ca_pem.clone())
            .with_server_name("db.internal.example");
        let mut stream_ok = TransportStream::connect(&opts_ok).await.unwrap();
        assert!(stream_ok.is_tls());
        stream_ok.write_all(b"OK").await.unwrap();
        let mut out = [0u8; 2];
        stream_ok.read_exact(&mut out).await.unwrap();
        assert_eq!(&out, b"HI");
        server_ok.await.unwrap();

        let listener_bad = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_bad = listener_bad.local_addr().unwrap();
        let server_bad = tokio::spawn(async move {
            let (sock, _) = listener_bad.accept().await.unwrap();
            let _ = acceptor.accept(sock).await;
        });

        let opts_bad = ConnectOptions::tcp("127.0.0.1", addr_bad.port())
            .with_tls_mode(TlsMode::VerifyFull)
            .with_ca_pem(pki.ca_pem.clone())
            .with_server_name("wrong-host.example");
        let err = TransportStream::connect(&opts_bad).await.unwrap_err();
        assert!(matches!(err, TransportError::Tls(_)));
        assert!(!err.is_transient());
        server_bad.await.unwrap();
    }

    #[tokio::test]
    async fn cloudsql_mtls_verifies_instance_cn_and_alloydb_runs_metadata_exchange() {
        let csql_pki = TestPki::generate(&[], "proj-a:inst-1");
        let csql_acceptor = csql_pki.server_acceptor(true);
        let csql_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let csql_addr = csql_listener.local_addr().unwrap();
        let csql_acceptor_ok = csql_acceptor.clone();
        let csql_srv = tokio::spawn(async move {
            let (sock, _) = csql_listener.accept().await.unwrap();
            let mut tls = csql_acceptor_ok.accept(sock).await.unwrap();
            tls.write_all(b"CSQL").await.unwrap();
        });

        let csql_opts = ConnectOptions {
            target: EndpointTarget::CloudSql {
                instance: CloudSqlInstanceUri::parse("proj-a:us-central1:inst-1").unwrap(),
                addr: csql_addr,
                server_ca_pem: csql_pki.ca_pem.clone(),
                client_cert_pem: csql_pki.client_cert_pem.clone(),
                client_key_pem: csql_pki.client_key_pem.clone(),
                peer_identity: CloudPeerIdentity::CloudSql {
                    server_name: None,
                    expected_cn: "proj-a:inst-1".to_string(),
                },
            },
            tls_mode: TlsMode::VerifyFull,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tcp_nodelay: true,
            application_name: None,
        };
        let mut csql_stream = TransportStream::connect(&csql_opts).await.unwrap();
        assert!(csql_stream.peer_leaf_der().is_some());
        let mut tag = [0u8; 4];
        csql_stream.read_exact(&mut tag).await.unwrap();
        assert_eq!(&tag, b"CSQL");
        csql_srv.await.unwrap();

        let csql_bad_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let csql_bad_addr = csql_bad_listener.local_addr().unwrap();
        let csql_bad_srv = tokio::spawn(async move {
            let (sock, _) = csql_bad_listener.accept().await.unwrap();
            let _ = csql_acceptor.accept(sock).await;
        });
        let csql_bad_opts = ConnectOptions {
            target: EndpointTarget::CloudSql {
                instance: CloudSqlInstanceUri::parse("proj-a:us-central1:other-inst").unwrap(),
                addr: csql_bad_addr,
                server_ca_pem: csql_pki.ca_pem,
                client_cert_pem: csql_pki.client_cert_pem,
                client_key_pem: csql_pki.client_key_pem,
                peer_identity: CloudPeerIdentity::CloudSql {
                    server_name: None,
                    expected_cn: "proj-a:other-inst".to_string(),
                },
            },
            ..csql_opts
        };
        assert!(matches!(
            TransportStream::connect(&csql_bad_opts).await.unwrap_err(),
            TransportError::Tls(_)
        ));
        csql_bad_srv.await.unwrap();

        let alloy_pki = TestPki::generate(&["127.0.0.1"], "alloydb-psc");
        let alloy_acceptor = alloy_pki.server_acceptor(true);
        let alloy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let alloy_addr = alloy_listener.local_addr().unwrap();
        let alloy_srv = tokio::spawn(async move {
            let (sock, _) = alloy_listener.accept().await.unwrap();
            let mut tls = alloy_acceptor.accept(sock).await.unwrap();
            let mut hdr = [0u8; 4];
            tls.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            tls.read_exact(&mut body).await.unwrap();
            let resp = encode_alloydb_metadata_response(1, "");
            tls.write_all(&resp).await.unwrap();
            tls.write_all(b"READY").await.unwrap();
        });

        let alloy_opts = ConnectOptions {
            target: EndpointTarget::AlloyDb {
                cluster: AlloyDbClusterUri::parse(
                    "projects/p1/locations/us-central1/clusters/c1/instances/i1",
                )
                .unwrap(),
                addr: alloy_addr,
                server_ca_pem: alloy_pki.ca_pem,
                client_cert_pem: alloy_pki.client_cert_pem,
                client_key_pem: alloy_pki.client_key_pem,
                user_agent: "vertebra-db/test".to_string(),
                oauth2_token: "mock-oauth-token".to_string(),
                auth_type: AuthType::Iam,
                peer_identity: CloudPeerIdentity::AlloyDb {
                    expected_san: "127.0.0.1".to_string(),
                },
            },
            tls_mode: TlsMode::VerifyFull,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tcp_nodelay: true,
            application_name: None,
        };
        let mut alloy_stream = TransportStream::connect(&alloy_opts).await.unwrap();
        let mut ready = [0u8; 5];
        alloy_stream.read_exact(&mut ready).await.unwrap();
        assert_eq!(&ready, b"READY");
        alloy_srv.await.unwrap();
    }

    #[tokio::test]
    async fn subject_cn_parser_rejects_issuer_cn_fallback_and_ou_oid_spoofing() {
        use rcgen::{DistinguishedName, DnValue};

        let expected_cn = "proj-a:inst-1";
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        ca_params
            .distinguished_name
            .push(DnType::CommonName, expected_cn);
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_issuer = Issuer::new(ca_params, ca_key);

        let mut cli_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        cli_params
            .distinguished_name
            .push(DnType::CommonName, "vertebra-test-client");
        let cli_key = KeyPair::generate().unwrap();
        let cli_cert = cli_params.signed_by(&cli_key, &ca_issuer).unwrap();

        let mut empty_subj_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
        empty_subj_params.distinguished_name = DistinguishedName::new();
        let empty_subj_key = KeyPair::generate().unwrap();
        let empty_subj_cert = empty_subj_params
            .signed_by(&empty_subj_key, &ca_issuer)
            .unwrap();
        assert_eq!(extract_der_common_name(empty_subj_cert.der()), None);

        let mut wrong_subj_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        wrong_subj_params.distinguished_name = DistinguishedName::new();
        wrong_subj_params
            .distinguished_name
            .push(DnType::CommonName, "wrong:inst");
        let wrong_subj_key = KeyPair::generate().unwrap();
        let wrong_subj_cert = wrong_subj_params
            .signed_by(&wrong_subj_key, &ca_issuer)
            .unwrap();
        assert_eq!(
            extract_der_common_name(wrong_subj_cert.der()).as_deref(),
            Some("wrong:inst")
        );

        let spoof_ou = "\x06\x03\x55\x04\x03\x0c\x0dproj-a:inst-1";
        let mut ou_spoof_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ou_spoof_params.distinguished_name = DistinguishedName::new();
        ou_spoof_params.distinguished_name.push(
            DnType::OrganizationalUnitName,
            DnValue::Utf8String(spoof_ou.to_string()),
        );
        let ou_spoof_key = KeyPair::generate().unwrap();
        let ou_spoof_cert = ou_spoof_params
            .signed_by(&ou_spoof_key, &ca_issuer)
            .unwrap();
        assert_eq!(extract_der_common_name(ou_spoof_cert.der()), None);

        let mut ou_plus_wrong_cn_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ou_plus_wrong_cn_params.distinguished_name = DistinguishedName::new();
        ou_plus_wrong_cn_params
            .distinguished_name
            .push(DnType::CommonName, "wrong:inst");
        ou_plus_wrong_cn_params.distinguished_name.push(
            DnType::OrganizationalUnitName,
            DnValue::Utf8String(spoof_ou.to_string()),
        );
        let ou_plus_wrong_cn_key = KeyPair::generate().unwrap();
        let ou_plus_wrong_cn_cert = ou_plus_wrong_cn_params
            .signed_by(&ou_plus_wrong_cn_key, &ca_issuer)
            .unwrap();
        assert_eq!(
            extract_der_common_name(ou_plus_wrong_cn_cert.der()).as_deref(),
            Some("wrong:inst")
        );

        let bmp_cn_rdn = [
            0x31, 0x0D, 0x30, 0x0B, 0x06, 0x03, 0x55, 0x04, 0x03, 0x1E, 0x04, 0x00, b'A', 0x00,
            b'B',
        ];
        assert_eq!(scan_last_cn_in_rdn(&bmp_cn_rdn), None);
        assert_eq!(scan_last_cn_in_rdn(&[0x31, 0x05, 0x30, 0x08]), None);

        for (srv_cert_pem, srv_key_pem) in [
            (
                empty_subj_cert.pem().into_bytes(),
                empty_subj_key.serialize_pem().into_bytes(),
            ),
            (
                ou_spoof_cert.pem().into_bytes(),
                ou_spoof_key.serialize_pem().into_bytes(),
            ),
        ] {
            let pki = TestPki {
                ca_pem: ca_cert.pem().into_bytes(),
                server_cert_pem: srv_cert_pem,
                server_key_pem: srv_key_pem,
                client_cert_pem: cli_cert.pem().into_bytes(),
                client_key_pem: cli_key.serialize_pem().into_bytes(),
            };
            let acceptor = pki.server_acceptor(true);
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let srv = tokio::spawn(async move {
                let (sock, _) = listener.accept().await.unwrap();
                let _ = acceptor.accept(sock).await;
            });
            let opts = ConnectOptions {
                target: EndpointTarget::CloudSql {
                    instance: CloudSqlInstanceUri::parse("proj-a:us-central1:inst-1").unwrap(),
                    addr,
                    server_ca_pem: pki.ca_pem,
                    client_cert_pem: pki.client_cert_pem,
                    client_key_pem: pki.client_key_pem,
                    peer_identity: CloudPeerIdentity::CloudSql {
                        server_name: None,
                        expected_cn: "proj-a:inst-1".to_string(),
                    },
                },
                tls_mode: TlsMode::VerifyFull,
                ca_pem: None,
                client_cert_pem: None,
                client_key_pem: None,
                server_name: None,
                connect_timeout: DEFAULT_CONNECT_TIMEOUT,
                tcp_nodelay: true,
                application_name: None,
            };
            assert!(matches!(
                TransportStream::connect(&opts).await.unwrap_err(),
                TransportError::Tls(_)
            ));
            srv.await.unwrap();
        }
    }

    #[tokio::test]
    async fn tcp_keepalive_applies_to_tcp_and_rejects_unix_or_invalid_bounds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
        });
        let opts = ConnectOptions::tcp("127.0.0.1", addr.port());
        let stream = TransportStream::connect(&opts).await.unwrap();
        stream
            .set_tcp_keepalive(TcpKeepaliveConfig {
                enabled: true,
                idle: Some(Duration::from_secs(30)),
                interval: Some(Duration::from_secs(10)),
                retries: Some(3),
            })
            .expect("apply keepalive");
        stream
            .set_tcp_keepalive(TcpKeepaliveConfig {
                enabled: false,
                idle: None,
                interval: None,
                retries: None,
            })
            .expect("disable keepalive");
        assert!(matches!(
            stream.set_tcp_keepalive(TcpKeepaliveConfig {
                enabled: false,
                idle: Some(Duration::from_secs(10)),
                interval: None,
                retries: None,
            }),
            Err(TransportError::InvalidConfig(_))
        ));
        srv.await.unwrap();

        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let sock_path = dir.path().join("ka.sock");
            let unix_listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
            let unix_srv = tokio::spawn(async move {
                let _ = unix_listener.accept().await.unwrap();
            });
            let unix_stream = TransportStream::connect(&ConnectOptions::unix(&sock_path))
                .await
                .unwrap();
            assert!(matches!(
                unix_stream.set_tcp_keepalive(TcpKeepaliveConfig::default()),
                Err(TransportError::InvalidConfig(_))
            ));
            unix_srv.await.unwrap();
        }
    }

    #[tokio::test]
    async fn alloydb_split_transport_stages() {
        let alloy_pki = TestPki::generate(&["127.0.0.1"], "alloydb-split");
        let alloy_acceptor = alloy_pki.server_acceptor(true);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut tls = alloy_acceptor.accept(sock).await.unwrap();
            let mut hdr = [0u8; 4];
            tls.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes(hdr) as usize;
            let mut body = vec![0u8; len];
            tls.read_exact(&mut body).await.unwrap();
            let resp = encode_alloydb_metadata_response(1, "");
            tls.write_all(&resp).await.unwrap();
            tls.write_all(b"SPLIT").await.unwrap();
        });

        let opts = ConnectOptions {
            target: EndpointTarget::AlloyDb {
                cluster: AlloyDbClusterUri::parse(
                    "projects/p1/locations/us-central1/clusters/c1/instances/i1",
                )
                .unwrap(),
                addr,
                server_ca_pem: alloy_pki.ca_pem,
                client_cert_pem: alloy_pki.client_cert_pem,
                client_key_pem: alloy_pki.client_key_pem,
                user_agent: "vertebra-db/split-test".to_string(),
                oauth2_token: "split-oauth-token".to_string(),
                auth_type: AuthType::Iam,
                peer_identity: CloudPeerIdentity::AlloyDb {
                    expected_san: "127.0.0.1".to_string(),
                },
            },
            tls_mode: TlsMode::VerifyFull,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            server_name: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            tcp_nodelay: true,
            application_name: None,
        };

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut stream = TransportStream::connect_alloydb_tls(&opts).await.unwrap();
        assert!(stream.is_tls());
        assert!(stream.peer_leaf_der().is_some());
        finish_alloydb_metadata(&mut stream, &opts, deadline)
            .await
            .unwrap();
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"SPLIT");
        srv.await.unwrap();
    }
}
