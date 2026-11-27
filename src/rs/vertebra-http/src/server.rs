//! Hyper/Tokio accept loop.
//!
//! Supports tuned direct HTTP/1.1 (`http1::Builder`) on the default zero-peek
//! cleartext fastpath, optional cleartext HTTP/2 (`h2c` prior knowledge), and
//! ALPN-negotiated HTTP/1.1 + HTTP/2 over TLS.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use hyper::Request;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

use crate::access_log::AccessLogger;
use crate::assets::{AssetManager, StaticAssetServeConfig};
use crate::batcher::PendingRequest;
use crate::compression::CompressionOffloadConfig;
use crate::config::{OpenApiCacheConfig, ServerConfig, StaticMountConfig};
use crate::cors::CompiledCorsOffload;
use crate::discovery::WellKnownDiscoveryCache;
use crate::dispatcher::Dispatcher;
use crate::h2c::{CleartextProtocol, detect_cleartext_protocol};
use crate::health::HealthEndpoint;
use crate::hmr::HmrState;
use crate::profile::ProfileEndpoint;
use crate::pubsub::PubSubBroker;
use crate::response::handle_with_peer_shard;
use crate::routing::Router;
use crate::sse::SseHub;
use crate::ws_mount::{WsConfig, WsConnectionRegistry};
use crate::zpages::ZPagesEndpoint;

#[cfg(feature = "tls")]
use crate::tls::TlsAcceptor;

/// Startup-compiled static file mount served directly in Rust.
pub struct StaticAssetMount {
    pub path_prefix: String,
    pub managers: Vec<AssetManager>,
    pub send_as_attachment: bool,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl StaticAssetMount {
    pub fn from_config(cfg: &StaticMountConfig) -> Self {
        let trimmed = cfg.path_prefix.trim_matches('/');
        let path_prefix = if trimmed.is_empty() {
            "/".to_string()
        } else {
            format!("/{trimmed}")
        };
        let serve_cfg = StaticAssetServeConfig {
            spa_fallback: cfg.html_mode,
            ..StaticAssetServeConfig::default()
        };
        let managers = cfg
            .directories
            .iter()
            .map(|dir| AssetManager::with_config(Some(dir.as_str()), None, serve_cfg.clone()))
            .collect();
        let headers = cfg
            .headers
            .iter()
            .filter_map(|(k, v)| {
                let name = HeaderName::from_bytes(k.as_bytes()).ok()?;
                let val = HeaderValue::from_str(v).ok()?;
                Some((name, val))
            })
            .collect();
        Self {
            path_prefix,
            managers,
            send_as_attachment: cfg.send_as_attachment,
            headers,
        }
    }

    #[inline]
    pub fn match_relative_path<'a>(&self, req_path: &'a str) -> Option<&'a str> {
        if self.path_prefix == "/" {
            return Some(req_path);
        }
        if req_path == self.path_prefix {
            return Some("/");
        }
        let rest = req_path.strip_prefix(self.path_prefix.as_str())?;
        if rest.starts_with('/') {
            Some(rest)
        } else {
            None
        }
    }
}

/// Startup-compiled OpenAPI JSON cache entry served directly in Rust.
pub struct CompiledOpenApiCache {
    pub schema_path: String,
    pub body: Bytes,
    pub etag: HeaderValue,
    pub content_type: HeaderValue,
    pub etag_str: String,
}

impl CompiledOpenApiCache {
    pub fn from_config(cfg: &OpenApiCacheConfig) -> Option<Self> {
        if cfg.schema_path.is_empty() {
            return None;
        }
        let body = if cfg.body.trim_start().starts_with('{') {
            Bytes::copy_from_slice(cfg.body.as_bytes())
        } else {
            base64::engine::general_purpose::STANDARD
                .decode(cfg.body.as_bytes())
                .map(Bytes::from)
                .unwrap_or_else(|_| Bytes::copy_from_slice(cfg.body.as_bytes()))
        };
        let etag = HeaderValue::from_str(&cfg.etag).ok()?;
        let ct_raw = if cfg.content_type.is_empty() {
            "application/vnd.oai.openapi+json"
        } else {
            cfg.content_type.as_str()
        };
        let content_type = HeaderValue::from_str(ct_raw).ok()?;
        Some(Self {
            schema_path: cfg.schema_path.clone(),
            body,
            etag,
            content_type,
            etag_str: cfg.etag.clone(),
        })
    }
}

/// Startup-configured WebSocket mount handled directly in Rust via `PubSubBroker`.
#[derive(Debug, Clone)]
pub struct WsMount {
    pub path_prefix: String,
    pub topic: Option<String>,
    pub allowed_origins: Vec<String>,
    pub supported_subprotocols: Vec<String>,
    pub config: WsConfig,
}

impl WsMount {
    /// Construct a `WsMount` for `path_prefix` using default `WsConfig`.
    pub fn new(path_prefix: impl Into<String>) -> Self {
        let raw = path_prefix.into();
        let trimmed = raw.trim_matches('/');
        let path_prefix = if trimmed.is_empty() {
            "/".to_string()
        } else {
            format!("/{trimmed}")
        };
        Self {
            path_prefix,
            topic: None,
            allowed_origins: Vec::new(),
            supported_subprotocols: Vec::new(),
            config: WsConfig::default(),
        }
    }

    /// Resolve the pub/sub topic name if `req_path` matches this mount.
    pub fn match_topic<'a>(&'a self, req_path: &'a str) -> Option<&'a str> {
        if self.path_prefix == "/" {
            let trimmed = req_path.trim_start_matches('/');
            if let Some(ref topic) = self.topic {
                return Some(topic.as_str());
            }
            return (!trimmed.is_empty()).then_some(trimmed);
        }
        if req_path == self.path_prefix {
            if let Some(ref topic) = self.topic {
                return Some(topic.as_str());
            }
            let fallback = self.path_prefix.trim_start_matches('/');
            return (!fallback.is_empty()).then_some(fallback);
        }
        let rest = req_path.strip_prefix(self.path_prefix.as_str())?;
        let topic = rest.strip_prefix('/')?;
        if topic.is_empty() {
            return None;
        }
        Some(self.topic.as_deref().unwrap_or(topic))
    }
}

/// Shared state threaded through every request service call.
pub struct ServerState {
    pub router: Arc<Router>,
    pub dispatcher: Arc<Dispatcher>,
    pub config: Arc<ServerConfig>,
    pub collector_shards: Vec<tokio::sync::mpsc::Sender<PendingRequest>>,
    pub next_shard: AtomicUsize,
    pub hmr: Arc<HmrState>,
    pub health_endpoint: HealthEndpoint,
    pub cors: Option<CompiledCorsOffload>,
    pub compression: Option<CompressionOffloadConfig>,
    pub static_mounts: Vec<StaticAssetMount>,
    pub openapi_cache: Option<CompiledOpenApiCache>,
    pub profile_endpoint: ProfileEndpoint,
    pub zpages_endpoint: ZPagesEndpoint,
    pub discovery_cache: WellKnownDiscoveryCache,
    pub access_logger: Option<Arc<AccessLogger>>,
    pub sse_hub: Arc<SseHub>,
    pub pubsub_broker: Arc<PubSubBroker>,
    pub ws_registry: Arc<WsConnectionRegistry>,
    pub ws_config: WsConfig,
    pub ws_allowed_origins: Vec<String>,
    pub ws_supported_subprotocols: Vec<String>,
    pub ws_mounts: Vec<WsMount>,
    /// Bound UDP port of the active HTTP/3 QUIC listener (`None` when inactive).
    pub http3_port: Option<u16>,
}

impl ServerState {
    /// Construct a `ServerState` caching health and configured Rust middleware offloads.
    pub fn new(
        router: Arc<Router>,
        dispatcher: Arc<Dispatcher>,
        config: Arc<ServerConfig>,
        collector_shards: Vec<tokio::sync::mpsc::Sender<PendingRequest>>,
    ) -> Self {
        let cors = config.cors.clone().map(CorsOffloadConfigExt::into_compiled);
        let compression = config.compression.clone();
        let static_mounts = config
            .static_mounts
            .iter()
            .map(StaticAssetMount::from_config)
            .collect();
        let openapi_cache = config
            .openapi_cache
            .as_ref()
            .and_then(CompiledOpenApiCache::from_config);
        let profile_endpoint = ProfileEndpoint::new(config.profile.clone());
        let zpages_endpoint =
            ZPagesEndpoint::new(config.zpages.clone(), config.bind.ip().is_loopback());
        let discovery_cache = WellKnownDiscoveryCache::new();
        for (doc_path, body_str) in &config.discovery {
            let body_bytes = Bytes::copy_from_slice(body_str.as_bytes());
            match doc_path.as_str() {
                "/.well-known/agent-card.json" => discovery_cache.register_agent_card(body_bytes),
                "/.well-known/mcp.json" => discovery_cache.register_mcp_manifest(body_bytes),
                "/.well-known/oauth-protected-resource" => {
                    discovery_cache.register_oauth_protected_resource(body_bytes);
                }
                other => {
                    discovery_cache.register_document(other, "application/json", None, body_bytes);
                }
            }
        }
        let access_logger = config
            .access_log
            .clone()
            .map(|fmt| Arc::new(AccessLogger::new(fmt)));
        let sse_hub = Arc::new(SseHub::new(256, std::time::Duration::from_secs(15)));
        let pubsub_broker = Arc::new(PubSubBroker::new(0, 256, Some(Arc::clone(&sse_hub)), None));
        if matches!(
            dispatcher.backend(),
            crate::dispatcher::DispatchBackend::Bridge(_)
        ) && let Some(bridge) = vertebra_core::bridge::BatchBridge::get()
        {
            let broker_weak = Arc::downgrade(&pubsub_broker);
            bridge.set_pubsub_sink(Arc::new(move |topic, payload, idempotency_key| {
                broker_weak
                    .upgrade()
                    .and_then(|broker| {
                        broker.pubsub_publish(
                            topic,
                            Bytes::copy_from_slice(payload),
                            idempotency_key,
                        )
                    })
                    .unwrap_or(0)
            }));
        }
        let ws_registry = Arc::new(WsConnectionRegistry::new());

        Self {
            router,
            dispatcher,
            config,
            collector_shards,
            next_shard: AtomicUsize::new(0),
            hmr: Arc::new(HmrState::default()),
            health_endpoint: HealthEndpoint::default(),
            cors,
            compression,
            static_mounts,
            openapi_cache,
            profile_endpoint,
            zpages_endpoint,
            discovery_cache,
            access_logger,
            sse_hub,
            pubsub_broker,
            ws_registry,
            ws_config: WsConfig::default(),
            ws_allowed_origins: Vec::new(),
            ws_supported_subprotocols: Vec::new(),
            ws_mounts: Vec::new(),
            http3_port: None,
        }
    }

    /// Set the bound UDP HTTP/3 QUIC port on this `ServerState`.
    pub fn with_http3_port(mut self, port: Option<u16>) -> Self {
        self.http3_port = port;
        self
    }

    /// Configure default WebSocket settings for `/__vertebra/ws/{topic}`.
    pub fn with_ws_config(mut self, ws_config: WsConfig) -> Self {
        self.ws_config = ws_config;
        self
    }

    /// Configure allowed `Origin` values for `/__vertebra/ws/{topic}`.
    pub fn with_ws_allowed_origins(mut self, origins: Vec<String>) -> Self {
        self.ws_allowed_origins = origins;
        self
    }

    /// Configure supported `Sec-WebSocket-Protocol` values for `/__vertebra/ws/{topic}`.
    pub fn with_ws_supported_subprotocols(mut self, subprotocols: Vec<String>) -> Self {
        self.ws_supported_subprotocols = subprotocols;
        self
    }

    /// Add a custom WebSocket mount entry handled directly in Rust.
    pub fn with_ws_mount(mut self, mount: WsMount) -> Self {
        self.ws_mounts.push(mount);
        self
    }

    /// Publish an event to `topic` on this server's `PubSubBroker`.
    pub fn pubsub_publish(
        &self,
        topic: &str,
        payload: Bytes,
        idempotency_key: Option<&str>,
    ) -> Option<u64> {
        self.pubsub_broker
            .pubsub_publish(topic, payload, idempotency_key)
    }

    /// Select a collector shard index once per accepted connection.
    #[inline]
    pub fn next_collector_shard(&self) -> usize {
        let len = self.collector_shards.len();
        if len <= 1 {
            0
        } else {
            self.next_shard.fetch_add(1, Ordering::Relaxed) % len
        }
    }

    /// Return the collector shard sender for a preselected shard index.
    #[inline]
    pub fn collector_for_shard(
        &self,
        shard_idx: usize,
    ) -> &tokio::sync::mpsc::Sender<PendingRequest> {
        let len = self.collector_shards.len();
        &self.collector_shards[shard_idx % len]
    }

    /// Get a collector shard sender (round-robin).
    #[inline]
    pub fn get_collector(&self) -> &tokio::sync::mpsc::Sender<PendingRequest> {
        let len = self.collector_shards.len();
        if len == 1 {
            return &self.collector_shards[0];
        }
        let idx = self.next_shard.fetch_add(1, Ordering::Relaxed) % len;
        &self.collector_shards[idx]
    }
}

trait CorsOffloadConfigExt {
    fn into_compiled(self) -> CompiledCorsOffload;
}

impl CorsOffloadConfigExt for crate::cors::CorsOffloadConfig {
    fn into_compiled(self) -> CompiledCorsOffload {
        self.compile()
    }
}

/// Bind a non-blocking `TcpListener` with `SO_REUSEADDR`, optional `SO_REUSEPORT`,
/// `TCP_NODELAY`, and a listen backlog of `2048`.
pub fn bind_listener(addr: SocketAddr, reuse_port: bool) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    #[cfg(unix)]
    if reuse_port {
        socket.set_reuseport(true)?;
    }
    #[cfg(not(unix))]
    let _ = reuse_port;
    let _ = socket.set_nodelay(true);
    socket.bind(addr)?;
    socket.listen(2048)
}

/// Bind a non-blocking `std::net::UdpSocket` with `SO_REUSEADDR` and optional Unix `SO_REUSEPORT`.
pub fn bind_udp_listener(addr: SocketAddr, reuse_port: bool) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(unix))]
    let _ = reuse_port;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

/// Run the HTTP server with sharded collector channels until the shutdown signal resolves.
pub async fn run_with_collector(
    config: Arc<ServerConfig>,
    router: Arc<Router>,
    dispatcher: Arc<Dispatcher>,
    collector_shards: Vec<tokio::sync::mpsc::Sender<PendingRequest>>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = ServerState::new(router, dispatcher, config, collector_shards);
    run_with_collector_state(state, shutdown).await
}

/// Run the HTTP server using a pre-constructed [`ServerState`] until `shutdown` resolves.
pub async fn run_with_collector_state(
    state: ServerState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = bind_listener(state.config.bind, state.config.reuse_port)?;
    run_with_listener_state(listener, state, shutdown).await
}

/// Run the HTTP server on a pre-bound `TcpListener` until the shutdown signal resolves.
pub async fn run_with_listener(
    listener: TcpListener,
    config: Arc<ServerConfig>,
    router: Arc<Router>,
    dispatcher: Arc<Dispatcher>,
    collector_shards: Vec<tokio::sync::mpsc::Sender<PendingRequest>>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = ServerState::new(router, dispatcher, config, collector_shards);
    run_with_listener_state(listener, state, shutdown).await
}

/// Run the HTTP server on a pre-bound `TcpListener` and pre-constructed [`ServerState`].
pub async fn run_with_listener_state(
    listener: TcpListener,
    mut state: ServerState,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Arc::clone(&state.config);
    config.validate_http3()?;

    let bound_addr = listener.local_addr().unwrap_or(config.bind);
    info!(addr = %bound_addr, reuse_port = config.reuse_port, "vertebra-http listening");

    #[cfg(feature = "tls")]
    let tls_acceptor: Option<TlsAcceptor> = config
        .tls
        .as_ref()
        .map(TlsAcceptor::from_config)
        .transpose()?;

    #[cfg(not(feature = "tls"))]
    let tls_acceptor: Option<()> = None;

    let _ = &tls_acceptor; // suppress unused warning in non-tls builds

    #[cfg(feature = "http3")]
    let (h3_endpoint_setup, quic_port) = if config.enable_http3() {
        let tls_acc = tls_acceptor
            .as_ref()
            .ok_or("HTTP/3 requires TLS configuration")?;
        let rustls_cfg = (*tls_acc.rustls_config()).clone();
        let quic_cfg = crate::http3::build_quic_server_config(rustls_cfg, &config.http3)?;
        let udp_addr = SocketAddr::new(bound_addr.ip(), bound_addr.port());
        let udp_socket = bind_udp_listener(udp_addr, config.reuse_port)?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(quic_cfg),
            udp_socket,
            Arc::new(quinn::TokioRuntime),
        )?;
        let quic_port = endpoint.local_addr()?.port();
        info!(udp_addr = %udp_addr, quic_port, "vertebra-http HTTP/3 QUIC listening");
        (Some((endpoint, quic_port)), Some(quic_port))
    } else {
        (None, None)
    };

    #[cfg(not(feature = "http3"))]
    let quic_port: Option<u16> = None;

    state.http3_port = quic_port;
    let state = Arc::new(state);

    #[cfg(feature = "http3")]
    let mut h3_handle = if let Some((endpoint, quic_port)) = h3_endpoint_setup {
        let (h3_shutdown_tx, h3_shutdown_rx) = tokio::sync::watch::channel(false);
        let h3_state = Arc::clone(&state);
        let h3_cfg = config.http3.clone();
        let embedded_topology = config.topology == crate::config::Topology::Embedded;
        if !embedded_topology && h3_cfg.enable_webtransport {
            warn!(
                "WebTransport over HTTP/3 is only supported in Topology::Embedded; CONNECT requests will return 501 Not Implemented under Topology::Shm"
            );
        }
        let h3_handler = Arc::new(move |req: http::Request<Bytes>| {
            let state = Arc::clone(&h3_state);
            async move {
                match crate::response::handle_buffered(req, state).await {
                    Ok(resp) => resp,
                    Err(infallible) => match infallible {},
                }
            }
        });
        let h3_max_body_size = state.config.payload_limits.max_body_size;
        let join = tokio::spawn(crate::http3::serve_http3_with_bridge(
            endpoint,
            quic_port,
            h3_cfg,
            h3_max_body_size,
            h3_handler,
            embedded_topology,
            h3_shutdown_rx,
        ));
        Some((h3_shutdown_tx, join))
    } else {
        None
    };

    let conn_semaphore =
        (config.max_connections > 0).then(|| Arc::new(Semaphore::new(config.max_connections)));
    let graceful = GracefulShutdown::new();
    let (h1_drain_tx, _) = watch::channel(());
    let mut conn_tasks = JoinSet::new();

    tokio::pin!(shutdown);

    loop {
        while conn_tasks.try_join_next().is_some() {}

        let permit = if let Some(ref sem) = conn_semaphore {
            tokio::select! {
                res = Arc::clone(sem).acquire_owned() => match res {
                    Ok(permit) => Some(permit),
                    Err(_) => break,
                },
                _ = &mut shutdown => {
                    info!("shutdown signal received, stopping accept loop");
                    break;
                }
            }
        } else {
            None
        };

        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let state = Arc::clone(&state);
                        let watcher = graceful.watcher();
                        #[cfg(feature = "tls")]
                        if let Some(ref tls_acc) = tls_acceptor {
                            let tls_acc = tls_acc.clone();
                            conn_tasks.spawn(async move {
                                let _permit = permit;
                                if let Err(e) = serve_tls(stream, peer, state, tls_acc, watcher).await {
                                    debug!(peer = %peer, error = %e, "TLS connection error");
                                }
                            });
                            continue;
                        }
                        let h1_drain_rx = h1_drain_tx.subscribe();
                        conn_tasks.spawn(async move {
                            let _permit = permit;
                            if let Err(e) = serve_plain(stream, peer, state, watcher, h1_drain_rx).await {
                                debug!(peer = %peer, error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        drop(permit);
                        error!(error = %e, "accept failed");
                    }
                }
            }
            _ = &mut shutdown => {
                drop(permit);
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    drop(listener);

    let drain_fut = async {
        tokio::join!(graceful.shutdown(), async {
            let _ = h1_drain_tx.send(());
            h1_drain_tx.closed().await;
        });
    };
    if tokio::time::timeout(config.drain_timeout, drain_fut)
        .await
        .is_err()
    {
        warn!(
            drain_timeout_ms = config.drain_timeout.as_millis() as u64,
            "graceful connection drain timed out, aborting stalled connections"
        );
        conn_tasks.shutdown().await;
    }

    state.sse_hub.close_all();
    state
        .ws_registry
        .close_all(1001, "Server shutting down")
        .await;

    #[cfg(feature = "http3")]
    if let Some((h3_shutdown_tx, join)) = h3_handle.take() {
        let _ = h3_shutdown_tx.send(true);
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(error = %e, "HTTP/3 endpoint shutdown error");
                return Err(Box::new(e));
            }
            Err(join_err) => {
                error!(error = %join_err, "HTTP/3 endpoint task panicked during shutdown");
                return Err(Box::new(io::Error::other(format!(
                    "HTTP/3 endpoint task failed: {join_err}"
                ))));
            }
        }
    }

    if let Some(ref logger) = state.access_logger {
        let logger = Arc::clone(logger);
        let _ = tokio::task::spawn_blocking(move || logger.flush(Duration::from_secs(2))).await;
    }

    Ok(())
}

struct IdleTimeoutStream<S> {
    inner: S,
    timeout: Duration,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    activity: bool,
}

impl<S> IdleTimeoutStream<S> {
    fn new(inner: S, timeout: Duration) -> Self {
        let deadline = (!timeout.is_zero()).then(|| Box::pin(tokio::time::sleep(timeout)));
        Self {
            inner,
            timeout,
            deadline,
            activity: false,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeoutStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > before {
                    self.activity = true;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => {
                let this = &mut *self;
                if let Some(ref mut sleep) = this.deadline
                    && sleep.as_mut().poll(cx).is_ready()
                {
                    if this.activity {
                        this.activity = false;
                        sleep
                            .as_mut()
                            .reset(tokio::time::Instant::now() + this.timeout);
                        if sleep.as_mut().poll(cx).is_ready() {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::TimedOut,
                                "HTTP/1 connection idle timeout",
                            )));
                        }
                    } else {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "HTTP/1 connection idle timeout",
                        )));
                    }
                }
                Poll::Pending
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeoutStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => {
                if n > 0 {
                    self.activity = true;
                }
                Poll::Ready(Ok(n))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

async fn drive_h1_upgradeable<I, S, B>(
    conn: http1::UpgradeableConnection<I, S>,
    mut shutdown_rx: watch::Receiver<()>,
) -> hyper::Result<()>
where
    S: hyper::service::HttpService<Incoming, ResBody = B>,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
    B: hyper::body::Body + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    tokio::pin!(conn);
    tokio::select! {
        res = &mut conn => res,
        _ = shutdown_rx.changed() => {
            conn.as_mut().graceful_shutdown();
            conn.await
        }
    }
}

fn configure_auto_builder(
    config: &ServerConfig,
    pipeline_flush: bool,
) -> AutoBuilder<TokioExecutor> {
    let mut builder = AutoBuilder::new(TokioExecutor::new());
    builder
        .http1()
        .keep_alive(true)
        .pipeline_flush(pipeline_flush)
        .max_buf_size(65_536)
        .timer(TokioTimer::new())
        .header_read_timeout(config.header_read_timeout);
    {
        let mut h2 = builder.http2();
        h2.timer(TokioTimer::new())
            .adaptive_window(config.h2_adaptive_window)
            .initial_stream_window_size(config.h2_initial_stream_window_size)
            .initial_connection_window_size(config.h2_initial_connection_window_size)
            .max_concurrent_streams(config.h2_max_concurrent_streams);
        if let Some(max_header_list_size) = config.h2_max_header_list_size {
            h2.max_header_list_size(max_header_list_size);
        }
        if let Some(interval_ms) = config.h2_keep_alive_interval_ms
            && interval_ms > 0
        {
            h2.keep_alive_interval(Some(Duration::from_millis(interval_ms)))
                .keep_alive_timeout(config.keepalive_timeout);
        }
    }
    builder
}

async fn serve_plain(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
    watcher: Watcher,
    h1_drain_rx: watch::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "plain TCP connection");
    let collector_shard = state.next_collector_shard();

    if !state.config.enable_h2c_prior_knowledge {
        let io = TokioIo::new(IdleTimeoutStream::new(
            stream,
            state.config.keepalive_timeout,
        ));
        let conn = http1::Builder::new()
            .keep_alive(true)
            .pipeline_flush(false)
            .max_buf_size(65_536)
            .timer(TokioTimer::new())
            .header_read_timeout(state.config.header_read_timeout)
            .serve_connection(
                io,
                service_fn(move |req: Request<Incoming>| {
                    let state = Arc::clone(&state);
                    async move { handle_with_peer_shard(req, peer, collector_shard, state).await }
                }),
            )
            .with_upgrades();
        drive_h1_upgradeable(conn, h1_drain_rx).await?;
        return Ok(());
    }

    let proto = tokio::time::timeout(
        state.config.header_read_timeout,
        detect_cleartext_protocol(&stream),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cleartext protocol peek timeout"))??;

    match proto {
        CleartextProtocol::H2cPriorKnowledge => {
            let io = TokioIo::new(stream);
            let builder = configure_auto_builder(&state.config, false).http2_only();
            let conn = builder.serve_connection(
                io,
                service_fn(move |req: Request<Incoming>| {
                    let state = Arc::clone(&state);
                    async move { handle_with_peer_shard(req, peer, collector_shard, state).await }
                }),
            );
            watcher.watch(conn.into_owned()).await?;
        }
        CleartextProtocol::Http1 => {
            let io = TokioIo::new(IdleTimeoutStream::new(
                stream,
                state.config.keepalive_timeout,
            ));
            let builder = configure_auto_builder(&state.config, false);
            let conn = builder.serve_connection_with_upgrades(
                io,
                service_fn(move |req: Request<Incoming>| {
                    let state = Arc::clone(&state);
                    async move { handle_with_peer_shard(req, peer, collector_shard, state).await }
                }),
            );
            watcher.watch(conn.into_owned()).await?;
        }
    }
    Ok(())
}

#[cfg(feature = "tls")]
async fn serve_tls(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
    acceptor: TlsAcceptor,
    watcher: Watcher,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "TLS connection");
    let collector_shard = state.next_collector_shard();
    let tls_stream = acceptor.accept(stream).await?;
    let io = TokioIo::new(tls_stream);

    let builder = configure_auto_builder(&state.config, true);
    let conn = builder.serve_connection_with_upgrades(
        io,
        service_fn(move |req: Request<Incoming>| {
            let state = Arc::clone(&state);
            async move { handle_with_peer_shard(req, peer, collector_shard, state).await }
        }),
    );
    watcher.watch(conn.into_owned()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_stream_extends_on_activity_and_times_out_when_idle() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut wrapped = IdleTimeoutStream::new(server, Duration::from_secs(5));

        client.write_all(b"ping").await.expect("write ping");
        let mut buf = [0u8; 4];
        wrapped.read_exact(&mut buf).await.expect("read ping");
        assert_eq!(&buf, b"ping");

        tokio::time::advance(Duration::from_secs(6)).await;
        let read_fut = wrapped.read(&mut buf);
        tokio::pin!(read_fut);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut read_fut)
                .await
                .is_err(),
            "first timer tick after activity must reset instead of timing out"
        );

        let err = read_fut
            .await
            .expect_err("second idle period with no activity must time out");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_stream_extends_on_write_activity() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut wrapped = IdleTimeoutStream::new(server, Duration::from_secs(5));

        wrapped.write_all(b"pong").await.expect("write pong");
        let mut client_buf = [0u8; 4];
        client
            .read_exact(&mut client_buf)
            .await
            .expect("client read pong");
        assert_eq!(&client_buf, b"pong");

        tokio::time::advance(Duration::from_secs(6)).await;
        let mut server_buf = [0u8; 4];
        let read_fut = wrapped.read(&mut server_buf);
        tokio::pin!(read_fut);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut read_fut)
                .await
                .is_err(),
            "write activity must extend the idle deadline"
        );
        let err = read_fut
            .await
            .expect_err("subsequent idle window with no activity must time out");
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_stream_zero_timeout_never_times_out() {
        let (_client, server) = tokio::io::duplex(64);
        let mut wrapped = IdleTimeoutStream::new(server, Duration::ZERO);
        let mut buf = [0u8; 4];
        assert!(
            tokio::time::timeout(Duration::from_secs(30), wrapped.read(&mut buf))
                .await
                .is_err(),
            "zero idle timeout must disable idle deadline"
        );
    }

    #[test]
    fn collector_shard_selection_pins_and_round_robins() {
        let (tx0, _rx0) = mpsc::channel(4);
        let (tx1, _rx1) = mpsc::channel(4);
        let (tx2, _rx2) = mpsc::channel(4);
        let state = ServerState::new(
            Arc::new(Router::from_entries(Vec::new()).unwrap()),
            Arc::new(Dispatcher::new_direct(4).0),
            Arc::new(ServerConfig::default()),
            vec![tx0, tx1, tx2],
        );

        let s0 = state.next_collector_shard();
        let s1 = state.next_collector_shard();
        let s2 = state.next_collector_shard();
        let s3 = state.next_collector_shard();
        assert_eq!([s0, s1, s2, s3], [0, 1, 2, 0]);
        assert!(
            state
                .collector_for_shard(1)
                .same_channel(state.collector_for_shard(4))
        );
    }
}
