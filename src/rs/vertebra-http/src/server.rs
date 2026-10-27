//! Hyper/Tokio accept loop.
//!
//! Supports tuned direct HTTP/1.1 (`http1::Builder`) on the default zero-peek
//! cleartext fastpath, optional cleartext HTTP/2 (`h2c` prior knowledge), and
//! ALPN-negotiated HTTP/1.1 + HTTP/2 over TLS.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use base64::Engine as _;
use bytes::Bytes;
use http::{HeaderName, HeaderValue};
use hyper::Request;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tracing::{debug, error, info};

use crate::access_log::AccessLogger;
use crate::assets::{AssetManager, StaticAssetServeConfig};
use crate::batcher::PendingRequest;
use crate::compression::CompressionOffloadConfig;
use crate::config::{OpenApiCacheConfig, ServerConfig, StaticMountConfig};
use crate::cors::CompiledCorsOffload;
use crate::discovery::WellKnownDiscoveryCache;
use crate::dispatcher::Dispatcher;
use crate::health::HealthEndpoint;
use crate::hmr::HmrState;
use crate::profile::ProfileEndpoint;
use crate::pubsub::PubSubBroker;
use crate::response::handle;
use crate::routing::Router;
use crate::sse::SseHub;
use crate::ws_mount::WsConnectionRegistry;

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
    pub discovery_cache: WellKnownDiscoveryCache,
    pub access_logger: Option<Arc<AccessLogger>>,
    pub sse_hub: Arc<SseHub>,
    pub pubsub_broker: Arc<PubSubBroker>,
    pub ws_registry: Arc<WsConnectionRegistry>,
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
            discovery_cache,
            access_logger,
            sse_hub,
            pubsub_broker,
            ws_registry,
        }
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

/// Run the HTTP server with sharded collector channels until the shutdown signal resolves.
pub async fn run_with_collector(
    config: Arc<ServerConfig>,
    router: Arc<Router>,
    dispatcher: Arc<Dispatcher>,
    collector_shards: Vec<tokio::sync::mpsc::Sender<PendingRequest>>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = bind_listener(config.bind, config.reuse_port)?;
    run_with_listener(
        listener,
        config,
        router,
        dispatcher,
        collector_shards,
        shutdown,
    )
    .await
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
    let bound_addr = listener.local_addr().unwrap_or(config.bind);
    info!(addr = %bound_addr, reuse_port = config.reuse_port, "vertebra-http listening");

    let state = Arc::new(ServerState::new(
        router,
        dispatcher,
        Arc::clone(&config),
        collector_shards,
    ));

    #[cfg(feature = "tls")]
    let tls_acceptor: Option<TlsAcceptor> = config
        .tls
        .as_ref()
        .map(|tc| TlsAcceptor::from_config(tc))
        .transpose()?;

    #[cfg(not(feature = "tls"))]
    let tls_acceptor: Option<()> = None;

    let _ = &tls_acceptor; // suppress unused warning in non-tls builds

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        let state = Arc::clone(&state);
                        #[cfg(feature = "tls")]
                        if let Some(ref tls_acc) = tls_acceptor {
                            let tls_acc = tls_acc.clone();
                            tokio::spawn(async move {
                                if let Err(e) = serve_tls(stream, peer, state, tls_acc).await {
                                    debug!(peer = %peer, error = %e, "TLS connection error");
                                }
                            });
                            continue;
                        }
                        tokio::spawn(async move {
                            if let Err(e) = serve_plain(stream, peer, state).await {
                                debug!(peer = %peer, error = %e, "HTTP connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "accept failed");
                    }
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received, stopping accept loop");
                break;
            }
        }
    }

    state
        .ws_registry
        .close_all(1001, "Server shutting down")
        .await;

    Ok(())
}

async fn serve_plain(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "plain TCP connection");

    if !state.config.enable_h2c_prior_knowledge {
        let io = TokioIo::new(stream);
        http1::Builder::new()
            .keep_alive(true)
            .pipeline_flush(false)
            .max_buf_size(65_536)
            .timer(TokioTimer::new())
            .serve_connection(
                io,
                service_fn(move |req: Request<Incoming>| {
                    let state = Arc::clone(&state);
                    async move { handle(req, state).await }
                }),
            )
            .with_upgrades()
            .await?;
        return Ok(());
    }

    let _ = crate::h2c::detect_cleartext_protocol(&stream).await?;
    let io = TokioIo::new(stream);

    let mut builder = AutoBuilder::new(TokioExecutor::new());
    builder.http1().pipeline_flush(false).max_buf_size(65_536);
    builder
        .http2()
        .adaptive_window(state.config.h2_adaptive_window)
        .initial_stream_window_size(state.config.h2_initial_stream_window_size)
        .initial_connection_window_size(state.config.h2_initial_connection_window_size)
        .max_concurrent_streams(state.config.h2_max_concurrent_streams);

    builder
        .serve_connection_with_upgrades(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await?;
    Ok(())
}

#[cfg(feature = "tls")]
async fn serve_tls(
    stream: TcpStream,
    peer: SocketAddr,
    state: Arc<ServerState>,
    acceptor: TlsAcceptor,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    debug!(peer = %peer, "TLS connection");
    let tls_stream = acceptor.accept(stream).await?;
    let io = TokioIo::new(tls_stream);
    let state = Arc::clone(&state);

    let mut builder = AutoBuilder::new(TokioExecutor::new());
    builder.http1().pipeline_flush(true);

    builder
        .serve_connection_with_upgrades(
            io,
            service_fn(move |req: Request<Incoming>| {
                let state = Arc::clone(&state);
                async move { handle(req, state).await }
            }),
        )
        .await?;
    Ok(())
}
