//! HTTP/3 over QUIC endpoint (`quinn 0.11.12`, `h3 0.0.8`, `h3-quinn 0.0.10`) and
//! `Alt-Svc` header advertisement.

#[cfg(feature = "http3")]
use std::convert::Infallible;
#[cfg(feature = "http3")]
use std::future::Future;
#[cfg(feature = "http3")]
use std::sync::Arc;
#[cfg(feature = "http3")]
use std::sync::atomic::Ordering;
#[cfg(feature = "http3")]
use std::time::Duration;

#[cfg(feature = "http3")]
use bytes::{Buf, BufMut, Bytes, BytesMut};
#[cfg(feature = "http3")]
use h3::ext::Protocol;
#[cfg(feature = "http3")]
use h3::proto::frame::Frame;
#[cfg(feature = "http3")]
use h3::quic::{
    Connection as H3QuicConnection, RecvStream as H3RecvStream, SendStream as H3SendStream,
    StreamId,
};
use http::{HeaderMap, HeaderValue};
#[cfg(feature = "http3")]
use http::{HeaderName, Method, Request, Response, StatusCode, header};
#[cfg(feature = "http3")]
use http_body_util::{BodyExt, Either, Full};
#[cfg(feature = "http3")]
use hyper::body::Body;
#[cfg(feature = "http3")]
use tokio::sync::{oneshot, watch};
#[cfg(feature = "http3")]
use tracing::{debug, warn};

#[cfg(feature = "http3")]
use crate::webtransport::{
    CloseCommand, WebTransportError, WebTransportRegistry, WebTransportSession, WebTransportStream,
};

#[cfg(feature = "http3")]
fn map_wt_error(err: WebTransportError) -> vertebra_core::bridge::WebTransportBridgeError {
    use vertebra_core::bridge::WebTransportBridgeError;
    match err {
        WebTransportError::SessionClosed(id) => WebTransportBridgeError::SessionClosed(id),
        WebTransportError::Stream(msg) => WebTransportBridgeError::Stream(msg),
        WebTransportError::Datagram(msg) => WebTransportBridgeError::Datagram(msg),
        WebTransportError::Protocol(msg) => WebTransportBridgeError::Protocol(msg),
    }
}

#[cfg(feature = "http3")]
impl vertebra_core::bridge::WebTransportStreamOps for WebTransportStream {
    fn stream_id(&self) -> u64 {
        WebTransportStream::stream_id(self)
    }

    fn recv(
        self: Arc<Self>,
        max_bytes: usize,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<Bytes, vertebra_core::bridge::WebTransportBridgeError>,
    > {
        Box::pin(async move {
            WebTransportStream::recv(&self, max_bytes)
                .await
                .map_err(map_wt_error)
        })
    }

    fn send(
        self: Arc<Self>,
        data: Bytes,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<(), vertebra_core::bridge::WebTransportBridgeError>,
    > {
        Box::pin(async move {
            WebTransportStream::send(&self, &data)
                .await
                .map_err(map_wt_error)
        })
    }

    fn finish(
        self: Arc<Self>,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<(), vertebra_core::bridge::WebTransportBridgeError>,
    > {
        Box::pin(async move {
            WebTransportStream::finish(&self)
                .await
                .map_err(map_wt_error)
        })
    }
}

#[cfg(feature = "http3")]
impl vertebra_core::bridge::WebTransportSessionOps for WebTransportSession {
    fn session_id(&self) -> u64 {
        WebTransportSession::session_id(self)
    }

    fn path(&self) -> &str {
        WebTransportSession::path(self)
    }

    fn headers(&self) -> &[(Vec<u8>, Vec<u8>)] {
        WebTransportSession::headers(self)
    }

    fn accept_bi(
        self: Arc<Self>,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<
            vertebra_core::bridge::WebTransportStreamHandle,
            vertebra_core::bridge::WebTransportBridgeError,
        >,
    > {
        Box::pin(async move {
            WebTransportSession::accept_bi(&self)
                .await
                .map(|stream| Arc::new(stream) as vertebra_core::bridge::WebTransportStreamHandle)
                .map_err(map_wt_error)
        })
    }

    fn open_bi(
        self: Arc<Self>,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<
            vertebra_core::bridge::WebTransportStreamHandle,
            vertebra_core::bridge::WebTransportBridgeError,
        >,
    > {
        Box::pin(async move {
            WebTransportSession::open_bi(&self)
                .await
                .map(|stream| Arc::new(stream) as vertebra_core::bridge::WebTransportStreamHandle)
                .map_err(map_wt_error)
        })
    }

    fn recv_datagram(
        self: Arc<Self>,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<Bytes, vertebra_core::bridge::WebTransportBridgeError>,
    > {
        Box::pin(async move {
            WebTransportSession::recv_datagram(&self)
                .await
                .map_err(map_wt_error)
        })
    }

    fn send_datagram(
        &self,
        data: &[u8],
    ) -> Result<(), vertebra_core::bridge::WebTransportBridgeError> {
        WebTransportSession::send_datagram(self, data).map_err(map_wt_error)
    }

    fn close(
        self: Arc<Self>,
        error_code: u32,
        reason: String,
    ) -> vertebra_core::bridge::WebTransportFuture<
        Result<(), vertebra_core::bridge::WebTransportBridgeError>,
    > {
        Box::pin(async move {
            WebTransportSession::close(&self, error_code, &reason)
                .await
                .map_err(map_wt_error)
        })
    }
}

#[cfg(feature = "http3")]
struct WebTransportDispatch<W: ?Sized> {
    is_ready: Arc<dyn Fn() -> bool + Send + Sync>,
    on_session: Arc<W>,
}

#[cfg(feature = "http3")]
impl<W: ?Sized> Clone for WebTransportDispatch<W> {
    fn clone(&self) -> Self {
        Self {
            is_ready: Arc::clone(&self.is_ready),
            on_session: Arc::clone(&self.on_session),
        }
    }
}

/// Configuration for the HTTP/3 over QUIC UDP listener and `Alt-Svc` advertisement.
#[derive(Debug, Clone)]
pub struct Http3Config {
    /// Whether the HTTP/3 UDP QUIC listener is enabled.
    pub enabled: bool,
    /// `ma=` max-age parameter in seconds advertised in `Alt-Svc` headers (default: `86400`).
    pub alt_svc_max_age: u64,
    /// Maximum concurrent bidirectional QUIC streams per connection.
    pub max_concurrent_bidi_streams: u32,
    /// QUIC keep-alive ping interval in milliseconds (`0` disables keep-alive).
    pub keep_alive_interval_ms: u64,
    /// Whether WebTransport over HTTP/3 extended `CONNECT` is enabled (default: `true`).
    pub enable_webtransport: bool,
    /// Maximum concurrent WebTransport sessions per HTTP/3 connection (default: `64`).
    pub max_webtransport_sessions: u64,
}

impl Default for Http3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            alt_svc_max_age: 86_400,
            max_concurrent_bidi_streams: 256,
            keep_alive_interval_ms: 15_000,
            enable_webtransport: true,
            max_webtransport_sessions: 64,
        }
    }
}

/// Format an `Alt-Svc` header value advertising HTTP/3 on `port` with `max_age` seconds.
pub fn format_alt_svc_header(port: u16, max_age: u64) -> String {
    format!("h3=\":{port}\"; ma={max_age}")
}

/// Inject `Alt-Svc: h3=":<port>"; ma=<max_age>` into `headers` if not already present.
pub fn inject_alt_svc_header(headers: &mut HeaderMap, port: u16, max_age: u64) {
    if headers.contains_key(http::header::ALT_SVC) {
        return;
    }
    if let Ok(val) = HeaderValue::from_str(&format_alt_svc_header(port, max_age)) {
        headers.insert(http::header::ALT_SVC, val);
    }
}

/// Errors returned by the HTTP/3 QUIC subsystem.
#[derive(Debug, thiserror::Error)]
pub enum Http3Error {
    #[error("I/O error on QUIC UDP socket: {0}")]
    Io(#[from] std::io::Error),
    #[error("TLS configuration error for QUIC: {0}")]
    Tls(String),
    #[error("HTTP/3 connection error: {0}")]
    H3(String),
}

/// Build a `quinn::ServerConfig` with ALPN `b"h3"` and configured QUIC transport parameters.
#[cfg(feature = "http3")]
pub fn build_quic_server_config(
    mut tls_config: rustls::ServerConfig,
    cfg: &Http3Config,
) -> Result<quinn::ServerConfig, Http3Error> {
    tls_config.alpn_protocols = vec![b"h3".to_vec()];
    let quic_tls = quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
        .map_err(|e| Http3Error::Tls(e.to_string()))?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_tls));
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(cfg.max_concurrent_bidi_streams.into());
    if cfg.keep_alive_interval_ms > 0 {
        transport.keep_alive_interval(Some(Duration::from_millis(cfg.keep_alive_interval_ms)));
    }
    if cfg.enable_webtransport {
        transport.datagram_receive_buffer_size(Some(65_536));
        transport.datagram_send_buffer_size(65_536);
    }
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

#[cfg(feature = "http3")]
const H3_CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(feature = "http3")]
const H3_ENDPOINT_IDLE_TIMEOUT: Duration = Duration::from_millis(500);

#[cfg(feature = "http3")]
const HOP_BY_HOP_HEADERS: [HeaderName; 5] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

/// Strip HTTP/1.x connection-specific hop-by-hop headers forbidden by RFC 9114 §4.2 on HTTP/3.
#[cfg(feature = "http3")]
pub fn strip_hop_by_hop_headers(headers: &mut HeaderMap) {
    for name in &HOP_BY_HOP_HEADERS {
        headers.remove(name);
    }
}

/// Run the HTTP/3 over QUIC accept loop on `endpoint` until `shutdown_rx` signals `true`
/// or closes, then cleanly drain active connections before closing the endpoint.
#[cfg(feature = "http3")]
pub async fn serve_http3_endpoint<F, Fut, B>(
    endpoint: quinn::Endpoint,
    alt_svc_port: u16,
    cfg: Http3Config,
    max_body_size: usize,
    handler: Arc<F>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    serve_http3_inner(
        endpoint,
        alt_svc_port,
        cfg,
        max_body_size,
        handler,
        None::<WebTransportDispatch<fn(WebTransportSession)>>,
        shutdown_rx,
    )
    .await
}

/// Run the HTTP/3 over QUIC accept loop bridging WebTransport sessions through `BatchBridge`
/// when `embedded_topology` is `true` (responding `501 Not Implemented` otherwise or when
/// no embedded sink is attached).
#[cfg(feature = "http3")]
pub async fn serve_http3_with_bridge<F, Fut, B>(
    endpoint: quinn::Endpoint,
    alt_svc_port: u16,
    cfg: Http3Config,
    max_body_size: usize,
    handler: Arc<F>,
    embedded_topology: bool,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    let wt_dispatch = embedded_topology.then(|| WebTransportDispatch {
        is_ready: Arc::new(|| {
            vertebra_core::bridge::BatchBridge::get()
                .is_some_and(vertebra_core::bridge::BatchBridge::has_webtransport_sink)
        }),
        on_session: Arc::new(|session: WebTransportSession| {
            if let Some(bridge) = vertebra_core::bridge::BatchBridge::get() {
                let _ = bridge.submit_webtransport_session(Arc::new(session));
            }
        }),
    });
    serve_http3_inner(
        endpoint,
        alt_svc_port,
        cfg,
        max_body_size,
        handler,
        wt_dispatch,
        shutdown_rx,
    )
    .await
}

/// Run the HTTP/3 over QUIC accept loop with optional WebTransport session handling.
#[cfg(feature = "http3")]
pub async fn serve_http3_with_webtransport<F, Fut, B, W>(
    endpoint: quinn::Endpoint,
    cfg: Http3Config,
    max_body_size: usize,
    handler: Arc<F>,
    wt_handler: Option<Arc<W>>,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
    W: Fn(WebTransportSession) + Send + Sync + 'static,
{
    let alt_svc_port = endpoint.local_addr().map(|a| a.port()).unwrap_or(0);
    let wt_dispatch = wt_handler.map(|on_session| WebTransportDispatch {
        is_ready: Arc::new(|| true),
        on_session,
    });
    serve_http3_inner(
        endpoint,
        alt_svc_port,
        cfg,
        max_body_size,
        handler,
        wt_dispatch,
        shutdown_rx,
    )
    .await
}

#[cfg(feature = "http3")]
async fn serve_http3_inner<F, Fut, B, W>(
    endpoint: quinn::Endpoint,
    alt_svc_port: u16,
    cfg: Http3Config,
    max_body_size: usize,
    handler: Arc<F>,
    wt_dispatch: Option<WebTransportDispatch<W>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
    W: Fn(WebTransportSession) + Send + Sync + 'static,
{
    let mut conn_tasks = tokio::task::JoinSet::new();
    if !*shutdown_rx.borrow_and_update() {
        loop {
            tokio::select! {
                maybe_incoming = endpoint.accept() => {
                    let Some(incoming) = maybe_incoming else {
                        break;
                    };
                    let handler = Arc::clone(&handler);
                    let wt_dispatch = wt_dispatch.clone();
                    let conn_cfg = cfg.clone();
                    let conn_shutdown = shutdown_rx.clone();
                    conn_tasks.spawn(async move {
                        match incoming.await {
                            Ok(quic_conn) => {
                                if let Err(e) = serve_h3_connection(
                                    quic_conn,
                                    alt_svc_port,
                                    conn_cfg,
                                    max_body_size,
                                    handler,
                                    wt_dispatch,
                                    conn_shutdown,
                                ).await {
                                    debug!(error = %e, "HTTP/3 connection closed");
                                }
                            }
                            Err(e) => {
                                debug!(error = %e, "QUIC handshake failed");
                            }
                        }
                    });
                }
                Some(res) = conn_tasks.join_next(), if !conn_tasks.is_empty() => {
                    if let Err(e) = res {
                        warn!(error = %e, "HTTP/3 connection task panicked");
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow_and_update() {
                        break;
                    }
                }
            }
        }
    }

    let drain_deadline = tokio::time::sleep(H3_CONNECTION_DRAIN_TIMEOUT);
    tokio::pin!(drain_deadline);
    while !conn_tasks.is_empty() {
        tokio::select! {
            res = conn_tasks.join_next() => {
                if let Some(Err(e)) = res {
                    warn!(error = %e, "HTTP/3 connection task panicked during shutdown");
                }
            }
            _ = &mut drain_deadline => {
                conn_tasks.abort_all();
                break;
            }
        }
    }

    endpoint.close(0u32.into(), b"server shutting down");
    let _ = tokio::time::timeout(H3_ENDPOINT_IDLE_TIMEOUT, endpoint.wait_idle()).await;
    Ok(())
}

#[cfg(feature = "http3")]
async fn serve_h3_connection<F, Fut, B, W>(
    quic_conn: quinn::Connection,
    alt_svc_port: u16,
    cfg: Http3Config,
    max_body_size: usize,
    handler: Arc<F>,
    wt_dispatch: Option<WebTransportDispatch<W>>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
    W: Fn(WebTransportSession) + Send + Sync + 'static,
{
    let raw_quic_conn = quic_conn.clone();
    let mut h3_builder = h3::server::builder();
    h3_builder
        .enable_webtransport(cfg.enable_webtransport)
        .enable_extended_connect(true)
        .enable_datagram(cfg.enable_webtransport)
        .max_webtransport_sessions(if cfg.enable_webtransport {
            cfg.max_webtransport_sessions
        } else {
            0
        });

    let mut h3_conn: h3::server::Connection<h3_quinn::Connection, Bytes> = h3_builder
        .build(h3_quinn::Connection::new(quic_conn))
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;

    let opener = <h3_quinn::Connection as H3QuicConnection<Bytes>>::opener(&h3_conn.inner.conn);
    let wt_registry = (cfg.enable_webtransport && wt_dispatch.is_some())
        .then(|| Arc::new(WebTransportRegistry::new(cfg.max_webtransport_sessions)));
    let (conn_close_tx, conn_close_rx) = watch::channel(false);

    let mut stream_tasks = tokio::task::JoinSet::new();
    let mut datagrams_active = wt_registry.is_some();

    if *shutdown_rx.borrow_and_update() {
        let _ = h3_conn.shutdown(0).await;
    } else {
        loop {
            tokio::select! {
                accept_res = h3_conn.accept() => {
                    match accept_res {
                        Ok(Some(mut resolver)) => {
                            let handler = Arc::clone(&handler);
                            let wt_dispatch = wt_dispatch.clone();
                            let wt_registry = wt_registry.as_ref().map(Arc::clone);
                            let raw_quic_conn = raw_quic_conn.clone();
                            let opener = opener.clone();
                            let conn_close_rx = conn_close_rx.clone();
                            let alt_svc_max_age = cfg.alt_svc_max_age;
                            let wt_enabled = cfg.enable_webtransport;
                            stream_tasks.spawn(async move {
                                let first_frame = std::future::poll_fn(|cx| {
                                    resolver.frame_stream.poll_next(cx)
                                })
                                .await;
                                if let Ok(Some(Frame::WebTransportStream(wt_sid))) = first_frame {
                                    let session_id = StreamId::from(wt_sid).into_inner();
                                    let mut buf_stream = resolver.frame_stream.into_inner();
                                    if let Some(ref registry) = wt_registry {
                                        if !registry.dispatch_bidi_stream(session_id, buf_stream) {
                                            debug!(session_id, "dropped WebTransport bidi stream");
                                        }
                                    } else {
                                        H3RecvStream::stop_sending(
                                            &mut buf_stream,
                                            h3::error::Code::H3_FRAME_UNEXPECTED.value(),
                                        );
                                        H3SendStream::reset(
                                            &mut buf_stream,
                                            h3::error::Code::H3_FRAME_UNEXPECTED.value(),
                                        );
                                    }
                                    return;
                                }

                                let resolved = match resolver.accept_with_frame(first_frame) {
                                    Ok(r) => r,
                                    Err(e) => {
                                        debug!(error = %e, "failed accepting HTTP/3 request frame");
                                        return;
                                    }
                                };
                                match resolved.resolve().await {
                                    Ok((req, stream)) => {
                                        let is_wt_connect = req.method() == Method::CONNECT
                                            && req.extensions().get::<Protocol>()
                                                == Some(&Protocol::WEB_TRANSPORT);
                                        if is_wt_connect {
                                            if let Err(e) = handle_webtransport_connect(
                                                req,
                                                stream,
                                                alt_svc_port,
                                                alt_svc_max_age,
                                                wt_enabled,
                                                wt_dispatch,
                                                wt_registry,
                                                raw_quic_conn,
                                                opener,
                                                conn_close_rx,
                                            )
                                            .await
                                            {
                                                warn!(error = %e, "failed handling WebTransport CONNECT stream");
                                            }
                                        } else if let Err(e) = handle_h3_request(
                                            req,
                                            stream,
                                            alt_svc_port,
                                            alt_svc_max_age,
                                            max_body_size,
                                            handler,
                                        )
                                        .await
                                        {
                                            warn!(error = %e, "failed handling HTTP/3 request stream");
                                        }
                                    }
                                    Err(e) => {
                                        debug!(error = %e, "failed resolving HTTP/3 request");
                                    }
                                }
                            });
                        }
                        Ok(None) => break,
                        Err(e) => {
                            debug!(error = %e, "HTTP/3 accept loop terminated");
                            break;
                        }
                    }
                }
                dgram_res = raw_quic_conn.read_datagram(), if datagrams_active => {
                    match dgram_res {
                        Ok(raw) => {
                            if let Some(ref registry) = wt_registry {
                                let _ = registry.dispatch_datagram(raw);
                            }
                        }
                        Err(_) => {
                            datagrams_active = false;
                        }
                    }
                }
                Some(res) = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                    if let Err(e) = res {
                        warn!(error = %e, "HTTP/3 request task panicked");
                    }
                }
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow_and_update() {
                        let _ = h3_conn.shutdown(0).await;
                        break;
                    }
                }
            }
        }
    }

    if let Some(ref registry) = wt_registry {
        registry.clear();
    }
    let _ = conn_close_tx.send(true);

    while let Some(res) = stream_tasks.join_next().await {
        if let Err(e) = res {
            warn!(error = %e, "HTTP/3 request task panicked during drain");
        }
    }
    Ok(())
}

#[cfg(feature = "http3")]
#[allow(clippy::too_many_arguments)]
async fn handle_webtransport_connect<W>(
    req: Request<()>,
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    alt_svc_port: u16,
    alt_svc_max_age: u64,
    wt_enabled: bool,
    wt_dispatch: Option<WebTransportDispatch<W>>,
    wt_registry: Option<Arc<WebTransportRegistry>>,
    quic_conn: quinn::Connection,
    opener: h3_quinn::OpenStreams,
    mut conn_close_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    W: Fn(WebTransportSession) + Send + Sync + 'static,
{
    let (Some(dispatch), Some(registry)) = (
        wt_enabled
            .then_some(wt_dispatch)
            .flatten()
            .filter(|d| (d.is_ready)()),
        wt_registry,
    ) else {
        warn!(
            session_id = stream.id().into_inner(),
            wt_enabled,
            "WebTransport CONNECT rejected with 501 Not Implemented: disabled, Shm topology, or no embedded sink attached"
        );
        let mut resp = Response::builder()
            .status(StatusCode::NOT_IMPLEMENTED)
            .body(())
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        inject_alt_svc_header(resp.headers_mut(), alt_svc_port, alt_svc_max_age);
        stream
            .send_response(resp)
            .await
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        stream
            .finish()
            .await
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        return Ok(());
    };

    let session_id = stream.id().into_inner();
    let Some(registration) = registry.register(session_id) else {
        let mut resp = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(())
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        inject_alt_svc_header(resp.headers_mut(), alt_svc_port, alt_svc_max_age);
        stream
            .send_response(resp)
            .await
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        stream
            .finish()
            .await
            .map_err(|e| Http3Error::H3(e.to_string()))?;
        return Ok(());
    };

    let closed_flag = Arc::clone(&registration.closed);
    let resp = Response::builder()
        .status(StatusCode::OK)
        .body(())
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    if let Err(e) = stream.send_response(resp).await {
        registry.deregister(session_id);
        return Err(Http3Error::H3(e.to_string()));
    }

    let path = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let headers = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect();
    let (close_tx, mut close_rx) = oneshot::channel::<CloseCommand>();
    let session = WebTransportSession::new(
        session_id,
        path,
        headers,
        registration,
        quic_conn,
        opener,
        Arc::clone(&registry),
        close_tx,
    );
    (dispatch.on_session)(session);

    if !*conn_close_rx.borrow_and_update() {
        loop {
            tokio::select! {
                recv_res = stream.recv_data() => {
                    match recv_res {
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => break,
                    }
                }
                close_cmd = &mut close_rx => {
                    registry.deregister(session_id);
                    closed_flag.store(true, Ordering::Release);
                    if let Ok((error_code, _reason, ack_tx)) = close_cmd {
                        if error_code == 0 {
                            let _ = stream.finish().await;
                        } else {
                            stream.stop_sending(h3::error::Code::H3_NO_ERROR);
                            stream.stop_stream(h3::error::Code::H3_NO_ERROR);
                        }
                        let _ = ack_tx.send(());
                        return Ok(());
                    }
                    let _ = stream.finish().await;
                    return Ok(());
                }
                changed = conn_close_rx.changed() => {
                    if changed.is_err() || *conn_close_rx.borrow_and_update() {
                        break;
                    }
                }
            }
        }
    }

    registry.deregister(session_id);
    closed_flag.store(true, Ordering::Release);
    let _ = stream.finish().await;
    Ok(())
}

#[cfg(feature = "http3")]
async fn send_payload_too_large(
    stream: &mut h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    alt_svc_port: u16,
    alt_svc_max_age: u64,
) -> Result<(), Http3Error> {
    stream.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);
    let mut head_resp = Response::builder()
        .status(StatusCode::PAYLOAD_TOO_LARGE)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )
        .body(())
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    inject_alt_svc_header(head_resp.headers_mut(), alt_svc_port, alt_svc_max_age);
    stream
        .send_response(head_resp)
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    stream
        .send_data(Bytes::from_static(b"Payload Too Large"))
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    stream
        .finish()
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    Ok(())
}

/// Convert an `h3` request and `RequestStream` into a `hyper::Request<Bytes>`, invoke
/// `handler` after enforcing `max_body_size` before buffering, and write the resulting
/// response body and optional HTTP/3 trailers back over the QUIC stream.
#[cfg(feature = "http3")]
pub async fn handle_h3_request<F, Fut, B>(
    req: Request<()>,
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    alt_svc_port: u16,
    alt_svc_max_age: u64,
    max_body_size: usize,
    handler: Arc<F>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    let (parts, ()) = req.into_parts();
    if let Some(len) = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<usize>().ok())
        && len > max_body_size
    {
        return send_payload_too_large(&mut stream, alt_svc_port, alt_svc_max_age).await;
    }

    let mut body_buf = BytesMut::new();
    while let Some(chunk) = stream
        .recv_data()
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?
    {
        let chunk_len = chunk.remaining();
        if chunk_len > max_body_size.saturating_sub(body_buf.len()) {
            return send_payload_too_large(&mut stream, alt_svc_port, alt_svc_max_age).await;
        }
        body_buf.put(chunk);
    }

    let full_req = Request::from_parts(parts, body_buf.freeze());
    let resp = handler(full_req).await;
    let (mut resp_parts, resp_body) = resp.into_parts();
    strip_hop_by_hop_headers(&mut resp_parts.headers);
    inject_alt_svc_header(&mut resp_parts.headers, alt_svc_port, alt_svc_max_age);

    let head_resp = Response::from_parts(resp_parts, ());
    stream
        .send_response(head_resp)
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;

    match resp_body {
        Either::Left(mut full) => {
            while let Some(Ok(frame)) = full.frame().await {
                if let Ok(data) = frame.into_data()
                    && !data.is_empty()
                {
                    stream
                        .send_data(data)
                        .await
                        .map_err(|e| Http3Error::H3(e.to_string()))?;
                }
            }
        }
        Either::Right(mut streaming) => {
            while let Some(Ok(frame)) = streaming.frame().await {
                match frame.into_data() {
                    Ok(data) => {
                        if !data.is_empty() {
                            stream
                                .send_data(data)
                                .await
                                .map_err(|e| Http3Error::H3(e.to_string()))?;
                        }
                    }
                    Err(frame) => {
                        if let Ok(trailers) = frame.into_trailers()
                            && !trailers.is_empty()
                        {
                            stream
                                .send_trailers(trailers)
                                .await
                                .map_err(|e| Http3Error::H3(e.to_string()))?;
                        }
                    }
                }
            }
        }
    }

    stream
        .finish()
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?;
    Ok(())
}
