//! HTTP/3 over QUIC endpoint (`quinn 0.11.9`, `h3 0.0.8`, `h3-quinn 0.0.10`) and
//! `Alt-Svc` header advertisement.

use std::convert::Infallible;
#[cfg(feature = "http3")]
use std::future::Future;
use std::pin::Pin;
#[cfg(feature = "http3")]
use std::sync::Arc;
use std::task::{Context, Poll};
#[cfg(feature = "http3")]
use std::time::Duration;

use bytes::Bytes;
#[cfg(feature = "http3")]
use bytes::{Buf, BytesMut};
use http::{HeaderMap, HeaderValue};
#[cfg(feature = "http3")]
use http::{Request, Response};
#[cfg(feature = "http3")]
use http_body_util::{BodyExt, Either, Full};
use hyper::body::{Body, Frame};
use tokio::sync::mpsc;
#[cfg(feature = "http3")]
use tokio::sync::watch;
#[cfg(feature = "http3")]
use tracing::{debug, warn};

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
}

impl Default for Http3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            alt_svc_max_age: 86_400,
            max_concurrent_bidi_streams: 256,
            keep_alive_interval_ms: 15_000,
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

/// Stream body backed by a bounded `tokio::sync::mpsc::Receiver<Result<Frame<Bytes>, Infallible>>`.
pub struct H3ChannelBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl H3ChannelBody {
    /// Wrap a frame receiver into an `http_body::Body`.
    pub fn new(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> Self {
        Self { rx }
    }
}

impl Body for H3ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.rx.poll_recv(cx)
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
    server_config.transport_config(Arc::new(transport));
    Ok(server_config)
}

/// Run the HTTP/3 over QUIC accept loop on `endpoint` until `shutdown_rx` signals `true`.
#[cfg(feature = "http3")]
pub async fn serve_http3_endpoint<F, Fut, B>(
    endpoint: quinn::Endpoint,
    alt_svc_port: u16,
    cfg: Http3Config,
    handler: Arc<F>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    loop {
        tokio::select! {
            maybe_incoming = endpoint.accept() => {
                let Some(incoming) = maybe_incoming else {
                    break;
                };
                let handler = Arc::clone(&handler);
                let alt_svc_max_age = cfg.alt_svc_max_age;
                tokio::spawn(async move {
                    match incoming.await {
                        Ok(quic_conn) => {
                            if let Err(e) = serve_h3_connection(
                                quic_conn,
                                alt_svc_port,
                                alt_svc_max_age,
                                handler,
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
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    endpoint.close(0u32.into(), b"server shutting down");
                    break;
                }
            }
        }
    }
    Ok(())
}

#[cfg(feature = "http3")]
async fn serve_h3_connection<F, Fut, B>(
    quic_conn: quinn::Connection,
    alt_svc_port: u16,
    alt_svc_max_age: u64,
    handler: Arc<F>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    let mut h3_conn: h3::server::Connection<h3_quinn::Connection, Bytes> =
        h3::server::Connection::new(h3_quinn::Connection::new(quic_conn))
            .await
            .map_err(|e| Http3Error::H3(e.to_string()))?;

    loop {
        match h3_conn.accept().await {
            Ok(Some(resolver)) => {
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    match resolver.resolve_request().await {
                        Ok((req, stream)) => {
                            if let Err(e) = handle_h3_request(
                                req,
                                stream,
                                alt_svc_port,
                                alt_svc_max_age,
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
    Ok(())
}

/// Convert an `h3` request and `RequestStream` into a `hyper::Request<Bytes>`, invoke
/// `handler`, and write the resulting `Either<Full<Bytes>, B>` body and optional HTTP/3
/// trailers back over the QUIC stream.
#[cfg(feature = "http3")]
pub async fn handle_h3_request<F, Fut, B>(
    req: Request<()>,
    mut stream: h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
    alt_svc_port: u16,
    alt_svc_max_age: u64,
    handler: Arc<F>,
) -> Result<(), Http3Error>
where
    F: Fn(Request<Bytes>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Response<Either<Full<Bytes>, B>>> + Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Send + Unpin + 'static,
{
    let (parts, ()) = req.into_parts();
    let mut body_buf = BytesMut::new();
    while let Some(mut chunk) = stream
        .recv_data()
        .await
        .map_err(|e| Http3Error::H3(e.to_string()))?
    {
        body_buf.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }

    let full_req = Request::from_parts(parts, body_buf.freeze());
    let resp = handler(full_req).await;
    let (mut resp_parts, resp_body) = resp.into_parts();
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
