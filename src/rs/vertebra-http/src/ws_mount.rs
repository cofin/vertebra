//! RFC 6455 WebSocket upgrade negotiation, `WsConnectionRegistry`, pre-upgrade
//! `WebSocketHandshakeDecision` (`websocket.http.response`), frame/message size
//! limits, and idle ping/pong watchdog timers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{Method, Request, Response, StatusCode, Version};
use http_body_util::Full;
use hyper::body::Incoming;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};

/// Configuration for WebSocket frame limits, compression, and ping/pong watchdog timers.
#[derive(Debug, Clone)]
pub struct WsConfig {
    pub max_frame_size: usize,
    pub max_message_size: usize,
    pub ping_interval: Duration,
    pub idle_timeout: Duration,
    pub enable_permessage_deflate: bool,
    pub fast_connect: bool,
}

impl Default for WsConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 16 * 1024 * 1024,
            max_message_size: 64 * 1024 * 1024,
            ping_interval: Duration::from_secs(20),
            idle_timeout: Duration::from_secs(60),
            enable_permessage_deflate: true,
            fast_connect: false,
        }
    }
}

impl WsConfig {
    /// Convert limits into a `tungstenite::protocol::WebSocketConfig`.
    pub fn to_tungstenite_config(&self) -> WebSocketConfig {
        let mut cfg = WebSocketConfig::default();
        cfg.max_frame_size = Some(self.max_frame_size);
        cfg.max_message_size = Some(self.max_message_size);
        cfg
    }
}

/// Outcome of the pre-upgrade ASGI/RSGI handshake phase.
#[derive(Debug, Clone)]
pub enum WebSocketHandshakeDecision {
    /// Accept the WebSocket upgrade (`101 Switching Protocols`).
    Accept {
        subprotocol: Option<String>,
        headers: Vec<(String, String)>,
    },
    /// Reject the handshake before upgrading (`websocket.http.response`).
    Reject {
        status: u16,
        headers: Vec<(String, String)>,
        body: Bytes,
    },
}

/// Inbound frame forwarded from the WebSocket reader loop to the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsIncoming {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, Option<String>),
}

/// Outbound frame sent from the application to the WebSocket writer loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsOutgoing {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(u16, String),
}

/// Metadata and control handle for an active WebSocket connection.
#[derive(Debug, Clone)]
pub struct WsConnectionHandle {
    pub connection_id: u64,
    pub path: String,
    pub subprotocol: Option<String>,
    pub connected_at: Instant,
    close_tx: mpsc::Sender<(u16, String)>,
}

/// Thread-safe registry of all active WebSocket connections.
#[derive(Debug, Default)]
pub struct WsConnectionRegistry {
    next_id: AtomicU64,
    connections: DashMap<u64, WsConnectionHandle>,
}

impl WsConnectionRegistry {
    /// Create an empty connection registry.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            connections: DashMap::new(),
        }
    }

    /// Register a newly established WebSocket connection.
    pub fn register(
        &self,
        path: String,
        subprotocol: Option<String>,
        close_tx: mpsc::Sender<(u16, String)>,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let handle = WsConnectionHandle {
            connection_id: id,
            path,
            subprotocol,
            connected_at: Instant::now(),
            close_tx,
        };
        self.connections.insert(id, handle);
        id
    }

    /// Remove a connection from the registry on disconnect.
    pub fn unregister(&self, connection_id: u64) {
        self.connections.remove(&connection_id);
    }

    /// Return the number of currently active WebSocket connections.
    pub fn active_count(&self) -> usize {
        self.connections.len()
    }

    /// Broadcast a close frame to all registered connections during graceful shutdown.
    pub async fn close_all(&self, code: u16, reason: &str) {
        let handles: Vec<mpsc::Sender<(u16, String)>> = self
            .connections
            .iter()
            .map(|entry| entry.value().close_tx.clone())
            .collect();
        for tx in handles {
            let _ = tx.send((code, reason.to_owned())).await;
        }
    }
}

/// Return `true` if the HTTP request is a valid RFC 6455 WebSocket upgrade request on any route.
pub fn is_websocket_upgrade(req: &Request<Incoming>) -> bool {
    is_websocket_upgrade_parts(req.method(), req.version(), req.headers())
}

/// Validate RFC 6455 upgrade method, HTTP version, and headers.
pub fn is_websocket_upgrade_parts(method: &Method, version: Version, headers: &HeaderMap) -> bool {
    method == Method::GET
        && version >= Version::HTTP_11
        && has_header_token(headers, header::CONNECTION, "upgrade")
        && has_header_token(headers, header::UPGRADE, "websocket")
        && has_header_token(headers, header::SEC_WEBSOCKET_VERSION, "13")
        && headers.contains_key(header::SEC_WEBSOCKET_KEY)
}

fn has_header_token(headers: &HeaderMap, name: HeaderName, expected: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split([',', ' ']))
        .any(|token| token.trim().eq_ignore_ascii_case(expected))
}

/// Validate the `Origin` header against an allowlist of origins (empty allowlist allows all).
pub fn validate_origin(headers: &HeaderMap, allowed_origins: &[&str]) -> bool {
    if allowed_origins.is_empty() || allowed_origins.contains(&"*") {
        return true;
    }
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    allowed_origins
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(origin.trim()))
}

/// Negotiate the first client-offered `Sec-WebSocket-Protocol` present in `supported`.
pub fn negotiate_subprotocol(headers: &HeaderMap, supported: &[&str]) -> Option<String> {
    let requested: Vec<&str> = headers
        .get_all(header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    for candidate in requested {
        if supported.iter().any(|s| s.eq_ignore_ascii_case(candidate)) {
            return Some(candidate.to_owned());
        }
    }
    None
}

/// Return `true` if the client offered `permessage-deflate` in `Sec-WebSocket-Extensions`.
pub fn client_offers_permessage_deflate(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::SEC_WEBSOCKET_EXTENSIONS)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|ext| ext.to_ascii_lowercase().contains("permessage-deflate"))
}

/// Construct either a `101 Switching Protocols` response or a pre-upgrade HTTP rejection response.
pub fn build_handshake_response(
    sec_websocket_key: &str,
    negotiate_deflate: bool,
    decision: WebSocketHandshakeDecision,
) -> Response<Full<Bytes>> {
    match decision {
        WebSocketHandshakeDecision::Accept {
            subprotocol,
            headers,
        } => {
            let accept_key = derive_accept_key(sec_websocket_key.as_bytes());
            let mut builder = Response::builder()
                .status(StatusCode::SWITCHING_PROTOCOLS)
                .header(header::CONNECTION, HeaderValue::from_static("Upgrade"))
                .header(header::UPGRADE, HeaderValue::from_static("websocket"))
                .header(header::SEC_WEBSOCKET_ACCEPT, accept_key);

            if let Some(proto) = subprotocol
                && let Ok(val) = HeaderValue::from_str(&proto)
            {
                builder = builder.header(header::SEC_WEBSOCKET_PROTOCOL, val);
            }

            if negotiate_deflate {
                builder = builder.header(
                    header::SEC_WEBSOCKET_EXTENSIONS,
                    HeaderValue::from_static(
                        "permessage-deflate; client_no_context_takeover; server_no_context_takeover",
                    ),
                );
            }

            for (name, value) in headers {
                if let (Ok(h_name), Ok(h_val)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(&value),
                ) {
                    builder = builder.header(h_name, h_val);
                }
            }

            builder
                .body(Full::new(Bytes::new()))
                .expect("valid 101 Switching Protocols response")
        }
        WebSocketHandshakeDecision::Reject {
            status,
            headers,
            body,
        } => {
            let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::FORBIDDEN);
            let mut builder = Response::builder().status(status_code);
            for (name, value) in headers {
                if let (Ok(h_name), Ok(h_val)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(&value),
                ) {
                    builder = builder.header(h_name, h_val);
                }
            }
            builder
                .body(Full::new(body))
                .expect("valid pre-upgrade HTTP rejection response")
        }
    }
}

/// Drive an upgraded `WebSocketStream`, enforcing frame/message size limits,
/// periodic ping keepalives, idle timeout closure (`1001 Going Away`), and
/// registry lifecycle tracking.
pub async fn run_ws_connection<S>(
    mut ws: WebSocketStream<S>,
    path: String,
    subprotocol: Option<String>,
    config: WsConfig,
    registry: Arc<WsConnectionRegistry>,
    incoming_tx: mpsc::Sender<WsIncoming>,
    mut outgoing_rx: mpsc::Receiver<WsOutgoing>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (close_tx, mut close_rx) = mpsc::channel::<(u16, String)>(1);
    let conn_id = registry.register(path, subprotocol, close_tx);

    let mut ping_ticker = tokio::time::interval(config.ping_interval);
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping_ticker.tick().await;

    let mut last_activity = Instant::now();

    loop {
        tokio::select! {
            biased;
            Some((code, reason)) = close_rx.recv() => {
                let frame = CloseFrame {
                    code: CloseCode::from(code),
                    reason: reason.into(),
                };
                let _ = ws.send(Message::Close(Some(frame))).await;
                let _ = incoming_tx.send(WsIncoming::Close(code, None)).await;
                break;
            }
            Some(out_msg) = outgoing_rx.recv() => {
                let frame = match out_msg {
                    WsOutgoing::Text(text) => {
                        if text.len() > config.max_message_size {
                            let close = CloseFrame {
                                code: CloseCode::Size,
                                reason: "Message too big".into(),
                            };
                            let _ = ws.send(Message::Close(Some(close))).await;
                            break;
                        }
                        Message::Text(text.into())
                    }
                    WsOutgoing::Binary(bytes) => {
                        if bytes.len() > config.max_message_size {
                            let close = CloseFrame {
                                code: CloseCode::Size,
                                reason: "Message too big".into(),
                            };
                            let _ = ws.send(Message::Close(Some(close))).await;
                            break;
                        }
                        Message::Binary(bytes.into())
                    }
                    WsOutgoing::Ping(payload) => Message::Ping(payload.into()),
                    WsOutgoing::Pong(payload) => Message::Pong(payload.into()),
                    WsOutgoing::Close(code, reason) => {
                        let frame = CloseFrame {
                            code: CloseCode::from(code),
                            reason: reason.into(),
                        };
                        let _ = ws.send(Message::Close(Some(frame))).await;
                        break;
                    }
                };
                if ws.send(frame).await.is_err() {
                    break;
                }
            }
            frame_opt = ws.next() => {
                match frame_opt {
                    Some(Ok(msg)) => {
                        last_activity = Instant::now();
                        match msg {
                            Message::Text(text) => {
                                if text.len() > config.max_message_size {
                                    let close = CloseFrame {
                                        code: CloseCode::Size,
                                        reason: "Message exceeds max_message_size".into(),
                                    };
                                    let _ = ws.send(Message::Close(Some(close))).await;
                                    break;
                                }
                                if incoming_tx.send(WsIncoming::Text(text.to_string())).await.is_err() {
                                    break;
                                }
                            }
                            Message::Binary(data) => {
                                if data.len() > config.max_message_size {
                                    let close = CloseFrame {
                                        code: CloseCode::Size,
                                        reason: "Message exceeds max_message_size".into(),
                                    };
                                    let _ = ws.send(Message::Close(Some(close))).await;
                                    break;
                                }
                                if incoming_tx.send(WsIncoming::Binary(data.to_vec())).await.is_err() {
                                    break;
                                }
                            }
                            Message::Ping(payload) => {
                                if ws.send(Message::Pong(payload)).await.is_err() {
                                    break;
                                }
                            }
                            Message::Pong(_) => {}
                            Message::Close(close_frame) => {
                                let (code, reason) = close_frame
                                    .map(|f| (u16::from(f.code), Some(f.reason.to_string())))
                                    .unwrap_or((1000, None));
                                let _ = incoming_tx.send(WsIncoming::Close(code, reason)).await;
                                break;
                            }
                            Message::Frame(_) => {}
                        }
                    }
                    Some(Err(_)) | None => {
                        let _ = incoming_tx.send(WsIncoming::Close(1006, None)).await;
                        break;
                    }
                }
            }
            _ = ping_ticker.tick() => {
                if last_activity.elapsed() >= config.idle_timeout {
                    let close = CloseFrame {
                        code: CloseCode::Away,
                        reason: "Idle timeout".into(),
                    };
                    let _ = ws.send(Message::Close(Some(close))).await;
                    let _ = incoming_tx.send(WsIncoming::Close(1001, Some("Idle timeout".to_string()))).await;
                    break;
                }
                if ws.send(Message::Ping(Bytes::from_static(b"vtb-ping"))).await.is_err() {
                    break;
                }
            }
        }
    }

    let _ = ws.close(None).await;
    registry.unregister(conn_id);
}
