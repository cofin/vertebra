//! Lightweight HMR control plane for local development.
//!
//! `GET /__vertebra/hmr` upgrades to a WebSocket when the client asks for one.
//! Plain `GET` requests return JSON status so the signal path remains debuggable
//! even without a browser WebSocket client.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::header::{self, HeaderValue};
use http::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::sync::broadcast;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Message, Role};
use tracing::{debug, warn};

use crate::server::ServerState;
use crate::ws_mount::is_websocket_upgrade;

type HyperResponse = Response<Full<Bytes>>;

pub const HMR_PATH: &str = "/__vertebra/hmr";
pub const HMR_SIGNAL_PATH: &str = "/__vertebra/hmr/signal";
pub const HMR_EVENT_NAME: &str = "vertebra:types-changed";

/// HMR event kind emitted over the HMR WebSocket channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HmrEventKind {
    Connected,
    CssUpdate,
    FullReload,
    TypesUpdate,
}

impl HmrEventKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::CssUpdate => "css-update",
            Self::FullReload => "full-reload",
            Self::TypesUpdate => "types-update",
        }
    }

    pub fn as_event_str(&self) -> &'static str {
        match self {
            Self::TypesUpdate => HMR_EVENT_NAME,
            other => other.as_str(),
        }
    }
}

/// Structured HMR event payload serialized with both `"type"` and `"event"` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HmrEvent {
    pub kind: HmrEventKind,
    pub path: String,
    pub generation: u64,
}

impl HmrEvent {
    pub fn new(kind: HmrEventKind, path: impl Into<String>, generation: u64) -> Self {
        Self {
            kind,
            path: path.into(),
            generation,
        }
    }

    pub fn serialize(&self) -> String {
        let kind_type = self.kind.as_str();
        let kind_event = self.kind.as_event_str();
        let escaped_path = self.path.replace('\\', "\\\\").replace('"', "\\\"");
        format!(
            r#"{{"type":"{kind_type}","event":"{kind_event}","path":"{escaped_path}","reason":"{escaped_path}","version":"{}","generation":{},"updates":[{{"path":"{escaped_path}","timestamp":{}}}],"timestamp":{}}}"#,
            self.generation, self.generation, self.generation, self.generation,
        )
    }
}

/// Shared HMR state used by both the signal endpoint and WebSocket clients.
pub struct HmrState {
    generation: AtomicU64,
    connected_clients: AtomicUsize,
    tx: broadcast::Sender<u64>,
}

impl Default for HmrState {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(32);
        Self {
            generation: AtomicU64::new(0),
            connected_clients: AtomicUsize::new(0),
            tx,
        }
    }
}

impl HmrState {
    pub fn signal(&self) -> u64 {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.tx.send(generation);
        generation
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn connected_clients(&self) -> usize {
        self.connected_clients.load(Ordering::Relaxed)
    }

    fn register_client(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    fn unregister_client(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    fn subscribe(&self) -> broadcast::Receiver<u64> {
        self.tx.subscribe()
    }
}

pub async fn handle(req: Request<Incoming>, state: Arc<ServerState>) -> HyperResponse {
    match (req.method(), req.uri().path()) {
        (&Method::GET, HMR_PATH) => handle_hmr_get(req, state).await,
        (&Method::POST, HMR_SIGNAL_PATH) => handle_hmr_signal(req, state).await,
        (_, HMR_PATH) => method_not_allowed("GET"),
        (_, HMR_SIGNAL_PATH) => method_not_allowed("POST"),
        _ => not_found(),
    }
}

fn handles_path(path: &str) -> bool {
    matches!(path, HMR_PATH | HMR_SIGNAL_PATH)
}

pub fn is_hmr_path(path: &str) -> bool {
    handles_path(path)
}

async fn handle_hmr_get(mut req: Request<Incoming>, state: Arc<ServerState>) -> HyperResponse {
    if !is_websocket_upgrade(&req) {
        return status_response(&state.hmr);
    }

    let on_upgrade = hyper::upgrade::on(&mut req);
    let accept = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .map(|key| derive_accept_key(key.as_bytes()));
    let version = req.version();
    let hmr = Arc::clone(&state.hmr);

    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => run_websocket(upgraded, hmr).await,
            Err(error) => debug!(error = %error, "hmr websocket upgrade failed"),
        }
    });

    match accept {
        Some(accept) => Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .version(version)
            .header(header::CONNECTION, HeaderValue::from_static("Upgrade"))
            .header(header::UPGRADE, HeaderValue::from_static("websocket"))
            .header(header::SEC_WEBSOCKET_ACCEPT, accept)
            .body(Full::new(Bytes::new()))
            .unwrap(),
        None => bad_request("missing websocket key"),
    }
}

async fn handle_hmr_signal(req: Request<Incoming>, state: Arc<ServerState>) -> HyperResponse {
    let (_, body) = req.into_parts();
    if let Err(error) = body.collect().await {
        warn!(error = %error, "failed to read hmr signal body");
        return bad_request("failed to read body");
    }

    let generation = state.hmr.signal();
    debug!(generation, "hmr signal emitted");

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .body(Full::new(Bytes::new()))
        .unwrap()
}

async fn run_websocket(upgraded: Upgraded, hmr: Arc<HmrState>) {
    let io = TokioIo::new(upgraded);
    let mut stream = WebSocketStream::from_raw_socket(io, Role::Server, None).await;
    let mut rx = hmr.subscribe();

    hmr.register_client();

    loop {
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(generation) => {
                        let payload = websocket_payload(generation);
                        if stream.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let payload = websocket_payload(hmr.generation());
                        if stream.send(Message::Text(payload.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Ping(payload))) => {
                        if stream.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let _ = stream.send(Message::Close(frame)).await;
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        debug!(error = %error, "hmr websocket client errored");
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    hmr.unregister_client();
}

fn status_response(hmr: &HmrState) -> HyperResponse {
    let body = format!(
        "{{\"ok\":true,\"event\":\"{HMR_EVENT_NAME}\",\"generation\":{},\"connected_clients\":{},\"websocket\":true}}",
        hmr.generation(),
        hmr.connected_clients(),
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )
        .header(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn websocket_payload(generation: u64) -> String {
    HmrEvent::new(HmrEventKind::TypesUpdate, HMR_PATH, generation).serialize()
}

fn method_not_allowed(allow: &'static str) -> HyperResponse {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(header::ALLOW, HeaderValue::from_static(allow))
        .body(Full::new(Bytes::from_static(b"Method Not Allowed")))
        .unwrap()
}

fn bad_request(message: &'static str) -> HyperResponse {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Full::new(Bytes::from_static(message.as_bytes())))
        .unwrap()
}

fn not_found() -> HyperResponse {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Full::new(Bytes::from_static(b"Not Found")))
        .unwrap()
}
