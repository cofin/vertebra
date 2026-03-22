//! Response construction and the top-level request dispatch function.
//!
//! `handle` is the `service_fn` target for both plain and TLS connections.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::Request;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use vertebra_core::transport::decode_headers_raw;

use crate::collector::{method_tag, PendingRequest, PendingResponse};
use crate::hmr;
use crate::router::RouterError;
use crate::server::ServerState;

type HyperResponse = Response<Full<Bytes>>;

/// Top-level request handler; called for every accepted HTTP connection.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<HyperResponse, Infallible> {
    if state.config.dev_mode && hmr::is_hmr_path(req.uri().path()) {
        return Ok(hmr::handle(req, state).await);
    }

    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path();

    // 1. Route lookup.
    let route_match = match state.router.lookup(method.as_str(), path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"Not Found")))
                .unwrap());
        }
        Err(RouterError::MethodNotAllowed { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::from_static(b"Method Not Allowed")))
                .unwrap());
        }
    };

    let handler_id = route_match.handler_id;
    debug!(handler_id, method = %method, path, "routed request");

    // 2. Collect body (buffered in Chapter 5; streaming deferred).
    let (parts, body) = req.into_parts();
    let body_bytes = match http_body_util::BodyExt::collect(body).await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            warn!(error = %e, "failed to read request body");
            return Ok(error_response(
                StatusCode::BAD_REQUEST,
                "failed to read body",
            ));
        }
    };

    // 3. Encode headers.
    let headers_mp = encode_headers_mp(&parts.headers);

    // 4. Create reply channel.
    let (reply_tx, reply_rx) = oneshot::channel::<PendingResponse>();

    let pending = PendingRequest {
        handler_id,
        method_tag: method_tag(&method),
        http_version_tag: version_tag(parts.version),
        flags: vertebra_core::protocol::flags::NONE,
        path: path.to_owned(),
        query: uri.query().map(str::to_owned),
        headers_mp,
        body: body_bytes,
        reply: reply_tx,
    };

    // 5. Send to collector for batching.
    //
    // The collector's try_recv drain loop handles the fast path naturally:
    // under low load, a single request arrives → collector fires immediately
    // after the timeout (200µs with adaptive policy). Under high load,
    // multiple requests accumulate and drain at preferred batch sizes.
    if state.collector_tx.send(pending).await.is_err() {
        return Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server shutting down",
        ));
    }

    // 6. Await response.
    match reply_rx.await {
        Ok(resp) => Ok(build_response(resp)),
        Err(_) => {
            warn!(handler_id, "worker dropped response channel");
            Ok(error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "worker error",
            ))
        }
    }
}

fn encode_headers_mp(headers: &http::HeaderMap) -> Bytes {
    if headers.is_empty() {
        return Bytes::new();
    }
    // Pre-allocate: estimate ~32 bytes per header (name + value + length prefixes)
    let mut buf = Vec::with_capacity(headers.len() * 32);
    for (name, value) in headers.iter() {
        let n = name.as_str().as_bytes();
        let v = value.as_bytes();
        buf.push(n.len() as u8);
        buf.extend_from_slice(n);
        let vlen = v.len() as u16;
        buf.extend_from_slice(&vlen.to_le_bytes());
        buf.extend_from_slice(v);
    }
    Bytes::from(buf)
}

/// Encode an HTTP version into a compact byte tag.
fn version_tag(version: http::Version) -> u8 {
    match version {
        http::Version::HTTP_10 => 0,
        http::Version::HTTP_11 => 1,
        http::Version::HTTP_2 => 2,
        http::Version::HTTP_3 => 3,
        _ => 1,
    }
}

fn build_response(resp: PendingResponse) -> HyperResponse {
    let mut builder = Response::builder().status(resp.status);

    // Decode response headers from compact binary format and set on Hyper response.
    if !resp.headers_mp.is_empty() {
        let headers = decode_headers_raw(&resp.headers_mp);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
    }

    builder.body(Full::new(resp.body)).unwrap_or_else(|_| {
        error_response(StatusCode::INTERNAL_SERVER_ERROR, "response build error")
    })
}

fn error_response(status: StatusCode, msg: &'static str) -> HyperResponse {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}

/// Encode response headers into the compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn encode_response_headers(headers: &[(String, String)]) -> Bytes {
    if headers.is_empty() {
        return Bytes::new();
    }
    let mut buf = Vec::with_capacity(headers.len() * 32);
    for (name, value) in headers {
        let n = name.as_bytes();
        let v = value.as_bytes();
        buf.push(n.len() as u8);
        buf.extend_from_slice(n);
        let vlen = v.len() as u16;
        buf.extend_from_slice(&vlen.to_le_bytes());
        buf.extend_from_slice(v);
    }
    Bytes::from(buf)
}
