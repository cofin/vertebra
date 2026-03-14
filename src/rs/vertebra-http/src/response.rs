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

use crate::collector::{method_tag, PendingRequest, PendingResponse};
use crate::router::RouterError;
use crate::server::ServerState;

type HyperResponse = Response<Full<Bytes>>;

/// Top-level request handler; called for every accepted HTTP connection.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<HyperResponse, Infallible> {
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
            return Ok(error_response(StatusCode::BAD_REQUEST, "failed to read body"));
        }
    };

    // 3. Encode headers.
    let headers_mp = encode_headers_mp(&parts.headers);

    // 4. Create reply channel.
    let (reply_tx, reply_rx) = oneshot::channel::<PendingResponse>();

    let pending = PendingRequest {
        handler_id,
        method_tag: method_tag(&method),
        path: path.to_owned(),
        query: uri.query().map(str::to_owned),
        headers_mp,
        body: body_bytes,
        reply: reply_tx,
    };

    // 5. Send to collector.
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
    let mut buf = Vec::new();
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

fn build_response(resp: PendingResponse) -> HyperResponse {
    Response::builder()
        .status(resp.status)
        .body(Full::new(resp.body))
        .unwrap_or_else(|_| {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "response build error")
        })
}

fn error_response(status: StatusCode, msg: &'static str) -> HyperResponse {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}
