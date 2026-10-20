//! Response construction and the top-level request dispatch function.
//!
//! `handle` is the `service_fn` target for both plain and TLS connections.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::Stream;
use http::{Response, StatusCode};
use http_body_util::{Either, Full, StreamBody};
use hyper::Request;
use hyper::body::{Body, Frame, Incoming};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use vertebra_core::transport::{encode_headers_compact, iter_raw_headers};

use crate::backpressure::{BackpressureMetricsSnapshot, CircuitState, RejectionReason};
use crate::collector::{PendingRequest, PendingResponse, method_tag};
use crate::hmr;
use crate::router::RouterError;
use crate::server::ServerState;

/// Path for the backpressure-aware health endpoint. Always returns 200 so
/// that load balancers can probe it even when the controller is
/// saturated or the circuit is open.
const HEALTH_PATH: &str = "/__vertebra/health";

/// Adapter implementing `futures_util::Stream` over a Tokio `mpsc::Receiver` of HTTP body frames.
pub struct ReceiverFrameStream {
    rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>,
}

impl ReceiverFrameStream {
    /// Wrap a bounded Tokio `mpsc::Receiver` as an HTTP frame stream.
    pub fn new(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> Self {
        Self { rx }
    }
}

impl Stream for ReceiverFrameStream {
    type Item = Result<Frame<Bytes>, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Progressive multi-chunk and trailer body stream.
pub type StreamingBody = StreamBody<ReceiverFrameStream>;

/// Dual-path HTTP response body: `Left(Full<Bytes>)` for zero-channel unary responses,
/// `Right(StreamingBody)` for chunked, SSE, and trailer-bearing responses.
pub type VertebraBody = Either<Full<Bytes>, StreamingBody>;

/// Canonical HTTP response type used across `vertebra-http`.
pub type VertebraResponse = Response<VertebraBody>;

type HyperResponse = VertebraResponse;

/// Wrap a contiguous byte buffer in the zero-allocation unary `Either::Left(Full<Bytes>)` branch.
#[inline]
pub fn full_body(bytes: impl Into<Bytes>) -> VertebraBody {
    Either::Left(Full::new(bytes.into()))
}

/// Wrap a frame receiver in the progressive streaming `Either::Right(StreamBody)` branch.
#[inline]
pub fn stream_body(rx: mpsc::Receiver<Result<Frame<Bytes>, Infallible>>) -> VertebraBody {
    Either::Right(StreamBody::new(ReceiverFrameStream::new(rx)))
}

/// Top-level request handler; called for every accepted HTTP connection.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<VertebraResponse, Infallible> {
    if state.config.dev_mode && hmr::is_hmr_path(req.uri().path()) {
        let hmr_resp = hmr::handle(req, state).await;
        let (parts, body) = hmr_resp.into_parts();
        return Ok(Response::from_parts(parts, Either::Left(body)));
    }

    // Health endpoint sits above routing and backpressure so probes always
    // work, even when the controller is shedding load.
    if req.uri().path() == HEALTH_PATH
        && let Some(bp) = state.dispatcher.backpressure()
    {
        return Ok(health_response(Some(bp.snapshot())));
    }
    if let Some(health_resp) =
        crate::health::HealthEndpoint::default().try_intercept(req.uri().path())
    {
        let (parts, body) = health_resp.into_parts();
        return Ok(Response::from_parts(parts, Either::Left(body)));
    }

    // Admission control. If a BackpressureController is attached, acquire a
    // permit before doing any real work; hold it for the lifetime of the
    // request via `_permit`.
    let _permit = if let Some(controller) = state.dispatcher.backpressure() {
        match controller.try_acquire_for_path(req.uri().path()).await {
            Ok(permit) => Some(permit),
            Err(reason) => return Ok(rejection_response(reason)),
        }
    } else {
        None
    };

    let (parts, body) = req.into_parts();
    let method = &parts.method;
    let path = parts.uri.path();

    // 1. Route lookup.
    let route_match = match state.router.lookup(method.as_str(), path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(full_body(Bytes::from_static(b"Not Found")))
                .unwrap());
        }
        Err(RouterError::MethodNotAllowed { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(full_body(Bytes::from_static(b"Method Not Allowed")))
                .unwrap());
        }
    };

    let handler_id = route_match.handler_id;
    debug!(handler_id, method = %method, path, "routed request");

    // 2. Collect body (buffered in Chapter 5; streaming deferred).
    let body_bytes = if body.is_end_stream() {
        Bytes::new()
    } else {
        match http_body_util::BodyExt::collect(body).await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                warn!(error = %e, "failed to read request body");
                return Ok(error_response(
                    StatusCode::BAD_REQUEST,
                    "failed to read body",
                ));
            }
        }
    };

    // 3. Encode headers.
    let headers_mp = encode_headers_mp(&parts.headers);

    // 4. Create reply channel.
    let (reply_tx, reply_rx) = oneshot::channel::<PendingResponse>();

    let pending = PendingRequest {
        handler_id,
        method_tag: method_tag(method),
        http_version_tag: version_tag(parts.version),
        flags: vertebra_core::protocol::flags::NONE,
        path: path.to_owned(),
        query: parts.uri.query().map(str::to_owned),
        headers_mp,
        body: body_bytes,
        reply: reply_tx,
    };

    // 5. Send to collector for batching.
    if state.get_collector().send(pending).await.is_err() {
        return Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "server shutting down",
        ));
    }

    // 6. Await response.
    let mut hyper_resp = match reply_rx.await {
        Ok(resp) => build_response(resp),
        Err(_) => {
            warn!(handler_id, "worker dropped response channel");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "worker error")
        }
    };

    if state.config.http3.enabled {
        crate::http3::inject_alt_svc_header(
            hyper_resp.headers_mut(),
            state.config.bind.port(),
            state.config.http3.alt_svc_max_age,
        );
    }

    // Feed the circuit breaker. 5xx counts as failure; everything else is
    // treated as success from the infrastructure's perspective.
    if let Some(controller) = state.dispatcher.backpressure() {
        if hyper_resp.status().is_server_error() {
            controller.record_failure();
        } else {
            controller.record_success();
        }
    }

    Ok(hyper_resp)
}

fn rejection_response(reason: RejectionReason) -> HyperResponse {
    let (status, msg) = match reason {
        RejectionReason::Saturated => (StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable"),
        RejectionReason::QueueTimeout => (StatusCode::SERVICE_UNAVAILABLE, "Server Busy"),
        RejectionReason::StreamLimitExceeded => {
            (StatusCode::SERVICE_UNAVAILABLE, "Stream Limit Exceeded")
        }
        RejectionReason::CircuitOpen => (StatusCode::TOO_MANY_REQUESTS, "Too Many Requests"),
    };
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("retry-after", "1")
        .body(full_body(Bytes::from_static(msg.as_bytes())))
        .unwrap_or_else(|_| error_response(status, msg))
}

fn health_response(snapshot: Option<BackpressureMetricsSnapshot>) -> HyperResponse {
    let body = match snapshot {
        Some(s) => format!(
            "{{\"status\":\"healthy\",\"backpressure\":{{\"in_flight\":{},\"queue_depth\":{},\"circuit_state\":\"{}\",\"counters\":{{\"accepted\":{},\"saturated_rejected\":{},\"queue_rejected\":{},\"circuit_rejected\":{},\"timeout_rejected\":{}}}}}}}",
            s.in_flight,
            s.queue_depth,
            circuit_state_str(s.circuit_state),
            s.accepted,
            s.saturated_rejected,
            s.queue_rejected,
            s.circuit_rejected,
            s.timeout_rejected,
        ),
        None => "{\"status\":\"healthy\"}".to_string(),
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(full_body(Bytes::from(body)))
        .unwrap_or_else(|_| error_response(StatusCode::INTERNAL_SERVER_ERROR, "health error"))
}

fn circuit_state_str(state: CircuitState) -> &'static str {
    match state {
        CircuitState::Closed => "closed",
        CircuitState::Open => "open",
        CircuitState::HalfOpen => "half_open",
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

/// Build a `VertebraResponse` from a `PendingResponse`, taking the streaming branch when `stream_rx` is present.
pub fn build_response(resp: PendingResponse) -> VertebraResponse {
    let mut builder = Response::builder().status(resp.status);

    // Decode response headers from compact binary format and set on Hyper response.
    if !resp.headers_mp.is_empty() {
        for (name, value) in iter_raw_headers(&resp.headers_mp) {
            builder = builder.header(name, value);
        }
    }

    let body = match resp.stream_rx {
        Some(stream_rx) => stream_body(stream_rx.into_inner()),
        None => full_body(resp.body),
    };

    builder.body(body).unwrap_or_else(|_| {
        error_response(StatusCode::INTERNAL_SERVER_ERROR, "response build error")
    })
}

fn error_response(status: StatusCode, msg: &'static str) -> HyperResponse {
    Response::builder()
        .status(status)
        .body(full_body(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}

/// Encode response headers into the compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn encode_response_headers(headers: &[(Vec<u8>, Vec<u8>)]) -> Bytes {
    Bytes::from(encode_headers_compact(headers))
}
