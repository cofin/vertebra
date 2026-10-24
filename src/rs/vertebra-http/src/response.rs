//! Response construction and the top-level request dispatch function.
//!
//! `handle` is the `service_fn` target for both plain and TLS connections.

use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode, header};
use http_body_util::{Either, Full};
use hyper::Request;
use hyper::body::{Body, Incoming};
use tokio::sync::oneshot;
use tracing::{debug, warn};

use vertebra_core::transport::{encode_headers_compact, iter_raw_headers};

use crate::backpressure::{BackpressureMetricsSnapshot, CircuitState, RejectionReason};
use crate::batcher::{PendingRequest, PendingResponse, http_version_tag, method_tag};
use crate::compression::CompressionOffloadConfig;
use crate::cors::CompiledCorsOffload;
use crate::health::HealthState;
use crate::hmr;
use crate::routing::RouterError;
use crate::server::ServerState;

/// Path for the backpressure-aware health endpoint. Always returns 200 so
/// that load balancers can probe it even when the controller is
/// saturated or the circuit is open.
const HEALTH_PATH: &str = "/__vertebra/health";

pub use crate::body::{FrameReceiver, HttpResponse, ResponseBody, full_body, stream_body};

/// Top-level request handler; called for every accepted HTTP connection.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    if state.config.dev_mode && hmr::is_hmr_path(req.uri().path()) {
        let hmr_resp = hmr::handle(req, state).await;
        let (parts, body) = hmr_resp.into_parts();
        return Ok(Response::from_parts(parts, Either::Left(body)));
    }

    // Health endpoint sits above routing and backpressure so probes always
    // work, even when the controller is shedding load.
    let req_path = req.uri().path();
    if req_path == HEALTH_PATH
        && state.health_endpoint.state() == HealthState::Healthy
        && let Some(bp) = state.dispatcher.backpressure()
    {
        return Ok(health_response(Some(bp.snapshot())));
    }
    if let Some(health_resp) = state.health_endpoint.try_intercept(req_path) {
        let (parts, body) = health_resp.into_parts();
        return Ok(Response::from_parts(parts, Either::Left(body)));
    }

    // Zero-GIL CORS OPTIONS preflight offload.
    if *req.method() == Method::OPTIONS
        && let Some(ref cors) = state.cors
        && let Some(origin) = req
            .headers()
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
        && let Some(req_method) = req
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|v| v.to_str().ok())
    {
        let req_headers = req
            .headers()
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .and_then(|v| v.to_str().ok());
        let (status, preflight_headers) =
            cors.evaluate_preflight(origin, Some(req_method), req_headers);
        let mut resp = Response::builder()
            .status(status)
            .body(full_body(Bytes::new()))
            .unwrap();
        *resp.headers_mut() = preflight_headers;
        return Ok(resp);
    }

    // Zero-GIL OpenAPI schema cache offload.
    if matches!(*req.method(), Method::GET | Method::HEAD)
        && let Some(ref openapi) = state.openapi_cache
        && req_path == openapi.schema_path
    {
        let inm_matches = req
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|inm| inm.trim() == openapi.etag_str);
        let (status, body) = if inm_matches {
            (StatusCode::NOT_MODIFIED, Bytes::new())
        } else if *req.method() == Method::HEAD {
            (StatusCode::OK, Bytes::new())
        } else {
            (StatusCode::OK, openapi.body.clone())
        };
        let mut resp = Response::builder()
            .status(status)
            .body(full_body(body))
            .unwrap();
        let hdrs = resp.headers_mut();
        hdrs.insert(header::CONTENT_TYPE, openapi.content_type.clone());
        hdrs.insert(header::ETAG, openapi.etag.clone());
        hdrs.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=0, must-revalidate"),
        );
        if let Some(ref cors) = state.cors
            && let Some(origin) = req
                .headers()
                .get(header::ORIGIN)
                .and_then(|v| v.to_str().ok())
        {
            cors.apply_response_headers(origin, hdrs);
        }
        return Ok(resp);
    }

    // Zero-GIL static asset mount offload.
    if !state.static_mounts.is_empty() && matches!(*req.method(), Method::GET | Method::HEAD) {
        for mount in &state.static_mounts {
            if let Some(rel_path) = mount.match_relative_path(req_path) {
                for manager in &mount.managers {
                    if let Some(static_resp) =
                        manager.serve_static_request(rel_path, req.headers()).await
                    {
                        let (mut parts, full) = static_resp.into_parts();
                        for (name, val) in &mount.headers {
                            parts.headers.insert(name.clone(), val.clone());
                        }
                        if mount.send_as_attachment {
                            parts.headers.insert(
                                header::CONTENT_DISPOSITION,
                                HeaderValue::from_static("attachment"),
                            );
                        }
                        if let Some(ref cors) = state.cors
                            && let Some(origin) = req
                                .headers()
                                .get(header::ORIGIN)
                                .and_then(|v| v.to_str().ok())
                        {
                            cors.apply_response_headers(origin, &mut parts.headers);
                        }
                        let body = if *req.method() == Method::HEAD {
                            Either::Left(Full::new(Bytes::new()))
                        } else {
                            Either::Left(full)
                        };
                        return Ok(Response::from_parts(parts, body));
                    }
                }
            }
        }
    }

    // Admission control. If a BackpressureController is attached, acquire a
    // permit before doing any real work; hold it for the lifetime of the
    // request via `_permit`.
    let _permit = if let Some(controller) = state.dispatcher.backpressure() {
        match controller.try_acquire_for_path(req_path).await {
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
    let route_match = match state.router.lookup_method(method, path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(full_body(Bytes::from_static(b"Not Found")))
                .unwrap());
        }
        Err(RouterError::MethodNotAllowed { allowed, .. }) => {
            return Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("allow", allowed.join(", "))
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

    let cors_origin = if state.cors.is_some() {
        parts.headers.get(header::ORIGIN).cloned()
    } else {
        None
    };
    let accept_encoding = if state.compression.is_some() {
        parts.headers.get(header::ACCEPT_ENCODING).cloned()
    } else {
        None
    };

    // 3. Encode headers directly to compact binary format in one pass.
    let headers_mp = encode_header_map_compact(&parts.headers);

    // 4. Create reply channel.
    let (reply_tx, reply_rx) = oneshot::channel::<PendingResponse>();

    let path_and_query = parts
        .uri
        .into_parts()
        .path_and_query
        .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/"));
    let offload_path = if state.compression.is_some() {
        Some(path_and_query.clone())
    } else {
        None
    };

    let pending = PendingRequest {
        handler_id,
        method_tag: method_tag(method),
        http_version_tag: http_version_tag(parts.version),
        flags: vertebra_core::protocol::flags::NONE,
        path_and_query,
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

    // 6. Await response and apply configured Rust CORS / compression offloads.
    let mut hyper_resp = match reply_rx.await {
        Ok(resp) => build_response_with_offloads(
            resp,
            state.cors.as_ref(),
            cors_origin.as_ref(),
            state.compression.as_ref(),
            offload_path.as_ref().map(|pq| pq.path()),
            accept_encoding.as_ref(),
        ),
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

fn rejection_response(reason: RejectionReason) -> HttpResponse {
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

fn health_response(snapshot: Option<BackpressureMetricsSnapshot>) -> HttpResponse {
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

/// Encode an `http::HeaderMap` directly into compact binary `Bytes` in a single allocation.
///
/// Wire format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`.
/// Duplicate header names are preserved in iteration order and oversized components are clamped.
#[inline]
pub fn encode_header_map_compact(headers: &HeaderMap) -> Bytes {
    if headers.is_empty() {
        return Bytes::new();
    }
    let mut total_bytes = 0usize;
    for (name, value) in headers.iter() {
        let nlen = name.as_str().len().min(u8::MAX as usize);
        let vlen = value.as_bytes().len().min(u16::MAX as usize);
        total_bytes += 3 + nlen + vlen;
    }

    let mut buf = Vec::with_capacity(total_bytes);
    for (name, value) in headers.iter() {
        let n = name.as_str().as_bytes();
        let v = value.as_bytes();
        let nlen = n.len().min(u8::MAX as usize);
        let vlen = v.len().min(u16::MAX as usize);
        buf.push(nlen as u8);
        buf.extend_from_slice(&n[..nlen]);
        buf.extend_from_slice(&(vlen as u16).to_le_bytes());
        buf.extend_from_slice(&v[..vlen]);
    }
    Bytes::from(buf)
}

/// Build an `HttpResponse` from a `PendingResponse`, taking the streaming branch when `stream_rx` is present.
pub fn build_response(resp: PendingResponse) -> HttpResponse {
    build_response_with_offloads(resp, None, None, None, None, None)
}

fn build_response_with_offloads(
    resp: PendingResponse,
    cors: Option<&CompiledCorsOffload>,
    cors_origin: Option<&HeaderValue>,
    compression: Option<&CompressionOffloadConfig>,
    req_path: Option<&str>,
    accept_encoding: Option<&HeaderValue>,
) -> HttpResponse {
    let mut response = Response::new(full_body(Bytes::new()));
    *response.status_mut() =
        StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    if !resp.headers_mp.is_empty() {
        let headers = response.headers_mut();
        for (name, value) in iter_raw_headers(&resp.headers_mp) {
            if let (Ok(hname), Ok(hval)) = (
                http::HeaderName::from_bytes(name),
                HeaderValue::from_bytes(value),
            ) {
                headers.append(hname, hval);
            }
        }
    }

    if let (Some(cors_engine), Some(origin_val)) = (cors, cors_origin)
        && let Ok(origin_str) = origin_val.to_str()
    {
        cors_engine.apply_response_headers(origin_str, response.headers_mut());
    }

    match resp.stream_rx {
        Some(stream_rx) => {
            *response.body_mut() = stream_body(stream_rx.into_inner());
        }
        None => {
            let final_body = if let Some(comp) = compression {
                let accept_str = accept_encoding.and_then(|v| v.to_str().ok());
                comp.compress_if_eligible(
                    req_path.unwrap_or("/"),
                    accept_str,
                    response.headers_mut(),
                    resp.body,
                )
            } else {
                resp.body
            };
            *response.body_mut() = full_body(final_body);
        }
    }

    response
}

fn error_response(status: StatusCode, msg: &'static str) -> HttpResponse {
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
