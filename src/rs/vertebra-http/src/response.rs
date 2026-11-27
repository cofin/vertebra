//! Response construction and the top-level request dispatch function.
//!
//! `handle` is the `service_fn` target for both plain and TLS connections.

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::uri::PathAndQuery;
use http::{HeaderMap, HeaderValue, Method, Response, StatusCode, header};
use http_body_util::{Either, Full};
use hyper::Request;
use hyper::body::{Body, Incoming};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use tokio::sync::oneshot;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::Role;
use tracing::{debug, warn};

use vertebra_core::transport::{encode_headers_compact, iter_raw_headers};

use crate::access_log::AccessLogEntry;
use crate::backpressure::{BackpressureMetricsSnapshot, CircuitState, RejectionReason};
use crate::batcher::{PendingRequest, PendingResponse, http_version_tag, method_tag};
use crate::compression::CompressionOffloadConfig;
use crate::cors::CompiledCorsOffload;
use crate::dispatcher::{BridgeCorrelation, DispatchBackend, ShmDispatcher, ShmTargetError};
use crate::grpc::{
    GRPC_HEADER_LEN, GrpcError, GrpcStatus, GrpcWireFormat, build_grpc_trailers_only_response,
    parse_grpc_timeout,
};
use crate::health::HealthState;
use crate::hmr;
use crate::mcp_bridge::extract_mcp_observability_headers;
use crate::multipart::parse_multipart_body;
use crate::payload::{
    PayloadError, collect_bounded_body, decode_buffered_body, is_active_content_encoding,
};
use crate::routing::{RouterError, ZPAGES_HANDLER_ID};
use crate::server::ServerState;
use crate::ws_mount::{
    WebSocketHandshakeDecision, WsConfig, WsIncoming, WsOutgoing, build_handshake_response,
    client_offers_permessage_deflate, is_websocket_upgrade_parts, negotiate_subprotocol,
    run_ws_connection, validate_origin,
};
use crate::zpages::{
    WorkerZPageOutcome, ZPAGES_REQUEST_TIMEOUT, ZPagesRequest, json_response,
    merge_shm_zpages_responses,
};

/// Path for the backpressure-aware health endpoint. Always returns 200 so
/// that load balancers can probe it even when the controller is
/// saturated or the circuit is open.
const HEALTH_PATH: &str = "/__vertebra/health";

pub use crate::body::{FrameReceiver, HttpResponse, ResponseBody, full_body, stream_body};

enum RequestBodySource {
    Incoming(Incoming),
    Buffered(Bytes),
}

/// Top-level request handler; delegates with an unspecified peer address.
pub async fn handle(
    req: Request<Incoming>,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    handle_with_peer(req, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)), state).await
}

/// Top-level request handler with connected peer address; called for every accepted HTTP connection.
pub async fn handle_with_peer(
    req: Request<Incoming>,
    peer: SocketAddr,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    handle_with_peer_inner(req, peer, None, state).await
}

/// Top-level request handler pinned to a per-connection collector shard.
pub async fn handle_with_peer_shard(
    req: Request<Incoming>,
    peer: SocketAddr,
    collector_shard: usize,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    handle_with_peer_inner(req, peer, Some(collector_shard), state).await
}

async fn handle_with_peer_inner(
    mut req: Request<Incoming>,
    peer: SocketAddr,
    collector_shard: Option<usize>,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    if state.config.dev_mode && hmr::is_hmr_path(req.uri().path()) {
        let hmr_resp = hmr::handle(req, state).await;
        let (parts, body) = hmr_resp.into_parts();
        return Ok(Response::from_parts(parts, Either::Left(body)));
    }
    let on_upgrade = req
        .headers()
        .contains_key(header::UPGRADE)
        .then(|| hyper::upgrade::on(&mut req));
    let (parts, body) = req.into_parts();
    dispatch_request(
        parts,
        RequestBodySource::Incoming(body),
        on_upgrade,
        peer.ip(),
        collector_shard,
        state,
    )
    .await
}

/// Shared ingress request handler for protocols supplying pre-buffered request bodies (HTTP/3 QUIC).
pub async fn handle_buffered(
    req: Request<Bytes>,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    let (parts, body) = req.into_parts();
    dispatch_request(
        parts,
        RequestBodySource::Buffered(body),
        None,
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        None,
        state,
    )
    .await
}

async fn dispatch_request(
    parts: http::request::Parts,
    body: RequestBodySource,
    on_upgrade: Option<OnUpgrade>,
    peer_ip: IpAddr,
    collector_shard: Option<usize>,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible> {
    let mut resp =
        dispatch_request_inner(parts, body, on_upgrade, peer_ip, collector_shard, &state).await;
    if let Some(port) = state.http3_port {
        crate::http3::inject_alt_svc_header(
            resp.headers_mut(),
            port,
            state.config.http3.alt_svc_max_age,
        );
    }
    Ok(resp)
}

async fn dispatch_request_inner(
    mut parts: http::request::Parts,
    body: RequestBodySource,
    on_upgrade: Option<OnUpgrade>,
    peer_ip: IpAddr,
    collector_shard: Option<usize>,
    state: &Arc<ServerState>,
) -> HttpResponse {
    // Health endpoint sits above routing and backpressure so probes always
    // work, even when the controller is shedding load.
    let req_path = parts.uri.path();
    if req_path == HEALTH_PATH
        && state.health_endpoint.state() == HealthState::Healthy
        && let Some(bp) = state.dispatcher.backpressure()
    {
        return health_response(Some(bp.snapshot()));
    }
    if let Some(health_resp) = state.health_endpoint.try_intercept(req_path) {
        let (resp_parts, body) = health_resp.into_parts();
        return Response::from_parts(resp_parts, Either::Left(body));
    }

    // Token-guarded `/debug/pprof/*` diagnostic endpoint.
    if state.config.profile.enabled {
        let pprof_uri = parts
            .uri
            .path_and_query()
            .map_or(req_path, http::uri::PathAndQuery::as_str);
        if let Some(pprof_resp) = state
            .profile_endpoint
            .try_handle_async(pprof_uri, &parts.headers)
            .await
        {
            return pprof_resp.map(Either::Left);
        }
    }

    // Opt-in z-pages diagnostic endpoints (`/taskz`, `/statusz`, `/varz`, `/rpcz`, `/brokerz`).
    if state.zpages_endpoint.is_enabled()
        && let Some(decision) =
            state
                .zpages_endpoint
                .preflight(&parts.method, &parts.uri, &parts.headers)
    {
        return match decision {
            Ok(zreq) => handle_zpages_request(zreq, state).await,
            Err(resp) => resp.map(Either::Left),
        };
    }

    // Zero-GIL `/.well-known/*` discovery cache.
    if req_path.starts_with("/.well-known/") && matches!(parts.method, Method::GET | Method::HEAD) {
        let inm = parts
            .headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok());
        if let Some(disc) = state.discovery_cache.serve(req_path, inm) {
            let resp_body = if parts.method == Method::HEAD {
                Bytes::new()
            } else {
                disc.body
            };
            let mut resp = Response::builder()
                .status(disc.status)
                .body(full_body(resp_body))
                .unwrap();
            *resp.headers_mut() = disc.headers;
            return resp;
        }
    }

    // Native Rust SSE topic subscription fastpath (`/__vertebra/sse/{topic}`).
    if parts.method == Method::GET
        && let Some(topic) = req_path.strip_prefix("/__vertebra/sse/")
        && !topic.is_empty()
    {
        let last_event_id = parts
            .headers
            .get("last-event-id")
            .and_then(|v| v.to_str().ok());
        let sse_resp = state.sse_hub.subscribe_response(topic, last_event_id);
        let (resp_parts, body) = sse_resp.into_parts();
        return Response::from_parts(resp_parts, Either::Right(body));
    }

    // Native Rust RFC 6455 WebSocket pub/sub mount (`/__vertebra/ws/{topic}` and `ServerState::ws_mounts`).
    if parts.headers.contains_key(header::UPGRADE) {
        if let Some(ws_mount) = match_ws_mount(req_path, state) {
            if is_websocket_upgrade_parts(&parts.method, parts.version, &parts.headers) {
                return handle_ws_upgrade(&parts, on_upgrade, ws_mount, state).await;
            }
            return error_response(StatusCode::BAD_REQUEST, "Invalid WebSocket upgrade request");
        }
    } else if req_path
        .strip_prefix("/__vertebra/ws/")
        .is_some_and(|topic| !topic.is_empty())
    {
        return Response::builder()
            .status(StatusCode::UPGRADE_REQUIRED)
            .header(header::UPGRADE, HeaderValue::from_static("websocket"))
            .body(full_body(Bytes::from_static(b"Upgrade Required")))
            .unwrap();
    }

    // Zero-GIL CORS OPTIONS preflight offload.
    if parts.method == Method::OPTIONS
        && let Some(ref cors) = state.cors
        && let Some(origin) = parts
            .headers
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
        && let Some(req_method) = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_METHOD)
            .and_then(|v| v.to_str().ok())
    {
        let req_headers = parts
            .headers
            .get(header::ACCESS_CONTROL_REQUEST_HEADERS)
            .and_then(|v| v.to_str().ok());
        let (status, preflight_headers) =
            cors.evaluate_preflight(origin, Some(req_method), req_headers);
        let mut resp = Response::builder()
            .status(status)
            .body(full_body(Bytes::new()))
            .unwrap();
        *resp.headers_mut() = preflight_headers;
        return resp;
    }

    // Zero-GIL OpenAPI schema cache offload.
    if matches!(parts.method, Method::GET | Method::HEAD)
        && let Some(ref openapi) = state.openapi_cache
        && req_path == openapi.schema_path
    {
        let inm_matches = parts
            .headers
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|inm| inm.trim() == openapi.etag_str);
        let (status, resp_body) = if inm_matches {
            (StatusCode::NOT_MODIFIED, Bytes::new())
        } else if parts.method == Method::HEAD {
            (StatusCode::OK, Bytes::new())
        } else {
            (StatusCode::OK, openapi.body.clone())
        };
        let mut resp = Response::builder()
            .status(status)
            .body(full_body(resp_body))
            .unwrap();
        let hdrs = resp.headers_mut();
        hdrs.insert(header::CONTENT_TYPE, openapi.content_type.clone());
        hdrs.insert(header::ETAG, openapi.etag.clone());
        hdrs.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=0, must-revalidate"),
        );
        if let Some(ref cors) = state.cors
            && let Some(origin) = parts
                .headers
                .get(header::ORIGIN)
                .and_then(|v| v.to_str().ok())
        {
            cors.apply_response_headers(origin, hdrs);
        }
        return resp;
    }

    // Zero-GIL static asset mount offload.
    if !state.static_mounts.is_empty() && matches!(parts.method, Method::GET | Method::HEAD) {
        for mount in &state.static_mounts {
            if let Some(rel_path) = mount.match_relative_path(req_path) {
                for manager in &mount.managers {
                    if let Some(static_resp) =
                        manager.serve_static_request(rel_path, &parts.headers).await
                    {
                        let (mut resp_parts, full) = static_resp.into_parts();
                        for (name, val) in &mount.headers {
                            resp_parts.headers.insert(name.clone(), val.clone());
                        }
                        if mount.send_as_attachment {
                            resp_parts.headers.insert(
                                header::CONTENT_DISPOSITION,
                                HeaderValue::from_static("attachment"),
                            );
                        }
                        if let Some(ref cors) = state.cors
                            && let Some(origin) = parts
                                .headers
                                .get(header::ORIGIN)
                                .and_then(|v| v.to_str().ok())
                        {
                            cors.apply_response_headers(origin, &mut resp_parts.headers);
                        }
                        let resp_body = if parts.method == Method::HEAD {
                            Either::Left(Full::new(Bytes::new()))
                        } else {
                            Either::Left(full)
                        };
                        return Response::from_parts(resp_parts, resp_body);
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
            Err(reason) => return rejection_response(reason),
        }
    } else {
        None
    };

    let access_log_start = state
        .access_logger
        .as_ref()
        .map(|_| std::time::Instant::now());
    let is_grpc = parts.method == Method::POST
        && parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ct| ct.starts_with("application/grpc"));
    if is_grpc
        && let Some(raw_timeout) = parts
            .headers
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
        && let Err(err) = parse_grpc_timeout(raw_timeout)
    {
        let wf = GrpcWireFormat::from_headers(&parts.headers);
        return build_grpc_trailers_only_response(wf, err.status(), Some(&err.to_string()))
            .map(Either::Left);
    }

    let method = &parts.method;
    let path = parts.uri.path();

    // 1. Route lookup.
    let route_match = match state.router.lookup_method(method, path) {
        Ok(m) => m,
        Err(RouterError::NotFound { .. }) => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(full_body(Bytes::from_static(b"Not Found")))
                .unwrap();
        }
        Err(RouterError::MethodNotAllowed { allowed, .. }) => {
            return Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header("allow", allowed.join(", "))
                .body(full_body(Bytes::from_static(b"Method Not Allowed")))
                .unwrap();
        }
    };

    let handler_id = route_match.handler_id;
    debug!(handler_id, method = %method, path, "routed request");

    // 2. Collect bounded body and optionally decompress Content-Encoding.
    let collected_res: Result<(Bytes, bool), PayloadError> = match body {
        RequestBodySource::Incoming(incoming) => {
            if incoming.size_hint().exact() == Some(0) {
                Ok((Bytes::new(), false))
            } else {
                let (content_length, active_encoding) = extract_body_headers(&parts.headers);
                collect_bounded_body(
                    incoming,
                    content_length,
                    active_encoding,
                    &state.config.payload_limits,
                )
                .await
                .map(|bytes| (bytes, active_encoding.is_some()))
            }
        }
        RequestBodySource::Buffered(raw_bytes) => {
            let (content_length, active_encoding) = extract_body_headers(&parts.headers);
            decode_buffered_body(
                raw_bytes,
                content_length,
                active_encoding,
                &state.config.payload_limits,
            )
        }
    };

    let body_bytes = match collected_res {
        Ok((bytes, was_decompressed)) => {
            if was_decompressed {
                parts.headers.remove(header::CONTENT_ENCODING);
                if let Ok(cl_val) = HeaderValue::from_str(&bytes.len().to_string()) {
                    parts.headers.insert(header::CONTENT_LENGTH, cl_val);
                }
            }
            bytes
        }
        Err(err) => {
            warn!(error = %err, "request payload rejected");
            let msg = match err {
                PayloadError::TooLarge { .. } | PayloadError::DecompressedTooLarge { .. } => {
                    "Payload Too Large"
                }
                PayloadError::UnsupportedEncoding(_) => "Unsupported Media Type",
                PayloadError::DecompressionFailed(_) | PayloadError::StreamRead(_) => "Bad Request",
            };
            return error_response(err.status_code(), msg);
        }
    };

    if !is_grpc
        && let Some(ct) = parts
            .headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .filter(|ct| is_multipart_content_type(ct))
    {
        if let Err(err) =
            parse_multipart_body(body_bytes.clone(), ct, &state.config.multipart_limits).await
        {
            warn!(error = %err, "multipart request rejected");
            let msg = match err.status_code() {
                StatusCode::PAYLOAD_TOO_LARGE => "Payload Too Large",
                _ => "Bad Request",
            };
            return error_response(err.status_code(), msg);
        }
    } else if !body_bytes.is_empty() {
        if is_grpc {
            if body_bytes.len() >= GRPC_HEADER_LEN {
                let flag = body_bytes[0];
                let msg_len = u32::from_be_bytes([
                    body_bytes[1],
                    body_bytes[2],
                    body_bytes[3],
                    body_bytes[4],
                ]) as usize;
                if flag > 1 {
                    let wf = GrpcWireFormat::from_headers(&parts.headers);
                    let err = GrpcError::InvalidCompressionFlag(flag);
                    return build_grpc_trailers_only_response(
                        wf,
                        err.status(),
                        Some(&err.to_string()),
                    )
                    .map(Either::Left);
                }
                if msg_len > state.config.max_grpc_message_size {
                    let wf = GrpcWireFormat::from_headers(&parts.headers);
                    return build_grpc_trailers_only_response(
                        wf,
                        GrpcStatus::ResourceExhausted,
                        Some("gRPC message exceeds max_grpc_message_size"),
                    )
                    .map(Either::Left);
                }
            }
        } else if parts.method == Method::POST
            && parts.uri.path().starts_with("/mcp")
            && let Some(obs) = extract_mcp_observability_headers(&body_bytes)
        {
            if let Some(ref m) = obs.mcp_method
                && let Ok(val) = HeaderValue::from_str(m)
            {
                parts.headers.insert("x-mcp-method", val);
            }
            if let Some(ref n) = obs.mcp_name
                && let Ok(val) = HeaderValue::from_str(n)
            {
                parts.headers.insert("x-mcp-name", val);
            }
        }
    }

    let log_meta = if state.access_logger.is_some() {
        Some((
            parts.method.as_str().to_owned(),
            parts.uri.path().to_owned(),
            match parts.version {
                http::Version::HTTP_10 => "HTTP/1.0",
                http::Version::HTTP_11 => "HTTP/1.1",
                http::Version::HTTP_2 => "HTTP/2.0",
                http::Version::HTTP_3 => "HTTP/3.0",
                _ => "HTTP/1.1",
            },
            parts
                .headers
                .get(header::REFERER)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_owned(),
            parts
                .headers
                .get(header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_owned(),
        ))
    } else {
        None
    };
    let method = &parts.method;

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
    let collector = match collector_shard {
        Some(shard) => state.collector_for_shard(shard),
        None => state.get_collector(),
    };
    if collector.send(pending).await.is_err() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "server shutting down");
    }

    // 6. Await response and apply configured Rust CORS / compression offloads.
    let hyper_resp = match reply_rx.await {
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

    // Feed the circuit breaker. 5xx counts as failure; everything else is
    // treated as success from the infrastructure's perspective.
    if let Some(controller) = state.dispatcher.backpressure() {
        if hyper_resp.status().is_server_error() {
            controller.record_failure();
        } else {
            controller.record_success();
        }
    }

    if let (
        Some(logger),
        Some(started),
        Some((log_method, log_path, log_proto, log_referer, log_ua)),
    ) = (&state.access_logger, access_log_start, log_meta)
    {
        let resp_bytes = hyper_resp.body().size_hint().lower() as usize;
        let remote_addr = peer_ip.to_string();
        logger.record(&AccessLogEntry {
            remote_addr: &remote_addr,
            method: &log_method,
            path: &log_path,
            protocol: log_proto,
            status: hyper_resp.status().as_u16(),
            response_bytes: resp_bytes,
            duration_micros: started.elapsed().as_micros() as u64,
            referer: &log_referer,
            user_agent: &log_ua,
        });
    }

    hyper_resp
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

#[inline]
fn extract_body_headers(headers: &HeaderMap) -> (Option<usize>, Option<&str>) {
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<usize>().ok());
    let active_encoding = is_active_content_encoding(
        headers
            .get(header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok()),
    );
    (content_length, active_encoding)
}

/// Encode an `http::HeaderMap` directly into compact binary `Bytes` in a single pass.
///
/// Wire format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`.
/// Duplicate header names are preserved in iteration order and oversized components are clamped.
#[inline]
pub fn encode_header_map_compact(headers: &HeaderMap) -> Bytes {
    if headers.is_empty() {
        return Bytes::new();
    }
    let mut buf = Vec::with_capacity(headers.len().saturating_mul(32));
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

fn is_multipart_content_type(ct: &str) -> bool {
    let trimmed = ct.trim_start();
    trimmed
        .get(..19)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("multipart/form-data"))
        && trimmed[19..]
            .chars()
            .next()
            .is_none_or(|ch| ch == ';' || ch.is_ascii_whitespace())
}

struct ResolvedWsMount<'a> {
    topic: &'a str,
    allowed_origins: &'a [String],
    supported_subprotocols: &'a [String],
    config: &'a WsConfig,
}

fn match_ws_mount<'a>(req_path: &'a str, state: &'a ServerState) -> Option<ResolvedWsMount<'a>> {
    if let Some(topic) = req_path.strip_prefix("/__vertebra/ws/")
        && !topic.is_empty()
    {
        return Some(ResolvedWsMount {
            topic,
            allowed_origins: &state.ws_allowed_origins,
            supported_subprotocols: &state.ws_supported_subprotocols,
            config: &state.ws_config,
        });
    }
    for mount in &state.ws_mounts {
        if let Some(topic) = mount.match_topic(req_path) {
            return Some(ResolvedWsMount {
                topic,
                allowed_origins: &mount.allowed_origins,
                supported_subprotocols: &mount.supported_subprotocols,
                config: &mount.config,
            });
        }
    }
    None
}

fn payload_to_ws_outgoing(payload: Bytes) -> WsOutgoing {
    match std::str::from_utf8(&payload) {
        Ok(text) => WsOutgoing::Text(text.to_owned()),
        Err(_) => WsOutgoing::Binary(payload.to_vec()),
    }
}

async fn handle_ws_upgrade(
    parts: &http::request::Parts,
    on_upgrade: Option<OnUpgrade>,
    ws_mount: ResolvedWsMount<'_>,
    state: &Arc<ServerState>,
) -> HttpResponse {
    let req_path = parts.uri.path();
    let _handshake_permit = if let Some(controller) = state.dispatcher.backpressure() {
        match controller.try_acquire_for_path(req_path).await {
            Ok(permit) => Some(permit),
            Err(reason) => return rejection_response(reason),
        }
    } else {
        None
    };
    let stream_guard = if let Some(controller) = state.dispatcher.backpressure() {
        match controller.acquire_stream() {
            Ok(guard) => Some(guard),
            Err(reason) => return rejection_response(reason),
        }
    } else {
        None
    };

    let allowed_refs: Vec<&str> = ws_mount
        .allowed_origins
        .iter()
        .map(String::as_str)
        .collect();
    if !validate_origin(&parts.headers, &allowed_refs) {
        return build_handshake_response(
            "",
            false,
            WebSocketHandshakeDecision::Reject {
                status: StatusCode::FORBIDDEN.as_u16(),
                headers: vec![(
                    "content-type".to_string(),
                    "text/plain; charset=utf-8".to_string(),
                )],
                body: Bytes::from_static(b"Origin not allowed"),
            },
        )
        .map(Either::Left);
    }

    let subprotocol_refs: Vec<&str> = ws_mount
        .supported_subprotocols
        .iter()
        .map(String::as_str)
        .collect();
    let subprotocol = negotiate_subprotocol(&parts.headers, &subprotocol_refs);
    let sec_key = match parts
        .headers
        .get(header::SEC_WEBSOCKET_KEY)
        .and_then(|v| v.to_str().ok())
    {
        Some(key) if !key.is_empty() => key,
        _ => return error_response(StatusCode::BAD_REQUEST, "Missing Sec-WebSocket-Key"),
    };
    let negotiate_deflate = ws_mount.config.enable_permessage_deflate
        && client_offers_permessage_deflate(&parts.headers);
    let Some(on_upgrade) = on_upgrade else {
        return error_response(
            StatusCode::UPGRADE_REQUIRED,
            "WebSocket upgrade requires HTTP/1.1 connection",
        );
    };

    let last_event_id = parts
        .headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.trim().parse::<u64>().ok());
    let topic_owned = ws_mount.topic.to_owned();
    let req_path_owned = req_path.to_owned();
    let ws_config = ws_mount.config.clone();
    let broker = Arc::clone(&state.pubsub_broker);
    let (replayed, mut envelope_rx) = broker.subscribe_with_replay(&topic_owned, last_event_id);
    let mut pattern_rx = topic_owned
        .contains('*')
        .then(|| broker.subscribe_pattern(&topic_owned));

    let resp = build_handshake_response(
        sec_key,
        negotiate_deflate,
        WebSocketHandshakeDecision::Accept {
            subprotocol: subprotocol.clone(),
            headers: Vec::new(),
        },
    )
    .map(Either::Left);

    let registry = Arc::clone(&state.ws_registry);
    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let upgraded = match on_upgrade.await {
            Ok(upgraded) => upgraded,
            Err(err) => {
                debug!(path = %req_path_owned, error = %err, "websocket upgrade failed");
                return;
            }
        };
        let ws_stream = WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded),
            Role::Server,
            Some(ws_config.to_tungstenite_config()),
        )
        .await;

        let (incoming_tx, mut incoming_rx) = tokio::sync::mpsc::channel::<WsIncoming>(64);
        let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::channel::<WsOutgoing>(64);

        let forward_topic = topic_owned.clone();
        let forward_broker = Arc::clone(&broker);
        let forward_task = tokio::spawn(async move {
            for env in replayed {
                if outgoing_tx
                    .send(payload_to_ws_outgoing(env.payload))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            loop {
                tokio::select! {
                    recv = envelope_rx.recv() => {
                        match recv {
                            Ok(env) => {
                                if outgoing_tx
                                    .send(payload_to_ws_outgoing(env.payload))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                debug!(skipped, topic = %forward_topic, "websocket pubsub subscriber lagged");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    pat_recv = async {
                        match pattern_rx.as_mut() {
                            Some(rx) => rx.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        match pat_recv {
                            Ok(payload) => {
                                if outgoing_tx
                                    .send(payload_to_ws_outgoing(payload))
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                debug!(skipped, pattern = %forward_topic, "websocket pattern subscriber lagged");
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    inc = incoming_rx.recv() => {
                        match inc {
                            Some(WsIncoming::Text(text)) => {
                                forward_broker.pubsub_publish(
                                    &forward_topic,
                                    Bytes::from(text.into_bytes()),
                                    None,
                                );
                            }
                            Some(WsIncoming::Binary(data)) => {
                                forward_broker.pubsub_publish(
                                    &forward_topic,
                                    Bytes::from(data),
                                    None,
                                );
                            }
                            Some(WsIncoming::Close(_, _)) | None => break,
                        }
                    }
                }
            }
        });

        run_ws_connection(
            ws_stream,
            req_path_owned,
            subprotocol,
            ws_config,
            registry,
            incoming_tx,
            outgoing_rx,
        )
        .await;

        forward_task.abort();
        let _ = forward_task.await;
    });

    resp
}

fn error_response(status: StatusCode, msg: &'static str) -> HttpResponse {
    Response::builder()
        .status(status)
        .body(full_body(Bytes::from_static(msg.as_bytes())))
        .unwrap()
}

fn build_zpage_pending(path: &'static str) -> (PendingRequest, oneshot::Receiver<PendingResponse>) {
    let (tx, rx) = oneshot::channel();
    let pending = PendingRequest {
        handler_id: ZPAGES_HANDLER_ID,
        method_tag: 0,
        http_version_tag: 1,
        flags: vertebra_core::protocol::flags::NONE,
        path_and_query: PathAndQuery::from_static(path),
        headers_mp: Bytes::new(),
        body: Bytes::new(),
        reply: tx,
    };
    (pending, rx)
}

enum CancelTarget<'a> {
    Bridge(&'a BridgeCorrelation),
    Shm(&'a ShmDispatcher),
}

struct CorrelationCancelGuard<'a> {
    target: CancelTarget<'a>,
    cid: Option<u64>,
}

impl<'a> CorrelationCancelGuard<'a> {
    fn bridge(corr: &'a BridgeCorrelation, cid: u64) -> Self {
        Self {
            target: CancelTarget::Bridge(corr),
            cid: Some(cid),
        }
    }

    fn shm(shm: &'a ShmDispatcher, cid: u64) -> Self {
        Self {
            target: CancelTarget::Shm(shm),
            cid: Some(cid),
        }
    }

    fn disarm(&mut self) {
        self.cid = None;
    }
}

impl Drop for CorrelationCancelGuard<'_> {
    fn drop(&mut self) {
        if let Some(cid) = self.cid.take() {
            match self.target {
                CancelTarget::Bridge(corr) => {
                    let _ = corr.cancel_correlation(cid);
                }
                CancelTarget::Shm(shm) => {
                    let _ = shm.cancel_correlation(cid);
                }
            }
        }
    }
}

async fn handle_zpages_request(zreq: ZPagesRequest, state: &Arc<ServerState>) -> HttpResponse {
    handle_zpages_request_with_timeout(zreq, state, ZPAGES_REQUEST_TIMEOUT).await
}

async fn handle_zpages_request_with_timeout(
    zreq: ZPagesRequest,
    state: &Arc<ServerState>,
    timeout: Duration,
) -> HttpResponse {
    match state.dispatcher.backend() {
        DispatchBackend::Bridge(corr) => {
            if let Some(id) = zreq.target_worker
                && id > 0
            {
                let body = format!(
                    "{{\"error\":\"worker_not_found\",\"worker_id\":{id},\"worker_count\":1}}"
                );
                return json_response(StatusCode::NOT_FOUND, Bytes::from(body), zreq.is_head)
                    .map(Either::Left);
            }
            let (pending, reply_rx) = build_zpage_pending(zreq.path);
            let Some(cid) = corr.dispatch_single(pending) else {
                return json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"error\":\"zpages_unavailable\"}"),
                    zreq.is_head,
                )
                .map(Either::Left);
            };
            let mut guard = CorrelationCancelGuard::bridge(corr, cid);
            match tokio::time::timeout(timeout, reply_rx).await {
                Ok(Ok(resp)) => {
                    guard.disarm();
                    let status = StatusCode::from_u16(resp.status)
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                    json_response(status, resp.body, zreq.is_head).map(Either::Left)
                }
                Ok(Err(_)) => json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"error\":\"zpages_unavailable\"}"),
                    zreq.is_head,
                )
                .map(Either::Left),
                Err(_) => json_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"{\"error\":\"zpages_timeout\"}"),
                    zreq.is_head,
                )
                .map(Either::Left),
            }
        }
        DispatchBackend::Direct(_) => {
            if let Some(id) = zreq.target_worker
                && id > 0
            {
                let body = format!(
                    "{{\"error\":\"worker_not_found\",\"worker_id\":{id},\"worker_count\":1}}"
                );
                return json_response(StatusCode::NOT_FOUND, Bytes::from(body), zreq.is_head)
                    .map(Either::Left);
            }
            let (pending, reply_rx) = build_zpage_pending(zreq.path);
            let mut reqs = vec![pending];
            if state.dispatcher.dispatch_requests(&mut reqs) == 0 {
                return json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"error\":\"zpages_unavailable\"}"),
                    zreq.is_head,
                )
                .map(Either::Left);
            }
            match tokio::time::timeout(timeout, reply_rx).await {
                Ok(Ok(resp)) => {
                    let status = StatusCode::from_u16(resp.status)
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                    json_response(status, resp.body, zreq.is_head).map(Either::Left)
                }
                Ok(Err(_)) => json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"error\":\"zpages_unavailable\"}"),
                    zreq.is_head,
                )
                .map(Either::Left),
                Err(_) => json_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    Bytes::from_static(b"{\"error\":\"zpages_timeout\"}"),
                    zreq.is_head,
                )
                .map(Either::Left),
            }
        }
        DispatchBackend::Shm(shm) => {
            if let Some(worker_id) = zreq.target_worker {
                let (pending, reply_rx) = build_zpage_pending(zreq.path);
                let cid = match shm.dispatch_to_worker(worker_id, pending) {
                    Ok(cid) => cid,
                    Err(ShmTargetError::NotFound) => {
                        let body = format!(
                            "{{\"error\":\"worker_not_found\",\"worker_id\":{worker_id},\"worker_count\":{}}}",
                            shm.worker_count()
                        );
                        return json_response(
                            StatusCode::NOT_FOUND,
                            Bytes::from(body),
                            zreq.is_head,
                        )
                        .map(Either::Left);
                    }
                    Err(ShmTargetError::Unhealthy) => {
                        let body =
                            format!("{{\"error\":\"worker_unhealthy\",\"worker_id\":{worker_id}}}");
                        return json_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            Bytes::from(body),
                            zreq.is_head,
                        )
                        .map(Either::Left);
                    }
                    Err(
                        ShmTargetError::RingFull
                        | ShmTargetError::CorrelationFull
                        | ShmTargetError::SendFailed,
                    ) => {
                        let body = format!(
                            "{{\"error\":\"worker_unavailable\",\"worker_id\":{worker_id}}}"
                        );
                        return json_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            Bytes::from(body),
                            zreq.is_head,
                        )
                        .map(Either::Left);
                    }
                };
                let mut guard = CorrelationCancelGuard::shm(shm, cid);
                return match tokio::time::timeout(timeout, reply_rx).await {
                    Ok(Ok(resp)) => {
                        guard.disarm();
                        let status = StatusCode::from_u16(resp.status)
                            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
                        json_response(status, resp.body, zreq.is_head).map(Either::Left)
                    }
                    Ok(Err(_)) => {
                        let body = format!(
                            "{{\"error\":\"worker_unavailable\",\"worker_id\":{worker_id}}}"
                        );
                        json_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            Bytes::from(body),
                            zreq.is_head,
                        )
                        .map(Either::Left)
                    }
                    Err(_) => {
                        let body =
                            format!("{{\"error\":\"zpages_timeout\",\"worker_id\":{worker_id}}}");
                        json_response(StatusCode::GATEWAY_TIMEOUT, Bytes::from(body), zreq.is_head)
                            .map(Either::Left)
                    }
                };
            }

            let healthy_ids = shm.healthy_worker_ids();
            if healthy_ids.is_empty() {
                return json_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"{\"error\":\"no_healthy_workers\"}"),
                    zreq.is_head,
                )
                .map(Either::Left);
            }

            let futs = healthy_ids.into_iter().map(|worker_id| {
                let (pending, reply_rx) = build_zpage_pending(zreq.path);
                let guard_res = shm
                    .dispatch_to_worker(worker_id, pending)
                    .map(|cid| CorrelationCancelGuard::shm(shm, cid));
                async move {
                    let mut guard = match guard_res {
                        Ok(guard) => guard,
                        Err(ShmTargetError::NotFound) => {
                            return WorkerZPageOutcome::Error {
                                worker_id,
                                message: "worker_not_found".to_string(),
                            };
                        }
                        Err(ShmTargetError::Unhealthy) => {
                            return WorkerZPageOutcome::Error {
                                worker_id,
                                message: "worker_unhealthy".to_string(),
                            };
                        }
                        Err(
                            ShmTargetError::RingFull
                            | ShmTargetError::CorrelationFull
                            | ShmTargetError::SendFailed,
                        ) => {
                            return WorkerZPageOutcome::Error {
                                worker_id,
                                message: "worker_unavailable".to_string(),
                            };
                        }
                    };
                    match tokio::time::timeout(timeout, reply_rx).await {
                        Ok(Ok(resp)) => {
                            guard.disarm();
                            if resp.status != StatusCode::OK.as_u16() {
                                return WorkerZPageOutcome::Error {
                                    worker_id,
                                    message: format!("worker_status_{}", resp.status),
                                };
                            }
                            match serde_json::from_slice::<serde_json::Value>(&resp.body) {
                                Ok(payload) => WorkerZPageOutcome::Ok { worker_id, payload },
                                Err(_) => WorkerZPageOutcome::Error {
                                    worker_id,
                                    message: "invalid_json".to_string(),
                                },
                            }
                        }
                        Ok(Err(_)) => WorkerZPageOutcome::Error {
                            worker_id,
                            message: "worker_unavailable".to_string(),
                        },
                        Err(_) => WorkerZPageOutcome::Timeout { worker_id },
                    }
                }
            });

            let outcomes = futures_util::future::join_all(futs).await;
            let (status, body) = merge_shm_zpages_responses(zreq.path, outcomes);
            json_response(status, body, zreq.is_head).map(Either::Left)
        }
    }
}

/// Encode response headers into the compact binary format.
///
/// Format per header: `[name_len: u8][name: bytes][value_len: u16 LE][value: bytes]`
pub fn encode_response_headers(headers: &[(Vec<u8>, Vec<u8>)]) -> Bytes {
    Bytes::from(encode_headers_compact(headers))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::time::Duration;

    use http_body_util::BodyExt;
    use tokio::sync::mpsc;
    use vertebra_core::transport::BatchSnapshot;
    use vertebra_core::transport::ring_pair::{RingPair, RingPairConfig};

    use crate::backpressure::BackpressureConfig;
    use crate::batcher::Batch;
    use crate::config::ServerConfig;
    use crate::dispatcher::{Dispatcher, ShmDispatcher};
    use crate::routing::{RouteEntry, Router, ZPAGES_HANDLER_ID, handler_id};
    use crate::zpages::{ZPagesConfig, ZPagesRequest};

    async fn read_http_body(resp: HttpResponse) -> Bytes {
        resp.into_body()
            .collect()
            .await
            .expect("collect response body")
            .to_bytes()
    }

    fn make_direct_state(
        zpages: ZPagesConfig,
        with_user_taskz: bool,
        with_saturated_bp: bool,
    ) -> (
        Arc<ServerState>,
        mpsc::Receiver<Batch>,
        mpsc::Receiver<PendingRequest>,
    ) {
        let mut entries = Vec::new();
        if with_user_taskz {
            entries.push(RouteEntry {
                method: "GET".to_string(),
                path: "/taskz".to_string(),
                handler_id: handler_id("GET", "/taskz"),
            });
        }
        let router = Router::from_entries(entries).unwrap();
        let (mut dispatcher, batch_rx) = Dispatcher::new_direct(16);
        if with_saturated_bp {
            dispatcher = dispatcher.with_backpressure(BackpressureConfig {
                max_in_flight: 0,
                ..BackpressureConfig::default()
            });
        }
        let cfg = ServerConfig {
            zpages,
            ..ServerConfig::default()
        };
        let (collector_tx, collector_rx) = mpsc::channel(16);
        let state = Arc::new(ServerState::new(
            Arc::new(router),
            Arc::new(dispatcher),
            Arc::new(cfg),
            vec![collector_tx],
        ));
        (state, batch_rx, collector_rx)
    }

    fn make_shm_state(
        zpages: ZPagesConfig,
        worker_count: usize,
    ) -> (Arc<ServerState>, Arc<ShmDispatcher>, Vec<Arc<RingPair>>) {
        let pairs: Vec<Arc<RingPair>> = (0..worker_count)
            .map(|_| Arc::new(RingPair::create(RingPairConfig::default()).unwrap()))
            .collect();
        let shm = ShmDispatcher::new(pairs.clone(), 64);
        let dispatcher = Dispatcher::from_shm(Arc::clone(&shm));
        let cfg = ServerConfig {
            zpages,
            ..ServerConfig::default()
        };
        let (collector_tx, _collector_rx) = mpsc::channel(16);
        let state = Arc::new(ServerState::new(
            Arc::new(Router::from_entries(Vec::new()).unwrap()),
            Arc::new(dispatcher),
            Arc::new(cfg),
            vec![collector_tx],
        ));
        (state, shm, pairs)
    }

    #[tokio::test]
    async fn disabled_zpages_routes_to_user_app() {
        let (state, _batch_rx, mut collector_rx) =
            make_direct_state(ZPagesConfig::default(), true, false);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/taskz")
            .body(Bytes::new())
            .unwrap();

        let resp_task = tokio::spawn(handle_buffered(req, Arc::clone(&state)));
        let pending = collector_rx
            .recv()
            .await
            .expect("user route should reach collector");
        assert_eq!(pending.handler_id, handler_id("GET", "/taskz"));
        assert_ne!(pending.handler_id, ZPAGES_HANDLER_ID);
        pending
            .reply
            .send(PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"user-taskz"),
                stream_rx: None,
            })
            .unwrap();

        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_http_body(resp).await.as_ref(), b"user-taskz");
    }

    #[tokio::test]
    async fn enabled_zpages_intercepts_above_backpressure() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, mut batch_rx, _collector_rx) = make_direct_state(cfg, true, true);
        let req = Request::builder()
            .method(Method::GET)
            .uri("/taskz")
            .body(Bytes::new())
            .unwrap();

        let resp_task = tokio::spawn(handle_buffered(req, Arc::clone(&state)));
        let mut batch = batch_rx
            .recv()
            .await
            .expect("zpages request should dispatch directly");
        assert_eq!(batch.requests.len(), 1);
        let pending = batch.requests.pop().unwrap();
        assert_eq!(pending.handler_id, ZPAGES_HANDLER_ID);
        assert_eq!(pending.path(), "/taskz");
        pending
            .reply
            .send(PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"{\"total_tasks\":1}"),
                stream_rx: None,
            })
            .unwrap();

        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            resp.headers()
                .get("cache-control")
                .and_then(|v| v.to_str().ok()),
            Some("no-store")
        );
        assert_eq!(read_http_body(resp).await.as_ref(), b"{\"total_tasks\":1}");
    }

    #[tokio::test]
    async fn head_taskz_returns_empty_body() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, mut batch_rx, _collector_rx) = make_direct_state(cfg, false, false);
        let req = Request::builder()
            .method(Method::HEAD)
            .uri("/taskz")
            .body(Bytes::new())
            .unwrap();

        let resp_task = tokio::spawn(handle_buffered(req, Arc::clone(&state)));
        let mut batch = batch_rx
            .recv()
            .await
            .expect("zpages HEAD should dispatch directly");
        let pending = batch.requests.pop().unwrap();
        pending
            .reply
            .send(PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"{\"total_tasks\":3}"),
                stream_rx: None,
            })
            .unwrap();

        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert!(read_http_body(resp).await.is_empty());
    }

    #[tokio::test]
    async fn wedged_worker_returns_504_timeout() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, mut batch_rx, _collector_rx) = make_direct_state(cfg, false, false);
        let zreq = ZPagesRequest {
            path: "/taskz",
            is_head: false,
            target_worker: None,
        };

        let resp_task = tokio::spawn(async move {
            handle_zpages_request_with_timeout(zreq, &state, Duration::from_millis(50)).await
        });
        let _held_batch = batch_rx.recv().await.expect("receive wedged batch");

        let resp = resp_task.await.unwrap();
        assert_eq!(resp.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(
            read_http_body(resp).await.as_ref(),
            b"{\"error\":\"zpages_timeout\"}"
        );
    }

    #[tokio::test]
    async fn shm_targeted_worker_dispatch_and_errors() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, shm, pairs) = make_shm_state(cfg, 2);

        // Target worker 1 only.
        let req = Request::builder()
            .method(Method::GET)
            .uri("/taskz?worker=1")
            .body(Bytes::new())
            .unwrap();
        let resp_task = tokio::spawn(handle_buffered(req, Arc::clone(&state)));

        let view = loop {
            if let Ok(v) = pairs[1].request_ring.try_recv_view() {
                break v;
            }
            tokio::task::yield_now().await;
        };
        assert!(pairs[0].request_ring.try_recv_view().is_err());
        let snap = BatchSnapshot::from_mpmc_slot(view).unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap.node(0).handler_id, ZPAGES_HANDLER_ID);
        let cid = snap.node(0).correlation_id;
        assert!(shm.complete_one(
            cid,
            PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"{\"worker\":1}"),
                stream_rx: None,
            },
        ));
        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read_http_body(resp).await.as_ref(), b"{\"worker\":1}");

        // Out-of-range worker 9 returns 404.
        let req_missing = Request::builder()
            .method(Method::GET)
            .uri("/taskz?worker=9")
            .body(Bytes::new())
            .unwrap();
        let resp_missing = handle_buffered(req_missing, Arc::clone(&state))
            .await
            .unwrap();
        assert_eq!(resp_missing.status(), StatusCode::NOT_FOUND);
        let missing_json: serde_json::Value =
            serde_json::from_slice(&read_http_body(resp_missing).await).unwrap();
        assert_eq!(missing_json["error"], "worker_not_found");
        assert_eq!(missing_json["worker_id"], 9);
        assert_eq!(missing_json["worker_count"], 2);

        // Unhealthy worker 1 returns 503.
        shm.set_worker_healthy(1, false);
        let req_unhealthy = Request::builder()
            .method(Method::GET)
            .uri("/taskz?worker=1")
            .body(Bytes::new())
            .unwrap();
        let resp_unhealthy = handle_buffered(req_unhealthy, Arc::clone(&state))
            .await
            .unwrap();
        assert_eq!(resp_unhealthy.status(), StatusCode::SERVICE_UNAVAILABLE);
        let unhealthy_json: serde_json::Value =
            serde_json::from_slice(&read_http_body(resp_unhealthy).await).unwrap();
        assert_eq!(unhealthy_json["error"], "worker_unhealthy");
        assert_eq!(unhealthy_json["worker_id"], 1);
    }

    #[tokio::test]
    async fn shm_fanout_merges_two_workers_in_parallel() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, shm, pairs) = make_shm_state(cfg, 2);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/taskz")
            .body(Bytes::new())
            .unwrap();
        let resp_task = tokio::spawn(handle_buffered(req, Arc::clone(&state)));

        for (worker_id, pair) in pairs.iter().enumerate() {
            let view = loop {
                if let Ok(v) = pair.request_ring.try_recv_view() {
                    break v;
                }
                tokio::task::yield_now().await;
            };
            let snap = BatchSnapshot::from_mpmc_slot(view).unwrap();
            assert_eq!(snap.node(0).handler_id, ZPAGES_HANDLER_ID);
            let cid = snap.node(0).correlation_id;
            let body = serde_json::to_vec(&serde_json::json!({
                "total_tasks": worker_id + 1,
                "tasks": [{"id": worker_id, "name": format!("t{worker_id}")}],
                "loops": {"main": {"running": true}}
            }))
            .unwrap();
            assert!(shm.complete_one(
                cid,
                PendingResponse {
                    status: 200,
                    headers_mp: Bytes::new(),
                    body: Bytes::from(body),
                    stream_rx: None,
                },
            ));
        }

        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&read_http_body(resp).await).unwrap();
        assert_eq!(json["total_tasks"], 3);
        assert_eq!(json["tasks"].as_array().unwrap().len(), 2);
        assert!(json["loops"].get("worker.0.main").is_some());
        assert!(json["loops"].get("worker.1.main").is_some());
        assert_eq!(json["workers"].as_array().unwrap().len(), 2);
        assert_eq!(shm.in_flight(), 0);
    }

    #[tokio::test]
    async fn shm_timeout_and_client_drop_reclaim_slots_immediately() {
        let cfg = ZPagesConfig {
            enabled: true,
            bearer_token: None,
        };
        let (state, shm, pairs) = make_shm_state(cfg, 2);

        // 1. Timeout case: worker 0 replies, worker 1 never replies.
        let zreq = ZPagesRequest {
            path: "/taskz",
            is_head: false,
            target_worker: None,
        };
        let state_clone = Arc::clone(&state);
        let resp_task = tokio::spawn(async move {
            handle_zpages_request_with_timeout(zreq, &state_clone, Duration::from_millis(50)).await
        });

        let view0 = loop {
            if let Ok(v) = pairs[0].request_ring.try_recv_view() {
                break v;
            }
            tokio::task::yield_now().await;
        };
        let snap0 = BatchSnapshot::from_mpmc_slot(view0).unwrap();
        assert!(shm.complete_one(
            snap0.node(0).correlation_id,
            PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"{\"total_tasks\":1,\"tasks\":[],\"loops\":{}}"),
                stream_rx: None,
            },
        ));
        // Drain worker 1 ring without replying.
        let _view1 = loop {
            if let Ok(v) = pairs[1].request_ring.try_recv_view() {
                break v;
            }
            tokio::task::yield_now().await;
        };

        let resp = resp_task.await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(shm.in_flight(), 0);
        assert_eq!(shm.worker_in_flight(1), 0);

        // 2. Client drop case: poll once so requests are dispatched, then drop future.
        let mut drop_fut = Box::pin(handle_zpages_request_with_timeout(
            zreq,
            &state,
            Duration::from_secs(5),
        ));
        let waker = futures_util::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(drop_fut.as_mut().poll(&mut cx).is_pending());
        assert_eq!(shm.in_flight(), 2);
        drop(drop_fut);
        assert_eq!(shm.in_flight(), 0);
        assert_eq!(shm.worker_in_flight(0), 0);
        assert_eq!(shm.worker_in_flight(1), 0);
    }

    #[test]
    fn encode_header_map_compact_single_pass_roundtrips_duplicates_and_long_values() {
        assert!(encode_header_map_compact(&HeaderMap::new()).is_empty());

        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("example.test"));
        headers.append(header::SET_COOKIE, HeaderValue::from_static("a=1"));
        headers.append(header::SET_COOKIE, HeaderValue::from_static("b=2"));
        let long_val = "x".repeat(512);
        headers.insert(
            http::HeaderName::from_static("x-long-value"),
            HeaderValue::from_str(&long_val).unwrap(),
        );

        let encoded = encode_header_map_compact(&headers);
        let decoded: Vec<(&[u8], &[u8])> = iter_raw_headers(&encoded).collect();
        assert_eq!(decoded.len(), 4);
        assert!(decoded.contains(&(b"host".as_slice(), b"example.test".as_slice())));
        assert!(decoded.contains(&(b"set-cookie".as_slice(), b"a=1".as_slice())));
        assert!(decoded.contains(&(b"set-cookie".as_slice(), b"b=2".as_slice())));
        assert!(decoded.contains(&(b"x-long-value".as_slice(), long_val.as_bytes())));
    }

    #[tokio::test]
    async fn dispatch_request_pins_explicit_collector_shard_and_validates_buffered_encoding() {
        let router = Router::from_entries(vec![RouteEntry {
            method: "POST".to_string(),
            path: "/echo".to_string(),
            handler_id: handler_id("POST", "/echo"),
        }])
        .unwrap();
        let (dispatcher, _batch_rx) = Dispatcher::new_direct(8);
        let (tx0, mut rx0) = mpsc::channel(8);
        let (tx1, mut rx1) = mpsc::channel(8);
        let state = Arc::new(ServerState::new(
            Arc::new(router),
            Arc::new(dispatcher),
            Arc::new(ServerConfig::default()),
            vec![tx0, tx1],
        ));

        let req = Request::builder()
            .method(Method::POST)
            .uri("/echo")
            .body(Bytes::from_static(b"hello"))
            .unwrap();
        let (parts, body) = req.into_parts();
        let resp_task = tokio::spawn(dispatch_request(
            parts,
            RequestBodySource::Buffered(body),
            None,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            Some(1),
            Arc::clone(&state),
        ));
        let pending = rx1
            .recv()
            .await
            .expect("pinned shard 1 must receive request");
        assert!(
            rx0.try_recv().is_err(),
            "shard 0 must not receive pinned request"
        );
        assert_eq!(pending.body.as_ref(), b"hello");
        pending
            .reply
            .send(PendingResponse {
                status: 200,
                headers_mp: Bytes::new(),
                body: Bytes::from_static(b"ok"),
                stream_rx: None,
            })
            .unwrap();
        let resp = resp_task.await.unwrap().unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bad_enc = Request::builder()
            .method(Method::POST)
            .uri("/echo")
            .header(header::CONTENT_ENCODING, "bogus-codec")
            .body(Bytes::from_static(b"data"))
            .unwrap();
        let resp_bad = handle_buffered(bad_enc, Arc::clone(&state)).await.unwrap();
        assert_eq!(resp_bad.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
}
