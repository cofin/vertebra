---
type: Guide
title: "Protocol and Offload Modules in vertebra-http"
description: "Inventory of vertebra-http protocol, body streaming, realtime, MCP bridge, and offload modules with their public symbols and server wiring."
status: stable
tags:
  - architecture
  - http
  - protocols
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: ff9f78cb
  - by: agent:flow-executor
    at: "2026-11-14T22:27:00Z"
    commit: ff9f78cb
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-lib-rs
    resource: src/rs/vertebra-http/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/lib.rs)"
  - id: src-vertebra-http-src-body-rs
    resource: src/rs/vertebra-http/src/body.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/body.rs)"
  - id: src-vertebra-http-src-mcp-bridge-rs
    resource: src/rs/vertebra-http/src/mcp_bridge.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/mcp_bridge.rs)"
  - id: src-vertebra-http-src-response-rs
    resource: src/rs/vertebra-http/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/response.rs)"
  - id: src-vertebra-http-src-server-rs
    resource: src/rs/vertebra-http/src/server.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/server.rs)"
  - id: src-vertebra-http-src-cors-rs
    resource: src/rs/vertebra-http/src/cors.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/cors.rs)"
  - id: src-vertebra-http-src-compression-rs
    resource: src/rs/vertebra-http/src/compression.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/compression.rs)"
  - id: src-vertebra-http-src-assets-rs
    resource: src/rs/vertebra-http/src/assets.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/assets.rs)"
  - id: src-vertebra-http-src-h2c-rs
    resource: src/rs/vertebra-http/src/h2c.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/h2c.rs)"
  - id: src-vertebra-http-src-http3-rs
    resource: src/rs/vertebra-http/src/http3.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/http3.rs)"
  - id: src-vertebra-http-src-tls-rs
    resource: src/rs/vertebra-http/src/tls.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/tls.rs)"
  - id: src-vertebra-http-src-health-rs
    resource: src/rs/vertebra-http/src/health.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/health.rs)"
  - id: src-vertebra-http-src-hmr-rs
    resource: src/rs/vertebra-http/src/hmr.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/hmr.rs)"
  - id: src-vertebra-http-src-payload-rs
    resource: src/rs/vertebra-http/src/payload.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/payload.rs)"
  - id: src-vertebra-http-src-multipart-rs
    resource: src/rs/vertebra-http/src/multipart.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/multipart.rs)"
  - id: src-vertebra-http-src-access-log-rs
    resource: src/rs/vertebra-http/src/access_log.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/access_log.rs)"
  - id: src-vertebra-http-src-profile-rs
    resource: src/rs/vertebra-http/src/profile.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/profile.rs)"
  - id: src-vertebra-http-src-discovery-rs
    resource: src/rs/vertebra-http/src/discovery.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/discovery.rs)"
  - id: src-vertebra-http-src-grpc-rs
    resource: src/rs/vertebra-http/src/grpc.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/grpc.rs)"
  - id: src-vertebra-http-src-sse-rs
    resource: src/rs/vertebra-http/src/sse.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/sse.rs)"
  - id: src-vertebra-http-src-ws-mount-rs
    resource: src/rs/vertebra-http/src/ws_mount.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/ws_mount.rs)"
  - id: src-vertebra-http-src-pubsub-rs
    resource: src/rs/vertebra-http/src/pubsub.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/pubsub.rs)"
  - id: src-vertebra-http-src-dispatcher-direct-rs
    resource: src/rs/vertebra-http/src/dispatcher/direct.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/direct.rs)"
  - id: src-vertebra-http-src-webtransport-rs
    resource: src/rs/vertebra-http/src/webtransport.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/webtransport.rs)"
  - id: src-vertebra-core-src-logger-rs
    resource: src/rs/vertebra-core/src/logger.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/logger.rs)"
---

# Protocol and Offload Modules in vertebra-http

<!-- truth: start -->
- `vertebra-http` executes health checks, CORS preflight/response headers, `zstd`/`brotli`/`gzip` compression, static mounts, OpenAPI caching, HMR WebSockets, and `.well-known` endpoints directly in Rust without waking Python workers.
- Realtime, body-streaming, and framing modules (`body.rs`, `grpc.rs`, `sse.rs`, `ws_mount.rs`, `pubsub.rs`, `multipart.rs`, `mcp_bridge.rs`, `http3.rs`) expose Rust protocol primitives wired into `ServerState`, `server::run_with_listener`, `response::handle`, and `response::handle_buffered` (alongside standalone library helpers).
<!-- truth: end -->

[`lib.rs`](../../../../../src/rs/vertebra-http/src/lib.rs)[^src-vertebra-http-src-lib-rs] exports every top-level module as `pub mod` (whereas `dispatcher::direct` in [`dispatcher/mod.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs) is a private `mod direct;`). The modules in the first table below are wired directly into `main.rs`, `server.rs`, and [`response::handle`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs]. The remaining modules are standalone library components exercised by `src/rs/vertebra-http/tests/`, unit tests, and `benches/server_throughput.rs` (`vertebra-py` does not depend on `vertebra-http`).

## Wired into the server

| Module | Symbols | Use on the request path |
|---|---|---|
| [`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs)[^src-vertebra-http-src-hmr-rs] | `HmrState`, `HmrEvent`, `HmrEventKind`, `HMR_PATH` (`/__vertebra/hmr`), `HMR_SIGNAL_PATH`, `HMR_EVENT_NAME` (`vertebra:types-changed`), `is_hmr_path`, `handle` | Dev-only WebSocket and signal endpoint, first step of `response::handle` |
| [`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs)[^src-vertebra-http-src-health-rs] | `HealthEndpoint`, `HealthState` (`Healthy`, `Draining`, `ShuttingDown`) | Cached once on `ServerState::health_endpoint`; `try_intercept` answers `/healthz`, `/livez`, `/__vertebra/health`, `/readyz` |
| [`cors.rs`](../../../../../src/rs/vertebra-http/src/cors.rs)[^src-vertebra-http-src-cors-rs] | `CorsOffloadConfig`, `CompiledCorsOffload` (`evaluate_preflight`, `apply_response_headers`), `append_vary_token` | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `VertebraPlugin`; intercepts `OPTIONS` preflights and attaches CORS + multi-token `Vary` response headers |
| [`compression.rs`](../../../../../src/rs/vertebra-http/src/compression.rs)[^src-vertebra-http-src-compression-rs] | `CompressionBackend` (`Zstd`, `Brotli`, `Gzip`), `CompressionOffloadConfig` (`compress_if_eligible`) | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `VertebraPlugin`; negotiates `Accept-Encoding`, skips incompressible `Content-Type`s, compresses unary bodies >= `minimum_size`, and delegates `Vary: Accept-Encoding` to `cors::append_vary_token` |
| [`assets.rs`](../../../../../src/rs/vertebra-http/src/assets.rs)[^src-vertebra-http-src-assets-rs] & [`server.rs`](../../../../../src/rs/vertebra-http/src/server.rs)[^src-vertebra-http-src-server-rs] | `AssetManager`, `StaticAssetServeConfig`, `StaticAssetMount`, `CompiledOpenApiCache` | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `VertebraPlugin`; serves pre-rendered OpenAPI JSON (`ETag`/`304`) and static directories (pre-compressed sidecars, `ETag`/`304`, `Range`/`206`, SPA fallback) before backpressure/Python |
| [`h2c.rs`](../../../../../src/rs/vertebra-http/src/h2c.rs)[^src-vertebra-http-src-h2c-rs] | `H2C_PREFACE`, `CleartextProtocol`, `detect_cleartext_protocol` | Gated behind `ServerConfig::enable_h2c_prior_knowledge` (`VERTEBRA_H2C=1`); selects `configure_auto_builder(...).http2_only()` on `CleartextProtocol::H2cPriorKnowledge`, skipped on the default zero-peek `http1::Builder` fastpath |
| [`http3.rs`](../../../../../src/rs/vertebra-http/src/http3.rs)[^src-vertebra-http-src-http3-rs] & [`webtransport.rs`](../../../../../src/rs/vertebra-http/src/webtransport.rs)[^src-vertebra-http-src-webtransport-rs] | `Http3Config` (`enabled: false`, `alt_svc_max_age: 86_400`, `max_concurrent_bidi_streams: 256`, `keep_alive_interval_ms: 15_000`, `enable_webtransport: true`, `max_webtransport_sessions: 64`), `Http3Error`, `format_alt_svc_header`, `strip_hop_by_hop_headers`, `build_quic_server_config`, `serve_http3_with_bridge`, `handle_h3_request`, `inject_alt_svc_header`, `WebTransportSession`, `WebTransportStream`, `WebTransportRegistry`, `WebTransportError` | Feature `http3`; `server::run_with_listener` validates TLS + feature flags (`validate_http3`), binds the UDP `quinn::Endpoint` (`bind_udp_listener`), enforces `max_body_size` pre-buffering, strips RFC 9114 hop-by-hop response headers, dispatches HTTP/3 requests via `response::handle_buffered`, routes extended `CONNECT` WebTransport sessions through `BatchBridge::submit_webtransport_session` (`Topology::Embedded`) or returns `501 Not Implemented` (`Topology::Shm`), and gates `Alt-Svc` on `ServerState::http3_port` |
| [`tls.rs`](../../../../../src/rs/vertebra-http/src/tls.rs)[^src-vertebra-http-src-tls-rs] | `TlsAcceptor` (`from_config`, `accept`, `rustls_config`, `peer_identity`), `MtlsConfig`, `ClientAuthMode` | Acceptor used by `serve_tls` (feature `tls`) |
| [`profile.rs`](../../../../../src/rs/vertebra-http/src/profile.rs)[^src-vertebra-http-src-profile-rs] | `ProfileConfig`, `ProfileEndpoint` (`try_handle_async`, `try_handle`) | Cached on `ServerState::profile_endpoint` (`VERTEBRA_PPROF_ENABLED` / `VERTEBRA_PPROF_TOKEN`); `response.rs` calls `try_handle_async` to serve `/debug/pprof/profile` (`tokio::task::spawn_blocking` CPU sampling with `pprof` under feature `profiling`) and `/debug/pprof/heap` (`404`) with constant-time Bearer token validation |
| [`discovery.rs`](../../../../../src/rs/vertebra-http/src/discovery.rs)[^src-vertebra-http-src-discovery-rs] | `WellKnownDiscoveryCache`, `CachedDiscoveryDoc`, `DiscoveryResponse` | Cached on `ServerState::discovery_cache`; `serve` answers `/.well-known/agent-card.json` and `/.well-known/mcp.json` (`ETag`/`304 Not Modified`) before backpressure |
| [`payload.rs`](../../../../../src/rs/vertebra-http/src/payload.rs)[^src-vertebra-http-src-payload-rs] | `PayloadLimits`, `PayloadError`, `collect_bounded_body`, `decode_buffered_body`, `decode_request_payload` | Configured via `ServerConfig::payload_limits`; enforces streaming (`handle`) and pre-buffered (`handle_buffered`) byte caps (`413 Payload Too Large`) plus bounded `Content-Encoding` decompression |
| [`multipart.rs`](../../../../../src/rs/vertebra-http/src/multipart.rs)[^src-vertebra-http-src-multipart-rs] | `MultipartLimits`, `MultipartPart`, `ParsedMultipart`, `parse_multipart_body` | Configured via `ServerConfig::multipart_limits`; `response.rs` invokes `parse_multipart_body` (`multer`) on `multipart/form-data` requests, returning `413` / `400` on limit or boundary errors |
| [`grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs)[^src-vertebra-http-src-grpc-rs] | `parse_grpc_timeout`, `GRPC_HEADER_LEN`, `GrpcWireFormat`, `GrpcError`, `GrpcStatus`, `build_grpc_trailers_only_response` | Configured via `ServerConfig::max_grpc_message_size`; validates `application/grpc` request framing in `response.rs` (`is_grpc_request`, `decode_grpc_frames`, `encode_grpc_frame`, and `build_grpc_response_with_trailers` are library helpers) |
| [`access_log.rs`](../../../../../src/rs/vertebra-http/src/access_log.rs)[^src-vertebra-http-src-access-log-rs] | `AccessLogger`, `AccessLogFormat`, `AccessLogEntry`, `format_utc_clf_timestamp` | Cached on `ServerState::access_logger` (`VERTEBRA_ACCESS_LOG`); formats CLF or JSON entries (including live `peer_ip` from `handle_with_peer`) directly into a bounded non-blocking [`AsyncLogCore`](../../../../../src/rs/vertebra-core/src/logger.rs)[^src-vertebra-core-src-logger-rs] stream (`OverflowPolicy::DropOldest`) and flushes on server shutdown |
| [`body.rs`](../../../../../src/rs/vertebra-http/src/body.rs)[^src-vertebra-http-src-body-rs] | `FrameReceiver`, `ResponseBody` (`Either<Full<Bytes>, FrameReceiver>`), `HttpResponse`, `PendingResponse` (`unary`, `streaming`), `full_body`, `stream_body` | Re-exported by `response.rs`; carries unary `Full<Bytes>` and progressive chunk/SSE/gRPC trailer streams through `batcher.rs` and `response::handle` |
| [`mcp_bridge.rs`](../../../../../src/rs/vertebra-http/src/mcp_bridge.rs)[^src-vertebra-http-src-mcp-bridge-rs] | `extract_mcp_observability_headers`, `McpObservabilityHeaders` | Wired into `response.rs` to inspect JSON-RPC 2.0 `method` and `params.name` fields on MCP requests |
| [`sse.rs`](../../../../../src/rs/vertebra-http/src/sse.rs)[^src-vertebra-http-src-sse-rs], [`pubsub.rs`](../../../../../src/rs/vertebra-http/src/pubsub.rs)[^src-vertebra-http-src-pubsub-rs], [`ws_mount.rs`](../../../../../src/rs/vertebra-http/src/ws_mount.rs)[^src-vertebra-http-src-ws-mount-rs] | `SseHub`, `PubSubBroker`, `WsConnectionRegistry`, `WsMount`, `is_websocket_upgrade_parts` | Wired on `ServerState` and `response.rs`: `/__vertebra/sse/{topic}` subscribes to `sse_hub`, `/__vertebra/ws/{topic}` and `ws_mounts` upgrade RFC 6455 connections via `ws_mount` (`handle_ws_upgrade`), `PubSubBroker::pubsub_publish` receives events from `BatchBridge` (`Embedded`) or `spawn_shm_channel_forwarder` (`Shm`), and `ws_registry.close_all(1001, "Server shutting down")` drains WebSockets on shutdown |

## Library only (no production caller in `response::handle`)

| Module | Symbols | Notes |
|---|---|---|
| `tls.rs` (identity helpers) | `IpCidr`, `TrustedProxyConfig`, `ClientIdentity`, `ProxyAuthMetadata`, `extract_client_identity_from_der`, `normalize_trusted_proxy_headers` | Certificate CN and proxy XFCC/IAP header helpers |
| [`dispatcher/direct.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/direct.rs)[^src-vertebra-http-src-dispatcher-direct-rs] & `benches/support/mod.rs` | `dispatch_direct_vec` (`dispatcher/direct.rs`); `DirectWorkerPool`, `AdaptiveWorkerConfig`, `run_adaptive_worker`, `BatchHandler` (`benches/support/mod.rs`, uncompiled sketch) | Direct-mode channel producer for `DispatchBackend::Direct` |

Additional domain-specific modules:

- `discovery.rs`: [Litestar Integration](../../domains/extensions/litestar.md) and [MCP & A2A Protocol Harness](../../domains/agent/mcp-and-a2a-protocol-harness.md).
- `grpc.rs`: [gRPC Services](../../domains/extensions/grpc.md).

## Rules for changes

- Wiring a library module means caching compiled configuration on `ServerState` (`0` per-request overhead when unconfigured), invoking it in `response::handle` (request interceptors before admission/routing, response filters in `build_response_with_offloads`), and exposing configuration in `ServerConfig::from_env` or `apply_offload_json`.
- Verify live call sites with `rg 'crate::<module>' src/rs/vertebra-http/src` before documenting a module as active on the `vertebra-server` hot path.

[^src-vertebra-http-src-lib-rs]: `src/rs/vertebra-http/src/lib.rs` — Rust implementation module (src/rs/vertebra-http/src/lib.rs).
[^src-vertebra-http-src-body-rs]: `src/rs/vertebra-http/src/body.rs` — Rust implementation module (src/rs/vertebra-http/src/body.rs).
[^src-vertebra-http-src-mcp-bridge-rs]: `src/rs/vertebra-http/src/mcp_bridge.rs` — Rust implementation module (src/rs/vertebra-http/src/mcp_bridge.rs).
[^src-vertebra-http-src-response-rs]: `src/rs/vertebra-http/src/response.rs` — Rust implementation module (src/rs/vertebra-http/src/response.rs).
[^src-vertebra-http-src-server-rs]: `src/rs/vertebra-http/src/server.rs` — Rust implementation module (src/rs/vertebra-http/src/server.rs).
[^src-vertebra-http-src-cors-rs]: `src/rs/vertebra-http/src/cors.rs` — Rust implementation module (src/rs/vertebra-http/src/cors.rs).
[^src-vertebra-http-src-compression-rs]: `src/rs/vertebra-http/src/compression.rs` — Rust implementation module (src/rs/vertebra-http/src/compression.rs).
[^src-vertebra-http-src-assets-rs]: `src/rs/vertebra-http/src/assets.rs` — Rust implementation module (src/rs/vertebra-http/src/assets.rs).
[^src-vertebra-http-src-h2c-rs]: `src/rs/vertebra-http/src/h2c.rs` — Rust implementation module (src/rs/vertebra-http/src/h2c.rs).
[^src-vertebra-http-src-http3-rs]: `src/rs/vertebra-http/src/http3.rs` — Rust implementation module (src/rs/vertebra-http/src/http3.rs).
[^src-vertebra-http-src-tls-rs]: `src/rs/vertebra-http/src/tls.rs` — Rust implementation module (src/rs/vertebra-http/src/tls.rs).
[^src-vertebra-http-src-health-rs]: `src/rs/vertebra-http/src/health.rs` — Rust implementation module (src/rs/vertebra-http/src/health.rs).
[^src-vertebra-http-src-hmr-rs]: `src/rs/vertebra-http/src/hmr.rs` — Rust implementation module (src/rs/vertebra-http/src/hmr.rs).
[^src-vertebra-http-src-payload-rs]: `src/rs/vertebra-http/src/payload.rs` — Rust implementation module (src/rs/vertebra-http/src/payload.rs).
[^src-vertebra-http-src-multipart-rs]: `src/rs/vertebra-http/src/multipart.rs` — Rust implementation module (src/rs/vertebra-http/src/multipart.rs).
[^src-vertebra-http-src-access-log-rs]: `src/rs/vertebra-http/src/access_log.rs` — Rust implementation module (src/rs/vertebra-http/src/access_log.rs).
[^src-vertebra-http-src-profile-rs]: `src/rs/vertebra-http/src/profile.rs` — Rust implementation module (src/rs/vertebra-http/src/profile.rs).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
[^src-vertebra-http-src-grpc-rs]: `src/rs/vertebra-http/src/grpc.rs` — Rust implementation module (src/rs/vertebra-http/src/grpc.rs).
[^src-vertebra-http-src-sse-rs]: `src/rs/vertebra-http/src/sse.rs` — Rust implementation module (src/rs/vertebra-http/src/sse.rs).
[^src-vertebra-http-src-ws-mount-rs]: `src/rs/vertebra-http/src/ws_mount.rs` — Rust implementation module (src/rs/vertebra-http/src/ws_mount.rs).
[^src-vertebra-http-src-pubsub-rs]: `src/rs/vertebra-http/src/pubsub.rs` — Rust implementation module (src/rs/vertebra-http/src/pubsub.rs).
[^src-vertebra-http-src-dispatcher-direct-rs]: `src/rs/vertebra-http/src/dispatcher/direct.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/direct.rs).
[^src-vertebra-http-src-webtransport-rs]: `src/rs/vertebra-http/src/webtransport.rs` — Rust implementation module (src/rs/vertebra-http/src/webtransport.rs).
[^src-vertebra-core-src-logger-rs]: `src/rs/vertebra-core/src/logger.rs` — Rust implementation module (src/rs/vertebra-core/src/logger.rs).
