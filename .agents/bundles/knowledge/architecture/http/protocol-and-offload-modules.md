---
type: Guide
title: "Protocol and Offload Modules in vertebra-http"
description: "Inventory of vertebra-http protocol, realtime, and offload modules with their public symbols and whether response::handle or main.rs calls them."
status: stable
tags: [architecture, http, protocols, rust]
sources:
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/cors.rs
  - resource: src/rs/vertebra-http/src/compression.rs
  - resource: src/rs/vertebra-http/src/assets.rs
  - resource: src/rs/vertebra-http/src/h2c.rs
  - resource: src/rs/vertebra-http/src/http3.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/health.rs
  - resource: src/rs/vertebra-http/src/hmr.rs
  - resource: src/rs/vertebra-http/src/payload.rs
  - resource: src/rs/vertebra-http/src/multipart.rs
  - resource: src/rs/vertebra-http/src/access_log.rs
  - resource: src/rs/vertebra-http/src/profile.rs
  - resource: src/rs/vertebra-http/src/sse.rs
  - resource: src/rs/vertebra-http/src/ws_mount.rs
  - resource: src/rs/vertebra-http/src/pubsub.rs
  - resource: src/rs/vertebra-http/src/dispatcher/direct.rs
---

# Protocol and Offload Modules in vertebra-http

[`lib.rs`](../../../../../src/rs/vertebra-http/src/lib.rs) exports every module as `pub mod`. The modules in the first table below are wired directly into `main.rs`, `server.rs`, and [`response::handle`](../../../../../src/rs/vertebra-http/src/response.rs). The remaining modules are standalone library components exercised by `src/rs/vertebra-http/tests/` and unit tests (`vertebra-py` does not depend on `vertebra-http`).

## Wired into the server

| Module | Symbols | Use on the request path |
|---|---|---|
| [`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs) | `HmrState`, `HmrEvent`, `HmrEventKind`, `HMR_PATH` (`/__vertebra/hmr`), `HMR_SIGNAL_PATH`, `HMR_EVENT_NAME` (`vertebra:types-changed`), `is_hmr_path`, `handle` | Dev-only WebSocket and signal endpoint, first step of `response::handle` |
| [`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs) | `HealthEndpoint`, `HealthState` (`Healthy`, `Draining`, `ShuttingDown`) | Cached once on `ServerState::health_endpoint`; `try_intercept` answers `/healthz`, `/livez`, `/__vertebra/health`, `/readyz` |
| [`cors.rs`](../../../../../src/rs/vertebra-http/src/cors.rs) | `CorsOffloadConfig`, `CompiledCorsOffload` (`evaluate_preflight`, `apply_response_headers`), `append_vary_token` | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `LitestarPlugin`; intercepts `OPTIONS` preflights and attaches CORS + multi-token `Vary` response headers |
| [`compression.rs`](../../../../../src/rs/vertebra-http/src/compression.rs) | `CompressionBackend` (`Zstd`, `Brotli`, `Gzip`), `CompressionOffloadConfig` (`compress_if_eligible`), `append_vary_token` | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `LitestarPlugin`; negotiates `Accept-Encoding`, skips incompressible `Content-Type`s, compresses unary bodies >= `minimum_size`, and appends `Vary: Accept-Encoding` |
| [`assets.rs`](../../../../../src/rs/vertebra-http/src/assets.rs) & [`server.rs`](../../../../../src/rs/vertebra-http/src/server.rs) | `AssetManager`, `StaticAssetServeConfig`, `StaticAssetMount`, `CompiledOpenApiCache` | Configured via `VERTEBRA_OFFLOAD_CONFIG` / `LitestarPlugin`; serves pre-rendered OpenAPI JSON (`ETag`/`304`) and static directories (pre-compressed sidecars, `ETag`/`304`, `Range`/`206`, SPA fallback) before backpressure/Python |
| [`h2c.rs`](../../../../../src/rs/vertebra-http/src/h2c.rs) | `H2C_PREFACE`, `CleartextProtocol`, `detect_cleartext_protocol` | Gated behind `ServerConfig::enable_h2c_prior_knowledge` (`VERTEBRA_H2C=1`); skipped on the default zero-peek `http1::Builder` fastpath |
| [`http3.rs`](../../../../../src/rs/vertebra-http/src/http3.rs) | `Http3Config` (`enabled: false`, `alt_svc_max_age: 86_400`, `max_concurrent_bidi_streams: 256`, `keep_alive_interval_ms: 15_000`), `inject_alt_svc_header` | Appends `Alt-Svc` when `http3.enabled` is true |
| [`tls.rs`](../../../../../src/rs/vertebra-http/src/tls.rs) | `TlsAcceptor` (`from_config`, `accept`, `rustls_config`, `peer_identity`), `MtlsConfig`, `ClientAuthMode` | Acceptor used by `serve_tls` (feature `tls`) |

## Library only (no production caller in `response::handle`)

| Module | Symbols | Notes |
|---|---|---|
| `h2c.rs` (upgrade helpers) | `parse_h2c_upgrade_headers`, `build_h2c_switching_protocols_response`, `decode_base64url`, `PrependStream`, `H2cUpgradeParams` | RFC 9113 `Upgrade: h2c` parsing helpers |
| `http3.rs` (QUIC listener) | `build_quic_server_config`, `serve_http3_endpoint`, `handle_h3_request`, `H3ChannelBody` | Feature `http3`; standalone QUIC endpoint helpers |
| `tls.rs` (identity helpers) | `IpCidr`, `TrustedProxyConfig`, `ClientIdentity`, `ProxyAuthMetadata`, `extract_client_identity_from_der`, `normalize_trusted_proxy_headers` | Certificate CN and proxy XFCC/IAP header helpers |
| [`payload.rs`](../../../../../src/rs/vertebra-http/src/payload.rs) | `PayloadLimits`, `PayloadError`, `decode_request_payload` | Bounded request decompression |
| [`multipart.rs`](../../../../../src/rs/vertebra-http/src/multipart.rs) | `MultipartLimits`, `MultipartPart`, `ParsedMultipart`, `parse_multipart_body` | `multer`-based multipart parser |
| [`access_log.rs`](../../../../../src/rs/vertebra-http/src/access_log.rs) | `AccessLogger`, `AccessLogFormat`, `AccessLogEntry`, `format_utc_clf_timestamp` | In-memory formatted access log buffer (`drain_buffered`) |
| [`profile.rs`](../../../../../src/rs/vertebra-http/src/profile.rs) | `ProfileConfig`, `ProfileEndpoint` | Constant-time token-guarded runtime metadata endpoint |
| [`sse.rs`](../../../../../src/rs/vertebra-http/src/sse.rs) | `SseEvent`, `SseTopicBuffer` (`replay_since`), `SseHub`, `SSE_KEEPALIVE_PING`, `DEFAULT_TOPIC_CHANNEL_CAPACITY` (256) | Used by `pubsub.rs` |
| [`ws_mount.rs`](../../../../../src/rs/vertebra-http/src/ws_mount.rs) | `WsConfig`, `WebSocketHandshakeDecision`, `WsConnectionRegistry`, `validate_origin`, `negotiate_subprotocol`, `build_handshake_response`, `run_ws_connection` | `is_websocket_upgrade` is called by `hmr.rs` |
| [`pubsub.rs`](../../../../../src/rs/vertebra-http/src/pubsub.rs) | `PubSubBroker`, `PubSubEnvelope`, `TopicHub`, `ShmBroadcastRing`, `topic_matches_pattern`, `MAX_IDEMPOTENCY_KEYS` (1024), `DEFAULT_BROADCAST_CAPACITY` (1024) | Topic broadcast and idempotency primitives |
| [`dispatcher/direct.rs`](../../../../../src/rs/vertebra-http/src/dispatcher/direct.rs) | `DirectWorkerPool`, `AdaptiveWorkerConfig`, `run_adaptive_worker`, `BatchHandler` | Consumer for `DispatchBackend::Direct` |

Additional domain-specific modules:

- `discovery.rs`: [Litestar Integration](../../domains/extensions/litestar.md) and [MCP & A2A Protocol Harness](../../domains/agent/mcp-and-a2a-protocol-harness.md).
- `grpc.rs`: [gRPC Services](../../domains/extensions/grpc.md).

## Rules for changes

- Wiring a library module means caching compiled configuration on `ServerState` (`0` per-request overhead when unconfigured), invoking it in `response::handle` (request interceptors before admission/routing, response filters in `build_response_with_offloads`), and exposing configuration in `ServerConfig::from_env` or `apply_offload_json`.
- Verify live call sites with `rg 'crate::<module>' src/rs/vertebra-http/src` before documenting a module as active on the `vertebra-server` hot path.
