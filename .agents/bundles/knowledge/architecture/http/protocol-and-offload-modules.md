---
type: Guide
title: "Protocol and Offload Modules in vertebra-http"
description: "Inventory of vertebra-http protocol, realtime, and offload modules with their public symbols and whether response::handle or main.rs calls them."
status: stable
tags: [architecture, http, protocols, rust]
sources:
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/h2c.rs
  - resource: src/rs/vertebra-http/src/http3.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/health.rs
  - resource: src/rs/vertebra-http/src/hmr.rs
  - resource: src/rs/vertebra-http/src/assets.rs
  - resource: src/rs/vertebra-http/src/payload.rs
  - resource: src/rs/vertebra-http/src/multipart.rs
  - resource: src/rs/vertebra-http/src/access_log.rs
  - resource: src/rs/vertebra-http/src/profile.rs
  - resource: src/rs/vertebra-http/src/sse.rs
  - resource: src/rs/vertebra-http/src/ws_mount.rs
  - resource: src/rs/vertebra-http/src/pubsub.rs
  - resource: src/rs/vertebra-http/src/direct_worker.rs
---

# Protocol and Offload Modules in vertebra-http

[`lib.rs`](../../../../../src/rs/vertebra-http/src/lib.rs) exports every module as `pub mod`. Only a few are reached from `main.rs`, `server.rs`, or [`response::handle`](../../../../../src/rs/vertebra-http/src/response.rs). The rest are library code exercised only by `src/rs/vertebra-http/tests/` and in-module tests. `vertebra-py` does not depend on `vertebra-http`, so Python cannot reach them either. Check this table before assuming a feature is live in `vertebra-server`.

## Wired into the server

| Module | Symbols | Use on the request path |
|---|---|---|
| [`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs) | `HmrState`, `HmrEvent`, `HmrEventKind`, `HMR_PATH` (`/__vertebra/hmr`), `HMR_SIGNAL_PATH`, `HMR_EVENT_NAME` (`vertebra:types-changed`), `is_hmr_path`, `handle` | Dev-only WebSocket and signal endpoint, first step of `handle` |
| [`health.rs`](../../../../../src/rs/vertebra-http/src/health.rs) | `HealthEndpoint`, `HealthState` (`Healthy`, `Draining`, `ShuttingDown`) | `try_intercept` on a per-request default instance |
| [`h2c.rs`](../../../../../src/rs/vertebra-http/src/h2c.rs) | `H2C_PREFACE`, `CleartextProtocol`, `detect_cleartext_protocol` | Called in `serve_plain`; the result is discarded |
| [`http3.rs`](../../../../../src/rs/vertebra-http/src/http3.rs) | `Http3Config` (`enabled: false`, `alt_svc_max_age: 86_400`, `max_concurrent_bidi_streams: 256`, `keep_alive_interval_ms: 15_000`), `inject_alt_svc_header` | Adds `Alt-Svc` when `http3.enabled`; `from_env` never enables it |
| [`tls.rs`](../../../../../src/rs/vertebra-http/src/tls.rs) | `TlsAcceptor` (`from_config`, `accept`, `rustls_config`, `peer_identity`), `MtlsConfig`, `ClientAuthMode` | Acceptor used by `serve_tls` (feature `tls`) |

## Library only (no production caller)

| Module | Symbols | Notes |
|---|---|---|
| `h2c.rs` (rest) | `parse_h2c_upgrade_headers`, `build_h2c_switching_protocols_response`, `decode_base64url`, `PrependStream`, `H2cUpgradeParams` | `Upgrade: h2c` is not implemented in the accept loop |
| `http3.rs` (rest) | `build_quic_server_config`, `serve_http3_endpoint`, `handle_h3_request`, `H3ChannelBody` | Feature `http3`; no QUIC listener is started |
| `tls.rs` (rest) | `IpCidr`, `TrustedProxyConfig`, `ClientIdentity`, `ProxyAuthMetadata`, `extract_client_identity_from_der`, `normalize_trusted_proxy_headers` | Identity extraction is not attached to requests |
| [`assets.rs`](../../../../../src/rs/vertebra-http/src/assets.rs) | `AssetManager`, `StaticAssetServeConfig`, `ManifestEntry`, `Manifest` | Vite manifest `resolve_entry`, `serve_static_request` with pre-compressed sidecars, weak ETag and 304, Range and 206, SPA fallback, `..` rejection, extension-based MIME, immutable caching for fingerprinted names |
| [`payload.rs`](../../../../../src/rs/vertebra-http/src/payload.rs) | `PayloadLimits`, `PayloadError`, `decode_request_payload` | Bounded request decompression |
| [`multipart.rs`](../../../../../src/rs/vertebra-http/src/multipart.rs) | `MultipartLimits`, `MultipartPart`, `ParsedMultipart`, `parse_multipart_body` | `multer`-based parser |
| [`access_log.rs`](../../../../../src/rs/vertebra-http/src/access_log.rs) | `AccessLogger`, `AccessLogFormat`, `AccessLogEntry`, `format_utc_clf_timestamp` | Buffers formatted lines in memory (`drain_buffered`) |
| [`profile.rs`](../../../../../src/rs/vertebra-http/src/profile.rs) | `ProfileConfig`, `ProfileEndpoint` | Token-guarded endpoint that returns process metadata, not a profile |
| [`sse.rs`](../../../../../src/rs/vertebra-http/src/sse.rs) | `SseEvent`, `SseTopicBuffer` (`replay_since`), `SseHub`, `SSE_KEEPALIVE_PING`, `DEFAULT_TOPIC_CHANNEL_CAPACITY` (256) | Used by `pubsub.rs` only |
| [`ws_mount.rs`](../../../../../src/rs/vertebra-http/src/ws_mount.rs) | `WsConfig`, `WebSocketHandshakeDecision`, `WsConnectionRegistry`, `validate_origin`, `negotiate_subprotocol`, `build_handshake_response`, `run_ws_connection` | Only `is_websocket_upgrade` is used, by `hmr.rs` |
| [`pubsub.rs`](../../../../../src/rs/vertebra-http/src/pubsub.rs) | `PubSubBroker`, `PubSubEnvelope`, `ShmBroadcastRing`, `topic_matches_pattern`, `MAX_IDEMPOTENCY_KEYS` (1024), `DEFAULT_BROADCAST_CAPACITY` (1024) | `ShmBroadcastRing` is a heap `Vec<Mutex<Vec<u8>>>`, not shared memory |
| [`direct_worker.rs`](../../../../../src/rs/vertebra-http/src/direct_worker.rs) | `DirectWorkerPool`, `AdaptiveWorkerConfig`, `run_adaptive_worker`, `BatchHandler` | Consumer for `DispatchMode::Direct`, which the binary never selects |

Modules covered elsewhere:

- `cors.rs` and `compression.rs` (Litestar offload targets): [Litestar Integration](../../domains/extensions/litestar.md).
- `discovery.rs` and `mcp_bridge.rs`: [Litestar Integration](../../domains/extensions/litestar.md).
- `grpc.rs`: [gRPC Services](../../domains/extensions/grpc.md).

## Rules for changes

- Wiring a library module means adding a call in `response::handle` (request interceptors before routing, response filters after `build_response`) and an env knob in `ServerConfig::from_env`. Benchmark the change; `handle` is the hot path.
- Do not describe a module in this list as active in user-facing docs until `rg 'crate::<module>' src/rs/vertebra-http/src` shows a caller outside tests.
