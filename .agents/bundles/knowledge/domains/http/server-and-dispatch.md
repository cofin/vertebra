---
type: Guide
title: "Rust HTTP Ingress, Batching & Dispatch Domain"
description: "Canonical vertebra-http types grouped by module, with what each owns and whether the server binary uses it."
status: stable
tags:
  - domains
  - http
  - dispatch
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-learn
    at: "2026-11-13T16:02:00Z"
    commit: 05d88d00
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-dispatcher-streams-rs
    resource: src/rs/vertebra-http/src/dispatcher/streams.rs
    title: Bounded response stream registry and worker lifecycle cleanup
  - id: src-vertebra-http-src-lib-rs
    resource: src/rs/vertebra-http/src/lib.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/lib.rs)"
  - id: src-vertebra-http-src-config-rs
    resource: src/rs/vertebra-http/src/config.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/config.rs)"
  - id: src-vertebra-http-src-server-rs
    resource: src/rs/vertebra-http/src/server.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/server.rs)"
  - id: src-vertebra-http-src-response-rs
    resource: src/rs/vertebra-http/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/response.rs)"
  - id: src-vertebra-http-src-routing-rs
    resource: src/rs/vertebra-http/src/routing.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/routing.rs)"
  - id: src-vertebra-http-src-cors-rs
    resource: src/rs/vertebra-http/src/cors.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/cors.rs)"
  - id: src-vertebra-http-src-compression-rs
    resource: src/rs/vertebra-http/src/compression.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/compression.rs)"
  - id: src-vertebra-http-src-tls-rs
    resource: src/rs/vertebra-http/src/tls.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/tls.rs)"
  - id: src-vertebra-http-src-batcher-rs
    resource: src/rs/vertebra-http/src/batcher.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/batcher.rs)"
  - id: src-vertebra-http-src-dispatcher-mod-rs
    resource: src/rs/vertebra-http/src/dispatcher/mod.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/mod.rs)"
  - id: src-vertebra-http-src-dispatcher-bridge-rs
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/bridge.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs)"
  - id: src-vertebra-http-src-dispatcher-direct-rs
    resource: src/rs/vertebra-http/src/dispatcher/direct.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/direct.rs)"
  - id: src-vertebra-http-src-dispatcher-correlation-rs
    resource: src/rs/vertebra-http/src/dispatcher/correlation.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/correlation.rs)"
  - id: src-vertebra-http-src-dispatcher-shm-reader-rs
    resource: src/rs/vertebra-http/src/dispatcher/shm_reader.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs)"
  - id: src-vertebra-http-src-backpressure-rs
    resource: src/rs/vertebra-http/src/backpressure.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/backpressure.rs)"
  - id: src-vertebra-http-src-workers-factory-rs
    resource: src/rs/vertebra-http/src/workers/factory.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/workers/factory.rs)"
  - id: src-vertebra-http-src-assets-rs
    resource: src/rs/vertebra-http/src/assets.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/assets.rs)"
---

# Rust HTTP Ingress, Batching & Dispatch Domain

<!-- truth: start -->
- `vertebra-http` packages `ServerConfig`, `ServerState`, `Router` (`matchit 0.9`), `RequestBatcher`, `Dispatcher` / `ShmDispatcher`, and `BackpressureController` as both a Rust library and the `vertebra-server` binary.
- Ingress offloads (CORS, compression, static mounts, OpenAPI cache, health, HMR) execute natively in Rust before Python worker dispatch.
- The native bridge and SHM reader route progressive response frames through bounded `ResponseStreamRegistry` channels; Python submission and mounted ASGI/RSGI producers remain buffered until their separate cutover [^src-vertebra-http-src-dispatcher-streams-rs].
<!-- truth: end -->

[`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs)[^src-vertebra-http-src-lib-rs] is both a library and the `vertebra-server` binary. The crate root re-exports `Batch`, `BatchLimit`, `PendingRequest`, `RequestBatcher`, `CorrelationId`, `InFlightRequests`, `HandlerId`, `RouteTable`, `Router`, and `RouterError`. Mechanics are in [HTTP Ingress](../../architecture/http/ingress-and-routing.md) and [Request Batching, Dispatch & Backpressure](../../architecture/http/batching-dispatch-and-backpressure.md); protocol and offload modules are listed in [Protocol and Offload Modules](../../architecture/http/protocol-and-offload-modules.md).

## Configuration, server, offloads, and responses

| Symbol | Module | Responsibility |
|---|---|---|
| [`ServerConfig`](../../../../../src/rs/vertebra-http/src/config.rs)[^src-vertebra-http-src-config-rs] / `Topology` (`Embedded`, `Shm`) / `WorkerConfig` / `TlsConfig` / `StaticMountConfig` / `OpenApiCacheConfig` | `vertebra_http::config` | Environment-driven configuration (`ServerConfig::from_env`, `apply_offload_json`). |
| [`ServerState`](../../../../../src/rs/vertebra-http/src/server.rs)[^src-vertebra-http-src-server-rs] / `StaticAssetMount` / `CompiledOpenApiCache` / `bind_listener` / `run_with_listener` / `run_with_collector` | `vertebra_http::server` | `SO_REUSEADDR` / `SO_REUSEPORT` listener binding (`listen(2048)`), `TCP_NODELAY`, zero-peek tuned `http1::Builder` (or opt-in `VERTEBRA_H2C=1` `AutoBuilder`), optional TLS, cached health/CORS/compression/static/OpenAPI state, and graceful drain. |
| [`handle`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs] / `ResponseBody` / `HttpResponse` / `FrameReceiver` / `full_body` / `stream_body` | `vertebra_http::response` (`vertebra_http::body`) | Per-request pipeline (`HMR` -> health -> profile -> discovery -> CORS preflight -> OpenAPI cache -> static mounts -> admission -> route lookup -> bodiless `size_hint().exact() == Some(0)` fast-path / `collect_bounded_body` / multipart / gRPC -> compact header encode -> batcher -> `build_response_with_offloads` -> circuit breaker -> access log). |
| [`CorsOffloadConfig`](../../../../../src/rs/vertebra-http/src/cors.rs)[^src-vertebra-http-src-cors-rs] / `CompiledCorsOffload` / `append_vary_token` | `vertebra_http::cors` | Zero-GIL CORS preflight and response header injection with multi-token `Vary` preservation. |
| [`CompressionOffloadConfig`](../../../../../src/rs/vertebra-http/src/compression.rs)[^src-vertebra-http-src-compression-rs] / `CompressionBackend` | `vertebra_http::compression` | Zero-GIL `Zstd`/`Brotli`/`Gzip` response compression skipping incompressible `Content-Type`s and preserving `Vary`. |
| [`TlsAcceptor`](../../../../../src/rs/vertebra-http/src/tls.rs)[^src-vertebra-http-src-tls-rs] / `MtlsConfig` / `ClientAuthMode` / `ClientIdentity` | `vertebra_http::tls` (feature `tls`) | PEM loading, optional mTLS via `WebPkiClientVerifier`, ALPN `h2` and `http/1.1`, `peer_identity`, `rustls_config`. |

## Routing

| Symbol | Module | Responsibility |
|---|---|---|
| [`HandlerId`](../../../../../src/rs/vertebra-http/src/routing.rs)[^src-vertebra-http-src-routing-rs] (`u64`) / `MOUNT_HANDLER_BASE` / `handler_id` / `RouteEntry` / `RouteTable` | `vertebra_http::routing` | FNV-1a handler IDs shared with Python, `MOUNT_HANDLER_BASE \| mount_idx` across `GET`..`HEAD`/`OPTIONS`, and startup builder (`insert`, `insert_with_id`, `insert_mount_prefix_indexed`, `into_entries`). |
| [`Router`](../../../../../src/rs/vertebra-http/src/routing.rs) / `RouteMatch` / `RouterError` (`NotFound`, `MethodNotAllowed`) | `vertebra_http::routing` | One `matchit 0.9` tree per method via `vertebra_core::router::normalize_typed_path`; zero-alloc `lookup_method` and `allowed_methods`. |

## Batching, dispatch, and correlation

| Symbol | Module | Responsibility |
|---|---|---|
| [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/batcher.rs)[^src-vertebra-http-src-batcher-rs] / `BatchLimit` / `PendingRequest` / `PendingResponse` / `method_tag` / `http_version_tag` | `vertebra_http::batcher` | Double-buffered opportunistic batcher (`spawn_pool`, `with_shard`; `1..8` shards, capacity `16,384`, `BatchLimit::default()` = `64`) draining ready requests and gating `yield_now().await` on `dispatcher.in_flight() > 0 \|\| !rx.is_empty()` into `Dispatcher::dispatch_requests_from_shard`. |
| [`Dispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs)[^src-vertebra-http-src-dispatcher-mod-rs] / `DispatchBackend` (`Direct`, `Bridge`, `Shm`) / `BridgeCorrelation` | `vertebra_http::dispatcher` | Routes batches by backend; `BridgeCorrelation` uses 8-shard `InFlightRequests`, zero-copy `BatchBridge::submit_inprocess_batch`, separate unary `ResponseSink` and bounded `StreamSink` completion, `register_pending` allocation with `503` overflow, and `spawn_bridge_sweeper`. |
| [`ShmDispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs)[^src-vertebra-http-src-dispatcher-shm-rs] | `vertebra_http::dispatcher::shm` | Lock-free 1:1 shard-sticky worker routing (`home = shard_idx % worker_count`, MRU warm-worker affinity at `prev_active == 0`, least-loaded spillover when unhealthy/full/`in_flight >= 64`), per-worker `healthy` and `in_flight` state, direct `try_send_with` / `send_with` slot serialization (`VQOV` overflow spill for `> 64 KiB`), Dekker `notify_request_if_sleeping`, and 8 `Mutex<InFlightRequests>` shards. |
| [`InFlightRequests`](../../../../../src/rs/vertebra-http/src/dispatcher/correlation.rs)[^src-vertebra-http-src-dispatcher-correlation-rs] / `CorrelationId` | `vertebra_http::dispatcher::correlation` | Generation-tagged slab: `allocate_batch`, `insert`, `complete`, `complete_many`, `sweep_expired`, `sweep_for_worker`. |
| [`spawn_response_readers`](../../../../../src/rs/vertebra-http/src/dispatcher/shm_reader.rs)[^src-vertebra-http-src-dispatcher-shm-reader-rs] | `vertebra_http::dispatcher::shm_reader` | Per-worker `AsyncFd` reader using flag-aware `drain_frames_with`, owned stream frames before awaited delivery, unary completion batching, shutdown-aware backpressure, and Dekker sleep handshakes. |
| [`DirectWorkerPool`](../../../../../src/rs/vertebra-http/src/dispatcher/direct.rs)[^src-vertebra-http-src-dispatcher-direct-rs] / `AdaptiveWorkerConfig` / `run_adaptive_worker` | `vertebra_http::dispatcher::direct` | Consumer for Direct mode. |

## Native response stream lifecycle

[`ResponseStreamRegistry`](../../../../../src/rs/vertebra-http/src/dispatcher/streams.rs)[^src-vertebra-http-src-dispatcher-streams-rs] handles first frames, ordered data, trailers, final/error frames, and disconnected clients. Its default capacity is 64 frames per response. The embedded sink calls `deliver_blocking` from a producer outside Tokio; the SHM reader calls `Dispatcher::deliver_stream_frame(...).await`, which forwards to `ShmDispatcher::deliver_stream_frame` and the same registry implementation. No send drops a frame merely because the queue is full.

First frames complete their correlation as `PendingResponse::streaming`, removing the pending-request timeout entry. Final or error frames close the channel; periodic dispatcher sweeps reclaim dropped receivers. `set_worker`, `close_worker`, and `sweep_closed` retain SHM worker ownership and cancel blocked async delivery when the worker exits. Unary `VRES` and overflow `VROV` responses retain their completion batching path [^src-vertebra-http-src-dispatcher-streams-rs] [^src-vertebra-http-src-dispatcher-shm-reader-rs].

Python workers emit `ResponseStreamFrame` chunks through `submit_stream_frame` / `max_stream_frame_payload`, flushing mounted ASGI (`AsgiProtocol`, `AsgiSend`), RSGI (`RsgiHttpProtocol`), and native `App` (`_dispatch.py`, `StreamingStorageResponse`, `ArrowCursor`) responses progressively across both embedded `BatchBridge` and `Shm` topologies.

## Admission and worker processes

| Symbol | Module | Responsibility |
|---|---|---|
| [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs)[^src-vertebra-http-src-backpressure-rs] / `BackpressureConfig` / `CircuitBreakerConfig` / `RejectionReason` / `StreamGuard` / `BackpressureMetricsSnapshot` | `vertebra_http::backpressure` | Lock-free atomic admission permits with `SeqCst` wakeup barriers, priority-route bypass, and 16-bucket atomic sliding-window circuit breaker (`503` on saturation/timeout, `429` on open circuit). |
| [`EmbeddedSupervisor`](../../../../../src/rs/vertebra-http/src/workers/factory.rs)[^src-vertebra-http-src-workers-factory-rs] | `vertebra_http::workers::factory` | Multi-process `SO_REUSEPORT` supervisor for `Topology::Embedded` (`workers > 1`), pre-allocating shared SHM broker FDs, computing CPU-clamped per-worker `(tokio_threads, py_threads)`, forwarding signals, and respawning crashed children. |
| [`RingFactory`](../../../../../src/rs/vertebra-http/src/workers/factory.rs) / `Supervisor` / `WorkerLifecycle` / `cleanup_stale_shm` / `install_panic_cleanup_hook` | `vertebra_http::workers::factory` | Per-worker `RingPair` factory, SHM worker spawner (`python -m vertebra.supervisor._worker`), crash-backoff supervisor, and `/dev/shm` cleanup helpers. |
| [`AssetManager`](../../../../../src/rs/vertebra-http/src/assets.rs)[^src-vertebra-http-src-assets-rs] / `StaticAssetServeConfig` / `ManifestEntry` | `vertebra_http::assets` | Static file serving (sidecar compression, `ETag`/`304`, `Range`/`206`, SPA fallback) and Vite manifest resolver. |

Provenance sources: [^src-vertebra-http-src-dispatcher-bridge-rs].

[^src-vertebra-http-src-lib-rs]: `src/rs/vertebra-http/src/lib.rs` — Rust implementation module (src/rs/vertebra-http/src/lib.rs).
[^src-vertebra-http-src-config-rs]: `src/rs/vertebra-http/src/config.rs` — Rust implementation module (src/rs/vertebra-http/src/config.rs).
[^src-vertebra-http-src-server-rs]: `src/rs/vertebra-http/src/server.rs` — Rust implementation module (src/rs/vertebra-http/src/server.rs).
[^src-vertebra-http-src-response-rs]: `src/rs/vertebra-http/src/response.rs` — Rust implementation module (src/rs/vertebra-http/src/response.rs).
[^src-vertebra-http-src-routing-rs]: `src/rs/vertebra-http/src/routing.rs` — Rust implementation module (src/rs/vertebra-http/src/routing.rs).
[^src-vertebra-http-src-cors-rs]: `src/rs/vertebra-http/src/cors.rs` — Rust implementation module (src/rs/vertebra-http/src/cors.rs).
[^src-vertebra-http-src-compression-rs]: `src/rs/vertebra-http/src/compression.rs` — Rust implementation module (src/rs/vertebra-http/src/compression.rs).
[^src-vertebra-http-src-tls-rs]: `src/rs/vertebra-http/src/tls.rs` — Rust implementation module (src/rs/vertebra-http/src/tls.rs).
[^src-vertebra-http-src-batcher-rs]: `src/rs/vertebra-http/src/batcher.rs` — Rust implementation module (src/rs/vertebra-http/src/batcher.rs).
[^src-vertebra-http-src-dispatcher-mod-rs]: `src/rs/vertebra-http/src/dispatcher/mod.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/mod.rs).
[^src-vertebra-http-src-dispatcher-bridge-rs]: `src/rs/vertebra-http/src/dispatcher/bridge.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/bridge.rs).
[^src-vertebra-http-src-dispatcher-shm-rs]: `src/rs/vertebra-http/src/dispatcher/shm.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm.rs).
[^src-vertebra-http-src-dispatcher-direct-rs]: `src/rs/vertebra-http/src/dispatcher/direct.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/direct.rs).
[^src-vertebra-http-src-dispatcher-correlation-rs]: `src/rs/vertebra-http/src/dispatcher/correlation.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/correlation.rs).
[^src-vertebra-http-src-dispatcher-shm-reader-rs]: `src/rs/vertebra-http/src/dispatcher/shm_reader.rs` — Rust implementation module (src/rs/vertebra-http/src/dispatcher/shm_reader.rs).
[^src-vertebra-http-src-backpressure-rs]: `src/rs/vertebra-http/src/backpressure.rs` — Rust implementation module (src/rs/vertebra-http/src/backpressure.rs).
[^src-vertebra-http-src-workers-factory-rs]: `src/rs/vertebra-http/src/workers/factory.rs` — Rust implementation module (src/rs/vertebra-http/src/workers/factory.rs).
[^src-vertebra-http-src-assets-rs]: `src/rs/vertebra-http/src/assets.rs` — Rust implementation module (src/rs/vertebra-http/src/assets.rs).

[^src-vertebra-http-src-dispatcher-streams-rs]: `src/rs/vertebra-http/src/dispatcher/streams.rs` — Bounded native response delivery and worker lifecycle cleanup.
