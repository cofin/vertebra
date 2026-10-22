---
type: Guide
title: "Rust HTTP Ingress, Batching & Dispatch Domain"
description: "Canonical vertebra-http types grouped by module, with what each owns and whether the server binary uses it."
status: stable
tags: [domains, http, dispatch, rust]
sources:
  - resource: src/rs/vertebra-http/src/lib.rs
  - resource: src/rs/vertebra-http/src/config.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/router.rs
  - resource: src/rs/vertebra-http/src/route_table.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/collector.rs
  - resource: src/rs/vertebra-http/src/policy.rs
  - resource: src/rs/vertebra-http/src/dispatcher/mod.rs
  - resource: src/rs/vertebra-http/src/dispatcher/shm.rs
  - resource: src/rs/vertebra-http/src/direct_worker.rs
  - resource: src/rs/vertebra-http/src/correlation.rs
  - resource: src/rs/vertebra-http/src/response_reader.rs
  - resource: src/rs/vertebra-http/src/backpressure.rs
  - resource: src/rs/vertebra-http/src/ring_factory.rs
  - resource: src/rs/vertebra-http/src/assets.rs
---

# Rust HTTP Ingress, Batching & Dispatch Domain

[`vertebra-http`](../../../../../src/rs/vertebra-http/src/lib.rs) is both a library and the `vertebra-server` binary. The crate root re-exports `RequestBatcher`, `InFlightRequests`, `BatchPolicy`, `RouteTable`, and `Router`. Mechanics are in [HTTP Ingress](../../architecture/http/ingress-and-routing.md) and [Request Batching, Dispatch & Backpressure](../../architecture/http/batching-dispatch-and-backpressure.md); modules not called by the server are listed in [Protocol and Offload Modules](../../architecture/http/protocol-and-offload-modules.md).

## Configuration, server, and responses

| Symbol | Module | Responsibility |
|---|---|---|
| [`ServerConfig`](../../../../../src/rs/vertebra-http/src/config.rs) / `Topology` (`Embedded`, `Shm`) / `WorkerConfig` / `TlsConfig` | `vertebra_http::config` | Environment-driven configuration (`ServerConfig::from_env`). Several fields are declared but never read. |
| [`ServerState`](../../../../../src/rs/vertebra-http/src/server.rs) / `run_with_listener` / `run_with_collector` | `vertebra_http::server` | Accept loop, `TCP_NODELAY`, `AutoBuilder` HTTP/1.1 + HTTP/2, optional TLS, round-robin batcher selection. Stops accepting when the shutdown future resolves. |
| [`handle`](../../../../../src/rs/vertebra-http/src/response.rs) / `ResponseBody` / `HttpResponse` / `StreamingBody` / `ReceiverFrameStream` | `vertebra_http::response` | Per-request pipeline and the `Either<Full<Bytes>, StreamingBody>` body (`full_body`, `stream_body`, `build_response`). |
| [`TlsAcceptor`](../../../../../src/rs/vertebra-http/src/tls.rs) / `MtlsConfig` / `ClientAuthMode` / `ClientIdentity` | `vertebra_http::tls` (feature `tls`) | PEM loading, optional mTLS via `WebPkiClientVerifier`, ALPN `h2` and `http/1.1`, `peer_identity`, `rustls_config` for QUIC reuse. There is no self-signed certificate generator; `rcgen` is a dev-dependency. |

## Routing

| Symbol | Module | Responsibility |
|---|---|---|
| [`HandlerId`](../../../../../src/rs/vertebra-http/src/route_table.rs) (`u64`) / `handler_id` / `RouteEntry` / `RouteTable` | `vertebra_http::route_table` | FNV-1a handler IDs shared with Python, and the startup builder (`insert`, `into_entries`). |
| [`Router`](../../../../../src/rs/vertebra-http/src/router.rs) / `RouteMatch` / `RouterError` (`NotFound`, `MethodNotAllowed`) | `vertebra_http::router` | One `matchit 0.9` tree per method, built by `from_entries` through `vertebra_core::router::normalize_typed_path` (`{id:int}`, `:id` → `{id}`; `*rest`, `{rest:path}` → `{*rest}`). `lookup` and `allowed_methods`. |

## Batching, dispatch, and correlation

| Symbol | Module | Responsibility |
|---|---|---|
| [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/collector.rs) / `PendingRequest` / `PendingResponse` / `Batch` / `method_tag` | `vertebra_http::collector` | Per-task batcher (`spawn_pool`; 8 tasks and capacity 16,384 in `main.rs`) that drains queued requests up to `BatchPolicy::batch_size()` and calls `Dispatcher::dispatch`. |
| [`BatchPolicy`](../../../../../src/rs/vertebra-http/src/policy.rs) | `vertebra_http::policy` | `Fixed { size, timeout }` or `Adaptive { preferred_sizes, max_queue_delay, ema_arrival_rate, ema_alpha, min_size, max_size }`. Only `batch_size()` and the EMA update are used. |
| [`Dispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/mod.rs) / `DispatchMode` (`Direct`, `Bridge`, `SharedMemory`) / `DirectBatch` / `DirectRequest` | `vertebra_http::dispatcher` | Routes batches by mode; `complete`, `complete_many`, `backpressure`. |
| [`ShmDispatcher`](../../../../../src/rs/vertebra-http/src/dispatcher/shm.rs) | `vertebra_http::dispatcher` | Round-robin worker selection, `ShmBatchWriter` slot serialization, 8 `Mutex<InFlightRequests>` shards with a 1 s / 30 s sweep, and an uncalled `handle_raw_response_slot` stream path. |
| [`InFlightRequests`](../../../../../src/rs/vertebra-http/src/correlation.rs) | `vertebra_http::correlation` | Generation-tagged slab: `insert`, `complete`, `sweep_expired`, `sweep_for_worker`, `in_flight`, `capacity`. |
| [`run_response_reader`](../../../../../src/rs/vertebra-http/src/response_reader.rs) / `PendingRequests` / `PendingResponder` (`Unary`, `Stream`) | `vertebra_http::response_reader` | Per-worker `AsyncFd` reader for SHM responses. `PendingRequests` is a `DashMap` used only by the uncalled stream path and tests. |
| [`DirectWorkerPool`](../../../../../src/rs/vertebra-http/src/direct_worker.rs) / `AdaptiveWorkerConfig` / `run_adaptive_worker` | `vertebra_http::direct_worker` | Consumer for Direct mode. No production caller. |

## Admission and worker processes

| Symbol | Module | Responsibility |
|---|---|---|
| [`BackpressureController`](../../../../../src/rs/vertebra-http/src/backpressure.rs) / `BackpressureConfig` / `CircuitBreakerConfig` / `RejectionReason` / `StreamGuard` / `BackpressureMetricsSnapshot` | `vertebra_http::backpressure` | Unary permits with bounded queueing, priority-route semaphore, sliding-window breaker (`Closed`, `Open`, `HalfOpen`), and an unused stream-lease API. 503 for saturation, queue timeout, or stream limit; 429 for an open circuit. |
| [`RingFactory`](../../../../../src/rs/vertebra-http/src/ring_factory.rs) | `vertebra_http::ring_factory` | Creates one inheritable `RingPair` per worker, `spawn_worker` / `spawn_all` run `python -m vertebra.supervisor._worker`, and `shutdown_workers` sends SIGTERM then SIGKILL. |
| `WorkerLifecycle` / `WorkerState` / `cleanup_stale_shm` / `install_panic_cleanup_hook` | `vertebra_http::ring_factory` | Crash backoff, circuit tripping, and SHM cleanup helpers. No production caller, so crashed workers are not respawned. |
| [`AssetManager`](../../../../../src/rs/vertebra-http/src/assets.rs) / `StaticAssetServeConfig` / `ManifestEntry` | `vertebra_http::assets` | Static file and Vite manifest helper. Not on the request path. |
