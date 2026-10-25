---
type: Guide
title: "HTTP Ingress, Request Pipeline & Native Routing"
description: "How vertebra-server accepts connections, orders the per-request pipeline in response::handle, matches routes in Rust, and reads its environment configuration."
status: stable
tags: [architecture, http, routing, tls, rust]
sources:
  - resource: src/rs/vertebra-http/src/main.rs
  - resource: src/rs/vertebra-http/src/server.rs
  - resource: src/rs/vertebra-http/src/response.rs
  - resource: src/rs/vertebra-http/src/config.rs
  - resource: src/rs/vertebra-http/src/routing.rs
  - resource: src/rs/vertebra-http/src/cors.rs
  - resource: src/rs/vertebra-http/src/compression.rs
  - resource: src/rs/vertebra-http/src/assets.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/hmr.rs
  - resource: src/rs/vertebra-http/src/health.rs
  - resource: src/rs/vertebra-core/src/router.rs
  - resource: src/py/vertebra/framework/_router.py
---

# HTTP Ingress, Request Pipeline & Native Routing

The `vertebra-server` binary ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs), package `vertebra-http`) terminates TCP or TLS, executes zero-GIL Rust offloads (health, CORS, OpenAPI schema cache, static assets, response compression), matches every request against a Rust route table, and hands matched requests to the batching pipeline described in [Request Batching, Dispatch & Backpressure](batching-dispatch-and-backpressure.md). Additional library modules in the crate are inventoried in [Protocol and Offload Modules](protocol-and-offload-modules.md).

## Startup (`main.rs`)

`vertebra-server` configures itself from `VERTEBRA_*` environment variables via [`ServerConfig::from_env`](../../../../../src/rs/vertebra-http/src/config.rs), with `VERTEBRA_APP` (default `app:app`) naming the Python application. In order, `main`:

1. Builds an explicit Tokio runtime via `build_tokio_runtime(worker_threads)`: `Builder::new_current_thread()` when `worker_threads <= 1` and `Builder::new_multi_thread().worker_threads(worker_threads)` when `> 1`.
2. When `Topology::Embedded` runs on Unix with `config.workers.count > 1` and `VERTEBRA_EMBEDDED_WORKER` is unset, starts [`EmbeddedSupervisor`](../../../../../src/rs/vertebra-http/src/workers/factory.rs) on a single-threaded Tokio runtime to pre-allocate shared SHM broker/pubsub FDs and supervise `W` `SO_REUSEPORT` child `vertebra-server` processes (`VERTEBRA_EMBEDDED_WORKER=1`, `VERTEBRA_REUSE_PORT=1`).
3. Registers the embedded `_vertebra` module with `append_to_inittab!(pymodule_init)` before the first `Python::attach` (feature `python`, on by default), and initializes `BatchBridge::init(256, 1_048_576, 16, config.thread_count.max(1))` strictly when `config.topology == Topology::Embedded`.
4. Imports `VERTEBRA_APP`, calls `extract_offload_json_from_app` (invoking `LitestarPlugin.ensure_finalized(app)` and reading `app.state["vertebra_offload_config_json"]` when `VERTEBRA_OFFLOAD_CONFIG` is unset), and builds a [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs) from `app.handler_index` plus `app.mounts`. Each mount prefix is registered via `insert_mount_prefix_indexed(prefix, mount_idx)` with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | mount_idx`) across `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD` (both exact prefix and `{prefix}/*rest`). A raw callable app is wrapped in `vertebra.framework._app.App` and mounted at `/`.
5. Selects a dispatcher:
   - `Topology::Shm` (Unix): creates a `RingFactory`, `ShmDispatcher::new(ring_pairs, 65536)` wrapped in `Dispatcher::from_shm` with `BackpressureConfig`, spawns `shm.spawn_sweeper`, `shm_reader::spawn_response_readers`, the Python worker processes, and `Supervisor::spawn`.
   - `Topology::Embedded`: creates `Dispatcher::new()` with `BackpressureConfig` (registering a direct `ResponseSink` on `BatchBridge` plus `spawn_bridge_sweeper`) and calls `spawn_python_workers`, launching `1` Python worker thread on GIL builds or `thread_count.min(available_cpus)` worker threads sharing `BatchBridge` on free-threaded Python `3.14t`/`3.15t`, each running `vertebra.server.worker.run_worker`.
6. Spawns `config.worker_threads.clamp(1, 8)` [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/batcher.rs) tasks (`BatchLimit::default()` of `64`, channel capacity `16,384`).
7. Runs `server::run_with_collector` until `SIGINT` or `SIGTERM`, cancels `CancellationToken`, joins embedded Python worker threads or drains SHM workers (`RingFactory::shutdown_workers`, 5 s grace before `SIGKILL`).

## Accept loop (`server.rs`)

[`run_with_listener`](../../../../../src/rs/vertebra-http/src/server.rs) (and `run_with_collector`, which binds via [`bind_listener`](../../../../../src/rs/vertebra-http/src/server.rs) using `TcpSocket` with `SO_REUSEADDR`, Unix `SO_REUSEPORT` when `config.reuse_port` is true, `TCP_NODELAY`, and `listen(2048)`) constructs a shared [`ServerState`](../../../../../src/rs/vertebra-http/src/server.rs) caching `Arc<Router>`, `Arc<Dispatcher>`, `Arc<ServerConfig>`, `collector_shards`, `HmrState`, `health_endpoint: HealthEndpoint`, `cors: Option<CompiledCorsOffload>`, `compression: Option<CompressionOffloadConfig>`, `static_mounts: Vec<StaticAssetMount>`, and `openapi_cache: Option<CompiledOpenApiCache>`.

- Every accepted stream gets `TCP_NODELAY`.
- When `enable_h2c_prior_knowledge` (`VERTEBRA_H2C`) is `false` (the default), `serve_plain` skips `MSG_PEEK` (`h2c::detect_cleartext_protocol`) and serves cleartext HTTP/1.1 directly through `hyper::server::conn::http1::Builder::new().keep_alive(true).pipeline_flush(false).max_buf_size(65_536).timer(TokioTimer::new())` with upgrades enabled. When `enable_h2c_prior_knowledge` is `true`, `serve_plain` runs `detect_cleartext_protocol` and `AutoBuilder` (`http1` + `http2` stream/connection windows from `ServerConfig`).
- When `ServerConfig::tls` is set (both `VERTEBRA_TLS_CERT` and `VERTEBRA_TLS_KEY`), [`TlsAcceptor::from_config`](../../../../../src/rs/vertebra-http/src/tls.rs) loads PEM certificates and advertises ALPN `h2` and `http/1.1` (with `WebPkiClientVerifier` when `MtlsConfig.mode` is `Optional` or `Required`).
- `ServerState::get_collector` short-circuits directly to `collector_shards[0]` when `len == 1` and selects round-robin via `Relaxed` `AtomicUsize` otherwise.

## Per-request pipeline (`response::handle`)

[`handle`](../../../../../src/rs/vertebra-http/src/response.rs) executes in order:

1. **Dev HMR**: when `dev_mode` (`VERTEBRA_DEV=1`) and `hmr::is_hmr_path(path)` matches `/__vertebra/hmr` or `/__vertebra/hmr/signal`, [`hmr::handle`](../../../../../src/rs/vertebra-http/src/hmr.rs) answers.
2. **Health probes**: `/__vertebra/health` returns `200` JSON with the live `BackpressureMetricsSnapshot` when healthy, and the cached `state.health_endpoint.try_intercept(path)` answers `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz` with zero per-request `HealthEndpoint` allocation.
3. **CORS `OPTIONS` preflight offload**: when `state.cors` is configured and an `OPTIONS` request carries `Origin` and `Access-Control-Request-Method`, `CompiledCorsOffload::evaluate_preflight` returns immediately without acquiring Python or backpressure permits.
4. **OpenAPI schema cache offload**: `GET`/`HEAD` requests matching `state.openapi_cache.schema_path` serve the pre-rendered OpenAPI payload (with `If-None-Match` -> `304 Not Modified`, `ETag`, `Cache-Control`, and optional CORS headers).
5. **Static asset mounts**: `GET`/`HEAD` requests matching `state.static_mounts` serve files directly via `AssetManager::serve_static_request` (pre-compressed sidecars, `ETag`/`304`, `Range`/`206`, SPA HTML fallback, custom headers, `Content-Disposition: attachment`, and CORS headers).
6. **Admission**: `BackpressureController::try_acquire_for_path` acquires an atomic in-flight permit (or returns `503`/`429` with `retry-after: 1`).
7. **Route lookup**: `Router::lookup_method` returns a matched `HandlerId` (`MOUNT_HANDLER_BASE | mount_idx` for mounted apps), `404 Not Found`, or `405 Method Not Allowed` with an `allow` header.
8. **Body & headers**: skips `BodyExt::collect` when `body.is_end_stream()` is already true, encodes `HeaderMap` in one pass into compact binary `Bytes` via `encode_header_map_compact` (`[name_len: u8][name][val_len: u16 LE][val]`), and sends `PendingRequest` to a `RequestBatcher` shard (`503` if shutting down).
9. **Response & post-filters**: awaits the `oneshot` reply, applies `build_response_with_offloads` (`CompiledCorsOffload::apply_response_headers` and `CompressionOffloadConfig::compress_if_eligible` with compressible `Content-Type` filtering and multi-token `Vary` preservation, streaming when `stream_rx` is set, or `500 worker error` on dropped channel), injects `Alt-Svc` when `http3.enabled`, and records lock-free circuit-breaker `record_failure` (`5xx`) or `record_success`.

## Routing (`routing.rs`)

- [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs) is a startup builder (`insert`, `insert_with_id`, `insert_mount_prefix_indexed`, `into_entries`). Dev reload restarts the worker/server process rather than hot-swapping `Router`.
- [`routing::handler_id`](../../../../../src/rs/vertebra-http/src/routing.rs) hashes uppercased method, `' '`, and path with `FnvHasher` via `std::hash::Hash`, matching [`vertebra.framework._router._make_handler_id`](../../../../../src/py/vertebra/framework/_router.py) byte-for-byte; mounted prefixes carry `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`).
- [`Router::from_entries`](../../../../../src/rs/vertebra-http/src/routing.rs) normalizes `:name`, `{name:type}`, `*name`, and `{name:path}` segments via [`vertebra_core::router::normalize_typed_path`](../../../../../src/rs/vertebra-core/src/router.rs) into per-method `matchit::Router<HandlerId>` trees (`lookup_method` avoids string allocation on standard HTTP verbs).

## Configuration (`config.rs`)

| Variable | Field | Default |
|---|---|---|
| `VERTEBRA_HOST`, `VERTEBRA_PORT` | `bind` | `127.0.0.1:8000` |
| `VERTEBRA_REUSE_PORT` | `reuse_port` (auto-enabled on Unix when `Embedded` and `workers.count > 1`) | `false` |
| `VERTEBRA_DEV` | `dev_mode` | `false` |
| `VERTEBRA_H2C` | `enable_h2c_prior_knowledge` | `false` |
| `VERTEBRA_OFFLOAD_CONFIG` | `cors`, `compression`, `static_mounts`, `openapi_cache` JSON | auto-extracted from `app.state` |
| `VERTEBRA_TLS_CERT`, `VERTEBRA_TLS_KEY` | `tls` | `None` |
| `VERTEBRA_TOPOLOGY` (`auto`, `embedded`, `shm`) | `topology` (`auto` resolves to `Embedded` on Unix) | `Embedded` |
| `VERTEBRA_WORKERS` | `workers.count` | `0` (`1` active worker) |
| `VERTEBRA_THREADS`, `VERTEBRA_TOKIO_THREADS` | `thread_count`, `worker_threads` (`VERTEBRA_THREADS` clamps `worker_threads` to `available_parallelism()`) | logical CPU count |
| `VERTEBRA_PYTHON_BIN`, `VERTEBRA_APP` | `workers.python_bin`, `workers.app_path` | `python3`, `app:app` |
| `VERTEBRA_BROKER_POOL_SIZE`, `VERTEBRA_BROKER_MAX_OVERFLOW`, `VERTEBRA_BROKER_STMT_CAPACITY` | `workers.broker_*` | `20`, `10`, `512` |
| `VERTEBRA_BP_MAX_IN_FLIGHT`, `VERTEBRA_BP_MAX_QUEUE_DEPTH`, `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `backpressure` | `1024`, `1024`, `100 ms` |
| `VERTEBRA_BP_CB_FAILURE_RATE`, `VERTEBRA_BP_CB_MIN_SAMPLES`, `VERTEBRA_BP_CB_WINDOW_MS`, `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `backpressure.circuit_breaker` | `0.5`, `20`, `10 s`, `5` |
