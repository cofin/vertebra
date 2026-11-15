---
type: Guide
title: "HTTP Ingress, Request Pipeline & Native Routing"
description: "How vertebra-server accepts connections, orders the per-request pipeline in response::handle, matches routes in Rust, and reads its environment configuration."
status: stable
tags:
  - architecture
  - http
  - routing
  - tls
  - rust
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-archive
    at: "2026-11-14T22:49:00Z"
    commit: f277a29b
  - by: agent:flow-executor
    at: "2026-11-14T22:27:00Z"
    commit: afa74da5
  - by: agent:flow-learn
    at: "2026-11-14T17:15:00Z"
    commit: 1b191388
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 067fa901
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-main-rs
    resource: src/rs/vertebra-http/src/main.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/main.rs)"
  - id: src-vertebra-http-src-server-rs
    resource: src/rs/vertebra-http/src/server.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/server.rs)"
  - id: src-vertebra-http-src-response-rs
    resource: src/rs/vertebra-http/src/response.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/response.rs)"
  - id: src-vertebra-http-src-config-rs
    resource: src/rs/vertebra-http/src/config.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/config.rs)"
  - id: src-vertebra-http-src-routing-rs
    resource: src/rs/vertebra-http/src/routing.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/routing.rs)"
  - id: src-vertebra-http-src-cors-rs
    resource: src/rs/vertebra-http/src/cors.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/cors.rs)"
  - id: src-vertebra-http-src-compression-rs
    resource: src/rs/vertebra-http/src/compression.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/compression.rs)"
  - id: src-vertebra-http-src-assets-rs
    resource: src/rs/vertebra-http/src/assets.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/assets.rs)"
  - id: src-vertebra-http-src-tls-rs
    resource: src/rs/vertebra-http/src/tls.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/tls.rs)"
  - id: src-vertebra-http-src-hmr-rs
    resource: src/rs/vertebra-http/src/hmr.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/hmr.rs)"
  - id: src-vertebra-http-src-health-rs
    resource: src/rs/vertebra-http/src/health.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/health.rs)"
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
  - id: src-vertebra-core-src-router-rs
    resource: src/rs/vertebra-core/src/router.rs
    title: "Rust implementation module (src/rs/vertebra-core/src/router.rs)"
  - id: src-framework-router-py
    resource: src/py/vertebra/framework/_router.py
    title: "Python implementation module (src/py/vertebra/framework/_router.py)"
---

# HTTP Ingress, Request Pipeline & Native Routing

<!-- truth: start -->
- `vertebra-server` loads `ServerConfig::from_env`, validates HTTP/3 prerequisites (`validate_http3`), terminates TCP/TLS and optional UDP HTTP/3 QUIC (`--features http3`), and shares a fixed-order ingress pipeline across `response::handle` and `response::handle_buffered`.
- `Alt-Svc` is emitted strictly when `ServerState::http3_port` holds a live bound QUIC port; `Topology::Embedded` registers `_vertebra` via `pyo3::append_to_inittab!(pymodule_init)` prior to CPython startup so Rust and Python share `BRIDGE`.
<!-- truth: end -->

The `vertebra-server` binary ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs], package `vertebra-http`) terminates TCP or TLS, executes zero-GIL Rust offloads (health, CORS, OpenAPI schema cache, static assets, response compression), matches every request against a Rust route table, and hands matched requests to the batching pipeline described in [Request Batching, Dispatch & Backpressure](batching-dispatch-and-backpressure.md). Additional library modules in the crate are inventoried in [Protocol and Offload Modules](protocol-and-offload-modules.md).

## Startup (`main.rs`)

`vertebra-server` configures itself from `VERTEBRA_*` environment variables via [`ServerConfig::from_env`](../../../../../src/rs/vertebra-http/src/config.rs)[^src-vertebra-http-src-config-rs], with `VERTEBRA_APP` (default `app:app`) naming the Python application. In order, `main`:

1. Builds an explicit Tokio runtime via `build_tokio_runtime(worker_threads)`: `Builder::new_current_thread()` when `worker_threads <= 1` and `Builder::new_multi_thread().worker_threads(worker_threads)` when `> 1`.
2. When `Topology::Embedded` runs on Unix with `config.workers.count > 1` and `VERTEBRA_EMBEDDED_WORKER` is unset, starts [`EmbeddedSupervisor`](../../../../../src/rs/vertebra-http/src/workers/factory.rs) on a single-threaded Tokio runtime to pre-allocate shared SHM broker/pubsub FDs and supervise `W` `SO_REUSEPORT` child `vertebra-server` processes (`VERTEBRA_EMBEDDED_WORKER=1`, `VERTEBRA_REUSE_PORT=1`).
3. Prepends `.venv/bin` / `VIRTUAL_ENV/bin` to `PATH` via `prepare_embedded_python_env` while single-threaded, registers the embedded `_vertebra` module with `append_to_inittab!(pymodule_init)` before the first `Python::attach` (feature `python`, on by default), and initializes `BatchBridge::init(256, 1_048_576, 16, config.thread_count.max(1))` strictly when `config.topology == Topology::Embedded`.
4. Attaches to Python, populates `sys.path` via `ensure_embedded_python_path` (including `.venv`/`VIRTUAL_ENV` `site-packages`), imports `VERTEBRA_APP`, calls `extract_offload_json_from_app` (invoking `VertebraPlugin.ensure_finalized(app)` and reading `app.state["vertebra_offload_config_json"]` when `VERTEBRA_OFFLOAD_CONFIG` is unset), and builds a [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs)[^src-vertebra-http-src-routing-rs] from `app.handler_index` plus `app.mounts`. Each mount prefix is registered via `insert_mount_prefix_indexed(prefix, mount_idx)` with `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | mount_idx`) across `GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`, and `HEAD` (both exact prefix and `{prefix}/*rest`). A raw callable app is wrapped in `vertebra.framework._app.App` and mounted at `/`.
5. Selects a dispatcher:
   - `Topology::Shm` (Unix): creates a `RingFactory::with_broker_config(worker_count.max(1), RingPairConfig::for_environment(worker_count.max(1)), ...)`, `ShmDispatcher::new(ring_pairs, 65536)` wrapped in `Dispatcher::from_shm` with `BackpressureConfig`, spawns `shm.spawn_sweeper`, `shm_reader::spawn_response_readers`, the Python worker processes, and `Supervisor::spawn`.
   - `Topology::Embedded`: creates `Dispatcher::new()` with `BackpressureConfig` (registering a direct `ResponseSink` on `BatchBridge` plus `spawn_bridge_sweeper`) and calls `spawn_python_workers`, launching `1` Python worker thread on GIL builds or `thread_count.min(available_cpus)` worker threads sharing `BatchBridge` on free-threaded Python `3.14t`/`3.15t`, each running `vertebra.server.worker.run_worker`. If an embedded worker thread errors or exits while the server is still active, it cancels `server_shutdown_token` and causes `run_server` to return an `Err` (non-zero exit).
6. Spawns [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/batcher.rs) tasks (`config.workers.count.clamp(1, 8)` in `Topology::Shm` for 1:1 batcher-to-worker shard affinity, and `config.worker_threads.clamp(1, 8)` in `Topology::Embedded`; `BatchLimit::default()` of `64`, channel capacity `16,384`).
7. Runs `server::run_with_collector` until `SIGINT`, `SIGTERM`, or worker failure cancellation, cancels `CancellationToken`, joins embedded Python worker threads or drains SHM workers (`RingFactory::shutdown_workers`, 5 s grace before `SIGKILL`).

## Accept loop (`server.rs`)

[`run_with_listener`](../../../../../src/rs/vertebra-http/src/server.rs)[^src-vertebra-http-src-server-rs] (and `run_with_collector`, which binds via [`bind_listener`](../../../../../src/rs/vertebra-http/src/server.rs) using `TcpSocket` with `SO_REUSEADDR`, Unix `SO_REUSEPORT` when `config.reuse_port` is true, `TCP_NODELAY`, and `listen(2048)`) calls `config.validate_http3()?` upfront (`main.rs` also calls `config.validate_http3()?` before `EmbeddedSupervisor::spawn_all()`) and constructs a shared [`ServerState`](../../../../../src/rs/vertebra-http/src/server.rs) caching `Arc<Router>`, `Arc<Dispatcher>`, `Arc<ServerConfig>`, `collector_shards`, `HmrState`, `health_endpoint: HealthEndpoint`, `profile_endpoint: ProfileEndpoint`, `access_logger: Option<Arc<AccessLogger>>`, `discovery_cache: WellKnownDiscoveryCache`, `sse_hub: Arc<SseHub>`, `pubsub_broker: Arc<PubSubBroker>`, `ws_registry: Arc<WsConnectionRegistry>`, `cors: Option<CompiledCorsOffload>`, `compression: Option<CompressionOffloadConfig>`, `static_mounts: Vec<StaticAssetMount>`, `openapi_cache: Option<CompiledOpenApiCache>`, and `http3_port: Option<u16>`. Under `--features http3` and `config.enable_http3()`, `run_with_listener` builds a `quinn::ServerConfig` from `(*tls_acc.rustls_config()).clone()`, binds a non-blocking UDP socket via `bind_udp_listener(udp_addr, config.reuse_port)` (`SO_REUSEADDR` plus Unix `SO_REUSEPORT`) wrapped in `quinn::Endpoint::new`, sets `state.http3_port = Some(quic_port)`, and spawns `http3::serve_http3_endpoint` enforcing `max_body_size` pre-buffering (`413` + `stop_sending`), RFC 9114 hop-by-hop response header stripping (`strip_hop_by_hop_headers`), and `response::handle_buffered` dispatch. Graceful shutdown closes WebSockets (`ws_registry.close_all`), triggers `h3_conn.shutdown(0)`, drains active QUIC connection/stream `JoinSet`s (`5 s` deadline), closes `endpoint`, and awaits bounded `endpoint.wait_idle()`.

- Every accepted stream gets `TCP_NODELAY`.
- When `enable_h2c_prior_knowledge` (`VERTEBRA_H2C`) is `false` (the default), `serve_plain` skips `MSG_PEEK` (`h2c::detect_cleartext_protocol`) and serves cleartext HTTP/1.1 directly through `hyper::server::conn::http1::Builder::new().keep_alive(true).pipeline_flush(false).max_buf_size(65_536).timer(TokioTimer::new())` with upgrades enabled. When `enable_h2c_prior_knowledge` is `true`, `serve_plain` runs `detect_cleartext_protocol` and `AutoBuilder` (`http1` + `http2` stream/connection windows from `ServerConfig`).
- When `ServerConfig::tls` is set (`VERTEBRA_TLS_CERT` and `VERTEBRA_TLS_KEY`), [`TlsAcceptor::from_config`](../../../../../src/rs/vertebra-http/src/tls.rs)[^src-vertebra-http-src-tls-rs] installs `rustls::crypto::ring::default_provider()`, loads PEM certificates, and advertises ALPN `h2` and `http/1.1` (with `WebPkiClientVerifier` when `MtlsConfig.mode` is `Optional` or `Required`).
- `ServerState::get_collector` short-circuits directly to `collector_shards[0]` when `len == 1` and selects round-robin via `Relaxed` `AtomicUsize` otherwise.

## Per-request pipeline (`response::handle`)

[`handle`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs] (for Hyper `Request<Incoming>`, handling dev HMR WebSockets first) and `handle_buffered` (for QUIC HTTP/3 `Request<Bytes>`) share `dispatch_request`, executing in order:

1. **Dev HMR**: when `dev_mode` (`VERTEBRA_DEV=1`) and `hmr::is_hmr_path(path)` matches `/__vertebra/hmr` or `/__vertebra/hmr/signal`, [`hmr::handle`](../../../../../src/rs/vertebra-http/src/hmr.rs)[^src-vertebra-http-src-hmr-rs] answers.
2. **Health probes**: `/__vertebra/health` returns `200` JSON with the live `BackpressureMetricsSnapshot` when healthy, and the cached `state.health_endpoint.try_intercept(path)` answers `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz` with zero per-request `HealthEndpoint` allocation.
3. **Runtime profile endpoint**: when `state.profile_endpoint` (`VERTEBRA_PPROF_ENABLED=1`) is enabled, `ProfileEndpoint::try_handle` serves `/debug/pprof/profile` and `/debug/pprof/heap` (requiring constant-time `Authorization: Bearer <token>` matching against `VERTEBRA_PPROF_TOKEN`).
4. **Discovery manifests**: `GET`/`HEAD` requests for `/.well-known/*` documents (`/.well-known/agent-card.json`, `/.well-known/mcp.json`) check `state.discovery_cache.serve` and return cached JSON with `ETag`/`304 Not Modified` support when populated.
5. **CORS `OPTIONS` preflight offload**: when `state.cors` is configured and an `OPTIONS` request carries `Origin` and `Access-Control-Request-Method`, `CompiledCorsOffload::evaluate_preflight` returns immediately without acquiring Python or backpressure permits.
6. **OpenAPI schema cache offload**: `GET`/`HEAD` requests matching `state.openapi_cache.schema_path` serve the pre-rendered OpenAPI payload (with `If-None-Match` -> `304 Not Modified`, `ETag`, `Cache-Control`, and optional CORS headers).
7. **Static asset mounts**: `GET`/`HEAD` requests matching `state.static_mounts` serve files directly via `AssetManager::serve_static_request` (pre-compressed sidecars, `ETag`/`304`, `Range`/`206`, SPA HTML fallback, custom headers, `Content-Disposition: attachment`, and CORS headers).
8. **Admission**: `BackpressureController::try_acquire_for_path` acquires an atomic in-flight permit (or returns `503`/`429` with `retry-after: 1`).
9. **Route lookup**: `Router::lookup_method` returns a matched `HandlerId` (`MOUNT_HANDLER_BASE | mount_idx` for mounted apps), `404 Not Found`, or `405 Method Not Allowed` with an `allow` header.
10. **Bounded body, decompression, multipart & gRPC validation**: `handle` fast-paths bodiless requests (`body.size_hint().exact() == Some(0)`) and uses `payload::collect_bounded_body`, while `handle_buffered` delegates length validation and bounded `Content-Encoding` (`gzip`, `deflate`, `br`, `zstd`) decompression to `payload::decode_buffered_body` (`413 Payload Too Large` on overflow). `multipart/form-data` bodies are checked against `MultipartLimits`, and `application/grpc` bodies are checked against `max_grpc_message_size` before `HeaderMap` is compact-encoded and sent to a `RequestBatcher` shard (`503` if shutting down).
11. **Response, post-filters & access log**: awaits the `oneshot` reply, applies `build_response_with_offloads` (`CompiledCorsOffload::apply_response_headers` and `CompressionOffloadConfig::compress_if_eligible` with compressible `Content-Type` filtering and multi-token `Vary` preservation, streaming when `stream_rx` is set, or `500 worker error` on dropped channel), records lock-free circuit-breaker `record_failure` (`5xx`) or `record_success`, logs `AccessLogEntry` when `state.access_logger` is enabled, and attaches `Alt-Svc` strictly when `state.http3_port` is `Some(port)`.

## Routing (`routing.rs`)

- [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs) is a startup builder (`insert`, `insert_with_id`, `insert_mount_prefix_indexed`, `into_entries`). Dev reload restarts the worker/server process rather than hot-swapping `Router`.
- [`routing::handler_id`](../../../../../src/rs/vertebra-http/src/routing.rs) hashes uppercased method, `' '`, and path with `FnvHasher` via `std::hash::Hash`, matching [`vertebra.framework._router._make_handler_id`](../../../../../src/py/vertebra/framework/_router.py)[^src-framework-router-py] byte-for-byte; mounted prefixes carry `MOUNT_HANDLER_BASE | mount_idx` (`0xFFFF_0000_0000_0000 | idx`).
- [`Router::from_entries`](../../../../../src/rs/vertebra-http/src/routing.rs) normalizes `:name`, `{name:type}`, `*name`, and `{name:path}` segments via [`vertebra_core::router::normalize_typed_path`](../../../../../src/rs/vertebra-core/src/router.rs)[^src-vertebra-core-src-router-rs] into per-method `matchit::Router<HandlerId>` trees (`lookup_method` avoids string allocation on standard HTTP verbs).

## Configuration (`config.rs`)

| Variable | Field | Default |
|---|---|---|
| `VERTEBRA_HOST`, `VERTEBRA_PORT` | `bind` | `127.0.0.1:8000` |
| `VERTEBRA_REUSE_PORT` | `reuse_port` (auto-enabled on Unix when `Embedded` and `workers.count > 1`) | `false` |
| `VERTEBRA_DEV` | `dev_mode` | `false` |
| `VERTEBRA_H2C` | `enable_h2c_prior_knowledge` | `false` |
| `VERTEBRA_OFFLOAD_CONFIG` | `cors`, `compression`, `static_mounts`, `openapi_cache`, `discovery`, `payload_limits`, `multipart_limits`, `max_grpc_message_size`, `access_log`, `profile` JSON | auto-extracted from `app.state` |
| `VERTEBRA_MAX_BODY_SIZE`, `VERTEBRA_MAX_DECOMPRESSED_SIZE`, `VERTEBRA_DECOMPRESSION` | `payload_limits` | `10 MiB`, `50 MiB`, `true` |
| `VERTEBRA_MULTIPART_MAX_FIELDS`, `VERTEBRA_MULTIPART_MAX_FILE_SIZE`, `VERTEBRA_MULTIPART_MAX_TOTAL_SIZE` | `multipart_limits` | `128`, `16 MiB`, `32 MiB` |
| `VERTEBRA_GRPC_MAX_MESSAGE_SIZE` | `max_grpc_message_size` | `4 MiB` |
| `VERTEBRA_PPROF_ENABLED`, `VERTEBRA_PPROF_TOKEN` | `profile` | `false`, `None` |
| `VERTEBRA_ACCESS_LOG` (`1`/`clf`/`common`, `json`, `0`/`off`) | `access_log` | `None` |
| `VERTEBRA_TLS_CERT`, `VERTEBRA_TLS_KEY` | `tls` | `None` |
| `VERTEBRA_HTTP3`, `VERTEBRA_HTTP3_ALT_SVC_MAX_AGE`, `VERTEBRA_HTTP3_MAX_STREAMS`, `VERTEBRA_HTTP3_KEEP_ALIVE_MS` | `http3` (`--features http3` + TLS) | `false`, `86400`, `256`, `15000 ms` |
| `VERTEBRA_TOPOLOGY` (`auto`, `embedded`, `shm`) | `topology` (`auto` resolves to `Embedded` on Unix) | `Embedded` |
| `VERTEBRA_WORKERS` | `workers.count` | `0` (`1` active worker) |
| `VERTEBRA_THREADS`, `VERTEBRA_TOKIO_THREADS` | `thread_count`, `worker_threads` (`VERTEBRA_THREADS` clamps `worker_threads` to cgroup/affinity-aware `num_cpus()`) | cgroup/affinity CPU count |
| `VERTEBRA_PYTHON_BIN`, `VERTEBRA_APP` | `workers.python_bin`, `workers.app_path` | `python3`, `app:app` |
| `VERTEBRA_BROKER_POOL_SIZE`, `VERTEBRA_BROKER_MAX_OVERFLOW`, `VERTEBRA_BROKER_STMT_CAPACITY` | `workers.broker_*` | `20`, `10`, `512` |
| `VERTEBRA_SHM_PROFILE` (`default`, `compact`/`serverless`), `VERTEBRA_OVERFLOW_BACKEND` (`shm`, `tmp`) | `RingPairConfig::for_environment` / `ShmRegion::create_overflow_region` | auto (`46 MiB/worker` default, `12 MiB/worker` compact), `shm` with `/tmp/vtb-*` fallback |
| `VERTEBRA_BP_MAX_IN_FLIGHT`, `VERTEBRA_BP_MAX_QUEUE_DEPTH`, `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `backpressure` | `4096`, `4096`, `5000 ms` |
| `VERTEBRA_BP_CB_FAILURE_RATE`, `VERTEBRA_BP_CB_MIN_SAMPLES`, `VERTEBRA_BP_CB_WINDOW_MS`, `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `backpressure.circuit_breaker` | `0.5`, `20`, `10 s`, `5` |

Provenance sources: [^src-vertebra-http-src-cors-rs], [^src-vertebra-http-src-compression-rs], [^src-vertebra-http-src-assets-rs], [^src-vertebra-http-src-health-rs], [^src-vertebra-http-src-payload-rs], [^src-vertebra-http-src-multipart-rs], [^src-vertebra-http-src-access-log-rs], [^src-vertebra-http-src-profile-rs], [^src-vertebra-http-src-discovery-rs].

[^src-vertebra-http-src-main-rs]: `src/rs/vertebra-http/src/main.rs` — Rust implementation module (src/rs/vertebra-http/src/main.rs).
[^src-vertebra-http-src-server-rs]: `src/rs/vertebra-http/src/server.rs` — Rust implementation module (src/rs/vertebra-http/src/server.rs).
[^src-vertebra-http-src-response-rs]: `src/rs/vertebra-http/src/response.rs` — Rust implementation module (src/rs/vertebra-http/src/response.rs).
[^src-vertebra-http-src-config-rs]: `src/rs/vertebra-http/src/config.rs` — Rust implementation module (src/rs/vertebra-http/src/config.rs).
[^src-vertebra-http-src-routing-rs]: `src/rs/vertebra-http/src/routing.rs` — Rust implementation module (src/rs/vertebra-http/src/routing.rs).
[^src-vertebra-http-src-cors-rs]: `src/rs/vertebra-http/src/cors.rs` — Rust implementation module (src/rs/vertebra-http/src/cors.rs).
[^src-vertebra-http-src-compression-rs]: `src/rs/vertebra-http/src/compression.rs` — Rust implementation module (src/rs/vertebra-http/src/compression.rs).
[^src-vertebra-http-src-assets-rs]: `src/rs/vertebra-http/src/assets.rs` — Rust implementation module (src/rs/vertebra-http/src/assets.rs).
[^src-vertebra-http-src-tls-rs]: `src/rs/vertebra-http/src/tls.rs` — Rust implementation module (src/rs/vertebra-http/src/tls.rs).
[^src-vertebra-http-src-hmr-rs]: `src/rs/vertebra-http/src/hmr.rs` — Rust implementation module (src/rs/vertebra-http/src/hmr.rs).
[^src-vertebra-http-src-health-rs]: `src/rs/vertebra-http/src/health.rs` — Rust implementation module (src/rs/vertebra-http/src/health.rs).
[^src-vertebra-http-src-payload-rs]: `src/rs/vertebra-http/src/payload.rs` — Rust implementation module (src/rs/vertebra-http/src/payload.rs).
[^src-vertebra-http-src-multipart-rs]: `src/rs/vertebra-http/src/multipart.rs` — Rust implementation module (src/rs/vertebra-http/src/multipart.rs).
[^src-vertebra-http-src-access-log-rs]: `src/rs/vertebra-http/src/access_log.rs` — Rust implementation module (src/rs/vertebra-http/src/access_log.rs).
[^src-vertebra-http-src-profile-rs]: `src/rs/vertebra-http/src/profile.rs` — Rust implementation module (src/rs/vertebra-http/src/profile.rs).
[^src-vertebra-http-src-discovery-rs]: `src/rs/vertebra-http/src/discovery.rs` — Rust implementation module (src/rs/vertebra-http/src/discovery.rs).
[^src-vertebra-core-src-router-rs]: `src/rs/vertebra-core/src/router.rs` — Rust implementation module (src/rs/vertebra-core/src/router.rs).
[^src-framework-router-py]: `src/py/vertebra/framework/_router.py` — Python implementation module (src/py/vertebra/framework/_router.py).
