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
    commit: ff9f78cb
  - by: agent:flow-executor
    at: "2026-11-14T22:27:00Z"
    commit: ff9f78cb
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
  - id: src-vertebra-http-src-ws-mount-rs
    resource: src/rs/vertebra-http/src/ws_mount.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/ws_mount.rs)"
  - id: src-vertebra-http-src-pubsub-rs
    resource: src/rs/vertebra-http/src/pubsub.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/pubsub.rs)"
  - id: src-vertebra-http-src-webtransport-rs
    resource: src/rs/vertebra-http/src/webtransport.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/webtransport.rs)"
---

# HTTP Ingress, Request Pipeline & Native Routing

<!-- truth: start -->
- `vertebra-server` loads `ServerConfig::from_env`, validates HTTP/3 prerequisites (`validate_http3`), terminates TCP/TLS and optional UDP HTTP/3 QUIC (`--features http3`), and runs a fixed-order ingress pipeline across `response::handle` and `response::handle_buffered`.
- `Alt-Svc` is emitted only when `ServerState::http3_port` holds a live bound QUIC port; `Topology::Embedded` registers `_vertebra` via `pyo3::append_to_inittab!(pymodule_init)` before CPython startup so Rust and Python share `BRIDGE`.
<!-- truth: end -->

The `vertebra-server` binary ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs)[^src-vertebra-http-src-main-rs]) terminates TCP/TLS/QUIC, runs zero-GIL Rust offloads, matches routes in Rust, and dispatches matched requests through [Request Batching, Dispatch & Backpressure](batching-dispatch-and-backpressure.md). See [Protocol and Offload Modules](protocol-and-offload-modules.md) for the full module inventory.

## Startup Sequence (`main.rs`)

`vertebra-server` reads `VERTEBRA_*` env vars via [`ServerConfig::from_env`](../../../../../src/rs/vertebra-http/src/config.rs)[^src-vertebra-http-src-config-rs] (default app `app:app`) and executes in order:

1. **Tokio Runtime**: `build_tokio_runtime(worker_threads)` builds a current-thread runtime when `<= 1`, else a multi-thread runtime.
2. **Multi-Process Embedded Supervision**: On Unix when `Topology::Embedded`, `workers.count > 1`, and `VERTEBRA_EMBEDDED_WORKER` is unset, starts `EmbeddedSupervisor` to pre-allocate SHM broker/pubsub FDs and supervise `W` `SO_REUSEPORT` child `vertebra-server` processes.
3. **Python Bootstrap**: Prepends `.venv/bin` to `PATH`, registers `_vertebra` via `append_to_inittab!(pymodule_init)` before the first `Python::attach`, and initializes `BatchBridge` only in `Topology::Embedded`.
4. **App & Route Loading**: Imports `VERTEBRA_APP`, extracts offload JSON (`VERTEBRA_OFFLOAD_CONFIG` or `app.state["vertebra_offload_config_json"]` after `VertebraPlugin.ensure_finalized`), and builds [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs)[^src-vertebra-http-src-routing-rs] from `app.handler_index` and `app.mounts` (mounts use `MOUNT_HANDLER_BASE | mount_idx` across standard HTTP verbs; raw callables are wrapped in `App` at `/`).
5. **Dispatcher Selection**:
   - `Topology::Shm`: allocates `RingFactory`, builds `ShmDispatcher` + `BackpressureConfig`, spawns sweeper, `ResponseReaderRegistry`, Python worker subprocesses, `Supervisor`, and `PubSubBroker::spawn_shm_channel_forwarder`.
   - `Topology::Embedded`: builds bridge `Dispatcher` (wiring `ResponseSink`, `StreamSink`, and `PubSubSink` on `BatchBridge`) and spawns 1 Python worker thread on GIL builds or `thread_count.min(cpus)` threads on free-threaded Python. Any unexpected worker exit cancels `server_shutdown_token`.
6. **Batchers & Shutdown**: Spawns `1..=8` [`RequestBatcher`](../../../../../src/rs/vertebra-http/src/batcher.rs) shards (`BatchLimit` 64, channel 16,384), serves until signal/failure, drains connections, joins readers and embedded threads, or terminates SHM workers (`5 s` grace before `SIGKILL`).

## Accept Loop & Connection Lifecycle (`server.rs`)

- **Listener & `ServerState`**: [`run_with_listener`](../../../../../src/rs/vertebra-http/src/server.rs)[^src-vertebra-http-src-server-rs] validates HTTP/3, binds TCP (`SO_REUSEADDR`, Unix `SO_REUSEPORT`, `TCP_NODELAY`, backlog 2,048), and shares one `ServerState` holding the router, dispatcher, config, batcher shards, health/profile/discovery caches, SSE/WS/PubSub registries, compiled offloads, and optional `http3_port`.
- **HTTP/1.1, h2c & TLS**: Cleartext defaults directly to Hyper `http1::Builder` (`max_buf_size(65_536)`, `pipeline_flush(false)`, `header_read_timeout` 10 s) without `MSG_PEEK`; enabling `VERTEBRA_H2C` peeks for `h2c` prior knowledge. TLS ([`tls.rs`](../../../../../src/rs/vertebra-http/src/tls.rs)[^src-vertebra-http-src-tls-rs]) uses `rustls` ring provider with ALPN `h2`/`http/1.1` and optional/required mTLS (`WebPkiClientVerifier`).
- **HTTP/3 & WebTransport**: Under `--features http3` + TLS, binds UDP with `quinn::Endpoint`, records `state.http3_port`, and serves `http3::serve_http3_with_bridge` (body pre-buffering, RFC 9114 hop-by-hop header stripping, `response::handle_buffered`, and extended `CONNECT` WebTransport sessions via [`webtransport.rs`](../../../../../src/rs/vertebra-http/src/webtransport.rs)[^src-vertebra-http-src-webtransport-rs] in `Embedded` mode; `501` in `Shm`).
- **Graceful Drain**: `GracefulShutdown` watches TLS/h2c/AutoBuilder tasks while `h1_drain_tx` drains upgradeable HTTP/1.1 connections up to `drain_timeout` (15 s), then closes SSE/WS registries, shuts down QUIC, and flushes `AccessLogger` (2 s).

## Per-Request Pipeline (`response.rs`)

[`response::handle`](../../../../../src/rs/vertebra-http/src/response.rs)[^src-vertebra-http-src-response-rs] and `handle_buffered` share `dispatch_request_inner` in fixed order:

1. **Dev HMR**: `/__vertebra/hmr` and `/__vertebra/hmr/signal` when `VERTEBRA_DEV=1` ([`hmr.rs`](../../../../../src/rs/vertebra-http/src/hmr.rs)[^src-vertebra-http-src-hmr-rs]).
2. **Health Probes**: `/healthz`, `/livez`, `/readyz`, and `/__vertebra/health` (with live `BackpressureMetricsSnapshot`).
3. **CPU Profiling**: `/debug/pprof/profile` when `VERTEBRA_PPROF_ENABLED=1` and Bearer token matches (`spawn_blocking` under `--features profiling`; `404` otherwise).
4. **Discovery Manifests**: `GET`/`HEAD` for `/.well-known/*` (`ETag`/`304`).
5. **SSE & WebSocket Pub/Sub**: `/__vertebra/sse/{topic}` (`PubSubBroker`[^src-vertebra-http-src-pubsub-rs]) and `/__vertebra/ws/{topic}` or `ws_mounts` ([`ws_mount.rs`](../../../../../src/rs/vertebra-http/src/ws_mount.rs)[^src-vertebra-http-src-ws-mount-rs]; `426` without `Upgrade`).
6. **CORS Preflight Offload**: Answers `OPTIONS` preflights with `Origin` + `Access-Control-Request-Method` without acquiring permits.
7. **OpenAPI Cache Offload**: Serves pre-rendered OpenAPI JSON with `ETag`/`304`.
8. **Static Asset Mounts**: Serves pre-compressed sidecars, `ETag`/`304`, `Range`/`206`, and SPA fallback via `AssetManager`.
9. **Admission**: Acquires a `BackpressureController` permit (`503`/`429` with `retry-after: 1` on rejection).
10. **Route Lookup**: `Router::lookup_method` returns `HandlerId`, `404`, or `405` with `allow`.
11. **Body Validation & Batching**: Fast-paths empty bodies; collects bounded bodies and decompresses `gzip`/`deflate`/`br`/`zstd` (`413` on overflow), validates multipart limits (`413`/`400`) and `max_grpc_message_size`, compact-encodes headers, and enqueues on a `RequestBatcher` shard.
12. **Response Offloads & Logging**: Applies CORS headers and response compression (with `Vary`), updates the circuit breaker, writes `AccessLogEntry` via `AsyncLogCore`, and attaches `Alt-Svc` only if `http3_port` is live.

## Native Routing (`routing.rs`)

- [`RouteTable`](../../../../../src/rs/vertebra-http/src/routing.rs) builds static startup route entries (dev reload restarts the server rather than hot-swapping `Router`).
- `routing::handler_id` hashes uppercased method + `' '` + path with FNV-1a, matching [`_router.py`](../../../../../src/py/vertebra/framework/_router.py)[^src-framework-router-py] byte-for-byte.
- `Router::from_entries` normalizes typed and wildcard segments via [`vertebra_core::router::normalize_typed_path`](../../../../../src/rs/vertebra-core/src/router.rs)[^src-vertebra-core-src-router-rs] into per-method `matchit::Router<HandlerId>` trees.

## Environment Configuration (`config.rs`)

| Variable | Field | Default |
|---|---|---|
| `VERTEBRA_HOST`, `VERTEBRA_PORT` | `bind` | `127.0.0.1:8000` |
| `VERTEBRA_REUSE_PORT`, `VERTEBRA_DEV`, `VERTEBRA_H2C` | `reuse_port`, `dev_mode`, `enable_h2c_prior_knowledge` | `false` (`reuse_port` auto-enabled for multi-worker `Embedded`) |
| `VERTEBRA_OFFLOAD_CONFIG` | CORS, compression, static, OpenAPI, discovery, limits, access log, profile JSON | auto-extracted from `app.state` |
| `VERTEBRA_MAX_BODY_SIZE`, `VERTEBRA_MAX_DECOMPRESSED_SIZE`, `VERTEBRA_DECOMPRESSION` | `payload_limits` | `10 MiB`, `50 MiB`, `true` |
| `VERTEBRA_MULTIPART_MAX_{FIELDS,FILE_SIZE,TOTAL_SIZE}` | `multipart_limits` | `128`, `16 MiB`, `32 MiB` |
| `VERTEBRA_GRPC_MAX_MESSAGE_SIZE` | `max_grpc_message_size` | `4 MiB` |
| `VERTEBRA_PPROF_ENABLED`, `VERTEBRA_PPROF_TOKEN`, `VERTEBRA_ACCESS_LOG` | `profile`, `access_log` | `false`, `None`, `None` |
| `VERTEBRA_TLS_CERT`, `VERTEBRA_TLS_KEY` | `tls` | `None` |
| `VERTEBRA_HTTP3*`, `VERTEBRA_WEBTRANSPORT*` | `http3` (`--features http3` + TLS) | `false`, `86400s`, `256` streams, WT enabled (`64` max) |
| `VERTEBRA_H2_MAX_HEADER_LIST_SIZE`, `VERTEBRA_HEADER_READ_TIMEOUT_MS`, `VERTEBRA_DRAIN_TIMEOUT_MS`, `VERTEBRA_STREAM_QUEUE_DEPTH` | connection/stream bounds | `65536`, `10000 ms`, `15000 ms`, `64` |
| `VERTEBRA_TOPOLOGY`, `VERTEBRA_WORKERS`, `VERTEBRA_THREADS`, `VERTEBRA_TOKIO_THREADS` | runtime topology & threads | `Embedded`, `0` (1 active), cgroup/affinity CPU count |
| `VERTEBRA_PYTHON_BIN`, `VERTEBRA_APP` | worker python & app path | `python3`, `app:app` |
| `VERTEBRA_BROKER_{POOL_SIZE,MAX_OVERFLOW,STMT_CAPACITY}` | SHM broker config | `20`, `10`, `512` |
| `VERTEBRA_SHM_PROFILE`, `VERTEBRA_OVERFLOW_BACKEND` | SHM sizing & overflow | auto (`46`/`12 MiB`), `shm` with `/tmp/vtb-*` fallback |
| `VERTEBRA_BP_MAX_*`, `VERTEBRA_BP_CB_*` | backpressure & circuit breaker | `4096`, `4096`, `5000 ms`; `0.5`, `20`, `10 s`, `5` |

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
[^src-vertebra-http-src-ws-mount-rs]: `src/rs/vertebra-http/src/ws_mount.rs` — Rust implementation module (src/rs/vertebra-http/src/ws_mount.rs).
[^src-vertebra-http-src-pubsub-rs]: `src/rs/vertebra-http/src/pubsub.rs` — Rust implementation module (src/rs/vertebra-http/src/pubsub.rs).
[^src-vertebra-http-src-webtransport-rs]: `src/rs/vertebra-http/src/webtransport.rs` — Rust implementation module (src/rs/vertebra-http/src/webtransport.rs).
