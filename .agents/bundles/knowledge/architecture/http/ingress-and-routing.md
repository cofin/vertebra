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
  - resource: src/rs/vertebra-http/src/router.rs
  - resource: src/rs/vertebra-http/src/route_table.rs
  - resource: src/rs/vertebra-http/src/tls.rs
  - resource: src/rs/vertebra-http/src/hmr.rs
  - resource: src/rs/vertebra-http/src/health.rs
  - resource: src/rs/vertebra-core/src/router.rs
  - resource: src/py/vertebra/framework/_router.py
---

# HTTP Ingress, Request Pipeline & Native Routing

The `vertebra-server` binary ([`main.rs`](../../../../../src/rs/vertebra-http/src/main.rs), package `vertebra-http`) terminates TCP or TLS, matches every request against a Rust route table, and hands matched requests to the batching pipeline described in [Request Batching, Dispatch & Backpressure](batching-dispatch-and-backpressure.md). Modules that exist in the crate but are not on this path are listed in [Protocol and Offload Modules](protocol-and-offload-modules.md).

## Startup (`main.rs`)

`vertebra-server` has no command-line flags. [`ServerConfig::from_env`](../../../../../src/rs/vertebra-http/src/config.rs) builds the configuration from `VERTEBRA_*` variables, and `VERTEBRA_APP` (default `app:app`) names the Python app. In order, `main`:

1. Calls `BatchBridge::init(256, 1_048_576, 4, workers)` for both topologies.
2. Registers the embedded `_vertebra` module with `append_to_inittab!(pymodule_init)` before the first `Python::attach` (feature `python`, on by default).
3. Imports the app and builds a [`RouteTable`](../../../../../src/rs/vertebra-http/src/route_table.rs) from `app.handler_index`. Each mount prefix is registered as `{prefix}/*rest` for `GET`, `POST`, `PUT`, `DELETE`, and `PATCH`. A raw callable app is wrapped in `vertebra.framework._app.App` and mounted at `/`.
4. Selects a dispatcher. With `Topology::Shm` on Unix it creates a `RingFactory`, `Dispatcher::new_shm(rings, 65536)`, `response_reader::run_response_reader`, and spawns the worker processes. Otherwise it uses the Bridge dispatcher, a bridge response-reader task, and one embedded Python thread that runs `vertebra.server._native.run_worker`.
5. Starts eight `RequestBatcher` tasks (`BatchPolicy::default_adaptive()`, channel capacity 16,384).
6. Runs `server::run_with_collector` until Ctrl-C (SIGINT).

After the accept loop returns, SHM workers get SIGTERM and then SIGKILL if still running after 5 s (`RingFactory::shutdown_workers`). Connection tasks that are already running are not awaited.

## Accept loop (`server.rs`)

[`run_with_listener`](../../../../../src/rs/vertebra-http/src/server.rs) (and `run_with_collector`, which binds `ServerConfig::bind` first) owns a [`ServerState`](../../../../../src/rs/vertebra-http/src/server.rs) holding the `Arc<Router>`, `Arc<Dispatcher>`, config, the batcher senders, and the dev `HmrState`.

- Every accepted socket gets `TCP_NODELAY`. No other socket options are set.
- Cleartext connections call `h2c::detect_cleartext_protocol` (one `peek`; the result is ignored), then serve through `hyper_util::server::conn::auto::Builder` with `http1().pipeline_flush(true)` and the `h2_*` window and stream settings from `ServerConfig`. HTTP/2 prior knowledge works on cleartext.
- When `ServerConfig::tls` is set (both `VERTEBRA_TLS_CERT` and `VERTEBRA_TLS_KEY`), [`TlsAcceptor::from_config`](../../../../../src/rs/vertebra-http/src/tls.rs) loads the PEM files and advertises ALPN `h2` and `http/1.1`. mTLS uses `WebPkiClientVerifier` when `MtlsConfig.mode` is `Optional` or `Required`; `from_env` always sets `MtlsConfig::default()`. The TLS path does not apply the `h2_*` settings.
- `ServerState::get_collector` picks a batcher sender round-robin with an `AtomicUsize`.

## Per-request pipeline (`response::handle`)

[`handle`](../../../../../src/rs/vertebra-http/src/response.rs) runs these steps in order:

1. Dev HMR: when `dev_mode` (`VERTEBRA_DEV=1`) and the path is `/__vertebra/hmr` or `/__vertebra/hmr/signal`, [`hmr::handle`](../../../../../src/rs/vertebra-http/src/hmr.rs) answers.
2. `/__vertebra/health` returns 200 with the backpressure snapshot as JSON when a controller is attached.
3. [`HealthEndpoint::default().try_intercept`](../../../../../src/rs/vertebra-http/src/health.rs) answers `/healthz`, `/livez`, `/__vertebra/health`, and `/readyz`. A new endpoint is built per request, so `set_state` changes are never visible here.
4. Admission: `BackpressureController::try_acquire_for_path`. This runs before routing, so 404 and 405 responses also take a permit.
5. `Router::lookup`: 404 `Not Found`, or 405 with an `allow` header.
6. The full request body is buffered with `BodyExt::collect`.
7. Headers are compact-encoded and a `PendingRequest` with a `oneshot` reply channel is sent to a batcher; a closed channel returns 503.
8. The reply is awaited and converted by `build_response`, which streams when `PendingResponse::stream_rx` is set. A dropped reply returns 500 `worker error`.
9. `http3::inject_alt_svc_header` runs when `http3.enabled` is true.
10. The circuit breaker records a failure for 5xx and a success otherwise.

## Routing (`router.rs`, `route_table.rs`)

- [`RouteTable`](../../../../../src/rs/vertebra-http/src/route_table.rs) is a startup builder: `insert(method, path) -> HandlerId` and `into_entries()`. There is no runtime route swap; dev reload restarts the process.
- [`route_table::handler_id`](../../../../../src/rs/vertebra-http/src/route_table.rs) returns a `u64` (`HandlerId`) by feeding the uppercased method, a space byte, and the path to `FnvHasher` through `std::hash::Hash`, which appends `0xff` after each `str`. [`vertebra.framework._router._make_handler_id`](../../../../../src/py/vertebra/framework/_router.py) writes the same bytes, so Python and Rust agree on IDs without a registration exchange.
- [`Router::from_entries`](../../../../../src/rs/vertebra-http/src/router.rs) builds a `HashMap<String, matchit::Router<HandlerId>>`, one tree per registered method. Each path goes through [`vertebra_core::router::normalize_typed_path`](../../../../../src/rs/vertebra-core/src/router.rs), which rewrites `:name` and `{name:type}` to `{name}`, and `*name` and `{name:path}` to `{*name}`.
- `Router::lookup` uppercases the method if needed. On a miss it calls `allowed_methods(path)` across all trees and returns `RouterError::MethodNotAllowed { allowed }` or `RouterError::NotFound`.

## Configuration (`config.rs`)

| Variable | Field | Default |
|---|---|---|
| `VERTEBRA_HOST`, `VERTEBRA_PORT` | `bind` | `127.0.0.1:8000` |
| `VERTEBRA_DEV` | `dev_mode` | off |
| `VERTEBRA_TLS_CERT`, `VERTEBRA_TLS_KEY` | `tls` | none |
| `VERTEBRA_TOPOLOGY` (`embedded`, `shm`) | `topology` | `Embedded` |
| `VERTEBRA_WORKERS` | `workers.count` (`> 0` selects `Shm` when `VERTEBRA_TOPOLOGY` is unset) | `0` |
| `VERTEBRA_THREADS` | `thread_count` | CPU count |
| `VERTEBRA_PYTHON_BIN`, `VERTEBRA_APP` | `workers.python_bin`, `workers.app_path` | `python3`, empty |
| `VERTEBRA_BP_MAX_IN_FLIGHT`, `VERTEBRA_BP_MAX_QUEUE_DEPTH`, `VERTEBRA_BP_MAX_QUEUE_TIME_MS` | `backpressure` | 1024, 1024, 100 ms |
| `VERTEBRA_BP_CB_FAILURE_RATE`, `VERTEBRA_BP_CB_MIN_SAMPLES`, `VERTEBRA_BP_CB_WINDOW_MS`, `VERTEBRA_BP_CB_HALF_OPEN_PROBES` | `backpressure.circuit_breaker` | 0.5, 20, 10 s, 5 |

These `ServerConfig` fields are never read outside `config.rs`: `worker_threads`, `batch_size`, `batch_timeout`, `keepalive_timeout`, `max_connections`, `thread_count`, `h2_keep_alive_interval_ms`, `enable_h2c_upgrade`, `trusted_proxies`, and `workers.app_path` (`main.rs` reads `VERTEBRA_APP` and `VERTEBRA_WORKERS` directly). Setting them has no effect.
