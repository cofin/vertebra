---
type: Spec
flow_id: runtime-perf-remediation
title: Runtime Performance Remediation and Full-Stack Data Benchmarks
description: Eliminate WSGI I/O serialization, Embedded 1w slowdown, HTTP ingress overhead, and add SQLite and PostgreSQL native, SQLSpec, and Advanced Alchemy benchmark parity.
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-wsgi-adaptive-pool
  - ch02-embedded-bridge-and-batching
  - ch03-ingress-and-worker-hotpaths
  - ch04-data-benchmarks-and-verification
current_chapter: ch03-ingress-and-worker-hotpaths
current_task: null
parent_flow: null
chapter_id: null
planning_status: complete
depends_on_chapters: []
replan_trigger: null
plan_revision: 2
plan_commit: "86527cf1"
last_verified_checkpoint: "9df526b0"
last_reviewed_chapter: ch02-embedded-bridge-and-batching
last_reviewed_commit: "9df526b0"
generated:
  by: flow/0.26.0
  at: "2026-11-27T01:36:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T01:58:43Z"
    commit: "86527cf1"
sources:
  - id: src-wsgi-runner
    resource: src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch
    title: Native Rust WSGI batch runner currently executing requests serially
  - id: src-wsgi-environ
    resource: src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron
    title: Lazy PEP 3333 WSGI environ mapping
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker inline event loop and batch dispatcher
  - id: src-server-dispatch
    resource: src/py/vertebra/server/_dispatch.py::compile_handler_spec
    title: Handler compilation and batch partitioning
  - id: src-http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig::from_env
    title: Server configuration and Tokio thread budgeting
  - id: src-http-main
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: Server startup, topology selection, and RequestBatcher shard sizing
  - id: src-http-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::compute_embedded_worker_budgets
    title: EmbeddedSupervisor multi-process CPU budgeting
  - id: src-core-bridge
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: In-process BatchBridge queue, Dekker notifier, and response sink
  - id: src-http-bridge
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Embedded correlation table and dispatch_bridge_vec
  - id: src-http-server
    resource: src/rs/vertebra-http/src/server.rs::IdleTimeoutStream
    title: Hyper HTTP/1.1 connection loop, idle timeout wrapper, and collector routing
  - id: src-http-backpressure
    resource: src/rs/vertebra-http/src/backpressure.rs::BackpressureController
    title: Lock-free admission control and sliding-window circuit breaker
  - id: src-http-correlation
    resource: src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests
    title: Generation-tagged O(1) correlation slab
  - id: src-http-shm
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher
    title: Shared-memory batch dispatcher
  - id: src-py-transport
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: PyO3 batch response submission across SHM and BatchBridge
  - id: src-py-request
    resource: src/rs/vertebra-py/src/request.rs::Request
    title: Zero-copy Request wrapper and body getter
  - id: src-py-asgi
    resource: src/rs/vertebra-py/src/asgi.rs::drive_asgi_batch
    title: Native Rust ASGI 3.0 scope and batch driver
  - id: src-py-rsgi
    resource: src/rs/vertebra-py/src/rsgi/http.rs::drive_rsgi_batch
    title: Native Rust RSGI 1.6 protocol and batch driver
  - id: src-bench-backend
    resource: tools/benchmarks/apps/_data_backend.py::DataBackend
    title: Shared benchmark database, cache, and storage backend
  - id: src-bench-sql
    resource: tools/benchmarks/adapters/sql.py::capture_sqlspec_report
    title: Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: HTTP, data, framework, and Granian benchmark target definitions
tags:
  - performance
  - runtime
  - wsgi
  - embedded
  - http-ingress
  - benchmarks
  - postgres
  - sqlite
---

# Runtime Performance Remediation and Full-Stack Data Benchmarks

<!-- truth: start -->
- Replace serial `drive_wsgi_batch` with an adaptive inline-plus-Rust `WsgiExecutor` pool (`VERTEBRA_WSGI_MODE=auto|concurrent|inline`, `VERTEBRA_WSGI_THREADS`) that preserves ~220k RPS on zero-I/O routes while restoring concurrent blocking I/O (`io_10ms` and sync DB drivers) across GIL and free-threaded Python.
- Eliminate the 1.3×–2.5× 1-worker Embedded slowdown by defaulting GIL 1w Embedded to a 1-thread `current_thread` Tokio runtime and consumer-aligned `RequestBatcher` sharding (`shard_count = 1`), batch-locking `BridgeCorrelation` allocation and completion (`BatchResponseSink`), eliminating `path`/`query` copies in `InProcessRequest`, and removing double-copy response submission in `transport.rs`.
- Raise the ~235k RPS HTTP ingress ceiling and trim per-request Python/PyO3 overhead via lazy `IdleTimeoutStream`, per-connection collector shard affinity, zero-Arc `AdmissionPermit` + coarse circuit clock, batch-amortized `Instant::now()` in `InFlightRequests`, reusable `ShmDispatcher` scratch buffers, single-pass native mixed sync/async dispatch, direct `Request::body`, specialized 1-arg extractors, lazy `RsgiHttpProtocol` stream state, and eventfd-aware `_drain_readable`.
- Wire real SQLite and PostgreSQL backends across native drivers, SQLSpec, and Advanced Alchemy in `tools/benchmarks/` and verify end-to-end against Granian (`granian-asgi`, `granian-rsgi`), Uvicorn, and standalone SQL/ORM comparators.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Benchmark run `20261126-213834-f73052` (12-core Linux host, HTTP/1.1, `c=128`) exposed four concrete performance defects and one verification gap:

1. **WSGI blocking I/O is serialized**: `vtb-embedded-wsgi` hit `85 RPS` (`2,508 ms` P99) and `vtb-shm-wsgi-4w` hit `380 RPS` (`529 ms` P99) on `io_10ms` because `drive_wsgi_batch` executes batches serially on one thread, flushes only after the full batch completes, and clones three owned `String` metadata fields per request in `WsgiEnviron` [^src-wsgi-runner] [^src-wsgi-environ] [^src-server-worker].
2. **1-worker Embedded is 1.3×–2.5× slower than 1-worker SHM**: `vtb-embedded-asgi` (`73.7k` hello, `55.2k` echo), `vtb-embedded-rsgi` (`96.0k` hello), and `vtb-embedded-native` (`162.5k` hello, `102.3k` json, `68.2k` echo) trail `vtb-shm-asgi-1w` (`182.1k` / `135.2k`) and `vtb-shm-native-1w` (`212.9k` / `202.0k` / `174.1k`) due to 12 Tokio threads + 8 `RequestBatcher` shards feeding 1 Python worker, per-request `BridgeCorrelation` locks, per-response `ResponseSink` dispatch, and double-copied buffers [^src-http-config] [^src-http-main] [^src-http-bridge] [^src-core-bridge] [^src-py-transport].
3. **HTTP frontend ceiling (~235k) and 8-worker oversubscription**: Per-request Tokio timer-wheel resets (`IdleTimeoutStream`), per-request `BackpressureController` `Arc::clone`/`Instant::now()`/fence work, per-request `InFlightRequests` `Instant::now()`, global `next_shard` atomic contention, per-batch `ShmDispatcher` `Vec` allocations, and cgroup-blind `EmbeddedSupervisor` budgeting cap multi-worker scaling [^src-http-server] [^src-http-backpressure] [^src-http-correlation] [^src-http-shm] [^src-http-factory].
4. **Python and PyO3 per-request overhead**: Unconditional `clear_request_notification()` on `call_soon`, dropping out of `all_sync_native` whenever any `async def` route exists, generator + dict allocation in simple extractors, `Request::body` double-copy, and eager `RsgiHttpProtocol` `Arc` allocations depress `echo`, `json`, ASGI, and RSGI throughput [^src-server-worker] [^src-server-dispatch] [^src-py-request] [^src-py-asgi] [^src-py-rsgi].
5. **Missing live apples-to-apples Granian and database benchmarks**: `benchmarks.duckdb` lacks current `granian-asgi`/`granian-rsgi` baselines and has zero real SQLite/PostgreSQL runs; `DataBackend` lacks Postgres, native `vertebra-sqlite`, SQLSpec, and Advanced Alchemy wiring, and `adapters/sql.py` lacks an Advanced Alchemy comparator [^src-bench-backend] [^src-bench-sql] [^src-bench-targets].

Objectives:
- Restore concurrent blocking WSGI I/O (`>= 10,000 RPS` on `io_10ms`) without sacrificing zero-I/O WSGI speed (`>= 200k RPS` on `hello`).
- Bring 1-worker Embedded ASGI, RSGI, and Native throughput up to beat Granian 1w and approach or exceed 1-worker SHM across `hello`, `json`, `body_10k`, and `echo`.
- Raise peak 4w/8w loopback throughput and eliminate unnecessary per-request allocations across Rust ingress and PyO3 runners.
- Deliver repeatable SQLite and PostgreSQL benchmarks across native drivers, SQLSpec, and Advanced Alchemy in both HTTP full-stack and standalone comparator suites.

### 1.2 Upstream Execution Baseline

- Baseline commit: `86527cf1` (note: implementation must run on a dedicated feature branch off `main`, never committing directly to `main`).
- Existing benchmark reference: run `20261126-213834-f73052` in `tools/benchmarks/results/benchmarks.duckdb`.
- No active sibling specs exist under `.agents/bundles/specs/`.

### 1.3 Code Analysis & Interface Summary

| Area | Files Examined (`path::Symbol`) | Public Interface & Dependency Category | Affected Docs / OKF Chapters |
| :--- | :--- | :--- | :--- |
| WSGI runner | `src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch`, `environ.rs::WsgiEnviron`, `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` | In-process PyO3 FFI (`drive_wsgi_batch`, `_WsgiStartResponse`, `WsgiAdapter`) | `architecture/worker/asgi-and-wsgi-runners.md` |
| Embedded runtime & bridge | `src/rs/vertebra-http/src/config.rs::ServerConfig`, `main.rs::run_server`, `workers/factory.rs::EmbeddedSupervisor`, `dispatcher/bridge.rs::BridgeCorrelation`, `src/rs/vertebra-core/src/bridge.rs::BatchBridge`, `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` | In-process Rust transport & PyO3 FFI (`BatchBridge`, `InProcessRequest`, `BatchResponseSink`, `ServerConfig`) | `architecture/runtime/topologies-and-embedding.md`, `architecture/ipc/tiered-rings-and-batch-bridge.md`, `patterns/rust/lock-free-shm-and-sharding.md` |
| HTTP ingress & SHM dispatch | `src/rs/vertebra-http/src/server.rs::IdleTimeoutStream`, `response.rs::dispatch_request_inner`, `backpressure.rs::BackpressureController`, `batcher.rs::RequestBatcher`, `dispatcher/correlation.rs::InFlightRequests`, `dispatcher/shm.rs::ShmDispatcher` | In-process Rust HTTP ingress (`ServerState`, `BackpressureController`, `Dispatcher`) | `architecture/http/ingress-and-routing.md`, `architecture/http/batching-dispatch-and-backpressure.md` |
| Worker & PyO3 drivers | `src/py/vertebra/server/worker.py::Worker`, `_dispatch.py::compile_handler_spec`, `src/rs/vertebra-py/src/request.rs::Request`, `asgi.rs::drive_asgi_batch`, `rsgi/http.rs::drive_rsgi_batch`, `rsgi/scope.rs::RsgiScope` | In-process Python worker & PyO3 FFI (`Request`, `AsgiScope`, `RsgiScope`, `RsgiHttpProtocol`) | `architecture/worker/pyo3-ffi-and-batch-dispatch.md`, `patterns/python/hot-path-and-async-dispatch.md` |
| Benchmark harness & comparators | `tools/benchmarks/apps/_data_backend.py::DataBackend`, `tools/benchmarks/adapters/sql.py`, `tools/benchmarks/targets.py::ALL_TARGETS`, `tools/benchmarks/fixtures.py::FIXTURE_SPECS` | Local test double (`memory`/`sqlite`) + Remote-owned Docker fixture (`postgres:17-alpine`) | `testing/benchmarks-and-free-threaded.md` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge chapters: `architecture/runtime/topologies-and-embedding.md`, `architecture/ipc/tiered-rings-and-batch-bridge.md`, `architecture/http/ingress-and-routing.md`, `architecture/http/batching-dispatch-and-backpressure.md`, `architecture/worker/pyo3-ffi-and-batch-dispatch.md`, `architecture/worker/asgi-and-wsgi-runners.md`, `patterns/python/hot-path-and-async-dispatch.md`, `patterns/rust/lock-free-shm-and-sharding.md`, `testing/benchmarks-and-free-threaded.md`.
- Project skills: `runtime-ipc`, `http-ingress`, `pyo3-bridge`, `python-framework`, `review-performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R1 (Adaptive WSGI concurrency)**: `drive_wsgi_batch` must execute zero-I/O WSGI batches inline on GIL builds for peak throughput and automatically hand off stalled (`> 250 µs` configurable) and subsequent batches to a native Rust OS thread pool (`WsgiExecutor`) submitting each response immediately upon completion, with `VERTEBRA_WSGI_MODE=auto|concurrent|inline` and `VERTEBRA_WSGI_THREADS` overrides and default `concurrent` mode on `Py_GIL_DISABLED`.
- **R2 (Zero-allocation WSGI batch metadata)**: Share batch-level `(root_path, server_name, server_port, url_scheme)` via `Arc<WsgiServerConfig>` across `WsgiEnviron` instances created in `drive_wsgi_batch` so per-request `String` cloning is eliminated.
- **R3 (Embedded 1w thread & shard right-sizing)**: Default GIL 1w Embedded (when `VERTEBRA_TOKIO_THREADS` is unset) to `worker_threads = 1` (`Builder::new_current_thread`) and size `RequestBatcher` `shard_count` to active Python consumer count (`1` for GIL Embedded, `py_threads.clamp(1, 8)` for FT Embedded, `workers.count.clamp(1, 8)` for SHM). Align `EmbeddedSupervisor` CPU detection with cgroup `cpu.max`.
- **R4 (Batch-locked bridge correlation & zero-copy bridge handoff)**: Batch-allocate correlation IDs under one shard `Mutex` in `BridgeCorrelation::dispatch_bridge_vec`, store `http::uri::PathAndQuery` directly in `InProcessRequest` without copying `path`/`query` slices, register a `BatchResponseSink` on `BatchBridge` calling a single-shard fast-path `BridgeCorrelation::complete_many`, and transfer owned `headers_raw`/`body` `Vec<u8>` via `Bytes::from` in `ResponseSubmitter::submit_batch_flushed`.
- **R5 (Rust HTTP ingress overhead reduction)**:
  - Replace per-I/O timer resets in `IdleTimeoutStream` with an activity flag checked on timer fire.
  - Replace per-request global `next_shard.fetch_add(1)` with per-connection shard assignment.
  - Eliminate per-request `Arc::clone` and redundant `mfence` in `BackpressureController` (`AdmissionPermit` + `fetch_sub(1, SeqCst)` + amortized coarse circuit-breaker timestamp), and gate `CONTENT_LENGTH`/`CONTENT_ENCODING` header scans behind non-empty request body checks.
  - Amortize `Instant::now()` once per batch in `InFlightRequests::insert_with_now`, reuse `RequestBatcher`-owned scratch buffers in `ShmDispatcher::dispatch_shm_vec_from_shard`, and add a single-shard fast-path in `ShmDispatcher::complete_many`.
- **R6 (Python & PyO3 hot-path trimming)**:
  - Separate `_on_notify_readable` from `_drain_batches` in `Worker._run_inline_event_loop` with a 1-turn grace check before `prepare_request_sleep`.
  - Add a single-pass `native_only` mixed sync/async fast path in `Worker._process_batch` avoiding `partition_batch` + `dispatch_batch` overhead for sync requests on apps containing async routes.
  - Construct `PyBytes::new(py, self.batch.body(self.index))` directly in `Request::body` without populating `OnceLock<Bytes>`.
  - Specialize 1-arg simple extractors (`body`, `headers`, `method`, `path`, `query`) in `compile_handler_spec` without generator expressions or empty dict allocations.
  - Make `RsgiHttpProtocol` stream state lazy (eliminating 2 `Arc` allocations per unary request), share batch-level `RsgiScope` metadata, and add populated-slot bitmask drop + shared `Arc<str>` `root_path` in `AsgiScope`.
- **R7 (Full-stack & standalone SQLite + PostgreSQL benchmarks with SQLSpec & Advanced Alchemy)**:
  - Extend `tools/benchmarks/apps/_data_backend.py` to support `db_backend` in `("memory", "sqlite", "postgres")` and `db_adapter` in `("native", "sqlspec", "advanced_alchemy", "stdlib")`, using real `vertebra-sqlite` / `vertebra-postgres` (including SHM broker pooling), `vertebra.extensions.sqlspec`, and `advanced_alchemy` (`+vtb` / `+vtb_async`) plus vanilla baselines.
  - Add `capture_advanced_alchemy_report` in `tools/benchmarks/adapters/sql.py` covering SQLite and PostgreSQL sync/async repositories against vanilla SQLAlchemy/Advanced Alchemy comparators, and register HTTP data targets in `tools/benchmarks/targets.py`.
  - Execute the live benchmark verification gate (Granian vs Vertebra HTTP/1.1 `default`, Embedded 1w Tokio 1 vs 2 A/B check, SQLite + PostgreSQL HTTP `data` + standalone SQL/SQLSpec/Advanced Alchemy comparators).

### 2.2 Shared Concrete Contracts

```rust
// src/rs/vertebra-core/src/bridge.rs
pub type BatchResponseSink = Arc<dyn Fn(Vec<BridgeResponse>) + Send + Sync>;

impl BatchBridge {
    pub fn set_batch_response_sink(&self, sink: BatchResponseSink);
    pub fn clear_batch_response_sink(&self);
}

// src/rs/vertebra-core/src/transport/mod.rs
enum InProcessPathQuery {
    Uri(http::uri::PathAndQuery),
    Bytes { path: bytes::Bytes, query: bytes::Bytes },
}

pub struct InProcessRequest {
    pub node: TransportNode,
    path_query: InProcessPathQuery,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
    pub path_params_raw: bytes::Bytes,
}

impl InProcessRequest {
    pub fn from_path_and_query(
        method_tag: u8,
        http_version_tag: u8,
        handler_id: u64,
        correlation_id: u64,
        path_and_query: http::uri::PathAndQuery,
        headers_raw: bytes::Bytes,
        body: bytes::Bytes,
        path_params_raw: bytes::Bytes,
    ) -> Self;

    pub fn path_bytes(&self) -> &[u8];
    pub fn path_str(&self) -> &str;
    pub fn query_bytes(&self) -> Option<&[u8]>;
    pub fn query_str(&self) -> Option<&str>;
}

// src/rs/vertebra-http/src/dispatcher/correlation.rs
impl InFlightRequests {
    pub fn insert_with_now(
        &mut self,
        shard: u8,
        reply: tokio::sync::oneshot::Sender<PendingResponse>,
        worker_id: u32,
        now: std::time::Instant,
    ) -> Option<CorrelationId>;
}

// src/rs/vertebra-py/src/wsgi/pool.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsgiExecutionMode {
    Auto,
    Concurrent,
    Inline,
}

pub struct WsgiServerConfig {
    pub root_path: String,
    pub server_name: String,
    pub server_port: u16,
    pub url_scheme: String,
}
```

```python
# tools/benchmarks/apps/_data_backend.py
class DataBackendConfig(msgspec.Struct, frozen=True, gc=False):
    db_backend: str = "memory"        # "memory" | "sqlite" | "postgres"
    db_adapter: str = "native"        # "native" | "sqlspec" | "advanced_alchemy" | "stdlib"
    db_adapter_flavor: str = "vertebra"  # "vertebra" | "upstream"
    db_dsn: str = ""
    # ... existing fields preserved unchanged
# Note: tools/benchmarks/targets.py::apply_shm_broker_dsn guards ?shm_broker=true to PostgreSQL DSNs only.
```

```python
# tools/benchmarks/adapters/sql.py
async def capture_advanced_alchemy_report(
    engine: Literal["sqlite", "postgres"],
    dsn: str,
    *,
    iterations: int = 100,
    warmup: int = 10,
    include_external: bool = True,
    strict_free_threading: bool = False,
) -> AdapterBenchmarkReport: ...
```

### 2.3 Caller -> Interface -> Collaborator Map

- `Worker._process_batch` -> `drive_wsgi_batch` -> `WsgiExecutor` (inline fastpath or Rust OS thread pool) -> `ResponseSubmitter::{submit_batch_flushed, submit_raw_inner}`.
- `serve_plain` -> `IdleTimeoutStream` -> `handle_with_peer` (connection-pinned collector shard) -> `BackpressureController::try_acquire_for_path` -> `RequestBatcher` (with owned `ShmDispatchScratch`) -> `Dispatcher::{BridgeCorrelation, ShmDispatcher}` -> `BatchBridge` / `RingPair`.
- `Worker._run_inline_event_loop` -> `_on_notify_readable` / `_drain_batches` -> `Worker._process_batch` (`native_only` / `drive_asgi_batch` / `drive_rsgi_batch` / `drive_wsgi_batch`) -> `ResponseSubmitter::submit_batch_flushed` -> `BatchBridge::submit_responses` -> `BatchResponseSink` -> `BridgeCorrelation::complete_many`.
- `tools.benchmarks.cli` -> `DataBackend(db_backend, db_adapter)` -> `{Sync,Async}{SQLite,Postgres}Driver` / `vertebra.extensions.sqlspec` / `advanced_alchemy` (`+vtb`) + `tools.benchmarks.adapters.sql::{capture_sqlite_report, capture_postgres_adapter_report, capture_sqlspec_report, capture_advanced_alchemy_report}`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| WSGI app raises exception or violates PEP 3333 in concurrent pool mode | Log exception via `log_app_exception`, emit HTTP `500` immediately via `submit_raw_inner`, never stall worker thread or leak `WsgiEnviron`. |
| WSGI blocking call stalls item 0 of a batch while items `1..N` wait | Watchdog detects stall past threshold (`250 µs`), wakes pool threads to drain remaining items `1..N` in parallel without waiting for item 0. |
| Worker shutdown while WSGI pool has in-flight tasks | Drain in-flight WSGI tasks cleanly on shutdown without panicking or holding GIL across thread join (`py.detach`). |
| `VERTEBRA_TOKIO_THREADS` explicitly set by user on Embedded 1w | Always honor explicit `VERTEBRA_TOKIO_THREADS`; default clamping applies only when unset. |
| Multi-shard responses in `complete_many` (when shard 0 overflowed to shard 1) | Fast path checks if all IDs share `first_shard`; falls back cleanly to multi-shard grouping when mixed. |
| `IdleTimeoutStream` receives no traffic for `keepalive_timeout` | Activity flag remains false when timer fires -> returns `io::ErrorKind::TimedOut` with identical semantics to eager reset. |
| `BackpressureController` saturated with active waiters (`queue_depth > 0`) | `fetch_sub(1, SeqCst)` + `queue_depth.load(SeqCst)` preserves Dekker store-load guarantee so zero waiters are missed. |
| `postgres` benchmark run without Docker or DSN | Offline unit tests validate backend/adapter selection and SQLAlchemy/SQLSpec/Advanced Alchemy SQLite paths; live Postgres targets fail loud with actionable setup message when unreachable. |

### 2.5 Acceptance Scenarios

1. `vtb-embedded-wsgi` and `vtb-shm-wsgi-4w` achieve `>= 10,000 RPS` on `io_10ms` (from `85`/`380 RPS`) while maintaining `>= 200k RPS` on `hello`.
2. `vtb-embedded-asgi`, `vtb-embedded-rsgi`, and `vtb-embedded-native` (1w) close the embedded slowdown gap, beating `granian-asgi` / `granian-rsgi` on `default` scenarios with zero errors.
3. `vtb-embedded-native-4w` and `vtb-shm-native-4w` improve peak loopback throughput and P99 latency over `20261126-213834-f73052`.
4. Live SQLite and PostgreSQL HTTP (`data`, `data-baselines`, `frameworks`) and standalone (`sqlite`, `postgres`, `sqlspec`, `advanced_alchemy`) benchmark runs succeed and ingest into `benchmarks.duckdb` / JSON reports.
5. `make lint-all`, `make rs-test`, and targeted Python suites pass with zero regressions on both GIL and free-threaded invariants.

### 2.6 Alternatives Considered

- **Python `ThreadPoolExecutor` for WSGI**: Rejected for maximum performance because Python `Future`, queue locks, closure overhead, and `Response` allocations depress both zero-I/O and blocking-I/O throughput compared to a native Rust `WsgiExecutor`.
- **Always-concurrent WSGI pool without inline fastpath**: Rejected because acquiring/bouncing the GIL across OS threads on every 3 µs `/hello` request would regress zero-I/O WSGI throughput on GIL Python.
- **Keep 2-thread Tokio default for 1w GIL Embedded**: Deferred to an empirical A/B check in `ch04`; defaulting to 1 `current_thread` Tokio runtime matches `EmbeddedSupervisor`'s proven 244k RPS configuration and minimizes cross-thread wakeup latency.

## 3. Documentation & OKF Knowledge Plan

- `[0/6 files documented]`
  - `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` (`ch04:t07`) — Document adaptive `WsgiExecutor` (`VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`), shared `WsgiServerConfig`, lazy `RsgiHttpProtocol`, and `AsgiScope` bitmask drop.
  - `.agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md` (`ch04:t06`) — Document 1w GIL Embedded Tokio thread default, consumer-aligned `RequestBatcher` sharding, and cgroup-aware `EmbeddedSupervisor`.
  - `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` (`ch04:t06`) — Document `BatchResponseSink`, `InProcessRequest` `PathAndQuery`, and `submit_batch_flushed`.
  - `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md` (`ch04:t06`) — Document lazy `IdleTimeoutStream`, connection-pinned collector shards, single-lock `BridgeCorrelation` batch registration/completion, `BackpressureController` fastpath, and reusable `ShmDispatcher` scratch buffers.
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md` (`ch04:t07`) — Document split `_on_notify_readable`/`_drain_batches`, `native_only` mixed sync/async fast path, direct `Request::body`, and specialized 1-arg simple extractors.
  - `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`ch04:t07`) — Document SQLite/PostgreSQL native, SQLSpec, and Advanced Alchemy full-stack and standalone comparator benchmarks.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Chapter | Verification Strategy |
| :--- | :--- | :--- |
| R1, R2 | `ch01-wsgi-adaptive-pool` | `regression_tdd`, `behavior_tdd` |
| R3, R4 | `ch02-embedded-bridge-and-batching` | `behavior_tdd`, `regression_tdd` |
| R5, R6 | `ch03-ingress-and-worker-hotpaths` | `behavior_tdd`, `regression_tdd` |
| R7 | `ch04-data-benchmarks-and-verification` | `integration_acceptance`, `documentation_validation` |

## 5. Implementation Plan

- [x] [6768cf7c] ch01-wsgi-adaptive-pool — Adaptive Rust `WsgiExecutor` pool and zero-allocation `WsgiServerConfig` (chapters/ch01-wsgi-adaptive-pool/spec.md)
- [x] [9df526b0] ch02-embedded-bridge-and-batching — Embedded 1w Tokio/shard right-sizing, batch-locked `BridgeCorrelation`, `BatchResponseSink`, and zero-copy bridge handoff (chapters/ch02-embedded-bridge-and-batching/spec.md)
- [~] ch03-ingress-and-worker-hotpaths — Lazy `IdleTimeoutStream`, connection-pinned shards, `BackpressureController`/`InFlightRequests`/`ShmDispatcher` hot paths, and Python/PyO3 worker/ASGI/RSGI/native optimizations (chapters/ch03-ingress-and-worker-hotpaths/spec.md)
- [ ] ch04-data-benchmarks-and-verification — SQLite and PostgreSQL (native, SQLSpec, Advanced Alchemy) benchmark wiring, Granian/DB empirical verification gate, and OKF doc sync (chapters/ch04-data-benchmarks-and-verification/spec.md)

### Chapter / Phase Recovery Checkpoint

Each chapter runs a `< 60s` targeted Rust + Python unit test and lint gate (`cargo test -p <crate> <filter>`, `make dev-ext` or `make dev` for touched extensions, targeted `uv run --no-sync pytest ...`, and `make lint-rs` / `ruff`). Full `make build`, Granian comparison, and SQLite/PostgreSQL benchmark matrices execute in `ch04-data-benchmarks-and-verification`.

## 6. Current State & Remaining Plan Snapshot

- **Active Chapter**: `ch03-ingress-and-worker-hotpaths` (`in_progress`; `ch01-wsgi-adaptive-pool` reviewed at `6768cf7c`, `ch02-embedded-bridge-and-batching` reviewed at `9df526b0`).
- **Remaining Chapters**: `ch03-ingress-and-worker-hotpaths`, `ch04-data-benchmarks-and-verification`.
- **Empirical Triggers**: `ch04-data-benchmarks-and-verification` is marked `empirical_dependent` to incorporate A/B measurements for Embedded 1w Tokio thread count (`1` vs `2`), `WsgiExecutor` stall threshold tuning, and live SQLite/PostgreSQL/SQLSpec/Advanced Alchemy profiles.

[^src-wsgi-runner]: `src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch` — Native Rust WSGI batch runner currently executing requests serially.
[^src-wsgi-environ]: `src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron` — Lazy PEP 3333 WSGI environ mapping.
[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker` — Worker inline event loop and batch dispatcher.
[^src-server-dispatch]: `src/py/vertebra/server/_dispatch.py::compile_handler_spec` — Handler compilation and batch partitioning.
[^src-http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig::from_env` — Server configuration and Tokio thread budgeting.
[^src-http-main]: `src/rs/vertebra-http/src/main.rs::run_server` — Server startup, topology selection, and RequestBatcher shard sizing.
[^src-http-factory]: `src/rs/vertebra-http/src/workers/factory.rs::compute_embedded_worker_budgets` — EmbeddedSupervisor multi-process CPU budgeting.
[^src-core-bridge]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process BatchBridge queue, Dekker notifier, and response sink.
[^src-http-bridge]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Embedded correlation table and dispatch_bridge_vec.
[^src-http-server]: `src/rs/vertebra-http/src/server.rs::IdleTimeoutStream` — Hyper HTTP/1.1 connection loop, idle timeout wrapper, and collector routing.
[^src-http-backpressure]: `src/rs/vertebra-http/src/backpressure.rs::BackpressureController` — Lock-free admission control and sliding-window circuit breaker.
[^src-http-correlation]: `src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests` — Generation-tagged O(1) correlation slab.
[^src-http-shm]: `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher` — Shared-memory batch dispatcher.
[^src-py-transport]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 batch response submission across SHM and BatchBridge.
[^src-py-request]: `src/rs/vertebra-py/src/request.rs::Request` — Zero-copy Request wrapper and body getter.
[^src-py-asgi]: `src/rs/vertebra-py/src/asgi.rs::drive_asgi_batch` — Native Rust ASGI 3.0 scope and batch driver.
[^src-py-rsgi]: `src/rs/vertebra-py/src/rsgi/http.rs::drive_rsgi_batch` — Native Rust RSGI 1.6 protocol and batch driver.
[^src-bench-backend]: `tools/benchmarks/apps/_data_backend.py::DataBackend` — Shared benchmark database, cache, and storage backend.
[^src-bench-sql]: `tools/benchmarks/adapters/sql.py::capture_sqlspec_report` — Standalone SQL, SQLAlchemy, and SQLSpec adapter comparator suite.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — HTTP, data, framework, and Granian benchmark target definitions.
