---
type: Spec
parent_flow: runtime-perf-remediation
chapter_id: ch03-ingress-and-worker-hotpaths
title: HTTP Ingress, Backpressure, SHM Batching, and Worker Hot Paths
description: Eliminate per-request timer resets, atomic shard contention, BackpressureController allocations and fences, per-batch SHM scratch allocations, and Python/PyO3 request/scope/eventfd overhead.
status: stable
state: completed
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters:
  - ch02-embedded-bridge-and-batching
replan_trigger: null
plan_revision: 2
plan_commit: "86527cf1"
current_task: null
last_verified_checkpoint: "cf919307"
last_reviewed_commit: "cf919307"
generated:
  by: flow/0.26.0
  at: "2026-11-27T01:40:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T01:58:43Z"
    commit: "86527cf1"
sources:
  - id: src-http-server
    resource: src/rs/vertebra-http/src/server.rs::IdleTimeoutStream
    title: Hyper HTTP/1.1 connection loop, idle timeout wrapper, and collector shard routing
  - id: src-http-response
    resource: src/rs/vertebra-http/src/response.rs::dispatch_request_inner
    title: Request header encoding, body collection gate, and shard dispatch entry point
  - id: src-http-backpressure
    resource: src/rs/vertebra-http/src/backpressure.rs::BackpressureController
    title: Lock-free admission permits, Dekker queue wakeups, and sliding-window circuit breaker
  - id: src-http-correlation
    resource: src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests
    title: Generation-tagged O(1) correlation slab
  - id: src-http-batcher
    resource: src/rs/vertebra-http/src/batcher.rs::RequestBatcher
    title: Opportunistic per-shard request batching task
  - id: src-http-shm
    resource: src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher
    title: Shared-memory batch serialization and sharded response completion
  - id: src-py-request
    resource: src/rs/vertebra-py/src/request.rs::Request
    title: Zero-copy Request proxy and body getter
  - id: src-py-asgi
    resource: src/rs/vertebra-py/src/asgi.rs::AsgiScope
    title: Lazy 24-slot ASGI 3.0 scope and inline batch driver
  - id: src-py-rsgi-http
    resource: src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol
    title: RSGI v1.6 HTTP protocol handler and inline batch driver
  - id: src-py-rsgi-scope
    resource: src/rs/vertebra-py/src/rsgi/scope.rs::RsgiScope
    title: Lazy RSGI v1.6 scope backed by BatchSnapshot
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker
    title: Worker inline event loop drain cycle and batch dispatch fast paths
  - id: src-server-dispatch
    resource: src/py/vertebra/server/_dispatch.py::compile_handler_spec
    title: Pre-compiled native handler extractors and invoker closures
tags:
  - performance
  - http-ingress
  - backpressure
  - shm-dispatch
  - pyo3
  - python-worker
---

# HTTP Ingress, Backpressure, SHM Batching, and Worker Hot Paths

<!-- truth: start -->
- Replace per-read/per-write `Sleep::reset` in `IdleTimeoutStream` with a lazy `activity: bool` flag, pin each accepted connection to one collector shard, gate `CONTENT_LENGTH`/`CONTENT_ENCODING` lookups on non-empty bodies, and single-pass `encode_header_map_compact`.
- Borrow `&BackpressureController` inside `AdmissionPermit<'a>` (zero per-request `Arc::clone`), replace `AcqRel` + `fence(SeqCst)` with `fetch_sub(1, SeqCst)` + `queue_depth.load(SeqCst)`, and amortize `elapsed_ms()` every 64 closed-circuit `record_success()` calls.
- Add `InFlightRequests::insert_with_now` called once per batch in both `ShmDispatcher` and `BridgeCorrelation`, own reusable `ShmDispatchScratch` inside each `RequestBatcher` task, and add a single-shard fast path in `ShmDispatcher::complete_many`.
- Construct `PyBytes` directly from `BatchSnapshot` in `Request::body`, share batch-level `Arc<str>` `root_path` and bitmask-tracked `[MaybeUninit<Py<PyAny>>; 24]` drop in `AsgiScope`, make `RsgiHttpProtocol` streaming state lazy with shared `Arc<RsgiServerMeta>` in `RsgiScope`.
- Split `_on_notify_readable` from `_drain_batches` with a 1-turn grace yield in `Worker._run_inline_event_loop`, add a single-pass `native_only` mixed sync/async loop in `Worker._process_batch`, and specialize 1-arg simple extractors in `compile_handler_spec`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

This chapter implements root requirements `R5` (Rust HTTP ingress overhead reduction) and `R6` (Python and PyO3 hot-path trimming) after `ch02-embedded-bridge-and-batching` establishes consumer-aligned sharding and batch-locked bridge correlation.

Current bottlenecks confirmed directly in source:

1. **Per-request HTTP/1.1 connection overhead**: `IdleTimeoutStream` resets its Tokio `Sleep` timer on every non-empty read and write, `ServerState::get_collector` increments a global `next_shard` atomic per request, `dispatch_request_inner` scans `CONTENT_LENGTH`/`CONTENT_ENCODING` before skipping empty bodies, and `encode_header_map_compact` iterates `HeaderMap` twice [^src-http-server] [^src-http-response].
2. **Per-request backpressure and batching overhead**: `AdmissionPermit` performs two `Arc::clone`s on acquire, two `Arc::drop`s and an extra `mfence` on release, and `record_success` executes `clock_gettime` per request; `InFlightRequests::insert` calls `Instant::now()` per request; `ShmDispatcher` allocates three scratch `Vec`s per batch and an 8-bucket array in `complete_many` [^src-http-backpressure] [^src-http-correlation] [^src-http-batcher] [^src-http-shm].
3. **Per-request PyO3 and Python worker overhead**: `Request::body` double-copies through `OnceLock<Bytes>`, `AsgiScope` clones `root_path` per request and scans 24 `Option` slots on drop, `RsgiHttpProtocol` eagerly allocates two `Arc`s per unary request, `RsgiScope` rebuilds server metadata per request, `_drain_readable` runs `clear_request_notification()` on every `call_soon`, any async route disables the inline native loop, and 1-arg simple extractors allocate generator expressions and empty dicts [^src-py-request] [^src-py-asgi] [^src-py-rsgi-http] [^src-py-rsgi-scope] [^src-server-worker] [^src-server-dispatch].

Objectives:

- Eliminate per-request timer-wheel mutations, global shard-counter contention, double header scans, and redundant `Arc`/fence/clock overhead on the Rust HTTP ingress path.
- Eliminate per-batch heap `Vec` allocations and redundant multi-shard bucketing in `RequestBatcher` and `ShmDispatcher`.
- Eliminate double body copies, eager unary streaming allocations, 24-slot ASGI drop scans, redundant `eventfd` reads, mixed-app batch partitioning, and simple-extractor closure allocations in `vertebra-py` and `vertebra.server`.

### 1.2 Upstream Execution Baseline

- Parent flow: `runtime-perf-remediation` at baseline commit `86527cf1`.
- Dependency: `ch02-embedded-bridge-and-batching` completes batch-locked `BridgeCorrelation::dispatch_bridge_vec` before `t03` switches its per-request `insert` call to `insert_with_now`.

### 1.3 Code Analysis & Interface Summary

| Subsystem | Scoped Files (`path::Symbol`) | Interface Category |
| :--- | :--- | :--- |
| HTTP/1.1 connection & ingress | `src/rs/vertebra-http/src/server.rs::IdleTimeoutStream`, `ServerState`, `serve_plain`, `serve_tls`; `src/rs/vertebra-http/src/response.rs::handle_with_peer_shard`, `dispatch_request_inner`, `encode_header_map_compact` | Internal Rust ingress (`handle`, `handle_with_peer`, `handle_buffered` remain compatible) |
| Backpressure admission & clock | `src/rs/vertebra-http/src/backpressure.rs::AdmissionPermit`, `BackpressureController` | Internal Rust ingress (`try_acquire`, `try_acquire_for_path`, `record_success`) |
| Correlation & SHM batching | `src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests`; `dispatcher/bridge.rs::BridgeCorrelation`; `batcher.rs::RequestBatcher`; `dispatcher/shm.rs::ShmDispatcher`, `ShmDispatchScratch`; `dispatcher/mod.rs::Dispatcher` | Internal Rust dispatch (`dispatch_shm_vec`, `complete_many`, `insert`) |
| PyO3 request & ASGI scope | `src/rs/vertebra-py/src/request.rs::Request`; `src/rs/vertebra-py/src/asgi.rs::AsgiScope`, `drive_asgi_targets_inline` | PyO3 FFI (`Request.body`, `AsgiScope`, `drive_asgi_batch`) |
| PyO3 RSGI protocol & scope | `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol`, `RsgiTransport`; `src/rs/vertebra-py/src/rsgi/scope.rs::RsgiScope`, `RsgiServerMeta` | PyO3 FFI (`RsgiHttpProtocol`, `RsgiScope`, `drive_rsgi_batch`) |
| Python worker & extractors | `src/py/vertebra/server/worker.py::Worker`; `src/py/vertebra/server/_dispatch.py::compile_handler_spec` | Internal Python runtime (`Worker._run_inline_event_loop`, `Worker._process_batch`, `HandlerSpec`) |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R5.1 (Lazy `IdleTimeoutStream` & connection-pinned collector shards)**: `IdleTimeoutStream` sets `activity = true` on non-empty reads/writes and only calls `Sleep::reset` when the timer fires with `activity == true` (re-polling `Sleep` to register the waker); idle connections with `activity == false` return `io::ErrorKind::TimedOut`. `serve_plain` and `serve_tls` select one collector shard index once per accepted connection and pass it via `handle_with_peer_shard`.
- **R5.2 (Empty-body header gate & single-pass compact headers)**: `dispatch_request_inner` skips `CONTENT_LENGTH` and `CONTENT_ENCODING` header lookups whenever `RequestBodySource::Incoming` reports `size_hint().exact() == Some(0)`. `encode_header_map_compact` encodes `HeaderMap` in one pass using a pre-sized `Vec` (`headers.len().saturating_mul(32)`).
- **R5.3 (Zero-Arc `AdmissionPermit`, fence-free Dekker drop, coarse circuit clock)**: `AdmissionPermit<'a>` borrows `&'a BackpressureController` with an `is_priority: bool` flag. Unary drop runs `in_flight.fetch_sub(1, Ordering::SeqCst)` followed by `queue_depth.load(Ordering::SeqCst)` (paired with `queue_depth.fetch_add(1, Ordering::SeqCst)` on the queued path). Closed-circuit `record_success()` uses a 64-call per-thread strided `coarse_elapsed_ms()` backed by `cached_elapsed_ms: AtomicU64` + `clock_offset_ms`, while `record_failure()` and non-closed states use exact `elapsed_ms()`.
- **R5.4 (Batch-amortized correlation timestamps & reusable SHM scratch)**: `InFlightRequests::insert_with_now` accepts a batch-level `Instant` called once per batch in both `ShmDispatcher` and `BridgeCorrelation`. Each `RequestBatcher` task owns a `ShmDispatchScratch` (`staged`, `nodes`, `components`, `rejected_replies`) passed through `Dispatcher::dispatch_requests_from_shard_with_scratch` to `ShmDispatcher::dispatch_shm_vec_from_shard_with_scratch`. `ShmDispatcher::complete_many` takes a single-shard lock fast path when all response correlation IDs map to one shard.
- **R6.1 (Direct `Request::body` & bitmask `AsgiScope`)**: `Request::body` builds `PyBytes::new(py, self.batch.body(self.index))` directly without populating `OnceLock<Bytes>`. `AsgiScope` shares `Arc<str>` for `root_path` across a batch and stores cached known keys in `[MaybeUninit<Py<PyAny>>; 24]` guarded by `populated_mask: u32`, dropping only populated slots via `trailing_zeros()`.
- **R6.2 (Lazy RSGI stream state & shared `RsgiServerMeta`)**: `RsgiScope` stores batch-invariant metadata in `Arc<RsgiServerMeta>` built once per batch in `drive_rsgi_targets_inline`. `RsgiHttpProtocol` keeps unary `HttpProtocolState` inline with zero eager `Arc` allocations, allocating `Arc<RsgiStreamState>` (and `Option<Arc<Mutex<Vec<u8>>>>` for non-direct buffered streams) lazily in `response_stream()`.
- **R6.3 (Eventfd-split drain, `native_only` mixed dispatch, 1-arg extractors)**: `Worker._run_inline_event_loop` splits `_on_notify_readable` (calls `clear_request_notification()`) from `_drain_batches` (no `read(eventfd)` syscall) with a 1-turn grace yield before `prepare_request_sleep()`. `Worker._process_batch` executes a single-pass `native_only` (`not has_mounts and not has_sync_thread`) loop that handles sync routes inline and routes async routes to `drive_native_batch` / `_schedule_coros` without `partition_batch`. `compile_handler_spec` specializes 1-arg simple extractors (`body`, `headers`, `method`, `path`, `query`) into direct closures without generator expressions or `{}` dicts.

### 2.2 Concrete Contracts

```rust
// src/rs/vertebra-http/src/server.rs
impl ServerState {
    #[inline]
    pub fn next_collector_shard(&self) -> usize;

    #[inline]
    pub fn collector_for_shard(&self, shard_idx: usize) -> &tokio::sync::mpsc::Sender<PendingRequest>;
}

// src/rs/vertebra-http/src/response.rs
pub async fn handle_with_peer_shard(
    mut req: Request<Incoming>,
    peer: SocketAddr,
    collector_shard: usize,
    state: Arc<ServerState>,
) -> Result<HttpResponse, Infallible>;

// src/rs/vertebra-http/src/backpressure.rs
#[derive(Debug)]
pub struct AdmissionPermit<'a> {
    controller: &'a BackpressureController,
    is_priority: bool,
}

impl BackpressureController {
    #[inline]
    pub async fn try_acquire(&self) -> Result<AdmissionPermit<'_>, RejectionReason>;
    #[inline]
    pub async fn try_acquire_for_path(&self, path: &str) -> Result<AdmissionPermit<'_>, RejectionReason>;
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

// src/rs/vertebra-http/src/dispatcher/shm.rs & dispatcher/mod.rs
#[derive(Default)]
pub struct ShmDispatchScratch {
    staged: Vec<StagedShmRequest>,
    nodes: Vec<TransportNode>,
    components: Vec<&'static [u8]>,
    rejected_replies: Vec<tokio::sync::oneshot::Sender<PendingResponse>>,
}

impl ShmDispatcher {
    pub fn dispatch_shm_vec_from_shard_with_scratch(
        &self,
        batcher_shard: Option<usize>,
        requests: &mut Vec<crate::batcher::PendingRequest>,
        scratch: &mut ShmDispatchScratch,
    ) -> usize;
}

impl Dispatcher {
    pub fn dispatch_requests_from_shard_with_scratch(
        &self,
        batcher_shard: Option<usize>,
        requests: &mut Vec<crate::batcher::PendingRequest>,
        scratch: &mut ShmDispatchScratch,
    ) -> usize;
}
```

```rust
// src/rs/vertebra-py/src/asgi.rs
impl AsgiScope {
    pub(crate) fn new_with_shared_root(
        batch: Arc<BatchSnapshot>,
        index: usize,
        root_path: Arc<str>,
    ) -> Self;
}

// src/rs/vertebra-py/src/rsgi/scope.rs
pub(crate) struct RsgiServerMeta {
    pub(crate) proto_kind: ProtoKind,
    pub(crate) scheme_kind: SchemeKind,
    pub(crate) root_path: Box<str>,
    pub(crate) server_override: Option<Box<str>>,
    pub(crate) client_override: Option<Box<str>>,
}

impl RsgiScope {
    pub(crate) fn from_batch_with_meta(
        batch: &Arc<BatchSnapshot>,
        index: usize,
        meta: Arc<RsgiServerMeta>,
    ) -> Self;
}
```

```python
# src/py/vertebra/server/worker.py
# Worker._process_batch adds optional keyword `native_only: bool = False` preserving `all_sync_native: bool`.
```

### 2.3 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| `IdleTimeoutStream` fires after recent I/O (`activity == true`) | Clear `activity`, reset `Sleep` to `now + timeout`, and re-poll `Sleep` once to register the waker before returning `Poll::Pending`. |
| `IdleTimeoutStream` configured with `Duration::ZERO` | `deadline` stays `None`; reads and writes never arm or check a timer. |
| `BackpressureController` waiter races with permit drop | `queue_depth.fetch_add(1, SeqCst)` + `notified.enable()` + `fence(SeqCst)` pairs with `in_flight.fetch_sub(1, SeqCst)` + `queue_depth.load(SeqCst)` so zero waiters stall. |
| `advance_circuit_clock_for_test` called during unit tests | `coarse_elapsed_ms()` always adds `clock_offset_ms.load(Ordering::Acquire)`, and non-closed states / `record_failure()` read exact `elapsed_ms()`. |
| `ShmDispatcher::complete_many` receives responses spanning multiple shards | Fast path verifies all IDs match `first_shard`; mixed batches fall back to the 8-bucket grouped completion loop. |
| `AsgiScope` drops after overwriting or deleting a cached slot | `set_cached` / `clear_cached` clear the bit in `populated_mask` before `assume_init_drop()` so double-drop is impossible. |
| RSGI client disconnects before first `response_stream` chunk | `notify_disconnect` sets the disconnect flag and propagates into `RsgiStreamState`; `take_error_for_batch` and `finalize_async_error` suppress spurious HTTP `500`. |
| Inline event loop transitions from load to idle or shutdown | `_drain_batches` runs at most one grace turn after a non-empty drain, cancels duplicate handles on readable wakeups, checks `_should_stop` before sleeping, and arms `prepare_request_sleep()` when idle. |
| Mixed native batch includes sync, async, z-page, special return, or 404 items | `native_only` handles each item identically to `partition_batch` + `dispatch_batch` and flushes `raw_items` before returning. |

## 3. Documentation & OKF Knowledge Plan

- `[0/3 files documented]` (deferred to `ch04-data-benchmarks-and-verification` closeout sync after benchmarks validate final behavior):
  - `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md` (`ch04:t06`) — Lazy `IdleTimeoutStream`, connection-pinned collector shards, zero-Arc `AdmissionPermit`, coarse circuit clock, and `ShmDispatchScratch`.
  - `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` (`ch04:t07`) — Bitmask `AsgiScope` drop, shared `RsgiServerMeta`, and lazy `RsgiHttpProtocol` stream state.
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md` (`ch04:t07`) — Split `_on_notify_readable`/`_drain_batches`, `native_only` single-pass dispatch, direct `Request::body`, and 1-arg simple extractors.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R5.1, R5.2 | `runtime-perf-remediation:ch03:t01` | `behavior_tdd` |
| R5.3 | `runtime-perf-remediation:ch03:t02` | `regression_tdd` |
| R5.4 (correlation) | `runtime-perf-remediation:ch03:t03` | `behavior_tdd` |
| R5.4 (SHM scratch & complete_many) | `runtime-perf-remediation:ch03:t04` | `behavior_tdd` |
| R6.1 | `runtime-perf-remediation:ch03:t05` | `regression_tdd` |
| R6.2 | `runtime-perf-remediation:ch03:t06` | `regression_tdd` |
| R6.3 | `runtime-perf-remediation:ch03:t07` | `behavior_tdd` |

## 5. Implementation Plan

- [x] [6e72abac] runtime-perf-remediation:ch03:t01 — Lazy `IdleTimeoutStream`, connection-pinned collector shards, empty-body header gate, and single-pass header encoding (`tasks/t01-http1-idle-timeout-and-ingress-fastpath.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t02 — Borrowed `AdmissionPermit<'a>`, fence-free Dekker drop, and strided coarse circuit clock in `BackpressureController` (`tasks/t02-backpressure-zero-arc-and-coarse-clock.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t03 — `InFlightRequests::insert_with_now` and batch-level timestamp call in `BridgeCorrelation` (`tasks/t03-correlation-insert-with-now.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t04 — `RequestBatcher`-owned `ShmDispatchScratch`, batch-level `insert_with_now`, and single-shard `ShmDispatcher::complete_many` fast path (`tasks/t04-batcher-shm-scratch-and-complete-many.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t05 — Direct `Request::body` `PyBytes` construction and shared-root bitmask-drop `AsgiScope` (`tasks/t05-pyo3-request-body-and-asgi-scope.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t06 — Lazy `RsgiHttpProtocol` streaming state and batch-shared `Arc<RsgiServerMeta>` in `RsgiScope` (`tasks/t06-pyo3-rsgi-lazy-stream-and-shared-meta.md`)
- [x] [6e72abac] runtime-perf-remediation:ch03:t07 — Eventfd-split inline drain with grace yield, single-pass `native_only` mixed dispatch, and specialized 1-arg simple extractors (`tasks/t07-worker-eventfd-drain-mixed-native-and-extractors.md`)

### Fast Chapter Gate (`< 60s`)

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py
make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_hotpath_dispatch_and_freethreaded.py src/py/tests/unit/server/test_runtime_tuning.py
make lint-rs && uv run --no-sync ruff check src/py/vertebra/server/worker.py src/py/vertebra/server/_dispatch.py
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `completed` (`7/7` tasks completed across `6e72abac..cf919307`, reviewed by `code-reviewer` + `quality-reviewer`).
- **Remaining Tasks**: None.

[^src-http-server]: `src/rs/vertebra-http/src/server.rs::IdleTimeoutStream` — Hyper HTTP/1.1 connection loop, idle timeout wrapper, and collector shard routing.
[^src-http-response]: `src/rs/vertebra-http/src/response.rs::dispatch_request_inner` — Request header encoding, body collection gate, and shard dispatch entry point.
[^src-http-backpressure]: `src/rs/vertebra-http/src/backpressure.rs::BackpressureController` — Lock-free admission permits, Dekker queue wakeups, and sliding-window circuit breaker.
[^src-http-correlation]: `src/rs/vertebra-http/src/dispatcher/correlation.rs::InFlightRequests` — Generation-tagged O(1) correlation slab.
[^src-http-batcher]: `src/rs/vertebra-http/src/batcher.rs::RequestBatcher` — Opportunistic per-shard request batching task.
[^src-http-shm]: `src/rs/vertebra-http/src/dispatcher/shm.rs::ShmDispatcher` — Shared-memory batch serialization and sharded response completion.
[^src-py-request]: `src/rs/vertebra-py/src/request.rs::Request` — Zero-copy Request proxy and body getter.
[^src-py-asgi]: `src/rs/vertebra-py/src/asgi.rs::AsgiScope` — Lazy 24-slot ASGI 3.0 scope and inline batch driver.
[^src-py-rsgi-http]: `src/rs/vertebra-py/src/rsgi/http.rs::RsgiHttpProtocol` — RSGI v1.6 HTTP protocol handler and inline batch driver.
[^src-py-rsgi-scope]: `src/rs/vertebra-py/src/rsgi/scope.rs::RsgiScope` — Lazy RSGI v1.6 scope backed by BatchSnapshot.
[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker` — Worker inline event loop drain cycle and batch dispatch fast paths.
[^src-server-dispatch]: `src/py/vertebra/server/_dispatch.py::compile_handler_spec` — Pre-compiled native handler extractors and invoker closures.
