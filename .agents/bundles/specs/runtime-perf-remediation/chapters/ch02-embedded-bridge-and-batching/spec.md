---
type: Spec
parent_flow: runtime-perf-remediation
chapter_id: ch02-embedded-bridge-and-batching
title: Embedded Runtime Sizing, Batch-Locked Bridge Correlation, and Zero-Copy Handoff
description: Right-size 1-worker GIL Embedded Tokio threads and RequestBatcher shards, batch-lock BridgeCorrelation with BatchResponseSink, store PathAndQuery zero-copy in InProcessRequest, and transfer owned response buffers without double copies.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch01-wsgi-adaptive-pool
replan_trigger: null
planning_quality: standard
planning_degraded_reason: null
plan_revision: 2
plan_commit: "86527cf1"
current_task: null
last_verified_checkpoint: "9df526b0"
last_reviewed_commit: "9df526b0"
generated:
  by: flow/0.26.0
  at: "2026-11-27T01:38:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T01:58:43Z"
    commit: "86527cf1"
sources:
  - id: src-http-config
    resource: src/rs/vertebra-http/src/config.rs::ServerConfig::from_env
    title: Server configuration, Tokio thread defaults, and cgroup CPU detection
  - id: src-http-main
    resource: src/rs/vertebra-http/src/main.rs::run_server
    title: Tokio runtime builder, embedded Python worker spawn, and RequestBatcher shard sizing
  - id: src-http-factory
    resource: src/rs/vertebra-http/src/workers/factory.rs::EmbeddedSupervisor::spawn_one
    title: Multi-process SO_REUSEPORT embedded worker CPU budgeting
  - id: src-core-transport
    resource: src/rs/vertebra-core/src/transport/mod.rs::InProcessRequest
    title: Zero-copy in-process request container and BatchSnapshot accessors
  - id: src-core-bridge
    resource: src/rs/vertebra-core/src/bridge.rs::BatchBridge
    title: In-process BatchBridge queue and direct response sink dispatch
  - id: src-http-bridge
    resource: src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation
    title: Sharded embedded correlation table, dispatch_bridge_vec, and complete_many
  - id: src-http-dispatcher
    resource: src/rs/vertebra-http/src/dispatcher/mod.rs::Dispatcher::dispatch_requests_from_shard
    title: Dispatcher backend routing and shard affinity forwarding
  - id: src-py-transport
    resource: src/rs/vertebra-py/src/transport.rs::ResponseSubmitter
    title: PyO3 response batch submission across SHM and BatchBridge
tags:
  - embedded
  - tokio
  - batch-bridge
  - correlation
  - zero-copy
  - performance
---

# Embedded Runtime Sizing, Batch-Locked Bridge Correlation, and Zero-Copy Handoff

<!-- truth: start -->
- Default non-free-threaded (`not(Py_GIL_DISABLED)`) `Topology::Embedded` with unset `VERTEBRA_TOKIO_THREADS` to `worker_threads = 1` (`Builder::new_current_thread`), while honoring explicit `VERTEBRA_TOKIO_THREADS` overrides and free-threaded scaling.
- Size `RequestBatcher` `shard_count` in `main.rs` to active Python consumers (`1` for GIL Embedded, `embedded_py_threads.clamp(1, 8)` for free-threaded Embedded, `workers.count.clamp(1, 8)` for SHM) and align `EmbeddedSupervisor::spawn_one` with cgroup-aware `config::num_cpus()`.
- Store `http::uri::PathAndQuery` directly in `InProcessRequest` via `from_path_and_query` while keeping `InProcessRequest::new` and `BatchSnapshot` accessors compatible, and add `BatchResponseSink` (`set_batch_response_sink` / `clear_batch_response_sink`) to `BatchBridge`.
- Allocate correlation IDs under one shard `Mutex` and one `active_requests.fetch_add` per batch in `dispatch_bridge_vec`, register `BatchResponseSink` on `BatchBridge`, and add a zero-bucket single-shard fast path to `BridgeCorrelation::complete_many`.
- Add `ResponseSubmitter::submit_batch_flushed` in `vertebra-py/src/transport.rs` to move owned `headers_raw` and `PreparedRawBody::Owned` buffers via `Bytes::from` without a second copy, and wire `drive_asgi_batch`, `drive_rsgi_batch`, `drive_native_batch`, and `drive_wsgi_batch`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Chapter Scope & Performance Defects Addressed

This chapter implements root requirements **R3** and **R4** to eliminate the 1.3x–2.5x throughput deficit of 1-worker Embedded mode relative to 1-worker SHM mode.

Four concrete bottlenecks exist in the current embedded path:

1. **Oversubscribed Tokio threads and batcher shards**: `ServerConfig::from_env` leaves `worker_threads = num_cpus()` for 1-worker GIL Embedded, and `main.rs` sizes `RequestBatcher` shards from `worker_threads` instead of active Python consumers [^src-http-config] [^src-http-main].
2. **Cgroup-blind embedded supervision**: `EmbeddedSupervisor::spawn_one` reads raw `std::thread::available_parallelism()` instead of cgroup-aware `config::num_cpus()` [^src-http-factory] [^src-http-config].
3. **Per-request locking, copies, and sink loops**: `dispatch_bridge_vec` copies `path`/`query` slices into `Bytes` and acquires a shard `Mutex` plus `active_requests.fetch_add(1)` per request, while `BatchBridge::submit_responses` loops over a single-item `ResponseSink` calling `Weak::upgrade()`, locking a shard `Mutex`, and calling `fetch_sub(1)` per response [^src-core-transport] [^src-core-bridge] [^src-http-bridge].
4. **Double-copied PyO3 response buffers**: `ResponseSubmitter::submit_batch_raw_inner` borrows `&[u8]` slices and calls `Bytes::copy_from_slice` even when protocol runners already own `Vec<u8>` header and body buffers [^src-py-transport].

### 1.2 Baseline & Cross-Chapter Boundaries

- Baseline commit: `86527cf1`.
- Ordered after `ch01-wsgi-adaptive-pool` (`depends_on_chapters: [ch01-wsgi-adaptive-pool]`) so `t06` cleanly updates the inline WSGI batch flush site in `wsgi/pool.rs` / `wsgi/runner.rs` without conflicting with `ch01` pool creation.
- Disjoint from `ch03-ingress-and-worker-hotpaths`: this chapter does not modify `dispatcher/correlation.rs`, `dispatcher/shm.rs`, `server.rs`, `backpressure.rs`, `batcher.rs`, `request.rs`, `AsgiScope`, `RsgiScope`, or `RsgiHttpProtocol` layouts.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R3.1 (GIL Embedded Tokio default)**: In `ServerConfig::from_env`, when `config.topology == Topology::Embedded` and `VERTEBRA_TOKIO_THREADS` is unset on `#[cfg(not(Py_GIL_DISABLED))]`, set `config.worker_threads = 1` so `build_tokio_runtime` builds `Builder::new_current_thread()` [^src-http-config] [^src-http-main].
- **R3.2 (Override & free-threaded preservation)**: Honor explicit `VERTEBRA_TOKIO_THREADS` on all builds and preserve multi-thread scaling on `Py_GIL_DISABLED` builds [^src-http-config].
- **R3.3 (Consumer-aligned batcher shards)**: Compute `RequestBatcher` `shard_count` as `config.workers.count.clamp(1, 8)` for `Topology::Shm` and `embedded_py_consumers.clamp(1, 8)` (`1` on GIL, `worker_threads` on FT) for `Topology::Embedded` [^src-http-main].
- **R3.4 (Cgroup-aware CPU detection)**: Expose `config::num_cpus()` and call it in `EmbeddedSupervisor::spawn_one` and `spawn_python_workers` instead of raw `available_parallelism()` [^src-http-config] [^src-http-factory] [^src-http-main].
- **R4.1 (Zero-copy `InProcessRequest`)**: Store `http::uri::PathAndQuery` directly in `InProcessRequest` via `from_path_and_query` without `Bytes::copy_from_slice` while preserving `node: TransportNode`, `path_params_raw`, `InProcessRequest::new(...)`, and `BatchSnapshot` accessors/serialization [^src-core-transport].
- **R4.2 (`BatchResponseSink` on `BatchBridge`)**: Add `BatchResponseSink`, `set_batch_response_sink`, and `clear_batch_response_sink` using a lock-free `OnceLock` fast path with `RwLock` override fallback so `submit_responses` invokes the batch sink once per batch [^src-core-bridge].
- **R4.3 (Single-lock batch correlation & single-shard `complete_many`)**: Acquire one shard `Mutex` per batch in `dispatch_bridge_vec` (spilling only when full), perform one `active_requests.fetch_add(allocated)`, register both `ResponseSink` and `BatchResponseSink` in `ensure_sink_registered`, and add a zero-bucket single-shard fast path to `BridgeCorrelation::complete_many` / `complete_bridge_batch` [^src-http-bridge] [^src-http-dispatcher].
- **R4.4 (Zero-copy owned-buffer PyO3 batch flush)**: Add `ResponseSubmitter::submit_batch_flushed(py, items: Vec<FlushedProtocolResponse>)` converting owned `headers_raw: Vec<u8>` and `PreparedRawBody::Owned(Vec<u8>)` via `Bytes::from` on Embedded mode, and wire `drive_asgi_batch`, `drive_rsgi_batch`, `drive_native_batch`, and `drive_wsgi_batch` [^src-py-transport].

### 2.2 Concrete Rust Contracts

```rust
// src/rs/vertebra-http/src/config.rs
pub fn num_cpus() -> usize;

pub fn compute_batcher_shard_count(
    topology: Topology,
    shm_workers: usize,
    embedded_consumers: usize,
) -> usize;

impl ServerConfig {
    fn apply_runtime_thread_env<F>(
        &mut self,
        available_cpus: usize,
        free_threaded: bool,
        get_env: F,
    )
    where
        F: Fn(&str) -> Option<String>;
}

// src/rs/vertebra-core/src/transport/mod.rs
#[derive(Debug, Clone)]
enum InProcessPathQuery {
    Uri(http::uri::PathAndQuery),
    Bytes {
        path: bytes::Bytes,
        query: bytes::Bytes,
    },
}

#[derive(Debug, Clone)]
pub struct InProcessRequest {
    pub node: TransportNode,
    path_query: InProcessPathQuery,
    pub headers_raw: bytes::Bytes,
    pub body: bytes::Bytes,
    pub path_params_raw: bytes::Bytes,
}

impl InProcessRequest {
    #[allow(clippy::too_many_arguments)]
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

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        method_tag: u8,
        http_version_tag: u8,
        handler_id: u64,
        correlation_id: u64,
        path: bytes::Bytes,
        query: bytes::Bytes,
        headers_raw: bytes::Bytes,
        body: bytes::Bytes,
        path_params_raw: bytes::Bytes,
    ) -> Self;

    pub fn path_bytes(&self) -> &[u8];
    pub fn path_str(&self) -> &str;
    pub fn query_bytes(&self) -> Option<&[u8]>;
    pub fn query_str(&self) -> Option<&str>;
}

// src/rs/vertebra-core/src/bridge.rs
pub type BatchResponseSink = Arc<dyn Fn(Vec<BridgeResponse>) + Send + Sync>;

impl BatchBridge {
    pub fn set_batch_response_sink(&self, sink: BatchResponseSink);
    pub fn clear_batch_response_sink(&self);
}

// src/rs/vertebra-http/src/dispatcher/bridge.rs
impl BridgeCorrelation {
    pub fn complete_bridge_batch(&self, responses: Vec<BridgeResponse>);
    pub fn complete_many(&self, responses: Vec<(u64, PendingResponse)>);
}

pub(super) fn dispatch_bridge_vec(
    bridge_corr: &Arc<BridgeCorrelation>,
    batcher_shard: Option<usize>,
    requests: &mut Vec<PendingRequest>,
) -> usize;

// src/rs/vertebra-py/src/transport.rs
impl ResponseSubmitter {
    pub fn submit_batch_flushed(
        py: Python<'_>,
        items: Vec<FlushedProtocolResponse>,
    ) -> PyResult<()>;
}
```

### 2.3 Edge-Case & Failure-Mode Matrix

| Edge Case | Required Behavior |
| :--- | :--- |
| `VERTEBRA_TOKIO_THREADS=2` set on GIL Embedded | Honor explicit value (`worker_threads = 2`) while keeping `RequestBatcher` `shard_count = 1` for 1 Python worker. |
| Request URI has empty query (`/?`) | `query_bytes()` and `query()` return `None`, matching existing `Bytes::is_empty()` behavior. |
| `BatchResponseSink` cleared via `clear_batch_response_sink()` | `submit_responses` falls back to `ResponseSink` if registered, or `resp_tx` channel otherwise. |
| Override `BatchResponseSink` replaces itself during callback | Clone `Arc<BatchResponseSink>` before dropping the `RwLock` read guard so re-entrant registration cannot deadlock. |
| Primary correlation shard fills mid-batch | Drop full shard guard, spill remaining requests to `(start + attempt) & 7`, reject with `503` outside locks only if all 8 shards are full. |
| Mixed-shard batch reaches `complete_many` after spill | Single-shard scan detects differing shard IDs and executes the 8-bucket multi-shard fallback cleanly. |
| `submit_inprocess_batch` fails after batch correlation allocation | Fail all allocated IDs with `503` outside locks and decrement `active_requests` back to baseline. |
| `submit_batch_flushed` receives empty vec or non-owned `PreparedRawBody` | Empty vec returns `Ok(())`; owned `Vec<u8>` converts via `Bytes::from`, borrowed/PyBytes variants copy once only when required. |

## 3. Documentation & OKF Knowledge Plan

- `[0/3 files documented]` (curated in `ch04:t06` closeout):
  - `.agents/bundles/knowledge/architecture/runtime/topologies-and-embedding.md` (`ch04:t06`) — Document 1w GIL Embedded `current_thread` Tokio default, consumer-aligned `RequestBatcher` sharding, and cgroup-aware `EmbeddedSupervisor`.
  - `.agents/bundles/knowledge/architecture/ipc/tiered-rings-and-batch-bridge.md` (`ch04:t06`) — Document `InProcessRequest::from_path_and_query`, `BatchResponseSink`, and `submit_batch_flushed`.
  - `.agents/bundles/knowledge/architecture/http/batching-dispatch-and-backpressure.md` (`ch04:t06`) — Document single-lock `dispatch_bridge_vec` allocation and single-shard fast-path `BridgeCorrelation::complete_many`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R3.1, R3.2, R3.3, R3.4 | `runtime-perf-remediation:ch02:t01` | `behavior_tdd` |
| R4.1, R4.2 | `runtime-perf-remediation:ch02:t02` | `behavior_tdd` |
| R4.3 | `runtime-perf-remediation:ch02:t03` | `behavior_tdd` |
| R4.4 (transport) | `runtime-perf-remediation:ch02:t04` | `behavior_tdd` |
| R4.4 (ASGI/Native/RSGI) | `runtime-perf-remediation:ch02:t05` | `regression_tdd` |
| R4.4 (WSGI) | `runtime-perf-remediation:ch02:t06` | `regression_tdd` |

## 5. Implementation Plan

- [x] [3024aee2] runtime-perf-remediation:ch02:t01 — Right-size Embedded Tokio threads, consumer-aligned RequestBatcher shards, and cgroup-aware CPU detection (tasks/t01-embedded-runtime-and-shard-sizing.md)
- [x] [3024aee2] runtime-perf-remediation:ch02:t02 — Zero-copy PathAndQuery in InProcessRequest and BatchResponseSink on BatchBridge (tasks/t02-zero-copy-inprocess-and-batch-sink.md)
- [x] [3024aee2] runtime-perf-remediation:ch02:t03 — Single-lock BridgeCorrelation batch allocation, BatchResponseSink registration, and single-shard complete_many fast path (tasks/t03-batch-locked-bridge-correlation.md)
- [x] [3024aee2] runtime-perf-remediation:ch02:t04 — Zero-copy owned-buffer ResponseSubmitter::submit_batch_flushed in vertebra-py (tasks/t04-zero-copy-transport-batch-submit.md)
- [x] [3024aee2] runtime-perf-remediation:ch02:t05 — Wire drive_asgi_batch, drive_native_batch, and drive_rsgi_batch to submit_batch_flushed (tasks/t05-wire-asgi-and-rsgi-batch-flush.md)
- [x] [3024aee2] runtime-perf-remediation:ch02:t06 — Wire drive_wsgi_batch inline flush to submit_batch_flushed (tasks/t06-wire-native-and-wsgi-batch-flush.md)

### Fast Chapter Gate (< 60s)

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -- bridge transport
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http -- dispatcher config
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py
make lint-rs
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `completed` (`6/6` tasks completed across `3024aee2..9df526b0`, reviewed by `code-reviewer` + `quality-reviewer`).
- **Remaining Tasks**: None.

[^src-http-config]: `src/rs/vertebra-http/src/config.rs::ServerConfig::from_env` — Server configuration, Tokio thread defaults, and cgroup CPU detection.
[^src-http-main]: `src/rs/vertebra-http/src/main.rs::run_server` — Tokio runtime builder, embedded Python worker spawn, and RequestBatcher shard sizing.
[^src-http-factory]: `src/rs/vertebra-http/src/workers/factory.rs::EmbeddedSupervisor::spawn_one` — Multi-process SO_REUSEPORT embedded worker CPU budgeting.
[^src-core-transport]: `src/rs/vertebra-core/src/transport/mod.rs::InProcessRequest` — Zero-copy in-process request container and BatchSnapshot accessors.
[^src-core-bridge]: `src/rs/vertebra-core/src/bridge.rs::BatchBridge` — In-process BatchBridge queue and direct response sink dispatch.
[^src-http-bridge]: `src/rs/vertebra-http/src/dispatcher/bridge.rs::BridgeCorrelation` — Sharded embedded correlation table, dispatch_bridge_vec, and complete_many.
[^src-http-dispatcher]: `src/rs/vertebra-http/src/dispatcher/mod.rs::Dispatcher::dispatch_requests_from_shard` — Dispatcher backend routing and shard affinity forwarding.
[^src-py-transport]: `src/rs/vertebra-py/src/transport.rs::ResponseSubmitter` — PyO3 response batch submission across SHM and BatchBridge.
