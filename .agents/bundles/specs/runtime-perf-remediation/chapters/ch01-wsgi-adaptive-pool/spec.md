---
type: Spec
parent_flow: runtime-perf-remediation
chapter_id: ch01-wsgi-adaptive-pool
title: Adaptive Rust WSGI Pool and Shared Server Config
description: Replace serial WSGI batch execution with an adaptive inline-plus-Rust thread pool executor and batch-shared WsgiServerConfig.
status: stable
state: completed
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters: []
replan_trigger: null
plan_revision: 2
plan_commit: "86527cf1"
current_task: null
last_verified_checkpoint: "6768cf7c"
last_reviewed_commit: "6768cf7c"
generated:
  by: flow/0.26.0
  at: "2026-11-27T01:40:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T01:58:43Z"
    commit: "86527cf1"
sources:
  - id: root-spec
    resource: .agents/bundles/specs/runtime-perf-remediation/spec.md
    title: Root Runtime Performance Remediation specification
  - id: src-wsgi-runner
    resource: src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch
    title: Native Rust WSGI batch runner currently executing requests serially
  - id: src-wsgi-environ
    resource: src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron
    title: Lazy PEP 3333 WSGI environ mapping with per-request String fields
  - id: src-wsgi-mod
    resource: src/rs/vertebra-py/src/wsgi/mod.rs
    title: WSGI Rust module exports
  - id: src-py-lib
    resource: src/rs/vertebra-py/src/lib.rs::pymodule_init
    title: PyO3 _vertebra native module registration
  - id: src-py-stub
    resource: src/py/vertebra/_vertebra.pyi
    title: Type stubs for the _vertebra native extension module
  - id: src-wsgi-adapter
    resource: src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter
    title: Python WSGI adapter delegating batches to drive_wsgi_batch
  - id: src-server-worker
    resource: src/py/vertebra/server/worker.py::Worker::run
    title: Worker lifecycle, batch dispatch, and shutdown cleanup
tags:
  - wsgi
  - performance
  - pyo3
  - concurrency
  - rust
  - worker
---

# Adaptive Rust WSGI Pool and Shared Server Config

<!-- truth: start -->
- Share batch-level WSGI server metadata (`root_path`, `server_name`, `server_port`, `url_scheme`) via `Arc<WsgiServerConfig>` in `WsgiEnviron` to remove three `String` clones per request while preserving `WsgiEnviron::new` and `WsgiEnviron::with_root_path`.
- Add native Rust `WsgiExecutor` (`src/rs/vertebra-py/src/wsgi/pool.rs`) with `WsgiExecutionMode::{Auto, Concurrent, Inline}` (`VERTEBRA_WSGI_MODE`), thread sizing (`VERTEBRA_WSGI_THREADS`, default `max(32, cpus * 4).clamp(32, 128)`), and configurable stall detection (`VERTEBRA_WSGI_STALL_US`, default `250`).
- Execute zero-I/O batches inline in `Auto` mode via `ResponseSubmitter::submit_batch_raw_inner`, and hand off stalled remaining items `1..N` plus subsequent batches during a 2 s hysteresis window to the Rust OS thread pool submitting each response immediately via `ResponseSubmitter::submit_raw_inner`.
- Export `shutdown_wsgi_executor()` from `_vertebra`, type it in `_vertebra.pyi`, call it in `Worker.run`'s `finally:` block, and detach from the GIL via `py.detach` before joining worker and watchdog threads.
- Verify blocking WSGI concurrency (`16 x 30 ms < 150 ms` plus a parallel barrier), inline fastpath execution, `500` exception isolation, and clean GIL-safe shutdown in Rust and Python unit tests.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Scope

`drive_wsgi_batch` currently executes every request in a batch serially on one thread and submits responses only after the entire batch finishes [^src-wsgi-runner] [^root-spec]. Blocking I/O in a WSGI handler (such as `io_10ms` or synchronous database calls) serializes the whole batch and stalls other connections (`85 RPS` on 1w embedded and `380 RPS` on 4w SHM) [^root-spec]. In addition, `drive_wsgi_batch` allocates three owned `String` copies (`root_path`, `server_name`, `url_scheme`) per request when building `WsgiEnviron` [^src-wsgi-environ] [^src-wsgi-runner].

This chapter implements root requirements **R1** and **R2** [^root-spec]:

1. Introduce `WsgiServerConfig` shared via `Arc<WsgiServerConfig>` across all `WsgiEnviron` instances in a batch while keeping existing constructors backwards-compatible [^src-wsgi-environ].
2. Implement native Rust `WsgiExecutor` in `src/rs/vertebra-py/src/wsgi/pool.rs` providing adaptive inline-plus-pool execution, a zero-idle-CPU stall watchdog, 2 s concurrent hysteresis, and GIL-safe shutdown.
3. Wire `drive_wsgi_batch`, WSGI module re-exports, `_vertebra` exports, `_vertebra.pyi`, `WsgiAdapter`, and `Worker.run` shutdown cleanup [^src-wsgi-runner] [^src-wsgi-mod] [^src-py-lib] [^src-py-stub] [^src-wsgi-adapter] [^src-server-worker].
4. Add Rust and Python unit and concurrency barrier tests proving parallel blocking I/O, inline zero-I/O fastpath behavior, `500` error isolation, and clean pool drain.

### 1.2 Upstream Baseline & Boundaries

- Baseline commit: `86527cf1` (implement on a feature branch off `main`).
- Out of scope for this chapter: Embedded Tokio runtime sizing (`ch02`), `BatchBridge` / `ResponseSubmitter` transport refactors (`ch02`), HTTP ingress and worker event loop refactors (`ch03`), and live benchmark runs (`ch04`).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.1 (Execution Modes & Environment Controls)**: Parse `VERTEBRA_WSGI_MODE` (`auto`, `concurrent`, `inline`, case-insensitive; default `Auto` on GIL builds and `Concurrent` on `#[cfg(Py_GIL_DISABLED)]`), `VERTEBRA_WSGI_THREADS` (default `max(32, cpus * 4).clamp(32, 128)`, explicit positive override clamped to `1..=512`), and `VERTEBRA_WSGI_STALL_US` (default `250`, positive `u64`).
- **R1.2 (Zero-I/O Inline Fastpath)**: In `Inline` mode, or in `Auto` mode outside the concurrent hysteresis window, execute batch items inline on the caller thread and submit completed responses in one call via `ResponseSubmitter::submit_batch_raw_inner`.
- **R1.3 (Adaptive Stall Handoff & Hysteresis)**: In `Auto` mode, track the active item start timestamp and atomic progress counter. When an inline item exceeds `VERTEBRA_WSGI_STALL_US`, atomically claim remaining unstarted items `cursor..N` for the Rust OS thread pool and enter a 2 s (`2_000_000` µs) concurrent hysteresis window. Each pool worker executes one item under `Python::attach` and submits its response immediately via `ResponseSubmitter::submit_raw_inner`.
- **R1.4 (Zero Idle CPU & Lazy Pool Threads)**: Keep the stall watchdog parked on `Condvar::wait` with zero CPU usage when no WSGI batch is active. Spawn OS worker threads lazily on the first concurrent enqueue so zero-I/O apps spawn no pool workers.
- **R1.5 (GIL-Safe Shutdown & Re-Initialization)**: Export `shutdown_wsgi_executor()` to Python and call it from `Worker.run`'s `finally:` block. Shutdown removes the global executor from its `RwLock<Option<Arc<WsgiExecutor>>>`, detaches from the GIL via `py.detach`, stops the watchdog, drains all queued tasks, and joins all threads. Subsequent `drive_wsgi_batch` calls lazily initialize a fresh executor.
- **R2.1 (Batch-Shared `WsgiServerConfig`)**: Store `config: Arc<WsgiServerConfig>` on `WsgiEnviron`, normalizing `root_path` once per batch (`"/"` -> `""`, trimming trailing slashes) while preserving `WsgiEnviron::new` and `WsgiEnviron::with_root_path` signatures.

### 2.2 Concrete Rust & Python Contracts

```rust
// src/rs/vertebra-py/src/wsgi/environ.rs
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsgiServerConfig {
    pub root_path: String,
    pub server_name: String,
    pub server_port: u16,
    pub url_scheme: String,
}

impl WsgiServerConfig {
    pub fn new(
        root_path: impl Into<String>,
        server_name: impl Into<String>,
        server_port: u16,
        url_scheme: impl Into<String>,
    ) -> Self;
}

impl WsgiEnviron {
    pub fn with_config(
        batch: Arc<BatchSnapshot>,
        index: usize,
        config: Arc<WsgiServerConfig>,
    ) -> Self;
    // Existing WsgiEnviron::new and WsgiEnviron::with_root_path remain unchanged
}
```

```rust
// src/rs/vertebra-py/src/wsgi/pool.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsgiExecutionMode {
    Auto,
    Concurrent,
    Inline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsgiPoolConfig {
    pub mode: WsgiExecutionMode,
    pub threads: usize,
    pub stall_threshold_us: u64,
    pub hysteresis_us: u64,
}

impl WsgiPoolConfig {
    pub fn from_env() -> Self;
}

pub enum WsgiBatchItems {
    Contiguous {
        snapshot: Arc<BatchSnapshot>,
        len: usize,
    },
    Indexed(Vec<(Arc<BatchSnapshot>, usize)>),
}

pub type WsgiItemExecutor = fn(
    Python<'_>,
    &Bound<'_, PyAny>,
    Arc<BatchSnapshot>,
    usize,
    Arc<WsgiServerConfig>,
    u64,
) -> FlushedProtocolResponse;

pub struct WsgiExecutor { /* lazy pool, Condvar queue, zero-idle watchdog, atomic hysteresis */ }

impl WsgiExecutor {
    pub fn new(config: WsgiPoolConfig) -> Self;
    pub fn config(&self) -> WsgiPoolConfig;
    pub fn dispatch_batch(
        self: &Arc<Self>,
        py: Python<'_>,
        app: &Bound<'_, PyAny>,
        items: WsgiBatchItems,
        server_config: Arc<WsgiServerConfig>,
        execute_item: WsgiItemExecutor,
    ) -> PyResult<usize>;
    pub fn shutdown(&self);
}

pub fn global_wsgi_executor() -> Arc<WsgiExecutor>;

#[pyfunction]
#[pyo3(name = "shutdown_wsgi_executor")]
pub fn shutdown_wsgi_executor(py: Python<'_>) -> PyResult<()>;
```

```rust
// src/rs/vertebra-py/src/wsgi/runner.rs
pub(crate) fn execute_single_wsgi_item(
    py: Python<'_>,
    app: &Bound<'_, PyAny>,
    snapshot: Arc<BatchSnapshot>,
    index: usize,
    config: Arc<WsgiServerConfig>,
    request_id: u64,
) -> FlushedProtocolResponse;
```

```python
# src/py/vertebra/_vertebra.pyi
def shutdown_wsgi_executor() -> None: ...
```

### 2.3 Atomic Inline-to-Pool Handoff Protocol

When `Auto` mode runs a batch of `total > 1` items inline:

1. Construct an `InlineBatchHandoff` holding a GIL-safe wrapper around `Py<PyAny>` (`GilSafePyObject`, whose `Drop` fallback attaches via `Python::attach` if ever dropped off-GIL, though on the normal path the caller retains ownership on-GIL and worker threads drop inside `Python::attach`), `WsgiBatchItems`, `Arc<WsgiServerConfig>`, `next_index: AtomicUsize(0)`, `handed_off: AtomicBool(false)`, `item_start_ns: AtomicU64(0)`, and `progress: AtomicU64(0)`.
2. Publish the active handoff and wake the watchdog only if `!watchdog_awake.load(Acquire)`. During steady-state `/hello` traffic, the watchdog stays awake for a brief idle grace (`2 ms`) and batches perform zero `notify_one()` syscalls.
3. On each loop iteration, the caller checks `handed_off.load(Acquire)` and claims `i = next_index.fetch_add(1, AcqRel)`. If `i >= total`, break. Before invoking `execute_item`, store `item_start_ns = now_ns()` (`Release`) and increment `progress` (`Release`).
4. If the watchdog observes `item_start_ns != 0` and `now_ns - item_start_ns >= stall_threshold_ns`, it calls `mark_concurrent_window(now_ns)` and attempts `handed_off.swap(true, AcqRel)`. On winning, it executes `start = next_index.swap(total, AcqRel)` and enqueues `start..total` onto the OS thread pool without acquiring the GIL. The watchdog never drops the final `Arc<InlineBatchHandoff>` off-GIL: either the caller still holds its `Arc` on-GIL while active, or `GilSafePyObject::drop` safely re-attaches via `Python::attach` if a non-GIL thread ever releases the final reference.
5. When the caller returns from item `i`, if `handed_off.load(Acquire)` is `true` or `item_elapsed >= stall_threshold_ns`, it breaks (claiming any remaining `start..total` if it won `handed_off.swap(true)`), clears `item_start_ns = 0`, unpublishes the handoff before dropping its on-GIL `Arc`, and submits its locally completed items via `submit_batch_raw_inner`.

Every index in `0..total` is executed strictly once: `fetch_add(1)` and `swap(total)` form a total order on `next_index`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case | Required Behavior |
| :--- | :--- |
| Blocking call stalls item 0 of a 16-item batch | Watchdog triggers after `VERTEBRA_WSGI_STALL_US`, hands off items `1..16` to pool threads in parallel, and item 0 flushes on completion. |
| Single-item (`N == 1`) slow batch in `Auto` mode | Runs inline without registering a handoff; elapsed time exceeding threshold sets the 2 s concurrent hysteresis window for subsequent batches. |
| WSGI callable raises exception or violates PEP 3333 in pool thread | `execute_single_wsgi_item` logs via `log_app_exception` and returns HTTP `500`; pool thread submits `500` via `submit_raw_inner` and remains healthy. |
| `shutdown_wsgi_executor()` called while blocking tasks hold/wait for GIL | Calling thread removes global executor and enters `py.detach(|| exec.shutdown())`, avoiding GIL deadlock while draining all queued tasks and joining threads. |
| Invalid or zero `VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`, or `VERTEBRA_WSGI_STALL_US` | Fall back to safe defaults (`Auto` or `Concurrent` on FT, `max(32, cpus * 4).clamp(32, 128)`, `250 µs`) without panicking. |
| Multiple test cases change WSGI env vars in one Python process | Calling `shutdown_wsgi_executor()` clears the global executor slot; the next `drive_wsgi_batch` re-reads env vars and builds a fresh executor. |

## 3. Documentation & OKF Knowledge Plan

- `[0/1 files documented]` (curated in `ch04` closeout alongside full benchmark evidence, or synced if touched early):
  - `.agents/bundles/knowledge/architecture/worker/asgi-and-wsgi-runners.md` — Document `WsgiServerConfig`, `WsgiExecutor`, `VERTEBRA_WSGI_MODE`, `VERTEBRA_WSGI_THREADS`, `VERTEBRA_WSGI_STALL_US`, and `shutdown_wsgi_executor()`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R2.1 | `runtime-perf-remediation:ch01:t01` | `behavior_tdd` |
| R1.1, R1.3, R1.4, R1.5 | `runtime-perf-remediation:ch01:t02` | `behavior_tdd` |
| R1.2, R1.3, R1.5, R2.1 | `runtime-perf-remediation:ch01:t03` | `behavior_tdd` |
| R1.5 | `runtime-perf-remediation:ch01:t04` | `regression_tdd` |
| R1.1–R1.5, R2.1 | `runtime-perf-remediation:ch01:t05` | `regression_tdd` |

## 5. Implementation Plan

- [x] [b110db7d] t01-wsgi-server-config — Introduce batch-shared `WsgiServerConfig` and `WsgiEnviron::with_config` (tasks/t01-wsgi-server-config.md)
- [x] [b110db7d] t02-wsgi-executor-pool — Implement native Rust `WsgiExecutor`, stall watchdog, and `shutdown_wsgi_executor` (tasks/t02-wsgi-executor-pool.md)
- [x] [b110db7d] t03-wsgi-batch-runner-and-exports — Wire `drive_wsgi_batch` to `WsgiServerConfig` and `WsgiExecutor`, and export `shutdown_wsgi_executor` (tasks/t03-wsgi-batch-runner-and-exports.md)
- [x] [b110db7d] t04-python-stubs-and-worker-shutdown — Add `_vertebra.pyi` stub, call `shutdown_wsgi_executor()` in `Worker.run`, and align `WsgiAdapter` docs (tasks/t04-python-stubs-and-worker-shutdown.md)
- [x] [b110db7d] t05-wsgi-adaptive-pool-tests — Add Python concurrency barrier, inline fastpath, error isolation, and shutdown regression tests (tasks/t05-wsgi-adaptive-pool-tests.md)

### Fast Chapter Gate (`< 60s`)

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-py wsgi
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-py --all-targets -- -D warnings
make dev-ext EXT=core
uv run --no-sync pytest src/py/tests/unit/server/test_wsgi_adaptive_pool.py
uv run --no-sync ruff check src/py/vertebra/_vertebra.pyi src/py/vertebra/server/_wsgi/_adapter.py src/py/vertebra/server/worker.py src/py/tests/unit/server/test_wsgi_adaptive_pool.py
uv run --no-sync python -m mypy.stubtest vertebra._vertebra
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `completed` (`5/5` tasks landed at `b110db7d`, reviewed and remediated at `6768cf7c`).
- **Next Command**: Proceed to `ch02-embedded-bridge-and-batching`.

[^root-spec]: `.agents/bundles/specs/runtime-perf-remediation/spec.md` — Root Runtime Performance Remediation specification.
[^src-wsgi-runner]: `src/rs/vertebra-py/src/wsgi/runner.rs::drive_wsgi_batch` — Native Rust WSGI batch runner currently executing requests serially.
[^src-wsgi-environ]: `src/rs/vertebra-py/src/wsgi/environ.rs::WsgiEnviron` — Lazy PEP 3333 WSGI environ mapping with per-request String fields.
[^src-wsgi-mod]: `src/rs/vertebra-py/src/wsgi/mod.rs` — WSGI Rust module exports.
[^src-py-lib]: `src/rs/vertebra-py/src/lib.rs::pymodule_init` — PyO3 _vertebra native module registration.
[^src-py-stub]: `src/py/vertebra/_vertebra.pyi` — Type stubs for the _vertebra native extension module.
[^src-wsgi-adapter]: `src/py/vertebra/server/_wsgi/_adapter.py::WsgiAdapter` — Python WSGI adapter delegating batches to drive_wsgi_batch.
[^src-server-worker]: `src/py/vertebra/server/worker.py::Worker::run` — Worker lifecycle, batch dispatch, and shutdown cleanup.
