---
type: Spec
flow_id: runtime-throughput-and-315
title: Runtime Throughput, Multi-Core Scaling, and Python 3.15 Optimizations
state: active
plan_revision: 1
plan_commit: null
state_revision: 6
current_task: "2.1"
last_operation: 20261024T163255Z-flow-executor-claim-2-1-00
operation_targets: ["2.1"]
last_verified_checkpoint: "phase:phase-1@c68065a"
created_at: 2026-10-24T16:02:00Z
updated_at: 2026-10-24T16:32:55Z
description: Eliminate embedded bridge deadlocks, single-ingress multi-worker contention, SHM syscall and lock overhead, ASGI/RSGI FFI dispatch allocations, and Python worker loop bottlenecks while adding Python 3.15 and 3.15t specializations.
---

# Runtime Throughput, Multi-Core Scaling, and Python 3.15 Optimizations

## Problem Statement & Architecture Goals

Single-core (`taskset -c 0`) and multi-core (`taskset -c 0-7`, `1w` / `4w` / `8w`) benchmarks against Granian `2.8.4` (`ASGI` and `RSGI`) exposed six root-cause bottlenecks across `vertebra-core`, `vertebra-http`, `vertebra-py`, and `src/py/vertebra/`:

1. **Embedded `BatchBridge` Deadlock (`vertebra-core/src/bridge.rs`)**: `batch_tx` (`crossbeam::bounded(4)`) and `resp_tx` (`tokio::mpsc::channel(4)`) deadlock whenever `> 5` batches are in flight (`c >= 16` on 1 core, `c >= 32` on 8 cores, or async `io10`), while copying every request through a locked `1 MiB` `DataSlab` and transcoding response headers into `Vec<(Vec<u8>, Vec<u8>)>`.
2. **Multi-Core `4w` / `8w` Scaling Wall (`vertebra-http`)**: `Topology::Auto` routes `workers > 1` through a single central Rust ingress process competing with `4`–`8` Python worker processes for the same CPU budget, serializing `ShmDispatcher` behind a global `Mutex<ShmBatchScratch>` and scattering individual items of a batch across all workers. `#[tokio::main]` also ignores `config.worker_threads` on 1-core runs.
3. **SHM Ring & `eventfd` Syscall Overhead (`vertebra-core`)**: `TieredRingConfig::default()` (`64` small slots) overflows at `c=256`, `SpscRing::try_recv_view` runs `slot_borrowed` CAS before checking `head == tail`, `SpscRing` lacks cached remote heads, `MpmcRing` shares 8 sequence counters per cache line, and both peers execute unconditional `eventfd` `write()` syscalls on every batch/response while copying SHM slots into fresh `Vec<u8>` buffers.
4. **Rust HTTP Ingress Hot-Path Locks & Transcoding (`vertebra-http`)**: `BackpressureController` acquires a global `Mutex` + `Instant::now()` + `VecDeque` twice per request, `response::handle` allocates a new `HealthEndpoint` per request, `server.rs` performs a discarded `MSG_PEEK` syscall on every TCP connection, and `RequestBatcher` allocates fresh `Vec`s and transcodes headers to MessagePack and back.
5. **Python Worker Loop, ASGI & RSGI Overhead (`vertebra-py`, `src/py/vertebra/server/`)**: `_loops.py` ignores `uvloop`, `worker.py` offloads every sync native route to `ThreadPoolExecutor(32)` (and double-executes non-native sync routes), `_dispatch_async_item` pins `Arc<BatchSnapshot>` across `await`, `EventLoopPool` pays cross-thread `call_soon_threadsafe` wakeups even when `size == 1`, `AsgiRunner` allocates `6`–`10` Python objects/tasks per request instead of single-step C-API `PyIter_Send` driving in Rust, and `RsgiScope` eagerly allocates `4` Rust `String`s and suffers an `&mut self` borrow bug on `RSGI_DONE`.
6. **Python `3.15` & `3.15t` Specializations (`vertebra-py`, `vertebra-bundler`, `vertebra-storage`, `src/py/vertebra/`)**: `freelist = 128` adds global `Mutex` contention under `#[cfg(Py_GIL_DISABLED)]`, `#[pyclass]` types omit `immutable_type`, `6` sites use runtime `PyString::intern`, `PyBytes` allocations copy intermediate `Vec<u8>`s instead of `PyBytes::new_with` (PEP 782), `Request.body_view` copies into `PyBytes` instead of anchored zero-copy buffer protocol, packages omit PEP 810 `__lazy_modules__`, and `vtb run` clobbers `VERTEBRA_*` env vars and double-mounts `App` under `--asgi`.

## Decisions & Scope

- Preserve explicit `--topology shm` cross-process isolation while enabling per-worker `SO_REUSEPORT` multi-process supervision for `--topology embedded --workers W` (and Unix `auto` when `workers > 1`) plus multi-thread embedded pools on free-threaded Python `3.14t`/`3.15t`.
- Bump `ShmHeader` `ABI_VERSION` from `2` to `3` to add cache-line-isolated `consumer_sleeping` and `active_flags` atomics for lost-wakeup-free Dekker `SeqCst` `eventfd` coalescing.
- Execute default synchronous native handlers inline in the vectorized batch loop (`submit_batch_raw`) and expose `sync_to_thread: bool = False` on route decorators for blocking sync handlers.
- Drive unary HTTP ASGI and RSGI coroutines in Rust via `PyIter_Send` (`drive_asgi_batch` / `drive_rsgi_batch`) so non-suspending handlers complete in zero `asyncio.Task` allocations while suspending handlers schedule cleanly onto `uvloop`.

## Implementation Plan

### Phase 1: Review Tooling, CLI, Supervisor, and Python 3.15 Build Readiness

- [x] Task 1.1: Align review agent tool manifests with subagent registry (`df36a30`)
- [x] Task 1.2: Fix CLI env preservation, ASGI App mount guard, Python 3.15 build/PGO/JIT detection, and benchmark harness (`c68065a`)

### Phase 2: Lock-Free In-Process Bridge, Right-Sized SHM Rings, and Dekker Wakeups

- [~] Task 2.1: Eliminate BatchBridge deadlock and add zero-copy in-process batch snapshot and direct response callback
- [ ] Task 2.2: Right-size SHM tiered rings, cache SPSC heads, pad MPMC sequence slots, and add Dekker zero-syscall wakeups

### Phase 3: Zero-Lock Rust HTTP Ingress, SO_REUSEPORT Embedded Runtime, and Middleware Offloads

- [ ] Task 3.1: Remove ingress hot-path locks, redundant h2c peek, and MessagePack header transcoding, and wire Rust middleware offloads
- [ ] Task 3.2: Add explicit Tokio runtime builder, multi-worker SO_REUSEPORT embedded topology, free-threaded worker pool, and lock-free sharded dispatchers

### Phase 4: PyO3 0.30.1 FFI, Python 3.15/3.15t Specializations, and Rust ASGI/RSGI Drivers

- [ ] Task 4.1: Apply Python 3.15 and 3.15t PyO3 optimizations, PEP 782 zero-init PyBytes, anchored Request.body_view, and Dekker batch response notifications
- [ ] Task 4.2: Implement Rust-native AsgiProtocol and drive_asgi_batch plus lazy RsgiScope, direct-submitting RsgiHttpProtocol, and drive_rsgi_batch

### Phase 5: Python Worker Loop, Inline Vectorized Sync Dispatch, uvloop, and Litestar Fastpaths

- [ ] Task 5.1: Overhaul Python worker loop with uvloop, inline vectorized sync dispatch, same-thread add_reader polling, Rust ASGI/RSGI batch drivers, O(1) mount routing, and PEP 810 lazy modules
- [ ] Task 5.2: Complete LitestarFastPathBypass.dispatch and Rust middleware offload configuration wiring

## Continuity Snapshot

- **Active Flow / Lifecycle**: `runtime-throughput-and-315` (`active`)
- **Current Task & Claimant**: `2.1` (`flow-executor`)
- **Last Verified Checkpoint**: `phase:phase-1@c68065a`
- **Decisions**:
  - `ABI_VERSION = 3` in `vertebra-core` adds `consumer_sleeping` and `active_flags` cache lines to `ShmHeader` for Dekker `SeqCst` zero-syscall notifications.
  - Per-worker `SO_REUSEPORT` embedded multi-process supervision powers multi-core throughput while preserving `--topology shm` with lock-free per-batch worker routing.
  - Default sync native routes run inline in vectorized batches; `sync_to_thread=True` opts into thread-pool offload.
  - `drive_asgi_batch` and `drive_rsgi_batch` step unary coroutines via `PyIter_Send` in Rust before falling back to `uvloop` tasks on real async suspension.
- **Recent Discoveries**: `none`
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: `Add a reproduction unit test in src/rs/vertebra-core/src/bridge.rs that pushes 64 batches into BatchBridge before draining responses from a single consumer thread, proving the bounded-channel ABBA deadlock or stall cannot occur, plus a test verifying zero-copy InProcessRequest batches exceeding 1 MiB succeed without SlabFull errors and invoke a registered direct response completion handler.`
- **Plan Identity**: `plan_revision: 1`, `plan_commit: null`
- **State Identity**: `revision: 6`, `last_operation: 20261024T163255Z-flow-executor-claim-2-1-00`, `operation_targets: ["2.1"]`
- **Relevant Rule & Knowledge Paths**:
  - `AGENTS.md`
  - `.agents/bundles/knowledge/workflow.md`
  - `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`
  - `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`
  - `.agents/bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md`

## Verification Evidence

- {"actor": "flow-executor", "affected_task_ids": ["1.1", "1.2"], "last_functional_commit": "c68065a", "occurred_at": "2026-10-24T16:31:52Z", "operation": "checkpoint", "operation_id": "20261024T163152Z-flow-executor-checkpoint-spec-00", "phase_id": "phase-1", "scope": "phase", "verification_evidence": [{"command": "uv run pytest src/py/tests/unit/test_review_setup.py src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/test_bench_infrastructure.py -q", "result": "50 passed in 1.65s"}, {"command": "make review-check", "result": "exit 0 (0 issues)"}]}
