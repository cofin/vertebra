---
type: Spec
flow_id: runtime-throughput-and-315
title: Runtime Throughput, Multi-Core Scaling, and Python 3.15 Optimizations
state: active
plan_revision: 3
plan_commit: null
state_revision: 28
current_task: null
last_operation: 20261024T223845Z-flow-executor-checkpoint-spec-00
operation_targets: []
last_verified_checkpoint: "phase:phase-5@013c702"
created_at: 2026-10-24T16:02:00Z
updated_at: 2026-10-24T22:38:45Z
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

- [x] Task 2.1: Eliminate BatchBridge deadlock and add zero-copy in-process batch snapshot and direct response callback (`b74ded5`)
- [x] Task 2.2: Right-size SHM tiered rings, cache SPSC heads, pad MPMC sequence slots, and add Dekker zero-syscall wakeups (`1ac8b83`)

### Phase 3: Zero-Lock Rust HTTP Ingress, SO_REUSEPORT Embedded Runtime, and Middleware Offloads

- [x] Task 3.1: Remove ingress hot-path locks, redundant h2c peek, and MessagePack header transcoding, and wire Rust middleware offloads (`dbc19f5`)
- [x] Task 3.2: Add explicit Tokio runtime builder, multi-worker SO_REUSEPORT embedded topology, free-threaded worker pool, and lock-free sharded dispatchers (`3ce98ae`)

### Phase 4: PyO3 0.30.1 FFI, Python 3.15/3.15t Specializations, and Rust ASGI/RSGI Drivers

- [x] Task 4.1: Apply Python 3.15 and 3.15t PyO3 optimizations, PEP 782 zero-init PyBytes, anchored Request.body_view, and Dekker batch response notifications (`7742f8b`)
- [x] Task 4.2: Implement Rust-native AsgiProtocol and drive_asgi_batch plus lazy RsgiScope, direct-submitting RsgiHttpProtocol, and drive_rsgi_batch (`00d0e7a`)

### Phase 5: Python Worker Loop, Inline Vectorized Sync Dispatch, uvloop, and Litestar Fastpaths

- [x] Task 5.1: Overhaul Python worker loop with uvloop, inline vectorized sync dispatch, same-thread add_reader polling, Rust ASGI/RSGI batch drivers, O(1) mount routing, and PEP 810 lazy modules (`e4ae09d`)
- [x] Task 5.2: Complete LitestarFastPathBypass.dispatch and Rust middleware offload configuration wiring (`5f4f421`)
- [x] Task 5.3: Harden SHM tier wakeups, admission fences, ingress Vary/compression offloads, cibuildwheel 3.15 matrix, and Litestar fastpath parity (`071e5b6`)
- [x] Task 5.4: Align Litestar v2/v3 layer resolution, zstd_compress_level offload, and fastpath middleware disqualifiers (`013c702`)

## Continuity Snapshot

- **Active Flow / Lifecycle**: `runtime-throughput-and-315` (`active`)
- **Current Task & Claimant**: `none`
- **Last Verified Checkpoint**: `phase:phase-5@013c702`
- **Decisions**:
  - `ABI_VERSION = 3` in `vertebra-core` adds `consumer_sleeping` and `active_flags` cache lines to `ShmHeader` for Dekker `SeqCst` zero-syscall notifications.
  - Per-worker `SO_REUSEPORT` embedded multi-process supervision powers multi-core throughput while preserving `--topology shm` with lock-free per-batch worker routing.
  - Default sync native routes run inline in vectorized batches; `sync_to_thread=True` opts into thread-pool offload.
  - `drive_asgi_batch` and `drive_rsgi_batch` step unary coroutines via `PyIter_Send` in Rust before falling back to `uvloop` tasks on real async suspension.
- **Recent Discoveries**:
  - `_SteppedCoroutine` drives suspending ASGI and RSGI coroutines directly on both `asyncio` and `uvloop` without an intermediate Python wrapper frame (`00d0e7a`).
  - `Worker._run_inline_event_loop` registers `loop.add_reader(notify_fd, ...)` with Dekker `prepare_request_sleep` / `cancel_request_sleep` on both `RingPair` (`shm`) and single-consumer `BatchBridge` (`embedded` when `consumer_count <= 1`) (`e4ae09d`, `a115466`).
  - `LitestarFastPathBypass.dispatch` executes pure-signature and DI/DTO Litestar handlers directly from `AsgiRunner` without ASGI scope construction, while `LitestarOffloadPlan.to_server_config_json()` wires CORS, compression, static assets, and OpenAPI cache directly into `vertebra-http` (`5f4f421`).
  - `TieredRingSet` active-tier ordering before `write_head` + `has_non_small_data()`, `BackpressureController` `SeqCst` fences, `append_vary_token` + compressible `Content-Type` checks, `pypa/cibuildwheel@v4.3.4` `cp314t`/`cp315`/`cp315t` wheel targets, `Worker._process_batch` `dispatch_app` routing, and `LitestarPlugin.ensure_finalized` + `LitestarFastPathBypass` fallback/exception/framing parity are verified green (`071e5b6`).
  - Litestar v2 `resolve_opts() -> None` in-place mutation, v2 `ownership_layers` `after_request`/`type_decoders`/`response_class` detection, v3 `zstd_compress_level` extraction, and un-offloaded Python `compression_config`/`cors_config` fastpath disqualifiers are aligned (`013c702`).
- **Blockers & Unblock Conditions**: `none`
- **Next Exact Step**: `Run final correctness review and mandatory quality review on cf5979e..HEAD, then execute Flow complete and archive.`
- **Plan Identity**: `plan_revision: 3`, `plan_commit: null`
- **State Identity**: `revision: 28`, `last_operation: 20261024T223845Z-flow-executor-checkpoint-spec-00`, `operation_targets: []`
- **Relevant Rule & Knowledge Paths**:
  - `AGENTS.md`
  - `.agents/bundles/knowledge/workflow.md`
  - `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`
  - `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`
  - `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md`
  - `.agents/bundles/knowledge/patterns/rust/lock-free-shm-and-sharding.md`

## Verification Evidence

- {"actor": "flow-executor", "affected_task_ids": ["1.1", "1.2"], "last_functional_commit": "c68065a", "occurred_at": "2026-10-24T16:31:52Z", "operation": "checkpoint", "operation_id": "20261024T163152Z-flow-executor-checkpoint-spec-00", "phase_id": "phase-1", "scope": "phase", "verification_evidence": [{"command": "uv run pytest src/py/tests/unit/test_review_setup.py src/py/tests/unit/cli/test_cli_commands.py src/py/tests/unit/build/test_packaging_and_pgo.py src/py/tests/unit/test_bench_infrastructure.py -q", "result": "50 passed in 1.65s"}, {"command": "make review-check", "result": "exit 0 (0 issues)"}]}
- {"actor": "flow-executor", "affected_task_ids": ["2.1", "2.2"], "last_functional_commit": "1ac8b83", "occurred_at": "2026-10-24T17:10:30Z", "operation": "checkpoint", "operation_id": "20261024T171030Z-flow-executor-checkpoint-phase-2-00", "phase_id": "phase-2", "scope": "phase", "verification_evidence": [{"command": "cargo test --manifest-path src/rs/Cargo.toml --workspace --features tls,compression", "result": "all workspace unit, integration, and doc tests passed"}, {"command": "cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression -- -D warnings", "result": "exit 0 (0 warnings)"}]}
- {"actor": "flow-executor", "affected_task_ids": ["3.1", "3.2"], "last_functional_commit": "3ce98ae", "occurred_at": "2026-10-24T17:55:58Z", "operation": "checkpoint", "operation_id": "20261024T175558Z-flow-executor-checkpoint-phase-3-00", "phase_id": "phase-3", "scope": "phase", "verification_evidence": [{"command": "cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http --features tls,compression", "result": "46 unit + 89 integration + 3 discovery bridge tests passed"}, {"command": "cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-http --all-targets --features tls,compression -- -D warnings", "result": "exit 0 (0 warnings)"}]}
- {"actor": "flow-executor", "affected_task_ids": ["4.1", "4.2"], "last_functional_commit": "00d0e7a", "occurred_at": "2026-10-24T18:48:00Z", "operation": "checkpoint", "operation_id": "20261024T184800Z-flow-executor-checkpoint-phase-4-00", "phase_id": "phase-4", "scope": "phase", "verification_evidence": [{"command": "uv run pytest src/py/tests/unit/server/test_asgi.py src/py/tests/unit/server/test_asgi_scope_and_extensions.py src/py/tests/unit/server/test_rsgi_protocol.py -q", "result": "62 passed in 0.77s"}, {"command": "uv run python -m mypy.stubtest vertebra._vertebra vertebra._bundler vertebra._storage", "result": "Success: no issues found in 3 modules"}]}
- {"actor": "flow-executor", "affected_task_ids": ["5.1", "5.2"], "last_functional_commit": "5f4f421", "occurred_at": "2026-10-24T19:29:00Z", "operation": "checkpoint", "operation_id": "20261024T192900Z-flow-executor-checkpoint-phase-5-00", "phase_id": "phase-5", "scope": "phase", "verification_evidence": [{"command": "uv run pytest src/py/tests/unit/server/test_litestar_direct_fastpath.py src/py/tests/unit/server/test_litestar_rust_offload.py src/py/tests/unit/extensions/test_litestar_plugin.py src/py/tests/unit/cli/test_cli_commands.py -q", "result": "27 passed in 0.45s"}, {"command": "uv run mypy src/py/vertebra tools/scripts", "result": "Success: no issues found in 210 source files"}]}
- {"actor": "flow-executor", "affected_task_ids": ["5.1", "5.2", "5.3"], "last_functional_commit": "071e5b6", "occurred_at": "2026-10-24T22:13:15Z", "operation": "checkpoint", "operation_id": "20261024T221315Z-flow-executor-checkpoint-spec-00", "phase_id": "phase-5", "scope": "phase", "verification_evidence": [{"command": "make rs-test", "result": "all workspace unit, integration, and doc tests passed"}, {"command": "make py-test", "result": "mypy.stubtest passed; 858 passed in 6.25s"}, {"command": "make ts-test", "result": "9 test files, 52 passed"}, {"command": "make coverage", "result": "858 passed, 81% total coverage"}, {"command": "make lint-all", "result": "exit 0 (Biome, Ruff, Mypy 210 files, stubtest 3 modules, rustfmt, Clippy, review-check)"}, {"command": "make review-check", "result": "exit 0 (0 issues)"}]}
- {"actor": "flow-executor", "affected_task_ids": ["5.1", "5.2", "5.3", "5.4"], "last_functional_commit": "013c702", "occurred_at": "2026-10-24T22:38:45Z", "operation": "checkpoint", "operation_id": "20261024T223845Z-flow-executor-checkpoint-spec-00", "phase_id": "phase-5", "scope": "phase", "verification_evidence": [{"command": "make rs-test", "result": "all workspace unit, integration, and doc tests passed"}, {"command": "uv run pytest -q", "result": "860 passed in 6.08s"}, {"command": "make ts-test", "result": "9 test files, 52 passed"}, {"command": "make coverage", "result": "81% total coverage"}, {"command": "make lint-all", "result": "exit 0 (Biome 36 files, Ruff 313 files, Mypy 210 files, stubtest 3 modules, rustfmt, Clippy)"}, {"command": "make review-check", "result": "exit 0 (0 issues)"}]}
