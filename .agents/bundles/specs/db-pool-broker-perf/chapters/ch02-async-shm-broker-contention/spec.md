---
type: Spec
parent_flow: db-pool-broker-perf
chapter_id: ch02-async-shm-broker-contention
title: Lock-Free Async SHM Broker Waiter Queue and Single-Waiter Futex Wake
description: Replace wake_all thundering herds with single-waiter futex wakeups, expose LeaseBroker.wait_for_wake_async over Tokio, and replace SyncPool contention in AsyncSHMBrokerTransport with per-loop local handoff and a single leader watcher.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
planning_status: complete
depends_on_chapters: []
replan_trigger: null
plan_revision: 2
plan_commit: "16ccb56e"
current_task: null
last_verified_checkpoint: null
last_reviewed_commit: null
generated:
  by: flow/0.26.0
  at: "2026-11-27T18:30:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T18:48:00Z"
    commit: "16ccb56e"
sources:
  - id: src-core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: ShmLeaseTable CAS acquire, release, and futex wake logic
  - id: src-py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 LeaseBroker methods
  - id: src-py-async-bridge
    resource: src/rs/vertebra-py/src/async_bridge.rs::spawn_rust_db_task
    title: Core Tokio-to-asyncio completion bridge in vertebra-py
  - id: src-py-broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport
    title: AsyncSHMBrokerTransport acquire and release implementation
  - id: src-vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi::LeaseBroker
    title: Native extension type stub for LeaseBroker
tags:
  - broker
  - shm
  - ipc
  - asyncio
  - performance
---

# Chapter 02: Lock-Free Async SHM Broker Waiter Queue and Single-Waiter Futex Wake

<!-- truth: start -->
- `ShmLeaseTable` adds `wake_one_waiter_seqcst()` (`wake_one()` when `partition_count <= 1`, `wake_all()` when `partition_count > 1`) for single-slot `release()` and `dispose_quarantined()`, plus `wake_seq() -> u32`, `partition_count() -> u32`, and `wait_for_wake(expected_seq, timeout) -> u32`, with zero SHM ABI layout changes.
- `LeaseBroker` exposes `wake_seq() -> int`, `partition_count() -> int`, and `wait_for_wake_async(expected_seq, timeout_ms=25) -> Awaitable[int]` via `crate::async_bridge::spawn_rust_db_task` and `tokio::task::spawn_blocking`.
- `AsyncSHMBrokerTransport` coordinates contended acquires via per-event-loop waiter deques with direct local handoff on `_release_impl` / `_dispose_quarantined_impl` (using the exact `int | None` partition returned by `_record_pool_release` / `_record_pool_dispose`) and at most one leader task per `(loop, partition)` awaiting `wait_for_wake_async`, removing `SyncPool.default()` from `acquire()`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Under 4-worker `c=128` load against a 40-lease SHM broker, `vtb-data-shm-native-4w-broker` loses ~51% throughput vs `per_worker` (`15,351` vs `31,637 RPS` on `/db`). Every contended `AsyncSHMBrokerTransport.acquire()` spawns a `SyncPool.default()` task (`contextvars.copy_context()`, `ThreadPoolExecutor` lock, GIL bounce) [^src-py-broker-transport], while every single-slot SHM release calls `hdr.wake_seq.0.wake_all()`, waking all ~80 blocked threads simultaneously [^src-core-broker].

### 1.2 Upstream Execution Baseline

- Parent spec: `../../spec.md` (`plan_revision: 2`, `plan_commit: "16ccb56e"`).
- `ShmLeaseHeader` has compile-time size assertions (`4544` B on x86_64, `4992` B on aarch64) and `partition_count: CacheLine<AtomicU32>` already resident on cache line 6 [^src-core-broker].
- `vertebra-py` already exposes `crate::async_bridge::spawn_rust_db_task` on `"_vertebra_core_completion_drainer"` [^src-py-async-bridge], and `LeaseBroker` methods are mirrored in `src/py/vertebra/_vertebra.pyi` [^src-py-broker] [^src-vertebra-pyi].

### 1.3 Code Analysis & Interface Summary

| Area | Files Examined (`path::Symbol`) | Public Interface & Dependency Category | Affected Docs / OKF Chapters |
| :--- | :--- | :--- | :--- |
| SHM lease table | `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` | Cross-process SHM layout (unchanged) + Rust API (`wake_seq`, `wait_for_wake`) | `domains/data/broker-and-shm-leases.md` |
| PyO3 broker & stubs | `src/rs/vertebra-py/src/broker.rs::LeaseBroker`, `src/py/vertebra/_vertebra.pyi` | In-process PyO3 FFI (`wake_seq`, `wait_for_wake_async`) | `domains/data/broker-and-shm-leases.md` |
| Async transport | `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` | In-process Python async transport (`acquire`, `release`, `dispose_quarantined`) | `domains/data/broker-and-shm-leases.md` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge chapters: `domains/data/broker-and-shm-leases.md`, `patterns/rust/lock-free-shm-and-sharding.md`.
- Skills: `runtime-ipc`, `pyo3-bridge`, `review-performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R1 (Single-waiter SHM wake)**: Add `ShmLeaseTable::wake_one_waiter_seqcst(&self)` that increments `hdr.wake_seq.0.fetch_add_seqcst(1)`, checks `hdr.waiters.0.load(Ordering::SeqCst) > 0`, and calls `wake_one()` if `hdr.partition_count.0.load(Ordering::Acquire) <= 1` else `wake_all()`. Use it in `release()` and `dispose_quarantined()`; keep `wake_waiters_seqcst()` (`wake_all()`) for `reap_expired()` and `reclaim_worker()`.
- **R2 (Rust & PyO3 async wake primitive)**: Add `ShmLeaseTable::wake_seq(&self) -> u32`, `ShmLeaseTable::partition_count(&self) -> u32`, and `ShmLeaseTable::wait_for_wake(&self, expected_seq: u32, timeout: Duration) -> u32` (increments `hdr.waiters` under `WaitGuard`, re-checks `wake_seq` under `SeqCst` before sleeping via `wait_timeout(expected_seq, timeout)`, returns the updated sequence). Bridge all three onto `LeaseBroker` (`wake_seq()`, `partition_count()`, and `wait_for_wake_async(expected_seq, timeout_ms=25)`) using `spawn_rust_db_task` + `tokio::task::spawn_blocking`, and declare them in `src/py/vertebra/_vertebra.pyi`.
- **R3 (Per-loop local handoff & single-leader cross-process watcher)**: In `AsyncSHMBrokerTransport`:
  - Fast path calls `try_acquire` / `try_acquire_in` synchronously with cached `_default_bind_hash` and skips `self._lock` / `lookup_partition` in `_resolve_partition` only when cross-process `self._native.partition_count() == 0` (keeping direct `os.getpid()` so `fork()` never serves a stale PID).
  - Contended path enqueues an `asyncio.Future[None]` wakeup waiter into a per-event-loop, per-partition `collections.deque`.
  - `_record_pool_release` and `_record_pool_dispose` return the resolved `int | None` partition ID (`self._partitions.get(pool_name) if pool_name else None`), and `_release_impl` / `_dispose_quarantined_impl` pass that exact `int | None` key to `_on_slot_freed(partition)` to wake one pending waiter directly on its event loop (or via `call_soon_threadsafe` across threads) with zero `SyncPool` thread hops.
  - At most one leader task per `(loop, partition)` runs `await self._native.wait_for_wake_async(seq, slice_ms)` in bounded slices (`<= 25 ms` clamped to remaining waiter deadline) when the queue is non-empty, waking waiters on each sequence change or timeout and cleanly handing leadership to the next waiter on cancellation.

### 2.2 Shared Concrete Contracts

See root `../../spec.md` Section 2.2 for `ShmLeaseTable::{wake_seq, partition_count, wait_for_wake}`, `LeaseBroker::{wake_seq, partition_count, wait_for_wake_async}`, and `_vertebra.pyi` signatures.

```python
# src/py/vertebra/extensions/broker/transport.py (internal waiter state on AsyncSHMBrokerTransport)
class _PartitionWaitQueue:
    __slots__ = ("leader_active", "loop", "waiters")
    loop: asyncio.AbstractEventLoop
    waiters: deque[asyncio.Future[None]]
    leader_active: bool
```

### 2.3 Caller -> Interface -> Collaborator Map

- `AsyncSHMBrokerTransport.acquire` -> `try_acquire[_in]` -> on miss, register `Future` in `_PartitionWaitQueue` -> either woken directly by local `_release_impl` -> `_notify_local_waiters`, or woken by single leader task awaiting `LeaseBroker.wait_for_wake_async` -> `ShmLeaseTable::wait_for_wake` -> woken by remote process `ShmLeaseTable::release` -> `wake_one_waiter_seqcst()`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| Zero-timeout acquire (`timeout_ms=0` or `timeout=0.0`) on exhausted pool | Raises `TimeoutError("Connection broker pool exhausted")` immediately without enqueuing a waiter or spawning a leader. |
| Waiter cancelled while sitting in follower deque | `finally` removes or marks future done; subsequent `_notify_local_waiters` skips done futures and wakes the next live waiter. |
| Leader coroutine cancelled while awaiting `wait_for_wake_async` | `finally` clears `leader_active` and wakes the front live follower so a new leader takes over immediately. |
| Slot released between failed `try_acquire` and `wait_for_wake_async` | Leader captures `seq = self._native.wake_seq()` *before* retrying `try_acquire`; if a release occurs in between, `wake_seq` increments and `wait_for_wake` returns immediately without sleeping. |
| Cross-process release on partitioned pool (`partition_count == 1` vs `> 1`) | `partition_count <= 1` wakes 1 waiter via `wake_one()` (exact for uniform unpartitioned or single-partition `AsyncBrokerPool` pools; if unpartitioned `None` and partitioned `Some(0)` callers share one table with `partition_0.pool_size < total_capacity`, wakeup is bounded by the 25 ms `wait_for_wake` slice); `partition_count > 1` wakes all via `wake_all()` to prevent cross-partition starvation. |
| `AsyncSHMBrokerTransport` with fake/mock `_native` lacking `wait_for_wake_async` | Fallback path cleanly uses `SyncPool.default().submit_async(self._native.acquire[_in], ...)` so existing mock unit tests remain 100% compatible. |

### 2.5 Acceptance Scenarios

1. `c=128` concurrent `AsyncSHMBrokerTransport.acquire()` tasks on a 10-slot pool complete all acquires/releases with `0` `SyncPool.default().submit_async` invocations and `0` leaked leases.
2. Two OS processes sharing a named SHM broker segment wake cross-process `AsyncSHMBrokerTransport` waiters promptly via `wait_for_wake_async` when the remote process releases a slot.
3. Cancelling a leader task while 10 followers wait does not stall the remaining followers.
4. `mypy.stubtest vertebra._vertebra` passes with zero missing or mismatched declarations.

### 2.6 Alternatives Considered

- **Native Rust `LeaseBroker.acquire_async` spawning one Tokio blocking task per Python coroutine**: Rejected because `c=128` coroutines would still spawn 128 Tokio blocking threads contending on the same futex instead of 1 leader per worker process.

## 3. Documentation & OKF Knowledge Plan

- Deferred to `ch03:t09` (`.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task ID | Verification Strategy |
| :--- | :--- | :--- |
| R1, R2 | `t04` | `behavior_tdd`, `regression_tdd` |
| R3 | `t05` | `behavior_tdd`, `regression_tdd` |
| R1, R2, R3 | `t06` | `regression_tdd` |

## 5. Implementation Plan

- [ ] t04 — Add `wake_one_waiter_seqcst`, `wake_seq`, and `wait_for_wake` in Rust + `LeaseBroker.wait_for_wake_async` & `.pyi` stub (`tasks/t04-rust-broker-single-wake-and-async-wait.md`)
- [ ] t05 — Implement per-loop local handoff, single-leader watcher, and hot-path caching in `AsyncSHMBrokerTransport` (`tasks/t05-async-broker-local-handoff-and-leader.md`)
- [ ] t06 — Add concurrency, cancellation, cross-process wake, and stubtest regression tests (`tasks/t06-async-broker-contention-tests.md`)

### Chapter / Phase Recovery Checkpoint

```bash
make dev-ext EXT=core && cargo test -p vertebra-core broker && cargo test -p vertebra-py broker && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py && uv run --no-sync python -m mypy.stubtest vertebra._vertebra
```

## 6. Current State & Remaining Plan Snapshot

- **Chapter State**: `planned` (`0/3` tasks complete).
- **Next Task**: `t04`.

[^src-core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — ShmLeaseTable CAS acquire, release, and futex wake logic.
[^src-py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker methods.
[^src-py-async-bridge]: `src/rs/vertebra-py/src/async_bridge.rs::spawn_rust_db_task` — Core Tokio-to-asyncio completion bridge in vertebra-py.
[^src-py-broker-transport]: `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` — AsyncSHMBrokerTransport acquire and release implementation.
[^src-vertebra-pyi]: `src/py/vertebra/_vertebra.pyi::LeaseBroker` — Native extension type stub for LeaseBroker.
