---
type: Task
id: async-loop-and-framework-fastpath:t03
title: "Replace per-op call_soon_threadsafe with RustCompletionRing + Notifier in vertebra-storage and batched SyncPool.submit_async wakeups"
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - async-loop-and-framework-fastpath:t02
files:
  - src/rs/vertebra-storage/Cargo.toml
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/utils/sync_tools.py
tests:
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/db/test_native_db_drivers.py
plan_revision: 2
plan_commit: 34429ff8c50d4622d0cacbd1288525f8a1211dc8
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-01T23:48:00Z
updated_at: 2026-11-02T00:03:00Z
commit: null
tags: [rust, pyo3, storage, database, async, eventfd, tdd]
---

# Task t03: Replace per-op `call_soon_threadsafe` with `RustCompletionRing` + `Notifier` in `vertebra-storage` and batched `SyncPool.submit_async` wakeups

## Objective
Eliminate Tokio-thread `Python::attach` GIL contention and per-operation `loop.call_soon_threadsafe` self-pipe syscalls in `src/rs/vertebra-storage/src/lib.rs::spawn_rust_task` by introducing a per-event-loop `RustCompletionRing` backed by `vertebra_core::notify::Notifier` (`eventfd`/`kqueue`) and `loop.add_reader`, and add per-loop batched completion draining to `src/py/vertebra/utils/sync_tools.py::SyncPool.submit_async` so concurrent async database operations across all `vertebra.db` drivers and `SHMBrokerTransport.acquire_async` coalesce into a single event-loop wakeup per batch.

## Context
- **Verification Strategy (`behavior_tdd`)**: Write failing tests in `src/py/tests/unit/storage/test_gcs_token_refresh.py` and `src/py/tests/unit/db/test_native_db_drivers.py` verifying high-concurrency `asyncio.gather` storage operations and batched `SyncPool.submit_async` / `AsyncPostgreSQLDriver` / `AsyncSQLiteDriver` completions.
- **Architectural Invariants**:
  - Tokio worker threads must push completed outcomes into the per-loop MPSC completion queue **without calling `Python::attach`** when the `Notifier` reader is active, and only signal `notifier.notify()` when `wake_armed.swap(true, Ordering::AcqRel) == false`.
  - If an event loop does not support `add_reader` (e.g. `ProactorEventLoop` on Windows or custom mock loop), fall back gracefully to `call_soon_threadsafe` of the batched drainer callable.
  - Clean up or tolerate closed loops gracefully if the Python event loop closes while a background Tokio or worker-pool task is in flight.

## Steps
1. **Write failing unit tests (`RED`)**:
   - In `src/py/tests/unit/storage/test_gcs_token_refresh.py`, add `test_storage_completion_ring_coalesces_concurrent_async_ops` running 64 concurrent `put_async` / `get_async` / `head_async` / `delete_async` operations via `asyncio.gather` and verifying accurate results and exception propagation (`FileNotFoundError`).
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add `test_sync_pool_and_async_drivers_coalesce_completion_wakeups` exercising 64 concurrent `SyncPool.default().submit_async(...)` and `AsyncSQLiteDriver` / `AsyncPostgreSQLDriver` queries via `asyncio.gather` and asserting that concurrent worker completions coalesce wakeups via the per-loop completion queue.
2. **Implement `RustCompletionRing` + `Notifier` in `vertebra-storage` and batched `SyncPool.submit_async` (`GREEN`)**:
   - Add `vertebra-core = { path = "../vertebra-core" }` to `src/rs/vertebra-storage/Cargo.toml` if not already present so `vertebra_core::notify::Notifier` is available.
   - In `src/rs/vertebra-storage/src/lib.rs` (and `src/py/vertebra/_storage.pyi` if any exported class changes), implement `LoopCompletionRing` (`Mutex<Vec<CompletedTask>>`, `wake_armed: AtomicBool`, `Notifier`) and `#[pyclass] CompletionRingDrainer`, keyed per running Python event loop (`usize` loop pointer + loop-closed check) and registered once via `loop_obj.call_method1("add_reader", (fd, drainer))` on Unix.
   - In `spawn_rust_task`, enqueue `(future_ref, producer)` into the loop's `LoopCompletionRing` from the Tokio task without acquiring the GIL on Tokio worker threads, and trigger `notifier.notify()` when `wake_armed` transitions from `false` to `true`.
   - In `src/py/vertebra/utils/sync_tools.py::SyncPool`, add a per-event-loop completion queue (`collections.deque` + armed boolean flag) so `submit_async` worker threads append `(future, result, exc)` to the loop's deque and invoke `loop.call_soon_threadsafe(drain_fn)` **only** when transitioning the armed flag from `False` to `True`, resolving all ready `asyncio.Future` instances in a single event-loop turn.
3. **Rebuild extensions & verify (`REFACTOR`)**:
   - Run `make dev`, `mypy.stubtest`, and the storage + database unit test suites.

## Verification
```bash
make dev
uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._storage --allowlist tools/stubtest_allowlist.txt
uv run --no-sync pytest src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_native_db_drivers.py -q
```

## Acceptance Criteria
- [ ] `spawn_rust_task` in `vertebra-storage` completes Tokio tasks without acquiring the GIL on Tokio threads when `add_reader` is active and coalesces wakeups via `Notifier` (R3).
- [ ] `SyncPool.submit_async` coalesces concurrent worker thread completions per event loop into a single batched wakeup and resolves all `vertebra.db` async driver futures accurately (R3).

## Notes & Discoveries
