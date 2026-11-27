---
type: Task
id: t05
chapter_id: ch02-async-shm-broker-contention
title: Replace SyncPool broker waiting with per-loop local handoff and single-leader watcher
description: Add per-event-loop waiter deques, direct local release handoff, single-leader wait_for_wake_async watching, and hot-path bind-hash and partition-check fastpaths in AsyncSHMBrokerTransport.
status: stable
state: pending
priority: high
verification_strategy: behavior_tdd
worksheet_format: full
depends_on:
  - t04
read_set:
  - src/py/vertebra/extensions/broker/transport.py
  - src/py/vertebra/extensions/sqlalchemy/pool.py
files:
  - src/py/vertebra/extensions/broker/transport.py
tests:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 210
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-py-broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport
    title: AsyncSHMBrokerTransport acquire, release, and dispose_quarantined paths
  - id: src-sa-pool
    resource: src/py/vertebra/extensions/sqlalchemy/pool.py::AsyncBrokerPool
    title: SQLAlchemy AsyncBrokerPool consumer of AsyncSHMBrokerTransport
tags:
  - broker
  - asyncio
  - performance
  - python
---

# t05: Replace SyncPool broker waiting with per-loop local handoff and single-leader watcher

## Objective & Scope Boundary

Eliminate `SyncPool.default()` thread contention and per-call metadata overhead in `AsyncSHMBrokerTransport.acquire()` by introducing per-event-loop local waiter handoff and a single leader watcher over `LeaseBroker.wait_for_wake_async()`.

In scope:
- `src/py/vertebra/extensions/broker/transport.py`:
  - Cache `_default_bind_hash = stable_hash64("default")` and skip `self._lock` / `lookup_partition` in `_resolve_partition` only when cross-process `self._native.partition_count() == 0` (keeping direct `os.getpid()` so post-fork child processes never serve a stale PID).
  - Update `_record_pool_release` and `_record_pool_dispose` to return the resolved `int | None` partition ID (`self._partitions.get(pool_name) if pool_name else None`), and pass that exact key to `_on_slot_freed(partition)` from `_release_impl` and `_dispose_quarantined_impl`.
  - Add per-event-loop `_PartitionWaitQueue` table (`WeakKeyDictionary[asyncio.AbstractEventLoop, dict[int | None, _PartitionWaitQueue]]`) protected by a dedicated lightweight `threading.Lock`.
  - On `AsyncSHMBrokerTransport.acquire()` miss:
    - If `effective_timeout <= 0`: raise `TimeoutError("Connection broker pool exhausted")`.
    - If `self._native` lacks `wait_for_wake_async` (e.g., mock test double): retain fallback to `SyncPool.default().submit_async(...)`.
    - Otherwise loop until deadline:
      - If no leader is active on `(loop, resolved_partition)`: become leader, capture `seq = self._native.wake_seq()`, retry `try_acquire[_in]`; if still `None`, await `self._native.wait_for_wake_async(seq, slice_ms)` (with `slice_ms = min(25, remaining_ms)`), then wake front follower(s) and retry.
      - If a leader is already active: append a fresh `asyncio.Future[None]` to `queue.waiters`, await `asyncio.wait_for(fut, timeout=remaining_s)` (or deadline timer), and retry `try_acquire[_in]` upon wakeup.
  - On `_release_impl` and `_dispose_quarantined_impl`, call `_on_slot_freed(partition)` to notify one waiting local future for the freed `partition` (and `None` unpartitioned queue when `partition is not None`) directly via `loop.call_soon` (same loop) or `loop.call_soon_threadsafe` (cross-thread/loop).

Out of scope:
- `SyncSHMBrokerTransport` blocking path (remains native `self._native.acquire` with GIL detached).
- `AsyncBrokerStream` / `execute_dbrpc` / `reclaim_worker_leases` methods.

## Bounded Read Manifest

- `src/py/vertebra/extensions/broker/transport.py` (lines 615-820, 1070-1200, 1515-1635) [^src-py-broker-transport]
- `src/py/vertebra/extensions/sqlalchemy/pool.py` (lines 386-455) [^src-sa-pool]

## Symbol & Signature Contracts

- Public signatures of `AsyncSHMBrokerTransport.{acquire, release, quarantine, dispose_quarantined}` remain 100% backward compatible.
- Internal helper `_PartitionWaitQueue` per `../spec.md` Section 2.2.
- `_BaseSHMBrokerTransport._on_slot_freed(self, partition: int | None) -> None` no-op hook overridden on `AsyncSHMBrokerTransport` so both `await transport.release(...)`, `lease.release_sync()`, and `await transport.dispose_quarantined(...)` trigger immediate local waiter wakeup!

## Edge Cases & Error Semantics

- **Leader handoff on cancellation or grant**: Whenever the leader exits (acquired slot, timed out, or `CancelledError`), its `finally` block sets `queue.leader_active = False` and resolves the first non-done follower future in `queue.waiters` so that follower immediately retries `try_acquire` and takes over leadership if needed.
- **No lost wakeups between local release and leader sleep**: Because `_release_impl` calls `self._native.release(...)` FIRST (which increments SHM `wake_seq` and calls `wake_one_waiter_seqcst()`), a sleeping leader wakes from `wait_for_wake_async`, AND `_on_slot_freed` wakes a queued follower directly on the event loop in the same tick.
- **Stale/cancelled futures**: Both `_notify_local_waiters` and leader handoff pop done futures (`while queue.waiters: fut = queue.waiters.popleft(); if not fut.done(): ...`).
- **Closed loop**: Suppress `RuntimeError` when calling `call_soon_threadsafe` on a closing loop and prune dead loops naturally via `WeakKeyDictionary`.
- **No inline `#` comments** in Python code (use Google-style docstrings).

## Public-Seam Test Blueprint

- Verify `AsyncSHMBrokerTransport.acquire()` under `c=128` concurrency over a 10-slot broker invokes `SyncPool.default().submit_async` `0` times while completing all 128 tasks with `active_leases == 0` afterward.
- Verify cancelling a leader task wakes waiting followers without stalling.
- Verify `timeout_ms=10` raises `TimeoutError` promptly when all slots remain held.
- Verify `lease.release_sync()` (used by `AsyncPostgreSQLDriver` drop releaser!) wakes local async waiters immediately.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py && uv run --no-sync mypy src/py/vertebra/extensions/broker/transport.py && uv run --no-sync ruff check src/py/vertebra/extensions/broker/transport.py
```

## Acceptance Criteria

- Contended `AsyncSHMBrokerTransport.acquire()` uses zero `SyncPool` threads when backed by native `LeaseBroker`.
- Existing mock-broker tests continue passing without regressions.
- `ruff`, `mypy`, and `pyright` pass cleanly.

## Notes & Discoveries

- Note that `AsyncPostgreSQLDriver._single_call_checked` releases leases synchronously via `_release_lease` -> `lease.release_sync()` -> `transport._release_sync(lease_id)` -> `transport._release_impl(lease_id)`. Wiring `_on_slot_freed` inside `_release_impl` and `_dispose_quarantined_impl` ensures *both* async `await transport.release(...)` and sync `lease.release_sync()` immediately wake queued async waiters on the event loop.

[^src-py-broker-transport]: `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` — AsyncSHMBrokerTransport acquire, release, and dispose_quarantined paths.
[^src-sa-pool]: `src/py/vertebra/extensions/sqlalchemy/pool.py::AsyncBrokerPool` — SQLAlchemy AsyncBrokerPool consumer of AsyncSHMBrokerTransport.
