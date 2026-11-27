---
type: Task
id: t06
chapter_id: ch02-async-shm-broker-contention
title: Add async SHM broker contention, cancellation, and cross-process wake tests
description: Add unit and multiprocessing tests proving zero SyncPool submissions under c=128 contention, leader cancellation handoff, release_sync local wakeup, cross-process wait_for_wake_async wakeup, and partitioned fairness.
status: stable
state: pending
priority: high
verification_strategy: regression_tdd
worksheet_format: compact
depends_on:
  - t04
  - t05
read_set:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py
files:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
tests:
  - src/py/tests/unit/extensions/test_shm_lease_broker.py
estimated_loc: 150
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-broker-tests
    resource: src/py/tests/unit/extensions/test_shm_lease_broker.py
    title: SHM lease broker unit and concurrency tests
tags:
  - broker
  - shm
  - testing
  - asyncio
---

# t06: Add async SHM broker contention, cancellation, and cross-process wake tests

## Objective & Scope Boundary

Verify the lock-free local handoff, single-leader watcher, cancellation safety, and cross-process futex wakeups of `AsyncSHMBrokerTransport` and `LeaseBroker.wait_for_wake_async`.

In scope:
- `src/py/tests/unit/extensions/test_shm_lease_broker.py`: add deterministic unit tests covering:
  - `c=128` concurrent async acquires on a 10-slot broker (monkeypatching `SyncPool.default().submit_async` to fail if called during `acquire()`, proving zero `SyncPool` usage) with `active_leases == 0` afterward.
  - Leader cancellation while multiple followers wait: cancelling the first contended waiter does not stall remaining waiters when a slot is released.
  - `lease.release_sync()` waking a queued async waiter on the same event loop without waiting for a timeout slice.
  - Cross-thread / cross-process release waking an `AsyncSHMBrokerTransport` waiter over a shared named SHM segment via `wait_for_wake_async`.
  - Multi-partition (`2` partitions) release waking a waiter blocked on partition B after partition A releases (`partition_count > 1` fallback to `wake_all()`).
  - Zero-timeout (`timeout_ms=0`) immediate `TimeoutError` and bounded `timeout_ms=20` expiration with zero leaked waiters.

## Bounded Read Manifest

- `src/py/tests/unit/extensions/test_shm_lease_broker.py` (tail section and async fixtures) [^src-broker-tests]

## Contracts & Edge Cases

- Clean up any named SHM segments in `finally` via `transport.unlink()` to prevent `/dev/shm` leaks.
- Zero inline `#` comments in Python test code.

## Test Matrix

- `test_async_broker_c128_contention_bypasses_sync_pool`
- `test_async_broker_leader_cancel_promotes_follower`
- `test_async_broker_release_sync_wakes_local_waiter`
- `test_async_broker_cross_process_wake_via_named_shm`
- `test_async_broker_multi_partition_waiters_no_starvation`
- `test_async_broker_zero_and_short_timeout_cleanup`

## Verification

```bash
make dev-ext EXT=core && cargo test -p vertebra-core broker && cargo test -p vertebra-py broker && uv run --no-sync pytest src/py/tests/unit/extensions/test_shm_lease_broker.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_broker_proxy.py && uv run --no-sync python -m mypy.stubtest vertebra._vertebra
```

## Acceptance Criteria

- All new and existing broker Rust and Python tests pass cleanly with zero warnings or leaked SHM files.
- `ruff`, `mypy`, `pyright`, and `mypy.stubtest vertebra._vertebra` exit `0`.

[^src-broker-tests]: `src/py/tests/unit/extensions/test_shm_lease_broker.py` — SHM lease broker unit and concurrency tests.
