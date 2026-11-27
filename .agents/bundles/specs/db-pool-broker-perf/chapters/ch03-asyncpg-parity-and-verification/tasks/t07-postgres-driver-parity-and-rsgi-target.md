---
type: Task
id: t07
chapter_id: ch03-asyncpg-parity-and-verification
title: Align NativePostgresWorldDriver checkout parity and add granian-rsgi-data-stdlib
description: Update NativePostgresWorldDriver multi-query async methods to check out one pooled connection and broker lease per request matching asyncpg, register granian-rsgi-data-stdlib on port 8170, and add offline unit tests.
status: stable
state: pending
priority: high
verification_strategy: behavior_tdd
worksheet_format: compact
depends_on: []
read_set:
  - tools/benchmarks/apps/_db_drivers.py
  - tools/benchmarks/targets.py
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
files:
  - tools/benchmarks/apps/_db_drivers.py
  - tools/benchmarks/targets.py
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
tests:
  - src/py/tests/unit/tools/test_e2e_benchmarks.py
estimated_loc: 120
plan_revision: 2
plan_commit: "16ccb56e"
commit: null
sources:
  - id: src-bench-drivers
    resource: tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver
    title: NativePostgresWorldDriver and StdlibPostgresWorldDriver
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: Benchmark target registry
  - id: src-bench-tests
    resource: src/py/tests/unit/tools/test_e2e_benchmarks.py
    title: Offline benchmark target and driver tests
tags:
  - benchmarks
  - postgres
  - asyncpg
---

# t07: Align NativePostgresWorldDriver checkout parity and add granian-rsgi-data-stdlib

## Objective & Scope Boundary

Match `NativePostgresWorldDriver` multi-query checkout semantics 1:1 with `StdlibPostgresWorldDriver` (`asyncpg`) and add the `granian-rsgi-data-stdlib` baseline target.

In scope:
- `tools/benchmarks/apps/_db_drivers.py`:
  - In `NativePostgresWorldDriver._ensure_async_driver`, call `await driver.connect()` so `driver.pool` is initialized once per event loop.
  - Add helper `_checkout_async_client(driver)` acquiring optional single `self.async_broker.acquire(...)` lease (only when `self.async_broker is not None`, never double-acquiring because `driver.pool` is the raw Rust `AsyncPostgresPool`) + `client = await driver.pool.acquire()`, releasing via `await client.close()` under `CancelScope(shield=True)` (Rust handles return or dirty discard without `pool.evict()`) and releasing the broker lease in `finally`.
  - Use `_checkout_async_client` in `fetch_worlds_async` (1 checkout + 20 `await client.fetch_dicts(...)`) and `update_worlds_async` (1 checkout + `begin` / sorted `row_id` `fetch_dicts` + `execute_dml` / `commit`, preserving returned row order). Also sort `updates` by `row_id` inside `StdlibPostgresWorldDriver.update_worlds_async` to avoid `40P01` lock-order deadlocks under `c=128`.
- `tools/benchmarks/targets.py`: add `granian-rsgi-data-stdlib` on port `8170` per `../spec.md` Section 2.2.
- `src/py/tests/unit/tools/test_e2e_benchmarks.py`: assert `granian-rsgi-data-stdlib` exists in `ALL_TARGETS` and `DATA_BASELINE_TARGETS` with unique port `8170`, and add a fake-pool + fake-broker unit test verifying `NativePostgresWorldDriver.fetch_worlds_async([1, 2, 3])` and `update_worlds_async([(5, 10), (2, 20)])` acquire and release exactly 1 broker lease and 1 pool connection per call, execute updates in ascending `row_id` order, and roll back + close + release on exception.

## Bounded Read Manifest

- `tools/benchmarks/apps/_db_drivers.py` (lines 630-760, 880-915) [^src-bench-drivers]
- `tools/benchmarks/targets.py` (lines 995-1078) [^src-bench-targets]
- `src/py/tests/unit/tools/test_e2e_benchmarks.py` (data target assertions) [^src-bench-tests]

## Contracts & Edge Cases

- On exception inside `update_worlds_async`, call `await client.rollback()` (suppressing secondary errors), close `client` via `await client.close()` under `CancelScope(shield=True)` (no non-existent `pool.evict()`), and release the broker lease in `finally`.
- Sorting updates by `row_id` before issuing `UPDATE` inside the transaction prevents PostgreSQL `40P01` deadlocks under concurrent `c=128` load.
- Zero inline `#` comments in Python files.

## Test Matrix

- `test_granian_rsgi_data_stdlib_target_registered`
- `test_native_postgres_world_driver_single_checkout_and_single_lease_per_batch`
- `test_native_postgres_world_driver_update_rollback_releases_lease_on_error`

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/tools/test_e2e_benchmarks.py src/py/tests/unit/tools/test_adapter_benchmarks_sql.py && uv run --no-sync ruff check tools/benchmarks/apps/_db_drivers.py tools/benchmarks/targets.py src/py/tests/unit/tools/test_e2e_benchmarks.py && uv run --no-sync mypy tools/benchmarks/apps/_db_drivers.py tools/benchmarks/targets.py
```

## Acceptance Criteria

- All offline benchmark unit tests pass with zero warnings.
- `NativePostgresWorldDriver` acquires exactly 1 connection and 1 broker lease per `fetch_worlds_async` / `update_worlds_async` call.

[^src-bench-drivers]: `tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver` — NativePostgresWorldDriver and StdlibPostgresWorldDriver.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — Benchmark target registry.
[^src-bench-tests]: `src/py/tests/unit/tools/test_e2e_benchmarks.py` — Offline benchmark target and driver tests.
