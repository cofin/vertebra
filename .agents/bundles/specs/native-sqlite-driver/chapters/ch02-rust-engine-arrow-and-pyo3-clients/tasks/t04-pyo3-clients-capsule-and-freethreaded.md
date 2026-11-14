---
type: Task
id: native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t04
flow_id: native-sqlite-driver
chapter_id: ch02-rust-engine-arrow-and-pyo3-clients
title: Expose SyncSQLiteClient, AsyncSQLiteClient, Broker Capsule, and Free-Threaded Verification
description: Bind SyncSQLiteClient, AsyncSQLiteClient, and broker_engine_capsule (tag 3) in vertebra._sqlite and verify GIL_USED = False and interrupt cancellation.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t03
files:
- src/rs/vertebra-sqlite/src/py_client.rs
- src/rs/vertebra-sqlite/src/lib.rs
- src/py/vertebra/_sqlite.pyi
- src/py/vertebra/extensions/broker/engines.py
tests:
- src/rs/vertebra-sqlite/tests/sqlite_client.rs
- src/py/tests/unit/test_freethreaded_gil.py
read_set:
- src/rs/vertebra-db/src/py_async.rs
- src/rs/vertebra-db/src/lib.rs
- src/py/vertebra/_sqlite.pyi
- src/rs/vertebra-sqlite/src/py_client.rs
- src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 245
tags:
- sqlite
- pyo3
- capsule
- async
- freethreaded
- concurrency
- tests
sources:
- id: py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: AsyncCompletionQueue and Notifier PyO3 bridge
- id: duckdb-pyi
  resource: src/py/vertebra/_duckdb.pyi
  title: Standalone extension stub precedent
- id: ft-gil-test
  resource: src/py/tests/unit/test_freethreaded_gil.py
  title: Free-threaded Python 3.14t/3.15t GIL_USED verification test
---

# Task `t04`: Expose SyncSQLiteClient, AsyncSQLiteClient, Broker Capsule, and Free-Threaded Verification

## Objective & Scope Boundary

Implement `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`) in `vertebra-sqlite` and synchronize `src/py/vertebra/_sqlite.pyi` per [`../spec.md` §2](../spec.md) [^py-async] [^duckdb-pyi].

Add `"vertebra._sqlite"` to `BrokerEngineDiscovery.ENGINE_MODULES`, register `vertebra._sqlite` in `test_freethreaded_gil.py`, and verify recursive-CTE `InterruptHandle` cancellation and multi-thread WAL concurrency per [`../spec.md` §2](../spec.md) [^ft-gil-test].

## Bounded Read Manifest

- `src/rs/vertebra-db/src/py_async.rs` — `SyncCompletionQueue`, `AsyncCompletionQueue`, and `Notifier`.
- `src/rs/vertebra-db/src/lib.rs` — `BrokerPoolBackend` and `new_broker_engine_pycapsule`.
- `src/py/vertebra/_sqlite.pyi` — Type stubs for `vertebra._sqlite`.

- `src/rs/vertebra-sqlite/src/py_client.rs` — PyO3 SQLite client classes from `t03`.
- `src/py/tests/unit/test_freethreaded_gil.py` — Extension `GIL_USED == False` and `frozen` class checks (`TEST-10`).

## Contracts & Edge Cases

- Release the GIL (`py.detach`) on all `SyncSQLiteClient` waits and drive `AsyncSQLiteClient` completions via `AsyncCompletionQueue` + `Notifier` + `cancel_operation(op_id)`.
- Export `broker_engine_capsule()` implementing `BrokerPoolBackend` (`DIALECT_TAG_SQLITE = 3`) from `broker-pool-sharing`.
- **Edge Case**: Dropping or closing a client shuts down worker threads deterministically without leaking `Notifier` file descriptors.

- Include `vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`) in `test_freethreaded_gil.py`.
- Assert `BrokerEngineDiscovery.register_installed()` registers `DIALECT_TAG_SQLITE = 3`.
- **Edge Case**: Recursive CTE `WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt` aborts within `< 50ms` via `cancel()` / `cancel_operation(op_id)` without corrupting subsequent queries on the connection.

## Test Matrix

- `mypy.stubtest vertebra._sqlite` verifies 100% signature parity across `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule`.

- Barrier-synchronized multi-thread WAL read/write test (zero `sleep` synchronization, `TEST-04`), recursive-CTE interrupt test, and `test_freethreaded_gil.py`.

## Verification

```bash
make sqlite-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q
```

## Acceptance Criteria

- [ ] `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` pass `stubtest` and Rust client tests.
- [ ] `make sqlite-test` and `test_freethreaded_gil.py` pass with `GIL_USED == False`.
- [ ] Zero regressions in the fast verification gate.

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^duckdb-pyi]: `src/py/vertebra/_duckdb.pyi` — Standalone extension stub precedent.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED verification test.
