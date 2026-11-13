---
type: Task
id: "native-sqlite-driver:ch03:t04"
flow_id: native-sqlite-driver
chapter_id: ch03-worker-pool-arrow-and-pyo3-clients
title: "Broker Discovery Wiring, Free-Threaded GIL Check, and Interrupt/Concurrency Suite"
description: "Add vertebra._sqlite to BrokerEngineDiscovery and verify 3.14t/3.15t GIL safety, <50ms CTE cancellation, and WAL concurrency."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch03:t03"
files:
  - src/py/vertebra/extensions/broker/engines.py
tests:
  - src/rs/vertebra-sqlite/tests/sqlite_client.rs
  - src/py/tests/unit/test_freethreaded_gil.py
read_set:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/py/tests/unit/test_freethreaded_gil.py
estimated_loc: 185
tags:
  - sqlite
  - freethreaded
  - concurrency
  - tests
sources:
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Free-threaded Python 3.14t/3.15t GIL_USED verification test"
---

# Task `t04`: Broker Discovery Wiring, Free-Threaded GIL Check, and Interrupt/Concurrency Suite

## Objective & Scope Boundary

Add `"vertebra._sqlite"` to `BrokerEngineDiscovery.ENGINE_MODULES`, register `vertebra._sqlite` in `test_freethreaded_gil.py`, and verify recursive-CTE `InterruptHandle` cancellation and multi-thread WAL concurrency per [`../spec.md` §2](../spec.md) [^ft-gil-test].

## Bounded Read Manifest

- `src/rs/vertebra-sqlite/src/py_client.rs` — PyO3 SQLite client classes from `t03`.
- `src/py/tests/unit/test_freethreaded_gil.py` — Extension `GIL_USED == False` and `frozen` class checks (`TEST-10`).

## Contracts & Edge Cases

- Include `vertebra._sqlite` (`SyncSQLiteClient`, `AsyncSQLiteClient`) in `test_freethreaded_gil.py`.
- Assert `BrokerEngineDiscovery.register_installed()` registers `DIALECT_TAG_SQLITE = 3`.
- **Edge Case**: Recursive CTE `WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt) SELECT count(*) FROM cnt` aborts within `< 50ms` via `cancel()` / `cancel_operation(op_id)` without corrupting subsequent queries on the connection.

## Test Matrix

- Barrier-synchronized multi-thread WAL read/write test (zero `sleep` synchronization, `TEST-04`), recursive-CTE interrupt test, and `test_freethreaded_gil.py`.

## Verification

```bash
make sqlite-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q
```

## Acceptance Criteria

- [ ] `make sqlite-test` and `test_freethreaded_gil.py` pass with `GIL_USED == False`.
- [ ] Zero regressions in the chapter fast verification gate.

[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED verification test.
