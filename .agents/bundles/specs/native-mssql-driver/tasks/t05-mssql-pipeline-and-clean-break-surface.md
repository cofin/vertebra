---
type: Task
id: t05
flow_id: native-mssql-driver
title: "Implement SyncMSSQLPipeline / AsyncMSSQLPipeline and clean-break PyO3 method surface (PY-07)"
description: "Remove duplicate begin_transaction / commit_transaction / rollback_transaction aliases on SyncMSSQLClient and AsyncMSSQLClient, add execute_with_description (preserving 0-row COLMETADATA column names), connect, and __repr__, complete src/py/vertebra/_mssql.pyi for zero-error mypy.stubtest, and implement SyncMSSQLPipeline and AsyncMSSQLPipeline in src/py/vertebra/db/adapters/mssql/pipeline.py with SAVE TRANSACTION savepoint nesting."
status: stable
state: completed
verification_strategy: tdd
depends_on:
  - t04
plan_revision: 1
state_revision: 2
commit_sha: null
claimed_at: "2026-11-09T16:03:00Z"
completed_at: "2026-11-09T16:12:00Z"
created_at: "2026-11-09T15:36:00Z"
updated_at: "2026-11-09T16:12:00Z"
reads:
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/_mssql.pyi
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/tests/integration/db/test_driver_conformance.py
targets:
  - src/rs/vertebra-mssql/src/lib.rs
  - src/py/vertebra/_mssql.pyi
  - src/py/vertebra/db/adapters/mssql/pipeline.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/tests/unit/db/test_mssql_driver.py
  - src/py/tests/integration/db/test_driver_conformance.py
sources:
  - id: mssql-lib
    resource: src/rs/vertebra-mssql/src/lib.rs
    title: PyO3 module bindings for vertebra._mssql
  - id: capabilities-py
    resource: src/py/vertebra/db/driver/capabilities.py
    title: SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols
---

# Task t05: Implement `SyncMSSQLPipeline` / `AsyncMSSQLPipeline` and clean-break PyO3 method surface (`PY-07`)

<!-- truth: start -->
- **Clean-Break Method Surface (`PY-07`) & 0-Row `COLMETADATA`**: `SyncMSSQLClient` and `AsyncMSSQLClient` expose canonical `begin(isolation_level=None)`, `commit()`, `rollback()`, `savepoint(name)`, `rollback_to_savepoint(name)`, `release_savepoint(name)`, `execute_with_description(sql, parameters=None)` (preserving `COLMETADATA` column names on `0` rows), `connect()`, `in_transaction`, `routing_destination`, and `__repr__()` without duplicate `*_transaction` aliases, with full parity in `src/py/vertebra/_mssql.pyi` verified by `mypy.stubtest vertebra._mssql` [^mssql-lib].
- **Transactional Query Pipelining (`SyncMSSQLPipeline` / `AsyncMSSQLPipeline`)**: `SyncMSSQLDriver.pipeline()` and `AsyncMSSQLDriver.pipeline()` implement `SyncPipelineCapable` and `AsyncPipelineCapable`, holding a delayed-commit transaction or nested `SAVE TRANSACTION _vtb_pipe_<n>` savepoint across `results()`, committing only on clean `__exit__`/`__aexit__`, and rolling back on statement failure, post-`results()` caller exception, or unexecuted queued operations [^capabilities-py].
<!-- truth: end -->

## Context

Aligns `vertebra._mssql` with `PY-07` clean-break method naming (removing `begin_transaction`, `commit_transaction`, `rollback_transaction`), ensures `src/py/vertebra/_mssql.pyi` declares every runtime symbol called by `driver.py` (`in_transaction`, `routing_destination`, `connect`, `query`, `query_raw`, `execute_with_description`, `__repr__`), verifies 0-row `COLMETADATA` column preservation, and implements `SyncMSSQLPipeline` and `AsyncMSSQLPipeline` in `src/py/vertebra/db/adapters/mssql/pipeline.py`.

## Public Seam Contract

- `vertebra._mssql`: `SyncMSSQLClient` and `AsyncMSSQLClient` (`begin(isolation_level=None)`, `commit()`, `rollback()`, `execute_with_description(sql, parameters=None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`, `connect()`, `in_transaction`, `routing_destination`, `__repr__()`).
- `vertebra.db.adapters.mssql.pipeline`: `SyncMSSQLPipeline` and `AsyncMSSQLPipeline` implementing `SyncPipeline` and `AsyncPipeline`.
- `SyncMSSQLDriver.pipeline() -> SyncMSSQLPipeline` and `AsyncMSSQLDriver.pipeline() -> AsyncMSSQLPipeline` (`supports_pipeline=True` in `test_driver_conformance.py`).

## Implementation Blueprint

1. Remove `begin_transaction`, `commit_transaction`, and `rollback_transaction` from `PySyncMSSQLClient` and `PyAsyncMSSQLClient` in `src/rs/vertebra-mssql/src/lib.rs`, add `execute_with_description`, `connect`, and `__repr__`, and update `src/py/vertebra/_mssql.pyi` to match 1:1.
2. Create `src/py/vertebra/db/adapters/mssql/pipeline.py` implementing `SyncMSSQLPipeline` and `AsyncMSSQLPipeline` with delayed commit on `__exit__`/`__aexit__` and `SAVE TRANSACTION _vtb_pipe_<n>` nesting when `driver.in_transaction` is already active.
3. Wire `.pipeline()` on `SyncMSSQLDriver` and `AsyncMSSQLDriver`, export `SyncMSSQLPipeline` and `AsyncMSSQLPipeline` in `src/py/vertebra/db/adapters/mssql/__init__.py`, and set `supports_pipeline=True` on the `mssql` entry in `src/py/tests/integration/db/test_driver_conformance.py`.
4. Add unit and loopback tests for `SyncMSSQLPipeline`, `AsyncMSSQLPipeline` (including rollback on statement error, post-`results()` caller exception, unexecuted queue, and nested savepoints), and 0-row `execute_with_description` `COLMETADATA` preservation in `src/py/tests/unit/db/test_mssql_driver.py`.

## Acceptance Criteria

- [x] `mypy.stubtest vertebra._mssql` passes with 0 errors.
- [x] `pytest src/py/tests/unit/db/test_mssql_driver.py src/py/tests/integration/db/test_driver_conformance.py` passes.

[^mssql-lib]: `src/rs/vertebra-mssql/src/lib.rs` — PyO3 module bindings for vertebra._mssql.
[^capabilities-py]: `src/py/vertebra/db/driver/capabilities.py` — SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols.
