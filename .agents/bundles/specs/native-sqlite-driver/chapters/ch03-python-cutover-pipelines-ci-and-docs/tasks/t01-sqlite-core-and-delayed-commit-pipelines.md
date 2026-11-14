---
type: Task
id: native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t01
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: Fix SQLiteCore Pragma Bug, Clean Up _vertebra, and Implement Delayed-Commit Pipelines
description: Fix 5-arg TypeError in SQLiteCore.initialization_pragmas, delete sqlite_initialization_pragmas from vertebra-py/_vertebra.pyi, and implement SyncSQLitePipeline and AsyncSQLitePipeline.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t04
files:
- src/py/vertebra/db/adapters/sqlite/core.py
- src/rs/vertebra-py/src/db.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/db/adapters/sqlite/pipeline.py
- src/py/vertebra/db/adapters/sqlite/__init__.py
tests:
- src/py/tests/unit/db/test_sqlite_driver.py
read_set:
- src/py/vertebra/db/adapters/sqlite/core.py
- src/rs/vertebra-py/src/db.rs
- src/py/vertebra/_vertebra.pyi
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/adapters/sqlite/__init__.py
- src/py/vertebra/_sqlite.pyi
estimated_loc: 220
tags:
- sqlite
- core
- debloat
- pipeline
- capabilities
sources:
- id: sqlite-core
  resource: src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore
  title: SQLiteCore pragma helper with 5-arg call defect
- id: vertebra-py-db
  resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
  title: Legacy sqlite_initialization_pragmas binding in vertebra-py
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Core vertebra._vertebra stub declarations
- id: driver-caps
  resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
  title: SyncPipeline and AsyncPipeline capability protocols
- id: sqlite-drv
  resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
  title: Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3
---

# Task `t01`: Fix SQLiteCore Pragma Bug, Clean Up _vertebra, and Implement Delayed-Commit Pipelines

## Objective & Scope Boundary

Fix `SQLiteCore.initialization_pragmas` directly and delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `_vertebra.pyi` per [`../spec.md` §2](../spec.md) [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi].

Create `SyncSQLitePipeline` and `AsyncSQLitePipeline` in `src/py/vertebra/db/adapters/sqlite/pipeline.py` and lazy-export them in `sqlite/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^sqlite-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/sqlite/core.py` — Broken 5-arg `native_fn` call and `contextlib.suppress(TypeError)`.
- `src/rs/vertebra-py/src/db.rs` — `sqlite_initialization_pragmas` PyO3 function.
- `src/py/vertebra/_vertebra.pyi` — `sqlite_initialization_pragmas` stub.

- `src/py/vertebra/db/driver/capabilities.py` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable`.
- `src/py/vertebra/db/adapters/sqlite/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_sqlite.pyi` — `execute_pipeline` signatures on `SyncSQLiteClient` and `AsyncSQLiteClient`.

## Contracts & Edge Cases

- Remove `contextlib.suppress(TypeError)` and `_vertebra.sqlite_initialization_pragmas` lookup from `SQLiteCore.initialization_pragmas`.
- Delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi`.
- **Edge Case**: Invalid `synchronous` values raise `ValueError` consistently.

- Conform to `@runtime_checkable` `SyncPipeline` and `AsyncPipeline` (`execute`, `select`, `results`, context managers).
- Flush queued operations in one `execute_pipeline` worker turn: wrap in delayed-commit `BEGIN IMMEDIATE ... COMMIT` when idle, or `SAVEPOINT ... RELEASE SAVEPOINT` (`ROLLBACK TO SAVEPOINT` on error) when nested inside an active transaction.
- **Edge Case**: Calling `results()` on an empty pipeline returns `[]` without starting a transaction.
- **Edge Case**: Unhandled exception inside `with driver.pipeline():` discards un-flushed operations cleanly.

## Test Matrix

- `@pytest.mark.parametrize` verifies `SQLiteCore.initialization_pragmas` for WAL/non-WAL, `:memory:`, and `foreign_keys` combinations; `mypy.stubtest vertebra._vertebra` passes.

- `@pytest.mark.parametrize` verifies standalone delayed-commit atomicity, nested savepoint rollback preserving outer transaction rows, and `isinstance(..., SyncPipeline | AsyncPipeline)`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q -k pipeline && uv run --no-sync ruff check src/py/vertebra/db/adapters/sqlite
```

## Acceptance Criteria

- [ ] `sqlite_initialization_pragmas` is removed from `_vertebra` and `SQLiteCore.initialization_pragmas` has zero silent `TypeError` fallbacks.
- [ ] `SyncSQLitePipeline` and `AsyncSQLitePipeline` satisfy capability protocols and nested savepoint semantics.
- [ ] Zero regressions in the fast verification gate.

[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline and AsyncPipeline capability protocols.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
