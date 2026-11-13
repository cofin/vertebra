---
type: Task
id: "native-sqlite-driver:ch04:t02"
flow_id: native-sqlite-driver
chapter_id: ch04-python-cutover-pipelines-and-ci
title: "Delayed-Commit SyncSQLitePipeline and AsyncSQLitePipeline with Savepoint Nesting"
description: "Implement SyncSQLitePipeline and AsyncSQLitePipeline batching queued statements with BEGIN IMMEDIATE or nested SAVEPOINT."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch04:t01"
files:
  - src/py/vertebra/db/adapters/sqlite/pipeline.py
  - src/py/vertebra/db/adapters/sqlite/__init__.py
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/sqlite/__init__.py
  - src/py/vertebra/_sqlite.pyi
estimated_loc: 185
tags:
  - sqlite
  - pipeline
  - capabilities
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: "SyncPipeline and AsyncPipeline capability protocols"
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
    title: "Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3"
---

# Task `t02`: Delayed-Commit SyncSQLitePipeline and AsyncSQLitePipeline with Savepoint Nesting

## Objective & Scope Boundary

Create `SyncSQLitePipeline` and `AsyncSQLitePipeline` in `src/py/vertebra/db/adapters/sqlite/pipeline.py` and lazy-export them in `sqlite/__init__.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^sqlite-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable`.
- `src/py/vertebra/db/adapters/sqlite/__init__.py` — Lazy package exports (`PY-06`).
- `src/py/vertebra/_sqlite.pyi` — `execute_pipeline` signatures on `SyncSQLiteClient` and `AsyncSQLiteClient`.

## Contracts & Edge Cases

- Conform to `@runtime_checkable` `SyncPipeline` and `AsyncPipeline` (`execute`, `select`, `results`, context managers).
- Flush queued operations in one `execute_pipeline` worker turn: wrap in delayed-commit `BEGIN IMMEDIATE ... COMMIT` when idle, or `SAVEPOINT ... RELEASE SAVEPOINT` (`ROLLBACK TO SAVEPOINT` on error) when nested inside an active transaction.
- **Edge Case**: Calling `results()` on an empty pipeline returns `[]` without starting a transaction.
- **Edge Case**: Unhandled exception inside `with driver.pipeline():` discards un-flushed operations cleanly.

## Test Matrix

- `@pytest.mark.parametrize` verifies standalone delayed-commit atomicity, nested savepoint rollback preserving outer transaction rows, and `isinstance(..., SyncPipeline | AsyncPipeline)`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q -k pipeline && uv run --no-sync ruff check src/py/vertebra/db/adapters/sqlite
```

## Acceptance Criteria

- [ ] `SyncSQLitePipeline` and `AsyncSQLitePipeline` satisfy capability protocols and nested savepoint semantics.
- [ ] Zero regressions in the chapter fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline and AsyncPipeline capability protocols.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
