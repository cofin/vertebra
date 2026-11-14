---
type: Task
id: native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t02
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: Cut SyncSQLiteDriver and AsyncSQLiteDriver Over to vertebra._sqlite
description: Replace stdlib sqlite3 and ThreadOffloadAsyncDatabaseDriver with native SyncSQLiteClient/AsyncSQLiteClient, pipelines, and backup hooks.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t01
files:
- src/py/vertebra/db/adapters/sqlite/driver.py
- src/py/vertebra/db/adapters/sqlite/config.py
tests:
- src/py/tests/unit/db/test_sqlite_driver.py
- src/py/tests/unit/db/test_enterprise_olap_drivers.py
read_set:
- src/py/vertebra/db/adapters/sqlite/driver.py
- src/py/vertebra/db/adapters/sqlite/config.py
- src/py/vertebra/db/adapters/duckdb/driver.py
- src/rs/vertebra-db/src/py_async.rs
- src/py/vertebra/_sqlite.pyi
estimated_loc: 245
tags:
- sqlite
- driver
- async
- arrow
sources:
- id: sqlite-drv
  resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
  title: Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3
- id: sqlite-cfg
  resource: src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig
  title: Python SQLiteConfig struct
- id: duckdb-lib
  resource: src/rs/vertebra-duckdb/src/lib.rs
  title: Embedded worker-thread and VTBARROW PyO3 extension precedent
---

# Task `t02`: Cut SyncSQLiteDriver and AsyncSQLiteDriver Over to vertebra._sqlite

## Objective & Scope Boundary

Rewrite `SyncSQLiteDriver` and `AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig])` onto `vertebra._sqlite` with zero `import sqlite3` per [`../spec.md` §2](../spec.md) [^sqlite-drv] [^sqlite-cfg] [^duckdb-lib].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/sqlite/driver.py` — Current stdlib `sqlite3` `SyncSQLiteDriver` and `ThreadOffloadAsyncDatabaseDriver` `AsyncSQLiteDriver`.
- `src/py/vertebra/db/adapters/sqlite/config.py` — `SQLiteConfig` fields.
- `src/py/vertebra/db/adapters/duckdb/driver.py` — Direct `AsyncDatabaseDriver` + `spawn_rust_db_task_with_attr_and_cancel` pattern.
- `src/rs/vertebra-db/src/py_async.rs` — Rust async completion drainer registration and `spawn_rust_db_task_with_attr_and_cancel` contract.
- `src/py/vertebra/_sqlite.pyi` — `SyncSQLiteClient` and `AsyncSQLiteClient` stubs.

## Contracts & Edge Cases

- Probe `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")` in `__init__`.
- Implement `pipeline()`, `backup_to()`, `restore_from()`, `select_to_arrow()`, `stream_arrow_batches()`, `load_from_arrow()`, `execute_script()`, and `cancel()` on both `SyncSQLiteDriver` and `AsyncSQLiteDriver`.
- **Edge Case**: Missing `vertebra._sqlite` raises `MissingDependencyError` naming `vertebra-core[sqlite]`.
- **Edge Case**: Async task cancellation triggers `AsyncSQLiteClient.cancel_operation(op_id)` (`InterruptHandle::interrupt()`).

## Test Matrix

- `@pytest.mark.parametrize` covers missing-wheel `MissingDependencyError`, `JSONB` round-trips, extended constraint errors, active `cancel()`, `backup_to`/`restore_from`, and zero-copy `VTBARROW` `ArrowCursor`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/sqlite
```

## Acceptance Criteria

- [ ] Zero `import sqlite3` remains in `src/py/vertebra/db/adapters/sqlite/`.
- [ ] `SyncSQLiteDriver` and `AsyncSQLiteDriver` pass unit, pipeline, backup, cancel, and Arrow tests.
- [ ] Zero regressions in the fast verification gate.

[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
[^sqlite-cfg]: `src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig` — Python SQLiteConfig struct.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
