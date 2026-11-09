---
type: Task
id: "native-duckdb-driver:t03"
title: "Cut over Python SyncDuckDBDriver and AsyncDuckDBDriver to vertebra._duckdb without ThreadOffloadAsyncDatabaseDriver"
description: "Cut src/py/vertebra/db/adapters/duckdb/driver.py over from PyPI duckdb and sqlite3 fallback to ensure_module('vertebra._duckdb', package='vertebra-core[duckdb]', extra='duckdb') delegating directly to SyncDuckDBClient and AsyncDuckDBClient with AsyncDatabaseDriver[DuckDBConfig] inheritance and zero asyncio.to_thread."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t02"
read_set:
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/db/driver/_async.py
files:
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/arrow.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 450
plan_revision: 4
plan_commit: d7a629b1
state_revision: 2
claimed_by: agent
claimed_at: "2026-11-09T14:40:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T16:30:00Z"
last_verified_commit: 657ce621
verification_evidence: "pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/storage/test_gcs_token_refresh.py -k duckdb"
created_at: "2026-11-09T02:52:00Z"
updated_at: "2026-11-09T16:30:00Z"
commit: null
sources:
  - id: duckdb-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module
  - id: duckdb-core-py
    resource: src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore
    title: DuckDBCore URI, pragma, and GCS secret SQL builder helpers
  - id: duckdb-config-py
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig
    title: DuckDBConfig immutable configuration and cgroup-clamped memory settings
tags:
  - python
  - duckdb
  - driver
  - pyo3
---

# Task t03: Cut Over Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` to `vertebra._duckdb` Without `ThreadOffloadAsyncDatabaseDriver`

## Objective & Scope Boundary

Cut `src/py/vertebra/db/adapters/duckdb/driver.py` (`SyncDuckDBDriver`, `AsyncDuckDBDriver`) over from the PyPI `duckdb` package and any `sqlite3` fallback to `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")` delegating directly to `SyncDuckDBClient` and `AsyncDuckDBClient` while preserving the full `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract [^duckdb-py] [^duckdb-core-py] [^duckdb-config-py]. `AsyncDuckDBDriver` inherits directly from `AsyncDatabaseDriver[DuckDBConfig]` without `ThreadOffloadAsyncDatabaseDriver` or `asyncio.to_thread`.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/duckdb/driver.py`:
  - `SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig])`: delegates to `vertebra._duckdb.SyncDuckDBClient`, exposes `cursor()`, `execute()`, `execute_many()`, `execute_tuples()`, `execute_with_description()`, `select()`, `select_one()`, `select_one_or_none()`, `select_value()`, `select_with_total()`, `select_to_arrow()`, `stream_arrow()`, `stream_arrow_batches()`, `load_from_arrow()`, `begin()`, `commit()`, `rollback()`, `savepoint()`, `rollback_to_savepoint()`, `release_savepoint()`, `pipeline()`, `apply_gcs_secret()`, `cancel()`, `close()`, and raises `MissingDependencyError` (`package="vertebra-core[duckdb]"`, `extra="duckdb"`) when `vertebra._duckdb` is unavailable.
  - `AsyncDuckDBDriver(AsyncDatabaseDriver[DuckDBConfig])`: inherits directly from `AsyncDatabaseDriver[DuckDBConfig]` and delegates to `vertebra._duckdb.AsyncDuckDBClient` (`AsyncCompletionQueue` + `Notifier`) natively without `asyncio.to_thread` or `ThreadOffloadAsyncDatabaseDriver`.

## Acceptance Criteria

- [x] `SyncDuckDBDriver` and `AsyncDuckDBDriver` delegate to `vertebra._duckdb` (`SyncDuckDBClient`, `AsyncDuckDBClient`) with zero PyPI `duckdb`, `sqlite3`, `ThreadOffloadAsyncDatabaseDriver`, or `asyncio.to_thread` references.
- [x] `MissingDependencyError` with `package="vertebra-core[duckdb]"` and `extra="duckdb"` is raised when `vertebra._duckdb` cannot be imported.
- [x] `SyncDuckDBDriver` and `AsyncDuckDBDriver` support `GcpTokenCache` (`SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenCacheCapsule`) and callable token providers in `apply_gcs_secret`.

[^duckdb-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` target cutover module.
[^duckdb-core-py]: `src/py/vertebra/db/adapters/duckdb/core.py::DuckDBCore` — `DuckDBCore` URI, pragma, and GCS secret SQL builder helpers.
[^duckdb-config-py]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig` — `DuckDBConfig` immutable configuration and cgroup-clamped memory settings.
