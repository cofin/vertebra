---
type: Task
id: "native-duckdb-driver:t03"
title: "Cut over Python SyncDuckDBDriver and AsyncDuckDBDriver to vertebra._duckdb, activate conformance, and sync documentation"
description: "Cut src/py/vertebra/db/adapters/duckdb/driver.py over from PyPI duckdb and sqlite3 fallback to ensure_module('vertebra._duckdb', package='vertebra-core[duckdb]', extra='duckdb') delegating to SyncDuckDBClient and AsyncDuckDBClient, activate duckdb in test_driver_conformance.py, add unit tests, and update docs and OKF knowledge."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-duckdb-driver:t02"
read_set:
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - docs/usage/databases.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
files:
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 450
plan_revision: 3
plan_commit: d7a629b1
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T02:52:00Z"
updated_at: "2026-11-09T02:52:00Z"
commit: null
sources:
  - id: duckdb-py
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: Python SyncDuckDBDriver and AsyncDuckDBDriver target cutover module
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine database driver conformance matrix
  - id: docs-db
    resource: docs/usage/databases.md
    title: Public database drivers usage documentation
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: OKF knowledge chapter for database drivers and dialers
tags:
  - python
  - duckdb
  - driver
  - conformance
  - docs
---

# Task t03: Cut Over Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` to `vertebra._duckdb`, Activate Conformance, and Sync Documentation

## Objective & Scope Boundary

Cut `src/py/vertebra/db/adapters/duckdb/driver.py` (`SyncDuckDBDriver`, `AsyncDuckDBDriver`) over from the PyPI `duckdb` package and any `sqlite3` fallback to `ensure_module("vertebra._duckdb", package="vertebra-core[duckdb]", extra="duckdb")` delegating to `SyncDuckDBClient` and `AsyncDuckDBClient` while preserving the full `SyncDatabaseDriver` / `AsyncDatabaseDriver` contract (`cursor()`, `execute()`, `execute_many()`, `execute_tuples()`, `execute_with_description()`, `select()`, `select_one()`, `select_one_or_none()`, `select_value()`, `select_with_total()`, `select_to_arrow()`, `stream_arrow()`, `stream_arrow_batches()`, `load_from_arrow()`, `begin()`/`commit()`/`rollback()`, `pipeline()`, `apply_gcs_secret()`, `cancel()`, `close()`) [^duckdb-py]. Activate `duckdb` (`pending_flow_id=None`) in `src/py/tests/integration/db/test_driver_conformance.py`, add comprehensive unit tests in `src/py/tests/unit/db/test_duckdb_driver.py`, update `test_enterprise_olap_drivers.py` and `test_gcs_token_refresh.py`, and synchronize `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` [^conformance-py] [^docs-db] [^okf-drivers].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/duckdb/driver.py`:
  - `SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig])`: delegates to `vertebra._duckdb.SyncDuckDBClient`, exposes `cursor()`, `execute()`, `execute_many()`, `execute_tuples()`, `execute_with_description()`, `select()`, `select_one()`, `select_one_or_none()`, `select_value()`, `select_with_total()`, `select_to_arrow()`, `stream_arrow()`, `stream_arrow_batches()`, `load_from_arrow()`, `begin()`, `commit()`, `rollback()`, `pipeline()`, `apply_gcs_secret()`, `cancel()`, `close()`, and raises `MissingDependencyError` (`package="vertebra-core[duckdb]"`, `extra="duckdb"`) when `vertebra._duckdb` is unavailable.
  - `AsyncDuckDBDriver(AsyncDatabaseDriver[DuckDBConfig])`: delegates to `vertebra._duckdb.AsyncDuckDBClient` natively without `asyncio.to_thread` or `ThreadOffloadAsyncDatabaseDriver`.

## Acceptance Criteria

- [ ] `SyncDuckDBDriver` and `AsyncDuckDBDriver` delegate to `vertebra._duckdb` (`SyncDuckDBClient`, `AsyncDuckDBClient`) with zero PyPI `duckdb` or `sqlite3` imports.
- [ ] `src/py/tests/integration/db/test_driver_conformance.py` sets `pending_flow_id=None` for `duckdb` and passes all sync and async conformance cases.
- [ ] `src/py/tests/unit/db/test_duckdb_driver.py`, `test_enterprise_olap_drivers.py`, and `test_gcs_token_refresh.py` pass against `vertebra._duckdb`.
- [ ] `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` are updated and pass `tools.scripts.review_setup validate`.

[^duckdb-py]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Python `SyncDuckDBDriver` and `AsyncDuckDBDriver` target cutover module.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine database driver conformance matrix.
[^docs-db]: `docs/usage/databases.md` — Public database drivers usage documentation.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — OKF knowledge chapter for database drivers and dialers.
