---
type: Task
id: t04
flow_id: native-mssql-driver
title: "Cut Python SyncMSSQLDriver and AsyncMSSQLDriver over to vertebra._mssql, update tests, and refresh docs"
description: "Cut src/py/vertebra/db/adapters/mssql/driver.py over to ensure_module('vertebra._mssql', package='vertebra-core[mssql]', extra='mssql') delegating to SyncMSSQLClient and AsyncMSSQLClient, remove all sqlite3/memory:// stand-in paths, update unit and conformance tests, verify wheel isolation, and update docs and OKF chapters."
status: stable
state: completed
verification_strategy: tdd
depends_on:
  - t03
plan_revision: 1
state_revision: 3
commit_sha: 974501d1
claimed_at: "2026-11-09T15:15:00Z"
completed_at: "2026-11-09T15:48:00Z"
created_at: "2026-11-09T03:20:00Z"
updated_at: "2026-11-09T15:48:00Z"
reads:
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
targets:
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/__init__.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/db/test_mssql_driver.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - docs/usage/databases.md
  - docs/reference/database-capabilities.md
  - .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
sources:
  - id: mssql-py
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: Python SyncMSSQLDriver and AsyncMSSQLDriver target cutover module
  - id: conformance-py
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: Cross-driver conformance test suite
---

# Task t04: Cut Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` over to `vertebra._mssql`, update tests, and refresh docs

<!-- truth: start -->
- **Zero-`sqlite3` Native Cutover**: `SyncMSSQLDriver` and `AsyncMSSQLDriver` resolve `vertebra._mssql` via `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")`, delegate all sync/async execution, transactions, savepoints, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, and `cancel` to `SyncMSSQLClient` / `AsyncMSSQLClient`, and forward `dialer` on `AsyncMSSQLDriver` [^mssql-py].
- **Unit, Loopback & Conformance Coverage**: Unit and integration tests cover `MissingDependencyError` when `vertebra._mssql` is blocked, end-to-end Python `SyncMSSQLDriver` and `AsyncMSSQLDriver` execution against the Rust TDS loopback server and live SQL Server (`pytest-databases`), and wheel isolation (`maturin build` without leaking `vertebra/*.py`) [^conformance-py].
- **Documentation & OKF Alignment**: `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` document `vertebra-core[mssql]` (`vertebra._mssql`, `SyncMSSQLClient`, `AsyncMSSQLClient`, Cloud SQL `:3307` mTLS, `VTBARROW`, `BulkLoad`, and `ATTENTION` cancellation).
<!-- truth: end -->

## Context

Completes the Python-side cutover in `src/py/vertebra/db/adapters/mssql/driver.py` so `SyncMSSQLDriver` and `AsyncMSSQLDriver` wrap `vertebra._mssql.SyncMSSQLClient` and `vertebra._mssql.AsyncMSSQLClient` directly, removing all `sqlite3` stand-in imports and `:memory:` fallbacks, and updates unit tests, conformance tests, documentation, and the OKF knowledge base.

## Public Seam Contract

- `SyncMSSQLDriver(dsn="mssql://...", *, pool=None, dialer=None, config=None)` and `AsyncMSSQLDriver(dsn="mssql://...", *, pool=None, dialer=None, config=None)` delegating to `vertebra._mssql.SyncMSSQLClient` / `AsyncMSSQLClient` via `_require_mssql_native()`.
- `MSSQLConnectionConfig.create_sync_driver()` and `create_async_driver()` passing config and `dialer` through cleanly.

## Implementation Blueprint

1. Refactor `src/py/vertebra/db/adapters/mssql/driver.py` to remove `import sqlite3` and all in-memory SQLite fallback logic, add `_require_mssql_native()` calling `ensure_module("vertebra._mssql", package="vertebra-core[mssql]", extra="mssql")`, forward `dialer` in `AsyncMSSQLDriver.__init__`, and wire all `SyncDatabaseDriver` / `AsyncDatabaseDriver` methods (`execute`, `execute_many`, `select`, `select_one`, `select_one_or_none`, `select_value`, `select_with_total`, `select_to_arrow`, `stream_arrow_batches`, `load_from_arrow`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `release_savepoint`, `cancel`, `ping`, `close`) to `SyncMSSQLClient` and `AsyncMSSQLClient`.
2. Add comprehensive unit and loopback integration tests in `src/py/tests/unit/db/test_mssql_driver.py` and update `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, and `src/py/tests/integration/db/test_driver_conformance.py`.
3. Verify wheel isolation by building the `vertebra-mssql` wheel into `/tmp/vtb-wheels-mssql` and asserting only `vertebra/_mssql*.so`, `vertebra/_mssql.pyi`, and `.dist-info/` are packaged.
4. Update `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`.

## Acceptance Criteria

- [x] `pytest src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py` passes.
- [x] `mypy`, `ruff`, `mypy.stubtest vertebra._mssql`, and `review_setup validate` pass.
- [x] Wheel isolation check confirms zero leaked `vertebra/*.py` files in the built `vertebra_mssql` wheel.

[^mssql-py]: `src/py/vertebra/db/adapters/mssql/driver.py` — Python SyncMSSQLDriver and AsyncMSSQLDriver target cutover module.
[^conformance-py]: `src/py/tests/integration/db/test_driver_conformance.py` — Cross-driver conformance test suite.
