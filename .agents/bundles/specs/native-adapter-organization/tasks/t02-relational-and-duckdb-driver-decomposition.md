---
type: Task
id: "native-adapter-organization:t02"
flow_id: native-adapter-organization
title: "Decompose Oracle, MySQL, SQL Server, and DuckDB drivers and encapsulate loose helpers"
description: "Extract oracle/pipeline.py, split Oracle, MySQL, SQL Server, and DuckDB driver.py modules into _sync.py and _async.py barrels, and encapsulate loose module helpers onto domain classes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-adapter-organization:t01"
files:
  - src/py/vertebra/db/adapters/oracle/pipeline.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/db/test_mssql_driver.py
  - src/py/tests/unit/db/test_duckdb_driver.py
read_set:
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
estimated_loc: 245
tags:
  - database
  - oracle
  - mysql
  - mssql
  - duckdb
sources:
  - id: ora-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOraclePipeline
    title: "SyncOraclePool, AsyncOraclePool, SyncOraclePipeline, and AsyncOraclePipeline embedded in oracle/driver.py"
  - id: mysql-core
    resource: src/py/vertebra/db/adapters/mysql/core.py::compile_mysql_statement
    title: "Loose module-level MySQL statement compilation and row normalization functions"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: "SyncDuckDBDriver, AsyncDuckDBDriver, cursors, and loose module helpers in duckdb/driver.py"
---

# Task `t02`: Decompose Oracle, MySQL, SQL Server, and DuckDB Drivers and Encapsulate Loose Helpers

## Objective & Scope Boundary

Implement `R2` in [`../spec.md` §2](../spec.md) by extracting `oracle/pipeline.py`, splitting `oracle`, `mysql`, `mssql`, and `duckdb` `driver.py` modules into `_sync.py` + `_async.py` + thin `driver.py` barrels, and encapsulating single-owner module-level `_...` functions onto their domain classes [^ora-driver] [^mysql-core] [^duckdb-driver]. Out of scope: DataFusion (`t03`) and public docs/OKF (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/{driver,__init__,config,notify}.py` — Oracle pools, pipelines, drivers, and lazy export map.
- `src/py/vertebra/db/adapters/mysql/{core,driver}.py`, `mssql/{config,driver}.py`, `duckdb/{config,core,driver}.py` — Sync/async driver classes and loose helper functions.

## Contracts & Edge Cases

- **Oracle (`oracle/pipeline.py`, `_sync.py`, `_async.py`, `driver.py`, `__init__.py`, `config.py`, `core.py`, `notify.py`)**:
  - Move `SyncOraclePipeline` and `AsyncOraclePipeline` to `oracle/pipeline.py` (updating `oracle/__init__.py` `_EXPORT_MAP` and `__lazy_modules__`), place `SyncOraclePool` + `SyncOracleDriver` in `oracle/_sync.py` and `AsyncOraclePool` + `AsyncOracleDriver` in `oracle/_async.py`, reduce `oracle/driver.py` to a barrel re-exporting the drivers and pools, fold single-caller `_...` helpers onto `OracleCore`, `OracleConnectDescriptor`, and `OracleNotificationSource` (enforcing public-before-private ordering in `notify.py`), and remove module-level alias shims from `oracle/core.py` and `oracle/__init__.py` (migrating unit test callers to `OracleCore.*`).
- **MySQL, SQL Server & DuckDB (`{mysql,mssql,duckdb}/{_sync,_async,driver,config,core,pipeline}.py`)**:
  - Split `SyncMySQLDriver`/`AsyncMySQLDriver`, `SyncMSSQLDriver`/`AsyncMSSQLDriver`, and `SyncDuckDBDriver`/`AsyncDuckDBDriver` (with `_SyncDuckDBCursor`/`_AsyncDuckDBCursor`) into `_sync.py` and `_async.py` behind 12-line `driver.py` barrels.
  - Introduce `class MySQLCore` owning `coerce_param`, `compile_statement`, `normalize_rows`, `build_connect_kwargs`, and `map_exception` in `mysql/core.py`, and move single-owner `_...` helpers in `mssql/{config,driver}.py` and `duckdb/{config,core,driver}.py` onto `MSSQLConfig`, `DuckDBSecretConfig`, `DuckDBCore`, and the driver classes (zero inline `#` comments, strict public-before-private member ordering including `mssql/{core,pipeline}.py`).

## Test Matrix

- Parameterized tests across `test_oracle_adapter.py`, `test_mysql_driver.py`, `test_mssql_driver.py`, and `test_duckdb_driver.py` confirming `from vertebra.db.adapters.<engine> import Sync*Driver, Async*Driver` and `from vertebra.db.adapters.<engine>.pipeline import Sync*Pipeline, Async*Pipeline` resolve identically with zero regressions in pool, pipeline, Arrow, and secret refresh behavior.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/db/test_mssql_driver.py src/py/tests/unit/db/test_duckdb_driver.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/mysql src/py/vertebra/db/adapters/mssql src/py/vertebra/db/adapters/duckdb && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/mysql src/py/vertebra/db/adapters/mssql src/py/vertebra/db/adapters/duckdb
```

## Acceptance Criteria

- [ ] `oracle`, `mysql`, `mssql`, and `duckdb` each expose `_sync.py`, `_async.py`, `pipeline.py`, and a thin `driver.py` barrel with zero single-owner module-level `_...` functions.
- [ ] All unit, Ruff, and Mypy gates pass with zero behavioral regressions.

[^ora-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOraclePipeline` — SyncOraclePool, AsyncOraclePool, SyncOraclePipeline, and AsyncOraclePipeline embedded in oracle/driver.py.
[^mysql-core]: `src/py/vertebra/db/adapters/mysql/core.py::compile_mysql_statement` — Loose module-level MySQL statement compilation and row normalization functions.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — SyncDuckDBDriver, AsyncDuckDBDriver, cursors, and loose module helpers in duckdb/driver.py.
