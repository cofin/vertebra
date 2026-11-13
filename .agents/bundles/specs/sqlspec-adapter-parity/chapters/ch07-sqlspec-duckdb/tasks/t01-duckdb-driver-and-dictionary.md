---
type: Task
id: "sqlspec-adapter-parity:ch07-sqlspec-duckdb:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch07-sqlspec-duckdb
title: "Implement SQLSpec DuckDB Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/duckdb/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-sqlspec-core-driver-and-artifact-ref:t02"
files:
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/duckdb/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/vertebra/db/adapters/duckdb/core.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 235
tags:
  - sqlspec
  - duckdb
  - data-dictionary
sources:
  - id: duckdb-drv
    resource: src/py/vertebra/db/adapters/duckdb/driver.py
    title: "Native DuckDB sync and async drivers"
  - id: duckdb-core
    resource: src/py/vertebra/db/adapters/duckdb/core.py
    title: "Native DuckDB core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing DuckDB SQLSpec adapter"
---

# Task `t01`: Implement SQLSpec DuckDB Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{core,driver,data_dictionary}.py` (`DuckDBSyncDriver`, `DuckDBAsyncDriver`, `DuckDBSyncDataDictionary`, `DuckDBAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^duckdb-drv] [^duckdb-core] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/duckdb/driver.py` & `src/py/vertebra/db/adapters/duckdb/core.py` — native DuckDB execution and parameter encoding.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='duckdb')` supporting `{ParameterStyle.QMARK, ParameterStyle.NUMERIC}` (`?` and `$1`), DuckDB 2.0+ lexical `COPY (SELECT ... ?) TO ?` parameter ordering, native Python `fetchall()` preservation for Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `INTERVAL`, `MAP`) alongside zero-copy `select_to_arrow` / `load_from_arrow`, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary`. Keep all helpers local to `adapters/duckdb/core.py`.
- `DuckDBSyncDataDictionary(SyncDataDictionaryBase)` and `DuckDBAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/duckdb/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `DuckDBSyncDriver` and `DuckDBAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/duckdb/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-drv]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Native DuckDB sync and async drivers.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py` — Native DuckDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing DuckDB SQLSpec adapter.
