---
type: Task
id: "sqlspec-adapter-parity:ch06-sqlspec-mssql:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch06-sqlspec-mssql
title: "Implement SQLSpec MSSQL Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/mssql/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/mssql/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 235
tags:
  - sqlspec
  - mssql
  - data-dictionary
sources:
  - id: mssql-drv
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "Native SQL Server (MSSQL) sync and async drivers"
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py
    title: "Native SQL Server (MSSQL) core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing MSSQL SQLSpec adapter"
---

# Task `t01`: Implement SQLSpec MSSQL Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{core,driver,data_dictionary}.py` (`MSSQLSyncDriver`, `MSSQLAsyncDriver`, `MSSQLSyncDataDictionary`, `MSSQLAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^mssql-drv] [^mssql-core] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/mssql/driver.py` & `src/py/vertebra/db/adapters/mssql/core.py` — native SQL Server (MSSQL) execution and parameter encoding.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='tsql')` with `ParameterStyle.QMARK` (`?` positional tuples rewritten by `MSSQLCore.rewrite_placeholders` to `@P1..@Pn` for `vertebra._mssql`), `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` savepoints, error `2601`/`2627`/`547`/`1205` mapping, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary`. Keep all T-SQL helpers strictly local to `adapters/mssql/core.py`.
- `MSSQLSyncDataDictionary(SyncDataDictionaryBase)` and `MSSQLAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/mssql/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `MSSQLSyncDriver` and `MSSQLAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/mssql/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py` — Native SQL Server (MSSQL) core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
