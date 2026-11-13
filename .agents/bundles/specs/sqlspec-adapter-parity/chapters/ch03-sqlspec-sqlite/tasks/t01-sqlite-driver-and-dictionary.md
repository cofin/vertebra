---
type: Task
id: "sqlspec-adapter-parity:ch03-sqlspec-sqlite:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-sqlite
title: "Implement SQLSpec SQLite Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/sqlite/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/sqlite/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/sqlite/driver.py
  - src/py/vertebra/db/adapters/sqlite/core.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 235
tags:
  - sqlspec
  - sqlite
  - data-dictionary
sources:
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: "Native SQLite sync and async drivers"
  - id: sqlite-core
    resource: src/py/vertebra/db/adapters/sqlite/core.py
    title: "Native SQLite core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing SQLite SQLSpec adapter"
---

# Task `t01`: Implement SQLSpec SQLite Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{core,driver,data_dictionary}.py` (`SQLiteSyncDriver`, `SQLiteAsyncDriver`, `SQLiteSyncDataDictionary`, `SQLiteAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^sqlite-drv] [^sqlite-core] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/sqlite/driver.py` & `src/py/vertebra/db/adapters/sqlite/core.py` — native SQLite execution and parameter encoding.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='sqlite')` supporting execution `{ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}` (`?` tuples and `:name` dicts without unnecessary rewriting), `PRAGMA` row-returning classification, WAL/foreign_keys/busy_timeout pragmas, and savepoints. Keep all SQLite SQL/parameter helpers strictly inside `adapters/sqlite/core.py`.
- `SQLiteSyncDataDictionary(SyncDataDictionaryBase)` and `SQLiteAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/sqlite/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `SQLiteSyncDataDictionary`/`SQLiteAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `SQLiteSyncDriver` and `SQLiteAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/sqlite/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Native SQLite sync and async drivers.
[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py` — Native SQLite core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing SQLite SQLSpec adapter.
