---
type: Task
id: "sqlspec-adapter-parity:ch02-sqlspec-postgres:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-postgres
title: "Implement SQLSpec Postgres Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/postgres/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/postgres/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/core.py
  - src/py/vertebra/db/adapters/postgres/notify.py
estimated_loc: 235
tags:
  - sqlspec
  - postgres
  - data-dictionary
sources:
  - id: postgres-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native PostgreSQL sync and async drivers"
  - id: postgres-core
    resource: src/py/vertebra/db/adapters/postgres/core.py
    title: "Native PostgreSQL core parameter and wire helpers"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification source"
---

# Task `t01`: Implement SQLSpec Postgres Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{core,driver,data_dictionary}.py` (`PostgresSyncDriver`, `PostgresAsyncDriver`, `PostgresSyncDataDictionary`, `PostgresAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^postgres-drv] [^postgres-core] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/postgres/driver.py` & `src/py/vertebra/db/adapters/postgres/core.py` — native PostgreSQL execution and parameter encoding.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='postgres')` with default and execution `ParameterStyle.NUMERIC` (`$1, $2` positional sequences matching `vertebra._postgres` instead of legacy `%s`), native Arrow `COPY BINARY` `select_to_arrow`, server-side portal `dispatch_select_stream`, pipeline `execute_stack`, and savepoints. Duplicate all PostgreSQL SQL/parameter helpers locally inside `adapters/postgres/core.py` (zero cross-adapter helper modules).
- `PostgresSyncDataDictionary(SyncDataDictionaryBase)` and `PostgresAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/postgres/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `PostgresSyncDataDictionary`/`PostgresAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `PostgresSyncDriver` and `PostgresAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/postgres/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^postgres-core]: `src/py/vertebra/db/adapters/postgres/core.py` — Native PostgreSQL core parameter and wire helpers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
