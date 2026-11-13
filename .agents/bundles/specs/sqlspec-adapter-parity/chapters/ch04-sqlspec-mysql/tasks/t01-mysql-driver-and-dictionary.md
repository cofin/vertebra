---
type: Task
id: "sqlspec-adapter-parity:ch04-sqlspec-mysql:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch04-sqlspec-mysql
title: "Implement SQLSpec MySQL Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/mysql/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/mysql/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/mysql/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/mysql/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/db/adapters/mysql/core.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
estimated_loc: 235
tags:
  - sqlspec
  - mysql
  - data-dictionary
sources:
  - id: mysql-drv
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: "Native MySQL & MariaDB sync and async drivers"
  - id: mysql-core
    resource: src/py/vertebra/db/adapters/mysql/core.py
    title: "Native MySQL & MariaDB core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Legacy MySQL SQLSpec adapter setting broken %s execution style"
---

# Task `t01`: Implement SQLSpec MySQL Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/mysql/{core,driver,data_dictionary}.py` (`MySQLSyncDriver`, `MySQLAsyncDriver`, `MySQLSyncDataDictionary`, `MySQLAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^mysql-drv] [^mysql-core] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/mysql/driver.py` & `src/py/vertebra/db/adapters/mysql/core.py` — native MySQL & MariaDB execution and parameter encoding.

## Contracts & Edge Cases

- Fix the critical `%s` bug by setting `StatementConfig(dialect='mysql')` default and execution style to `ParameterStyle.QMARK` (`?` positional tuples required by `vertebra._mysql` binary prepared statements), map errno `1062`/`1451`/`1452`/`1213` to `sqlspec.exceptions.*`, and implement `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` (MySQL 8/9 vs MariaDB JSON/version flags). Duplicate MySQL-family helpers strictly inside `adapters/mysql/core.py` (zero cross-adapter shared helper modules).
- `MySQLSyncDataDictionary(SyncDataDictionaryBase)` and `MySQLAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/mysql/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `MySQLSyncDataDictionary`/`MySQLAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mysql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `MySQLSyncDriver` and `MySQLAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/mysql/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^mysql-drv]: `src/py/vertebra/db/adapters/mysql/driver.py` — Native MySQL & MariaDB sync and async drivers.
[^mysql-core]: `src/py/vertebra/db/adapters/mysql/core.py` — Native MySQL & MariaDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Legacy MySQL SQLSpec adapter setting broken %s execution style.
