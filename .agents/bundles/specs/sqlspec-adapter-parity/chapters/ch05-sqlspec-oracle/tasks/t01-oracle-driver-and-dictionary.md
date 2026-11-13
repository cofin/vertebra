---
type: Task
id: "sqlspec-adapter-parity:ch05-sqlspec-oracle:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch05-sqlspec-oracle
title: "Implement SQLSpec Oracle Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/oracle/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/oracle/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 235
tags:
  - sqlspec
  - oracle
  - data-dictionary
sources:
  - id: oracle-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native Oracle sync and async drivers"
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py
    title: "Native Oracle core parameter and wire helpers"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "Oracle TxEventQ notification source"
---

# Task `t01`: Implement SQLSpec Oracle Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/oracle/{core,driver,data_dictionary}.py` (`OracleSyncDriver`, `OracleAsyncDriver`, `OracleSyncDataDictionary`, `OracleAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^oracle-drv] [^oracle-core] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/oracle/driver.py` & `src/py/vertebra/db/adapters/oracle/core.py` — native Oracle execution and parameter encoding.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='oracle')` supporting `{ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON}` (`:name` dicts and `:1` tuples), unwrap `TypedParameter` before `OracleCore` JSON/`array('f')` `VECTOR` encoding, map `ORA-00001`/`ORA-02291`/`ORA-00060`/`ORA-08177`, and implement `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` (23ai `OSON` JSON vs 19c `BLOB CHECK`). Keep all helpers local to `adapters/oracle/core.py`.
- `OracleSyncDataDictionary(SyncDataDictionaryBase)` and `OracleAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/oracle/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `OracleSyncDataDictionary`/`OracleAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `OracleSyncDriver` and `OracleAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/oracle/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^oracle-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle sync and async drivers.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py` — Native Oracle core parameter and wire helpers.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification source.
