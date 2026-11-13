---
type: Task
id: "sqlspec-adapter-parity:ch08-sqlspec-bigquery:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch08-sqlspec-bigquery
title: "Implement SQLSpec BigQuery Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/bigquery/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/bigquery/driver.py
  - src/py/vertebra/db/adapters/bigquery/core.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
estimated_loc: 235
tags:
  - sqlspec
  - bigquery
  - data-dictionary
sources:
  - id: bigquery-drv
    resource: src/py/vertebra/db/adapters/bigquery/driver.py
    title: "Native Google BigQuery sync and async drivers"
  - id: bigquery-core
    resource: src/py/vertebra/db/adapters/bigquery/core.py
    title: "Native Google BigQuery core parameter and wire helpers"
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "Current top-level BigQuery SQLSpec module"
---

# Task `t01`: Implement SQLSpec BigQuery Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{core,driver,data_dictionary}.py` (`BigQuerySyncDriver`, `BigQueryAsyncDriver`, `BigQuerySyncDataDictionary`, `BigQueryAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^bigquery-drv] [^bigquery-core] [^sqlspec-bq].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/bigquery/driver.py` & `src/py/vertebra/db/adapters/bigquery/core.py` — native Google BigQuery execution and parameter encoding.

## Contracts & Edge Cases

- Lift BigQuery SQLSpec logic onto `BaseSyncDriver`/`BaseAsyncDriver` with `StatementConfig(dialect='bigquery')` and `ParameterStyle.NAMED_AT` (`@name`, preserving `TypedParameter` `STRUCT`/`ARRAY`/`NUMERIC`/`GEOGRAPHY`/`JSON` hints), unsplit procedural `BEGIN ... END` script execution, `session_id` transactions, `dry_run`, zero-copy `VTBARROW` `select_to_arrow`, Storage Write `PENDING` atomic `load_from_arrow`, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary`. Keep all helpers local to `adapters/bigquery/core.py`.
- `BigQuerySyncDataDictionary(SyncDataDictionaryBase)` and `BigQueryAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/bigquery/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py
```

## Acceptance Criteria

- [ ] `BigQuerySyncDriver` and `BigQueryAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/bigquery/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^bigquery-core]: `src/py/vertebra/db/adapters/bigquery/core.py` — Native Google BigQuery core parameter and wire helpers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
