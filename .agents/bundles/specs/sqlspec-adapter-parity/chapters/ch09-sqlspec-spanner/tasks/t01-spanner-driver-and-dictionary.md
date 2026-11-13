---
type: Task
id: "sqlspec-adapter-parity:ch09-sqlspec-spanner:t01"
flow_id: sqlspec-adapter-parity
chapter_id: ch09-sqlspec-spanner
title: "Implement SQLSpec Spanner Core, Sync/Async Drivers & DataDictionaries"
description: "Implement src/py/vertebra/extensions/sqlspec/adapters/spanner/{core,driver,data_dictionary}.py over BaseSyncDriver / BaseAsyncDriver and upstream metadata SQL."
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
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/spanner/data_dictionary.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_spanner.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/db/adapters/spanner/core.py
  - src/py/vertebra/extensions/sqlspec/spanner.py
estimated_loc: 235
tags:
  - sqlspec
  - spanner
  - data-dictionary
sources:
  - id: spanner-drv
    resource: src/py/vertebra/db/adapters/spanner/driver.py
    title: "Native Google Cloud Spanner & Spangres sync and async drivers"
  - id: spanner-core
    resource: src/py/vertebra/db/adapters/spanner/core.py
    title: "Native Google Cloud Spanner & Spangres core parameter and wire helpers"
  - id: sqlspec-spn
    resource: src/py/vertebra/extensions/sqlspec/spanner.py
    title: "Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import"
---

# Task `t01`: Implement SQLSpec Spanner Core, Sync/Async Drivers & DataDictionaries

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{core,driver,data_dictionary}.py` (`SpannerSyncDriver`, `SpannerAsyncDriver`, `SpannerSyncDataDictionary`, `SpannerAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^spanner-drv] [^spanner-core] [^sqlspec-spn].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/spanner/driver.py` & `src/py/vertebra/db/adapters/spanner/core.py` — native Google Cloud Spanner & Spangres execution and parameter encoding.

## Contracts & Edge Cases

- Fix invalid `sqlglot` dialect names by setting `StatementConfig.dialect = 'spanner'` (`ParameterStyle.NAMED_AT` `@name`, `TypeCode.JSON` and `TypedParameter(semantic_name='ARRAY<FLOAT32>')`) when `spanner_dialect == 'googlesql'` and `StatementConfig.dialect = 'postgres'` (`ParameterStyle.NUMERIC` `$1 -> p1`) when `spanner_dialect == 'spangres'`. Support `execute_partitioned_dml`, `ExecuteBatchDml` `execute_stack`, and `SpannerSyncDataDictionary`/`SpannerAsyncDataDictionary` routing `googlesql` vs `postgresql` catalog SQL. Keep helpers strictly inside `adapters/spanner/core.py`.
- `SpannerSyncDataDictionary(SyncDataDictionaryBase)` and `SpannerAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/spanner/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `SpannerSyncDataDictionary`/`SpannerAsyncDataDictionary` catalog tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

## Acceptance Criteria

- [ ] `SpannerSyncDriver` and `SpannerAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/spanner/`.
- [ ] Zero regressions in the chapter fast verification gate.

[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^spanner-core]: `src/py/vertebra/db/adapters/spanner/core.py` — Native Google Cloud Spanner & Spangres core parameter and wire helpers.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
