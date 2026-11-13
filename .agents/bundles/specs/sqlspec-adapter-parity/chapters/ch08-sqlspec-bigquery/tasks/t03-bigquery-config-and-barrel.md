---
type: Task
id: "sqlspec-adapter-parity:ch08-sqlspec-bigquery:t03"
flow_id: sqlspec-adapter-parity
chapter_id: ch08-sqlspec-bigquery
title: "Implement BigQuerySyncConfig / BigQueryAsyncConfig and PEP 562 Lazy Barrel"
description: "Wire src/py/vertebra/extensions/sqlspec/adapters/bigquery/{config,__init__}.py with adapter_name = 'vertebra_bigquery' and explicit store class attributes."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t02
files:
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/litestar.py
estimated_loc: 145
tags:
  - sqlspec
  - bigquery
  - config
sources:
  - id: bigquery-drv
    resource: src/py/vertebra/db/adapters/bigquery/driver.py
    title: "Native Google BigQuery sync and async drivers"
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: "Current top-level BigQuery SQLSpec module"
---

# Task `t03`: Implement BigQuerySyncConfig / BigQueryAsyncConfig and PEP 562 Lazy Barrel

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{config,__init__}.py` declaring `adapter_name = 'vertebra_bigquery'` and explicit store class attributes per [`../spec.md`](../spec.md) [^bigquery-drv] [^sqlspec-bq].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Define `BigQuerySyncConfig` and `BigQueryAsyncConfig` declaring `adapter_name = 'vertebra_bigquery'`, fixing `BigQueryConnectionShim.query` to honor `job_config.query_parameters` and `BigQueryAsyncConfig.create_connection` to reuse a single driver, plus explicit `*_store_class` attributes and PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_bigquery'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py
```

## Acceptance Criteria

- [ ] `BigQuerySyncConfig` and `BigQueryAsyncConfig` resolve `adapter_name = 'vertebra_bigquery'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the chapter fast verification gate.

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
