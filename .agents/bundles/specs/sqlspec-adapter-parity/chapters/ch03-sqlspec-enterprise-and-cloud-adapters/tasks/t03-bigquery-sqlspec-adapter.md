---
type: Task
id: sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t03
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-enterprise-and-cloud-adapters
title: Deliver Feature-Complete BigQuery SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/bigquery/ (@name binds, unsplit BEGIN...END, VECTOR_SEARCH ADK + ArtifactReference/ObjectRef, DataDictionary, adapter_name='vertebra_bigquery').
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t02
files:
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/data_dictionary.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/adk.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/events.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/litestar.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/config.py
  - src/py/vertebra/extensions/sqlspec/adapters/bigquery/__init__.py
tests:
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
read_set:
  - src/py/vertebra/extensions/sqlspec/_driver.py
  - src/py/vertebra/db/adapters/bigquery/_sync.py
  - src/py/vertebra/db/adapters/bigquery/_async.py
  - src/py/vertebra/db/adapters/bigquery/core.py
  - src/py/vertebra/db/adapters/bigquery/object_tables.py
  - src/py/vertebra/extensions/sqlspec/bigquery.py
  - src/py/vertebra/storage/_store.py
estimated_loc: 245
tags:
  - sqlspec
  - bigquery
  - data-dictionary
  - adk
  - events
  - litestar
  - config
sources:
  - id: bigquery-drv
    resource: src/py/vertebra/db/adapters/bigquery/_sync.py
    title: Native Google BigQuery sync and async drivers
  - id: bigquery-core
    resource: src/py/vertebra/db/adapters/bigquery/core.py
    title: Native Google BigQuery core parameter and wire helpers
  - id: sqlspec-bq
    resource: src/py/vertebra/extensions/sqlspec/bigquery.py
    title: Current top-level BigQuery SQLSpec module
  - id: storage-art
    resource: src/py/vertebra/storage/_store.py
    title: Storage-neutral ArtifactReference value struct
verification_strategy: regression_tdd
---

# Task `t03`: Deliver Feature-Complete BigQuery SQLSpec Adapter Package

## Objective & Scope Boundary

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`BigQuerySyncDriver`/`BigQueryAsyncDriver`, `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `BigQuerySyncConfig`/`BigQueryAsyncConfig`) per [`../spec.md`](../spec.md) [^bigquery-drv] [^bigquery-core] [^sqlspec-bq] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/bigquery/_sync.py` and `src/py/vertebra/db/adapters/bigquery/_async.py` — native Google BigQuery sync/async execution, Storage Read/Write Arrow, and session IDs.
- `src/py/vertebra/db/adapters/bigquery/core.py` — native BigQuery `@name` parameter and `STRUCT`/`ARRAY`/`NUMERIC`/`GEOGRAPHY`/`JSON` wire helpers.
- `src/py/vertebra/db/adapters/bigquery/object_tables.py` — native `ObjectRef` struct retained for Object Tables.
- `src/py/vertebra/extensions/sqlspec/bigquery.py` — current top-level BigQuery SQLSpec module to migrate cleanly.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Lift BigQuery SQLSpec logic onto `BaseSyncDriver`/`BaseAsyncDriver` with `StatementConfig(dialect='bigquery')` and `ParameterStyle.NAMED_AT` (`@name`, preserving `TypedParameter` `STRUCT`/`ARRAY`/`NUMERIC`/`GEOGRAPHY`/`JSON` hints), unsplit procedural `BEGIN ... END` script execution, `session_id` transactions, `dry_run`, zero-copy `VTBARROW` `select_to_arrow`, Storage Write `PENDING` atomic `load_from_arrow`, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary` executing upstream `sqlspec/data_dictionary/dialects/bigquery/sql/*.sql` catalog queries. Keep all helpers local to `adapters/bigquery/core.py`.
- Implement zero-SDK `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, `VECTOR_SEARCH` / `SEARCH` `BigQuerySyncADKMemoryStore`/`BigQueryAsyncADKMemoryStore`, `BigQuerySyncADKArtifactStore`/`BigQueryAsyncADKArtifactStore` translating native `vertebra.db.adapters.bigquery.ObjectRef` into `vertebra.storage.ArtifactReference` on the shared artifact surface (while keeping `ObjectRef` on native Object Tables), partitioned/clustered `BigQueryEventQueueStore`, and `BigQueryLitestarStore` (`MERGE`) executing via `self._config.provide_session()` with zero `google.cloud.bigquery` or `sqlspec.adapters.*` imports.
- Define `BigQuerySyncConfig` and `BigQueryAsyncConfig` declaring `adapter_name = 'vertebra_bigquery'`, fixing `BigQueryConnectionShim.query` to honor `job_config.query_parameters` and `BigQueryAsyncConfig.create_connection` to reuse a single driver, plus explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` (and lazy `*ADKArtifactStore`) via PEP 562 `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized `@name` compilation, `TypedParameter` hints, unsplit `BEGIN ... END` procedures, zero-row `SELECT`, `dry_run`, `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary` catalog tests, store contract tests (`VECTOR_SEARCH`, `ObjectRef` -> `ArtifactReference`, partitioned events, Litestar `MERGE`), connection shim tests, PEP 562 lazy `google.adk` import tests, and resolver checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/bigquery && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_bigquery.py
```

## Acceptance Criteria

- [ ] `BigQuerySyncDriver` and `BigQueryAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/bigquery/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `BigQuerySyncConfig` and `BigQueryAsyncConfig` resolve `adapter_name = 'vertebra_bigquery'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/_sync.py` and `src/py/vertebra/db/adapters/bigquery/_async.py` — Native Google BigQuery sync and async drivers.
[^bigquery-core]: `src/py/vertebra/db/adapters/bigquery/core.py` — Native Google BigQuery core parameter and wire helpers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
