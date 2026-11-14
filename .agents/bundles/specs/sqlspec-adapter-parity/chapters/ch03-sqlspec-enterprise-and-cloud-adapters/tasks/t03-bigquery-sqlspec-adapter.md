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
plan_commit: 1b191388
depends_on:
- sqlspec-adapter-parity:ch01-sqlspec-core-driver-and-artifact-ref:t03
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
- src/py/vertebra/db/adapters/bigquery/driver.py
- src/py/vertebra/db/adapters/bigquery/core.py
- src/py/vertebra/extensions/sqlspec/bigquery.py
- src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py
- src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py
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
  resource: src/py/vertebra/db/adapters/bigquery/driver.py
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
---

# Task `t03`: Deliver Feature-Complete BigQuery SQLSpec Adapter Package

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{core,driver,data_dictionary}.py` (`BigQuerySyncDriver`, `BigQueryAsyncDriver`, `BigQuerySyncDataDictionary`, `BigQueryAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^bigquery-drv] [^bigquery-core] [^sqlspec-bq].

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^bigquery-drv] [^storage-art] [^sqlspec-bq].

Implement `src/py/vertebra/extensions/sqlspec/adapters/bigquery/{config,__init__}.py` declaring `adapter_name = 'vertebra_bigquery'` and explicit store class attributes per [`../spec.md`](../spec.md) [^bigquery-drv] [^sqlspec-bq].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/bigquery/driver.py` & `src/py/vertebra/db/adapters/bigquery/core.py` — native Google BigQuery execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/bigquery/core.py` — Google BigQuery SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/bigquery/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Lift BigQuery SQLSpec logic onto `BaseSyncDriver`/`BaseAsyncDriver` with `StatementConfig(dialect='bigquery')` and `ParameterStyle.NAMED_AT` (`@name`, preserving `TypedParameter` `STRUCT`/`ARRAY`/`NUMERIC`/`GEOGRAPHY`/`JSON` hints), unsplit procedural `BEGIN ... END` script execution, `session_id` transactions, `dry_run`, zero-copy `VTBARROW` `select_to_arrow`, Storage Write `PENDING` atomic `load_from_arrow`, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary`. Keep all helpers local to `adapters/bigquery/core.py`.
- `BigQuerySyncDataDictionary(SyncDataDictionaryBase)` and `BigQueryAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/bigquery/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement zero-SDK `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, `VECTOR_SEARCH` / `SEARCH` `BigQuerySyncADKMemoryStore`/`BigQueryAsyncADKMemoryStore`, `BigQuerySyncADKArtifactStore`/`BigQueryAsyncADKArtifactStore` translating native `vertebra.db.adapters.bigquery.ObjectRef` into `vertebra.storage.ArtifactReference` on the shared artifact surface (while keeping `ObjectRef` on native Object Tables), partitioned/clustered `BigQueryEventQueueStore`, and `BigQueryLitestarStore` (`MERGE`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `BigQuerySyncConfig` and `BigQueryAsyncConfig` declaring `adapter_name = 'vertebra_bigquery'`, fixing `BigQueryConnectionShim.query` to honor `job_config.query_parameters` and `BigQueryAsyncConfig.create_connection` to reuse a single driver, plus explicit `*_store_class` attributes and PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_bigquery'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `BigQuerySyncDataDictionary`/`BigQueryAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `BigQuerySyncADKStore`/`BigQueryAsyncADKStore`, memory search, `ArtifactReference` metadata, `BigQueryEventQueueStore`, and `BigQueryLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

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

[^bigquery-drv]: `src/py/vertebra/db/adapters/bigquery/driver.py` — Native Google BigQuery sync and async drivers.
[^bigquery-core]: `src/py/vertebra/db/adapters/bigquery/core.py` — Native Google BigQuery core parameter and wire helpers.
[^sqlspec-bq]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Current top-level BigQuery SQLSpec module.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
