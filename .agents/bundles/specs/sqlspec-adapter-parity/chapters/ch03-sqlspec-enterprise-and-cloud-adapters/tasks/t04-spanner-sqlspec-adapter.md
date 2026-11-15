---
type: Task
id: sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t04
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-enterprise-and-cloud-adapters
title: Deliver Feature-Complete Cloud Spanner & Spangres SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/spanner/ (sqlglot 'spanner'/'postgres' dialect fix, ScaNN/RRF ADK + ArtifactReference, sharded stores, adapter_name='vertebra_spanner').
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
- src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/data_dictionary.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/adk.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/events.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/litestar.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/config.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlspec_spanner.py
read_set:
- src/py/vertebra/extensions/sqlspec/_driver.py
- src/py/vertebra/db/adapters/spanner/driver.py
- src/py/vertebra/db/adapters/spanner/core.py
- src/py/vertebra/extensions/sqlspec/spanner.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py
- src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py
estimated_loc: 245
tags:
- sqlspec
- spanner
- data-dictionary
- adk
- events
- litestar
- config
sources:
- id: spanner-drv
  resource: src/py/vertebra/db/adapters/spanner/driver.py
  title: Native Google Cloud Spanner & Spangres sync and async drivers
- id: spanner-core
  resource: src/py/vertebra/db/adapters/spanner/core.py
  title: Native Google Cloud Spanner & Spangres core parameter and wire helpers
- id: sqlspec-spn
  resource: src/py/vertebra/extensions/sqlspec/spanner.py
  title: Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import
- id: storage-art
  resource: src/py/vertebra/storage/_store.py
  title: Storage-neutral ArtifactReference value struct
---

# Task `t04`: Deliver Feature-Complete Cloud Spanner & Spangres SQLSpec Adapter Package

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{core,driver,data_dictionary}.py` (`SpannerSyncDriver`, `SpannerAsyncDriver`, `SpannerSyncDataDictionary`, `SpannerAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^spanner-drv] [^spanner-core] [^sqlspec-spn].

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^spanner-drv] [^storage-art] [^sqlspec-spn].

Implement `src/py/vertebra/extensions/sqlspec/adapters/spanner/{config,__init__}.py` declaring `adapter_name = 'vertebra_spanner'` and explicit store class attributes per [`../spec.md`](../spec.md) [^spanner-drv] [^sqlspec-spn].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/spanner/driver.py` & `src/py/vertebra/db/adapters/spanner/core.py` — native Google Cloud Spanner & Spangres execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/spanner/core.py` — Google Cloud Spanner & Spangres SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/spanner/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Fix invalid `sqlglot` dialect names by setting `StatementConfig.dialect = 'spanner'` (`ParameterStyle.NAMED_AT` `@name`, `TypeCode.JSON` and `TypedParameter(semantic_name='ARRAY<FLOAT32>')`) when `spanner_dialect == 'googlesql'` and `StatementConfig.dialect = 'postgres'` (`ParameterStyle.NUMERIC` `$1 -> p1`) when `spanner_dialect == 'spangres'`. Support `execute_partitioned_dml`, `ExecuteBatchDml` `execute_stack`, and `SpannerSyncDataDictionary`/`SpannerAsyncDataDictionary` routing `googlesql` vs `postgresql` catalog SQL. Keep helpers strictly inside `adapters/spanner/core.py`.
- `SpannerSyncDataDictionary(SyncDataDictionaryBase)` and `SpannerAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/spanner/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement zero-SDK `SpannerSyncADKStore`/`SpannerAsyncADKStore` (`provide_session(transaction=False)` lock-free snapshots on reads, `transaction=True` on writes), ScaNN/FTS/RRF/PropertyGraph `SpannerSyncADKMemoryStore`/`SpannerAsyncADKMemoryStore`, `SpannerSyncADKArtifactStore`/`SpannerAsyncADKArtifactStore` constructing `vertebra.storage.ArtifactReference` directly with ZERO `vertebra.db.adapters.bigquery` imports, hash-sharded `MOD(FARM_FINGERPRINT(...))` `BYTES` `SpannerEventQueueStore` (valid `ROW DELETION POLICY` / `TTL INTERVAL`, no invalid TTL on `updated_at`), and `SpannerLitestarStore`.
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `SpannerSyncConfig`/`SpannerAsyncConfig` and `SpangresSyncConfig`/`SpangresAsyncConfig` declaring `adapter_name = 'vertebra_spanner'`, isolated per-session transaction state on `provide_session`, and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_spanner'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `SpannerSyncDataDictionary`/`SpannerAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `SpannerSyncADKStore`/`SpannerAsyncADKStore`, memory search, `ArtifactReference` metadata, `SpannerEventQueueStore`, and `SpannerLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/spanner && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlspec_spanner.py
```

## Acceptance Criteria

- [ ] `SpannerSyncDriver` and `SpannerAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/spanner/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/spanner/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `SpannerSyncConfig` and `SpannerAsyncConfig` resolve `adapter_name = 'vertebra_spanner'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^spanner-drv]: `src/py/vertebra/db/adapters/spanner/driver.py` — Native Google Cloud Spanner & Spangres sync and async drivers.
[^spanner-core]: `src/py/vertebra/db/adapters/spanner/core.py` — Native Google Cloud Spanner & Spangres core parameter and wire helpers.
[^sqlspec-spn]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Current top-level Spanner SQLSpec module with invalid sqlglot dialect names and BigQuery ObjectRef import.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
