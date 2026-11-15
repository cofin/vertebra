---
type: Task
id: sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t02
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-enterprise-and-cloud-adapters
title: Deliver Feature-Complete DuckDB SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/duckdb/ (DuckDB 2.0+ lexical COPY ordering, Arrow-divergent fetchall() fidelity, DataDictionary, ADK/Events/Litestar stores, adapter_name='vertebra_duckdb').
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
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/data_dictionary.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/adk.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/events.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/litestar.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/config.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlspec/_driver.py
- src/py/vertebra/db/adapters/duckdb/driver.py
- src/py/vertebra/db/adapters/duckdb/core.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py
- src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py
estimated_loc: 245
tags:
- sqlspec
- duckdb
- data-dictionary
- adk
- events
- litestar
- config
sources:
- id: duckdb-drv
  resource: src/py/vertebra/db/adapters/duckdb/driver.py
  title: Native DuckDB sync and async drivers
- id: duckdb-core
  resource: src/py/vertebra/db/adapters/duckdb/core.py
  title: Native DuckDB core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Existing DuckDB SQLSpec adapter
- id: storage-art
  resource: src/py/vertebra/storage/_store.py
  title: Storage-neutral ArtifactReference value struct
---

# Task `t02`: Deliver Feature-Complete DuckDB SQLSpec Adapter Package

## Objective & Scope Boundary

Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{core,driver,data_dictionary}.py` (`DuckDBSyncDriver`, `DuckDBAsyncDriver`, `DuckDBSyncDataDictionary`, `DuckDBAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^duckdb-drv] [^duckdb-core] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^duckdb-drv] [^storage-art] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{config,__init__}.py` declaring `adapter_name = 'vertebra_duckdb'` and explicit store class attributes per [`../spec.md`](../spec.md) [^duckdb-drv] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/duckdb/driver.py` & `src/py/vertebra/db/adapters/duckdb/core.py` — native DuckDB execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/duckdb/core.py` — DuckDB SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/duckdb/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/duckdb/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/duckdb/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/duckdb/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='duckdb')` supporting `{ParameterStyle.QMARK, ParameterStyle.NUMERIC}` (`?` and `$1`), DuckDB 2.0+ lexical `COPY (SELECT ... ?) TO ?` parameter ordering, native Python `fetchall()` preservation for Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `INTERVAL`, `MAP`) alongside zero-copy `select_to_arrow` / `load_from_arrow`, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary`. Keep all helpers local to `adapters/duckdb/core.py`.
- `DuckDBSyncDataDictionary(SyncDataDictionaryBase)` and `DuckDBAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/duckdb/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, `array_cosine_similarity` / `fts` `DuckDBSyncADKMemoryStore`/`DuckDBAsyncADKMemoryStore`, `DuckDBSyncADKArtifactStore`/`DuckDBAsyncADKArtifactStore`, `DuckDBEventQueueStore`, and `DuckDBLitestarStore` (`ON CONFLICT DO UPDATE`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `DuckDBSyncConfig` and `DuckDBAsyncConfig` declaring `adapter_name = 'vertebra_duckdb'`, isolated `:memory:<name>` database handling, and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_duckdb'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, memory search, `ArtifactReference` metadata, `DuckDBEventQueueStore`, and `DuckDBLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/duckdb && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `DuckDBSyncDriver` and `DuckDBAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/duckdb/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `DuckDBSyncConfig` and `DuckDBAsyncConfig` resolve `adapter_name = 'vertebra_duckdb'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-drv]: `src/py/vertebra/db/adapters/duckdb/driver.py` — Native DuckDB sync and async drivers.
[^duckdb-core]: `src/py/vertebra/db/adapters/duckdb/core.py` — Native DuckDB core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing DuckDB SQLSpec adapter.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
