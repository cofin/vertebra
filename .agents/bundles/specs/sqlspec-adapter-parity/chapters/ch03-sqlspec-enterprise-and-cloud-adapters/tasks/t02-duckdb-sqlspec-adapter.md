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
- src/py/vertebra/db/adapters/duckdb/config.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/storage/_store.py
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

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/duckdb/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`DuckDBSyncDriver`/`DuckDBAsyncDriver`, `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `DuckDBSyncConfig`/`DuckDBAsyncConfig`) per [`../spec.md`](../spec.md) [^duckdb-drv] [^duckdb-core] [^sqlspec-adapters] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/duckdb/driver.py` — native DuckDB sync and async execution and Arrow drivers.
- `src/py/vertebra/db/adapters/duckdb/core.py` — native DuckDB parameter and wire helpers.
- `src/py/vertebra/db/adapters/duckdb/config.py` — `DuckDBConfig` `:memory:<name>` connection options.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy DuckDB SQLSpec logic to replace cleanly.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='duckdb')` supporting `{ParameterStyle.QMARK, ParameterStyle.NUMERIC}` (`?` and `$1`), DuckDB 2.0+ lexical `COPY (SELECT ... ?) TO ?` parameter ordering, native Python `fetchall()` preservation for Arrow-divergent types (`HUGEINT`, `UHUGEINT`, `BIT`, `TIMETZ`, `INTERVAL`, `MAP`) alongside zero-copy `select_to_arrow` / `load_from_arrow`, and `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary` executing upstream `sqlspec/data_dictionary/dialects/duckdb/sql/*.sql` catalog queries. Keep all helpers local to `adapters/duckdb/core.py`.
- Implement SDK-free `DuckDBSyncADKStore`/`DuckDBAsyncADKStore`, `array_cosine_similarity` / `fts` `DuckDBSyncADKMemoryStore`/`DuckDBAsyncADKMemoryStore`, `DuckDBSyncADKArtifactStore`/`DuckDBAsyncADKArtifactStore` (`ArtifactReference`), `DuckDBEventQueueStore`, and `DuckDBLitestarStore` (`ON CONFLICT DO UPDATE`) executing via `self._config.provide_session()` with zero third-party DB SDK or `sqlspec.adapters.*` imports.
- Define `DuckDBSyncConfig` and `DuckDBAsyncConfig` declaring `adapter_name = 'vertebra_duckdb'`, isolated `:memory:<name>` database handling, and explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` (plus lazy `*ADKArtifactStore` exports) via PEP 562 `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized QMARK/NUMERIC placeholder compilation, `COPY` lexical parameter ordering, Arrow-divergent `fetchall()` fidelity, zero-row `SELECT`, `RETURNING`/DML, `DuckDBSyncDataDictionary`/`DuckDBAsyncDataDictionary` catalog tests, store contract tests (ADK/FTS/`ArtifactReference`/events/Litestar), PEP 562 lazy `google.adk` import tests, and resolver checks.

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
