---
type: Task
id: sqlspec-adapter-parity:ch03-sqlspec-enterprise-and-cloud-adapters:t01
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-enterprise-and-cloud-adapters
title: Deliver Feature-Complete SQL Server SQLSpec Adapter Package
description: Build extensions/sqlspec/adapters/mssql/ (dialect='tsql', ? -> @P1, MERGE HOLDLOCK ADK/Events/Litestar stores, DataDictionary, adapter_name='vertebra_mssql').
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
- src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/data_dictionary.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/adk.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/events.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/litestar.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/config.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/__init__.py
tests:
- src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
- src/py/vertebra/extensions/sqlspec/_driver.py
- src/py/vertebra/db/adapters/mssql/driver.py
- src/py/vertebra/db/adapters/mssql/core.py
- src/py/vertebra/db/adapters/mssql/config.py
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/storage/_store.py
estimated_loc: 245
tags:
- sqlspec
- mssql
- data-dictionary
- adk
- events
- litestar
- config
sources:
- id: mssql-drv
  resource: src/py/vertebra/db/adapters/mssql/driver.py
  title: Native SQL Server (MSSQL) sync and async drivers
- id: mssql-core
  resource: src/py/vertebra/db/adapters/mssql/core.py
  title: Native SQL Server (MSSQL) core parameter and wire helpers
- id: sqlspec-adapters
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Existing MSSQL SQLSpec adapter
- id: storage-art
  resource: src/py/vertebra/storage/_store.py
  title: Storage-neutral ArtifactReference value struct
---

# Task `t01`: Deliver Feature-Complete SQL Server SQLSpec Adapter Package

## Objective & Scope Boundary

Implement the complete `src/py/vertebra/extensions/sqlspec/adapters/mssql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` SQLSpec adapter package (`MSSQLSyncDriver`/`MSSQLAsyncDriver`, `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary`, SDK-free ADK/Events/Litestar stores, and `MSSQLSyncConfig`/`MSSQLAsyncConfig`) per [`../spec.md`](../spec.md) [^mssql-drv] [^mssql-core] [^sqlspec-adapters] [^storage-art].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template, config lifecycle, and refcounted pool close.
- `src/py/vertebra/db/adapters/mssql/driver.py` — native SQL Server (MSSQL) sync and async execution drivers.
- `src/py/vertebra/db/adapters/mssql/core.py` — `MSSQLCore.rewrite_placeholders` and TDS wire helpers.
- `src/py/vertebra/db/adapters/mssql/config.py` — `MSSQLConfig` connection options.
- `src/py/vertebra/extensions/sqlspec/adapters.py` — legacy MSSQL SQLSpec logic to replace cleanly.
- `src/py/vertebra/storage/_store.py` — storage-neutral `ArtifactReference` value struct.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='tsql')` with `ParameterStyle.QMARK` (`?` positional tuples rewritten by `MSSQLCore.rewrite_placeholders` to `@P1..@Pn` for `vertebra._mssql`), `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` savepoints, error `2601`/`2627`/`547`/`1205` mapping, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary` executing upstream `sqlspec/data_dictionary/dialects/mssql/sql/*.sql` catalog queries. Keep all T-SQL helpers strictly local to `adapters/mssql/core.py`.
- Implement SDK-free `MSSQLSyncADKStore`/`MSSQLAsyncADKStore` (`MERGE ... WITH (HOLDLOCK)`, `NVARCHAR(MAX)` JSON, `DATETIMEOFFSET(6)`), `MSSQLSyncADKMemoryStore`/`MSSQLAsyncADKMemoryStore`, `MSSQLSyncADKArtifactStore`/`MSSQLAsyncADKArtifactStore` (`ArtifactReference`), `MSSQLEventQueueStore` (`IF OBJECT_ID(...) IS NULL` table/index DDL guards), and `MSSQLLitestarStore` (`VARBINARY(MAX)`) executing via `self._config.provide_session()` with zero third-party DB SDK or `sqlspec.adapters.*` imports.
- Define `MSSQLSyncConfig` and `MSSQLAsyncConfig` declaring `adapter_name = 'vertebra_mssql'`, explicit `adk_store_class`, `adk_memory_store_class`, `events_store_class`, and `litestar_store_class` (plus lazy `*ADKArtifactStore` exports) via PEP 562 `__init__.py` (verified via `resolve_adapter_name` and `resolve_extension_store_class` without `_compat.py`).

## Test Matrix

- Parametrized `@P1` placeholder rewrite, `TypedParameter` execution, zero-row `SELECT`, DML, `StatementConfig` override preservation, `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary` catalog tests, store contract tests (ADK `MERGE HOLDLOCK`/`ArtifactReference`/`IF OBJECT_ID` events/Litestar), PEP 562 lazy `google.adk` import tests, and resolver checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `MSSQLSyncDriver` and `MSSQLAsyncDriver` pass native parameterized execution and data dictionary introspection checks.
- [ ] Zero cross-adapter shared SQL helper imports exist in `src/py/vertebra/extensions/sqlspec/adapters/mssql/`.
- [ ] `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py` pass upstream store contract assertions with zero third-party driver SDK imports.
- [ ] `MSSQLSyncConfig` and `MSSQLAsyncConfig` resolve `adapter_name = 'vertebra_mssql'` and all extension store classes natively on `sqlspec>=0.71.1`.
- [ ] Zero regressions in the fast verification gate.

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py` — Native SQL Server (MSSQL) core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
[^storage-art]: `src/py/vertebra/storage/_store.py` — Storage-neutral ArtifactReference value struct.
