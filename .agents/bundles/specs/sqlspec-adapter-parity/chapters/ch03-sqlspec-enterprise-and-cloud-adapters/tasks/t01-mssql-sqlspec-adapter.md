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
- src/py/vertebra/extensions/sqlspec/adapters.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py
- src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py
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

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{core,driver,data_dictionary}.py` (`MSSQLSyncDriver`, `MSSQLAsyncDriver`, `MSSQLSyncDataDictionary`, `MSSQLAsyncDataDictionary`) per [`../spec.md`](../spec.md) [^mssql-drv] [^mssql-core] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py` over upstream SDK-free base classes per [`../spec.md`](../spec.md) [^mssql-drv] [^storage-art] [^sqlspec-adapters].

Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{config,__init__}.py` declaring `adapter_name = 'vertebra_mssql'` and explicit store class attributes per [`../spec.md`](../spec.md) [^mssql-drv] [^sqlspec-adapters].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/_driver.py` — `BaseSyncDriver` and `BaseAsyncDriver` dispatch template.
- `src/py/vertebra/db/adapters/mssql/driver.py` & `src/py/vertebra/db/adapters/mssql/core.py` — native SQL Server (MSSQL) execution and parameter encoding.

- `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py` & `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py` — SQL Server (MSSQL) SQLSpec session and dialect helpers.
- `src/py/vertebra/storage/_store.py` — `ArtifactReference` contract.

- `src/py/vertebra/extensions/sqlspec/adapters/mssql/driver.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/adk.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/events.py`, `src/py/vertebra/extensions/sqlspec/adapters/mssql/litestar.py` — adapter driver and store classes.
- `src/py/vertebra/extensions/sqlspec/_driver.py` — shared config lifecycle and refcounted pool close.

## Contracts & Edge Cases

- Set `StatementConfig(dialect='tsql')` with `ParameterStyle.QMARK` (`?` positional tuples rewritten by `MSSQLCore.rewrite_placeholders` to `@P1..@Pn` for `vertebra._mssql`), `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` savepoints, error `2601`/`2627`/`547`/`1205` mapping, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary`. Keep all T-SQL helpers strictly local to `adapters/mssql/core.py`.
- `MSSQLSyncDataDictionary(SyncDataDictionaryBase)` and `MSSQLAsyncDataDictionary(AsyncDataDictionaryBase)` execute upstream `sqlspec/data_dictionary/dialects/mssql/sql/*.sql` catalog queries (`get_version`, `get_feature_flag`, `get_optimal_type`, `get_tables`, `get_columns`, `get_indexes`, `get_foreign_keys`).

- Implement SDK-free `MSSQLSyncADKStore`/`MSSQLAsyncADKStore` (`MERGE ... WITH (HOLDLOCK)`, `NVARCHAR(MAX)` JSON, `DATETIMEOFFSET(6)`), `MSSQLSyncADKMemoryStore`/`MSSQLAsyncADKMemoryStore`, `MSSQLSyncADKArtifactStore`/`MSSQLAsyncADKArtifactStore`, `MSSQLEventQueueStore` (`IF OBJECT_ID(...) IS NULL` table/index DDL guards), and `MSSQLLitestarStore` (`VARBINARY(MAX)`).
- Never import `sqlspec.adapters.<third_party>.*` or third-party DB SDKs; execute all DDL and DML through `self._config.provide_session()`.

- Define `MSSQLSyncConfig` and `MSSQLAsyncConfig` declaring `adapter_name = 'vertebra_mssql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.
- Verify `sqlspec.utils.adapters.resolve_adapter_name` returns `'vertebra_mssql'` and `resolve_extension_store_class` resolves every store without `_compat.py`.

## Test Matrix

- Parametrized placeholder compilation, `TypedParameter` execution, zero-row `SELECT`, `RETURNING`/DML, `StatementConfig` override preservation, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary` catalog tests.

- Parametrized unit/contract tests for `MSSQLSyncADKStore`/`MSSQLAsyncADKStore`, memory search, `ArtifactReference` metadata, `MSSQLEventQueueStore`, and `MSSQLLitestarStore` TTL/upsert/delete.

- Resolver contract tests (`resolve_adapter_name`, `resolve_extension_store_class`, `_validate_adk_extension_config`), PEP 562 lazy import tests (zero eager `google.adk` import), and isolated `provide_session` transaction tests.

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
