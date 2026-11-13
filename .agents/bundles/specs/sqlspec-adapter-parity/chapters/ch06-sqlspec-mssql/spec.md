---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch06-sqlspec-mssql
title: "Chapter 06: SQLSpec SQL Server (MSSQL) Adapter Package (mssql)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/mssql/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_mssql' configs)."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:50:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:50:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:50:00Z"
    commit: 18baadcd
tags:
  - sqlspec
  - mssql
  - adk
  - events
  - litestar
sources:
  - id: mssql-drv
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "Native SQL Server (MSSQL) sync and async drivers"
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py
    title: "Native SQL Server (MSSQL) core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing MSSQL SQLSpec adapter"
---

# Chapter 06: SQLSpec SQL Server (MSSQL) Adapter Package (mssql)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: SQL Server (MSSQL) SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/mssql/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/mssql/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/mssql/driver.py` with `adapter_name = 'vertebra_mssql'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^mssql-drv] [^mssql-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/mssql/` SQLSpec adapter package over Vertebra's native SQL Server (MSSQL) driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/mssql/core.py` [^mssql-drv] [^mssql-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='tsql')` with `ParameterStyle.QMARK` (`?` positional tuples rewritten by `MSSQLCore.rewrite_placeholders` to `@P1..@Pn` for `vertebra._mssql`), `SAVE TRANSACTION` / `ROLLBACK TRANSACTION` savepoints, error `2601`/`2627`/`547`/`1205` mapping, and `MSSQLSyncDataDictionary`/`MSSQLAsyncDataDictionary`. Keep all T-SQL helpers strictly local to `adapters/mssql/core.py`.
- **Sub-Extension Stores**: Implement SDK-free `MSSQLSyncADKStore`/`MSSQLAsyncADKStore` (`MERGE ... WITH (HOLDLOCK)`, `NVARCHAR(MAX)` JSON, `DATETIMEOFFSET(6)`), `MSSQLSyncADKMemoryStore`/`MSSQLAsyncADKMemoryStore`, `MSSQLSyncADKArtifactStore`/`MSSQLAsyncADKArtifactStore`, `MSSQLEventQueueStore` (`IF OBJECT_ID(...) IS NULL` table/index DDL guards), and `MSSQLLitestarStore` (`VARBINARY(MAX)`).
- **Config & Lazy Exports**: Define `MSSQLSyncConfig` and `MSSQLAsyncConfig` declaring `adapter_name = 'vertebra_mssql'` and explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class` with PEP 562 lazy `__init__.py`.

```python
class MSSQLSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "tsql"

class MSSQLAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "tsql"

class MSSQLSyncDriver(BaseSyncDriver):
    dialect = "tsql"

class MSSQLAsyncDriver(BaseAsyncDriver):
    dialect = "tsql"

class MSSQLSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mssql"
    driver_type = MSSQLSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class MSSQLAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_mssql"
    driver_type = MSSQLAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| SQL Server (MSSQL) driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/mssql/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| SQL Server (MSSQL) ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| SQL Server (MSSQL) configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/mssql/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-mssql-driver-and-dictionary.md](tasks/t01-mssql-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{core,driver,data_dictionary}.py`.
- [ ] [t02-mssql-extension-stores.md](tasks/t02-mssql-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{adk,events,litestar}.py`.
- [ ] [t03-mssql-config-and-barrel.md](tasks/t03-mssql-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/mssql/{config,__init__}.py` (`adapter_name = 'vertebra_mssql'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch06-sqlspec-mssql` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/mssql && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^mssql-drv]: `src/py/vertebra/db/adapters/mssql/driver.py` — Native SQL Server (MSSQL) sync and async drivers.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py` — Native SQL Server (MSSQL) core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing MSSQL SQLSpec adapter.
