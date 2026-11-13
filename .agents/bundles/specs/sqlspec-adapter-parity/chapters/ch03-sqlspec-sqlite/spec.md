---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch03-sqlspec-sqlite
title: "Chapter 03: SQLSpec SQLite Adapter Package (sqlite)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/sqlite/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_sqlite' configs)."
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
  - sqlite
  - adk
  - events
  - litestar
sources:
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: "Native SQLite sync and async drivers"
  - id: sqlite-core
    resource: src/py/vertebra/db/adapters/sqlite/core.py
    title: "Native SQLite core parameter and wire helpers"
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py
    title: "Existing SQLite SQLSpec adapter"
---

# Chapter 03: SQLSpec SQLite Adapter Package (sqlite)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: SQLite SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/sqlite/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^sqlspec-adapters].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/sqlite/driver.py` with `adapter_name = 'vertebra_sqlite'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^sqlite-drv] [^sqlite-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/sqlite/` SQLSpec adapter package over Vertebra's native SQLite driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/sqlite/core.py` [^sqlite-drv] [^sqlite-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='sqlite')` supporting execution `{ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}` (`?` tuples and `:name` dicts without unnecessary rewriting), `PRAGMA` row-returning classification, WAL/foreign_keys/busy_timeout pragmas, and savepoints. Keep all SQLite SQL/parameter helpers strictly inside `adapters/sqlite/core.py`.
- **Sub-Extension Stores**: Implement `SQLiteSyncADKStore`/`SQLiteAsyncADKStore`, `FTS5`-backed `SQLiteSyncADKMemoryStore`/`SQLiteAsyncADKMemoryStore`, `SQLiteSyncADKArtifactStore`/`SQLiteAsyncADKArtifactStore` (`ArtifactReference`), `SQLiteEventQueueStore`, and `SQLiteLitestarStore` (`ON CONFLICT(key) DO UPDATE`) executing exclusively via `provide_session()` over native `SyncSQLiteDriver`/`AsyncSQLiteDriver`.
- **Config & Lazy Exports**: Define `SQLiteSyncConfig` and `SQLiteAsyncConfig` declaring `adapter_name = 'vertebra_sqlite'`, explicit `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, shared `:memory:` URI handling, and PEP 562 lazy `__init__.py`.

```python
class SQLiteSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "sqlite"

class SQLiteAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "sqlite"

class SQLiteSyncDriver(BaseSyncDriver):
    dialect = "sqlite"

class SQLiteAsyncDriver(BaseAsyncDriver):
    dialect = "sqlite"

class SQLiteSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_sqlite"
    driver_type = SQLiteSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class SQLiteAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_sqlite"
    driver_type = SQLiteAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| SQLite driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| SQLite ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| SQLite configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-sqlite-driver-and-dictionary.md](tasks/t01-sqlite-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{core,driver,data_dictionary}.py`.
- [ ] [t02-sqlite-extension-stores.md](tasks/t02-sqlite-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{adk,events,litestar}.py`.
- [ ] [t03-sqlite-config-and-barrel.md](tasks/t03-sqlite-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/sqlite/{config,__init__}.py` (`adapter_name = 'vertebra_sqlite'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-sqlspec-sqlite` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/sqlite && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Native SQLite sync and async drivers.
[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py` — Native SQLite core parameter and wire helpers.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Existing SQLite SQLSpec adapter.
