---
type: Spec
flow_id: sqlspec-adapter-parity
chapter_id: ch02-sqlspec-postgres
title: "Chapter 02: SQLSpec PostgreSQL Adapter Package (postgres)"
description: "Deliver full-parity src/py/vertebra/extensions/sqlspec/adapters/postgres/ (driver dispatch, Sync/Async DataDictionary, ADK/Events/Litestar stores, and adapter_name='vertebra_postgres' configs)."
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
  - postgres
  - adk
  - events
  - litestar
sources:
  - id: postgres-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native PostgreSQL sync and async drivers"
  - id: postgres-core
    resource: src/py/vertebra/db/adapters/postgres/core.py
    title: "Native PostgreSQL core parameter and wire helpers"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification source"
---

# Chapter 02: SQLSpec PostgreSQL Adapter Package (postgres)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: PostgreSQL SQLSpec integration lacks modular `src/py/vertebra/extensions/sqlspec/adapters/postgres/` parity, complete data dictionaries, and zero-SDK ADK/Events/Litestar stores [^pg-notify].
- **Target Architecture**: `src/py/vertebra/extensions/sqlspec/adapters/postgres/{__init__,config,core,driver,data_dictionary,adk,events,litestar}.py` implements full `sqlspec>=0.71.1` parity over native `src/py/vertebra/db/adapters/postgres/driver.py` with `adapter_name = 'vertebra_postgres'`, explicit `*_store_class` attributes, and zero cross-adapter SQL helper modules [^postgres-drv] [^postgres-core].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter builds the standalone `src/py/vertebra/extensions/sqlspec/adapters/postgres/` SQLSpec adapter package over Vertebra's native PostgreSQL driver, keeping all dialect SQL and parameter helpers local to `src/py/vertebra/extensions/sqlspec/adapters/postgres/core.py` [^postgres-drv] [^postgres-core].

## 2. Requirements, Concrete Contracts & Architecture

- **Driver & DataDictionary**: Set `StatementConfig(dialect='postgres')` with default and execution `ParameterStyle.NUMERIC` (`$1, $2` positional sequences matching `vertebra._postgres` instead of legacy `%s`), native Arrow `COPY BINARY` `select_to_arrow`, server-side portal `dispatch_select_stream`, pipeline `execute_stack`, and savepoints. Duplicate all PostgreSQL SQL/parameter helpers locally inside `adapters/postgres/core.py` (zero cross-adapter helper modules).
- **Sub-Extension Stores**: Subclass SDK-free upstream bases (`BaseSyncADKStore`/`BaseAsyncADKStore`, pgvector cosine/HNSW + `tsvector` `BaseSyncADKMemoryStore`/`BaseAsyncADKMemoryStore`, `BaseSyncADKArtifactStore`/`BaseAsyncADKArtifactStore` returning `ArtifactReference` on the shared surface, `BaseEventQueueStore` + `PostgresSyncEventsBackend`/`PostgresAsyncEventsBackend` over `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and `BaseSQLSpecStore` `ON CONFLICT (key) DO UPDATE`) with zero `psycopg` imports.
- **Config & Lazy Exports**: Define `PostgresSyncConfig` and `PostgresAsyncConfig` declaring `adapter_name = 'vertebra_postgres'`, `adk_store_class`, `adk_session_store_class`, `adk_memory_store_class`, `adk_artifact_store_class`, `events_store_class`, and `litestar_store_class`, using PEP 562 `__getattr__` in `__init__.py` so `google.adk` is loaded lazily and guarded via `MissingDependencyError`.

```python
class PostgresSyncDataDictionary(SyncDataDictionaryBase):
    dialect = "postgres"

class PostgresAsyncDataDictionary(AsyncDataDictionaryBase):
    dialect = "postgres"

class PostgresSyncDriver(BaseSyncDriver):
    dialect = "postgres"

class PostgresAsyncDriver(BaseAsyncDriver):
    dialect = "postgres"

class PostgresSyncConfig(SyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_postgres"
    driver_type = PostgresSyncDriver
    adk_store_class: ClassVar[type[Any]]
    adk_session_store_class: ClassVar[type[Any]]
    adk_memory_store_class: ClassVar[type[Any]]
    adk_artifact_store_class: ClassVar[type[Any]]
    events_store_class: ClassVar[type[Any]]
    litestar_store_class: ClassVar[type[Any]]

class PostgresAsyncConfig(AsyncAdapterConfig):
    adapter_name: ClassVar[str] = "vertebra_postgres"
    driver_type = PostgresAsyncDriver
```

## 3. Documentation & OKF Knowledge Plan

Adapter exports and store capabilities are documented in `ch10-sqlspec-cutover-storage-events-docs`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| PostgreSQL driver & data dictionary | `t01` | `src/py/vertebra/extensions/sqlspec/adapters/postgres/{core,driver,data_dictionary}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| PostgreSQL ADK, Events & Litestar stores | `t02` | `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| PostgreSQL configs & PEP 562 barrel | `t03` | `src/py/vertebra/extensions/sqlspec/adapters/postgres/{config,__init__}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-postgres-driver-and-dictionary.md](tasks/t01-postgres-driver-and-dictionary.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{core,driver,data_dictionary}.py`.
- [ ] [t02-postgres-extension-stores.md](tasks/t02-postgres-extension-stores.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{adk,events,litestar}.py`.
- [ ] [t03-postgres-config-and-barrel.md](tasks/t03-postgres-config-and-barrel.md) — Implement `src/py/vertebra/extensions/sqlspec/adapters/postgres/{config,__init__}.py` (`adapter_name = 'vertebra_postgres'`).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-sqlspec-postgres` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing`, `native-sqlite-driver`, `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^postgres-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL sync and async drivers.
[^postgres-core]: `src/py/vertebra/db/adapters/postgres/core.py` — Native PostgreSQL core parameter and wire helpers.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification source.
