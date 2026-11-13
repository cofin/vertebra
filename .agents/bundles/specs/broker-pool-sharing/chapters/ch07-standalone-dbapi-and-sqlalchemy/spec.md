---
type: Spec
flow_id: broker-pool-sharing
chapter_id: ch07-standalone-dbapi-and-sqlalchemy
title: "Chapter 07: Framework-Neutral vertebra.db.dbapi Package and SQLAlchemy BrokerPool"
description: "Extract SyncDBAPIConnection, AsyncDBAPIConnection, SyncDBAPIModule, AsyncDBAPIModule, cursors, and PEP 249 errors into vertebra.db.dbapi, isolate SQLAlchemy AsyncAdapt in extensions/sqlalchemy/async_adapter.py, and add BrokerPool."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - dbapi
  - sqlalchemy
  - advanced-alchemy
  - pool
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: "SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes"
  - id: db-config
    resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
    title: "BaseDatabaseConfig with dialect ClassVar and pool_key"
  - id: py-db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: "DatabaseRegistry URL parsing and driver resolution"
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport
    title: "Python SHM broker transports and lease objects"
---

# Chapter 07: Framework-Neutral vertebra.db.dbapi Package and SQLAlchemy BrokerPool

<!-- truth: start -->
- `vertebra.db.dbapi` exports PEP 249 errors, `is_row_returning_sql`, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, and `AsyncDBAPIModule` with zero imports from `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*` [^sa-dialects] [^db-config].
- `vertebra.extensions.sqlalchemy.async_adapter` wraps `vertebra.db.dbapi.AsyncDBAPIConnection` in `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection` [^sa-dialects].
- `extensions/sqlalchemy/dialects.py` deletes embedded DB-API duplicates with zero compatibility re-exports and checks `type(config).dialect` / `capabilities.supports_returning` instead of adapter `isinstance` [^sa-dialects] [^db-config].
- `extensions/sqlalchemy/pool.py` provides `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` so SQLAlchemy and Advanced Alchemy share broker-leased pools across workers [^broker-transport].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, `src/py/vertebra/extensions/sqlalchemy/dialects.py` embeds PEP 249 exceptions, `SyncDBAPI*`, and `AsyncDBAPI*` directly alongside SQLAlchemy dialects, importing `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection` and concrete `BigQueryConfig` / `SpannerConfig` / `SQLiteConfig` classes [^sa-dialects]. Extracting `vertebra.db.dbapi` establishes a clean database-layer PEP 249 seam for SQLAlchemy, Advanced Alchemy, and downstream Django backends while adding `BrokerPool`.

**Non-Goals:** Django backend rewrites (owned by `sqlalchemy-and-django-integration`); Relocating `vertebra.db.dbapi` in later flows.

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Neutral DB-API | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py` [^db-config] [^py-db-registry] | PEP 249 sync + async DB-API with strict import boundary |
| SQLAlchemy adapter | `src/py/vertebra/extensions/sqlalchemy/{async_adapter,dialects,alembic}.py` [^sa-dialects] | `AsyncAdapt_dbapi_connection` bridge + `+vertebra`/`+vtb` dialects |
| Broker pool | `src/py/vertebra/extensions/sqlalchemy/{pool,__init__}.py` [^broker-transport] | `BrokerPool`, `AsyncBrokerPool`, `broker_creator` |

**Knowledge Anchors:** [SQLSpec, Alchemy, and Dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R17a**: Create `vertebra.db.dbapi` exporting PEP 249 errors, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, `AsyncDBAPIModule`, `is_row_returning_sql`, and `connect` with zero upward or engine-specific imports (`t01`).
- **R17b / R17c**: Isolate `AsyncAdapt_dbapi_connection` in `extensions/sqlalchemy/async_adapter.py`, delete embedded DB-API classes from `dialects.py`, replace concrete config `isinstance` checks with `dialect` / capability metadata, and rewire import sites (`t02`).
- **R18**: Implement `BrokerPool`, `AsyncBrokerPool`, and `broker_creator` in `extensions/sqlalchemy/pool.py` with rollback-on-return and quarantine-on-failure (`t03`).

```python
# src/py/vertebra/db/dbapi/__init__.py
apilevel: Final = "2.0"
threadsafety: Final = 2
paramstyle: Final = "qmark"
class SyncDBAPICursor: ...
class SyncDBAPIConnection: ...
class SyncDBAPIModule: ...
class AsyncDBAPICursor: ...
class AsyncDBAPIConnection: ...
class AsyncDBAPIModule: ...
def is_row_returning_sql(operation: str) -> bool: ...
def connect(url: str, **kwargs: Any) -> SyncDBAPIConnection: ...

# src/py/vertebra/extensions/sqlalchemy/pool.py
class BrokerPool(Pool): ...
class AsyncBrokerPool(Pool): ...
def broker_creator(url: str, *, broker: SyncBrokerLeaseSource) -> Callable[[], SyncDBAPIConnection]: ...
```

**Integration Flow:**
- `Sync*Dialect.import_dbapi()` -> `vertebra.db.dbapi.SyncDBAPIModule`.
- `Async*Dialect.import_dbapi()` -> `extensions.sqlalchemy.async_adapter.SQLAlchemyAsyncDBAPI` wrapping `vertebra.db.dbapi.AsyncDBAPIConnection`.
- `create_engine(url, poolclass=BrokerPool, creator=broker_creator(url, broker=transport))` -> shared SHM partition checkout + quarantine on failed return rollback.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Import boundary | Fresh subprocess `import vertebra.db.dbapi` | Zero `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters` in `sys.modules` | Clean layer separation |
| Statement autocommit | `type(config).dialect in {"bigquery", "spanner"}` with `autocommit=False` | Raises PEP 249 `NotSupportedError` | No adapter `isinstance` coupling |
| Pool return failure | Rollback raises in `BrokerPool._do_return_conn` | Quarantines lease and raises `LeaseQuarantinedError` | No dirty pooled session |

## 3. Documentation & OKF Knowledge Plan

`vertebra.db.dbapi` and `BrokerPool` usage are documented in `ch08:t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R17a` | `t01` | `src/py/vertebra/db/dbapi/{__init__,_sync,_async}.py` | `pytest src/py/tests/unit/db/test_dbapi_standalone.py` |
| `R17b`, `R17c` | `t02` | `src/py/vertebra/extensions/sqlalchemy/{async_adapter,dialects,alembic}.py` | `pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `R18` | `t03` | `src/py/vertebra/extensions/sqlalchemy/{pool,__init__}.py` | `pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-dbapi-sync-and-async-extraction.md](tasks/t01-dbapi-sync-and-async-extraction.md) — Create vertebra.db.dbapi with PEP 249 errors, SyncDBAPI*, AsyncDBAPI*, and comment-aware is_row_returning_sql.
- [ ] [t02-sqlalchemy-adapter-and-dialect-rewire.md](tasks/t02-sqlalchemy-adapter-and-dialect-rewire.md) — Add extensions/sqlalchemy/async_adapter.py and remove embedded DB-API classes and adapter isinstance checks from dialects.py.
- [ ] [t03-sqlalchemy-and-advanced-alchemy-broker-pool.md](tasks/t03-sqlalchemy-and-advanced-alchemy-broker-pool.md) — Implement BrokerPool, AsyncBrokerPool, and broker_creator leasing shared broker connections with quarantine-on-error.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: All four SQLAlchemy entry points per engine (`+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async`) remain intact with zero re-export shims in `dialects.py`.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — SQLAlchemy-embedded PEP 249 module, connection, cursor, and dialect classes.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — BaseDatabaseConfig with dialect ClassVar and pool_key.
[^py-db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — DatabaseRegistry URL parsing and driver resolution.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::_BaseSHMBrokerTransport` — Python SHM broker transports and lease objects.
