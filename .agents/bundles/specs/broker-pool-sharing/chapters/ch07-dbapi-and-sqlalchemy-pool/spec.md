---
type: Spec
parent_flow: broker-pool-sharing
chapter_id: ch07-dbapi-and-sqlalchemy-pool
title: "Chapter 7: DB-API Package and SQLAlchemy Broker Pool"
description: Extract the PEP 249 module, connection, cursor, and error classes from the SQLAlchemy dialects into a framework-neutral vertebra.db.dbapi package, move SQLAlchemy async adaptation into extensions/sqlalchemy/async_adapter.py, and add a BrokerPool for SQLAlchemy and Advanced Alchemy configs.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-lease-safety-and-host
replan_trigger: null
current_task: "broker-pool-sharing:ch07:t01"
plan_revision: 3
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:40:00Z"
updated_at: "2026-11-12T01:40:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:40:00Z"
    commit: c8722c76
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: SQLAlchemy-embedded PEP 249 module, connection, cursor, and error classes
  - id: sa-async-module
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIModule
    title: Async DB-API module wrapping connections in AsyncAdapt_dbapi_connection
  - id: row-returning
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::_is_row_returning_sql
    title: Statement-kind classifier driving rowcount and description
  - id: db-config
    resource: src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig
    title: Base database config with dialect ClassVar and pool_key
  - id: db-registry
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry
    title: URL parsing and shared driver resolution
  - id: broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::SyncSHMBrokerTransport
    title: SHM broker transport leasing connections
tags:
  - dbapi
  - sqlalchemy
  - advanced-alchemy
  - broker
---

# Chapter 7: DB-API Package and SQLAlchemy Broker Pool

<!-- truth: start -->
- `src/py/vertebra/extensions/sqlalchemy/dialects.py` defines the PEP 249 surface (`Error` hierarchy, `SyncDBAPICursor`, `SyncDBAPIConnection`, `SyncDBAPIModule`, `AsyncDBAPICursor`, `AsyncDBAPIConnection`, `AsyncDBAPIModule`, `_is_row_returning_sql`) next to the SQLAlchemy dialects [^sa-dialects].
- `AsyncDBAPIModule.connect` returns `sqlalchemy.connectors.asyncio.AsyncAdapt_dbapi_connection(self, async_conn)`, so the async DB-API surface imports SQLAlchemy [^sa-async-module].
- `SyncDBAPIConnection` / `AsyncDBAPIConnection` import `BigQueryConfig`, `SpannerConfig`, and `SQLiteConfig` only for `isinstance` checks (statement autocommit and the sqlite3 fast path) [^sa-dialects].
- After this chapter, `vertebra.db.dbapi` holds the PEP 249 surface with imports limited to the standard library, `vertebra.db.registry`, `vertebra.db.driver`, `vertebra.typing`, and `vertebra.utils`; `extensions/sqlalchemy/async_adapter.py` owns `AsyncAdapt_dbapi_connection` wrapping; `extensions/sqlalchemy/pool.py::BrokerPool` leases from the shared broker.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The PEP 249 classes are the natural seam for Django, SQLAlchemy, and plain DB-API consumers, but they live inside the SQLAlchemy extension and the async module imports SQLAlchemy directly [^sa-dialects] [^sa-async-module]. SQLAlchemy and Advanced Alchemy configs pool per process through `QueuePool` / `AsyncAdaptedQueuePool`, so they cannot share the broker-owned pool. This chapter extracts the DB-API package with recorded contract decisions, isolates SQLAlchemy adaptation, rewires the dialects, and adds `BrokerPool`.

### 1.2 Non-Goals

- Django backend integration and any relocation of `vertebra.db.dbapi` (owned by `sqlalchemy-and-django-integration`).
- New dialect features; dialect behavior stays identical apart from import locations.

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Errors | `dialects.py::Error`, `::InterfaceError`, `::DatabaseError`, `::OperationalError`, `::ProgrammingError`, `::NotSupportedError` [^sa-dialects] | PEP 249 module attributes. |
| Sync DB-API | `dialects.py::SyncDBAPICursor`, `::SyncDBAPIConnection`, `::SyncDBAPIModule`, `::_is_row_returning_sql` [^sa-dialects] [^row-returning] | `DatabaseRegistry.parse_url`, shared sync drivers [^db-registry]. |
| Async DB-API | `dialects.py::AsyncDBAPICursor`, `::AsyncDBAPIConnection`, `::AsyncDBAPIModule.connect` [^sa-async-module] | SQLAlchemy `AsyncAdapt_dbapi_connection` (remote library). |
| Config identity | `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig`; per-adapter `dialect: ClassVar[str]` [^db-config] | Replaces engine `isinstance` checks. |
| Consumers | `extensions/sqlalchemy/__init__.py`, `alembic.py`, `tests/unit/extensions/test_django_plugin.py`, `tests/unit/db/test_native_db_drivers.py`, `tests/unit/db/test_enterprise_olap_drivers.py` | Import sites rewired without re-exports. |
| Broker | `src/py/vertebra/extensions/broker/transport.py::SyncSHMBrokerTransport`, `::AsyncSHMBrokerTransport` [^broker-transport] | Shared lease source for `BrokerPool`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database Drivers & Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Broker and SHM Leases](../../../../knowledge/domains/data/broker-and-shm-leases.md).
- Skills: `advanced-alchemy`, `.agents/skills/python-framework/SKILL.md`, `.agents/skills/review-tests/SKILL.md`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R17a**: `vertebra.db.dbapi` exports the error hierarchy and sync DB-API classes with recorded statement-kind, rowcount, and lastrowid semantics; an import-boundary test proves no `vertebra.extensions`, `sqlalchemy`, or `vertebra.db.adapters.*` imports (`t01`).
- **R17b**: Async DB-API classes move to `vertebra.db.dbapi._async`; `extensions/sqlalchemy/async_adapter.py` owns `AsyncAdapt_dbapi_connection` wrapping (`t02`).
- **R17c**: `dialects.py` imports from the new locations; embedded copies are deleted with no compatibility re-export (`t03`).
- **R18**: `BrokerPool` and `broker_creator` share broker pools for SQLAlchemy and Advanced Alchemy sync and async configs (`t04`).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
# vertebra.db.dbapi
apilevel: Final = "2.0"
threadsafety: Final = 2
paramstyle: Final = "qmark"
class Error(Exception): ...
class Warning(Exception): ...
class InterfaceError(Error): ...
class DatabaseError(Error): ...
class DataError(DatabaseError): ...
class OperationalError(DatabaseError): ...
class IntegrityError(DatabaseError): ...
class InternalError(DatabaseError): ...
class ProgrammingError(DatabaseError): ...
class NotSupportedError(DatabaseError): ...
class SyncDBAPICursor: ...
class SyncDBAPIConnection: ...
class SyncDBAPIModule: ...
class AsyncDBAPICursor: ...
class AsyncDBAPIConnection: ...
def is_row_returning_sql(operation: str) -> bool: ...
def connect(url: str, **kwargs: Any) -> SyncDBAPIConnection: ...

# vertebra.extensions.sqlalchemy.async_adapter
class AsyncDBAPIModule:
    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_dbapi_connection: ...

# vertebra.extensions.sqlalchemy.pool
class BrokerPool(Pool): ...
class AsyncBrokerPool(Pool): ...
def broker_creator(url: str, *, broker: SyncBrokerLeaseSource) -> Callable[[], SyncDBAPIConnection]: ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncPostgreSQLDialect.import_dbapi()` → `vertebra.db.dbapi.SyncDBAPIModule(paramstyle=...)` → `SyncDBAPIConnection` → `DatabaseRegistry.parse_url` → shared driver.
- `AsyncPostgreSQLDialect.import_dbapi()` → `extensions.sqlalchemy.async_adapter.AsyncDBAPIModule` → `AsyncAdapt_dbapi_connection(module, vertebra.db.dbapi.AsyncDBAPIConnection(...))`.
- `create_engine(url, poolclass=BrokerPool, creator=broker_creator(url, broker=transport))` → `BrokerPool._do_get` → `broker.acquire(pool_name=config.pool_key() digest, worker_pid=os.getpid())` → `SyncDBAPIConnection(driver=leased_driver)`; `_do_return_conn` → rollback → `lease.release()` or quarantine on failure.
- `advanced_alchemy.config.SQLAlchemyAsyncConfig(engine_config=EngineConfig(poolclass=AsyncBrokerPool, ...))` → same lease path through `AsyncSHMBrokerTransport`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Row-returning query | First keyword in `ROW_RETURNING_LEADING_KEYWORDS` (moved verbatim from `_ROW_RETURNING_LEADING_KEYWORDS`: `SELECT`, `WITH`, `PRAGMA`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`, and the remaining HEAD entries), or DML with a standalone `RETURNING` token | `execute_with_description`; `rowcount == len(rows)`; `description` set | Stable classification |
| DML without RETURNING | `UPDATE t SET ...` | `execute_dml`; `rowcount == affected`; `description is None` | PEP 249 rowcount |
| Leading comments | `-- c\n/* c */ select 1` | Classified as row-returning | Comment skipping |
| `RETURNING` in a string literal or identifier | `insert ... values ('returning')` | Not row-returning | Standalone keyword only |
| lastrowid | Native driver path | `lastrowid is None`; sqlite3 fast path forwards `cursor.lastrowid` | No fabricated ids |
| Statement autocommit | Config `dialect in {"bigquery", "spanner"}` | `autocommit=False` raises `NotSupportedError` | Engine behavior kept without engine imports |
| Boundary violation | `vertebra.db.dbapi` imports `sqlalchemy` | Import-boundary test fails | Framework-neutral package |
| Pool return with failed rollback | Rollback raises in `BrokerPool._do_return_conn` | Lease quarantined; `LeaseQuarantinedError` raised | No dirty connection |
| Broker absent | `BrokerPool` built without broker | `ImproperConfigurationError` at engine construction | Fail loud |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `import vertebra.db.dbapi` in a fresh interpreter, when `sys.modules` is inspected, then no `sqlalchemy` or `vertebra.extensions` module is loaded.
- Given two engines in two processes using `BrokerPool` on one `pool_key`, when both check out connections, then the broker reports leases from one pool partition.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Package location | `vertebra.db.dbapi` | `vertebra.extensions.dbapi`: DB-API is a database seam, not a framework extension. |
| Engine checks | `type(config).dialect` ClassVar | `isinstance` on adapter configs: forces engine imports into the neutral package. |
| Re-exports | None; rewire every import site | Compatibility re-export from `dialects.py`: violates the clean-break rule. |
| Pool integration | Custom `Pool` subclass plus `creator` | `NullPool` + creator only: no checkout/return hook to rollback and quarantine. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; `docs/usage/databases.md` and `docs/usage/extensions.md` coverage lives in `ch09 / t01`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R17a | [t01](tasks/t01-dbapi-sync-and-errors.md) | `behavior_tdd` | `src/py/tests/unit/db/test_dbapi.py` |
| R17b | [t02](tasks/t02-dbapi-async-and-sqlalchemy-adapter.md) | `behavior_tdd` | `src/py/tests/unit/db/test_dbapi.py` |
| R17c | [t03](tasks/t03-sqlalchemy-dialect-rewire.md) | `regression_tdd` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| R18 | [t04](tasks/t04-sqlalchemy-broker-pool.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py` |

## 5. Implementation Plan

- [ ] broker-pool-sharing:ch07:t01 — Extract sync DB-API classes and errors into `vertebra.db.dbapi` (tasks/t01-dbapi-sync-and-errors.md)
- [ ] broker-pool-sharing:ch07:t02 — Extract async DB-API classes and the SQLAlchemy async adapter (tasks/t02-dbapi-async-and-sqlalchemy-adapter.md)
- [ ] broker-pool-sharing:ch07:t03 — Rewire SQLAlchemy dialects and import sites to the new modules (tasks/t03-sqlalchemy-dialect-rewire.md)
- [ ] broker-pool-sharing:ch07:t04 — Add `BrokerPool` for SQLAlchemy and Advanced Alchemy (tasks/t04-sqlalchemy-broker-pool.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/db/test_dbapi.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_sqlalchemy_broker_pool.py src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/db/test_native_db_drivers.py -q && uv run --no-sync mypy src/py/vertebra/db/dbapi src/py/vertebra/extensions/sqlalchemy` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 3`, `plan_commit: null`, HEAD `c8722c76`; `state: planned`.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4` completed).
- **Active Invariants**: `vertebra.db.dbapi` never imports `sqlalchemy`, `vertebra.extensions`, or `vertebra.db.adapters.*`; between `t01` and `t03` the embedded copies in `dialects.py` remain the only ones SQLAlchemy uses, and `t03` deletes them.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — Embedded PEP 249 classes.
[^sa-async-module]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::AsyncDBAPIModule` — Async module with SQLAlchemy adaptation.
[^row-returning]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::_is_row_returning_sql` — Statement-kind classifier.
[^db-config]: `src/py/vertebra/db/driver/_common.py::BaseDatabaseConfig` — Base config.
[^db-registry]: `src/py/vertebra/db/registry.py::DatabaseRegistry` — URL parsing and shared drivers.
[^broker-transport]: `src/py/vertebra/extensions/broker/transport.py::SyncSHMBrokerTransport` — SHM broker transport.
