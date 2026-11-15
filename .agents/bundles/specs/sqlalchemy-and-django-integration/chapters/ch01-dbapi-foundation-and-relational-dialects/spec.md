---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: 'Chapter 01: DBAPI 2.0 Hardening, Dialect Base, and Relational SQLAlchemy Dialects'
description: Harden PEP 249 DBAPI and async greenlet adapters, build shared dialect base and native column types, and implement PostgreSQL, SQLite, MySQL, Oracle, and SQL Server SQLAlchemy 2.x dialects.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:18:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-14T23:18:00Z'
    commit: 1b191388
tags:
  - sqlalchemy
  - dbapi
  - dialects
  - postgresql
  - mysql
  - oracle
  - mssql
sources:
  - id: pyproject-deps
    resource: pyproject.toml
    title: Project optional dependencies and entry points
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: Current monolithic SQLAlchemy dialect module importing vertebra.db.dbapi
  - id: sa-types
    resource: src/py/vertebra/extensions/sqlalchemy/types.py
    title: SQLAlchemy custom vector and JSON types
  - id: db-common
    resource: src/py/vertebra/db/driver/_common.py
    title: BaseDatabaseConfig contract
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/_sync.py
    title: PostgreSQL native driver
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/_sync.py
    title: MySQL native driver
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/_sync.py
    title: Oracle native driver
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/_sync.py
    title: MSSQL native driver
current_task: "sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t01"
---

# Chapter 01: DBAPI 2.0 Hardening, Dialect Base, and Relational SQLAlchemy Dialects

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: Standalone `SyncDBAPIConnection` / `AsyncDBAPIConnection` live in `db/dbapi/{_sync,_async}.py` (extracted with `extensions/sqlalchemy/{async_adapter,pool}.py` in `broker-pool-sharing:ch04`, complete and archived at `246d7d0e`), while empty URLs still default to `sqlite+vtb:///:memory:` (`sqlite+vtb_async:///:memory:` on async), sync SQLite paths open a second stdlib `sqlite3` connection (`_sqlite_conn`), native `vertebra._vertebra` exceptions require PEP 249 `dbapi.*Error` mapping, `db/dbapi/_sync.py` selects autocommit-only engines through the hardcoded `_AUTOCOMMIT_ONLY_DIALECTS` name set, and all 18 relational and cloud SQLAlchemy dialect classes remain in `extensions/sqlalchemy/dialects.py` (five relational pairs already subclass SQLAlchemy built-ins; MySQL uses `paramstyle = "format"`) [^sa-dialects] [^db-common].
- **Target Architecture**: Hardened `vertebra.db.dbapi` (extracted in `broker-pool-sharing:ch04`, complete and archived at `246d7d0e`) requires an explicit URL (`InterfaceError` when omitted), maps native `vertebra._vertebra` exceptions to PEP 249 `dbapi.*Error`, runs SQLite exclusively through `SyncSQLiteDriver`/`AsyncSQLiteDriver`, reads `BaseDatabaseConfig.requires_statement_autocommit` / `effective_direct_dsn`, isolates `extensions/sqlalchemy/{async_adapter,engine}.py` + `types.py` (`PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, `MsgspecJSON`), and splits `dialects/{_base,postgresql,sqlite,mysql,oracle,mssql}.py` subclassing SQLAlchemy built-ins [^pyproject-deps] [^sa-types] [^pg-driver] [^mysql-driver] [^oracle-driver] [^mssql-driver].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Building on standalone `vertebra.db.dbapi`, `extensions/sqlalchemy/async_adapter.py`, and `extensions/sqlalchemy/pool.py` extracted in `broker-pool-sharing:ch04` (complete and archived at `246d7d0e`), this chapter hardens PEP 249 `SyncDBAPI*`/`AsyncDBAPI*` semantics (removing `_sqlite_conn`, stdlib `sqlite3`, and empty-URL defaults, and mapping native `vertebra._vertebra` exceptions to PEP 249 `dbapi.*Error`), adds engine-neutral metadata on `BaseDatabaseConfig`, extracts `extensions/sqlalchemy/engine.py` (`SyncEngineAdapter`, `attach_serverless_disconnect_guard`), completes native vector/JSON types in `types.py`, builds `_SyncDialectMixin`/`_AsyncDialectMixin` in `dialects/_base.py`, and implements all five relational SQLAlchemy dialect pairs (`PostgreSQL`, `SQLite`, `MySQL`, `Oracle`, `MSSQL`) inheriting compilation, quoting, and reflection from SQLAlchemy built-ins [^sa-dialects] [^sa-types].

**Non-Goals:** Analytical/cloud dialects (`DuckDB`, `BigQuery`, `Spanner`, `Spangres`), monolithic `dialects.py` deletion, Alembic split, and `dogpile.cache` backends (`ch02`); Django `DatabaseWrapper` backends (`ch03`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`db/dbapi` Purity, Error Mapping & URL Requirement)**: `SyncDBAPIModule.connect(url)` and `AsyncDBAPIModule.connect(url)` raise `InterfaceError` on empty/missing URLs, delete `_sqlite_conn`, stdlib `sqlite3`, and `database=`, map native `vertebra._vertebra` exceptions to PEP 249 `dbapi.*Error` subclasses, and forbid SQLAlchemy, Django, framework, or engine-adapter imports inside `db/dbapi/` (`t01`).
- **R2 (Engine-Neutral Config Metadata & Helpers)**: Declare `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str` on `BaseDatabaseConfig`; harden `extensions/sqlalchemy/async_adapter.py`; extract `SyncEngineAdapter` and `attach_serverless_disconnect_guard` into `extensions/sqlalchemy/engine.py`; declare an optional `dogpile = ["dogpile.cache>=1.3.3"]` extra (never inside `alchemy`), `django>=6.0`, and optional dialect extras in `pyproject.toml` (`t01`).
- **R3 (`_SyncDialectMixin` / `_AsyncDialectMixin` & `types.py`)**: Wire `import_dbapi`, full-URL `create_connect_args`, broker-aware `get_pool_class` (`BrokerPool` / `AsyncBrokerPool`), and a new `is_disconnect` in `dialects/_base.py` (renaming the existing `_NativeTransactionDialect` mixin); replace `Vector`/`BinaryJSON` with `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON` (routing via `encode_json` / `decode_json`) in `types.py` (`t02`).
- **R4 / R5 (Relational Built-In Dialect Subclasses)**: Move the existing `SyncPostgreSQLDialect`/`AsyncPostgreSQLDialect`, `SyncSQLiteDialect`/`AsyncSQLiteDialect`, `SyncMySQLDialect`/`AsyncMySQLDialect`, `SyncOracleDialect`/`AsyncOracleDialect`, and `SyncMSSQLDialect`/`AsyncMSSQLDialect` into per-engine modules with leading `Sync*`/`Async*` names, zero stdlib `sqlite3`, native `?`/`qmark` binds (MySQL moves from `format`; MSSQL), and `:name` binds + `RETURNING INTO` (Oracle) (`t03`–`t04`).

```python
class BaseDatabaseConfig(msgspec.Struct, frozen=True):
    requires_statement_autocommit: ClassVar[bool] = False

    @property
    def effective_direct_dsn(self) -> str: ...

class SyncDBAPIModule:
    paramstyle: ClassVar[str] = "qmark"
    threadsafety: ClassVar[int] = 2
    apilevel: ClassVar[str] = "2.0"

    @staticmethod
    def connect(url: str, *, autocommit: bool | None = None, **kwargs: Any) -> SyncDBAPIConnection: ...

class AsyncDBAPIModule:
    @staticmethod
    async def connect(url: str, *, autocommit: bool | None = None, **kwargs: Any) -> AsyncDBAPIConnection: ...

class SyncPostgreSQLDialect(_SyncDialectMixin, PGDialect):
    driver = "vertebra"
    supports_statement_cache = True

class AsyncPostgreSQLDialect(_AsyncDialectMixin, PGDialect):
    driver = "vertebra_async"
    is_async = True
    supports_statement_cache = True

class SyncSQLiteDialect(_SyncDialectMixin, BaseSQLiteDialect): ...
class AsyncSQLiteDialect(_AsyncDialectMixin, BaseSQLiteDialect): ...
class SyncMySQLDialect(_SyncDialectMixin, BaseMySQLDialect):
    paramstyle = "qmark"
class AsyncMySQLDialect(_AsyncDialectMixin, BaseMySQLDialect):
    paramstyle = "qmark"
class SyncOracleDialect(_SyncDialectMixin, BaseOracleDialect): ...
class AsyncOracleDialect(_AsyncDialectMixin, BaseOracleDialect): ...
class SyncMSSQLDialect(_SyncDialectMixin, MSDialect): ...
class AsyncMSSQLDialect(_AsyncDialectMixin, MSDialect): ...
```

## 3. Documentation & OKF Knowledge Plan

Public documentation, URL scheme tables, and OKF updates for DB-API hardening, custom SQLAlchemy types, and relational dialects execute in `ch03-django-backends-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter | `pyproject.toml`, `src/py/vertebra/db/dbapi/{_sync,_async}.py`, `src/py/vertebra/db/driver/_common.py`, `src/py/vertebra/extensions/sqlalchemy/{async_adapter,engine}.py` | `src/py/tests/unit/db/test_dbapi_standalone.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t02` | Implement Shared SQLAlchemy Dialect Mixins, ExecutionContext, and Native Types | `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py`, `src/py/vertebra/extensions/sqlalchemy/types.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Implement Native PostgreSQL and SQLite Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t04` | Implement Native MySQL, Oracle, and SQL Server Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/{mysql,oracle,mssql}.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-dbapi-hardening-and-async-adapter.md](tasks/t01-dbapi-hardening-and-async-adapter.md) — Update `pyproject.toml` extras, harden PEP 249 `db/dbapi`, add `BaseDatabaseConfig` metadata, and wire `async_adapter.py` + `engine.py`.
- [ ] [t02-dialect-base-and-native-types.md](tasks/t02-dialect-base-and-native-types.md) — Create `dialects/_base.py` and `types.py` handling parameter styles, broker pool wiring, and Vector/Geography/JSON type bindings.
- [ ] [t03-postgresql-and-sqlite-dialects.md](tasks/t03-postgresql-and-sqlite-dialects.md) — Build `dialects/postgresql.py` and `dialects/sqlite.py` with `RETURNING`, `ON CONFLICT`, `JSONB`, `PgVector`, WAL hooks, and isolation levels.
- [ ] [t04-mysql-oracle-and-mssql-dialects.md](tasks/t04-mysql-oracle-and-mssql-dialects.md) — Build `dialects/{mysql,oracle,mssql}.py` with `qmark`/named bind compilation, Oracle `RETURNING INTO` out-binds, and T-SQL `OUTPUT` reflection.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-dbapi-foundation-and-relational-dialects` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver` (`broker-pool-sharing` complete and archived at `246d7d0e`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/db/dbapi src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/dbapi src/py/vertebra/db/driver src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/db/test_dbapi_standalone.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_optional_driver_wheels.py -q
```

[^pyproject-deps]: `pyproject.toml` — Project optional dependencies and entry points.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current monolithic SQLAlchemy dialect module importing `vertebra.db.dbapi`.
[^sa-types]: `src/py/vertebra/extensions/sqlalchemy/types.py` — SQLAlchemy custom vector and JSON types.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig contract.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — PostgreSQL native driver.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/_sync.py` and `src/py/vertebra/db/adapters/mysql/_async.py` — MySQL native driver.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — Oracle native driver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/_sync.py` and `src/py/vertebra/db/adapters/mssql/_async.py` — MSSQL native driver.
