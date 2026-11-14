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
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
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
  title: Current monolithic SQLAlchemy dialect and DB-API module
- id: sa-types
  resource: src/py/vertebra/extensions/sqlalchemy/types.py
  title: SQLAlchemy custom vector and JSON types
- id: db-common
  resource: src/py/vertebra/db/driver/_common.py
  title: BaseDatabaseConfig contract
- id: pg-driver
  resource: src/py/vertebra/db/adapters/postgres/driver.py
  title: PostgreSQL native driver
- id: mysql-driver
  resource: src/py/vertebra/db/adapters/mysql/driver.py
  title: MySQL native driver
- id: oracle-driver
  resource: src/py/vertebra/db/adapters/oracle/driver.py
  title: Oracle native driver
- id: mssql-driver
  resource: src/py/vertebra/db/adapters/mssql/driver.py
  title: MSSQL native driver
---

# Chapter 01: DBAPI 2.0 Hardening, Dialect Base, and Relational SQLAlchemy Dialects

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` currently open a second stdlib `sqlite3` connection (`_sqlite_conn`), default `url` to `sqlite+vtb:///:memory:`, and check `isinstance(config, (BigQueryConfig, SpannerConfig, SQLiteConfig))` [^sa-dialects] [^db-common].
- **Target Architecture**: Shared `vertebra.db.dbapi` (extracted in `broker-pool-sharing:ch04`) requires an explicit URL (`InterfaceError` when omitted), executes SQLite exclusively via `SyncSQLiteDriver`/`AsyncSQLiteDriver`, and reads engine-neutral `BaseDatabaseConfig.requires_statement_autocommit` and `effective_direct_dsn` [^pyproject-deps].
- **Scope**: `pyproject.toml` extras (`dogpile.cache>=1.3.3`, `django>=6.0`), `db/dbapi/{_sync,_async}.py` hardening, `extensions/sqlalchemy/{async_adapter,engine,types}.py`, and `dialects/_base.py` [^sa-types].
- **Baseline (`18baadcd`)**: Relational dialects reside inside `1,656`-line `extensions/sqlalchemy/dialects.py` alongside inline DB-API classes [^sa-dialects].
- **Target Architecture**: Per-engine modules `dialects/{postgresql,sqlite,mysql,oracle,mssql}.py` define `Sync<Engine>Dialect` and `Async<Engine>Dialect` subclassing SQLAlchemy's built-in `PGDialect`, `SQLiteDialect`, `MySQLDialect`, `OracleDialect`, and `MSDialect` over `vertebra.db.dbapi` [^pg-driver] [^mysql-driver] [^oracle-driver] [^mssql-driver].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter hardens the shared PEP 249 package (`vertebra.db.dbapi`) after `broker-pool-sharing:ch04` extracts it, eliminates concrete adapter `isinstance` checks via `BaseDatabaseConfig` metadata, isolates SQLAlchemy greenlet adaptation in `extensions/sqlalchemy/async_adapter.py`, extracts `engine.py`, and builds `_SyncDialectMixin` / `_AsyncDialectMixin` plus custom vector/JSON SQLAlchemy types [^sa-dialects] [^sa-types].

This chapter extracts the five relational SQLAlchemy dialect pairs (`PostgreSQL`, `SQLite`, `MySQL`, `Oracle`, `MSSQL`) into dedicated per-engine modules inheriting full SQL compilation, identifier quoting, and reflection from SQLAlchemy's built-in dialect bases while routing execution through `vertebra.db.dbapi` [^sa-dialects].

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`db/dbapi` Purity & URL Requirement)**: `SyncDBAPIModule.connect(url)` and `AsyncDBAPIModule.connect(url)` raise `InterfaceError` on empty/missing URLs, delete `_sqlite_conn` and stdlib `sqlite3`, and import zero SQLAlchemy, Django, framework, or engine-adapter modules.
- **R2 (Engine-Neutral Config Metadata)**: `BaseDatabaseConfig` declares `requires_statement_autocommit: ClassVar[bool] = False` and `effective_direct_dsn -> str`.
- **R3 (`_SyncDialectMixin` / `_AsyncDialectMixin` & `types.py`)**: Mixins wire `import_dbapi`, full-URL `create_connect_args`, broker-aware `get_pool_class`, and `is_disconnect`; `types.py` implements `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, `BigQueryGeography`, and `MsgspecJSON`.

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
```

- **R4 (Relational Built-In Dialect Subclasses)**: Define `SyncPostgreSQLDialect`/`AsyncPostgreSQLDialect`, `SyncSQLiteDialect`/`AsyncSQLiteDialect`, `SyncMySQLDialect`/`AsyncMySQLDialect`, `SyncOracleDialect`/`AsyncOracleDialect`, and `SyncMSSQLDialect`/`AsyncMSSQLDialect` overriding `import_dbapi`, `get_pool_class`, `create_connect_args`, and native vector/type hooks.
- **R5 (Zero `sqlite3` & Exact Wire Binds)**: `SyncSQLiteDialect` reads version metadata from the native driver; `SyncMySQLDialect` and `SyncMSSQLDialect` emit `qmark` `?` binds; `SyncOracleDialect` emits `:name` named binds.

```python
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

Public documentation and OKF updates for DB-API hardening and custom SQLAlchemy types land in `ch03-django-backends-and-docs` (`t04`).

Dialect URL schemes and matrix documentation are synchronized in `ch03-django-backends-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Harden PEP 249 DBAPI, Config Metadata, and Greenlet Async Dialect Adapter | `pyproject.toml`, `src/py/vertebra/db/dbapi/_sync.py`, `src/py/vertebra/db/dbapi/_async.py` (+3 more) | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t02` | Implement Shared SQLAlchemy Dialect Mixins, ExecutionContext, and Native Types | `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py`, `src/py/vertebra/extensions/sqlalchemy/types.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t03` | Implement Native PostgreSQL and SQLite Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| `t04` | Implement Native MySQL, Oracle, and SQL Server Sync/Async SQLAlchemy Dialects | `src/py/vertebra/extensions/sqlalchemy/dialects/mysql.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/oracle.py`, `src/py/vertebra/extensions/sqlalchemy/dialects/mssql.py` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-dbapi-hardening-and-async-adapter.md](tasks/t01-dbapi-hardening-and-async-adapter.md) — Bump SQLAlchemy/Alembic/dogpile/Advanced Alchemy extras, harden PEP 249 cursor/connection semantics, and build greenlet-bridged AsyncConnection/AsyncCursor.
- [ ] [t02-dialect-base-and-native-types.md](tasks/t02-dialect-base-and-native-types.md) — Create dialects/_base.py and types.py handling parameter style compilation, server version introspection, and UUID/JSON/Vector/Numeric type bindings.
- [ ] [t03-postgresql-and-sqlite-dialects.md](tasks/t03-postgresql-and-sqlite-dialects.md) — Build dialects/postgresql.py and dialects/sqlite.py with RETURNING, ON CONFLICT, JSONB, pgvector, and isolation levels.
- [ ] [t04-mysql-oracle-and-mssql-dialects.md](tasks/t04-mysql-oracle-and-mssql-dialects.md) — Build dialects/{mysql,oracle,mssql}.py with qmark/named bind compilation, Oracle RETURNING out-binds, and T-SQL OUTPUT reflection.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-dbapi-foundation-and-relational-dialects` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^pyproject-deps]: `pyproject.toml` — Project optional dependencies and entry points.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Current monolithic SQLAlchemy dialect and DB-API module.
[^sa-types]: `src/py/vertebra/extensions/sqlalchemy/types.py` — SQLAlchemy custom vector and JSON types.
[^db-common]: `src/py/vertebra/db/driver/_common.py` — BaseDatabaseConfig contract.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py` — PostgreSQL native driver.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py` — MySQL native driver.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py` — Oracle native driver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py` — MSSQL native driver.
