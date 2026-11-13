---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch02-sqlalchemy-core-relational-dialects
title: "Chapter 02: SQLAlchemy Relational Dialects (PostgreSQL, SQLite, MySQL, Oracle, MSSQL)"
description: "Split relational Sync<Engine>Dialect and Async<Engine>Dialect classes onto SQLAlchemy built-in dialect bases over vertebra.db.dbapi."
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
  - sqlalchemy
  - dialects
  - postgresql
  - mysql
  - oracle
  - mssql
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: "Monolithic SQLAlchemy dialect module"
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "PostgreSQL native driver"
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py
    title: "MySQL native driver"
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Oracle native driver"
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py
    title: "MSSQL native driver"
---

# Chapter 02: SQLAlchemy Relational Dialects (PostgreSQL, SQLite, MySQL, Oracle, MSSQL)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Relational dialects reside inside `1,656`-line `extensions/sqlalchemy/dialects.py` alongside inline DB-API classes [^sa-dialects].
- **Target Architecture**: Per-engine modules `dialects/{postgresql,sqlite,mysql,oracle,mssql}.py` define `Sync<Engine>Dialect` and `Async<Engine>Dialect` subclassing SQLAlchemy's built-in `PGDialect`, `SQLiteDialect`, `MySQLDialect`, `OracleDialect`, and `MSDialect` over `vertebra.db.dbapi` [^pg-driver] [^mysql-driver] [^oracle-driver] [^mssql-driver].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter extracts the five relational SQLAlchemy dialect pairs (`PostgreSQL`, `SQLite`, `MySQL`, `Oracle`, `MSSQL`) into dedicated per-engine modules inheriting full SQL compilation, identifier quoting, and reflection from SQLAlchemy's built-in dialect bases while routing execution through `vertebra.db.dbapi` [^sa-dialects].

## 2. Requirements, Concrete Contracts & Architecture

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

Dialect URL schemes and matrix documentation are synchronized in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R4`, `R5` PostgreSQL & SQLite | `t01` | `dialects/{postgresql,sqlite}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R4`, `R5` MySQL | `t02` | `dialects/mysql.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |
| `R4`, `R5` Oracle & MSSQL | `t03` | `dialects/{oracle,mssql}.py` | `pytest test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] [t01-postgresql-and-sqlite-dialects.md](tasks/t01-postgresql-and-sqlite-dialects.md) — Implement `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` and `SyncSQLiteDialect` / `AsyncSQLiteDialect`.
- [ ] [t02-mysql-dialect.md](tasks/t02-mysql-dialect.md) — Implement `SyncMySQLDialect` and `AsyncMySQLDialect` with `qmark` binary prepared binds.
- [ ] [t03-oracle-and-mssql-dialects.md](tasks/t03-oracle-and-mssql-dialects.md) — Implement `SyncOracleDialect` / `AsyncOracleDialect` and `SyncMSSQLDialect` / `AsyncMSSQLDialect`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-sqlalchemy-core-relational-dialects` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Monolithic SQLAlchemy dialect module.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/driver.py` — PostgreSQL native driver.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py` — MySQL native driver.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py` — Oracle native driver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py` — MSSQL native driver.
