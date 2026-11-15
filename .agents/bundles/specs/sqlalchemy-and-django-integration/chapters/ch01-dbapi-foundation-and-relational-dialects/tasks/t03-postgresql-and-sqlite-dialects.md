---
type: Task
id: sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t03
flow_id: sqlalchemy-and-django-integration
chapter_id: ch01-dbapi-foundation-and-relational-dialects
title: Implement Native PostgreSQL and SQLite Sync/Async SQLAlchemy Dialects
description: Build dialects/postgresql.py and dialects/sqlite.py with RETURNING, ON CONFLICT, JSONB, pgvector, and isolation levels.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch01-dbapi-foundation-and-relational-dialects:t02
files:
  - src/py/vertebra/extensions/sqlalchemy/dialects/postgresql.py
  - src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
tests:
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
read_set:
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/sqlite/driver.py
estimated_loc: 220
tags:
  - sqlalchemy
  - postgresql
  - sqlite
  - dialects
sources:
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: Existing PostgreSQL and SQLite dialect definitions
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/_sync.py
    title: Native PostgreSQL sync and async drivers
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: SQLite driver exposing engine version metadata
verification_strategy: regression_tdd
---

# Task `t03`: Implement Native PostgreSQL and SQLite Sync/Async SQLAlchemy Dialects

## Objective & Scope Boundary

Implement `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` (`PGDialect`) and `SyncSQLiteDialect` / `AsyncSQLiteDialect` (`SQLiteDialect`) per [`../spec.md`](../spec.md) [^sa-dialects] [^pg-driver] [^sqlite-driver]. Out of scope: MySQL, Oracle, and MSSQL dialects (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — PostgreSQL and SQLite dialect overrides to extract (plus `_SyncDialectMixin` / `_AsyncDialectMixin` from `t02`).
- `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — native `$1` parameter, `RETURNING`, and isolation level semantics.
- `src/py/vertebra/db/adapters/sqlite/driver.py` — native SQLite version metadata and WAL pragma hooks.

## Contracts & Edge Cases

- `SyncPostgreSQLDialect(_SyncDialectMixin, PGDialect)` and `AsyncPostgreSQLDialect(_AsyncDialectMixin, PGDialect)` register `JSONB`, `UUID`, `ARRAY`, and `PgVector` in `ischema_names`, support `INSERT ... RETURNING`, and handle `SERIALIZABLE` / `REPEATABLE READ` / `READ COMMITTED` / `AUTOCOMMIT` isolation levels.
- `SyncSQLiteDialect(_SyncDialectMixin, SQLiteDialect)` and `AsyncSQLiteDialect(_AsyncDialectMixin, SQLiteDialect)` read `server_version_info` from the native SQLite driver (never importing stdlib `sqlite3`) and support savepoints and WAL `on_connect` hooks.

## Test Matrix

- Parametrized compilation, reflection (`ischema_names`), isolation level, and `:memory:` round-trip tests for `SyncPostgreSQLDialect`, `AsyncPostgreSQLDialect`, `SyncSQLiteDialect`, and `AsyncSQLiteDialect`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy/dialects && uv run --no-sync pytest src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
```

## Acceptance Criteria

- [ ] `dialects/postgresql.py` and `dialects/sqlite.py` subclass SQLAlchemy built-ins cleanly with leading `Sync*`/`Async*` class names.
- [ ] Zero stdlib `sqlite3` imports exist in `dialects/sqlite.py`.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Existing PostgreSQL and SQLite dialect definitions.
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — Native PostgreSQL sync and async drivers.
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/driver.py` — SQLite driver exposing engine version metadata.
