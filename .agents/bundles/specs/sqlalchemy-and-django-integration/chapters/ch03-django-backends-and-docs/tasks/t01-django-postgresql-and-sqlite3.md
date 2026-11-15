---
type: Task
id: sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t01
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: Upgrade Django PostgreSQL and SQLite3 Backends Over Native Drivers
description: Implement full DatabaseWrapper, DatabaseFeatures, DatabaseOperations, and DatabaseIntrospection for PostgreSQL and SQLite3.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch02-cloud-dialects-alembic-and-advanced-alchemy:t04
files:
  - src/py/vertebra/extensions/django/db/backends/postgresql/base.py
  - src/py/vertebra/extensions/django/db/backends/sqlite3/base.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/django/db/backends/postgresql/base.py
  - src/py/vertebra/extensions/django/db/backends/sqlite3/base.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/sqlite/config.py
estimated_loc: 225
tags:
  - django
  - postgresql
  - sqlite
  - dbapi
sources:
  - id: dj-pg
    resource: src/py/vertebra/extensions/django/db/backends/postgresql/base.py
    title: Current Django PostgreSQL backend wrapper
  - id: dj-sqlite
    resource: src/py/vertebra/extensions/django/db/backends/sqlite3/base.py
    title: Current Django SQLite3 backend wrapper
verification_strategy: regression_tdd
---

# Task `t01`: Upgrade Django PostgreSQL and SQLite3 Backends Over Native Drivers

## Objective & Scope Boundary

Rebase `DatabaseWrapper` in `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` and `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` onto Django `6.0+` built-in backends over `vertebra.db.dbapi` per [`../spec.md`](../spec.md) [^dj-pg] [^dj-sqlite].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/__init__.py` — current generic `DatabaseWrapperMixin`.
- `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` & `sqlite3/base.py` — existing wrappers.
- `src/py/vertebra/db/adapters/postgres/config.py` & `sqlite/config.py` — native engine config constructors.

## Contracts & Edge Cases

- `src/py/vertebra/extensions/django/db/backends/postgresql/base.py::DatabaseWrapper` subclasses `django.db.backends.postgresql.base.DatabaseWrapper`, sets `Database = SyncDBAPIModule`, builds `PostgreSQLConfig` locally in `get_connection_params()`, opens `SyncDBAPIConnection` (with optional broker lease pool), adapts `%s` binds cleanly, and implements `_set_autocommit` and `is_usable`.
- `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py::DatabaseWrapper` subclasses `django.db.backends.sqlite3.base.DatabaseWrapper`, builds `SQLiteConfig` locally, executes over native `SyncSQLiteDriver` via `SyncDBAPIConnection` (zero stdlib `sqlite3` fallback), and supports deterministic function/collation registration.
- Importing Django backends must never import `sqlalchemy` or `vertebra.extensions.sqlalchemy`.

## Test Matrix

- Parametrized `get_connection_params`, `get_new_connection`, `create_cursor`, `_set_autocommit`, `is_usable`, and SQLite `:memory:` `migrate` / ORM CRUD tests, plus `sys.modules` check proving SQLAlchemy is not imported.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django/db && uv run --no-sync mypy src/py/vertebra/extensions/django/db && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `postgresql` and `sqlite3` Django backends subclass Django `6.0+` built-ins over `vertebra.db.dbapi` with zero SQLAlchemy imports.
- [ ] Zero regressions in the fast verification gate.

[^dj-pg]: `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` — Current Django PostgreSQL backend wrapper.
[^dj-sqlite]: `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` — Current Django SQLite3 backend wrapper.
