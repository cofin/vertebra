---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch05-django-relational-backends
title: "Chapter 05: Native Django 6.0+ Relational Database Backends"
description: "Rebase Django postgresql, sqlite3, mysql, and oracle DatabaseWrapper backends onto Django built-ins over vertebra.db.dbapi."
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
  - django
  - postgresql
  - sqlite
  - mysql
  - oracle
sources:
  - id: dj-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: "Current generic Django DatabaseWrapperMixin with vendor config dispatch"
  - id: dj-pg
    resource: src/py/vertebra/extensions/django/db/backends/postgresql/base.py
    title: "Django PostgreSQL backend"
  - id: dj-sqlite
    resource: src/py/vertebra/extensions/django/db/backends/sqlite3/base.py
    title: "Django SQLite3 backend"
  - id: dj-mysql
    resource: src/py/vertebra/extensions/django/db/backends/mysql/base.py
    title: "Django MySQL backend"
  - id: dj-oracle
    resource: src/py/vertebra/extensions/django/db/backends/oracle/base.py
    title: "Django Oracle backend"
---

# Chapter 05: Native Django 6.0+ Relational Database Backends

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/django/db/backends/{postgresql,sqlite3,mysql,oracle}/base.py` wrap a generic `DatabaseWrapperMixin._build_native_config` dispatcher rather than subclassing Django's built-in `DatabaseWrapper` classes directly over `vertebra.db.dbapi` [^dj-backends].
- **Target Architecture**: Each `<vendor>/base.py::DatabaseWrapper` subclasses Django `6.0+`'s built-in backend (`django.db.backends.<vendor>.base.DatabaseWrapper`), constructs its owning `<Engine>Config` locally, and swaps `Database` for `vertebra.db.dbapi.SyncDBAPIModule` [^dj-pg] [^dj-sqlite] [^dj-mysql] [^dj-oracle].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter rebases the four relational Django `6.0+` database backends (`postgresql`, `sqlite3`, `mysql`, `oracle`) onto Django's built-in `DatabaseWrapper` classes backed by `vertebra.db.dbapi` (`SyncDBAPIModule` / `SyncDBAPIConnection`) with zero SQLAlchemy imports [^dj-backends].

## 2. Requirements, Concrete Contracts & Architecture

- **R10 (Django Built-In Relational Subclasses)**: `postgresql`, `sqlite3`, `mysql`, and `oracle` `DatabaseWrapper` classes subclass their corresponding `django.db.backends.<vendor>.base.DatabaseWrapper`, own their settings-to-`<Engine>Config` mapping locally, and execute through `vertebra.db.dbapi`.

```python
class DatabaseWrapper(DjangoPostgreSQLDatabaseWrapper):
    Database = SyncDBAPIModule

    def get_connection_params(self) -> dict[str, Any]: ...
    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection: ...
    def create_cursor(self, name: str | None = None) -> SyncDBAPICursor: ...
    def _set_autocommit(self, autocommit: bool) -> None: ...
    def is_usable(self) -> bool: ...
```

## 3. Documentation & OKF Knowledge Plan

Django `DATABASES` configuration guides are updated in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` Django `postgresql` & `sqlite3` | `t01` | `extensions/django/db/backends/{postgresql,sqlite3}/base.py` | `pytest test_django_plugin.py` |
| `R10` Django `mysql` & `oracle` | `t02` | `extensions/django/db/backends/{mysql,oracle}/base.py` | `pytest test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-django-postgresql-and-sqlite3.md](tasks/t01-django-postgresql-and-sqlite3.md) — Rebase Django `postgresql` and `sqlite3` `DatabaseWrapper` backends onto Django `6.0+` built-ins over `vertebra.db.dbapi`.
- [ ] [t02-django-mysql-and-oracle.md](tasks/t02-django-mysql-and-oracle.md) — Rebase Django `mysql` and `oracle` `DatabaseWrapper` backends onto Django `6.0+` built-ins over `vertebra.db.dbapi`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch05-django-relational-backends` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django/db && uv run --no-sync mypy src/py/vertebra/extensions/django/db && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Current generic Django DatabaseWrapperMixin with vendor config dispatch.
[^dj-pg]: `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` — Django PostgreSQL backend.
[^dj-sqlite]: `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` — Django SQLite3 backend.
[^dj-mysql]: `src/py/vertebra/extensions/django/db/backends/mysql/base.py` — Django MySQL backend.
[^dj-oracle]: `src/py/vertebra/extensions/django/db/backends/oracle/base.py` — Django Oracle backend.
