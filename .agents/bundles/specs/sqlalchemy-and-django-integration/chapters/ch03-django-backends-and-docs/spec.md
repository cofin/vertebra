---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: 'Chapter 03: Django 5.1–6.0 Database Backends, Unsupported Dialect Cleanup, and Docs/OKF'
description: Upgrade Django database backends for PostgreSQL, SQLite3, MySQL, Oracle, and Cloud Spanner, remove unsupported MSSQL/DuckDB/BigQuery Django stubs, and synchronize public docs and OKF chapters.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- django
- postgresql
- sqlite
- mysql
- oracle
- spanner
- debloat
- documentation
sources:
- id: dj-backends
  resource: src/py/vertebra/extensions/django/db/backends/__init__.py
  title: Current generic Django DatabaseWrapperMixin with vendor config dispatch
- id: dj-pg
  resource: src/py/vertebra/extensions/django/db/backends/postgresql/base.py
  title: Django PostgreSQL backend
- id: dj-sqlite
  resource: src/py/vertebra/extensions/django/db/backends/sqlite3/base.py
  title: Django SQLite3 backend
- id: dj-mysql
  resource: src/py/vertebra/extensions/django/db/backends/mysql/base.py
  title: Django MySQL backend
- id: dj-oracle
  resource: src/py/vertebra/extensions/django/db/backends/oracle/base.py
  title: Django Oracle backend
- id: dj-spanner
  resource: src/py/vertebra/extensions/django/db/backends/spanner/base.py
  title: Django Spanner backend
- id: dj-bq
  resource: src/py/vertebra/extensions/django/db/backends/bigquery/base.py
  title: Unsupported BigQuery Django backend
- id: dj-plugin
  resource: src/py/vertebra/extensions/django/plugin.py
  title: Django VertebraPlugin
- id: doc-ext
  resource: docs/usage/extensions.md
  title: Extensions usage documentation
- id: okf-dialects
  resource: .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  title: SQLSpec and SQLAlchemy dialects OKF chapter
- id: okf-litestar
  resource: .agents/bundles/knowledge/domains/extensions/litestar.md
  title: Litestar extension OKF chapter
- id: okf-limits
  resource: .agents/bundles/knowledge/domains/data/database-driver-limitations.md
  title: Database and dialect limitations OKF chapter
---

# Chapter 03: Django 5.1–6.0 Database Backends, Unsupported Dialect Cleanup, and Docs/OKF

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/django/db/backends/{postgresql,sqlite3,mysql,oracle}/base.py` wrap a generic `DatabaseWrapperMixin._build_native_config` dispatcher rather than subclassing Django's built-in `DatabaseWrapper` classes directly over `vertebra.db.dbapi` [^dj-backends].
- **Target Architecture**: Each `<vendor>/base.py::DatabaseWrapper` subclasses Django `6.0+`'s built-in backend (`django.db.backends.<vendor>.base.DatabaseWrapper`), constructs its owning `<Engine>Config` locally, and swaps `Database` for `vertebra.db.dbapi.SyncDBAPIModule` [^dj-pg] [^dj-sqlite] [^dj-mysql] [^dj-oracle].
- **Baseline (`18baadcd`)**: `extensions/django/db/backends/` ships hollow stub packages for `spanner`, `bigquery`, `duckdb`, and `mssql` alongside vendor-dispatching `DatabaseWrapperMixin._build_native_config` [^dj-spanner] [^dj-bq] [^dj-backends].
- **Target Architecture**: `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`; `bigquery`, `duckdb`, and `mssql` Django backends are deleted; `django/plugin.py` raises `ImproperConfigurationError` for unsupported engines [^dj-plugin].
- **Baseline (`18baadcd`)**: Public docs and OKF chapters still reference monolithic `extensions/sqlalchemy/{dialects,alembic,cache}.py`, `shm://` cache URLs, and unsupported `bigquery`/`duckdb`/`mssql` Django DB backends [^doc-ext] [^okf-dialects].
- **Target Architecture**: Present-tense `docs/` and `<= 200`-line OKF chapters document the 36 split `Sync<Engine>Dialect`/`Async<Engine>Dialect` entry points, `dogpile.cache` regions, 5 Django `6.0+` backends, and explicit limitations [^okf-litestar] [^okf-limits].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter rebases the four relational Django `6.0+` database backends (`postgresql`, `sqlite3`, `mysql`, `oracle`) onto Django's built-in `DatabaseWrapper` classes backed by `vertebra.db.dbapi` (`SyncDBAPIModule` / `SyncDBAPIConnection`) with zero SQLAlchemy imports [^dj-backends].

This chapter wires the official Google Cloud Spanner Django backend (`django_spanner`) onto Vertebra's Rust-backed `SyncDBAPIModule` and removes the unsupported `bigquery`, `duckdb`, and `mssql` Django backend stubs [^dj-spanner] [^dj-bq].

This chapter synchronizes user-facing guides, API references, and OKF v0.2 knowledge chapters with the completed SQLAlchemy, Advanced Alchemy `1.12.0+`, Alembic, and Django `6.0+` architecture [^doc-ext] [^okf-dialects].

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

- **R11 (Django Spanner Backend)**: `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (guarded via `ensure_module`) and swaps `google.cloud.spanner_dbapi` for `vertebra.db.dbapi.SyncDBAPIModule`.
- **R12 (Unsupported Backend Removal)**: Delete `extensions/django/db/backends/{bigquery,duckdb,mssql}/`, remove `_build_native_config` vendor dispatch from `backends/__init__.py`, and fail loud with `ImproperConfigurationError`.

```python
class DatabaseWrapper(DjangoSpannerDatabaseWrapper):
    vendor = "spanner"
    Database = SyncDBAPIModule

    def get_connection_params(self) -> dict[str, Any]: ...
    def get_new_connection(self, conn_params: Mapping[str, Any]) -> SyncDBAPIConnection: ...

SUPPORTED_DJANGO_BACKENDS: Final[frozenset[str]] = frozenset(
    {"postgresql", "mysql", "oracle", "sqlite3", "spanner"}
)
```

- **R13 (Docs & OKF Synchronization)**: Update `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`.

```text
Entry-Point Matrix (36 entries across 9 engines):
  <engine>.vertebra       -> vertebra.extensions.sqlalchemy.dialects.<mod>:Sync<Engine>Dialect
  <engine>.vtb            -> vertebra.extensions.sqlalchemy.dialects.<mod>:Sync<Engine>Dialect
  <engine>.vertebra_async -> vertebra.extensions.sqlalchemy.dialects.<mod>:Async<Engine>Dialect
  <engine>.vtb_async      -> vertebra.extensions.sqlalchemy.dialects.<mod>:Async<Engine>Dialect

Django 6.0+ Supported ENGINE Paths (5 backends):
  vertebra.extensions.django.db.backends.{postgresql,mysql,oracle,sqlite3,spanner}
```

## 3. Documentation & OKF Knowledge Plan

Django `DATABASES` configuration guides are updated in `ch03-django-backends-and-docs` (`t04`).

Supported Django `ENGINE` paths and BigQuery/DuckDB/MSSQL exclusions are documented in `ch03-django-backends-and-docs` (`t04`).

Updates `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/reference/extensions.md`, [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [litestar.md](../../../../knowledge/domains/extensions/litestar.md), and [database-driver-limitations.md](../../../../knowledge/domains/data/database-driver-limitations.md), keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Upgrade Django PostgreSQL and SQLite3 Backends Over Native Drivers | `src/py/vertebra/extensions/django/db/backends/postgresql/base.py`, `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` | `src/py/tests/unit/extensions/test_django_plugin.py` |
| `t02` | Upgrade Django MySQL and Oracle Backends Over Native Drivers | `src/py/vertebra/extensions/django/db/backends/mysql/base.py`, `src/py/vertebra/extensions/django/db/backends/oracle/base.py` | `src/py/tests/unit/extensions/test_django_plugin.py` |
| `t03` | Implement Django Cloud Spanner Backend and Remove Unsupported MSSQL/DuckDB/BigQuery Stubs | `src/py/vertebra/extensions/django/db/backends/spanner/base.py`, `src/py/vertebra/extensions/django/db/backends/spanner/__init__.py`, `src/py/vertebra/extensions/django/db/backends/__init__.py` (+1 more) | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t04` | Synchronize SQLAlchemy, Alembic, Advanced Alchemy, and Django Docs and OKF Chapters | `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/reference/extensions.md` (+3 more) | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py`, `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |

## 5. Implementation Plan

- [ ] [t01-django-postgresql-and-sqlite3.md](tasks/t01-django-postgresql-and-sqlite3.md) — Implement full DatabaseWrapper, DatabaseFeatures, DatabaseOperations, and DatabaseIntrospection for PostgreSQL and SQLite3.
- [ ] [t02-django-mysql-and-oracle.md](tasks/t02-django-mysql-and-oracle.md) — Implement full Django MySQL and Oracle backends including RETURNING INTO VariableWrapper out-binds and schema introspection.
- [ ] [t03-django-spanner-and-unsupported-cleanup.md](tasks/t03-django-spanner-and-unsupported-cleanup.md) — Upgrade the Django Spanner backend (bit-reversed sequences, INTERLEAVE IN PARENT, schema editor) and delete unsupported mssql/duckdb/bigquery Django backend packages.
- [ ] [t04-sqlalchemy-django-docs-and-okf.md](tasks/t04-sqlalchemy-django-docs-and-okf.md) — Update docs/usage/extensions.md and docs/reference/extensions.md and sync sqlspec-alchemy-and-dialects.md, django.md, and database-driver-limitations.md.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-django-backends-and-docs` (`t04`) is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py
```

[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Current generic Django DatabaseWrapperMixin with vendor config dispatch.
[^dj-pg]: `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` — Django PostgreSQL backend.
[^dj-sqlite]: `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` — Django SQLite3 backend.
[^dj-mysql]: `src/py/vertebra/extensions/django/db/backends/mysql/base.py` — Django MySQL backend.
[^dj-oracle]: `src/py/vertebra/extensions/django/db/backends/oracle/base.py` — Django Oracle backend.
[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Django Spanner backend.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django backend.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin.
[^doc-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec and SQLAlchemy dialects OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar extension OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database and dialect limitations OKF chapter.
