---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: 'Chapter 03: Django 6.0+ Database & Cache Backends, Unsupported Dialect Cleanup, and Docs/OKF'
description: Upgrade Django database backends for PostgreSQL, SQLite3, MySQL, Oracle, and Spanner, upgrade Django CacheBackend(BaseCache) onto SyncCacheBackend and AsyncCacheBackend, remove unsupported MSSQL/DuckDB/BigQuery Django stubs, and synchronize public docs and OKF chapters.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
depends_on_chapters:
  - ch02-cloud-dialects-alembic-and-advanced-alchemy
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
tags:
  - django
  - cache
  - shm
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
  - id: dj-cache
    resource: src/py/vertebra/extensions/django/cache.py::CacheBackend
    title: Django BaseCache backend
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
current_task: "sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t01"
---

# Chapter 03: Django 6.0+ Database & Cache Backends, Unsupported Dialect Cleanup, and Docs/OKF

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `extensions/django/db/backends/{postgresql,sqlite3,mysql,oracle}/base.py` wrap a generic `DatabaseWrapperMixin._build_native_config` dispatcher alongside hollow `spanner`, `bigquery`, `duckdb`, and `mssql` stub packages. `extensions/django/cache.py::CacheBackend` is a base-less in-memory `dict` stub that ignores `LOCATION`, `django/plugin.py` performs no `DATABASES`/`CACHES` validation, and public docs/OKF chapters still reference monolithic `extensions/sqlalchemy/{dialects,alembic}.py` and unsupported `bigquery`/`duckdb`/`mssql` Django DB backends [^dj-backends] [^dj-spanner] [^dj-bq] [^dj-cache] [^doc-ext] [^okf-dialects].
- **Target Architecture**: `{postgresql,sqlite3,mysql,oracle}/base.py::DatabaseWrapper` subclass Django `6.0+` built-in backends and `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi.SyncDBAPIModule`; `extensions/django/cache.py::CacheBackend(BaseCache)` delegates sync methods to `SyncCacheBackend` and `*a`-prefixed async methods (`aget`, `aset`, `aadd`, `adelete`, `aget_many`, `aset_many`, `adelete_many`, `aincr`, `adecr`, `atouch`, `ahas_key`, `aclear`, `aclose`) directly to `AsyncCacheBackend` on the event loop across `shm://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`, and `memory://`; `bigquery`/`duckdb`/`mssql` Django stubs are deleted and `django/plugin.py` raises `ImproperConfigurationError`; present-tense `docs/` and `<= 200`-line OKF chapters document the 36 split `Sync<Engine>Dialect`/`Async<Engine>Dialect` entry points, native `SyncCacheBackend`/`AsyncCacheBackend` + `ShmCache` + `dogpile.cache` plugins, 5 Django `6.0+` DB backends, Django `CacheBackend`, and explicit limitations [^dj-pg] [^dj-sqlite] [^dj-mysql] [^dj-oracle] [^dj-cache] [^dj-plugin] [^okf-litestar] [^okf-limits].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter rebases the relational (`postgresql`, `sqlite3`, `mysql`, `oracle`) and Spanner (`django_spanner`) Django `6.0+` database backends onto `vertebra.db.dbapi` (`SyncDBAPIModule` / `SyncDBAPIConnection`) with zero SQLAlchemy imports. It also upgrades `extensions/django/cache.py::CacheBackend(BaseCache)` onto `SyncCacheBackend` and `AsyncCacheBackend` (`shm://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`, `memory://`), deletes unsupported `bigquery`/`duckdb`/`mssql` Django stubs, and syncs docs and OKF chapters [^dj-backends] [^dj-spanner] [^dj-bq] [^dj-cache] [^doc-ext] [^okf-dialects].

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

- **R11 (Django Spanner Backend & Native Django `CacheBackend(BaseCache)`)**:
  - `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` (guarded via `ensure_module`) and swaps `google.cloud.spanner_dbapi` for `vertebra.db.dbapi.SyncDBAPIModule`.
  - `extensions/django/cache.py::CacheBackend` subclasses `django.core.cache.backends.base.BaseCache`, routes `server` (`LOCATION`) and `params["OPTIONS"]` (`L1_LOCATION`, `KEY_PREFIX`, `VERSION`, `TIMEOUT`) to `SyncCacheBackend` for synchronous calls (`get`, `set`, `add`, `delete`, `get_many`, `set_many`, `delete_many`, `incr`, `decr`, `touch`, `has_key`, `clear`, `close`) and to `AsyncCacheBackend` for native event-loop async calls (`aget`, `aset`, `aadd`, `adelete`, `aget_many`, `aset_many`, `adelete_many`, `aincr`, `adecr`, `atouch`, `ahas_key`, `aclear`, `aclose`) with zero `sync_to_async` thread hops.
- **R12 (Unsupported Backend Removal)**: Delete `extensions/django/db/backends/{bigquery,duckdb,mssql}/`, remove `_build_native_config` vendor dispatch from `backends/__init__.py`, and fail loud with `ImproperConfigurationError`.

```python
class CacheBackend(BaseCache):
    def __init__(self, server: str, params: Mapping[str, Any]) -> None: ...
    def get(self, key: str, default: Any = None, version: int | None = None) -> Any: ...
    async def aget(self, key: str, default: Any = None, version: int | None = None) -> Any: ...
    def set(self, key: str, value: Any, timeout: float | None = ..., version: int | None = None) -> None: ...
    async def aset(self, key: str, value: Any, timeout: float | None = ..., version: int | None = None) -> None: ...
```

- **R13 (Docs & OKF Synchronization)**: Update `docs/usage/{extensions,databases}.md`, `docs/reference/extensions.md`, `sqlspec-alchemy-and-dialects.md`, `litestar.md`, and `database-driver-limitations.md`.

## 3. Documentation & OKF Knowledge Plan

Updates `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/reference/extensions.md`, [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), [litestar.md](../../../../knowledge/domains/extensions/litestar.md), and [database-driver-limitations.md](../../../../knowledge/domains/data/database-driver-limitations.md) in `t04`, covering `SyncCacheBackend` / `AsyncCacheBackend`, `ShmCache` + SHM ring-buffer L1 invalidation, Advanced Alchemy `region_factory` vs optional `dogpile.cache` plugins, Django `DATABASES` and `CACHES` (`vertebra.extensions.django.cache.CacheBackend`), and keeping every OKF chapter `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Upgrade Django PostgreSQL and SQLite3 Backends Over Native Drivers | `src/py/vertebra/extensions/django/db/backends/postgresql/base.py`, `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` | `src/py/tests/unit/extensions/test_django_plugin.py` |
| `t02` | Upgrade Django MySQL and Oracle Backends Over Native Drivers | `src/py/vertebra/extensions/django/db/backends/mysql/base.py`, `src/py/vertebra/extensions/django/db/backends/oracle/base.py` | `src/py/tests/unit/extensions/test_django_plugin.py` |
| `t03` | Implement Django Spanner Backend, Native Django CacheBackend, and Unsupported Stub Cleanup | `src/py/vertebra/extensions/django/db/backends/spanner/{base,__init__}.py`, `src/py/vertebra/extensions/django/db/backends/__init__.py`, `src/py/vertebra/extensions/django/{cache,plugin}.py` | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| `t04` | Synchronize SQLAlchemy, Alembic, Advanced Alchemy, Caching, and Django Docs and OKF Chapters | `docs/usage/extensions.md`, `docs/usage/databases.md`, `docs/reference/extensions.md` (+3 more) | `src/py/tests/unit/extensions/test_vite_and_docs_portal.py`, `src/py/tests/unit/extensions/test_drop_in_names_and_generics.py` |

## 5. Implementation Plan

- [ ] [t01-django-postgresql-and-sqlite3.md](tasks/t01-django-postgresql-and-sqlite3.md) — Implement full DatabaseWrapper, DatabaseFeatures, DatabaseOperations, and DatabaseIntrospection for PostgreSQL and SQLite3.
- [ ] [t02-django-mysql-and-oracle.md](tasks/t02-django-mysql-and-oracle.md) — Implement full Django MySQL and Oracle backends including RETURNING INTO VariableWrapper out-binds and schema introspection.
- [ ] [t03-django-spanner-and-unsupported-cleanup.md](tasks/t03-django-spanner-and-unsupported-cleanup.md) — Upgrade the Django Spanner backend, upgrade `extensions/django/cache.py::CacheBackend(BaseCache)` onto `SyncCacheBackend` and `AsyncCacheBackend` (`shm://`, `valkey://`, `redis://`, `s3://`/`file://`, `memory://`), and delete unsupported mssql/duckdb/bigquery Django backend packages.
- [ ] [t04-sqlalchemy-django-docs-and-okf.md](tasks/t04-sqlalchemy-django-docs-and-okf.md) — Update docs/usage/extensions.md, docs/usage/databases.md, and docs/reference/extensions.md and sync sqlspec-alchemy-and-dialects.md, litestar.md, and database-driver-limitations.md.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-django-backends-and-docs` (`t04`) is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites `native-sqlite-driver`, `native-redis-driver`, and `native-valkey-driver`.

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync python -m tools.docs.scan_public_docstrings && uv run --no-sync python -m tools.scripts.review_setup validate && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_vite_and_docs_portal.py src/py/tests/unit/extensions/test_drop_in_names_and_generics.py -q
```

[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Current generic Django DatabaseWrapperMixin with vendor config dispatch.
[^dj-pg]: `src/py/vertebra/extensions/django/db/backends/postgresql/base.py` — Django PostgreSQL backend.
[^dj-sqlite]: `src/py/vertebra/extensions/django/db/backends/sqlite3/base.py` — Django SQLite3 backend.
[^dj-mysql]: `src/py/vertebra/extensions/django/db/backends/mysql/base.py` — Django MySQL backend.
[^dj-oracle]: `src/py/vertebra/extensions/django/db/backends/oracle/base.py` — Django Oracle backend.
[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Django Spanner backend.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django backend.
[^dj-cache]: `src/py/vertebra/extensions/django/cache.py::CacheBackend` — Django BaseCache backend.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin.
[^doc-ext]: `docs/usage/extensions.md` — Extensions usage documentation.
[^okf-dialects]: `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — SQLSpec and SQLAlchemy dialects OKF chapter.
[^okf-litestar]: `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar extension OKF chapter.
[^okf-limits]: `.agents/bundles/knowledge/domains/data/database-driver-limitations.md` — Database and dialect limitations OKF chapter.
