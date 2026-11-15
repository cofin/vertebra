---
type: Task
id: sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t03
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: Implement Django Spanner Backend, Native Django CacheBackend, and Unsupported Stub Cleanup
description: Upgrade the Django Spanner backend, upgrade extensions/django/cache.py::CacheBackend(BaseCache) onto SyncCacheBackend and AsyncCacheBackend (shm://, valkey://, redis://, s3:///file://, memory://), and delete unsupported mssql/duckdb/bigquery Django backend packages.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t02
files:
  - src/py/vertebra/extensions/django/db/backends/spanner/base.py
  - src/py/vertebra/extensions/django/db/backends/spanner/__init__.py
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/django/cache.py
  - src/py/vertebra/extensions/django/plugin.py
  - src/py/vertebra/extensions/django/db/backends/bigquery/__init__.py
  - src/py/vertebra/extensions/django/db/backends/bigquery/base.py
  - src/py/vertebra/extensions/django/db/backends/duckdb/__init__.py
  - src/py/vertebra/extensions/django/db/backends/duckdb/base.py
  - src/py/vertebra/extensions/django/db/backends/mssql/__init__.py
  - src/py/vertebra/extensions/django/db/backends/mssql/base.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/django/db/backends/spanner/base.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/utils/module_loader.py
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/django/db/backends/bigquery/base.py
  - src/py/vertebra/extensions/django/cache.py
  - src/py/vertebra/extensions/alchemy/cache.py
  - src/py/vertebra/extensions/django/plugin.py
estimated_loc: 245
tags:
  - django
  - cache
  - shm
  - spanner
  - dbapi
  - debloat
sources:
  - id: dj-spanner
    resource: src/py/vertebra/extensions/django/db/backends/spanner/base.py
    title: Current Django Spanner database backend stub
  - id: spn-config
    resource: src/py/vertebra/db/adapters/spanner/config.py
    title: SpannerConfig settings builder
  - id: dj-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: Generic Django backend mixin with _build_native_config vendor dispatch
  - id: dj-bq
    resource: src/py/vertebra/extensions/django/db/backends/bigquery/base.py
    title: Unsupported BigQuery Django DB backend slated for removal
  - id: dj-cache
    resource: src/py/vertebra/extensions/django/cache.py::CacheBackend
    title: Django BaseCache backend
  - id: dj-plugin
    resource: src/py/vertebra/extensions/django/plugin.py
    title: Django VertebraPlugin (no DATABASES or CACHES validation today)
verification_strategy: regression_tdd
---

# Task `t03`: Implement Django Spanner Backend, Native Django CacheBackend, and Unsupported Stub Cleanup

## Objective & Scope Boundary

Rebase `src/py/vertebra/extensions/django/db/backends/spanner/base.py::DatabaseWrapper` onto `django_spanner.base.DatabaseWrapper` (`django-google-spanner`) over `vertebra.db.dbapi`, upgrade `src/py/vertebra/extensions/django/cache.py::CacheBackend` (`django.core.cache.backends.base.BaseCache`) to delegate sync methods to `SyncCacheBackend` and `*a`-prefixed async methods directly to `AsyncCacheBackend` (`shm://`, `valkey://`, `redis://`, `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://`, `memory://`), delete unsupported `extensions/django/db/backends/{bigquery,duckdb,mssql}/` packages, strip vendor dispatch from `backends/__init__.py`, and fail loud in `extensions/django/plugin.py` per [`../spec.md`](../spec.md) [^dj-spanner] [^spn-config] [^dj-backends] [^dj-bq] [^dj-cache] [^dj-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — current stub backend.
- `src/py/vertebra/db/adapters/spanner/config.py` — `SpannerConfig` (`PROJECT`, `INSTANCE`, `NAME`, `OPTIONS`) mapped into `SyncDBAPIModule`.
- `src/py/vertebra/extensions/django/cache.py` & `src/py/vertebra/extensions/alchemy/cache.py` — `CacheBackend`, `SyncCacheBackend`, and `AsyncCacheBackend`.
- `src/py/vertebra/extensions/django/db/backends/__init__.py` — remove `_build_native_config` vendor dispatch and fallback backend.
- `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — representative unsupported stub package (`{bigquery,duckdb,mssql}/{__init__,base}.py` slated for `git rm`).
- `src/py/vertebra/extensions/django/plugin.py` — `VertebraPlugin` (no `DATABASES`/`CACHES` validation today; add it here).

## Contracts & Edge Cases

- Guard `django_spanner.base` via `ensure_module('django_spanner', package='vertebra-core[django-spanner]', extra='django-spanner')` -> `MissingDependencyError`; `DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper`, sets `Database = SyncDBAPIModule`, and opens `SyncDBAPIConnection` backed by `vertebra._spanner`.
- `src/py/vertebra/extensions/django/cache.py::CacheBackend` subclasses `django.core.cache.backends.base.BaseCache`, parses `server` (`LOCATION = "shm://..." | "valkey://..." | "redis://..." | "s3://..." | "file://..." | "memory://..."`) and `params["OPTIONS"]` (`L1_LOCATION`, `KEY_PREFIX`, `VERSION`, `TIMEOUT`), delegates sync methods (`get`, `set`, `add`, `delete`, `get_many`, `set_many`, `delete_many`, `incr`, `decr`, `touch`, `has_key`, `clear`, `close`) to `SyncCacheBackend`, and delegates async methods (`aget`, `aset`, `aadd`, `adelete`, `aget_many`, `aset_many`, `adelete_many`, `aincr`, `adecr`, `atouch`, `ahas_key`, `aclear`, `aclose`) directly to `AsyncCacheBackend` on the event loop with zero `sync_to_async`.
- Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/{__init__,base}.py` completely and raise `ImproperConfigurationError` in `extensions/django/plugin.py` when `bigquery`, `duckdb`, or `mssql` is configured in Django `DATABASES`.

## Test Matrix

- Unit tests verifying `django_spanner` `MissingDependencyError` and `get_connection_params` / `get_new_connection` over `SyncDBAPIModule`.
- Parametrized `CacheBackend` sync (`get`/`set`/`add`/`delete`/`get_many`/`set_many`/`delete_many`/`incr`/`decr`/`touch`/`has_key`/`clear`) and native async (`aget`/`aset`/`aadd`/`adelete`/`aget_many`/`aset_many`/`adelete_many`/`aincr`/`adecr`/`atouch`/`ahas_key`/`aclear`/`aclose`) tests across `shm://`, `memory://`, `valkey://`, `redis://`, and `file://` with `OPTIONS["L1_LOCATION"] = "shm://..."`.
- Parametrized negative tests asserting `bigquery`, `duckdb`, and `mssql` Django `DATABASES` entries raise `ImproperConfigurationError` and deleted backend modules raise `ModuleNotFoundError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q
```

## Acceptance Criteria

- [ ] `src/py/vertebra/extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`.
- [ ] `extensions/django/cache.py::CacheBackend(BaseCache)` executes sync methods via `SyncCacheBackend` and `*a`-prefixed async methods natively via `AsyncCacheBackend`.
- [ ] `django/db/backends/{bigquery,duckdb,mssql}/` are deleted and `backends/__init__.py` contains zero vendor dispatch.
- [ ] Zero regressions in the fast verification gate.

[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Current Django Spanner database backend stub.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig settings builder.
[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Generic Django backend mixin with _build_native_config vendor dispatch.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django DB backend slated for removal.
[^dj-cache]: `src/py/vertebra/extensions/django/cache.py::CacheBackend` — Django BaseCache backend.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin (no DATABASES or CACHES validation today).
