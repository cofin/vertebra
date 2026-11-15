---
type: Task
id: sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t03
flow_id: sqlalchemy-and-django-integration
chapter_id: ch03-django-backends-and-docs
title: Implement Django Cloud Spanner Backend and Remove Unsupported MSSQL/DuckDB/BigQuery Stubs
description: Upgrade the Django Spanner backend (bit-reversed sequences, INTERLEAVE IN PARENT, schema editor) and delete unsupported mssql/duckdb/bigquery Django backend packages.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- sqlalchemy-and-django-integration:ch03-django-backends-and-docs:t02
files:
- src/py/vertebra/extensions/django/db/backends/spanner/base.py
- src/py/vertebra/extensions/django/db/backends/spanner/__init__.py
- src/py/vertebra/extensions/django/db/backends/__init__.py
- src/py/vertebra/extensions/django/plugin.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
- src/py/vertebra/extensions/django/db/backends/spanner/base.py
- src/py/vertebra/db/adapters/spanner/config.py
- src/py/vertebra/utils/module_loader.py
- src/py/vertebra/extensions/django/db/backends/__init__.py
- src/py/vertebra/extensions/django/db/backends/bigquery/base.py
- src/py/vertebra/extensions/django/plugin.py
estimated_loc: 220
tags:
- django
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
- id: dj-plugin
  resource: src/py/vertebra/extensions/django/plugin.py
  title: Django VertebraPlugin configuration validator
---

# Task `t03`: Implement Django Cloud Spanner Backend and Remove Unsupported MSSQL/DuckDB/BigQuery Stubs

## Objective & Scope Boundary

Rebase `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` onto `django_spanner.base.DatabaseWrapper` (`django-google-spanner`) over `vertebra.db.dbapi`, delete unsupported `extensions/django/db/backends/{bigquery,duckdb,mssql}/` packages, strip vendor dispatch from `backends/__init__.py`, and fail loud in `extensions/django/plugin.py` per [`../spec.md`](../spec.md) [^dj-spanner] [^spn-config] [^dj-backends] [^dj-bq] [^dj-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — current stub backend.
- `src/py/vertebra/db/adapters/spanner/config.py` — `SpannerConfig` (`PROJECT`, `INSTANCE`, `NAME`, `OPTIONS`) mapped into `SyncDBAPIModule`.
- `src/py/vertebra/utils/module_loader.py` — `ensure_module` optional dependency guard.
- `src/py/vertebra/extensions/django/db/backends/__init__.py` — remove `_build_native_config` vendor dispatch and fallback backend.
- `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — representative unsupported stub package (`{bigquery,duckdb,mssql}/{__init__,base}.py` slated for `git rm`).
- `src/py/vertebra/extensions/django/plugin.py` — `DATABASES` engine validation.

## Contracts & Edge Cases

- Guard `django_spanner.base` via `ensure_module('django_spanner', package='vertebra-core[django-spanner]', extra='django-spanner')` -> `MissingDependencyError`.
- `DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper`, sets `Database = SyncDBAPIModule`, constructs `SpannerConfig` locally in `get_connection_params()` from Django `DATABASES` (`PROJECT`, `INSTANCE`, `NAME`, `OPTIONS`), preserves bit-reversed sequence, `INTERLEAVE IN PARENT`, and schema editor support, and opens `SyncDBAPIConnection` backed by `vertebra._spanner` instead of `google.cloud.spanner_dbapi`.
- Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/{__init__,base}.py` completely (BigQuery has no Django DB backend; DuckDB and MSSQL are not built-in Django backends).
- `backends/__init__.py` exports only the 5 supported engine paths (`postgresql`, `mysql`, `oracle`, `sqlite3`, `spanner`) and shared DB-API cursor helpers with zero vendor dispatch, and `extensions/django/plugin.py` raises `ImproperConfigurationError` with guidance toward SQLSpec or SQLAlchemy when `bigquery`, `duckdb`, or `mssql` is configured in Django `DATABASES`.

## Test Matrix

- Unit tests verifying `MissingDependencyError` when `django_spanner` is uninstalled and verifying `get_connection_params` / `get_new_connection` swap in `SyncDBAPIModule` (`SyncDBAPIConnection`) with schema DDL routing to `update_ddl`.
- Parametrized negative tests asserting `bigquery`, `duckdb`, and `mssql` Django `DATABASES` entries raise `ImproperConfigurationError` and deleted backend modules raise `ModuleNotFoundError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
```

## Acceptance Criteria

- [ ] `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`.
- [ ] `django/db/backends/{bigquery,duckdb,mssql}/` are deleted and `backends/__init__.py` contains zero vendor dispatch.
- [ ] Zero regressions in the fast verification gate.

[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Current Django Spanner database backend stub.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig settings builder.
[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Generic Django backend mixin with _build_native_config vendor dispatch.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django DB backend slated for removal.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin configuration validator.
