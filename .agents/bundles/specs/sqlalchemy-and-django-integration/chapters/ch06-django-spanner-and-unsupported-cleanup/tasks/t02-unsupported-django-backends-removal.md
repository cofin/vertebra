---
type: Task
id: "sqlalchemy-and-django-integration:ch06-django-spanner-and-unsupported-cleanup:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch06-django-spanner-and-unsupported-cleanup
title: "Remove Unsupported bigquery, duckdb & mssql Django Backends and Clean backends/__init__.py"
description: "Delete django/db/backends/{bigquery,duckdb,mssql}/, remove vendor dispatch from backends/__init__.py, and fail loud in django/plugin.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch05-django-relational-backends:t02"
  - t01
files:
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/django/plugin.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/db/backends/__init__.py
  - src/py/vertebra/extensions/django/db/backends/bigquery/base.py
  - src/py/vertebra/extensions/django/db/backends/duckdb/base.py
  - src/py/vertebra/extensions/django/db/backends/mssql/base.py
  - src/py/vertebra/extensions/django/plugin.py
estimated_loc: 140
tags:
  - django
  - debloat
  - dbapi
sources:
  - id: dj-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: "Generic Django backend mixin with _build_native_config vendor dispatch"
  - id: dj-bq
    resource: src/py/vertebra/extensions/django/db/backends/bigquery/base.py
    title: "Unsupported BigQuery Django DB backend slated for removal"
  - id: dj-plugin
    resource: src/py/vertebra/extensions/django/plugin.py
    title: "Django VertebraPlugin configuration validator"
---

# Task `t02`: Remove Unsupported bigquery, duckdb & mssql Django Backends and Clean backends/__init__.py

## Objective & Scope Boundary

Delete unsupported `extensions/django/db/backends/{bigquery,duckdb,mssql}/` packages, strip vendor dispatch from `backends/__init__.py`, and fail loud in `extensions/django/plugin.py` per [`../spec.md`](../spec.md) [^dj-backends] [^dj-bq] [^dj-plugin].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/` — six files (`__init__.py`, `base.py`) to delete via `git rm`.
- `src/py/vertebra/extensions/django/db/backends/__init__.py` — remove `_build_native_config` vendor dispatch and fallback backend.
- `src/py/vertebra/extensions/django/plugin.py` — `DATABASES` engine validation.

## Contracts & Edge Cases

- Delete `src/py/vertebra/extensions/django/db/backends/{bigquery,duckdb,mssql}/{__init__,base}.py` completely (BigQuery has no Django DB backend; DuckDB and MSSQL are not built-in Django backends).
- `backends/__init__.py` exports only the 5 supported engine paths (`postgresql`, `mysql`, `oracle`, `sqlite3`, `spanner`) and shared DB-API cursor helpers with zero vendor dispatch.
- `extensions/django/plugin.py` raises `ImproperConfigurationError` with guidance toward SQLSpec or SQLAlchemy when `bigquery`, `duckdb`, or `mssql` is configured in Django `DATABASES`.

## Test Matrix

- Parametrized negative tests asserting `bigquery`, `duckdb`, and `mssql` Django `DATABASES` entries raise `ImproperConfigurationError` and deleted backend modules raise `ModuleNotFoundError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
```

## Acceptance Criteria

- [ ] `django/db/backends/{bigquery,duckdb,mssql}/` are deleted and `backends/__init__.py` contains zero vendor dispatch.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Generic Django backend mixin with _build_native_config vendor dispatch.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django DB backend slated for removal.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin configuration validator.
