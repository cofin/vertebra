---
type: Spec
flow_id: sqlalchemy-and-django-integration
chapter_id: ch06-django-spanner-and-unsupported-cleanup
title: "Chapter 06: Django Cloud Spanner Backend & Unsupported Engine Removal"
description: "Subclass django_spanner.base.DatabaseWrapper over vertebra.db.dbapi and remove unsupported bigquery, duckdb, and mssql Django database backends."
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
  - spanner
  - debloat
sources:
  - id: dj-spanner
    resource: src/py/vertebra/extensions/django/db/backends/spanner/base.py
    title: "Django Spanner backend"
  - id: dj-bq
    resource: src/py/vertebra/extensions/django/db/backends/bigquery/base.py
    title: "Unsupported BigQuery Django backend"
  - id: dj-backends
    resource: src/py/vertebra/extensions/django/db/backends/__init__.py
    title: "Django backend registry and mixin"
  - id: dj-plugin
    resource: src/py/vertebra/extensions/django/plugin.py
    title: "Django VertebraPlugin"
---

# Chapter 06: Django Cloud Spanner Backend & Unsupported Engine Removal

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/django/db/backends/` ships hollow stub packages for `spanner`, `bigquery`, `duckdb`, and `mssql` alongside vendor-dispatching `DatabaseWrapperMixin._build_native_config` [^dj-spanner] [^dj-bq] [^dj-backends].
- **Target Architecture**: `spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`; `bigquery`, `duckdb`, and `mssql` Django backends are deleted; `django/plugin.py` raises `ImproperConfigurationError` for unsupported engines [^dj-plugin].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter wires the official Google Cloud Spanner Django backend (`django_spanner`) onto Vertebra's Rust-backed `SyncDBAPIModule` and removes the unsupported `bigquery`, `duckdb`, and `mssql` Django backend stubs [^dj-spanner] [^dj-bq].

## 2. Requirements, Concrete Contracts & Architecture

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

## 3. Documentation & OKF Knowledge Plan

Supported Django `ENGINE` paths and BigQuery/DuckDB/MSSQL exclusions are documented in `ch07-sqlalchemy-django-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R11` Django `spanner` backend | `t01` | `extensions/django/db/backends/spanner/{base,__init__}.py` | `pytest test_django_plugin.py` |
| `R12` Delete unsupported Django backends | `t02` | `extensions/django/db/backends/__init__.py`, `extensions/django/plugin.py` | `pytest test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-django-spanner-backend.md](tasks/t01-django-spanner-backend.md) — Subclass `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi` in `spanner/base.py`.
- [ ] [t02-unsupported-django-backends-removal.md](tasks/t02-unsupported-django-backends-removal.md) — Delete `bigquery`, `duckdb`, and `mssql` Django DB backends and remove vendor dispatch from `backends/__init__.py`.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch06-django-spanner-and-unsupported-cleanup` is `planned` behind upstream [`../../spec.md`](../../spec.md) prerequisites (`broker-pool-sharing` and `native-sqlite-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
```

[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Django Spanner backend.
[^dj-bq]: `src/py/vertebra/extensions/django/db/backends/bigquery/base.py` — Unsupported BigQuery Django backend.
[^dj-backends]: `src/py/vertebra/extensions/django/db/backends/__init__.py` — Django backend registry and mixin.
[^dj-plugin]: `src/py/vertebra/extensions/django/plugin.py` — Django VertebraPlugin.
