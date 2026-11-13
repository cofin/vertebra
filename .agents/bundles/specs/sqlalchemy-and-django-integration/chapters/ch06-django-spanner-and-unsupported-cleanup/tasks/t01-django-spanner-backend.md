---
type: Task
id: "sqlalchemy-and-django-integration:ch06-django-spanner-and-unsupported-cleanup:t01"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch06-django-spanner-and-unsupported-cleanup
title: "Rebase Django spanner DatabaseWrapper onto django_spanner Over vertebra.db.dbapi"
description: "Subclass django_spanner.base.DatabaseWrapper in extensions/django/db/backends/spanner/base.py and swap google.cloud.spanner_dbapi for vertebra.db.dbapi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch05-django-relational-backends:t01"
files:
  - src/py/vertebra/extensions/django/db/backends/spanner/base.py
  - src/py/vertebra/extensions/django/db/backends/spanner/__init__.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
read_set:
  - src/py/vertebra/extensions/django/db/backends/spanner/base.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/db/adapters/spanner/driver.py
  - src/py/vertebra/utils/module_loader.py
estimated_loc: 190
tags:
  - django
  - spanner
  - dbapi
sources:
  - id: dj-spanner
    resource: src/py/vertebra/extensions/django/db/backends/spanner/base.py
    title: "Current Django Spanner database backend stub"
  - id: spn-config
    resource: src/py/vertebra/db/adapters/spanner/config.py
    title: "SpannerConfig settings builder"
---

# Task `t01`: Rebase Django spanner DatabaseWrapper onto django_spanner Over vertebra.db.dbapi

## Objective & Scope Boundary

Rebase `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` onto `django_spanner.base.DatabaseWrapper` (`django-google-spanner`) over `vertebra.db.dbapi` per [`../spec.md`](../spec.md) [^dj-spanner] [^spn-config].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — current stub backend.
- `src/py/vertebra/db/adapters/spanner/config.py` & `driver.py` — `SpannerConfig` (`PROJECT`, `INSTANCE`, `NAME`, `OPTIONS`) and native `SyncSpannerDriver`.
- `src/py/vertebra/utils/module_loader.py` — `ensure_module` optional dependency guard.

## Contracts & Edge Cases

- Guard `django_spanner.base` via `ensure_module('django_spanner', package='vertebra-core[django-spanner]', extra='django-spanner')` -> `MissingDependencyError`.
- `DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper`, sets `Database = SyncDBAPIModule`, constructs `SpannerConfig` locally in `get_connection_params()` from Django `DATABASES` (`PROJECT`, `INSTANCE`, `NAME`, `OPTIONS`), and opens `SyncDBAPIConnection` backed by `vertebra._spanner` instead of `google.cloud.spanner_dbapi`.

## Test Matrix

- Unit tests verifying `MissingDependencyError` when `django_spanner` is uninstalled and verifying `get_connection_params` / `get_new_connection` swap in `SyncDBAPIModule` (`SyncDBAPIConnection`) with schema DDL routing to `update_ddl`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
```

## Acceptance Criteria

- [ ] `extensions/django/db/backends/spanner/base.py::DatabaseWrapper` subclasses `django_spanner.base.DatabaseWrapper` over `vertebra.db.dbapi`.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-spanner]: `src/py/vertebra/extensions/django/db/backends/spanner/base.py` — Current Django Spanner database backend stub.
[^spn-config]: `src/py/vertebra/db/adapters/spanner/config.py` — SpannerConfig settings builder.
