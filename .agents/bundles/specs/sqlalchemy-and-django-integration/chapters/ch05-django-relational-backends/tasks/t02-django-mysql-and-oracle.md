---
type: Task
id: "sqlalchemy-and-django-integration:ch05-django-relational-backends:t02"
flow_id: sqlalchemy-and-django-integration
chapter_id: ch05-django-relational-backends
title: "Rebase Django mysql and oracle DatabaseWrapper Backends onto Django Built-Ins"
description: "Subclass Django 6.0+ built-in mysql and oracle DatabaseWrapper classes over vertebra.db.dbapi with local config construction."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/extensions/django/db/backends/mysql/base.py
  - src/py/vertebra/extensions/django/db/backends/oracle/base.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/db/backends/mysql/base.py
  - src/py/vertebra/extensions/django/db/backends/oracle/base.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/oracle/config.py
estimated_loc: 230
tags:
  - django
  - mysql
  - oracle
  - dbapi
sources:
  - id: dj-mysql
    resource: src/py/vertebra/extensions/django/db/backends/mysql/base.py
    title: "Current Django MySQL backend wrapper"
  - id: dj-oracle
    resource: src/py/vertebra/extensions/django/db/backends/oracle/base.py
    title: "Current Django Oracle backend wrapper"
---

# Task `t02`: Rebase Django mysql and oracle DatabaseWrapper Backends onto Django Built-Ins

## Objective & Scope Boundary

Rebase `extensions/django/db/backends/{mysql,oracle}/base.py::DatabaseWrapper` onto Django `6.0+` built-in backends over `vertebra.db.dbapi` per [`../spec.md`](../spec.md) [^dj-mysql] [^dj-oracle].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/db/backends/mysql/base.py` & `oracle/base.py` — existing wrappers.
- `src/py/vertebra/db/adapters/mysql/config.py` & `oracle/config.py` — `MySQLConfig` and `OracleConfig`.

## Contracts & Edge Cases

- `mysql/base.py::DatabaseWrapper` subclasses `django.db.backends.mysql.base.DatabaseWrapper`, builds `MySQLConfig` locally from Django `DATABASES` settings, opens `SyncDBAPIConnection`, translates Django `%s` placeholders to `?` outside string literals for `vertebra._mysql`, and exposes `lastrowid` and MariaDB/MySQL server version metadata.
- `oracle/base.py::DatabaseWrapper` subclasses `django.db.backends.oracle.base.DatabaseWrapper`, builds `OracleConfig` locally, opens `SyncDBAPIConnection`, and adapts Django `:arg0` binds and `RETURNING INTO` cursor outvars.

## Test Matrix

- Parametrized `get_connection_params`, placeholder adaptation (`%s` -> `?` with literal/comment safety on MySQL; `:arg0` on Oracle), cursor execution, and `is_usable` unit tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django/db && uv run --no-sync mypy src/py/vertebra/extensions/django/db && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `mysql` and `oracle` Django backends subclass Django `6.0+` built-ins over `vertebra.db.dbapi` with local config builders.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-mysql]: `src/py/vertebra/extensions/django/db/backends/mysql/base.py` — Current Django MySQL backend wrapper.
[^dj-oracle]: `src/py/vertebra/extensions/django/db/backends/oracle/base.py` — Current Django Oracle backend wrapper.
