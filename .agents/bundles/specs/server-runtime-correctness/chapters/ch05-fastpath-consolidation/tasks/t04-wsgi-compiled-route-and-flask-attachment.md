---
type: Task
id: "server-runtime-correctness:ch05:t04"
flow_id: server-runtime-correctness
chapter_id: ch05-fastpath-consolidation
title: Unify Flask and Django CompiledRoute Structs and Flask FastPath Attachment
description: Subclass CompiledRouteBase in Flask and Django fastpath modules and attach Flask FastPath via set_attached_fastpath.
status: stable
state: completed
commit: 780338a4
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "server-runtime-correctness:ch05:t01"
files:
  - src/py/vertebra/extensions/flask/plugin.py
  - src/py/vertebra/extensions/flask/fastpath.py
  - src/py/vertebra/extensions/django/fastpath.py
tests:
  - src/py/tests/unit/extensions/test_flask_plugin.py
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/server/_fastpath_core.py
  - src/py/vertebra/extensions/flask/plugin.py
  - src/py/vertebra/extensions/flask/fastpath.py
  - src/py/vertebra/extensions/django/fastpath.py
estimated_loc: 85
tags:
  - fastpath
  - flask
  - django
  - wsgi
sources:
  - id: flask-fp-py
    resource: "src/py/vertebra/extensions/flask/fastpath.py::CompiledRoute"
    title: Flask CompiledRoute struct
  - id: django-fp-py
    resource: "src/py/vertebra/extensions/django/fastpath.py::CompiledRoute"
    title: Django CompiledRoute struct
---

# Task `t04`: Unify Flask and Django CompiledRoute Structs and Flask FastPath Attachment

## Objective & Scope Boundary

Complete `CompiledRoute(CompiledRouteBase)` unification across Flask and Django and wire `set_attached_fastpath` in `extensions/flask/plugin.py` per [`../spec.md` §2](../spec.md) [^flask-fp-py] [^django-fp-py].

## Bounded Read Manifest

- `src/py/vertebra/extensions/flask/{plugin,fastpath}.py` & `extensions/django/fastpath.py` — WSGI route plans and plugin attachment.

## Contracts & Edge Cases

- `flask.fastpath.CompiledRoute` and `django.fastpath.CompiledRoute` subclass `CompiledRouteBase` (`kw_only=True`, inheriting `handler_id`, `is_async`, `status_code`, `param_converters`), updating internal constructor call sites cleanly.
- `flask.plugin.VertebraPlugin.init_app` calls `set_attached_fastpath(app, self._fastpath)` when fastpath is enabled so `get_attached_fastpath(app)` resolves without probing `app.extensions['vertebra']`.

## Test Matrix

- Unit assertions in `test_flask_plugin.py` and `test_django_plugin.py` verifying `issubclass(CompiledRoute, CompiledRouteBase)` and `get_attached_fastpath(flask_app) is plugin.fastpath`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/flask src/py/vertebra/extensions/django/fastpath.py && uv run --no-sync mypy src/py/vertebra/extensions/flask src/py/vertebra/extensions/django/fastpath.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/extensions/test_django_plugin.py -q
```

## Acceptance Criteria

- [ ] All 5 host framework `CompiledRoute` structs inherit `CompiledRouteBase`.
- [ ] Flask `VertebraPlugin` publishes `__vertebra_fastpath__` via `set_attached_fastpath`.

[^flask-fp-py]: `src/py/vertebra/extensions/flask/fastpath.py::CompiledRoute` — Flask CompiledRoute struct.
[^django-fp-py]: `src/py/vertebra/extensions/django/fastpath.py::CompiledRoute` — Django CompiledRoute struct.
