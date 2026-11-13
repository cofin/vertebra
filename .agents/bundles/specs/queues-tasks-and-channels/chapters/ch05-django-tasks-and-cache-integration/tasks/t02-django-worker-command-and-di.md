---
type: Task
id: "queues-tasks-and-channels:ch05-django-tasks-and-cache-integration:t02"
flow_id: queues-tasks-and-channels
chapter_id: ch05-django-tasks-and-cache-integration
title: "Add Django vertebra_worker Management Command with Scoped Container DI"
description: "Create management/commands/vertebra_worker.py running Django 6.0+ task workers with per-task Container.scope() DI."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch03-worker-runner-scheduler-and-di:t01"
  - t01
files:
  - src/py/vertebra/extensions/django/management/__init__.py
  - src/py/vertebra/extensions/django/management/commands/__init__.py
  - src/py/vertebra/extensions/django/management/commands/vertebra_worker.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/tasks.py
  - src/py/vertebra/extensions/django/apps.py
  - src/py/vertebra/framework/queues/worker.py
  - src/py/vertebra/framework/di.py
estimated_loc: 175
tags:
  - django
  - tasks
  - worker
  - di
sources:
  - id: dj-apps
    resource: src/py/vertebra/extensions/django/apps.py
    title: "VertebraAppConfig Django app registration"
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: "Container per-task dependency scope"
---

# Task `t02`: Add Django vertebra_worker Management Command with Scoped Container DI

## Objective & Scope Boundary

Create the `vertebra_worker` Django management command executing `django.tasks` jobs through `WorkerRunner` and `Container.scope()` per [`../spec.md`](../spec.md) [^dj-apps] [^fw-di].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/apps.py` — `VertebraAppConfig`.
- `src/py/vertebra/framework/queues/worker.py` & `framework/di.py` — `WorkerRunner` and `Container`.

## Contracts & Edge Cases

- Implement `Command(BaseCommand)` in `management/commands/vertebra_worker.py` (`--backend`, `--queues`, `--concurrency`, `--once`, `-- burst`) invoking sync and async `@task` callables inside `Container.scope()` and recording `TaskResult` return values and `TaskError` tracebacks.

## Test Matrix

- `call_command('vertebra_worker', '--once')` end-to-end test executing both sync and async Django `@task` functions with DI-injected parameters.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `python manage.py vertebra_worker` executes enqueued Django `6.0+` tasks with scoped `Container` DI.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-apps]: `src/py/vertebra/extensions/django/apps.py` — VertebraAppConfig Django app registration.
[^fw-di]: `src/py/vertebra/framework/di.py` — Container per-task dependency scope.
