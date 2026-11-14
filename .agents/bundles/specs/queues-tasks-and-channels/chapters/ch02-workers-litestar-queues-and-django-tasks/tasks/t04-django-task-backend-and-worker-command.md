---
type: Task
id: queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t04
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: Implement Django 6.0+ TaskBackend (Sync/Async) and vertebra_worker Management Command
description: Delete django/queues.py, implement django/tasks.py::TaskBackend over SyncQueueStore/AsyncQueueStore, and add management/commands/vertebra_worker.py with Container.scope() DI.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t03
files:
- src/py/vertebra/extensions/django/tasks.py
- src/py/vertebra/extensions/django/__init__.py
- src/py/vertebra/extensions/django/management/__init__.py
- src/py/vertebra/extensions/django/management/commands/__init__.py
- src/py/vertebra/extensions/django/management/commands/vertebra_worker.py
tests:
- src/py/tests/unit/extensions/test_django_plugin.py
read_set:
- src/py/vertebra/extensions/django/queues.py
- src/py/vertebra/extensions/django/__init__.py
- src/py/vertebra/framework/queues/base.py
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/extensions/django/tasks.py
- src/py/vertebra/extensions/django/apps.py
estimated_loc: 235
tags:
- django
- tasks
- queues
- worker
- di
sources:
- id: dj-queues
  resource: src/py/vertebra/extensions/django/queues.py
  title: Fake deque-based Django queue module slated for deletion
- id: dj-init
  resource: src/py/vertebra/extensions/django/__init__.py
  title: Public Django extension exports
- id: dj-apps
  resource: src/py/vertebra/extensions/django/apps.py
  title: VertebraAppConfig Django app registration
- id: fw-di
  resource: src/py/vertebra/framework/di.py
  title: Container per-task dependency scope
---

# Task `t04`: Implement Django 6.0+ TaskBackend (Sync/Async) and vertebra_worker Management Command

## Objective & Scope Boundary

Delete `src/py/vertebra/extensions/django/queues.py` and implement native Django `6.0+` `TaskBackend(BaseTaskBackend)` in `extensions/django/tasks.py` per [`../spec.md`](../spec.md) [^dj-queues] [^dj-init].

Create the `vertebra_worker` Django management command executing `django.tasks` jobs through `WorkerRunner` and `Container.scope()` per [`../spec.md`](../spec.md) [^dj-apps] [^fw-di].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/queues.py` — legacy Flask-deque subclass to delete via `git rm`.
- `src/py/vertebra/db/driver/capabilities.py` & `framework/queues/base.py` — `SyncQueueStore`, `AsyncQueueStore`, and `QueueService`.

- `src/py/vertebra/extensions/django/apps.py` — `VertebraAppConfig`.
- `src/py/vertebra/framework/queues/worker.py` & `framework/di.py` — `WorkerRunner` and `Container`.

## Contracts & Edge Cases

- `TaskBackend(django.tasks.backends.base.BaseTaskBackend)` sets `supports_defer = True`, `supports_async_task = True`, `supports_get_result = True`, and `supports_priority = True`.
- `enqueue(task, args, kwargs)` executes natively via `SyncQueueStore` / `QueueService.enqueue_sync` with ZERO `PortalProvider` event-loop thread hops; `aenqueue(task, args, kwargs)` executes natively via `AsyncQueueStore` / `QueueService.enqueue`.
- `get_result(result_id)` and `aget_result(result_id)` return authentic `django.tasks.TaskResult` (`TaskResultStatus`, `TaskError`) instances.

- Implement `Command(BaseCommand)` in `management/commands/vertebra_worker.py` (`--backend`, `--queues`, `--concurrency`, `--once`, `-- burst`) invoking sync and async `@task` callables inside `Container.scope()` and recording `TaskResult` return values and `TaskError` tracebacks.

## Test Matrix

- Parametrized sync `task.enqueue()` (asserting no running event loop required) and async `await task.aenqueue()` + `get_result`/`aget_result` status transition tests (`READY`, `RUNNING`, `SUCCESSFUL`, `FAILED`).

- `call_command('vertebra_worker', '--once')` end-to-end test executing both sync and async Django `@task` functions with DI-injected parameters.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `extensions/django/queues.py` is deleted and `extensions/django/tasks.py::TaskBackend` implements sync `enqueue` via `SyncQueueStore` and async `aenqueue` via `AsyncQueueStore`.
- [ ] `python manage.py vertebra_worker` executes enqueued Django `6.0+` tasks with scoped `Container` DI.
- [ ] Zero regressions in the fast verification gate.

[^dj-queues]: `src/py/vertebra/extensions/django/queues.py` — Fake deque-based Django queue module slated for deletion.
[^dj-init]: `src/py/vertebra/extensions/django/__init__.py` — Public Django extension exports.
[^dj-apps]: `src/py/vertebra/extensions/django/apps.py` — VertebraAppConfig Django app registration.
[^fw-di]: `src/py/vertebra/framework/di.py` — Container per-task dependency scope.
