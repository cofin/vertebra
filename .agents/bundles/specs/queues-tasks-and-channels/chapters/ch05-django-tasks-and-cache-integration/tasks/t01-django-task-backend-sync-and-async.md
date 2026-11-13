---
type: Task
id: "queues-tasks-and-channels:ch05-django-tasks-and-cache-integration:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch05-django-tasks-and-cache-integration
title: "Implement Native Django 6.0+ TaskBackend (SyncQueueStore enqueue & AsyncQueueStore aenqueue)"
description: "Delete fake extensions/django/queues.py and implement django.tasks BaseTaskBackend in extensions/django/tasks.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch02-first-party-queue-backends-and-service:t02"
  - "ch02-first-party-queue-backends-and-service:t03"
files:
  - src/py/vertebra/extensions/django/tasks.py
  - src/py/vertebra/extensions/django/__init__.py
tests:
  - src/py/tests/unit/extensions/test_django_plugin.py
read_set:
  - src/py/vertebra/extensions/django/queues.py
  - src/py/vertebra/extensions/django/__init__.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/db/driver/capabilities.py
estimated_loc: 235
tags:
  - django
  - tasks
  - queues
sources:
  - id: dj-queues
    resource: src/py/vertebra/extensions/django/queues.py
    title: "Fake deque-based Django queue module slated for deletion"
  - id: dj-init
    resource: src/py/vertebra/extensions/django/__init__.py
    title: "Public Django extension exports"
---

# Task `t01`: Implement Native Django 6.0+ TaskBackend (SyncQueueStore enqueue & AsyncQueueStore aenqueue)

## Objective & Scope Boundary

Delete `src/py/vertebra/extensions/django/queues.py` and implement native Django `6.0+` `TaskBackend(BaseTaskBackend)` in `extensions/django/tasks.py` per [`../spec.md`](../spec.md) [^dj-queues] [^dj-init].

## Bounded Read Manifest

- `src/py/vertebra/extensions/django/queues.py` — legacy Flask-deque subclass to delete via `git rm`.
- `src/py/vertebra/db/driver/capabilities.py` & `framework/queues/base.py` — `SyncQueueStore`, `AsyncQueueStore`, and `QueueService`.

## Contracts & Edge Cases

- `TaskBackend(django.tasks.backends.base.BaseTaskBackend)` sets `supports_defer = True`, `supports_async_task = True`, `supports_get_result = True`, and `supports_priority = True`.
- `enqueue(task, args, kwargs)` executes natively via `SyncQueueStore` / `QueueService.enqueue_sync` with ZERO `PortalProvider` event-loop thread hops; `aenqueue(task, args, kwargs)` executes natively via `AsyncQueueStore` / `QueueService.enqueue`.
- `get_result(result_id)` and `aget_result(result_id)` return authentic `django.tasks.TaskResult` (`TaskResultStatus`, `TaskError`) instances.

## Test Matrix

- Parametrized sync `task.enqueue()` (asserting no running event loop required) and async `await task.aenqueue()` + `get_result`/`aget_result` status transition tests (`READY`, `RUNNING`, `SUCCESSFUL`, `FAILED`).

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

## Acceptance Criteria

- [ ] `extensions/django/queues.py` is deleted and `extensions/django/tasks.py::TaskBackend` implements sync `enqueue` via `SyncQueueStore` and async `aenqueue` via `AsyncQueueStore`.
- [ ] Zero regressions in the chapter fast verification gate.

[^dj-queues]: `src/py/vertebra/extensions/django/queues.py` — Fake deque-based Django queue module slated for deletion.
[^dj-init]: `src/py/vertebra/extensions/django/__init__.py` — Public Django extension exports.
