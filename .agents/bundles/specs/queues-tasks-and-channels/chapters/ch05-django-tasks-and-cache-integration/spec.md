---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch05-django-tasks-and-cache-integration
title: "Chapter 05: Native Django 6.0+ TaskBackend, Worker Command, Cache & Channels"
description: "Implement Django 6.0+ TaskBackend (SyncQueueStore enqueue and AsyncQueueStore aenqueue), vertebra_worker command with Container DI, and native Django cache/channels."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - django
  - tasks
  - queues
  - channels
  - cache
sources:
  - id: dj-queues
    resource: src/py/vertebra/extensions/django/queues.py
    title: "Legacy Django deque queue module"
  - id: dj-cache
    resource: src/py/vertebra/extensions/django/cache.py
    title: "Django cache backend"
  - id: dj-channels
    resource: src/py/vertebra/extensions/django/channels.py
    title: "Django channel layer"
---

# Chapter 05: Native Django 6.0+ TaskBackend, Worker Command, Cache & Channels

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `extensions/django/queues.py` subclasses Flask's in-memory deque queue instead of Django `6.0+`'s `BaseTaskBackend`, and `cache.py`/`channels.py` rely on in-process dicts [^dj-queues] [^dj-cache] [^dj-channels].
- **Target Architecture**: `extensions/django/tasks.py::TaskBackend(BaseTaskBackend)` routes sync `enqueue` to `SyncQueueStore` and async `aenqueue` to `AsyncQueueStore`, `vertebra_worker` executes tasks with `Container.scope()` DI, and `django/queues.py` is deleted.
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter delivers native Django `6.0+` `django.tasks` support (`TaskBackend` + `vertebra_worker` management command with Vertebra `Container` DI) and upgrades Django `CacheBackend` and `ChannelLayer` onto native drivers [^dj-queues].

## 2. Requirements, Concrete Contracts & Architecture

- **R8 (Django `6.0+` `TaskBackend` & Worker Command)**: `enqueue` uses `SyncQueueStore`, `aenqueue` uses `AsyncQueueStore`, `vertebra_worker` injects `Container.scope()` dependencies, and `django/queues.py` is deleted.
- **R9 (Django Native Cache & Channels)**: Prefix-safe `SCAN`+`UNLINK` Valkey/Redis `CacheBackend` and distributed `ChannelLayer`.

```python
class TaskBackend(BaseTaskBackend):
    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    def enqueue(self, task: Task, args: Sequence[Any], kwargs: Mapping[str, Any]) -> TaskResult: ...
    async def aenqueue(self, task: Task, args: Sequence[Any], kwargs: Mapping[str, Any]) -> TaskResult: ...
    def get_result(self, result_id: str) -> TaskResult: ...
    async def aget_result(self, result_id: str) -> TaskResult: ...
```

## 3. Documentation & OKF Knowledge Plan

Django `TASKS`, `CACHES`, and `CHANNEL_LAYERS` guides are updated in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R8` Django `TaskBackend` | `t01` | `extensions/django/{tasks,__init__}.py` | `pytest test_django_plugin.py` |
| `R8` `vertebra_worker` command & DI | `t02` | `extensions/django/management/commands/vertebra_worker.py` | `pytest test_django_plugin.py` |
| `R9` Django `CacheBackend` & `ChannelLayer` | `t03` | `extensions/django/{cache,channels}.py` | `pytest test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-django-task-backend-sync-and-async.md](tasks/t01-django-task-backend-sync-and-async.md) — Delete `django/queues.py` and implement Django `6.0+` `TaskBackend` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`).
- [ ] [t02-django-worker-command-and-di.md](tasks/t02-django-worker-command-and-di.md) — Add `vertebra_worker` Django management command with per-task `Container.scope()` DI.
- [ ] [t03-django-native-cache-and-channels.md](tasks/t03-django-native-cache-and-channels.md) — Upgrade Django `CacheBackend` and `ChannelLayer` onto `vertebra._redis` and `NotificationSource`s.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch05-django-tasks-and-cache-integration` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/extensions/test_django_plugin.py
```

[^dj-queues]: `src/py/vertebra/extensions/django/queues.py` — Legacy Django deque queue module.
[^dj-cache]: `src/py/vertebra/extensions/django/cache.py` — Django cache backend.
[^dj-channels]: `src/py/vertebra/extensions/django/channels.py` — Django channel layer.
