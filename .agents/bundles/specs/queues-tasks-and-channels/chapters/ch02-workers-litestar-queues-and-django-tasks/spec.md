---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: 'Chapter 02: WorkerRunner Scoped DI, CronScheduler Leases, litestar-queues>=0.14.0 & Django 6.0+ Tasks'
description: Implement WorkerRunner per-task Container.scope() DI and CronScheduler leases, wire Starlette/FastAPI/Flask queue lifespans, bump litestar-queues>=0.14.0 with SHM/StoreQueueBackend, and deliver Django 6.0+ TaskBackend + vertebra_worker.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
depends_on_chapters:
  - ch01-queue-stores-and-first-party-backends
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-14T18:50:00Z'
    commit: 1b191388
tags:
  - queues
  - worker
  - di
  - scheduler
  - litestar-queues
  - shm
  - django
  - tasks
sources:
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: Vertebra Container DI
  - id: queue-sched
    resource: src/py/vertebra/framework/queues/scheduler.py
    title: CronExpression and CronScheduler
  - id: starlette-q
    resource: src/py/vertebra/extensions/starlette/queues.py
    title: Starlette queue extension
  - id: flask-q
    resource: src/py/vertebra/extensions/flask/queues.py
    title: Flask queue extension
  - id: pyproject-lq
    resource: pyproject.toml
    title: litestar-queues dependency floor
  - id: lq-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
    title: litestar-queues backend module
  - id: lq-cfg
    resource: src/py/vertebra/extensions/litestar/queues/config.py
    title: litestar-queues config helpers
  - id: dj-queues
    resource: src/py/vertebra/extensions/django/queues.py
    title: Legacy Django deque queue module
  - id: srv-lanes
    resource: src/py/vertebra/server/lanes.py::WorkerRunner
    title: Existing multi-lane WorkerRunner with QueueService integration and lame-duck drain
current_task: "queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t01"
---

# Chapter 02: WorkerRunner Scoped DI, CronScheduler Leases, litestar-queues>=0.14.0 & Django 6.0+ Tasks

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `src/py/vertebra/server/lanes.py::WorkerRunner` runs handlers without per-task `Container.scope()` DI or lease heartbeats, `QueueService`'s inline cron path bypasses `CronScheduler`'s slot lease, the Flask queue module keeps an isolated in-memory deque while Starlette/FastAPI re-export the litestar-queues SHM names, `pyproject.toml` pins `litestar-queues>=0.12.0` (`queues` extra and `test` dependency group) with a broken `litestar_queues.records` import in `extensions/litestar/queues/backend.py`, and `extensions/django/queues.py` subclasses Flask's deque instead of Django `6.0+`'s `BaseTaskBackend` [^fw-di] [^srv-lanes] [^queue-sched] [^flask-q] [^pyproject-lq] [^lq-backend] [^dj-queues].
- **Target Architecture**: `WorkerRunner` enters `Container.scope()` per task attempt with heartbeats and lane semaphores; `CronScheduler` reserves `{schedule_name}:{slot_epoch}` under distributed lease; Starlette/FastAPI/Flask bind `QueueService`; `litestar-queues>=0.14.0` runs over slotted `SHMQueueBackend` (`@queue_backend('vertebra_shm')`), engine-neutral `StoreQueueBackend` (`@queue_backend('vertebra_store')`) with typed config objects carrying `backend_name`, `SQLSpecBackendConfig` (`queue_store_class` + `events_store_class`), and `ContainerTaskDependencyProvider`; `extensions/django/tasks.py::TaskBackend(BaseTaskBackend)` routes sync `enqueue` to `SyncQueueStore` and async `aenqueue` to `AsyncQueueStore` with `vertebra_worker` DI while deleting `django/queues.py` [^starlette-q] [^lq-cfg].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter wires native queue stores to the existing `src/py/vertebra/server/lanes.py::WorkerRunner` (extended in place; no second runner class), `framework/di.py::Container` scoped DI, `CronScheduler` distributed leases, Starlette/FastAPI/Flask lifespans, `litestar-queues>=0.14.0` (`SHMQueueBackend`, `StoreQueueBackend`, `SQLSpecBackendConfig`, `ContainerTaskDependencyProvider`), and native Django `6.0+` `django.tasks` (`TaskBackend` + `vertebra_worker` management command) [^fw-di] [^srv-lanes] [^queue-sched] [^lq-backend] [^lq-cfg] [^dj-queues].

## 2. Requirements, Concrete Contracts & Architecture

- **R5 (Per-Task Scoped DI & `WorkerRunner`)**: Extend `src/py/vertebra/server/lanes.py::WorkerRunner` to resolve handler parameters through `Container.scope()` per task attempt with lease heartbeats, keeping its lane semaphores, `submit`/`process_once`/`run`/`stop`, and lame-duck drain.
- **R6 (Single `CronExpression` & Distributed Leases)**: Consolidate cron parsing in `scheduler.py::CronExpression` and route `QueueService`'s inline cron loop through `CronScheduler` slot-keyed leases; `CronScheduler.tick` changes from `tick(self, now: datetime | None = None) -> list[TaskEnvelope]` to the signature below.

```python
class WorkerRunner:
    def __init__(
        self,
        lanes: Sequence[LaneConfig],
        *,
        queue_service: QueueService | None = None,
        lame_duck: LameDuckController | None = None,
        span_exporter: SpanExporter | None = None,
        scheduler: CronScheduler | None = None,
        container: Container | None = None,
        heartbeat_interval_seconds: float = 10.0,
    ) -> None: ...
    async def run(self) -> None: ...
    async def stop(self) -> None: ...

class CronScheduler:
    def register(self, name: str, cron_expr: str, task_name: str, *, kwargs: Mapping[str, Any] | None = None) -> None: ...
    async def tick(self, *, now: float | None = None) -> int: ...
```

- **R7 (`litestar-queues>=0.14.0` Backends & DI)**: Slotted `SHMQueueBackend` and engine-neutral `StoreQueueBackend`, `queue_store_class`/`events_store_class` support, and `ContainerTaskDependencyProvider`.

```python
class StoreQueueBackendConfig:
    backend_name: ClassVar[str] = "vertebra_store"
    store: AsyncQueueStore
    notification_source: AsyncNotificationSource | None = None
    terminal_ttl: timedelta | None = None
    def create_backend(self, config: QueueConfig) -> StoreQueueBackend: ...

@queue_backend("vertebra_store")
class StoreQueueBackend(BaseQueueBackend):
    __slots__ = ("_notification_source", "_store", "_terminal_ttl")

@queue_backend("vertebra_shm")
class SHMQueueBackend(BaseQueueBackend):
    __slots__ = ("_ring", "_segment_name", "_terminal_ttl")

class ContainerTaskDependencyProvider:
    def __init__(self, container: Container) -> None: ...
    def __call__(self, task: Any, record: Any, context: Any) -> AsyncContextManager[Mapping[str, Any]]: ...
```

- **R8 (Django `6.0+` `TaskBackend` & Worker Command)**: `enqueue` uses `SyncQueueStore`, `aenqueue` uses `AsyncQueueStore`, `vertebra_worker` injects `Container.scope()` dependencies, and `django/queues.py` is deleted.

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

Worker DI, cron configuration, `litestar-queues` backend selection, and Django `TASKS` / `CHANNEL_LAYERS` guides are documented in `ch03-distributed-channels-stores-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement WorkerRunner Per-Task Container.scope() DI, Lanes, and CronScheduler Leases | `src/py/vertebra/server/lanes.py`, `src/py/vertebra/framework/di.py`, `src/py/vertebra/framework/queues/scheduler.py` (+1 more) | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t02` | Wire Starlette, FastAPI & Flask Queue Extensions onto QueueService & Container DI | `src/py/vertebra/extensions/starlette/queues.py`, `src/py/vertebra/extensions/fastapi/queues.py`, `src/py/vertebra/extensions/flask/queues.py` (+1 more) | `src/py/tests/unit/extensions/test_starlette_plugin.py`, `src/py/tests/unit/extensions/test_flask_plugin.py` |
| `t03` | Bump litestar-queues>=0.14.0 and Implement SHMQueueBackend, StoreQueueBackend & DI Provider | `pyproject.toml`, `src/py/vertebra/extensions/litestar/queues/shm_backend.py`, `src/py/vertebra/extensions/litestar/queues/backend.py` (+2 more) | `src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py` |
| `t04` | Implement Django 6.0+ TaskBackend (Sync/Async) and vertebra_worker Management Command | `src/py/vertebra/extensions/django/tasks.py`, `src/py/vertebra/extensions/django/__init__.py`, `src/py/vertebra/extensions/django/management/__init__.py` (+2 more) | `src/py/tests/unit/extensions/test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-worker-runner-scoped-di-and-cron-leases.md](tasks/t01-worker-runner-scoped-di-and-cron-leases.md) — Extend the existing WorkerRunner to resolve handler dependencies via Container.scope() with heartbeats and route QueueService cron through CronScheduler slot-keyed leases.
- [ ] [t02-starlette-fastapi-flask-queue-wiring.md](tasks/t02-starlette-fastapi-flask-queue-wiring.md) — Replace the Flask deque stub and the Starlette/FastAPI litestar-queues re-exports with unified QueueService, QueueBackendSettings, and Container DI.
- [ ] [t03-litestar-queues-backends-and-di.md](tasks/t03-litestar-queues-backends-and-di.md) — Raise litestar-queues>=0.14.0 and wire slotted SHMQueueBackend, StoreQueueBackend(BaseQueueBackend), queue_store_class/events_store_class, and ContainerTaskDependencyProvider.
- [ ] [t04-django-task-backend-and-worker-command.md](tasks/t04-django-task-backend-and-worker-command.md) — Delete django/queues.py, implement django/tasks.py::TaskBackend over SyncQueueStore/AsyncQueueStore, and add management/commands/vertebra_worker.py with Container.scope() DI.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-workers-litestar-queues-and-django-tasks` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-redis-driver`, `native-valkey-driver`, and `sqlalchemy-and-django-integration`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/server/lanes.py src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/fastapi/queues.py src/py/vertebra/extensions/flask/queues.py src/py/vertebra/extensions/fastapi/plugin.py src/py/vertebra/extensions/litestar/queues src/py/vertebra/extensions/django && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/server/lanes.py src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/fastapi/queues.py src/py/vertebra/extensions/flask/queues.py src/py/vertebra/extensions/fastapi/plugin.py src/py/vertebra/extensions/litestar/queues src/py/vertebra/extensions/django && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py src/py/tests/unit/extensions/test_fastapi_plugin.py src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py src/py/tests/unit/extensions/test_django_plugin.py
```

[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container DI.
[^queue-sched]: `src/py/vertebra/framework/queues/scheduler.py` — CronExpression and CronScheduler.
[^starlette-q]: `src/py/vertebra/extensions/starlette/queues.py` — Starlette queue extension.
[^flask-q]: `src/py/vertebra/extensions/flask/queues.py` — Flask queue extension.
[^pyproject-lq]: `pyproject.toml` — litestar-queues dependency floor.
[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues backend module.
[^lq-cfg]: `src/py/vertebra/extensions/litestar/queues/config.py` — litestar-queues config helpers.
[^dj-queues]: `src/py/vertebra/extensions/django/queues.py` — Legacy Django deque queue module.
[^srv-lanes]: `src/py/vertebra/server/lanes.py::WorkerRunner` — Existing multi-lane WorkerRunner with QueueService integration and lame-duck drain.
