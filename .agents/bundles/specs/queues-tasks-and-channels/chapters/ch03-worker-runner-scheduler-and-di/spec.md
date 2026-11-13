---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch03-worker-runner-scheduler-and-di
title: "Chapter 03: WorkerRunner Scoped DI, CronScheduler Leases & ASGI/WSGI Queue Wiring"
description: "Implement WorkerRunner with per-task Container.scope() DI, distributed CronScheduler slot leases, and Starlette/FastAPI/Flask queue wiring."
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
  - queues
  - worker
  - di
  - scheduler
sources:
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: "Vertebra Container DI"
  - id: queue-sched
    resource: src/py/vertebra/framework/queues/scheduler.py
    title: "CronExpression and CronScheduler"
  - id: starlette-q
    resource: src/py/vertebra/extensions/starlette/queues.py
    title: "Starlette queue extension"
  - id: flask-q
    resource: src/py/vertebra/extensions/flask/queues.py
    title: "Flask queue extension"
---

# Chapter 03: WorkerRunner Scoped DI, CronScheduler Leases & ASGI/WSGI Queue Wiring

<!-- truth: start -->
- **Baseline (`18baadcd`)**: Queue handlers lack per-task `Container.scope()` DI resolution, cron ticks lack multi-worker slot reservation, and Flask/Starlette queue extensions maintain isolated in-memory deques [^fw-di] [^queue-sched] [^flask-q].
- **Target Architecture**: `WorkerRunner` enters `Container.scope()` per attempt with heartbeats and lane semaphores; `CronScheduler` reserves `{schedule_name}:{slot_epoch}` under lease; Starlette/FastAPI/Flask bind `QueueService` [^starlette-q].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter connects native queue backends to `WorkerRunner`, `framework/di.py::Container`, `CronScheduler`, and Starlette/FastAPI/Flask lifespans [^fw-di] [^queue-sched].

## 2. Requirements, Concrete Contracts & Architecture

- **R5 (Per-Task Scoped DI & `WorkerRunner`)**: Resolve handler parameters through `Container.scope()` per task attempt with heartbeats and lane semaphores.
- **R6 (Single `CronExpression` & Distributed Leases)**: Consolidate cron parsing in `scheduler.py::CronExpression` with slot-keyed lease idempotency.

```python
class WorkerRunner:
    def __init__(
        self,
        service: QueueService,
        *,
        container: Container | None = None,
        lane_concurrency: Mapping[TaskLane, int] | None = None,
        heartbeat_interval_seconds: float = 10.0,
    ) -> None: ...
    async def run(self) -> None: ...
    async def drain(self, *, timeout_seconds: float = 30.0) -> None: ...

class CronScheduler:
    def register(self, name: str, cron_expr: str, task_name: str, *, kwargs: Mapping[str, Any] | None = None) -> None: ...
    async def tick(self, *, now: float | None = None) -> int: ...
```

## 3. Documentation & OKF Knowledge Plan

Worker DI and cron configuration are documented in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5` `WorkerRunner` & per-task DI | `t01` | `framework/queues/worker.py`, `framework/di.py` | `pytest test_distributed_queues.py` |
| `R6` `CronScheduler` distributed leases | `t02` | `framework/queues/scheduler.py` | `pytest test_distributed_queues.py` |
| `R5` Starlette/FastAPI/Flask wiring | `t03` | `extensions/{starlette/queues,flask/queues,fastapi/plugin}.py` | `pytest test_starlette_plugin.py` |

## 5. Implementation Plan

- [ ] [t01-worker-runner-scoped-di-and-lanes.md](tasks/t01-worker-runner-scoped-di-and-lanes.md) — Implement `WorkerRunner` with per-task `Container.scope()` DI, heartbeats, and concurrency lanes.
- [ ] [t02-cron-scheduler-distributed-leases.md](tasks/t02-cron-scheduler-distributed-leases.md) — Harden `CronScheduler` (`CronExpression`) with slot-keyed idempotent distributed leases.
- [ ] [t03-starlette-fastapi-flask-queue-wiring.md](tasks/t03-starlette-fastapi-flask-queue-wiring.md) — Wire Starlette, FastAPI, and Flask queue extensions onto `QueueService` and `Container` DI.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch03-worker-runner-scheduler-and-di` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py
```

[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container DI.
[^queue-sched]: `src/py/vertebra/framework/queues/scheduler.py` — CronExpression and CronScheduler.
[^starlette-q]: `src/py/vertebra/extensions/starlette/queues.py` — Starlette queue extension.
[^flask-q]: `src/py/vertebra/extensions/flask/queues.py` — Flask queue extension.
