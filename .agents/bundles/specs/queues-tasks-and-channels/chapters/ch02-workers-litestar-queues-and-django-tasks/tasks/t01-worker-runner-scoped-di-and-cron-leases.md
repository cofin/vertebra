---
type: Task
id: queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t01
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: Implement WorkerRunner Per-Task Container.scope() DI, Lanes, and CronScheduler Leases
description: Extend the existing server/lanes.py WorkerRunner to resolve handler dependencies via Container.scope() with heartbeats and route QueueService cron through CronScheduler slot-keyed leases.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on:
  - queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t04
files:
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/framework/di.py
  - src/py/vertebra/framework/queues/scheduler.py
  - src/py/vertebra/framework/queues/base.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/server/lanes.py
  - src/py/vertebra/framework/di.py
  - src/py/vertebra/framework/_app.py
  - src/py/vertebra/framework/queues/scheduler.py
  - src/py/vertebra/db/driver/capabilities.py
estimated_loc: 235
tags:
  - queues
  - worker
  - di
  - cron
  - scheduler
sources:
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: Vertebra Container dependency injection and request/task scopes
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: QueueService task registration and dispatch
  - id: queue-sched
    resource: src/py/vertebra/framework/queues/scheduler.py
    title: CronExpression parser and CronScheduler
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py
    title: acquire_lease and reserve_unique_key store contracts
verification_strategy: regression_tdd
---

# Task `t01`: Implement WorkerRunner Per-Task Container.scope() DI, Lanes, and CronScheduler Leases

## Objective & Scope Boundary

Extend the existing `src/py/vertebra/server/lanes.py::WorkerRunner` (already re-exported from `vertebra.framework.queues` and used by `vtb workers run`) to resolve task dependencies inside a per-attempt `Container.scope()`, and route `QueueService`'s inline cron loop in `src/py/vertebra/framework/queues/base.py` through `CronScheduler` (`src/py/vertebra/framework/queues/scheduler.py`) slot-keyed leases per [`../spec.md`](../spec.md) [^fw-di] [^queue-base] [^queue-sched] [^queue-cap].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/base.py` — `QueueService` handler registry and result persistence.
- `src/py/vertebra/server/lanes.py` — `WorkerRunner`, `LaneConfig`, and `LaneMetrics` (lane semaphores, `submit`, `process_once`, `run`, `stop`, lame-duck drain).
- `src/py/vertebra/framework/di.py` — `Container` sync/async scope entry and parameter injection.
- `src/py/vertebra/framework/_app.py` — app-level container and worker lifespan wiring.
- `src/py/vertebra/framework/queues/scheduler.py` — `CronExpression` and `CronScheduler`.
- `src/py/vertebra/db/driver/capabilities.py` — `acquire_lease` and `reserve_unique_key`.

## Contracts & Edge Cases

- `WorkerRunner` gains `container: Container | None = None` and `heartbeat_interval_seconds: float = 10.0` keyword parameters (no second runner class) and enters a fresh `async with container.scope():` (or sync `with container.scope():` for sync workers; `Container.scope()` returns a `RequestScope` usable with both forms) per claimed task attempt, injecting typed dependencies into task handlers, tearing scoped resources down deterministically on completion/failure, and maintaining background lease heartbeats (`heartbeat`), stale-claim reaping (`reap_stale`), per-lane concurrency semaphores (`io`, `cpu`, `admin`), and graceful in-flight drain on shutdown.
- `CronExpression` serves as the sole cron parser across Vertebra queues; `CronScheduler.tick` changes from `tick(self, now: datetime | None = None) -> list[TaskEnvelope]` to `async tick(self, *, now: float | None = None) -> int`; `CronScheduler` acquires a token-fenced scheduler lease and reserves `{schedule_name}:{slot_epoch}` idempotency keys before enqueuing so multi-worker clusters fire each cron tick at most once.

## Test Matrix

- Unit tests verifying per-task DI scope isolation (distinct scoped instances per task, deterministic cleanup on error), lane starvation prevention, heartbeat renewal, and graceful drain, alongside parametrized `CronExpression` boundary and multi-scheduler concurrent race tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py
```

## Acceptance Criteria

- [ ] `WorkerRunner` resolves handler dependencies via `Container.scope()` and drains in-flight tasks cleanly.
- [ ] `CronScheduler` guarantees at-most-once per-slot cron enqueue across concurrent workers.
- [ ] Zero regressions in the fast verification gate.

[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container dependency injection and request/task scopes.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService task registration and dispatch.
[^queue-sched]: `src/py/vertebra/framework/queues/scheduler.py` — CronExpression parser and CronScheduler.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — acquire_lease and reserve_unique_key store contracts.
