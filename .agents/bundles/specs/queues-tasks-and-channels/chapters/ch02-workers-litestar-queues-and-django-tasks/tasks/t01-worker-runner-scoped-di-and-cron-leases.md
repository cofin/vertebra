---
type: Task
id: queues-tasks-and-channels:ch02-workers-litestar-queues-and-django-tasks:t01
flow_id: queues-tasks-and-channels
chapter_id: ch02-workers-litestar-queues-and-django-tasks
title: Implement WorkerRunner Per-Task Container.scope() DI, Lanes, and CronScheduler Leases
description: Build WorkerRunner resolving handler dependencies via Container.scope() with heartbeats/lanes and harden CronScheduler with slot-keyed distributed leases.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t04
files:
- src/py/vertebra/framework/queues/worker.py
- src/py/vertebra/framework/di.py
- src/py/vertebra/framework/queues/scheduler.py
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
---

# Task `t01`: Implement WorkerRunner Per-Task Container.scope() DI, Lanes, and CronScheduler Leases

## Objective & Scope Boundary

Implement `WorkerRunner` (`src/py/vertebra/framework/queues/worker.py`) resolving task dependencies inside a per-attempt `Container.scope()` per [`../spec.md`](../spec.md) [^fw-di] [^queue-base].

Upgrade `CronScheduler` (`src/py/vertebra/framework/queues/scheduler.py`) with distributed lease coordination and slot-keyed idempotency per [`../spec.md`](../spec.md) [^queue-sched] [^queue-cap].

## Bounded Read Manifest

- `src/py/vertebra/framework/di.py` — `Container` sync/async scope entry and parameter injection.
- `src/py/vertebra/framework/queues/base.py` — `QueueService` handler registry and result persistence.
- `src/py/vertebra/server/lanes.py` — `MultiLaneWorkerRunner` lane concurrency primitives.

- `src/py/vertebra/framework/queues/scheduler.py` — `CronExpression` and `CronScheduler`.
- `src/py/vertebra/db/driver/capabilities.py` — `acquire_lease` and `reserve_unique_key`.

## Contracts & Edge Cases

- `WorkerRunner` enters a fresh `async with container.scope():` (or sync `with container.sync_scope():` for sync workers) per claimed task attempt, injecting typed dependencies into task handlers and tearing scoped resources down deterministically on completion/failure.
- Maintain background lease heartbeats (`heartbeat`), stale-claim reaping (`reap_stale`), per-lane concurrency semaphores (`io`, `cpu`, `admin`), and graceful in-flight drain on shutdown.

- `CronExpression` serves as the single cron parser across Vertebra queues.
- `CronScheduler` acquires a token-fenced scheduler lease and reserves `{schedule_name}:{slot_epoch}` idempotency keys before enqueuing so multi-worker clusters fire each cron tick at most once.

## Test Matrix

- Unit tests verifying per-task DI scope isolation (distinct scoped instances per task, deterministic cleanup on error), lane starvation prevention, heartbeat renewal, and graceful drain.

- Parametrized `CronExpression` boundary tests and multi-scheduler race tests proving a cron window enqueues exactly once across concurrent workers.

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
