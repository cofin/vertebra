---
type: Task
id: "queues-tasks-and-channels:ch03-worker-runner-scheduler-and-di:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch03-worker-runner-scheduler-and-di
title: "Implement WorkerRunner with Per-Task Container.scope() DI, Heartbeats & Lanes"
description: "Build WorkerRunner resolving task handler dependencies via framework/di.py::Container.scope() with periodic heartbeats and lane semaphores."
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
  - src/py/vertebra/framework/queues/worker.py
  - src/py/vertebra/framework/di.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/framework/di.py
  - src/py/vertebra/framework/_app.py
estimated_loc: 235
tags:
  - queues
  - worker
  - di
sources:
  - id: fw-di
    resource: src/py/vertebra/framework/di.py
    title: "Vertebra Container dependency injection and request/task scopes"
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: "QueueService task registration and dispatch"
---

# Task `t01`: Implement WorkerRunner with Per-Task Container.scope() DI, Heartbeats & Lanes

## Objective & Scope Boundary

Implement `WorkerRunner` (`src/py/vertebra/framework/queues/worker.py`) resolving task dependencies inside a per-attempt `Container.scope()` per [`../spec.md`](../spec.md) [^fw-di] [^queue-base].

## Bounded Read Manifest

- `src/py/vertebra/framework/di.py` — `Container` sync/async scope entry and parameter injection.
- `src/py/vertebra/framework/queues/base.py` — `QueueService` handler registry and result persistence.

## Contracts & Edge Cases

- `WorkerRunner` enters a fresh `async with container.scope():` (or sync `with container.sync_scope():` for sync workers) per claimed task attempt, injecting typed dependencies into task handlers and tearing scoped resources down deterministically on completion/failure.
- Maintain background lease heartbeats (`heartbeat`), stale-claim reaping (`reap_stale`), per-lane concurrency semaphores (`io`, `cpu`, `admin`), and graceful in-flight drain on shutdown.

## Test Matrix

- Unit tests verifying per-task DI scope isolation (distinct scoped instances per task, deterministic cleanup on error), lane starvation prevention, heartbeat renewal, and graceful drain.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py
```

## Acceptance Criteria

- [ ] `WorkerRunner` resolves handler dependencies via `Container.scope()` and drains in-flight tasks cleanly.
- [ ] Zero regressions in the chapter fast verification gate.

[^fw-di]: `src/py/vertebra/framework/di.py` — Vertebra Container dependency injection and request/task scopes.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService task registration and dispatch.
