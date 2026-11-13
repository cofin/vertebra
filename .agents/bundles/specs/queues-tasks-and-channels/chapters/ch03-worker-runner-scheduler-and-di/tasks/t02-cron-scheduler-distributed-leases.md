---
type: Task
id: "queues-tasks-and-channels:ch03-worker-runner-scheduler-and-di:t02"
flow_id: queues-tasks-and-channels
chapter_id: ch03-worker-runner-scheduler-and-di
title: "Harden CronScheduler with Slot-Keyed Idempotent Distributed Leases"
description: "Ensure CronExpression in scheduler.py is the sole cron parser and prevent duplicate multi-worker cron enqueues via lease/uniqueness keys."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - t01
files:
  - src/py/vertebra/framework/queues/scheduler.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/scheduler.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/db/driver/capabilities.py
estimated_loc: 175
tags:
  - queues
  - cron
  - scheduler
sources:
  - id: queue-sched
    resource: src/py/vertebra/framework/queues/scheduler.py
    title: "CronExpression parser and CronScheduler"
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "acquire_lease and reserve_unique_key store contracts"
---

# Task `t02`: Harden CronScheduler with Slot-Keyed Idempotent Distributed Leases

## Objective & Scope Boundary

Upgrade `CronScheduler` (`src/py/vertebra/framework/queues/scheduler.py`) with distributed lease coordination and slot-keyed idempotency per [`../spec.md`](../spec.md) [^queue-sched] [^queue-cap].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/scheduler.py` — `CronExpression` and `CronScheduler`.
- `src/py/vertebra/db/driver/capabilities.py` — `acquire_lease` and `reserve_unique_key`.

## Contracts & Edge Cases

- `CronExpression` serves as the single cron parser across Vertebra queues.
- `CronScheduler` acquires a token-fenced scheduler lease and reserves `{schedule_name}:{slot_epoch}` idempotency keys before enqueuing so multi-worker clusters fire each cron tick at most once.

## Test Matrix

- Parametrized `CronExpression` boundary tests and multi-scheduler race tests proving a cron window enqueues exactly once across concurrent workers.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/framework/di.py src/py/vertebra/extensions/starlette/queues.py src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_starlette_plugin.py src/py/tests/unit/extensions/test_flask_plugin.py
```

## Acceptance Criteria

- [ ] `CronScheduler` guarantees at-most-once per-slot cron enqueue across concurrent workers.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-sched]: `src/py/vertebra/framework/queues/scheduler.py` — CronExpression parser and CronScheduler.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — acquire_lease and reserve_unique_key store contracts.
