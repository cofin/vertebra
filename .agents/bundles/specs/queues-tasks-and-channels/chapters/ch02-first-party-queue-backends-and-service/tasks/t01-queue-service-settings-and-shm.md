---
type: Task
id: "queues-tasks-and-channels:ch02-first-party-queue-backends-and-service:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch02-first-party-queue-backends-and-service
title: "Add QueueBackendSettings, Remove Container gc=False, and Harden SHMQueueBackend"
description: "Convert QueueService._build_backend to a settings-driven registry, drop gc=False on TaskEnvelope/DeadLetterEntry, and delete duplicate cron helpers."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-queue-store-capabilities-and-native-stores:t01"
files:
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/framework/queues/backends.py
  - src/py/vertebra/extensions/flask/queues.py
tests:
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/framework/queues/backends.py
  - src/py/vertebra/framework/queues/scheduler.py
  - src/py/vertebra/extensions/flask/queues.py
estimated_loc: 220
tags:
  - queues
  - shm
  - settings
sources:
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: "QueueService backend factory and TaskEnvelope definitions"
  - id: queue-shm
    resource: src/py/vertebra/framework/queues/backends.py
    title: "First-party MemoryQueueBackend and SHMQueueBackend"
  - id: flask-queues
    resource: src/py/vertebra/extensions/flask/queues.py
    title: "Flask TaskEnvelope with container gc=False"
---

# Task `t01`: Add QueueBackendSettings, Remove Container gc=False, and Harden SHMQueueBackend

## Objective & Scope Boundary

Introduce `QueueBackendSettings` + registry-driven `QueueService._build_backend`, drop `gc=False` from `TaskEnvelope`/`DeadLetterEntry`, delete duplicate `base.py::_cron_matches*`, and harden `SHMQueueBackend` per [`../spec.md`](../spec.md) [^queue-base] [^queue-shm] [^flask-queues].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/base.py` — `TaskEnvelope`, `DeadLetterEntry`, `QueueService._build_backend`, and duplicate `_cron_matches`.
- `src/py/vertebra/framework/queues/backends.py` — `MemoryQueueBackend` (test/explicit `kind='memory'` only) and `SHMQueueBackend`.
- `src/py/vertebra/extensions/flask/queues.py` — `TaskEnvelope` `gc=False` removal.

## Contracts & Edge Cases

- Remove `gc=False` from `TaskEnvelope` and `DeadLetterEntry` in `framework/queues/base.py` and `extensions/flask/queues.py`.
- Define `QueueBackendSettings(kind='postgres' | 'oracle' | 'redis' | 'valkey' | 'spanner' | 'shm' | 'memory', ...)` and `TaskResultRecord`; delete `base.py::_cron_matches` and `_cron_matches_field` so `scheduler.py::CronExpression` is the sole cron parser.
- Ensure `SHMQueueBackend` enforces visibility leases, dead-letter transitions, result storage, and futex/ulock wakeups.

## Test Matrix

- Parametrized `QueueService` construction tests across `QueueBackendSettings.kind` values, GC reference-cycle safety on `TaskEnvelope`, and `SHMQueueBackend` lease timeout + DLQ tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `QueueBackendSettings` drives `QueueService._build_backend`, duplicate cron functions in `base.py` are deleted, and `gc=False` is removed from `TaskEnvelope`/`DeadLetterEntry`.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService backend factory and TaskEnvelope definitions.
[^queue-shm]: `src/py/vertebra/framework/queues/backends.py` — First-party MemoryQueueBackend and SHMQueueBackend.
[^flask-queues]: `src/py/vertebra/extensions/flask/queues.py` — Flask TaskEnvelope with container gc=False.
