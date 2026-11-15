---
type: Task
id: queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t01
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-stores-and-first-party-backends
title: Split Sync/AsyncQueueStore Protocols and Add QueueBackendSettings & SHM Fixes
description: Split QueueStore into SyncQueueStore and AsyncQueueStore in capabilities.py, add QueueBackendSettings/TaskResultRecord, drop gc=False on TaskEnvelope/DeadLetterEntry, and remove duplicate cron helpers.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 473e1f2e
depends_on: []
files:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/framework/queues/backends.py
  - src/py/vertebra/extensions/flask/queues.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/framework/test_distributed_queues.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/framework/queues/base.py
  - src/py/vertebra/framework/queues/backends.py
  - src/py/vertebra/framework/queues/scheduler.py
  - src/py/vertebra/extensions/flask/queues.py
estimated_loc: 220
tags:
  - queues
  - capabilities
  - protocols
  - shm
  - settings
sources:
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
    title: Unprefixed async QueueStore capability protocol
  - id: drv-init
    resource: src/py/vertebra/db/driver/__init__.py
    title: Driver capability barrel exports
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: QueueService backend factory and TaskEnvelope definitions
  - id: queue-shm
    resource: src/py/vertebra/framework/queues/backends.py
    title: Queue backend re-export barrel receiving the new first-party SHMQueueBackend
  - id: flask-queues
    resource: src/py/vertebra/extensions/flask/queues.py
    title: Flask TaskEnvelope with container gc=False
verification_strategy: regression_tdd
---

# Task `t01`: Split Sync/AsyncQueueStore Protocols and Add QueueBackendSettings & SHM Fixes

## Objective & Scope Boundary

Split `QueueStore` in `src/py/vertebra/db/driver/capabilities.py` into `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore`, introduce `QueueBackendSettings` + registry-driven `QueueService._build_backend`, drop `gc=False` from `TaskEnvelope`/`DeadLetterEntry` (and fix mutable `kwargs = {}` default in `flask/queues.py`), delete duplicate `src/py/vertebra/framework/queues/base.py::_cron_matches*`, and create the first-party `SHMQueueBackend` per [`../spec.md`](../spec.md) [^queue-cap] [^drv-init] [^queue-base] [^queue-shm] [^flask-queues]. Out of scope: `SyncNotificationSource`/`AsyncNotificationSource` split (completed in `native-redis-driver:ch03`).

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `ClaimedJob` (renamed `QueueEntry`) and `QueueStore` (`complete`/`fail` renamed `ack`/`nack`) definitions.
- `src/py/vertebra/db/driver/__init__.py` — public driver barrel exports (mirrored in `src/py/vertebra/db/__init__.py`).
- `src/py/vertebra/framework/queues/base.py` — `TaskEnvelope`, `DeadLetterEntry`, `QueueService._build_backend`, and duplicate `_cron_matches`.
- `src/py/vertebra/framework/queues/backends.py` — re-export barrel (`MemoryQueueBackend` lives in `src/py/vertebra/framework/queues/base.py` and stays test/explicit `kind='memory'` only); receives the new first-party `SHMQueueBackend`.
- `src/py/vertebra/framework/queues/scheduler.py` — canonical `CronExpression` parser replacing duplicate helpers in `src/py/vertebra/framework/queues/base.py`.
- `src/py/vertebra/extensions/flask/queues.py` — `TaskEnvelope` `gc=False` and mutable `kwargs` default removal.

## Contracts & Edge Cases

- Define `@runtime_checkable class SyncQueueStore(Protocol)` and `@runtime_checkable class AsyncQueueStore(Protocol)` covering `ensure_schema`, `enqueue`, `enqueue_many`, `claim`, `ack`, `nack`, `heartbeat`, `reap_stale`, `promote_scheduled`, `cancel`, `get_entry`, `list_entries`, `next_due_delay`, `reserve_unique_key`, `release_unique_key`, `acquire_lease`, `release_lease`, and `close` / `aclose`; remove unprefixed `QueueStore` completely from `capabilities.py`, `db/driver/__init__.py`, and `db/__init__.py` with zero aliases.
- Remove `gc=False` from `TaskEnvelope` and `DeadLetterEntry` in `framework/queues/base.py` and from `TaskEnvelope` in `extensions/flask/queues.py` (also replacing mutable `kwargs: dict[str, Any] = {}` with `msgspec.field(default_factory=dict)`).
- Define `QueueBackendSettings(kind='postgres' | 'oracle' | 'redis' | 'valkey' | 'spanner' | 'shm' | 'memory', ...)` and `TaskResultRecord`; delete `src/py/vertebra/framework/queues/base.py::_cron_matches` and `_cron_matches_field` so `src/py/vertebra/framework/queues/scheduler.py::CronExpression` is the sole cron parser; implement first-party `SHMQueueBackend` enforcing visibility leases, dead-letter transitions, result storage, and futex/ulock wakeups.

## Test Matrix

- Runtime `@runtime_checkable` `isinstance` checks for compliant and non-compliant sync/async store objects plus assertion that unprefixed `QueueStore` is absent from `vertebra.db.driver.capabilities` and `vertebra.db`.
- Parametrized `QueueService` construction tests across `QueueBackendSettings.kind` values, GC reference-cycle safety on `TaskEnvelope`/`DeadLetterEntry`, and `SHMQueueBackend` lease timeout + DLQ tests.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/__init__.py src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/__init__.py src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `SyncQueueStore` and `AsyncQueueStore` are exported from `vertebra.db.driver` and `vertebra.db` with zero unprefixed `QueueStore` alias.
- [ ] `QueueBackendSettings` drives `QueueService._build_backend`, duplicate cron functions in `base.py` are deleted, and `gc=False` is removed from `TaskEnvelope`/`DeadLetterEntry`.
- [ ] Zero regressions in the fast verification gate.

[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Unprefixed async QueueStore capability protocol.
[^drv-init]: `src/py/vertebra/db/driver/__init__.py` — Driver capability barrel exports.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — QueueService backend factory and TaskEnvelope definitions.
[^queue-shm]: `src/py/vertebra/framework/queues/backends.py` — Queue backend re-export barrel receiving the new first-party SHMQueueBackend.
[^flask-queues]: `src/py/vertebra/extensions/flask/queues.py` — Flask TaskEnvelope with container gc=False.
