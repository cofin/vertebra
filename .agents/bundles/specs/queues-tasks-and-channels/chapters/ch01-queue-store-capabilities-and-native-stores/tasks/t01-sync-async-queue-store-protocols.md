---
type: Task
id: "queues-tasks-and-channels:ch01-queue-store-capabilities-and-native-stores:t01"
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-store-capabilities-and-native-stores
title: "Split QueueStore into @runtime_checkable SyncQueueStore and AsyncQueueStore"
description: "Replace unprefixed QueueStore in db/driver/capabilities.py with SyncQueueStore and AsyncQueueStore with zero aliases."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/db/__init__.py
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/driver/__init__.py
  - src/py/vertebra/db/__init__.py
estimated_loc: 165
tags:
  - queues
  - capabilities
  - protocols
sources:
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
    title: "Unprefixed async QueueStore capability protocol"
  - id: drv-init
    resource: src/py/vertebra/db/driver/__init__.py
    title: "Driver capability barrel exports"
---

# Task `t01`: Split QueueStore into @runtime_checkable SyncQueueStore and AsyncQueueStore

## Objective & Scope Boundary

Split `QueueStore` in `src/py/vertebra/db/driver/capabilities.py` into `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore` per [`../spec.md`](../spec.md) [^queue-cap] [^drv-init]. Out of scope: `SyncNotificationSource`/`AsyncNotificationSource` split (already completed in upstream `native-redis-driver:ch05`).

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `QueueEntry` and `QueueStore` definitions.
- `src/py/vertebra/db/driver/__init__.py` & `src/py/vertebra/db/__init__.py` — public barrel exports.

## Contracts & Edge Cases

- Define `@runtime_checkable class SyncQueueStore(Protocol)` and `@runtime_checkable class AsyncQueueStore(Protocol)` covering `ensure_schema`, `enqueue`, `enqueue_many`, `claim`, `ack`, `nack`, `heartbeat`, `reap_stale`, `promote_scheduled`, `cancel`, `get_entry`, `list_entries`, `next_due_delay`, `reserve_unique_key`, `release_unique_key`, `acquire_lease`, `release_lease`, and `close` / `aclose`.
- Remove unprefixed `QueueStore` completely with zero backward-compatibility aliases.

## Test Matrix

- Runtime `@runtime_checkable` `isinstance` checks for compliant and non-compliant sync/async store objects, plus assertion that `QueueStore` is absent from `vertebra.db.driver.capabilities`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

## Acceptance Criteria

- [ ] `SyncQueueStore` and `AsyncQueueStore` are exported from `vertebra.db.driver` and `vertebra.db` with zero unprefixed `QueueStore` alias.
- [ ] Zero regressions in the chapter fast verification gate.

[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Unprefixed async QueueStore capability protocol.
[^drv-init]: `src/py/vertebra/db/driver/__init__.py` — Driver capability barrel exports.
