---
type: Task
id: "queues-tasks-and-channels:ch04-litestar-queues-integration:t02"
flow_id: queues-tasks-and-channels
chapter_id: ch04-litestar-queues-integration
title: "Implement Engine-Neutral StoreQueueBackend(BaseQueueBackend) Over AsyncQueueStore"
description: "Build slotted StoreQueueBackend and StoreQueueBackendConfig in extensions/litestar/queues/backend.py wrapping any AsyncQueueStore."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "ch01-queue-store-capabilities-and-native-stores:t02"
  - "ch01-queue-store-capabilities-and-native-stores:t03"
  - t01
files:
  - src/py/vertebra/extensions/litestar/queues/backend.py
tests:
  - src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
read_set:
  - src/py/vertebra/extensions/litestar/queues/backend.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/postgres/queue.py
  - src/py/vertebra/db/adapters/oracle/queue.py
estimated_loc: 245
tags:
  - litestar-queues
  - queues
  - store-backend
sources:
  - id: lq-backend
    resource: src/py/vertebra/extensions/litestar/queues/backend.py
    title: "litestar-queues backend adapter module"
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py
    title: "AsyncQueueStore and AsyncNotificationSource protocols"
---

# Task `t02`: Implement Engine-Neutral StoreQueueBackend(BaseQueueBackend) Over AsyncQueueStore

## Objective & Scope Boundary

Implement engine-neutral slotted `StoreQueueBackend(BaseQueueBackend)` + `StoreQueueBackendConfig` (`@queue_backend('vertebra_store')`) in `extensions/litestar/queues/backend.py` per [`../spec.md`](../spec.md) [^lq-backend] [^queue-cap].

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/queues/backend.py` — remove broken `from litestar_queues.records import QueuedTaskRecord` import and engine-specific code.
- `src/py/vertebra/db/driver/capabilities.py` — `AsyncQueueStore` and `AsyncNotificationSource`.

## Contracts & Edge Cases

- `StoreQueueBackend(BaseQueueBackend)` declares `__slots__`, imports zero database engine packages, and adapts any `AsyncQueueStore` (`AsyncPostgresQueueStore`, `AsyncOracleQueueStore`, `AsyncSpannerQueueStore`) + optional `AsyncNotificationSource` to `litestar-queues>=0.14.0`.
- Implement claim fencing (`retry_count == expected_retry_count`), `touch_heartbeats`, `requeue_stale_running`, `cleanup_terminal`, `list_tasks`, `get_statistics`, maintenance leases, uniqueness reservations, and external Cloud Tasks / Cloud Run dispatch reservation hooks.

## Test Matrix

- End-to-end `litestar-queues` worker execution tests over `StoreQueueBackend` wrapping `AsyncPostgresQueueStore` and `AsyncOracleQueueStore` test harnesses, plus AST check confirming zero vendor imports in `backend.py`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/extensions/litestar/queues pyproject.toml && uv run --no-sync mypy src/py/vertebra/extensions/litestar/queues && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_ecosystem_queues.py
```

## Acceptance Criteria

- [ ] `StoreQueueBackend` passes `litestar-queues>=0.14.0` lifecycle, fencing, maintenance, and dispatch reservation tests with zero engine imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^lq-backend]: `src/py/vertebra/extensions/litestar/queues/backend.py` — litestar-queues backend adapter module.
[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py` — AsyncQueueStore and AsyncNotificationSource protocols.
