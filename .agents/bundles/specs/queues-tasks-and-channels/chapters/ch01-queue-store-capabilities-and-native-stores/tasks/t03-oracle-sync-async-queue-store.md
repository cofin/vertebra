---
type: Task
id: "queues-tasks-and-channels:ch01-queue-store-capabilities-and-native-stores:t03"
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-store-capabilities-and-native-stores
title: "Implement SyncOracleQueueStore and AsyncOracleQueueStore in db/adapters/oracle/queue.py"
description: "Build SQL-backed SyncOracleQueueStore and AsyncOracleQueueStore over vertebra._oracle with FOR UPDATE SKIP LOCKED and TxEventQ wakeups."
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
  - src/py/vertebra/db/adapters/oracle/queue.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/db/adapters/oracle/__init__.py
estimated_loc: 245
tags:
  - queues
  - oracle
  - skip-locked
sources:
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: "Oracle TxEventQ notification sources"
---

# Task `t03`: Implement SyncOracleQueueStore and AsyncOracleQueueStore in db/adapters/oracle/queue.py

## Objective & Scope Boundary

Implement `SyncOracleQueueStore` and `AsyncOracleQueueStore` in `src/py/vertebra/db/adapters/oracle/queue.py` per [`../spec.md`](../spec.md) [^ora-drv] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/oracle/{driver,notify,__init__}.py` — `:name` binds, `MERGE INTO`, and TxEventQ notifications.

## Contracts & Edge Cases

- Claim ready jobs using `FETCH FIRST :limit ROWS ONLY FOR UPDATE SKIP LOCKED` followed by fenced status/lease update inside the driver transaction; enforce `(job_id, attempt)` fencing on `ack`, `nack`, and `heartbeat`.
- Support `ORA-00955` idempotent `ensure_schema`, `MERGE INTO` uniqueness and cron leases, DLQ movement, and optional `SyncOracleNotificationSource` / `AsyncOracleNotificationSource` TxEventQ wakeups with adaptive `MIN(scheduled_at)` polling fallback when notifications are unconfigured.
- Import zero `oracledb` or framework modules.

## Test Matrix

- Parametrized unit tests covering `:name` bind shapes, `FOR UPDATE SKIP LOCKED` claim + fenced update, retry/DLQ transitions, cron lease reservation, TxEventQ wakeup vs polling fallback, and `SyncQueueStore`/`AsyncQueueStore` protocol checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

## Acceptance Criteria

- [ ] `SyncOracleQueueStore` and `AsyncOracleQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `oracledb` imports.
- [ ] Zero regressions in the chapter fast verification gate.

[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification sources.
