---
type: Task
id: "queues-tasks-and-channels:ch01-queue-store-capabilities-and-native-stores:t02"
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-store-capabilities-and-native-stores
title: "Implement SyncPostgresQueueStore and AsyncPostgresQueueStore in db/adapters/postgres/queue.py"
description: "Build SQL-backed SyncPostgresQueueStore and AsyncPostgresQueueStore with FOR UPDATE SKIP LOCKED, heartbeats, DLQ, cron leases, and pg_notify wakeups."
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
  - src/py/vertebra/db/adapters/postgres/queue.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/db/test_postgres_native_queries.py
read_set:
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
estimated_loc: 245
tags:
  - queues
  - postgres
  - skip-locked
sources:
  - id: pg-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: "PostgreSQL notification sources"
---

# Task `t02`: Implement SyncPostgresQueueStore and AsyncPostgresQueueStore in db/adapters/postgres/queue.py

## Objective & Scope Boundary

Implement `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` in `src/py/vertebra/db/adapters/postgres/queue.py` per [`../spec.md`](../spec.md) [^pg-drv] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/postgres/{driver,notify,__init__}.py` — `$1` extended query binds and `pg_notify` wakeups.

## Contracts & Edge Cases

- Execute single-roundtrip atomic claims via CTE + `FOR UPDATE OF q SKIP LOCKED` + `UPDATE ... RETURNING` (zero correlated scalar subqueries in `SELECT` lists).
- Enforce `(job_id, attempt)` fencing on `ack`, `nack`, and `heartbeat`; implement exponential retry backoff, dead-letter movement, uniqueness reservations, token-fenced cron/maintenance leases, and compact `pg_notify` wakeup emission (`< 8000` bytes) via `SyncPostgresNotificationSource` / `AsyncPostgresNotificationSource`.
- Import zero `psycopg`, `vertebra.framework`, or `vertebra.extensions` modules inside `vertebra.db`.

## Test Matrix

- Parametrized unit tests verifying SQL bind compilation (`$1..$N`), atomic `SKIP LOCKED` claim, stale-attempt fencing rejection, DLQ transition on retry exhaustion, cron lease contention, and `isinstance(..., SyncQueueStore | AsyncQueueStore)`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

## Acceptance Criteria

- [ ] `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `psycopg` or correlated scalar subqueries.
- [ ] Zero regressions in the chapter fast verification gate.

[^pg-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification sources.
