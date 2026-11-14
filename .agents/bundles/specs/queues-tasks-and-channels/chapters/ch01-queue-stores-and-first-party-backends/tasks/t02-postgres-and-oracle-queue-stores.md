---
type: Task
id: queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t02
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-stores-and-first-party-backends
title: Implement Sync/Async PostgresQueueStore and OracleQueueStore
description: Build Sync/AsyncPostgresQueueStore (FOR UPDATE OF q SKIP LOCKED, pg_notify, zero correlated subqueries) and Sync/AsyncOracleQueueStore (FOR UPDATE SKIP LOCKED, TxEventQ).
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t01
files:
- src/py/vertebra/db/adapters/postgres/queue.py
- src/py/vertebra/db/adapters/postgres/__init__.py
- src/py/vertebra/db/adapters/oracle/queue.py
- src/py/vertebra/db/adapters/oracle/__init__.py
tests:
- src/py/tests/unit/db/test_postgres_native_queries.py
- src/py/tests/unit/db/test_oracle_adapter.py
read_set:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/adapters/postgres/driver.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/db/adapters/postgres/__init__.py
- src/py/vertebra/db/adapters/oracle/driver.py
- src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 245
tags:
- queues
- postgres
- skip-locked
- oracle
sources:
- id: pg-drv
  resource: src/py/vertebra/db/adapters/postgres/driver.py
  title: Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver
- id: pg-notify
  resource: src/py/vertebra/db/adapters/postgres/notify.py
  title: PostgreSQL notification sources
- id: ora-drv
  resource: src/py/vertebra/db/adapters/oracle/driver.py
  title: Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver
- id: ora-notify
  resource: src/py/vertebra/db/adapters/oracle/notify.py
  title: Oracle TxEventQ notification sources
---

# Task `t02`: Implement Sync/Async PostgresQueueStore and OracleQueueStore

## Objective & Scope Boundary

Implement `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` in `src/py/vertebra/db/adapters/postgres/queue.py` per [`../spec.md`](../spec.md) [^pg-drv] [^pg-notify].

Implement `SyncOracleQueueStore` and `AsyncOracleQueueStore` in `src/py/vertebra/db/adapters/oracle/queue.py` per [`../spec.md`](../spec.md) [^ora-drv] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/postgres/{driver,notify,__init__}.py` — `$1` extended query binds and `pg_notify` wakeups.

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/oracle/{driver,notify,__init__}.py` — `:name` binds, `MERGE INTO`, and TxEventQ notifications.

## Contracts & Edge Cases

- Execute single-roundtrip atomic claims via CTE + `FOR UPDATE OF q SKIP LOCKED` + `UPDATE ... RETURNING` (zero correlated scalar subqueries in `SELECT` lists).
- Enforce `(job_id, attempt)` fencing on `ack`, `nack`, and `heartbeat`; implement exponential retry backoff, dead-letter movement, uniqueness reservations, token-fenced cron/maintenance leases, and compact `pg_notify` wakeup emission (`< 8000` bytes) via `SyncPostgresNotificationSource` / `AsyncPostgresNotificationSource`.
- Import zero `psycopg`, `vertebra.framework`, or `vertebra.extensions` modules inside `vertebra.db`.

- Claim ready jobs using `FETCH FIRST :limit ROWS ONLY FOR UPDATE SKIP LOCKED` followed by fenced status/lease update inside the driver transaction; enforce `(job_id, attempt)` fencing on `ack`, `nack`, and `heartbeat`.
- Support `ORA-00955` idempotent `ensure_schema`, `MERGE INTO` uniqueness and cron leases, DLQ movement, and optional `SyncOracleNotificationSource` / `AsyncOracleNotificationSource` TxEventQ wakeups with adaptive `MIN(scheduled_at)` polling fallback when notifications are unconfigured.
- Import zero `oracledb` or framework modules.

## Test Matrix

- Parametrized unit tests verifying SQL bind compilation (`$1..$N`), atomic `SKIP LOCKED` claim, stale-attempt fencing rejection, DLQ transition on retry exhaustion, cron lease contention, and `isinstance(..., SyncQueueStore | AsyncQueueStore)`.

- Parametrized unit tests covering `:name` bind shapes, `FOR UPDATE SKIP LOCKED` claim + fenced update, retry/DLQ transitions, cron lease reservation, TxEventQ wakeup vs polling fallback, and `SyncQueueStore`/`AsyncQueueStore` protocol checks.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

## Acceptance Criteria

- [ ] `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `psycopg` or correlated scalar subqueries.
- [ ] `SyncOracleQueueStore` and `AsyncOracleQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `oracledb` imports.
- [ ] Zero regressions in the fast verification gate.

[^pg-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification sources.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification sources.
