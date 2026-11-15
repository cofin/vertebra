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
plan_commit: 473e1f2e
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
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/vertebra/db/adapters/oracle/_sync.py
  - src/py/vertebra/db/adapters/oracle/_async.py
  - src/py/vertebra/db/adapters/oracle/notify.py
estimated_loc: 245
tags:
  - queues
  - postgres
  - skip-locked
  - oracle
sources:
  - id: pg-drv
    resource: src/py/vertebra/db/adapters/postgres/_sync.py
    title: Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py
    title: PostgreSQL notification sources
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/_sync.py
    title: Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py
    title: Oracle TxEventQ notification sources
verification_strategy: regression_tdd
---

# Task `t02`: Implement Sync/Async PostgresQueueStore and OracleQueueStore

## Objective & Scope Boundary

Implement `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` in `src/py/vertebra/db/adapters/postgres/queue.py` and `SyncOracleQueueStore`/`AsyncOracleQueueStore` in `src/py/vertebra/db/adapters/oracle/queue.py` (with public re-exports in each adapter `__init__.py`) per [`../spec.md`](../spec.md) [^pg-drv] [^pg-notify] [^ora-drv] [^ora-notify].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `SyncQueueStore` and `AsyncQueueStore` protocols.
- `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — `$1` extended query binds and transaction semantics.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `pg_notify` wakeup channels.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — adapter public exports (mirrored in `oracle/__init__.py`).
- `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — `:name` binds, `MERGE INTO`, and transactions.
- `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification sources.

## Contracts & Edge Cases

- Implement `SyncPostgresQueueStore`/`AsyncPostgresQueueStore`: single-roundtrip atomic claims via CTE + `FOR UPDATE OF q SKIP LOCKED` + `UPDATE ... RETURNING` (zero correlated scalar subqueries in `SELECT` lists), `(job_id, attempt)` fencing on `ack`/`nack`/`heartbeat`, exponential retry backoff, DLQ movement, uniqueness reservations, token-fenced cron/maintenance leases, compact `< 8000`B `pg_notify` wakeup emission via `SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, and zero `psycopg`, `vertebra.framework`, or `vertebra.extensions` imports.
- Implement `SyncOracleQueueStore`/`AsyncOracleQueueStore`: claim ready jobs via `FETCH FIRST :limit ROWS ONLY FOR UPDATE SKIP LOCKED` + fenced status/lease update inside transaction, `(job_id, attempt)` fencing, `ORA-00955`-idempotent `ensure_schema`, `MERGE INTO` uniqueness and cron leases, DLQ movement, optional TxEventQ wakeups via `SyncOracleNotificationSource`/`AsyncOracleNotificationSource` with adaptive `MIN(scheduled_at)` polling fallback when unconfigured, and zero `oracledb` or framework imports.

## Test Matrix

- Parametrized PostgreSQL unit tests (`$1..$N` SQL binds, atomic `SKIP LOCKED` claim, stale-attempt rejection, DLQ transition, cron lease contention, `SyncQueueStore`/`AsyncQueueStore` conformance).
- Parametrized Oracle unit tests (`:name` binds, `FOR UPDATE SKIP LOCKED` claim + fenced update, retry/DLQ transitions, cron lease reservation, TxEventQ wakeup vs polling fallback, `SyncQueueStore`/`AsyncQueueStore` conformance).

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

## Acceptance Criteria

- [ ] `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `psycopg` or correlated scalar subqueries.
- [ ] `SyncOracleQueueStore` and `AsyncOracleQueueStore` satisfy `SyncQueueStore` / `AsyncQueueStore` with zero `oracledb` imports.
- [ ] Zero regressions in the fast verification gate.

[^pg-drv]: `src/py/vertebra/db/adapters/postgres/_sync.py` and `src/py/vertebra/db/adapters/postgres/_async.py` — Native vertebra._postgres SyncPostgreSQLDriver and AsyncPostgreSQLDriver.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py` — PostgreSQL notification sources.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/_sync.py` and `src/py/vertebra/db/adapters/oracle/_async.py` — Native vertebra._oracle SyncOracleDriver and AsyncOracleDriver.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py` — Oracle TxEventQ notification sources.
