---
type: Task
id: queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t03
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-stores-and-first-party-backends
title: Auto-Wire PostgreSQLQueueBackend and OracleQueueBackend Over Native Queue Stores
description: Auto-construct native queue stores in PostgreSQLQueueBackend and OracleQueueBackend (replacing oracledb probe with vertebra-core[oracle]).
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- queues-tasks-and-channels:ch01-queue-stores-and-first-party-backends:t02
files:
- src/py/vertebra/framework/queues/postgres.py
- src/py/vertebra/framework/queues/oracle.py
tests:
- src/py/tests/unit/framework/test_distributed_queues.py
read_set:
- src/py/vertebra/framework/queues/postgres.py
- src/py/vertebra/framework/queues/oracle.py
- src/py/vertebra/db/adapters/postgres/queue.py
- src/py/vertebra/db/adapters/oracle/queue.py
estimated_loc: 225
tags:
- queues
- postgres
- oracle
sources:
- id: queue-pg
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQLQueueBackend requiring manual executor injection
- id: queue-ora
  resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
  title: OracleQueueBackend probing oracledb
---

# Task `t03`: Auto-Wire PostgreSQLQueueBackend and OracleQueueBackend Over Native Queue Stores

## Objective & Scope Boundary

Wire `PostgreSQLQueueBackend` and `OracleQueueBackend` to build `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore` automatically from configured drivers/DSNs and fix `OracleQueueBackend`'s install hint per [`../spec.md`](../spec.md) [^queue-pg] [^queue-ora].

## Bounded Read Manifest

- `src/py/vertebra/framework/queues/{postgres,oracle}.py` — current fail-loud backends.
- `src/py/vertebra/db/adapters/{postgres,oracle}/queue.py` — native sync/async queue stores.

## Contracts & Edge Cases

- `OracleQueueBackend` probes `ensure_module('vertebra._oracle', package='vertebra-core[oracle]', extra='oracle')` (deleting all `oracledb` probes and hints).
- When `store=` / `executor=` is omitted and a `postgresql://` or `oracle://` driver/DSN is configured, `PostgreSQLQueueBackend` and `OracleQueueBackend` automatically construct `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore`.
- Expose synchronous `enqueue_sync` (via `SyncQueueStore`) alongside async `enqueue` / `dequeue` / `ack` / `nack` (`AsyncQueueStore` + `AsyncNotificationSource` wakeups).

## Test Matrix

- Unit tests asserting `OracleQueueBackend` raises `MissingDependencyError` naming `vertebra-core[oracle]` when `vertebra._oracle` is absent, zero `oracledb`/`psycopg` strings exist in `framework/queues/`, and auto-constructed stores execute full enqueue/claim/ack/nack/DLQ lifecycles.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py
```

## Acceptance Criteria

- [ ] `PostgreSQLQueueBackend` and `OracleQueueBackend` auto-bind native `Sync*QueueStore`/`Async*QueueStore` from configured drivers and `git grep oracledb src/py/vertebra/framework` returns nothing.
- [ ] Zero regressions in the fast verification gate.

[^queue-pg]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQLQueueBackend requiring manual executor injection.
[^queue-ora]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — OracleQueueBackend probing oracledb.
