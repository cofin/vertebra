---
type: Task
id: "native-redis-driver:ch05:t02"
flow_id: native-redis-driver
chapter_id: ch05-notification-source-split
title: "Split PostgresNotificationSource into SyncPostgresNotificationSource and AsyncPostgresNotificationSource"
description: "Update db/__init__.py and split postgres/notify.py into SyncPostgresNotificationSource and AsyncPostgresNotificationSource."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-redis-driver:ch05:t01"
files:
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
tests:
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
  - src/py/tests/unit/db/test_postgres_native_notifications.py
read_set:
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/__init__.py
  - src/py/tests/unit/db/test_postgres_native_notifications.py
estimated_loc: 235
tags:
  - postgres
  - notifications
  - pubsub
sources:
  - id: db-init
    resource: src/py/vertebra/db/__init__.py
    title: "Lazy top-level exports in vertebra.db"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: "PostgreSQL notification source"
---

# Task `t02`: Split PostgresNotificationSource into SyncPostgresNotificationSource and AsyncPostgresNotificationSource

## Objective & Scope Boundary

Update `vertebra/db/__init__.py` (`SyncNotificationSource`, `AsyncNotificationSource`) and split `PostgresNotificationSource` into `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` per [`../spec.md` §2](../spec.md) [^db-init] [^pg-notify].

## Bounded Read Manifest

- `src/py/vertebra/db/__init__.py` — Top-level lazy capability exports.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `PostgresNotificationSource` implementation.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy PostgreSQL package exports.
- `src/py/tests/unit/db/test_postgres_native_notifications.py` — PostgreSQL notification streaming tests.

## Contracts & Edge Cases

- `AsyncPostgresNotificationSource` preserves generation-fenced `AsyncPostgresListener` streaming, `build_notify_sql`, `build_emit_message_sql`, and `ChangeFeed` hydration.
- `SyncPostgresNotificationSource` implements `SyncNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen() -> Iterator[NotificationEvent]`, `close()`, `with` context manager) over a dedicated direct/session `SyncPostgresListener` / sync `LISTEN` session with zero aliases.
- **Edge Case**: Both sync and async sources reject transaction-pooled `pgbouncer` configs lacking `direct_dsn` with `ImproperConfigurationError`.

## Test Matrix

- Parameterized sync and async tests in `test_pg_dataplane_and_datafusion.py` and `test_postgres_native_notifications.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, `publish`, `subscribe`/`unsubscribe`, `listen()`, and `ChangeFeed` spill hydration.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_native_notifications.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
```

## Acceptance Criteria

- [ ] `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` pass all unit and type checks with zero aliases.
- [ ] Zero regressions in the chapter fast verification gate.

[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
