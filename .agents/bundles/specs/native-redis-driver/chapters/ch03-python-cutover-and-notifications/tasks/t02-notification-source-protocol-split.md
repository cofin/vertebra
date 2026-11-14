---
type: Task
id: native-redis-driver:ch03-python-cutover-and-notifications:t02
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: Split NotificationSource into SyncNotificationSource and AsyncNotificationSource Across Capabilities, Postgres, and Oracle
description: Define @runtime_checkable SyncNotificationSource and AsyncNotificationSource in capabilities.py and ChannelBus, and split Postgres and Oracle notification sources.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-redis-driver:ch03-python-cutover-and-notifications:t01
files:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/driver/__init__.py
- src/py/vertebra/server/_channels.py
- src/py/vertebra/db/__init__.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/db/adapters/postgres/__init__.py
- src/py/vertebra/db/adapters/oracle/notify.py
- src/py/vertebra/db/adapters/oracle/driver.py
- src/py/vertebra/db/adapters/oracle/__init__.py
tests:
- src/py/tests/unit/db/test_native_db_drivers.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/db/test_postgres_native_notifications.py
- src/py/tests/unit/db/test_oracle_adapter.py
read_set:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/server/_channels.py
- src/py/vertebra/db/adapters/postgres/notify.py
- src/py/vertebra/db/adapters/oracle/notify.py
- src/py/vertebra/db/adapters/oracle/driver.py
- src/py/tests/unit/db/test_native_db_drivers.py
estimated_loc: 240
tags:
- capabilities
- notifications
- channels
- postgres
- pubsub
- oracle
- aq
sources:
- id: driver-caps
  resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
  title: NotificationSource capability protocol in capabilities.py
- id: driver-init
  resource: src/py/vertebra/db/driver/__init__.py
  title: Lazy capability exports in vertebra.db.driver
- id: channel-bus
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: ChannelBus notification pump
- id: db-init
  resource: src/py/vertebra/db/__init__.py
  title: Lazy top-level exports in vertebra.db
- id: pg-notify
  resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
  title: PostgreSQL notification source
- id: ora-notify
  resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
  title: Oracle AQ/TxEventQ notification source
- id: ora-drv
  resource: src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver
  title: Oracle driver notification_source factory
---

# Task `t02`: Split NotificationSource into SyncNotificationSource and AsyncNotificationSource Across Capabilities, Postgres, and Oracle

## Objective & Scope Boundary

Split `NotificationSource` into `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` across `capabilities.py`, `driver/__init__.py`, `db/__init__.py`, and `server/_channels.py`, and split `PostgresNotificationSource` + `OracleNotificationSource` (`SyncOracleListener`, `SyncOracleDriver.notification_source()`, `AsyncOracleDriver.notification_source()`) into symmetric `Sync*`/`Async*` pairs with zero aliases per [`../spec.md` §2](../spec.md) [^driver-caps] [^driver-init] [^channel-bus] [^db-init] [^pg-notify] [^ora-notify] [^ora-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `NotificationSource` protocol definition.
- `src/py/vertebra/server/_channels.py` — `ChannelBus` `notification_source` typing.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `PostgresNotificationSource` implementation.
- `src/py/vertebra/db/adapters/oracle/notify.py` — `OracleNotificationSource` and `OracleNotifyConfig`.
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver.notification_source`.
- `src/py/tests/unit/db/test_native_db_drivers.py` — Capability and `ChannelBus` unit tests.

## Contracts & Edge Cases

- Delete unprefixed `NotificationSource`, `PostgresNotificationSource`, and `OracleNotificationSource` with zero compatibility aliases (`PY-07`), updating `driver/__init__.py`, `db/__init__.py`, `postgres/__init__.py`, and `oracle/__init__.py` lazy exports (`PY-06`).
- Update `ChannelBus` (`src/py/vertebra/server/_channels.py`) to accept `AsyncNotificationSource | None`.
- `AsyncPostgresNotificationSource` preserves generation-fenced `AsyncPostgresListener` streaming, `build_notify_sql`, `build_emit_message_sql`, and `ChangeFeed` hydration; `SyncPostgresNotificationSource` implements `SyncNotificationSource` over a dedicated sync `LISTEN` session.
- `SyncOracleDriver.notification_source()` returns `SyncOracleNotificationSource` (`SyncOracleListener` over dedicated `SyncOracleClient` AQ dequeue); `AsyncOracleDriver.notification_source()` returns `AsyncOracleNotificationSource` (`AsyncOracleListener`).
- **Edge Case**: Pooled `pgbouncer` transaction-mode configs without `direct_dsn` raise `ImproperConfigurationError`; transient ORA disconnect codes (`3113`, `3114`, `3135`, `12541`) trigger bounded exponential reconnect backoff in both sync and async Oracle listeners.

## Test Matrix

- `test_native_db_drivers.py` verifies `SyncNotificationSource` and `AsyncNotificationSource` protocol conformance, zero `NotificationSource` alias (`ImportError`), and `ChannelBus.pump_notifications`.
- Parameterized Postgres (`test_pg_dataplane_and_datafusion.py`, `test_postgres_native_notifications.py`) and Oracle (`test_oracle_adapter.py`) tests verify sync and async `publish`, `subscribe`/`unsubscribe`, `listen()`, and `ChangeFeed` hydration.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_native_notifications.py src/py/tests/unit/db/test_oracle_adapter.py -q && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/server/_channels.py src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle
```

## Acceptance Criteria

- [ ] `SyncNotificationSource` and `AsyncNotificationSource` replace `NotificationSource` cleanly with zero aliases.
- [ ] Postgres and Oracle sync/async notification sources pass all unit and type checks.
- [ ] Zero regressions in the fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver` — Oracle driver notification_source factory.
