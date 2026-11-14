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
plan_commit: 104fd6fa
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
- src/py/tests/integration/db/test_oracle_native.py
read_set:
- src/py/vertebra/db/driver/capabilities.py
- src/py/vertebra/db/driver/__init__.py
- src/py/vertebra/server/_channels.py
- src/py/tests/unit/db/test_native_db_drivers.py
- src/py/vertebra/db/__init__.py
- src/py/vertebra/db/adapters/postgres/notify.py
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

Split `NotificationSource` into `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` across `capabilities.py`, `driver/__init__.py`, and `server/_channels.py` per [`../spec.md` §2](../spec.md) [^driver-caps] [^driver-init] [^channel-bus].

Update `vertebra/db/__init__.py` (`SyncNotificationSource`, `AsyncNotificationSource`) and split `PostgresNotificationSource` into `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` per [`../spec.md` §2](../spec.md) [^db-init] [^pg-notify].

Implement `SyncOracleListener`, `SyncOracleNotificationSource(SyncNotificationSource)`, and `AsyncOracleNotificationSource(AsyncNotificationSource)` across `oracle/{notify,driver,__init__}.py` per [`../spec.md` §2](../spec.md) [^ora-notify] [^ora-drv].

## Bounded Read Manifest

- `src/py/vertebra/db/driver/capabilities.py` — `NotificationSource` protocol definition.
- `src/py/vertebra/db/driver/__init__.py` — Lazy exports for `capabilities`.
- `src/py/vertebra/server/_channels.py` — `ChannelBus` `notification_source` typing.
- `src/py/tests/unit/db/test_native_db_drivers.py` — Capability and `ChannelBus` unit tests.

- `src/py/vertebra/db/__init__.py` — Top-level lazy capability exports.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `PostgresNotificationSource` implementation.
- `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy PostgreSQL package exports.
- `src/py/tests/unit/db/test_postgres_native_notifications.py` — PostgreSQL notification streaming tests.

- `src/py/vertebra/db/adapters/oracle/notify.py` — `OracleNotificationSource` and `OracleNotifyConfig`.
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver.notification_source`.
- `src/py/vertebra/db/adapters/oracle/__init__.py` — Lazy Oracle package exports.
- `src/py/tests/unit/db/test_oracle_adapter.py` — Oracle notification unit tests.

## Contracts & Edge Cases

- Delete `NotificationSource` with zero aliases (`PY-07`); export `SyncNotificationSource` and `AsyncNotificationSource` lazily in `driver/__init__.py` (`PY-06`).
- Update `ChannelBus` in `src/py/vertebra/server/_channels.py` to accept `AsyncNotificationSource | None`.
- **Edge Case**: `isinstance(obj, SyncNotificationSource)` and `isinstance(obj, AsyncNotificationSource)` validate protocol methods at runtime.

- `AsyncPostgresNotificationSource` preserves generation-fenced `AsyncPostgresListener` streaming, `build_notify_sql`, `build_emit_message_sql`, and `ChangeFeed` hydration.
- `SyncPostgresNotificationSource` implements `SyncNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen() -> Iterator[NotificationEvent]`, `close()`, `with` context manager) over a dedicated direct/session `SyncPostgresListener` / sync `LISTEN` session with zero aliases.
- **Edge Case**: Both sync and async sources reject transaction-pooled `pgbouncer` configs lacking `direct_dsn` with `ImproperConfigurationError`.

- `SyncOracleDriver.notification_source()` returns `SyncOracleNotificationSource` (using `SyncOracleListener` over dedicated `SyncOracleClient` AQ dequeue); `AsyncOracleDriver.notification_source()` returns `AsyncOracleNotificationSource` (over `AsyncOracleListener`).
- Export `SyncOracleNotificationSource`, `AsyncOracleNotificationSource`, `SyncOracleListener`, and `OracleNotifyConfig` lazily in `oracle/__init__.py`; delete unprefixed `OracleNotificationSource` (`PY-07`).
- **Edge Case**: Transient ORA disconnect codes (`3113`, `3114`, `3135`, `12541`, etc.) apply bounded exponential reconnect backoff in both sync and async listeners.

## Test Matrix

- Update `test_native_db_drivers.py` to verify both `SyncNotificationSource` and `AsyncNotificationSource` protocol conformance and `ChannelBus.pump_notifications`.

- Parameterized sync and async tests in `test_pg_dataplane_and_datafusion.py` and `test_postgres_native_notifications.py` verify `isinstance(..., SyncNotificationSource | AsyncNotificationSource)`, `publish`, `subscribe`/`unsubscribe`, `listen()`, and `ChangeFeed` spill hydration.

- Update `test_oracle_adapter.py` and `test_oracle_native.py` to test both `SyncOracleNotificationSource` and `AsyncOracleNotificationSource`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -q -k 'Notification or Listener' && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle
```

## Acceptance Criteria

- [ ] `SyncNotificationSource` and `AsyncNotificationSource` replace `NotificationSource` cleanly.
- [ ] `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` pass all unit and type checks with zero aliases.
- [ ] Oracle sync and async notification sources satisfy `SyncNotificationSource` and `AsyncNotificationSource` with zero aliases.
- [ ] Zero regressions in the fast verification gate.

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver` — Oracle driver notification_source factory.
