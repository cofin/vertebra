---
type: Spec
flow_id: native-redis-driver
chapter_id: ch05-notification-source-split
title: "Chapter 05: SyncNotificationSource / AsyncNotificationSource Split Across Capabilities, PostgreSQL, and Oracle"
description: "Split NotificationSource into @runtime_checkable SyncNotificationSource and AsyncNotificationSource with zero aliases, split PostgresNotificationSource and OracleNotificationSource into Sync*/Async* pairs, and update ChannelBus and existing tests."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - notifications
  - capabilities
  - postgres
  - oracle
  - pubsub
sources:
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: "NotificationSource capability protocol in capabilities.py"
  - id: driver-init
    resource: src/py/vertebra/db/driver/__init__.py
    title: "Lazy capability exports in vertebra.db.driver"
  - id: db-init
    resource: src/py/vertebra/db/__init__.py
    title: "Lazy top-level exports in vertebra.db"
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: "ChannelBus notification pump"
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: "PostgreSQL notification source"
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: "Oracle AQ/TxEventQ notification source"
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver
    title: "Oracle driver notification_source factory"
---

# Chapter 05: SyncNotificationSource / AsyncNotificationSource Split Across Capabilities, PostgreSQL, and Oracle

<!-- truth: start -->
- `src/py/vertebra/db/driver/capabilities.py` splits `NotificationSource` into `@runtime_checkable SyncNotificationSource` (`listen() -> Iterator[NotificationEvent]`) and `@runtime_checkable AsyncNotificationSource` (`listen() -> AsyncIterator[NotificationEvent]`) with leading `Sync*`/`Async*` prefixes and zero compatibility aliases (`PY-07`) [^driver-caps] [^driver-init] [^db-init].
- `ChannelBus` (`src/py/vertebra/server/_channels.py`) types `notification_source` as `AsyncNotificationSource | None` [^channel-bus].
- `PostgresNotificationSource` splits into `SyncPostgresNotificationSource` (over `SyncPostgresListener` / dedicated direct-or-session sync `LISTEN` connection) and `AsyncPostgresNotificationSource` (over `AsyncPostgresListener`) [^pg-notify].
- `OracleNotificationSource` splits into `SyncOracleNotificationSource` (`SyncOracleListener` over dedicated `SyncOracleClient` AQ dequeue) and `AsyncOracleNotificationSource` (`AsyncOracleListener`), with `SyncOracleDriver.notification_source()` and `AsyncOracleDriver.notification_source()` returning their respective mode [^ora-notify] [^ora-drv].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Hoisting the `NotificationSource` -> `SyncNotificationSource` / `AsyncNotificationSource` capability split into `native-redis-driver` (User Decision `A3`) keeps the cross-flow DAG strictly acyclic (`native-postgres-driver -> native-redis-driver -> queues-tasks-and-channels`) and ensures every database/cache notification transport ships symmetric sync (Django, Flask, CLI, synchronous workers) and async (`ChannelBus`, ASGI) classes before `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` land in `ch06` [^driver-caps] [^channel-bus].

**Non-Goals:** Redis and Valkey notification sources (`ch06:t03`–`t04`); Distributed channel fan-out extensions (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Capabilities & bus | `db/driver/{capabilities,__init__}.py`, `server/_channels.py` [^driver-caps] [^channel-bus] | `SyncNotificationSource` & `AsyncNotificationSource` (`@runtime_checkable`) |
| PostgreSQL notify | `db/__init__.py`, `db/adapters/postgres/{notify,__init__}.py` [^db-init] [^pg-notify] | `SyncPostgresNotificationSource` & `AsyncPostgresNotificationSource` |
| Oracle notify | `db/adapters/oracle/{notify,driver,__init__}.py` [^ora-notify] [^ora-drv] | `SyncOracleListener`, `SyncOracleNotificationSource`, `AsyncOracleNotificationSource` |

**Knowledge Anchors:** [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [PostgreSQL Native Driver](../../../../knowledge/domains/data/postgres-native-driver.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R13a**: Replace `NotificationSource` with `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` in `capabilities.py`, `driver/__init__.py`, and `server/_channels.py` (`t01`).
- **R13b**: Update `vertebra/db/__init__.py` exports and split `PostgresNotificationSource` into `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` with zero aliases (`t02`).
- **R13c**: Split `OracleNotificationSource` into `SyncOracleNotificationSource` (`SyncOracleListener`) and `AsyncOracleNotificationSource` (`AsyncOracleListener`) across `oracle/{notify,driver,__init__}.py` (`t03`).

```python
@runtime_checkable
class SyncNotificationSource(Protocol):
    def publish(self, channel: str, payload: bytes | str) -> None: ...
    def subscribe(self, channels: Sequence[str]) -> None: ...
    def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> Iterator[NotificationEvent]: ...

@runtime_checkable
class AsyncNotificationSource(Protocol):
    async def publish(self, channel: str, payload: bytes | str) -> None: ...
    async def subscribe(self, channels: Sequence[str]) -> None: ...
    async def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> AsyncIterator[NotificationEvent]: ...
```

**Integration Flow:**
- `ChannelBus` consumes `AsyncNotificationSource`; synchronous callers/workers consume `SyncNotificationSource`.
- `ch06` implements `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` directly against these protocols.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Legacy unprefixed import | `from vertebra.db.driver import NotificationSource` | Raises `ImportError` (`PY-07` clean break, zero aliases) | Strict `Sync*`/`Async*` symmetry |
| Pooled PgBouncer tx mode | `SyncPostgresNotificationSource` or `AsyncPostgresNotificationSource` without `direct_dsn` | Raises `ImproperConfigurationError` | Direct/session `LISTEN` invariant |

## 3. Documentation & OKF Knowledge Plan

Capability split is documented in `docs/usage/queues-and-realtime.md` (`ch07:t03`) and `changelog` (`ch08:t01`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R13a` | `t01` | `db/driver/{capabilities,__init__}.py`, `server/_channels.py` | `pytest src/py/tests/unit/db/test_native_db_drivers.py` |
| `R13b` | `t02` | `db/__init__.py`, `db/adapters/postgres/{notify,__init__}.py` | `pytest test_pg_dataplane_and_datafusion.py test_postgres_native_notifications.py` |
| `R13c` | `t03` | `db/adapters/oracle/{notify,driver,__init__}.py` | `pytest src/py/tests/unit/db/test_oracle_adapter.py` |

## 5. Implementation Plan

- [ ] [t01-capability-protocols-and-channelbus.md](tasks/t01-capability-protocols-and-channelbus.md) — Replace NotificationSource in capabilities.py, driver/__init__.py, and server/_channels.py with Sync*/Async* protocols.
- [ ] [t02-postgres-sync-and-async-notification-sources.md](tasks/t02-postgres-sync-and-async-notification-sources.md) — Update db/__init__.py and split postgres/notify.py into SyncPostgresNotificationSource and AsyncPostgresNotificationSource.
- [ ] [t03-oracle-sync-and-async-notification-sources.md](tasks/t03-oracle-sync-and-async-notification-sources.md) — Add SyncOracleListener and split OracleNotificationSource into Sync*/Async* across oracle/{notify,driver,__init__}.py.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/3` tasks completed.
- **Active Invariants**: Zero `NotificationSource`, `PostgresNotificationSource`, or `OracleNotificationSource` compatibility aliases remain anywhere in `src/py/vertebra/`.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_native_notifications.py src/py/tests/unit/db/test_oracle_adapter.py`

[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver` — Oracle driver notification_source factory.
