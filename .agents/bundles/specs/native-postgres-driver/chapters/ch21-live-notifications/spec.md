---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch21-live-notifications
title: "Chapter 21: Dedicated LISTEN/NOTIFY Sessions, Async Listener Bridges, and Active Notification Sources"
description: Implement dedicated non-pooled PostgreSQL LISTEN/NOTIFY sessions with bounded overflow and optional reconnect gap tracking, expose AsyncPostgresListener across sync and async native clients, and wire PostgresNotificationSource over typed adapter protocols.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch20-arrow-copy-import-and-portals
replan_trigger: null
current_task: "native-postgres-driver:ch21:t01"
plan_revision: 4
plan_commit: 45ff9563
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Dedicated PostgreSQL notification handle, closed snapshot, and protocol contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL NotificationResponse push pump, LISTEN/UNLISTEN quoting, and pooler exclusions
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop AsyncCompletionQueue bridge for non-blocking listener awaitables
  - id: pg-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: Public PostgresNotificationSource and PostgresNotifyConfig surface
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig derive_direct_config and transaction/statement pooler modes
tags:
  - postgres
  - rust
  - python
  - notifications
  - streaming
---

# Chapter 21: Dedicated LISTEN/NOTIFY Sessions, Async Listener Bridges, and Active Notification Sources

<!-- truth: start -->
- **Dedicated Non-Pooled `LISTEN`/`NOTIFY` Session**: `PostgresListener` owns an independent direct/session `PostgresConnection` and sole Tokio receive/control pump (never a query-pool lease or transaction/statement-pooler socket), enforcing `1024`-frame + `8 MiB` queue caps that fail closed with `OperationalError` on overflow rather than silently dropping events [^pg-contracts] [^pg-protocol-research].
- **At-Most-Once Reconnect Gap & `closed` Snapshot**: `AsyncPostgresListener` exposes read-only `closed: bool` with zero I/O and `(pid, channel, payload, reconnected_gap)` tuples; optional `reconnect=True` retries recoverable transport drops (`0.1s..5.0s` jitter, max `8` attempts) with fresh IAM/direct credentials, resubscribes active channels through `ReadyForQuery`, and flags `reconnected_gap=True` [^pg-contracts] [^py-async-rs].
- **Protocol-Typed `PostgresNotificationSource`**: `events.py::PostgresNotificationDriver` decouples `notify.py` from driver mode checks, routes `publish()` through uniform `await _driver._publish_notification(...)`, updates live `LISTEN`/`UNLISTEN` sets dynamically, and hydrates raw/`b64:`/`ChangeFeed` spills [^pg-notify-py] [^pg-config-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/py/vertebra/db/adapters/postgres/notify.py` currently raises `NotSupportedError` on `listen()` and branches on `isinstance(self._driver, AsyncDatabaseDriver)` inside `publish()` [^pg-notify-py]. Chapter 21 implements the native push `NotificationResponse` pump in `src/rs/vertebra-postgres/src/notifications.rs` over a dedicated non-pooled `PostgresConnection`, exposes `AsyncPostgresListener` and `open_listener` on both `SyncPostgresClient` and `AsyncPostgresClient` via `_vertebra_postgres_completion_drainer`, defines the permanent `PostgresNotificationDriver` protocol in `src/py/vertebra/db/adapters/postgres/events.py`, and wires active subscription updates and `ChangeFeed` hydration in `PostgresNotificationSource` [^pg-contracts] [^pg-protocol-research] [^py-async-rs].

### 1.2 Non-Goals

- Logical replication `CopyBoth` / `pgoutput` v1 streaming, `PostgresReplicationDriver`, and `PostgresChangeSource` (owned by Chapters 22–24, where `ch24:t01` extends `events.py` with `PostgresReplicationDriver`).
- Permanent `_sync.py` / `_async.py` driver `_open_notification_listener` and numeric `timeout=10.0` `_publish_notification` hooks + dedicated handle registry (owned by `ch26:t04`, followed by public barrel cutover and legacy driver removal in `ch27:t01`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R21.1 (`ch21:t01`)**: Implement `PostgresListener` in `src/rs/vertebra-postgres/src/notifications.rs` owning a dedicated `PostgresConnection` and single Tokio socket pump, validating and double-quoting `LISTEN` / `UNLISTEN` channel identifiers, routing `NotificationResponse` frames arriving both while idle and during `LISTEN`/`UNLISTEN` command responses into a bounded `1024`-frame + `8 MiB` queue (failing closed with `PostgresError::Operational` on overflow), servicing `subscribe`/`unsubscribe`/`close` over a separate bounded control channel without starvation, and supporting optional recoverable transport reconnect (`0.1s..5.0s` jitter, max `8` attempts, refreshed direct/IAM credentials, full resubscribe before delivery, and `reconnected_gap = true`).
- **R21.2 (`ch21:t02`)**: Expose `AsyncPostgresListener` (`next`, `subscribe`, `unsubscribe`, `aclose`, `__aenter__`, `__aexit__`, read-only `@property def closed -> bool`) and `open_listener(channels, *, queue_capacity=1024, reconnect=False) -> Awaitable[AsyncPostgresListener]` on both `SyncPostgresClient` and `AsyncPostgresClient` in `sync_client.rs`, `async_client.rs`, and `src/py/vertebra/_postgres.pyi` with single-pending `next()` enforcement (`InterfaceError` on overlap) and cancellation-safe session cleanup.
- **R21.3 (`ch21:t03`)**: Add permanent `PostgresNotificationDriver(Protocol)` in `src/py/vertebra/db/adapters/postgres/events.py`, add real `_publish_notification` methods to the two legacy classes in `src/py/vertebra/db/adapters/postgres/driver.py`, and upgrade `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` with uniform `await _driver._publish_notification(...)`, live `subscribe`/`unsubscribe` synchronization, `pooler_mode` direct-session validation, raw/`b64:`/`ChangeFeed` hydration, and idempotent `aclose()` / async context management.

### 2.2 Concrete Type & Signature Contracts

- `src/rs/vertebra-postgres/src/notifications.rs`:
  - `pub const DEFAULT_NOTIFICATION_QUEUE_CAPACITY: usize = 1024;`
  - `pub const DEFAULT_NOTIFICATION_MAX_BYTES: usize = 8 * 1024 * 1024;`
  - `pub struct NotificationFrame { pub pid: i32, pub channel: String, pub payload: Bytes, pub reconnected_gap: bool }`
  - `pub struct ListenerOptions { pub queue_capacity: usize, pub max_queue_bytes: usize, pub reconnect: bool }`
  - `pub struct PostgresListener` with `pub async fn open(config: crate::config::PostgresConfig, initial_channels: Vec<String>, options: ListenerOptions) -> Result<Self, crate::errors::PostgresError>` (raw `PostgresConfig` does not carry `pooler_mode` because `PoolerMode` lives in `crate::session::statements::PoolerMode` / `SessionHandle`, so `PoolerMode` rejection on listeners happens on pooled `SessionHandle` and Python `PostgreSQLConfig.derive_direct_config()` in `ch21:t03`), `pub async fn next(&self) -> Result<Option<NotificationFrame>, crate::errors::PostgresError>`, `pub async fn subscribe(&self, channels: Vec<String>) -> Result<(), crate::errors::PostgresError>`, `pub async fn unsubscribe(&self, channels: Vec<String>) -> Result<(), crate::errors::PostgresError>`, `pub async fn close(&self) -> Result<(), crate::errors::PostgresError>`, and `pub fn is_closed(&self) -> bool`.
- `src/py/vertebra/_postgres.pyi`:
  - `class AsyncPostgresListener`:
    - `@property def closed(self) -> bool: ...` (read-only, no setter)
    - `def next(self) -> Awaitable[tuple[int, str, bytes, bool] | None]: ...`
    - `def subscribe(self, channels: Sequence[str]) -> Awaitable[None]: ...`
    - `def unsubscribe(self, channels: Sequence[str]) -> Awaitable[None]: ...`
    - `def aclose(self) -> Awaitable[None]: ...`
    - `def __aenter__(self) -> Awaitable[AsyncPostgresListener]: ...`
    - `def __aexit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> Awaitable[None]: ...`
  - `SyncPostgresClient.open_listener` & `AsyncPostgresClient.open_listener(self, channels: Sequence[str], *, queue_capacity: int = 1024, reconnect: bool = False) -> Awaitable[AsyncPostgresListener]`
- `src/py/vertebra/db/adapters/postgres/events.py` & `notify.py`:
  - `class PostgresNotificationDriver(Protocol)` (`@property def config(self) -> PostgreSQLConfig`, `async def _open_notification_listener(...) -> AsyncPostgresListener`, `async def _publish_notification(self, channel: str, payload: str) -> None`)
  - `PostgresNotificationSource.__init__(self, driver: PostgresNotificationDriver, config: PostgresNotifyConfig | None = None, *, change_feed: ChangeFeed | None = None, queue_capacity: int = 1024, reconnect: bool = False) -> None`

### 2.3 Caller -> Seam -> Collaborator Map

```
PostgresNotificationSource (src/py/vertebra/db/adapters/postgres/notify.py)
  -> PostgresNotificationDriver protocol (src/py/vertebra/db/adapters/postgres/events.py)
    -> Sync/AsyncPostgresClient.open_listener -> AsyncPostgresListener (sync_client.rs / async_client.rs)
      -> _vertebra_postgres_completion_drainer (non-blocking asyncio Future bridge)
        -> PostgresListener (src/rs/vertebra-postgres/src/notifications.rs)
          -> Dedicated direct PostgresConnection (crate::config::PostgresConfig + protocol.rs) + sole Tokio socket/control pump
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Failure / Edge Condition | Expected Behavior |
| --- | --- |
| Listener idle on a client whose query pool has `max_size = 1` | Listener uses its own dedicated `PostgresConnection`, leaving the `max_size = 1` query pool completely unblocked for concurrent queries/transactions. |
| Bounded queue exceeds `queue_capacity` (`1024`) or `8 MiB` payload bytes | Pump closes the socket, marks `closed = true`, and surfaces explicit `OperationalError` loss failure rather than silently dropping oldest or newest notifications. |
| `NotificationResponse` arrives mid-`LISTEN` / `UNLISTEN` before `ReadyForQuery` | Sole socket pump enqueues the notification in wire arrival order while `subscribe`/`unsubscribe` resolves only after server `ReadyForQuery`. |
| Recoverable transport drop with `reconnect=True` vs auth/protocol/overflow error | Transport drop retries up to `8` times (`0.1s..5.0s` jitter) keeping `closed == False`, refreshes IAM/direct config, resubscribes all active channels, and sets `reconnected_gap = True` on the next delivered event; auth/protocol/overflow errors terminate immediately with `closed == True`. |
| `pooler_mode in ("transaction", "statement")` without `direct_dsn` | Rejected on pooled `SessionHandle` (`crate::session::statements::PoolerMode`) and in Python `PostgreSQLConfig.derive_direct_config()` (`ch21:t03`, raising `ImproperConfigurationError`) before opening a listener, since raw `crate::config::PostgresConfig` does not carry `pooler_mode`. |

### 2.5 Alternatives Considered

| Decision | Chosen Seam & Rejected Alternative |
| --- | --- |
| Dedicated `PostgresConnection` vs borrowing from `PhysicalPool` | Dedicated direct connection prevents long-lived `LISTEN` streams from starving `max_size` query pools or binding `LISTEN` state onto multiplexed transaction poolers. |
| Fail-closed overflow (`OperationalError`) vs ring-buffer overwrite | Explicit termination forces consumers to handle backpressure or reconnect gaps rather than silently losing notifications while appearing healthy. |
| Adapter-owned `events.py::PostgresNotificationDriver` protocol vs `isinstance` / temporary shim | Structural protocol with `TYPE_CHECKING` imports avoids eager optional `vertebra._postgres` imports and removes `isinstance`/`asyncio.to_thread` branching from `notify.py`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch21]` (user-facing `LISTEN`/`NOTIFY` and `ChangeFeed` spill guides in `docs/usage/databases.md` and `.agents/bundles/knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` land in `ch30:t01` and `ch30:t02`; inline docstrings and `_postgres.pyi` annotations land in `ch21:t01`–`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R21.1 | [t01](tasks/t01-dedicated-notification-session.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs` |
| R21.2 | [t02](tasks/t02-async-notification-bridge.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_notifications.py`, `src/rs/vertebra-postgres/tests/postgres_notification_wire_tests.rs` |
| R21.3 | [t03](tasks/t03-python-notification-source.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_notifications.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch21:t01 — Pump PostgreSQL notifications on a dedicated bounded session with reconnect gap state (tasks/t01-dedicated-notification-session.md)
- [ ] native-postgres-driver:ch21:t02 — Expose AsyncPostgresListener across sync and async native clients (tasks/t02-async-notification-bridge.md)
- [ ] native-postgres-driver:ch21:t03 — Wire PostgresNotificationSource over typed adapter protocols and active subscriptions (tasks/t03-python-notification-source.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -k "native_listener or notification_source or notif" -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/notify.py src/py/tests/unit/db/test_postgres_native_notifications.py` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch22-copyboth-and-pgoutput-decoding` (`CopyBoth` logical replication), `ch24:t01` (`events.py` replication protocol), and `ch26:t04` (`dedicated-driver-event-hooks`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Dedicated PostgreSQL notification handle, closed snapshot, and protocol contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL NotificationResponse push pump, LISTEN/UNLISTEN quoting, and pooler exclusions.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue bridge for non-blocking listener awaitables.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — Public PostgresNotificationSource and PostgresNotifyConfig surface.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQLConfig `derive_direct_config` and transaction/statement pooler modes.
