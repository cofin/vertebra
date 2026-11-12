---
type: Task
id: "native-postgres-driver:ch26:t04"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Open and close dedicated native PostgreSQL event sessions from drivers
description: Implement EventSessionRegistry in events.py and wire dedicated notification, replication identity, replication stream, and pg_notify publisher hooks across permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/utils/portal.py
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/events.py
tests:
  - src/py/tests/unit/db/test_postgres_event_hooks.py
estimated_loc: 245
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: EventSessionRegistry handle/client close symmetry, _publish_notification mode rules, and PortalProvider sync shutdown
  - id: pg-pyi
    resource: src/py/vertebra/_postgres.pyi::AsyncPostgresClient
    title: AsyncPostgresClient.close vs AsyncPostgresListener.aclose and AsyncPostgresReplication.aclose signatures
  - id: pg-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgresNotificationSource consumer seam
  - id: portal-py
    resource: src/py/vertebra/utils/portal.py::PortalProvider
    title: PortalProvider.default().call synchronous-to-async cleanup bridge
tags:
  - postgres
  - events
  - notifications
  - replication
---

# Task t04: Open and Close Dedicated Native PostgreSQL Event Sessions from Drivers

## Objective & Scope Boundary

Implement adapter-internal `EventSessionRegistry` in `src/py/vertebra/db/adapters/postgres/events.py` (omitted from `postgres/__init__.py`) and wire `_open_notification_listener`, `_replication_identity`, `_open_replication_stream`, `_publish_notification`, and deterministic shutdown across `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` and `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver` so both permanent drivers structurally satisfy `PostgresNotificationDriver` and `PostgresReplicationDriver` over isolated direct `AsyncPostgresClient` sockets closed via `await client.close()` before `ch27:t01` activation [^npd-contracts] [^pg-pyi] [^pg-notify-py] [^portal-py]. Public `driver.py` and `postgres/__init__.py` remain untouched in this task.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Dedicated `(handle, AsyncPostgresClient)` registry pruning on `.closed`, `handle.aclose()` + `client.close()` teardown, `_publish_notification` `timeout=10.0` + sync transaction `InterfaceError` preflight, and `PortalProvider` sync driver close [^npd-contracts] [^pg-pyi].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Permanent `SyncPostgreSQLDriver` (`close`, `execute`, active sync scope checks).
- `src/py/vertebra/db/adapters/postgres/_async.py` — Permanent `AsyncPostgreSQLDriver` (`close`, `execute`, ambient async transaction scope).
- `src/py/vertebra/db/adapters/postgres/config.py` — `PostgreSQLConfig.derive_direct_config()` and `resolve_async_options(include_pool_options=False)` from `ch25:t01`.
- `src/py/vertebra/db/adapters/postgres/events.py` — `PostgresNotificationDriver` (`ch21:t03`) and `PostgresReplicationDriver` (`ch24:t01`) protocols.
- `src/py/vertebra/utils/portal.py` — `PortalProvider.default().call` background event-loop runner [^portal-py].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/events.py`:
  - `class EventSessionRegistry:` (domain-named adapter-internal class included in `events.__all__` for `_sync.py` / `_async.py`, never re-exported from `postgres/__init__.py`):
    - Pairs each opened raw `AsyncPostgresListener` or `AsyncPostgresReplication` handle with its dedicated direct `AsyncPostgresClient` (never borrowing a query-pool lease and never relying on `weakref` or native callbacks).
    - `async def prune_closed(self) -> None` — inspects `handle.closed` (`bool`), removes entries with `handle.closed is True`, and awaits `await client.close()` in `finally` (`AsyncPostgresClient` defines `close() -> Awaitable[None]`, whereas `aclose()` exists strictly on listener and replication handles) so repeated short-lived sources retain zero idle clients/sockets.
    - `async def open_and_register(self, client_factory: ..., opener: ...) -> Any` — rejects opens after registry closure (`InterfaceError`), runs `prune_closed()`, constructs direct `AsyncPostgresClient` via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, awaits `opener(client)`, registers `(handle, client)` only on success, and runs shielded `await handle.aclose()` (when opened) + `await client.close()` in `finally` on failure or `CancelledError`.
    - `async def aclose(self) -> None` — marks registry closed before awaiting, awaits `await handle.aclose()` strictly on each `AsyncPostgresListener` / `AsyncPostgresReplication` handle followed by `await client.close()` on its `AsyncPostgresClient`, preserves the first exception while still closing all remaining pairs, and is idempotent on repeat calls.
- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py` (both structurally satisfying `PostgresNotificationDriver` and `PostgresReplicationDriver`):
  - `async def _open_notification_listener(self, channels: Sequence[str], *, queue_capacity: int = 1024, reconnect: bool = False) -> AsyncPostgresListener`:
    - Calls `direct_cfg = self.config.derive_direct_config()` (rejecting `transaction`/`statement` poolers lacking `direct_dsn` before network I/O) and `direct_cfg.resolve_async_options(include_pool_options=False)`, loads `vertebra._postgres` via `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, and opens `client.open_listener(list(channels), queue_capacity=queue_capacity, reconnect=reconnect)` via `self._event_registry`.
  - `async def _replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity`:
    - Rejects closed driver (`InterfaceError`), resolves direct options via `derive_direct_config()`, loads `vertebra._postgres` with exact `package="vertebra-core[postgres]"` metadata, connects a fresh `AsyncPostgresClient`, awaits `client.replication_identity(slot, publication)` (zero `START_REPLICATION`), and always awaits `await client.close()` inside `CancelScope(shield=True)` in `finally`.
  - `async def _open_replication_stream(self, config: PostgresChangeConfig, *, start_lsn: int | None = None, reconnect: bool = False) -> AsyncPostgresReplication`:
    - Unpacks `config.slot`, `config.publication`, and `config.queue_capacity` (never passing `config.checkpoint_timeout` to native `open_replication`), and opens `client.open_replication(config.slot, config.publication, start_lsn=start_lsn, queue_capacity=config.queue_capacity, reconnect=reconnect)` via `self._event_registry`.
  - `async def _publish_notification(self, channel: str, payload: str) -> None`:
    - `AsyncPostgreSQLDriver`: `await self.execute("SELECT pg_notify($1, $2)", (channel, payload), timeout=10.0)` in the caller's task (joining any ambient `async with driver.transaction():` so rollback suppresses delivery).
    - `SyncPostgreSQLDriver`: checks this driver's active `Sync` transaction `ContextVar` scope first and raises `InterfaceError` before `asyncio.to_thread` or I/O when active; outside a sync scope, awaits `await asyncio.to_thread(self.execute, "SELECT pg_notify($1, $2)", (channel, payload), timeout=10.0)`.
  - Driver `close()` hooks:
    - `AsyncPostgreSQLDriver.close()` awaits `self._event_registry.aclose()` inside `CancelScope(shield=True)` before `await pool.close()`.
    - `SyncPostgreSQLDriver.close()` runs `PortalProvider.default().call(self._event_registry.aclose, timeout=self.config.close_timeout)` before `pool.close()`.

## Edge Cases & Error Semantics

- Cancelling `_open_notification_listener`, `_open_replication_stream`, or `_replication_identity` mid-handshake closes the dedicated `AsyncPostgresClient` via `await client.close()` immediately without leaking entries into `EventSessionRegistry` or touching query-pool capacity.
- Calling `PostgresNotificationSource(sync_driver).publish(...)` inside `with sync_driver.transaction():` raises `InterfaceError` with zero worker offload and zero SQL frames emitted, whereas `sync_driver.execute("SELECT pg_notify($1, $2)", ...)` inside that `with` block runs on the pinned transaction socket.
- Calling `sync_driver.close()` from inside a running `asyncio` event loop succeeds cleanly via `PortalProvider.default().call` while releasing the GIL so concurrent Python threads progress.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_event_hooks.py` (testing `_sync.SyncPostgreSQLDriver` and `_async.AsyncPostgreSQLDriver` with `sslmode="disable"` against scripted native Protocol 3.0 TCP wire peers — zero fake hook stubs).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Live scripted wire listener, identity preflight, & replication open on BOTH drivers | Open `PostgresNotificationSource` and `PostgresChangeSource` over `_sync.SyncPostgreSQLDriver` and `_async.AsyncPostgreSQLDriver` (`sslmode="disable"`) | Static `d1: PostgresNotificationDriver = sync_drv` and `d2: PostgresReplicationDriver = async_drv` pass `mypy`; wire peer observes direct sessions (`0` query-pool leases borrowed), `LISTEN`, identity query without `START_REPLICATION` closed cleanly via `await client.close()`, and `CopyBoth` on stream open. |
| Handle `.closed` pruning & `Sync`/`Async` driver `close()` from active loop | Open and `await handle.aclose()` `3` short-lived listeners, open `2` more left active, then call `sync_driver.close()` inside an `async def` test (with sibling Python thread counter) and `await async_driver.close()` | `EventSessionRegistry.prune_closed()` awaits `client.close()` on `.closed` pairs; driver `close()` calls `await handle.aclose()` + `await client.close()` down to TCP EOF, sibling thread advances, and first close error is preserved while remaining pairs still close. |
| Pooler rejection, missing wheel metadata, & cancelled open cleanup | `PostgreSQLConfig(pooler_mode="transaction")` without `direct_dsn`; absent `vertebra._postgres`; cancel `_open_replication_stream` mid-startup | Pooler config raises `ImproperConfigurationError` (`0` sockets); absent wheel raises `MissingDependencyError` carrying `package="vertebra-core[postgres]"`; cancelled open awaits `client.close()` with `0` retained registry entries. |
| `_publish_notification` mode & `timeout=10.0` invariants | (a) `PostgresNotificationSource(async_drv).publish` inside rolled-back `async with async_drv.transaction():`; (b) `PostgresNotificationSource(sync_drv).publish` inside vs outside `with sync_drv.transaction():`; (c) stalled peer past `timeout` vs server `57014` | (a) Joins async tx (`SELECT pg_notify($1, $2)`, `timeout=10.0`), rollback suppresses delivery; (b) inside sync tx raises `InterfaceError` before `to_thread`/SQL, outside runs `pg_notify($1, $2)` with `timeout=10.0`; (c) stalled peer raises `OperationalError(sqlstate=None)` + retires socket, `57014` raises `QueryCancelledError`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_event_hooks.py -q`
- Final Green + CDC Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/db/test_postgres_change_source.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/tests/unit/db/test_postgres_event_hooks.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/tests/unit/db/test_postgres_event_hooks.py
  ```

## Acceptance Criteria

- [ ] `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` structurally satisfy `PostgresNotificationDriver` and `PostgresReplicationDriver` (verified by `mypy`) over dedicated direct `AsyncPostgresClient` sockets loaded with `package="vertebra-core[postgres]"` (R21, R22, R23, R24).
- [ ] `EventSessionRegistry` (adapter-internal in `events.py`, omitted from `postgres/__init__.py`) closes handles with `await handle.aclose()` and clients with `await client.close()`, prunes `.closed` handles, drains via `CancelScope(shield=True)` / `PortalProvider.default().call`, and enforces `_publish_notification` `timeout=10.0` + sync-transaction `InterfaceError` preflight (R21, R22, R23, R24).
- [ ] Commit message follows `feat(postgres): own dedicated event driver sessions` once verified.

## Notes & Discoveries

- Notification source naming: this worksheet executes before `queues-tasks-and-channels` t01 splits `NotificationSource` into `SyncNotificationSource` / `AsyncNotificationSource` and renames `PostgresNotificationSource` to `AsyncPostgresNotificationSource` alongside a new `SyncPostgresNotificationSource` (see [native-data-platform](../../../../native-data-platform/spec.md) Seam 2). It keeps the HEAD names, adds no new unprefixed notification protocol, class, or alias, and leaves the rename of every symbol, doc, and OKF reference it touches to that task. The synchronous-driver `publish` cases asserted here (sync transaction rejection and out-of-transaction `pg_notify($1, $2)`) carry over to `SyncPostgresNotificationSource` when the rename lands.
- With `ch26:t04` complete, `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` expose the complete production surface required for atomic public barrel and consumer cutover in `ch27:t01`–`t02` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — EventSessionRegistry handle/client close symmetry, _publish_notification mode rules, and PortalProvider sync shutdown.
[^pg-pyi]: `src/py/vertebra/_postgres.pyi::AsyncPostgresClient` — AsyncPostgresClient.close vs AsyncPostgresListener.aclose and AsyncPostgresReplication.aclose signatures.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgresNotificationSource consumer seam.
[^portal-py]: `src/py/vertebra/utils/portal.py::PortalProvider` — PortalProvider.default().call synchronous-to-async cleanup bridge.
