---
type: Task
id: "native-postgres-driver:ch21:t03"
chapter_id: ch21-live-notifications
title: Wire PostgresNotificationSource over typed adapter protocols and active subscriptions
description: Define permanent PostgresNotificationDriver in events.py, add real _publish_notification methods on legacy driver.py classes, and upgrade PostgresNotificationSource with live listener subscriptions and ChangeFeed hydration.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch21:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch21-live-notifications/spec.md
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/cdc.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/db/adapters/postgres/events.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/postgres/notify.py
tests:
  - src/py/tests/unit/db/test_postgres_native_notifications.py
  - src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
estimated_loc: 225
plan_revision: 7
plan_commit: 1c75af65
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
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresNotificationDriver protocol, uniform publish seam, and dedicated listener rules
  - id: pg-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: Existing PostgresNotificationSource and PostgresNotifyConfig implementation
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Existing legacy SyncPostgreSQLDriver and AsyncPostgreSQLDriver classes
  - id: pg-config-py
    resource: src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig
    title: PostgreSQLConfig derive_direct_config and pooler_mode validation
  - id: cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeFeed
    title: ChangeFeed publish_event and hydrate_notification spill resolution
  - id: pg-dataplane-test
    resource: src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::test_postgres_notify_publish
    title: Existing publish, channel-validation, and unsupported-listener unit tests
tags:
  - postgres
  - python
  - notifications
  - cdc
  - typing
---

# Task t03: Wire PostgresNotificationSource over Typed Adapter Protocols and Active Subscriptions

## Objective & Scope Boundary

Create the permanent adapter-owned `PostgresNotificationDriver` protocol in `src/py/vertebra/db/adapters/postgres/events.py`, relocate `pg_notify` execution into real `async def _publish_notification` methods on the two legacy classes in `src/py/vertebra/db/adapters/postgres/driver.py`, and upgrade `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` to stream native `AsyncPostgresListener` events with live `subscribe`/`unsubscribe` updates and `ChangeFeed` hydration [^pg-contracts] [^pg-notify-py] [^pg-driver-py] [^cdc-py]. `events.py` is extended with `PostgresReplicationDriver` in `ch24:t01` and the dedicated handle registry in `ch26:t04`; permanent `_sync.py`/`_async.py` numeric publishers land in `ch26:t04`, and legacy `driver.py` classes are deleted in `ch27:t03`–`t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 21 `PostgresNotificationDriver` protocol and active subscription synchronization contracts.
- `src/py/vertebra/db/adapters/postgres/notify.py` — `PostgresNotifyConfig` and `PostgresNotificationSource` [^pg-notify-py].
- `src/py/vertebra/db/adapters/postgres/driver.py` — Legacy `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` receiving only `asyncio` import + `_publish_notification` [^pg-driver-py].
- `src/py/vertebra/db/adapters/postgres/config.py` — `PostgreSQLConfig.derive_direct_config()` and `pooler_mode` / `direct_dsn` invariants [^pg-config-py].
- `src/py/vertebra/db/cdc.py` — `ChangeFeed.publish_event` and `ChangeFeed.hydrate_notification` [^cdc-py].
- [contracts.md](../../../contracts.md) — Event protocol and publication invariants (remember: zero inline `#` comments in Python code; use Google docstrings and PEP 585/604 types) [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/events.py`:
  - Uses `from __future__ import annotations` and `if TYPE_CHECKING:` imports for `AsyncPostgresListener` (`vertebra._postgres`) and `PostgreSQLConfig` so optional `vertebra._postgres` is never eagerly imported:
    ```python
    class PostgresNotificationDriver(Protocol):
        @property
        def config(self) -> PostgreSQLConfig: ...

        async def _open_notification_listener(
            self,
            channels: Sequence[str],
            *,
            queue_capacity: int = 1024,
            reconnect: bool = False,
        ) -> AsyncPostgresListener: ...

        async def _publish_notification(self, channel: str, payload: str) -> None: ...
    ```
- `src/py/vertebra/db/adapters/postgres/driver.py`:
  - Add ONLY `import asyncio` (if absent) and real `async def _publish_notification(self, channel: str, payload: str) -> None` on the two existing legacy classes:
    - `SyncPostgreSQLDriver._publish_notification`: `await asyncio.to_thread(self.execute, "SELECT pg_notify(%s, %s)", (channel, payload))`
    - `AsyncPostgreSQLDriver._publish_notification`: `await self.execute("SELECT pg_notify($1, $2)", (channel, payload))`
    - Do NOT add any dummy `_open_notification_listener` hook on legacy classes.
- `src/py/vertebra/db/adapters/postgres/notify.py`:
  - Annotate `driver: PostgresNotificationDriver` (imported under `TYPE_CHECKING`), replacing `AsyncPostgreSQLDriver | SyncPostgreSQLDriver` and removing `AsyncDatabaseDriver` / `isinstance` / `asyncio.to_thread` from `notify.py`.
  - Extend `PostgresNotificationSource.__init__` with keyword-only `queue_capacity: int = 1024` (validating non-`bool` `queue_capacity > 0`) and `reconnect: bool = False`, plus active `_listener: AsyncPostgresListener | None` and `_listening: bool` slots.
  - `publish(self, channel: str, payload: bytes | str) -> None`: validates `channel`, wraps raw/`b64:`/`ChangeFeed` envelope identically, and awaits `self._driver._publish_notification(channel, wire_payload)`.
  - `subscribe(self, channels: Sequence[str]) -> None` and `unsubscribe(self, channels: Sequence[str]) -> None`: validate ALL channel identifiers before mutating state or performing I/O; update `self._subscribed_channels` snapshot AND, when `self._listener is not None and not self._listener.closed`, await `self._listener.subscribe(validated)` / `self._listener.unsubscribe(validated)`.
  - `listen(self) -> AsyncIterator[NotificationEvent]`:
    - Raises `InterfaceError` if `self._listening` is already `True` (concurrent `listen()`).
    - Inspects `getattr(self._driver, "config", None)`: when a `PostgreSQLConfig` is present, calls `direct_cfg = cfg.derive_direct_config()` and raises `ImproperConfigurationError` if `direct_cfg.pooler_mode in ("transaction", "statement")` or `direct_cfg.pgbouncer`.
    - Looks up `open_listener = getattr(self._driver, "_open_notification_listener", None)`; if `None`, raises `NotSupportedError("Live PostgreSQL LISTEN streaming requires the native Rust PostgreSQL driver")`.
    - Opens `listener = await open_listener(tuple(self._subscribed_channels), queue_capacity=self._queue_capacity, reconnect=self._reconnect)` (including default `self._config.channel` only when explicitly subscribed or preserving existing explicit `_subscribed_channels` contract) and loops `await listener.next()` until `None`:
      - Hydrates payload: if `self._change_feed is not None`, calls `evt = self._change_feed.hydrate_notification(raw_payload)` (propagating evicted/store spill errors!) and decodes `base64.b64decode(evt.payload["data_b64"])`; else if UTF-8 string starts with `"b64:"`, base64-decodes bytes; else preserves raw `bytes` payload.
      - Yields `NotificationEvent(channel=ch, payload=hydrated_bytes, sender_id=str(pid), metadata={"reconnected": gap, "delivery": "at_most_once"})`.
    - `finally:` clears `self._listening = False` / `self._listener = None` and awaits `listener.aclose()`.
  - Add idempotent `async def aclose(self) -> None`, `async def __aenter__(self) -> Self`, and `async def __aexit__(...) -> None`.

## Edge Cases & Error Semantics

- Invalid channel inside `subscribe(["orders", "bad;sql"])` raises `ValueError` before mutating `_subscribed_channels` or sending `LISTEN "orders"` to an active listener.
- Breaking out of `async for event in source.listen():`, raising inside consumer body, or cancelling the consumer task closes the underlying `AsyncPostgresListener` in `finally` so `source.listen()` can be reused cleanly on a subsequent iteration.

## Public-Seam Test Blueprint

- **Canonical test files**:
  - `src/py/tests/unit/db/test_postgres_native_notifications.py` (`test_notification_source_*` suite with typed `NativeNotificationDriverPort`)
  - `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` (updated legacy `publish` side-effect binding + removal of obsolete mock `listen_notifications` delegation test) [^pg-dataplane-test]
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Typed `NativeNotificationDriverPort` end-to-end `publish` + live `subscribe`/`unsubscribe` + `ChangeFeed` spill hydration | `port: PostgresNotificationDriver = NativeNotificationDriverPort(...)` (real `SyncPostgresClient` query in `asyncio.to_thread` for `_publish_notification` + real `AsyncPostgresClient.open_listener` against scripted server); publish `str`, binary `b"\x00\xff"`, and spilled `ChangeFeed` payload; dynamic `subscribe`/`unsubscribe` mid-stream | Yields exact `NotificationEvent(payload=b"...", sender_id=str(pid), metadata={"reconnected": False, "delivery": "at_most_once"})`, hydrates spilled `ChangeFeed` bytes, propagates missing spill `KeyError`, does not starve `max_size = 1` pool, and closes listener on `break`/cancel allowing source reuse. |
| Pooler mode & concurrent `listen()` guards | (a) `PostgreSQLConfig(dsn=..., pooler_mode="transaction")` without `direct_dsn` vs with `direct_dsn="postgresql://localhost:5432/app"`; (b) second `source.listen()` while first is active | (a) Without `direct_dsn` raises `ImproperConfigurationError`; with `direct_dsn` succeeds via `derive_direct_config()`; (b) second `listen()` raises `InterfaceError`. |
| Legacy `driver.py` `_publish_notification` & unsupported listener in `test_pg_dataplane_and_datafusion.py` | Bind `driver._publish_notification.side_effect = driver_type._publish_notification.__get__(driver, driver_type)` in `test_postgres_notify_publish`; run `test_postgres_notify_listen_without_native_stream` | Sync `driver.execute` runs off-loop with `"SELECT pg_notify(%s, %s)"`; async `driver.execute` is awaited with `"SELECT pg_notify($1, $2)"`; legacy driver without `_open_notification_listener` raises `NotSupportedError`. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k notification_source -q`
- Final Green + stubtest + pytest + Mypy gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k notification_source -q
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -k notif -q
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/events.py src/py/vertebra/db/adapters/postgres/notify.py src/py/tests/unit/db/test_postgres_native_notifications.py
  ```
- Planned local commit: `feat(postgres): wire typed active notification sources`

## Acceptance Criteria

- [ ] `events.py::PostgresNotificationDriver` types `PostgresNotificationSource` cleanly under `mypy` without eager `vertebra._postgres` imports, and `notify.py` routes `publish()` uniformly through `await self._driver._publish_notification(...)` (R21).
- [ ] `PostgresNotificationSource` validates channel lists atomically before I/O, updates active `AsyncPostgresListener` subscriptions awaiting `ReadyForQuery`, rejects `transaction`/`statement` poolers lacking `direct_dsn`, hydrates raw/`b64:`/`ChangeFeed` payloads with `reconnected` + `at_most_once` metadata, and closes listeners idempotently on exit/cancel (R21).
- [ ] `NativeNotificationDriverPort` exercises real `SyncPostgresClient` + `AsyncPostgresClient` wire sessions while `test_pg_dataplane_and_datafusion.py` verifies real bound legacy `_publish_notification` execution and `NotSupportedError` fallback without dummy hooks (R21).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresNotificationDriver protocol, uniform publish seam, and dedicated listener rules.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — Existing PostgresNotificationSource and PostgresNotifyConfig implementation.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Existing legacy SyncPostgreSQLDriver and AsyncPostgreSQLDriver classes.
[^pg-config-py]: `src/py/vertebra/db/adapters/postgres/config.py::PostgreSQLConfig` — PostgreSQLConfig `derive_direct_config` and `pooler_mode` validation.
[^cdc-py]: `src/py/vertebra/db/cdc.py::ChangeFeed` — ChangeFeed `publish_event` and `hydrate_notification` spill resolution.
[^pg-dataplane-test]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py::test_postgres_notify_publish` — Existing publish, channel-validation, and unsupported-listener unit tests.
