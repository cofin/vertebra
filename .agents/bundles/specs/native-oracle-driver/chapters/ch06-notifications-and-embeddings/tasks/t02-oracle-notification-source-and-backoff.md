---
type: Task
id: "native-oracle-driver:ch06:t02"
flow_id: native-oracle-driver
chapter_id: ch06-notifications-and-embeddings
title: "Task 02: Implement OracleNotificationSource and Reconnect Backoff"
description: Implement OracleNotifyConfig and runtime_checkable NotificationSource-compliant OracleNotificationSource in src/py/vertebra/db/adapters/oracle/notify.py and wire notification hooks on SyncOracleDriver and AsyncOracleDriver with ChangeFeed integration and exponential reconnect backoff.
status: stable
state: completed
priority: P1
owner: null
depends_on:
  - "native-oracle-driver:ch06:t01"
files:
  - src/py/vertebra/db/adapters/oracle/notify.py
  - src/py/vertebra/db/adapters/oracle/driver.py
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/cdc.py
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/db/adapters/oracle/driver.py
verification_strategy: behavior_tdd
estimated_loc: 210
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-09T17:20:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T17:30:00Z"
last_verified_commit: 1232f10d
verification_evidence: "pytest src/py/tests/unit/db/test_oracle_adapter.py -k 'notify or notification_source or channel_name' (12 passed) and ruff/mypy clean"
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T17:30:00Z"
completed_at: "2026-11-09T17:30:00Z"
commit_sha: null
commit: null
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public contracts for OracleNotificationSource and driver notification hooks
  - id: notify-cap-py
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource and NotificationEvent runtime_checkable capability protocols
  - id: db-cdc-py
    resource: src/py/vertebra/db/cdc.py::ChangeFeed
    title: Engine-neutral ChangeFeed and ChangeEvent definitions
  - id: pg-notify-py
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: Reference NotificationSource implementation with ChangeFeed integration
  - id: ora-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver and AsyncOracleDriver target module
tags:
  - oracle
  - notifications
  - aq
  - txeventq
  - python
---

# Task 02: Implement OracleNotificationSource and Reconnect Backoff

## Objective & Scope Boundary

Create `src/py/vertebra/db/adapters/oracle/notify.py` implementing `OracleNotifyConfig` and `@runtime_checkable NotificationSource`-compliant `OracleNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen() -> AsyncIterator[NotificationEvent]`) with optional `change_feed: ChangeFeed | None = None` from `vertebra.db.cdc` and AQ convenience helpers (`subscribe_aq`, `publish_aq`, `listen_aq`, `close`), and wire notification listener/publisher hooks on `SyncOracleDriver` and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-contracts] [^notify-cap-py] [^db-cdc-py] [^pg-notify-py].

- **In Scope**:
  - `OracleNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True)` and `OracleNotificationSource(NotificationSource)` in `src/py/vertebra/db/adapters/oracle/notify.py`.
  - `@runtime_checkable NotificationSource` methods: `publish(self, channel: str, payload: bytes | str) -> None`, `subscribe(self, channels: Sequence[str]) -> None`, `unsubscribe(self, channels: Sequence[str]) -> None`, and `listen(self) -> AsyncIterator[NotificationEvent]` (yielding `NotificationEvent(channel=..., payload=..., metadata=...)` with `payload: bytes`).
  - Optional `*, change_feed: ChangeFeed | None = None` in `OracleNotificationSource.__init__` imported from `vertebra.db.cdc`, plus `decode_json` / `encode_json` from `vertebra.utils.serializers`.
  - AQ / TxEventQ convenience helpers: `subscribe_aq(queue_name, *, consumer_name=None)`, `publish_aq(queue_name, payload, *, correlation_id=None)`, `listen_aq(queue_name=None, *, consumer_name=None)`, and `close()`.
  - Exponential reconnect backoff (`reconnect_initial_delay` -> `reconnect_max_delay`, up to `max_reconnect_attempts`) on transient `OperationalError` / `InterfaceError` during `listen()` / `listen_aq()`.
  - `SyncOracleDriver.publish_notification(...)` and `AsyncOracleDriver.notification_source(...)` / `listen_notifications(...)` / `publish_notification(...)` hooks in `src/py/vertebra/db/adapters/oracle/driver.py`.
- **Out of Scope**:
  - Rust AQ worker thread implementation (completed in `native-oracle-driver:ch06:t01`).
  - Vector embedding pushdown (owned by `native-oracle-driver:ch06:t03`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Section 5 notification contract.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md` — Section 2.2 `OracleNotifyConfig` and `OracleNotificationSource` specification.
3. `src/py/vertebra/db/driver/capabilities.py` — `NotificationSource` and `NotificationEvent` definitions.
4. `src/py/vertebra/db/cdc.py` — `ChangeFeed` and `ChangeEvent` definitions.
5. `src/py/vertebra/db/adapters/postgres/notify.py` — Reference `PostgresNotificationSource` implementation.
6. `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver` class layout.

## Symbol & Signature Contracts

### `src/py/vertebra/db/adapters/oracle/notify.py`

```python
class OracleNotifyConfig(FrozenBaseStruct, frozen=True, gc=False, kw_only=True):
    queue_prefix: str = "VTB_AQ_"
    consumer_name: str | None = None
    wait_timeout_seconds: int = 5
    reconnect_initial_delay: float = 0.25
    reconnect_max_delay: float = 15.0
    max_reconnect_attempts: int = 5
    poll_timeout: float = 1.0


def validate_oracle_channel_name(channel: str, *, queue_prefix: str = "VTB_AQ_") -> str: ...


class OracleNotificationSource(NotificationSource):
    def __init__(
        self,
        driver: AsyncOracleDriver | SyncOracleDriver,
        config: OracleNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> None: ...

    @property
    def change_feed(self) -> ChangeFeed | None: ...

    async def publish(self, channel: str, payload: bytes | str) -> None: ...
    async def subscribe(self, channels: Sequence[str]) -> None: ...
    async def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> AsyncIterator[NotificationEvent]: ...

    async def subscribe_aq(self, queue_name: str, *, consumer_name: str | None = None) -> None: ...
    async def publish_aq(self, queue_name: str, payload: bytes | str, *, correlation_id: str | None = None) -> str: ...
    def listen_aq(
        self,
        queue_name: str | None = None,
        *,
        consumer_name: str | None = None,
    ) -> AsyncIterator[NotificationEvent]: ...
    async def close(self) -> None: ...
```

### `src/py/vertebra/db/adapters/oracle/driver.py`

```python
# On SyncOracleDriver:
def publish_notification(
    self,
    channel: str,
    payload: bytes | str,
    *,
    queue_prefix: str = "VTB_AQ_",
    consumer_name: str | None = None,
    correlation_id: str | None = None,
) -> str: ...

# On AsyncOracleDriver:
def notification_source(
    self,
    config: OracleNotifyConfig | None = None,
    *,
    change_feed: ChangeFeed | None = None,
) -> OracleNotificationSource: ...

async def publish_notification(
    self,
    channel: str,
    payload: bytes | str,
    *,
    queue_prefix: str = "VTB_AQ_",
    consumer_name: str | None = None,
    correlation_id: str | None = None,
) -> str: ...

def listen_notifications(
    self,
    channels: Sequence[str],
    *,
    config: OracleNotifyConfig | None = None,
) -> AsyncIterator[NotificationEvent]: ...
```

## Edge Cases & Error Semantics

- **Runtime Protocol Compliance**: `isinstance(OracleNotificationSource(driver), NotificationSource)` must evaluate to `True`.
- **Channel & Queue Identifier Validation**: `validate_oracle_channel_name(channel, queue_prefix=...)` strips whitespace, uppercases unquoted identifiers, prepends `queue_prefix` when not already prefixed, and verifies the resulting queue identifier matches `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`; empty or invalid names raise `ProgrammingError("Invalid Oracle AQ channel/queue identifier ...", sqlstate="42602")`.
- **Unsubscribed `listen()` Guard**: Calling `listen()` when zero channels have been registered via `subscribe()` / `subscribe_aq()` raises `ProgrammingError("At least one notification channel must be subscribed before calling listen()", sqlstate="42602")` on generator entry.
- **Cancellation & `close()` Cleanup**: When a consumer breaks out of `async for event in source.listen():`, cancels the task (`asyncio.CancelledError`), or calls `await source.close()`, the `finally:` block awaits `listener.close()` on every open `AsyncOracleListener` so dedicated AQ sessions are terminated immediately via `break_execution()`.
- **Exponential Reconnect Backoff**: When `listener.next_notification()` raises a transient `OperationalError` (e.g., `ORA-03113` or `ORA-03135`), `OracleNotificationSource.listen()` closes the broken listener, sleeps `min(reconnect_max_delay, reconnect_initial_delay * (2 ** (attempt - 1)))`, and re-opens the listener up to `max_reconnect_attempts`; non-transient errors (such as `ConnectionFailure` `ORA-01017` or `ProgrammingError`) or exhausted attempts raise immediately.

## Public-Seam Test Blueprint

Add unit tests in `src/py/tests/unit/db/test_oracle_adapter.py`:
- `test_validate_oracle_channel_name_rejects_invalid_identifiers`: verifies valid channel names resolve to uppercase `<queue_prefix><CHANNEL>` and invalid characters (`;`, spaces, leading digits, `> 128` chars) raise `ProgrammingError` (`sqlstate="42602"`).
- `test_oracle_notification_source_protocol_publish_subscribe_listen_and_change_feed`: verifies `isinstance(source, NotificationSource) is True`, `subscribe(["orders"])` + `publish("orders", b"payload")` + `listen()` yields `NotificationEvent(channel="orders", payload=b"payload", ...)`, and configuring `change_feed=ChangeFeed()` wraps published payloads in `ChangeFeed` envelopes via `vertebra.utils.serializers`.
- `test_oracle_notification_source_reconnect_backoff_and_cancellation_cleanup`: simulates a transient `OperationalError(vendor_code=3113, is_transient=True)` followed by recovery, verifying exponential backoff delays (`0.25s`, `0.50s`) and verifying that task cancellation or `await source.close()` awaits `listener.close()` in `finally:`.

## Verification

```bash
uv run pytest src/py/tests/unit/db/test_oracle_adapter.py -k "notify or notification_source"
make lint-all
```

## Acceptance Criteria

- [x] `src/py/vertebra/db/adapters/oracle/notify.py` implements `OracleNotifyConfig`, `validate_oracle_channel_name`, and `@runtime_checkable NotificationSource`-compliant `OracleNotificationSource` (`publish`, `subscribe`, `unsubscribe`, `listen`, `subscribe_aq`, `publish_aq`, `listen_aq`, `close`) using `ChangeFeed` from `vertebra.db.cdc` and `decode_json`/`encode_json` from `vertebra.utils.serializers` with zero inline `#` comments [^ora-contracts] [^notify-cap-py] [^db-cdc-py].
- [x] `SyncOracleDriver` and `AsyncOracleDriver` expose `publish_notification`, `notification_source`, and `listen_notifications` hooks in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-driver-py].
- [x] Unit tests in `src/py/tests/unit/db/test_oracle_adapter.py` and `make lint-all` pass with zero errors.

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public contracts for `OracleNotificationSource` and driver notification hooks.
[^notify-cap-py]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — `NotificationSource` and `NotificationEvent` `runtime_checkable` capability protocols.
[^db-cdc-py]: `src/py/vertebra/db/cdc.py::ChangeFeed` — Engine-neutral `ChangeFeed` and `ChangeEvent` definitions.
[^pg-notify-py]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — Reference `NotificationSource` implementation with `ChangeFeed` integration.
[^ora-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver` and `AsyncOracleDriver` target module.
