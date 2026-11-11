---
type: Task
id: "native-postgres-driver:ch21:t02"
chapter_id: ch21-live-notifications
title: Expose AsyncPostgresListener across sync and async native clients
description: Expose AsyncPostgresListener and open_listener on both SyncPostgresClient and AsyncPostgresClient via _vertebra_postgres_completion_drainer with PoolerMode rejection, read-only closed state, PendingDeliveryGuard cancelled-next retention, and reconnect gap tuples.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch21:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch21-live-notifications/spec.md
  - src/rs/vertebra-postgres/src/notifications.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_notifications.py
estimated_loc: 215
plan_revision: 6
plan_commit: 043d8444
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T23:08:35Z"
last_verified_commit: b2c449c0
verification_evidence: "make dev, ruff, mypy, mypy.stubtest vertebra._postgres, pytest test_postgres_native_notifications.py (47 passed), cargo test postgres_notification_wire_tests (11 passed), and cargo clippy exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T23:08:35Z"
commit: b2c449c0
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: AsyncPostgresListener closed property, open_listener signature, and reconnect contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop AsyncCompletionQueue bridge and cancelled-future cleanup
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL LISTEN/NOTIFY ReadyForQuery acknowledgement and at-most-once gap rules
tags:
  - postgres
  - rust
  - pyo3
  - notifications
  - async
---

# Task t02: Expose AsyncPostgresListener Across Sync and Async Native Clients

## Objective & Scope Boundary

Expose `AsyncPostgresListener` and `open_listener(channels, *, queue_capacity=1024, reconnect=False)` on BOTH `SyncPostgresClient` and `AsyncPostgresClient` in `src/rs/vertebra-postgres/src/{sync_client,async_client}.rs` and `src/py/vertebra/_postgres.pyi` over `_vertebra_postgres_completion_drainer` [^pg-contracts] [^py-async-rs] [^pg-protocol-research]. Python `PostgresNotificationSource` and `events.py` protocol integration follow in `ch21:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 21 `AsyncPostgresListener`, `closed` snapshot, and client `open_listener` contracts.
- `src/rs/vertebra-postgres/src/notifications.rs` — `PostgresListener`, `NotificationFrame`, `ListenerOptions`, `push_front_unconsumed`, and `is_closed()` (`ch21:t01`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient` connection configuration (`crate::config::PostgresConfig` carrying `pooler_mode: PoolerMode`) and shared async drainer access.
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient` and `_vertebra_postgres_completion_drainer` task helpers (`ch07:t01`, `ch20:t04`).
- `src/rs/vertebra-db/src/py_async.rs` — `DbOutcomeProducer` (`FnOnce(Python<'_>) -> PyResult<Py<PyAny>>`) and `DbCompletionDrainer` skip-on-cancelled-future `Drop` semantics [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Public stub definitions verified by `mypy.stubtest`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/async_client.rs`, `sync_client.rs`, & `src/py/vertebra/_postgres.pyi`:
  - `#[pyclass(name = "AsyncPostgresListener", module = "vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresListener`:
    - `@property def closed(self) -> bool` (read-only snapshot delegating to `PostgresListener::is_closed()` with zero I/O; no setter in implementation or `.pyi` stub: `False` while active or during recoverable reconnect backoff, `True` after normal terminal end, unrecoverable error, or `aclose()`, and `True` across repeated `aclose()` calls)
    - `def next(self) -> Awaitable[tuple[int, str, bytes, bool] | None]`:
      - Resolves to `(pid, channel, payload_bytes, reconnected_gap)` or `None` after `aclose()`; concurrent `next()` raises `InterfaceError`.
      - Wraps each popped `NotificationFrame` in an RAII `PendingDeliveryGuard` inside the `map_ok` (`DbOutcomeProducer`: `FnOnce(Python<'_>) -> PyResult<Py<PyAny>>`) closure: when `map_ok` runs on the event loop, it constructs the Python tuple and disarms the guard; when a Tokio `next()` waiter is cancelled OR `DbCompletionDrainer` drops `map_ok` because the caller's Python `asyncio.Future` was cancelled while keeping the listener handle open, `PendingDeliveryGuard::drop` restores the unconsumed frame via `listener.push_front_unconsumed(frame)` so the next `await listener.next()` receives that exact frame in order without loss.
    - `def subscribe(self, channels: Sequence[str]) -> Awaitable[None]` (acknowledges server `ReadyForQuery` before resolving)
    - `def unsubscribe(self, channels: Sequence[str]) -> Awaitable[None]` (acknowledges server `ReadyForQuery` before resolving)
    - `def aclose(self) -> Awaitable[None]` (idempotent; wakes any pending `next()` with `None`, cancels active backoff/waits, clears front unconsumed slot and bounded queue, and marks `closed = True`)
    - `def __aenter__(self) -> Awaitable[AsyncPostgresListener]` and `def __aexit__(self, exc_type: Any = None, exc_val: Any = None, exc_tb: Any = None) -> Awaitable[None]`
  - Both `SyncPostgresClient` and `AsyncPostgresClient` expose:
    - `def open_listener(self, channels: Sequence[str], *, queue_capacity: int = 1024, reconnect: bool = False) -> Awaitable[AsyncPostgresListener]`
    - Validates `queue_capacity > 0` (rejecting `bool` / `<= 0`) and rejects `PoolerMode::Transaction | PoolerMode::Statement | PoolerMode::Auto` on `config.pooler_mode` (requiring explicit `PoolerMode::Direct | PoolerMode::Session`) with `InterfaceError` (`PostgresError::Interface`, also enforced inside `PostgresListener::open`) before dialing any standalone listener socket; spawns standalone `PostgresListener::open` on `db_runtime_handle()` via `_vertebra_postgres_completion_drainer` so `SyncPostgresClient.open_listener(...)` also awaits without blocking the event loop or GIL. (Because raw `SyncPostgresClient.__new__` / `AsyncPostgresClient.__new__` default to `PoolerMode::Direct`, Python `pooler_mode` rejection tests acquire a client from a `SyncPostgresPool` / `AsyncPostgresPool` configured with `pooler_mode` in `("transaction", "statement", "auto")` and assert `0` standalone listener socket accepts when `open_listener` fails with `InterfaceError`.)
    - Wraps the opened `PostgresListener` in a pending-publication guard inside `map_ok` so if the caller cancels the `open_listener(...)` `asyncio.Future` after the Tokio task connects (before the drainer publishes `AsyncPostgresListener`), `Drop` immediately closes the `PostgresListener` and leaves zero orphaned backend sessions.

## Edge Cases & Error Semantics

- Cancelling `listener.next()` while waiting on an idle channel OR after the worker pops a `NotificationFrame` before `DbCompletionDrainer` invokes `map_ok` clears the single-waiter slot and restores any popped frame to the `1`-slot unconsumed front buffer so the subsequent `await listener.next()` receives that exact frame in order without `InterfaceError` or data loss.
- `aclose()` during reconnect backoff cancels the sleep timer immediately and transitions `listener.closed` to `True`.
- Authentication (`28P01`), protocol violation, or queue overflow errors set `listener.closed == True` without retrying even when `reconnect=True`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_notifications.py` (`test_native_listener_*` suite against scripted native PostgreSQL TCP servers, zero fake listener mocks)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `SyncPostgresClient` & `AsyncPostgresClient` `open_listener`, `PoolerMode` rejection, `next()`, `subscribe`, `unsubscribe`, and duplicate `next()` | Acquire clients from `SyncPostgresPool`/`AsyncPostgresPool` configured with `pooler_mode` in `("transaction", "statement", "auto")` and call `open_listener`; open direct/session client on `["orders"]`, receive push, `subscribe(["audit"])`, receive on `"audit"`, `unsubscribe(["orders"])`, and call two concurrent `listener.next()` coroutines | `("transaction", "statement", "auto")` pool-acquired clients raise `InterfaceError` with `0` standalone listener socket accepts; active listener yields `(pid, "orders", b"...", False)` then `(pid, "audit", b"...", False)`; second concurrent `next()` raises `InterfaceError`; `aclose()` wakes pending `next()` with `None`. |
| Cancelled `next()` retention (`PendingDeliveryGuard`) on open handle | Pause event-loop drainer (and test Tokio-wait cancel) while peer pushes frames `f1` and `f2`; cancel first `next()` task after `f1` is queued on drainer while keeping `listener` open; then call `await listener.next()` twice | First cancelled task raises `CancelledError` without dropping `f1`; next two `await listener.next()` calls return `f1` then `f2` in exact wire order (preserving `reconnected_gap` when set). |
| Canonical `test_native_listener_closed_state` & read-only property check | Inspect `listener.closed` when active, during `reconnect=True` backoff, after clean EOF (`reconnect=False`), after overflow/auth error, after `await listener.aclose()`, and after second `await listener.aclose()`; attempt `listener.closed = False` | `False` -> `False` (reconnecting) -> `True` on terminal error/EOF/`aclose()` -> `True` on repeated `aclose()`; `AttributeError` on assignment; `mypy.stubtest` confirms no setter. |
| Reconnect gap (`reconnected_gap=True`), peer drop during `subscribe`, and cancellation in `open_listener` / backoff | (a) Drop peer idle & mid-`subscribe` with `reconnect=True` and refreshed token; (b) cancel `open_listener` task while queued on drainer and cancel `next()` during backoff | (a) Restores full channel set, awaits `ReadyForQuery`, yields next tuple with `reconnected_gap is True`; (b) zero orphaned server connections remain open. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k native_listener -q`
- Final Green + lint + type-check + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_notifications.py
  uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_notifications.py
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k native_listener -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
  ```
- Planned local commit: `feat(postgres): expose notification handles and reconnect state`

## Acceptance Criteria

- [x] Both `SyncPostgresClient` and `AsyncPostgresClient` reject `PoolerMode::Transaction | PoolerMode::Statement | PoolerMode::Auto` (requiring `PoolerMode::Direct | PoolerMode::Session`, verified via `SyncPostgresPool`/`AsyncPostgresPool`-acquired clients with `0` standalone listener socket accepts) with `InterfaceError` and expose `open_listener(...) -> Awaitable[AsyncPostgresListener]` with `next`, `subscribe`, `unsubscribe`, `aclose`, async context methods, and read-only `closed: bool` passing `ruff`, `mypy`, and `mypy.stubtest vertebra._postgres` (R21).
- [x] `test_native_listener_closed_state` verifies `closed` across active (`False`), recoverable reconnect (`False`), terminal EOF/error/`aclose` (`True`), and repeated `aclose` (`True`) with zero I/O (R21).
- [x] Duplicate `next()` raises `InterfaceError`, `PendingDeliveryGuard` + `push_front_unconsumed` restore unconsumed frames on cancelled `next()` so the following `await listener.next()` receives that exact frame in order, reconnect restoration flags `reconnected_gap=True`, and task cancellation during `open_listener` or backoff leaves zero orphaned sessions (R21).

## Notes & Discoveries

`AsyncPostgresListener.next()` combines a retiring-worker barrier (`NextControl` + `WorkerRunGuard`) with `PendingDeliveryGuard` so Tokio-wait cancellation, paused-drainer cancellation, and dropped unawaited futures restore unconsumed `NotificationFrame` items before releasing the single-waiter slot, while `aclose()` / `__aexit__()` run through a non-aborting task cancel callback.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — AsyncPostgresListener closed property, open_listener signature, and reconnect contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue bridge and cancelled-future cleanup.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL LISTEN/NOTIFY ReadyForQuery acknowledgement and at-most-once gap rules.
