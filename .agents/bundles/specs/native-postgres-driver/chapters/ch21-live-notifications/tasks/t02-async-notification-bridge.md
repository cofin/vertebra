---
type: Task
id: "native-postgres-driver:ch21:t02"
chapter_id: ch21-live-notifications
title: Expose AsyncPostgresListener across sync and async native clients
description: Expose AsyncPostgresListener and open_listener on both SyncPostgresClient and AsyncPostgresClient via _vertebra_postgres_completion_drainer with read-only closed state, reconnect gap tuples, and cancellation safety.
status: stable
state: pending
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
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_notifications.py
estimated_loc: 215
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
- `src/rs/vertebra-postgres/src/notifications.rs` — `PostgresListener`, `NotificationFrame`, `ListenerOptions`, and `is_closed()` (`ch21:t01`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient` connection configuration (`crate::config::PostgresConfig`), `SessionHandle` `PoolerMode` (`crate::session::statements::PoolerMode`), and shared async drainer access.
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient` and `_vertebra_postgres_completion_drainer` task helpers (`ch07:t01`, `ch20:t04`).
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr` cancellation and drainer semantics [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Public stub definitions verified by `mypy.stubtest`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/async_client.rs`, `sync_client.rs`, & `src/py/vertebra/_postgres.pyi`:
  - `#[pyclass(name = "AsyncPostgresListener", module = "vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresListener`:
    - `@property def closed(self) -> bool` (read-only snapshot delegating to `PostgresListener::is_closed()` with zero I/O; no setter in implementation or `.pyi` stub: `False` while active or during recoverable reconnect backoff, `True` after normal terminal end, unrecoverable error, or `aclose()`, and `True` across repeated `aclose()` calls)
    - `def next(self) -> Awaitable[tuple[int, str, bytes, bool] | None]` (resolves to `(pid, channel, payload_bytes, reconnected_gap)` or `None` after `aclose()`; concurrent `next()` raises `InterfaceError`)
    - `def subscribe(self, channels: Sequence[str]) -> Awaitable[None]` (acknowledges server `ReadyForQuery` before resolving)
    - `def unsubscribe(self, channels: Sequence[str]) -> Awaitable[None]` (acknowledges server `ReadyForQuery` before resolving)
    - `def aclose(self) -> Awaitable[None]` (idempotent; wakes any pending `next()` with `None`, cancels active backoff/waits, clears bounded queue, and marks `closed = True`)
    - `def __aenter__(self) -> Awaitable[AsyncPostgresListener]` and `def __aexit__(...) -> Awaitable[None]`
  - Both `SyncPostgresClient` and `AsyncPostgresClient` expose:
    - `def open_listener(self, channels: Sequence[str], *, queue_capacity: int = 1024, reconnect: bool = False) -> Awaitable[AsyncPostgresListener]`
    - Validates `queue_capacity > 0` (rejecting `bool` / `<= 0`) and rejects `PoolerMode::Transaction` / `PoolerMode::Statement` on pooled `SessionHandle` (`crate::session::statements::PoolerMode`) before scheduling; spawns dedicated `crate::config::PostgresConfig` connection + initial `LISTEN` setup on `db_runtime_handle()` via `_vertebra_postgres_completion_drainer` so `SyncPostgresClient.open_listener(...)` also awaits without blocking the event loop or GIL.
    - Wraps the opened `PostgresListener` in a pending-publication guard inside `map_ok` so if the caller cancels the `open_listener(...)` `asyncio.Future` after the Tokio task connects (before the drainer publishes `AsyncPostgresListener`), `Drop` immediately closes the `PostgresListener` and leaves zero orphaned backend sessions.

## Edge Cases & Error Semantics

- Cancelling `listener.next()` while waiting on an idle channel clears the single-waiter slot cleanly so a subsequent `await listener.next()` or `await listener.aclose()` succeeds without `InterfaceError` or lost session state.
- `aclose()` during reconnect backoff cancels the sleep timer immediately and transitions `listener.closed` to `True`.
- Authentication (`28P01`), protocol violation, or queue overflow errors set `listener.closed == True` without retrying even when `reconnect=True`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_notifications.py` (`test_native_listener_*` suite against scripted native PostgreSQL TCP servers, zero fake listener mocks)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `SyncPostgresClient` & `AsyncPostgresClient` `open_listener`, `next()`, `subscribe`, `unsubscribe`, and duplicate `next()` | Open on `["orders"]`, receive push, `subscribe(["audit"])`, receive on `"audit"`, `unsubscribe(["orders"])`, and call two concurrent `listener.next()` coroutines | Yields `(pid, "orders", b"...", False)` then `(pid, "audit", b"...", False)`; second concurrent `next()` raises `InterfaceError`; `aclose()` wakes pending `next()` with `None`. |
| Canonical `test_native_listener_closed_state` & read-only property check | Inspect `listener.closed` when active, during `reconnect=True` backoff, after clean EOF (`reconnect=False`), after overflow/auth error, after `await listener.aclose()`, and after second `await listener.aclose()`; attempt `listener.closed = False` | `False` -> `False` (reconnecting) -> `True` on terminal error/EOF/`aclose()` -> `True` on repeated `aclose()`; `AttributeError` on assignment; `mypy.stubtest` confirms no setter. |
| Reconnect gap (`reconnected_gap=True`), peer drop during `subscribe`, and cancellation in `open_listener` / `next` / backoff | (a) Drop peer idle & mid-`subscribe` with `reconnect=True` and refreshed token; (b) cancel `open_listener` task while queued on drainer and cancel `next()` during backoff | (a) Restores full channel set, awaits `ReadyForQuery`, yields next tuple with `reconnected_gap is True`; (b) zero orphaned server connections remain open. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k native_listener -q`
- Final Green + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_notifications.py -k native_listener -q
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_notification_wire_tests
  ```
- Planned local commit: `feat(postgres): expose notification handles and reconnect state`

## Acceptance Criteria

- [ ] Both `SyncPostgresClient` and `AsyncPostgresClient` expose `open_listener(...) -> Awaitable[AsyncPostgresListener]` with `next`, `subscribe`, `unsubscribe`, `aclose`, async context methods, and read-only `closed: bool` passing `mypy.stubtest vertebra._postgres` (R21).
- [ ] `test_native_listener_closed_state` verifies `closed` across active (`False`), recoverable reconnect (`False`), terminal EOF/error/`aclose` (`True`), and repeated `aclose` (`True`) with zero I/O (R21).
- [ ] Duplicate `next()` raises `InterfaceError`, reconnect restoration flags `reconnected_gap=True` without claiming durability, and task cancellation during `open_listener`, `next()`, or backoff leaves zero orphaned sessions (R21).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — AsyncPostgresListener closed property, open_listener signature, and reconnect contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue bridge and cancelled-future cleanup.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL LISTEN/NOTIFY ReadyForQuery acknowledgement and at-most-once gap rules.
