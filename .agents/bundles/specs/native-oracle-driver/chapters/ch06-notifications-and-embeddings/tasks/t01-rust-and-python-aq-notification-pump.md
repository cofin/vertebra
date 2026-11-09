---
type: Task
id: "native-oracle-driver:ch06:t01"
flow_id: native-oracle-driver
chapter_id: ch06-notifications-and-embeddings
title: "Task 01: Implement Rust and Python AQ / TxEventQ Notification Pump"
description: Implement OracleNotification and AsyncOracleListener in vertebra-oracle::notify with a bounded 1024-frame queue, DBMS_AQ.DEQUEUE polling on a dedicated worker thread, out-of-band break_execution wakeup, and AsyncCompletionQueue delivery.
status: stable
state: completed
priority: P1
owner: agent:executor
depends_on:
  - "native-oracle-driver:ch05:t03"
files:
  - src/rs/vertebra-oracle/src/notify.rs
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - .agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/postgres/notify.py
verification_strategy: behavior_tdd
estimated_loc: 210
plan_revision: 3
plan_commit: 769302bc
state_revision: 1
claimed_by: agent:executor
claimed_at: "2026-11-09T17:13:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T17:24:18Z"
last_verified_commit: null
verification_evidence: "cargo test -p vertebra-oracle (16 passed), mypy.stubtest vertebra._oracle, and pytest test_oracle_notification_and_listener_exports_and_stub_parity passed."
created_at: "2026-11-09T00:48:00Z"
updated_at: "2026-11-09T17:24:18Z"
completed_at: "2026-11-09T17:24:18Z"
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
    title: Native Oracle listener and worker-thread completion contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: notify-cap-py
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource and NotificationEvent contracts
tags:
  - oracle
  - rust
  - pyo3
  - aq
  - txeventq
---

# Task 01: Implement Rust and Python AQ / TxEventQ Notification Pump

## Objective & Scope Boundary

Implement the native Rust AQ / TxEventQ notification pump in `src/rs/vertebra-oracle/src/notify.rs`, register `OracleNotification` and `AsyncOracleListener` in `src/rs/vertebra-oracle/src/lib.rs`, and declare their typing contracts in `src/py/vertebra/_oracle.pyi` [^ora-contracts] [^py-async-rs].

- **In Scope**:
  - `OracleNotification` (`#[pyclass(frozen, module = "vertebra._oracle")]`) exposing `queue_name: str`, `consumer_name: str | None`, `msg_id: bytes`, `payload: bytes`, and `enqueued_at_epoch_ms: int | None`.
  - `AsyncOracleListener` (`#[pyclass(frozen, module = "vertebra._oracle")]`) owning a dedicated non-pooled `oracledb::Connection` on a dedicated Rust worker thread that runs bounded `DBMS_AQ.DEQUEUE` iterations, buffers up to `1024` frames (`oldest-drop` backpressure with `dropped_count`), wakes blocked waits out-of-band via `connection.break_execution()` on `close()`, and delivers frames to Python via `spawn_rust_db_task_with_attr` (`_vertebra_oracle_completion_drainer`).
  - `AsyncOracleClient.open_listener` and `SyncOracleClient.enqueue_notification` / `AsyncOracleClient.enqueue_notification` helpers for `DBMS_AQ.ENQUEUE`.
- **Out of Scope**:
  - High-level Python `OracleNotificationSource` reconnect loop and `ChangeFeed` wrapper (owned by `native-oracle-driver:ch06:t02`).

## Bounded Read Manifest

1. `.agents/bundles/specs/native-oracle-driver/contracts.md` — Native `_oracle` exports and worker-thread cancellation rules.
2. `.agents/bundles/specs/native-oracle-driver/chapters/ch06-notifications-and-embeddings/spec.md` — Chapter 06 architecture and AQ dequeue loop design.
3. `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr` completion queue helper.
4. `src/py/vertebra/db/driver/capabilities.py` — `NotificationEvent` and `NotificationSource` semantics.
5. `src/py/vertebra/db/adapters/postgres/notify.py` — Reference native listener lifecycle and backpressure handling.

## Symbol & Signature Contracts

### `src/rs/vertebra-oracle/src/notify.rs` & `src/py/vertebra/_oracle.pyi`

```python
class OracleNotification:
    @property
    def queue_name(self) -> str: ...
    @property
    def consumer_name(self) -> str | None: ...
    @property
    def msg_id(self) -> bytes: ...
    @property
    def payload(self) -> bytes: ...
    @property
    def enqueued_at_epoch_ms(self) -> int | None: ...

class AsyncOracleListener:
    @property
    def queue_name(self) -> str: ...
    @property
    def consumer_name(self) -> str | None: ...
    @property
    def is_closed(self) -> bool: ...
    @property
    def dropped_count(self) -> int: ...
    def next_notification(self, timeout: float | None = None) -> Awaitable[OracleNotification | None]: ...
    def close(self) -> Awaitable[None]: ...
```

And on `AsyncOracleClient` / `SyncOracleClient`:

```python
# AsyncOracleClient
def open_listener(
    self,
    *,
    queue_name: str,
    consumer_name: str | None = None,
    wait_timeout_seconds: int = 5,
    max_buffered: int = 1024,
) -> Awaitable[AsyncOracleListener]: ...

def enqueue_notification(
    self,
    *,
    queue_name: str,
    payload: bytes,
    consumer_name: str | None = None,
    correlation_id: str | None = None,
) -> Awaitable[bytes]: ...

# SyncOracleClient
def enqueue_notification(
    self,
    *,
    queue_name: str,
    payload: bytes,
    consumer_name: str | None = None,
    correlation_id: str | None = None,
) -> bytes: ...
```

## Edge Cases & Error Semantics

- **Identifier Validation Before PL/SQL Construction**: `queue_name` and optional `consumer_name` must match `^[A-Za-z_][A-Za-z0-9_$#]{0,127}$`; invalid identifiers fail fast with `ProgrammingError` (`sqlstate="42602"`) before any PL/SQL block is formatted or executed.
- **Idle Dequeue Timeout (`ORA-25228`)**: When `DBMS_AQ.DEQUEUE` times out after `wait_timeout_seconds` with `ORA-25228`, the worker loop checks its atomic `closed` flag and immediately starts the next dequeue iteration without surfacing an error to Python; if `next_notification(timeout=...)` expires on the caller side, it resolves cleanly to `None`.
- **Bounded Queue & Oldest-Drop Backpressure**: When `max_buffered` (`1024` default) is reached while Python is not draining frames, the worker pops the oldest queued `OracleNotification`, pushes the newest frame, and increments `dropped_count` so memory growth remains strictly bounded.
- **Out-of-Band Break on `close()`**: Calling `await listener.close()` sets `closed = true`, invokes `connection.break_execution()` on the dedicated AQ session so any blocked `DBMS_AQ.DEQUEUE` call returns immediately, suppresses the resulting `ORA-01013` during shutdown, resolves pending waiters with `None`, and is idempotent across repeated `close()` calls.

## Public-Seam Test Blueprint

- **Rust Unit Tests (`src/rs/vertebra-oracle/src/notify.rs`)**:
  - `test_validate_aq_identifier_accepts_valid_and_rejects_injection`: verifies valid Oracle queue/consumer identifiers pass and SQL injection strings or empty names return `ProgrammingError` (`sqlstate="42602"`).
  - `test_listener_buffer_enforces_max_buffered_oldest_drop`: pushes `1026` frames into a `1024`-capacity listener ring, asserting `dropped_count == 2` and the first popped frame is frame `#3`.
  - `test_ora_25228_is_classified_as_idle_dequeue_timeout`: verifies `ORA-25228` is recognized as an idle poll boundary while `ORA-03113` surfaces as a fatal `OperationalError` to pending waiters.
- **Python Unit Tests (`src/py/tests/unit/db/test_oracle_adapter.py`)**:
  - `test_oracle_notification_and_listener_exports_and_stub_parity`: verifies `OracleNotification` (`msg_id`, `payload`, `queue_name`, `consumer_name`, `enqueued_at_epoch_ms`) and `AsyncOracleListener` are exported from `vertebra._oracle` with matching `_oracle.pyi` signatures.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle
uv run python -m mypy.stubtest vertebra._oracle --concise
uv run pytest src/py/tests/unit/db/test_oracle_adapter.py -k "notification or listener"
```

## Acceptance Criteria

- [x] `OracleNotification` (`msg_id: bytes`, `payload: bytes`) and `AsyncOracleListener` are implemented in `src/rs/vertebra-oracle/src/notify.rs`, exported from `vertebra._oracle`, and typed in `src/py/vertebra/_oracle.pyi` [^ora-contracts].
- [x] `AsyncOracleListener` runs `DBMS_AQ.DEQUEUE` on a dedicated worker thread, suppresses `ORA-25228` idle timeouts, enforces `max_buffered` (`1024`) with `dropped_count`, and wakes blocked waits via `connection.break_execution()` on `close()` [^py-async-rs].
- [x] `cargo test -p vertebra-oracle`, `mypy.stubtest vertebra._oracle`, and unit tests in `src/py/tests/unit/db/test_oracle_adapter.py` pass with zero warnings.

## Notes & Discoveries

None yet.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Native Oracle listener and worker-thread completion contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue` and independent event-loop drainer.
[^notify-cap-py]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — `NotificationSource` and `NotificationEvent` contracts.
