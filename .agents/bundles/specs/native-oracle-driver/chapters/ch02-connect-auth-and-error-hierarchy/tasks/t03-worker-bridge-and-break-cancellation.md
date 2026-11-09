---
type: Task
id: "native-oracle-driver:ch02:t03"
chapter_id: ch02-connect-auth-and-error-hierarchy
title: Implement per-connection worker bridge, SyncOracleClient/AsyncOracleClient lifecycle, and out-of-band break_execution cancellation
description: Implement dedicated per-connection Rust worker threads in src/rs/vertebra-oracle/src/client.rs bridged via SyncCompletionQueue and AsyncCompletionQueue (_vertebra_oracle_completion_drainer), wire out-of-band connection.break_execution() cancellation (ORA-01013 -> QueryCancelledError), and export SyncOracleClient and AsyncOracleClient in vertebra._oracle and _oracle.pyi.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch02:t02"
read_set:
  - src/rs/vertebra-oracle/src/config.rs
  - src/rs/vertebra-oracle/src/dialer.rs
  - src/rs/vertebra-oracle/src/errors.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/rs/vertebra-core/src/notify.rs
  - .agents/bundles/specs/native-oracle-driver/contracts.md
files:
  - src/rs/vertebra-oracle/src/client.rs
  - src/rs/vertebra-oracle/src/lib.rs
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 235
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared AsyncCompletionQueue and independent event-loop drainer
  - id: sync-queue-rs
    resource: src/rs/vertebra-core/src/notify.rs::SyncCompletionQueue
    title: Cross-platform synchronous completion queue primitive
  - id: spanner-lib-rs
    resource: src/rs/vertebra-spanner/src/lib.rs::_spanner
    title: Reference PyO3 client class registration and immutable_type enforcement
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public SyncOracleClient, AsyncOracleClient, and cancellation contracts
tags:
  - oracle
  - worker
  - async
  - cancellation
  - pyo3
---

# Task t03: Implement Per-Connection Worker Bridge, `SyncOracleClient`/`AsyncOracleClient` Lifecycle, and Out-of-Band `break_execution` Cancellation

## Objective & Scope Boundary

Implement `src/rs/vertebra-oracle/src/client.rs` owning a dedicated Rust worker thread per `oracledb::Connection`, bridging synchronous requests via `SyncCompletionQueue` inside `py.detach(...)` and asynchronous requests via `spawn_rust_db_task_with_attr` (`ORACLE_COMPLETION_DRAINER_ATTR = "_vertebra_oracle_completion_drainer"`), wiring out-of-band `connection.break_execution()` cancellation (`ORA-01013` -> `QueryCancelledError`, `sqlstate="57014"`, `vendor_code=1013`), and exporting `SyncOracleClient` and `AsyncOracleClient` connection/transaction lifecycle methods (`connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`, `closed`, `in_transaction`) in `src/rs/vertebra-oracle/src/lib.rs` and `src/py/vertebra/_oracle.pyi` [^py-async-rs] [^sync-queue-rs] [^spanner-lib-rs] [^ora-contracts]. `OracleColumn`, `OracleQueryResult`, full 26ai `OSON`/`VECTOR`/LOB codecs, `query`, `execute_many`, `DML RETURNING INTO`, and `arrow 59` `VTBARROW` SHM transfer belong to `ch03-types-bind-and-arrow-shm` (`ch03:t01`–`t03`) ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/vertebra-oracle/src/config.rs` — `NativeOracleConfig` and `RawOracleOptions` from `ch02:t01`.
- `src/rs/vertebra-oracle/src/dialer.rs` — `resolve_endpoint` and `connect_oracledb` from `ch02:t02`.
- `src/rs/vertebra-oracle/src/errors.rs` — `OracleError`, `classify_oracle_error`, and `OracleError::to_py_err` from `ch02:t01`.
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr` and `RustDbTaskAwaitable` [^py-async-rs].
- `src/rs/vertebra-core/src/notify.rs` — `SyncCompletionQueue` [^sync-queue-rs].
- `.agents/bundles/specs/native-oracle-driver/contracts.md` — Constructor signature, worker-thread completion, and cancellation contracts ([../../../contracts.md](../../../contracts.md)) [^ora-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-oracle/src/client.rs`:
  - `pub const ORACLE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_oracle_completion_drainer";`
  - `pub struct OracleBreakHandle` holding a thread-safe reference / callback to invoke `connection.break_execution()` (or abort an active synthetic test sleep/query) out-of-band without waiting on the worker command channel.
  - `pub struct OracleWorkerHandle` spawning a named OS thread (`"vtb-ora-worker"`) that owns the `oracledb::Connection` (lazy-connected on first command or explicit `connect()`), executes `WorkerCommand::{Connect, Execute, Ping, Begin, Commit, Rollback, Savepoint, RollbackToSavepoint, Close}`, and pushes outcomes onto a caller-supplied `SyncCompletionQueue` or Tokio oneshot channel bridged into `spawn_rust_db_task_with_attr`.
  - `#[pyclass(name = "SyncOracleClient", module = "vertebra._oracle", frozen, immutable_type)] pub struct SyncOracleClient` and `#[pyclass(name = "AsyncOracleClient", module = "vertebra._oracle", frozen, immutable_type)] pub struct AsyncOracleClient` implementing the exact 31-kwarg `__new__` constructor from `contracts.md` plus `connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`, and properties `closed: bool` and `in_transaction: bool` (do not define `OracleColumn` or `OracleQueryResult` in `client.rs`; they are defined once in `src/rs/vertebra-oracle/src/types.rs` in `ch03:t01`).
- `src/rs/vertebra-oracle/src/lib.rs`:
  - Register `pub mod client;` and add `SyncOracleClient` and `AsyncOracleClient` to `#[pymodule(gil_used = false)] fn _oracle`.
- `src/py/vertebra/_oracle.pyi`:
  - Add `@final class SyncOracleClient` and `@final class AsyncOracleClient` with exact 31-kwarg `__init__` signatures, connection/transaction lifecycle methods (`connect`, `close`, `ping`, `cancel`, `begin`, `commit`, `rollback`, `savepoint`, `rollback_to_savepoint`, `execute`), and `closed` / `in_transaction` properties matching `src/lib.rs` at `ch02:t03`, and update `__all__`.

## Edge Cases & Error Semantics

- `SyncOracleClient.__new__` and `AsyncOracleClient.__new__` validate `NativeOracleConfig` purely in memory and never perform DNS, file, or socket I/O during construction.
- All blocking worker-thread waits in `SyncOracleClient` execute inside `py.detach(|| ...)` over `SyncCompletionQueue` so the GIL (when enabled) is released and free-threaded `3.14t`/`3.15t` worker threads never block.
- Validate savepoint identifiers (`savepoint(name)` / `rollback_to_savepoint(name)`) against ASCII `[A-Za-z_][A-Za-z0-9_$#]*` before emitting `SAVEPOINT <name>` or `ROLLBACK TO SAVEPOINT <name>`; invalid identifiers raise `ProgrammingError` (`sqlstate="42000"`).
- When `AsyncOracleClient` awaits a worker command via `spawn_rust_db_task_with_attr(..., ORACLE_COMPLETION_DRAINER_ATTR)`, cancelling the returned Python `asyncio.Future` / `RustDbTaskAwaitable` triggers a drop guard on the spawned Rust future that immediately invokes `OracleBreakHandle::break_execution()`.
- Calling `cancel()` when no query is in flight or after `close()` is an idempotent no-op (`Ok(())`).
- Calling `execute()`, `ping()`, `begin()`, `commit()`, `savepoint()`, or `rollback_to_savepoint()` after `close()` raises `InterfaceError` ("Oracle client is closed").

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_oracle_adapter.py` (plus Rust unit tests in `src/rs/vertebra-oracle/src/client.rs` and `mypy.stubtest vertebra._oracle`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| Constructor validation without I/O | `SyncOracleClient(host="nonexistent.invalid", port=1521, service_name="FREEPDB1", user="hr", password="pw")` | Succeeds in memory (`client.closed is False`, `client.in_transaction is False`); invalid `port=0` or `purity="BAD"` raises `InterfaceError`. |
| Immutable PyO3 types & `GIL_USED = False` | Attempt `SyncOracleClient._x = 1`, `AsyncOracleClient._x = 1` | Each raises `TypeError`; `vertebra._oracle.GIL_USED is False`. |
| Sync & async out-of-band `cancel()` -> `QueryCancelledError` | Trigger simulated/in-flight worker delay and call `client.cancel()` (sync) or `task.cancel()` / `client.cancel()` (async) | Worker aborts via `OracleBreakHandle` and raises `QueryCancelledError` (`sqlstate == "57014"`, `vendor_code == 1013`), leaving the worker usable for subsequent calls. |
| Transaction & savepoint state tracking | `begin()`, `savepoint("sp_1")`, `rollback_to_savepoint("sp_1")`, `commit()`, `rollback()`, and invalid savepoint `"sp;drop"` | `in_transaction` transitions `False -> True -> False`; invalid savepoint raises `ProgrammingError`. |
| Event-loop drainer isolation & post-`close()` misuse | Run `AsyncOracleClient` on `asyncio` loop; call `client.close()` then `client.execute("SELECT 1")` | `getattr(loop, "_vertebra_oracle_completion_drainer")` coexists cleanly; `close()` and `cancel()` are idempotent; `execute()` after `close()` raises `InterfaceError`. |

## Verification

Run the Rust tests, Clippy, `make dev`, `mypy.stubtest`, and pytest unit suite:

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings
make dev
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/test_freethreaded_gil.py -v
```

Commit as `feat(oracle): implement worker-thread completion bridge and out-of-band break cancellation`.

## Acceptance Criteria

- [ ] `SyncOracleClient` and `AsyncOracleClient` run each `oracledb::Connection` on a dedicated Rust worker thread bridged via `SyncCompletionQueue` (`py.detach`) and `spawn_rust_db_task_with_attr` (`_vertebra_oracle_completion_drainer`) (R3).
- [ ] Out-of-band `cancel()` and `asyncio` task cancellation invoke `break_execution()` immediately and surface `ORA-01013` as `QueryCancelledError` (`sqlstate="57014"`, `vendor_code=1013`) without wedging the worker thread (R3, R4).
- [ ] `vertebra._oracle` and `src/py/vertebra/_oracle.pyi` export `SyncOracleClient` and `AsyncOracleClient` with `frozen, immutable_type` (leaving `OracleColumn` and `OracleQueryResult` to `ch03:t01`–`t02`), and `mypy.stubtest vertebra._oracle` exits `0` (R1, R3).

## Notes & Discoveries

- [2026-11-09 01:15] Attaching a `Drop` guard inside the future passed to `spawn_rust_db_task_with_attr` ensures that when Python cancels the `asyncio.Future` (which aborts the Tokio wrapper task via `DbTaskCanceller`), the drop guard immediately triggers `OracleBreakHandle::break_execution()` on the worker thread's active `oracledb::Connection`.

[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared `AsyncCompletionQueue` and independent event-loop drainer.
[^sync-queue-rs]: `src/rs/vertebra-core/src/notify.rs::SyncCompletionQueue` — Cross-platform synchronous completion queue primitive.
[^spanner-lib-rs]: `src/rs/vertebra-spanner/src/lib.rs::_spanner` — Reference PyO3 client class registration and `immutable_type` enforcement.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public `SyncOracleClient`, `AsyncOracleClient`, and cancellation contracts.
