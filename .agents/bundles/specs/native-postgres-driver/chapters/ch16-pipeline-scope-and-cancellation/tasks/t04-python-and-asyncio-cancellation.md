---
type: Task
id: "native-postgres-driver:ch16:t04"
chapter_id: ch16-pipeline-scope-and-cancellation
title: "Expose explicit and asyncio client cancellation with independent native cleanup"
description: "Expose out-of-band SyncPostgresClient.cancel and AsyncPostgresClient.cancel and route asyncio Future cancellation through protocol CancelRequest and native session retirement."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch16:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch16-pipeline-scope-and-cancellation/spec.md
  - src/rs/vertebra-postgres/src/cancel.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-db/src/py_async.rs
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_cancel.py
estimated_loc: 230
plan_revision: 5
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T19:32:00Z"
last_verified_commit: 7b67a4ed
verification_evidence: "pytest test_postgres_native_cancel.py (4 passed) + mypy.stubtest vertebra._postgres + ruff/clippy exit 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen explicit client.cancel, asyncio task cancellation, and pool PID replacement contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared PyO3 Tokio completion bridge and custom cancellation callback hook
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical QueryCancelledError (SQLSTATE 57014) in vertebra._vertebra
tags:
  - postgres
  - cancellation
  - asyncio
  - pyo3
---

# Task t04: Expose Explicit and `asyncio` Client Cancellation with Independent Native Cleanup

## Objective & Scope Boundary

Expose out-of-band `SyncPostgresClient.cancel() -> None` and `AsyncPostgresClient.cancel() -> Awaitable[None]` in `vertebra._postgres` and route `AsyncPostgresClient` operations through `spawn_rust_db_task_with_attr_and_cancel` so cancelled Python `Future`s (`asyncio.wait_for`, `Task.cancel()`) send protocol `CancelRequest` while keeping native drain/retirement alive until socket closure and pool permit recovery complete [^pg-contracts] [^py-async-rs]. High-level `SyncPostgreSQLDriver.cancel()` / `AsyncPostgreSQLDriver.cancel()` active-operation selection belongs to Chapters 25 and 26 (`ch25:t03`–`t04`).

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 16 out-of-band client cancellation and `asyncio` `CancelledError` preservation rules.
- [contracts.md](../../../contracts.md) — `QueryCancelledError` (`57014`), ambient transaction failure retention, and fresh backend PID replacement rules [^pg-contracts].
- `src/rs/vertebra-postgres/src/cancel.rs` (`crate::session::cancel`) — `PostgresCancelHandle`, `OperationGeneration`, and `CancelDisposition` from `ch16:t03`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient` methods.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient` methods.
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr_and_cancel` from `ch16:t03` [^py-async-rs] [^db-errors-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs`:
  - Add `fn cancel(&self, py: Python<'_>) -> PyResult<()>`:
    - Claims the active `OperationGeneration` out-of-band without waiting on the busy session execution lock and runs `PostgresCancelHandle` inside `py.detach(|| block_on_db(...))`.
    - Idle or already-completed generation is a clean `Ok(())` no-op; active wire cancellation causes the blocked `query` / `query_multi` / `pipeline_results` call to drain `ErrorResponse("57014")`, raise `vertebra._vertebra.QueryCancelledError` (`sqlstate == "57014"`), and retire the physical socket [^db-errors-rs].
- `src/rs/vertebra-postgres/src/async_client.rs`:
  - Add `fn cancel(&self, py: Python<'_>) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[None]`) running out-of-band `PostgresCancelHandle` via `_vertebra_postgres_completion_drainer`.
  - Upgrade `AsyncPostgresClient` operation tasks (`query`, `query_multi`, `pipeline_results`, transaction/savepoint commands) to reserve their `OperationGeneration` before spawning and register a nonblocking cancel trigger via `spawn_rust_db_task_with_attr_and_cancel`:
    - When `asyncio.wait_for` or `Future.cancel()` cancels the Python `Future` before wire submission, queued work aborts cleanly without sending a stale `CancelRequest`.
    - When cancelled while in flight on the wire, the callback spawns `PostgresCancelHandle` onto `db_runtime_handle()` while the worker task drains/retires the socket and releases any pooled permit even if the Python `Future`, client, or event loop is dropped; Python caller still observes `asyncio.CancelledError`.
    - Cancellation after result publication into `DbCompletionRing` is a no-op that neither sends `CancelRequest` nor aborts cleanup.
- `src/py/vertebra/_postgres.pyi`:
  - Add `def cancel(self) -> None: ...` on `SyncPostgresClient` and `def cancel(self) -> Awaitable[None]: ...` on `AsyncPostgresClient`.

## Edge Cases & Error Semantics

- **Ambient transaction vs pooled replacement**: Cancelling an operation inside `client.begin()` marks the transaction failed (`InterfaceError` / aborted-transaction rejection on subsequent queries until `client.rollback()`), whereas cancelling an operation on a pooled lease outside a transaction retires the old backend PID and allows the next `pool.acquire()` to obtain a fresh backend PID with full pool capacity restored [^pg-contracts].
- **Repeated cancellation**: Calling `client.cancel()` multiple times concurrently or after `Future.cancel()` is idempotent and never leaks sockets or permits.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_cancel.py` exercising `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, and `AsyncPostgresPool` against coordinated scripted PostgreSQL wire servers.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Explicit `SyncPostgresClient.cancel()` from worker thread | Thread A blocks in `client.query("SELECT pg_sleep(10)")`; Thread B calls `client.cancel()` once server receives `Execute` | Server receives `CancelRequest(pid, key)` on second socket; Thread A raises `vertebra._vertebra.QueryCancelledError` (`sqlstate == "57014"`). |
| Explicit `await AsyncPostgresClient.cancel()` | Task A awaits slow `client.query(...)`; Task B awaits `client.cancel()`; repeat `await client.cancel()` when idle | Task A raises `QueryCancelledError` (`"57014"`); repeated idle `cancel()` returns `None` with `0` extra cancel connections. |
| `asyncio.wait_for` & `Future.cancel()` with dropped client | `await asyncio.wait_for(pooled_client.query("SELECT pg_sleep(10)"), timeout=0.05)` on `max_size=1` `AsyncPostgresPool`, then `await pooled_client.close()` and re-acquire | `wait_for` raises `TimeoutError` (`CancelledError` on inner `Future`), server receives `CancelRequest`, old PID retires, and next `await pool.acquire()` succeeds with a fresh backend PID. |
| Cancelled pipeline & ambient transaction gating | Enter `client.begin()`, cancel in-flight `query` or `pipeline_results`, attempt `client.query("SELECT 1")`, then `client.rollback()` | Post-cancel `query` fails while ambient transaction is failed; `rollback()` completes cleanup cleanly. |
| Cancel after publication / before submission | Cancel `Future` immediately before worker starts I/O, and call `cancel()` right after `ReadyForQuery` publishes | Pre-I/O cancel sends `0` `CancelRequest` packets; post-publication cancel sends `0` `CancelRequest` packets. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_cancel.py -q
  ```
- Final Green, stubtest, Ruff, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_cancel.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_cancel.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SyncPostgresClient.cancel()` and `AsyncPostgresClient.cancel()` trigger out-of-band protocol `CancelRequest` without waiting on busy session locks and match `_postgres.pyi` (R7, R18).
- [ ] `asyncio` `Future` cancellation (`wait_for`, `Task.cancel()`) sends protocol `CancelRequest`, preserves `asyncio.CancelledError`, and completes native drain/retirement + pool permit return even if the `Future` or client is dropped (R7, R18).
- [ ] Outside ambient transactions, subsequent pool acquisitions obtain a fresh backend PID; inside ambient transactions, queries are rejected until explicit `rollback()` (R18).
- [ ] `test_postgres_native_cancel.py`, `mypy.stubtest vertebra._postgres`, Ruff, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen explicit and `asyncio` cancellation, session retirement, and ambient transaction recovery contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared PyO3 Tokio completion bridge and `spawn_rust_db_task_with_attr_and_cancel`.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `QueryCancelledError` exception class.
