---
type: Task
id: "native-postgres-driver:ch17:t03"
chapter_id: ch17-command-deadlines-portals-and-tz-fallback
title: "Expose session-pinned SyncPostgresPortal and AsyncPostgresPortal facades"
description: "Expose immutable SyncPostgresPortal and AsyncPostgresPortal PyO3 classes and open_portal on SyncPostgresClient and AsyncPostgresClient while retaining streaming transaction and lease ownership."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch17:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/python_values.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_portals.py
estimated_loc: 235
plan_revision: 5
plan_commit: 936daa9f
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T20:21:59Z"
last_verified_commit: ffe3e745
verification_evidence: "pytest test_postgres_native_portals.py (9 passed), mypy.stubtest vertebra._postgres, ruff, mypy, and clippy passed"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: f7535c47
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen portal lease-pinning, result projection, and ownership contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL row-limited portal streaming, transaction pinning, and early-close cleanup rules
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared PyO3 Tokio completion bridge and non-aborting cancellation hook
tags:
  - postgres
  - portals
  - streaming
  - pyo3
---

# Task t03: Expose Session-Pinned `SyncPostgresPortal` and `AsyncPostgresPortal` Facades

## Objective & Scope Boundary

Expose immutable `SyncPostgresPortal` and `AsyncPostgresPortal` PyO3 classes and `open_portal` on `SyncPostgresClient` and `AsyncPostgresClient` in `vertebra._postgres`, delivering bounded `PostgresQueryResult` batches while keeping the underlying `SessionHandle` / `PooledConnection` lease and `TransactionScope` pinned until exhaustion or cleanup [^pg-contracts] [^pg-protocol-research]. Zero-copy `VTBARROW` SHM Arrow portal streaming (`ch20:t03`–`t04`) reuses the underlying Rust `Portal` (`start_fetch` / `next_event`) directly without going through Python tuple rows.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 17 `SyncPostgresPortal` and `AsyncPostgresPortal` contracts.
- [contracts.md](../../../contracts.md) — Portal lease pinning, `PostgresQueryResult` shape, and active-stream exclusion rules [^pg-contracts].
- `src/rs/vertebra-postgres/src/portal.rs` — Concrete Rust `Portal` (`fetch`, `close`, `is_exhausted`, `is_closed`, `Drop` cleanup) and `SessionHandle::open_portal` from `ch13:t02` [^pg-protocol-research].
- `src/rs/vertebra-postgres/src/python_values.rs` — `extract_parameters` and `query_result_to_python` from `ch14:t03`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient` and `sync_client::register`.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient`, `async_client::register`, and `spawn_rust_db_task_with_attr_and_cancel` [^py-async-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` (registered via `sync_client::register(m)`):
  - `#[pyclass(name = "SyncPostgresPortal", module = "vertebra._postgres", frozen, immutable_type)] pub struct PySyncPostgresPortal`:
    - `fn fetch(&self, py: Python<'_>, max_rows: isize) -> PyResult<Py<PyAny>>` — rejects `bool` and `max_rows <= 0` or `> i32::MAX` (`InterfaceError` / `ValueError`), runs `Portal::fetch(max_rows as i32)` inside `py.detach(|| block_on_db(...))`, and converts the chunk via `query_result_to_python` after releasing internal portal state locks.
    - `fn close(&self, py: Python<'_>) -> PyResult<()>` — idempotent explicit portal close + `TransactionScope` completion inside `py.detach(|| block_on_db(...))`.
    - `#[getter] fn exhausted(&self) -> bool` and `#[getter] fn closed(&self) -> bool` — atomic/non-I/O status snapshots; zero raw sockets or backend cancellation secrets cross Python.
  - On `PySyncPostgresClient`:
    - `#[pyo3(signature = (sql, parameters=None, *, parameter_oids=None, batch_size=65536))] fn open_portal(&self, py: Python<'_>, sql: &str, parameters: Option<&Bound<'_, PyAny>>, parameter_oids: Option<&Bound<'_, PyAny>>, batch_size: isize) -> PyResult<PySyncPostgresPortal>` (routing through `SessionAccess::Ordinary` so opening a portal inside an active pipeline scope or on a busy client raises `InterfaceError`).
- `src/rs/vertebra-postgres/src/async_client.rs` (registered via `async_client::register(m)`):
  - `#[pyclass(name = "AsyncPostgresPortal", module = "vertebra._postgres", frozen, immutable_type)] pub struct PyAsyncPostgresPortal`:
    - `fn fetch(&self, py: Python<'_>, max_rows: isize) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[PostgresQueryResult]`, wired with `spawn_rust_db_task_with_attr_and_cancel` so cancelling a `fetch` `Future` sends protocol `CancelRequest` and schedules native portal cleanup/retirement without prematurely releasing the pooled lease) [^py-async-rs].
    - `fn close(&self, py: Python<'_>) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[None]`, idempotent).
    - `#[getter] fn exhausted(&self) -> bool` and `#[getter] fn closed(&self) -> bool`.
  - On `PyAsyncPostgresClient`:
    - `#[pyo3(signature = (sql, parameters=None, *, parameter_oids=None, batch_size=65536))] fn open_portal(&self, py: Python<'_>, sql: &str, parameters: Option<&Bound<'_, PyAny>>, parameter_oids: Option<&Bound<'_, PyAny>>, batch_size: isize) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[AsyncPostgresPortal]`, guarding the opened `Portal` in `DbOutcomeProducer` so if the `open_portal` `Future` is cancelled after creation, background `Portal` `Drop` cleanup runs immediately).
- `src/py/vertebra/_postgres.pyi`:
  - Export `SyncPostgresPortal` and `AsyncPostgresPortal` (`__all__` + full typed methods/properties) and add `open_portal` to `SyncPostgresClient` and `AsyncPostgresClient`.

## Edge Cases & Error Semantics

- **Pinned lease & competing client operations**: While a `SyncPostgresPortal` or `AsyncPostgresPortal` is open and neither exhausted nor closed, calling `client.query(...)`, `client.commit()`, `client.close()`, or borrowing the same `max_size=1` pool slot from another waiter fails / blocks until `portal.close()` (or final `fetch` exhaustion) finishes the streaming `TransactionScope`.
- **Repeated `close()` & post-close `fetch()`**: `close()` is idempotent on already-closed or exhausted portals; calling `fetch()` after `close()` raises `InterfaceError`; calling `fetch()` after `exhausted == True` raises `InterfaceError` or returns empty per `Portal::fetch` contract consistently.
- **Unawaited / garbage-collected portal**: Dropping `SyncPostgresPortal` or `AsyncPostgresPortal` without `close()` triggers `Portal`'s nonblocking background cleanup/retirement without deadlocking the GIL or leaking pool capacity.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_portals.py` testing `SyncPostgresPortal` and `AsyncPostgresPortal` over `SyncPostgresPool` / `AsyncPostgresPool` (`max_size=1`) against stateful loopback PostgreSQL servers.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Bounded batch progression to exhaustion | Open portal over 5 rows (`batch_size=2`); call `fetch(2)`, `fetch(2)`, `fetch(2)` | Returns `((0,), (1,))` (`exhausted=False`), `((2,), (3,))` (`exhausted=False`), `((4,),)` (`exhausted=True, closed=True`); client immediately reusable afterward. |
| Early `close()` & idempotent repeat | Open portal on `max_size=1` pooled client, `fetch(1)`, call `close()` twice, then `client.close()` and re-acquire from pool | First `close()` sends `Close(b'P')` + `"ROLLBACK"` and sets `closed=True`; second `close()` is a clean no-op; subsequent `fetch(1)` raises `InterfaceError`; pool slot returns cleanly. |
| Competing client/pool operations blocked | While `portal` is active (`closed=False`), call `client.query("SELECT 1")`, `client.close()`, and second `pool.acquire()` (`acquire_timeout=0.05`) | `client.query` and `client.close` raise `InterfaceError`; second `pool.acquire()` raises `PoolExhaustedError` because the streaming lease remains pinned. |
| Cancelled `AsyncPostgresPortal.fetch` & GC drop | Cancel in-flight `await portal.fetch(10)` via `asyncio.wait_for(..., 0.05)` (and separately `del portal; gc.collect()` without `close()`) | Cancelled `fetch` sends `CancelRequest`, retires socket, and restores `max_size=1` pool capacity with a fresh PID after `await client.close()`. |
| Invalid `max_rows` / `batch_size` | `open_portal(..., batch_size=0)` or `portal.fetch(0)` / `portal.fetch(True)` | Raises `TypeError` / `ValueError` / `InterfaceError` before wire emission. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_portals.py -q
  ```
- Final Green, stubtest, Ruff, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_portals.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_portals.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [x] `SyncPostgresClient.open_portal` and `AsyncPostgresClient.open_portal` return immutable `SyncPostgresPortal` and `AsyncPostgresPortal` handles with `fetch`, `close`, `exhausted`, and `closed` matching `_postgres.pyi` (R6, R7, R16).
- [x] Active portals pin their `SessionHandle` / `PooledConnection` lease and `TransactionScope`, blocking competing client operations and early lease release until exhaustion, explicit `close()`, or native drop/cancel retirement completes (R7, R16).
- [x] Concrete Rust `Portal` remains directly accessible in `vertebra-postgres` for Arrow batch streaming (`ch20:t03`–`t04`) without Python row materialization (R16).
- [x] `test_postgres_native_portals.py`, `mypy.stubtest vertebra._postgres`, Ruff, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen portal streaming and pinned-lease contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL portal transaction lifetime and early-abandonment cleanup rules.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared PyO3 Tokio completion bridge.
