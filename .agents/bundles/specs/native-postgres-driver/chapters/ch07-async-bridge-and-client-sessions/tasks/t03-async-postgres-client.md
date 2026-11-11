---
type: Task
id: "native-postgres-driver:ch07:t03"
chapter_id: ch07-async-bridge-and-client-sessions
title: Expose async physical sessions with cancellation-safe publication
description: Implement PyAsyncPostgresClient (AsyncPostgresClient in vertebra._postgres) using _vertebra_postgres_completion_drainer with synchronous connect reservation, map_ok PendingSession publication, and cancellation-race cleanup.
status: stable
state: completed
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch07:t02"
read_set:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/tests/unit/db/test_postgres_driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
estimated_loc: 245
plan_revision: 5
plan_commit: 0147d80c
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
updated_at: "2026-11-08T02:05:22Z"
commit: adf34226
sources:
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop completion queue spawner and DbCompletionDrainer
  - id: pg-driver-tests
    resource: src/py/tests/unit/db/test_postgres_driver.py
    title: Canonical PostgreSQL sync and async native client unit tests
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Complete 25-parameter AsyncPostgresClient constructor and async session contract
tags:
  - postgres
  - pyo3
  - async
  - cancellation
---

# Task t03: Expose Async Physical Sessions with Cancellation-Safe Publication

## Objective & Scope Boundary

Implement `PyAsyncPostgresClient` (`AsyncPostgresClient` in `vertebra._postgres`) in `src/rs/vertebra-postgres/src/async_client.rs`, register it alongside `vertebra_db::py_async::{DbCompletionDrainer, RustDbTaskAwaitable}` in `src/rs/vertebra-postgres/src/lib.rs`, and declare `@final` `AsyncPostgresClient` (`def __new__(cls, *, ...) -> AsyncPostgresClient`) in `src/py/vertebra/_postgres.pyi` while preserving `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool` [^py-async-rs] [^pg-contracts]. `AsyncPostgresClient` shares the exact 25-parameter keyword-only constructor and `SessionHandle` engine with `SyncPostgresClient`, uses dedicated loop attribute `"_vertebra_postgres_completion_drainer"`, commits `PendingSession` exclusively inside `map_ok` on the Python event loop thread, and guarantees deterministic socket/state cleanup across in-flight cancellation, queued-completion cancellation races, and `add_done_callback` setup failures [^pg-driver-tests].

## Bounded Read Manifest

- `src/rs/vertebra-postgres/src/sync_client.rs` — `extract_string_map` and shared 25-parameter `PostgresConfig` construction from `ch07:t02`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `BeginConnect::{Attempt(ConnectAttempt), AlreadyReady}`, `PendingSession`, and `parameters()`.
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr`, `RustDbTaskAwaitable`, `DbCompletionDrainer`, and `ch07:t01` `ArmedWorkerGuard` [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Stub file updated in `ch07:t02`.
- `src/py/tests/unit/db/test_postgres_driver.py` — Loopback socket server fixture from `ch07:t02` [^pg-driver-tests].
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Chapter [spec.md](../spec.md) and [contracts.md](../../../contracts.md) `Connection constructors` [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/async_client.rs`:
  - `pub const POSTGRES_COMPLETION_DRAINER_ATTR: &str = "_vertebra_postgres_completion_drainer";`
  - Annotate `PyAsyncPostgresClient` with conditional `freelist = 128` (`not(Py_GIL_DISABLED)`), `name = "AsyncPostgresClient"`, `module = "vertebra._postgres"`, `frozen`, `immutable_type`, wrapping `session: Arc<SessionHandle>`.
  - `#[new]` with the exact same 25 keyword-only parameters and defaults as `PySyncPostgresClient` (`user`, `database`, `host = "localhost"`, `port = 5432`, `password = None`, `unix_socket = None`, `sslmode = "verify-full", ca_pem = None, client_cert_pem = None, client_key_pem = None, server_name = None, channel_binding = "prefer", connect_timeout = 10.0, application_name = "vertebra", session_gucs = None, keepalives = true, keepalives_idle = None, keepalives_interval = None, keepalives_count = None, cloud_sql_instance = None, cloud_sql_ip_type = "PRIVATE", alloydb_instance_uri = None, alloydb_ip_type = "PRIVATE", enable_iam_auth = false, cloud_auth_options = None, iam_auth_options = None`).
  - `pub fn connect<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>>`:
    - Reserve synchronously via `self.session.begin_connect().map_err(|e| postgres_error_to_py(py, e))?`.
    - For `BeginConnect::AlreadyReady`, schedule/return a guard-free no-op `RustDbTaskAwaitable` owning zero `ConnectAttempt` or `PendingSession` so cancelling that awaitable leaves the existing `Ready` session intact.
    - For `BeginConnect::Attempt(attempt)`, call `spawn_rust_db_task_with_attr(py, POSTGRES_COMPLETION_DRAINER_ATTR, async move { attempt.run().await }, |py, pending| { pending.commit().map_err(|e| postgres_error_to_py(py, e))?; Ok(py.None()) }, |py, err| postgres_error_to_py(py, err))`:
      - Zero `Python::attach` calls or `Py<PyAny>` captures inside the Tokio future.
      - Pre-spawn `get_running_loop` failure drops `attempt` synchronously -> rolls back state to `Closed`.
      - Post-spawn `add_done_callback` failure triggers `ch07:t01` `ArmedWorkerGuard` abort -> drops `attempt`/`PendingSession` -> rolls back to `Closed`.
      - Only `map_ok` calls `pending.commit()`; if the Python `Future` is cancelled while the completed `PendingSession` is sitting in `DbCompletionRing`, `DbCompletionDrainer` skips `produce` and drops `PendingSession`, closing the socket and setting state to `Closed`.
  - `pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>>`:
    - Synchronously transitions `SessionHandle` to `Closed` and takes any live `TransportStream` before scheduling bounded async `Terminate`/shutdown (so cancelling `await client.close()` cannot restore `Ready`).
  - `#[getter] pub fn closed(&self) -> bool` and `#[getter] pub fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>>` (allocating a fresh `PyDict` from `session.parameters()` on each access).
- `src/rs/vertebra-postgres/src/lib.rs` & `src/py/vertebra/_postgres.pyi`:
  - Register `PyAsyncPostgresClient` (keeping `__version__`, `FREE_THREADED`, and `GIL_USED` on `vertebra._postgres`, and keeping `_RustDbTaskAwaitable` / `_DbCompletionDrainer` private to FFI machinery), and declare `@final class AsyncPostgresClient:` in `_postgres.pyi` with `def __new__(cls, *, ...) -> AsyncPostgresClient` (not `def __init__`, matching `#[pyclass(frozen, immutable_type)]` `mypy.stubtest` rules), `def connect(self) -> Awaitable[None]: ...`, `def close(self) -> Awaitable[None]: ...`, and `__all__ = ["FREE_THREADED", "GIL_USED", "AsyncPostgresClient", "SyncPostgresClient", "__version__"]`.

## Edge Cases & Error Semantics

- Overlapping concurrent `client.connect()` calls on one `AsyncPostgresClient` raise `InterfaceError` synchronously on the second call without perturbing the first in-flight connect.
- Cancelling `await client.connect()` on an already `Ready` client raises `asyncio.CancelledError` on that task while `client.closed` remains `False` and the underlying socket stays open.
- Controlled queued-completion cancellation race (forcing fallback wakeup via `NotImplementedError` on `loop.add_reader` for `_DbCompletionDrainer` and holding the drainer callback passed to `loop.call_soon_threadsafe` until after `fut.cancel()`) drops `PendingSession` when the held drainer executes, leaving `client.closed is True` and closing the server socket without timing `sleep`s.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_driver.py` (`test_native_async_*` and `test_native_async_cancel_*`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Concurrent clients, event-loop progress & overlap guard (`test_native_async_lifecycle`) | `8` concurrent `AsyncPostgresClient` connects while a background `asyncio.Task` increments a counter while server holds greeting; plus overlapping `connect()` on one client | Background loop task progresses without blocking; all `8` clients reach `closed is False` with isolated `parameters`; second overlapping `connect()` raises `InterfaceError`. |
| Already-`Ready` no-op cancellation preserves session | Connected `AsyncPostgresClient`; create task `t = asyncio.create_task(client.connect())`, cancel `t`, await `CancelledError` | `client.closed is False` and server socket observes zero EOF/`Terminate`. |
| Setup `add_done_callback` failure cleans up `Connecting` | Custom event loop / `Future` subclass whose `add_done_callback` waits for server-accept `Event` then raises `RuntimeError("cb-fail")` | `client.connect()` raises `RuntimeError("cb-fail")`; server observes EOF after worker abort; subsequent `client.connect()` raises `InterfaceError` for `Closed` (not stuck `Connecting`). |
| In-flight & queued-completion cancellation race (`test_native_async_cancel_*`) | (a) Hold server `ReadyForQuery` frame and cancel task; (b) `loop.add_reader` raises `NotImplementedError` for `_DbCompletionDrainer`, intercept `call_soon_threadsafe` to hold drainer until server finishes `ReadyForQuery` and completion is queued, cancel task, then invoke held drainer | Both raise `asyncio.CancelledError`, server socket receives EOF/close, and `client.closed is True` (never publishing a leaked `Ready` session). |

## Verification

- Initial Red baseline:
  - `make dev && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_async -q`
- Final Green + Stubtest gate:
  - `make dev`
  - `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_driver.py -k native_ -q`
  - `uv run --no-sync python -m mypy.stubtest vertebra._postgres`
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests`

## Acceptance Criteria

- [x] `AsyncPostgresClient` is exported from `vertebra._postgres` with `_vertebra_postgres_completion_drainer` isolation and exact `_postgres.pyi` `Awaitable[None]` signatures passing `mypy.stubtest` (R7).
- [x] `PendingSession` commits exclusively inside `map_ok`; in-flight cancellation, queued-completion cancellation races, and `add_done_callback` setup failures drop `PendingSession`, close the socket, and transition to `Closed` without sleeps or production test hooks (R7).
- [x] Cancelling an already-`Ready` `connect()` no-op preserves the live session, cancelling `close()` never restores `Ready`, and commit `feat(postgres): expose async physical connection client` passes all verification gates (R7).

## Notes & Discoveries

- None.

[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared event-loop completion queue and `DbCompletionDrainer`.
[^pg-driver-tests]: `src/py/tests/unit/db/test_postgres_driver.py` — Canonical PostgreSQL sync and async client unit tests.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native `AsyncPostgresClient` constructor and lifecycle contracts.
