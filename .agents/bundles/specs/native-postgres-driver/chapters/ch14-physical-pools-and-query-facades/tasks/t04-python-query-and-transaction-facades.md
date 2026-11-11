---
type: Task
id: "native-postgres-driver:ch14:t04"
chapter_id: ch14-physical-pools-and-query-facades
title: "Expose typed sync and async query and transaction client facades"
description: "Extend SyncPostgresClient and AsyncPostgresClient with typed query, query_multi, transaction, and savepoint methods plus _postgres.pyi stubs."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch14:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch14-physical-pools-and-query-facades/spec.md
  - src/rs/vertebra-postgres/src/python_values.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_queries.py
estimated_loc: 245
plan_revision: 5
plan_commit: 936daa9f
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
commit: e7c0a42f
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen SyncPostgresClient and AsyncPostgresClient query, query_multi, and transaction contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared block_on_db and AsyncCompletionQueue task scheduling helpers
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical vertebra._vertebra exception hierarchy
tags:
  - postgres
  - pyo3
  - client
  - queries
  - transactions
---

# Task t04: Expose Typed Sync and Async Query and Transaction Client Facades

## Objective & Scope Boundary

Expose typed `query`, `query_multi`, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint` methods on `SyncPostgresClient` and `AsyncPostgresClient` in `vertebra._postgres`, synchronized with `src/py/vertebra/_postgres.pyi` [^pg-contracts] [^py-async-rs]. Python pool classes (`ch15:t01`), pipeline methods (`ch16:t02`), cancellation (`ch16:t04`), `timeout=` command deadlines (`ch17:t02`), and portal wrappers (`ch17:t03`) are out of scope.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 14 sync/async query and transaction facade contracts.
- [contracts.md](../../../contracts.md) — `query` vs `query_multi` single-statement/multi-statement rules, `PostgresQueryResult` shape, and `.completed_result` error semantics [^pg-contracts].
- `src/rs/vertebra-postgres/src/python_values.rs` — `extract_parameters`, `query_result_to_python`, and `errors::postgres_error_to_py` from `ch14:t03`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — Existing `PySyncPostgresClient` (`SyncPostgresClient`) from `ch07:t02`.
- `src/rs/vertebra-postgres/src/async_client.rs` — Existing `PyAsyncPostgresClient` (`AsyncPostgresClient`) and `_vertebra_postgres_completion_drainer` from `ch07:t03` [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Public type stub for `vertebra._postgres` [^db-errors-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` (`#[pyclass(name = "SyncPostgresClient", module = "vertebra._postgres", frozen, immutable_type)]`):
  - Provide `pub(crate) fn from_session(session: Arc<SessionHandle>) -> Self` and `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>` so pool (`ch15:t01`) and portal (`ch17:t03`) tasks reuse clean module registration.
  - Exact `#[pymethods]` additions (releasing the GIL via `py.detach(|| block_on_db(...))` after `extract_parameters`, then projecting results after the `SessionHandle` guard drops):
    - `#[pyo3(signature = (sql, parameters=None, *, parameter_oids=None))] fn query(&self, py: Python<'_>, sql: &str, parameters: Option<&Bound<'_, PyAny>>, parameter_oids: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>>` (default empty tuple `()` in `.pyi`).
    - `fn query_multi(&self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>>` returning `tuple[PostgresQueryResult, ...]` (no public `simple_query` alias).
    - `fn begin(&self, py: Python<'_>) -> PyResult<()>`
    - `fn commit(&self, py: Python<'_>) -> PyResult<()>`
    - `fn rollback(&self, py: Python<'_>) -> PyResult<()>`
    - `fn savepoint(&self, py: Python<'_>, token: u64) -> PyResult<()>`
    - `fn release_savepoint(&self, py: Python<'_>, token: u64) -> PyResult<()>`
    - `fn rollback_to_savepoint(&self, py: Python<'_>, token: u64) -> PyResult<()>`
- `src/rs/vertebra-postgres/src/async_client.rs` (`#[pyclass(name = "AsyncPostgresClient", module = "vertebra._postgres", frozen, immutable_type)]`):
  - Provide `pub(crate) fn from_session(session: Arc<SessionHandle>) -> Self` and `pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()>`.
  - Expose identical method names and keyword defaults returning `RustDbTaskAwaitable` (`Awaitable[PostgresQueryResult]`, `Awaitable[tuple[PostgresQueryResult, ...]]`, `Awaitable[None]`) via `spawn_rust_db_task_with_attr(..., "_vertebra_postgres_completion_drainer")`, extracting parameters synchronously before spawning and projecting `QueryResult` / `PostgresError` inside the event-loop completion callback [^py-async-rs].
- `src/py/vertebra/_postgres.pyi`:
  - Import `PostgresQueryResult`, `PostgresValue`, and `PgParameter` from `vertebra.db.adapters.postgres.types` and declare exact typed signatures and defaults (`parameters: Sequence[PostgresValue | PgParameter] = ()`, `*, parameter_oids: Sequence[int] | None = None`) on `SyncPostgresClient` and `AsyncPostgresClient`.

## Edge Cases & Error Semantics

- **Single-statement `query` vs `query_multi`**: `query` always uses the extended protocol; passing multi-statement SQL (`"SELECT 1; SELECT 2"`) raises canonical `ProgrammingError` (`42601`) rather than silently returning one result set. `query_multi` accepts no parameters and returns every simple-query `PostgresQueryResult` in execution order [^pg-contracts].
- **`bool` rejected for `token: int`**: Savepoint `token` rejects Python `bool` (`TypeError`) and negative integers before touching `SessionHandle`.
- **`AfterCompletion.completed_result` end-to-end**: When post-`ReadyForQuery` metadata enrichment hits transport EOF after a side-effecting `INSERT ... RETURNING`, `query()` raises canonical `vertebra._vertebra.ConnectionFailure` carrying `.completed_result` (`PostgresQueryResult` with exact `affected_rows`, `command_tag`, `'I'`/`'T'` status, and fallback text values) with only one `Execute` emitted.
- **Busy or closed client**: Calling any query/transaction method on a closed client or concurrently on an already-busy `SessionHandle` raises `vertebra._vertebra.InterfaceError` without blocking the Python `asyncio` event loop.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_queries.py` exercising `SyncPostgresClient` and `AsyncPostgresClient` against scripted loopback PostgreSQL peers (zero mocks of `_postgres`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Extended `query` rows, DML, & duplicate columns | `client.query("SELECT $1::int4 AS x, 'ok' AS x", [42])` and `client.query("UPDATE t SET v = $1", ["a"])` | Returns frozen `PostgresQueryResult` with 2 `PostgresColumn`s named `"x"`, `rows == ((42, "ok"),)`, `transaction_status == "I"`; DML returns `rows == ()`, `affected_rows == 3`, `command_tag == "UPDATE 3"`. |
| Typed `NULL` & explicit `parameter_oids` | `client.query("SELECT $1", [PgParameter(None, 23)])` and `client.query("SELECT $1", [None], parameter_oids=[25])` | Sends exact parameter OIDs `23` and `25` in `Parse`, returning `rows == ((None,),)`. |
| `query_multi` ordered results | `client.query_multi("SELECT 1 AS a; INSERT INTO t VALUES (1); SELECT 2 AS b")` | Returns 3-element `tuple[PostgresQueryResult, ...]` preserving rows, `affected_rows == 1`, and per-step `transaction_status`. |
| Multi-statement `query` & wrong param count | `client.query("SELECT 1; SELECT 2")` and `client.query("SELECT $1, $2", [1])` | Raises `isinstance(exc, vertebra._vertebra.ProgrammingError)` (`sqlstate == "42601"` / `"08P01"`) with structured diagnostic attributes. |
| Transaction & savepoint lifecycle (`I`/`T`/`E`) | `begin()`, query (`"T"`), `savepoint(1)`, failing query (`IntegrityError`, `"23505"` -> `"E"`), `rollback_to_savepoint(1)`, `release_savepoint(1)`, `commit()` | Transitions `'I'` -> `'T'` -> `'E'` -> `'T'` -> `'I'`; `COMMIT` returning `ROLLBACK` in `'E'` raises `OperationalError`. |
| `AfterCompletion.completed_result` & async loop responsiveness | Drop metadata catalog socket after `INSERT ... RETURNING unknown_oid` `ReadyForQuery`; also run concurrent `asyncio` timer alongside slow `AsyncPostgresClient.query` | Raises `ConnectionFailure` with `exc.completed_result.affected_rows == 1` and 1 wire `Execute`; `asyncio` timer ticks without GIL/loop blockage. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q
  ```
- Final Green, stubtest, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_queries.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SyncPostgresClient` and `AsyncPostgresClient` expose `query`, `query_multi`, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint` matching `_postgres.pyi` and `contracts.md` (R6, R7, R12, R13).
- [ ] Sync calls release the GIL via `py.detach`, async calls deliver via `_vertebra_postgres_completion_drainer` without worker-thread `Python::attach`, and `gil_used = false` / `immutable_type` invariants hold (R6, R7).
- [ ] Canonical `vertebra._vertebra` exceptions preserve `sqlstate`, diagnostics, and `AfterCompletion.completed_result` without replaying completed SQL (R12, R13).
- [ ] `test_postgres_native_queries.py`, `mypy.stubtest vertebra._postgres`, Ruff, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen `query`, `query_multi`, transaction, and `.completed_result` contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio completion ring and `block_on_db` runtime bridge.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `vertebra._vertebra` exception types.
