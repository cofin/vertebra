---
type: Task
id: "native-cursor-data-path:single:t12"
chapter_id: single-call-paths
title: "PostgreSQL async single-call methods on client and pool, completed fetch awaitables"
description: "AsyncPostgresClient and AsyncPostgresPool gain the four single-call methods, each returning one awaitable, and AsyncPostgresCursor fetches return an awaitable that is already complete."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:single:t11"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t01-ready-awaitable.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t11-postgres-sync-single-call.md"
  - "src/rs/vertebra-postgres/src/cursor.rs:774-933"
  - "src/rs/vertebra-postgres/src/cursor.rs:973-1151"
  - "src/rs/vertebra-postgres/src/cursor.rs:1358-1430"
  - "src/rs/vertebra-postgres/src/async_client.rs:1788-1792"
  - "src/rs/vertebra-postgres/src/python_pool.rs:501-511"
  - "src/rs/vertebra-postgres/src/python_pool.rs:636-664"
  - "src/rs/vertebra-db/src/py_async.rs:1-25"
  - "src/py/vertebra/_postgres.pyi:304-342"
  - "src/py/vertebra/_postgres.pyi:530-566"
  - "src/py/vertebra/_postgres.pyi:697-750"
  - "src/py/tests/unit/db/test_postgres_native_queries.py:1480-1860"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/async_client.rs"
  - "src/rs/vertebra-postgres/src/python_pool.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_queries.py"
  - "src/py/tests/unit/db/test_postgres_native_pools.py"
estimated_loc: 280
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
created_at: 2026-11-21T16:23:02Z
commit: null
sources:
  - id: root-spec
    resource: .agents/bundles/specs/native-cursor-data-path/spec.md
    title: "Root spec, sections Single-call methods (R16) and Completed awaitables (R17)"
  - id: pg-async-cursor
    resource: "src/rs/vertebra-postgres/src/cursor.rs::PyAsyncPostgresCursor"
    title: "Async cursor whose fetch methods spawn a task today"
  - id: hot-path-research
    resource: .agents/bundles/research/native-cursor-hot-paths_20261121.md
    title: "Section 5: two awaitables and four thread crossings per async query"
tags: [postgres, rust, pyo3, async, performance]
---

# PostgreSQL async single-call methods on client and pool, completed fetch awaitables

## Objective & Scope Boundary

An async query awaits one awaitable and gets finished Python objects back. A fetch on an async cursor returns at once, without a task and without a trip through the event loop.

`estimated_loc` counts about 190 Rust lines and 60 stub lines, plus about 30 lines removed from the fetch methods. Tests add about 170 lines.

Out of scope:

- `ReadyAwaitable`, `ready_ok`, and `ready_err`. Task t01 adds them.
- The Python drivers (t13, t14).
- `executemany` on the async cursor. It keeps its task.

## Bounded Read Manifest

- Root spec, sections "Single-call methods (R16)" and "Completed awaitables (R17)".
- The t01 worksheet for the helper signatures: `ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>` and `ready_err(py: Python<'_>, err: PyErr) -> PyResult<Py<ReadyAwaitable>>`. Both and `ReadyAwaitable` are imported from `py_async` in `vertebra-db`.
- The t11 worksheet for `StatementCall`, `prepare_statement_call`, `ensure_source_ready`, and the four result builders.
- `execute` on `PyAsyncPostgresCursor` in `cursor.rs` (`:978-1151`) holds the cancellable statement run inline. `AsyncOpControl` (`:774-824`) carries the cancel state.
- `cursor.rs:1358-1430`: `fetchone`, `fetchmany`, `fetchall`, and `close` each call `spawn_rust_db_task_with_attr` with a future that does nothing.

## Contracts & Edge Cases

### Shared cancellable runner in `cursor.rs`

- `AsyncOpControl` becomes `pub(crate)`.
- Add `pub(crate) async fn run_statement_cancellable(source: CursorSource, call: StatementCall, budget: CommandBudget, lazy_begin: bool, control: Arc<AsyncOpControl>) -> Result<Option<QueryResult>, PostgresError>`. It is the body of the task in `execute` today, from the first `control.is_cancelled()` check to the query result, without the execute lock and without the in-flight guard. It returns `Ok(None)` wherever today's code returns early because the operation was cancelled before a generation was registered. `call.timeout` is not read; the budget is started on the calling thread, as today.
- `execute` on `PyAsyncPostgresCursor` calls it inside its execute lock and keeps its in-flight guard, its three state closures, and its cancel closure. Its behavior does not change.
- Add:

```rust
pub(crate) fn spawn_single_call<'py>(
    py: Python<'py>,
    source: CursorSource,
    closed_pool_message: &str,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
    parameter_oids: Option<&Bound<'py, PyAny>>,
    timeout: Option<&Bound<'py, PyAny>>,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
```

Steps, in order:

1. `prepare_statement_call`, then `ensure_source_ready`, then `start_from_duration(call.timeout)` on `CommandBudget`. All three run on the calling thread, and an error from any of them is raised by the call, not by the await. This matches the async cursor's `execute`.
2. Create one `AsyncOpControl`. Keep the session handle for the cancel closure when the source is a session; a pool source has none.
3. Call `spawn_rust_db_task_with_attr_and_cancel` once with `POSTGRES_COMPLETION_DRAINER_ATTR`. The future is `run_statement_cancellable(source, call, budget, false, control)`. The success closure runs on the loop thread and returns `build(py, &result)` for `Some(result)` and `None` for `Ok(None)`. The error closure is `postgres_error_to_py`. The cancel closure calls `control.cancel(session)`.

No cursor state exists, so there is no in-flight rule for a single-call method. Two single-call awaitables on one client are ordered by the session, as two cursors on one client are today.

### Python methods

On `PyAsyncPostgresClient` (`async_client.rs`, next to `cursor()` at `:1788-1792`) and on `PyAsyncPostgresPool` (`python_pool.rs`, next to `cursor()` at `:654-664`), four methods each:

```rust
#[pyo3(
    signature = (sql, params = None, *, parameter_oids = None, timeout = None),
    text_signature = "($self, sql, params=(), *, parameter_oids=None, timeout=None)"
)]
pub fn fetch_rows<'py>(
    &self,
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
    parameter_oids: Option<&Bound<'py, PyAny>>,
    timeout: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
```

`fetch_dicts`, `fetch_value`, and `execute_dml` have the same parameters. Each body is one call to `spawn_single_call` in the crate's `cursor` module with the builder from t11 (`rows_and_names`, `row_dicts`, `first_value`, `dml_outcome`). The client passes the `Session` variant of `CursorSource`. The pool passes the `Pool` variant of `CursorSource` and the message `cannot run a statement on a closed pool`.

- The awaitable resolves to the same value the sync method returns.
- `fetch_value` on an empty result raises `NotFoundError` when awaited.
- Cancelling the awaiting task cancels the statement on the server for a client. The client stays usable. A pool method has no session to cancel through; the one-shot path retires or resets the connection by its existing rules.
- A client method inside an open transaction runs in it. A pool method runs in autocommit.

### Completed fetch awaitables on `PyAsyncPostgresCursor`

Today the cursor state errors of these methods surface when the awaitable is awaited, because they are raised inside the completion closure. That rule is kept: an error is returned through `ready_err`.

Add one private function:

```rust
fn ready_from(py: Python<'_>, outcome: PyResult<Py<PyAny>>) -> PyResult<Py<ReadyAwaitable>>
```

It returns `ready_ok(py, value)` for `Ok` and `ready_err(py, err)` for `Err`.

| Method | New signature | Value computed at the call |
| --- | --- | --- |
| `fetchone` | `pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | `lock_cursor_state(&self.state).fetchone(py)`, with `None` for no row |
| `fetchmany` | `pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>`, still `#[pyo3(signature = (size = None))]` | `ensure_open`, the negative-size `ProgrammingError`, then `fetchmany` |
| `fetchall` | `pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | `fetchall` as a list |
| `close` | `pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | Closes the state; the value is `None` |

- Rows are taken from the buffer when the method is called, not when the result is awaited.
- `close` never does I/O on this cursor, so it always returns the completed awaitable. An `execute` that is in flight sees the closed state in its completion closure and clears its result, as today.
- A fetch does not check the in-flight flag, as today. A second `execute` or `executemany` while one is in flight still raises `InterfaceError` with `cursor is already executing`.
- Remove the `spawn_rust_db_task_with_attr` import from `cursor.rs` when these four were its last users.

### Stub

Add to `AsyncPostgresClient` and to `AsyncPostgresPool` the four methods with the parameter lists from t11 and these return types:

| Method | Return type |
| --- | --- |
| `fetch_rows` | `Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]` |
| `fetch_dicts` | `Awaitable[list[dict[str, Any]]]` |
| `fetch_value` | `Awaitable[Any]` |
| `execute_dml` | `Awaitable[tuple[int, None]]` |

The stub lines of the four cursor fetch methods do not change.

## Test Matrix

Tests named `test_async_single_call_*` and `test_async_cursor_fetch_ready_*` go in `test_postgres_native_queries.py`. Pool tests named `test_async_pool_single_call_*` go in `test_postgres_native_pools.py`. Each extension has its own copy of the awaitable class, so tests check behavior or `type(x).__name__` and never `isinstance`.

| Case | Input | Expected |
| --- | --- | --- |
| Fetch does not iterate the loop | After `await cur.execute(sql)`: `loop.call_soon(ran.append, 1)`, then `rows = await cur.fetchall()` | `rows` correct and `ran == []` |
| Same for the other three | `fetchone`, `fetchmany(2)`, `close` | `ran == []` after each await |
| Completed type | `type(cur.fetchall()).__name__` | `"ReadyAwaitable"` |
| Error on await | `await cur.close()`, then `aw = cur.fetchall()` | The call returns; `await aw` raises `InterfaceError` |
| Negative size | `aw = cur.fetchmany(-1)` | The call returns; `await aw` raises `ProgrammingError` |
| One awaitable per pool call | `aw = pool.fetch_rows("SELECT 1 AS a")`, counted by a wrapper whose `__await__` increments a counter | The call returns one object that is not a coroutine; one await yields `([(1,)], ("a",))`; the counter is 1 |
| Select equality | `await client.fetch_rows(sql)` against `cursor().execute(sql)` then `fetchall` | Same rows and names |
| Dicts, value, DML | The t11 matrix rows, awaited, on client and pool | Same values as the sync methods |
| Not found | `await client.fetch_value(select_without_rows)` | `NotFoundError` |
| Call-time error | `client.fetch_rows("SELECT 1", (1,))` | `ProgrammingError` raised by the call |
| Server error | Unique violation | `IntegrityError` on await |
| Timeout | `timeout` shorter than the scripted delay | The class the async cursor raises |
| Cancel | Cancel the task awaiting `client.fetch_rows(slow_sql)` | The server sees a cancel request; the next statement on the client succeeds |
| Concurrent pool calls | `asyncio.gather` of five `pool.fetch_value` | Five values |
| Closed pool | `await pool.close()` then `pool.fetch_rows(sql)` | `InterfaceError` |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make postgres-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_postgres_native_pools.py -p no:xdist -q`
- `test -z "$(git grep -n 'async { Ok::<(), PostgresError>(()) }' -- src/rs/vertebra-postgres/src/cursor.rs)"`

## Acceptance Criteria

- [ ] The four fetch methods of `PyAsyncPostgresCursor` spawn no task and return `Py<ReadyAwaitable>`.
- [ ] Each of the eight new methods makes one call to `spawn_rust_db_task_with_attr_and_cancel` and builds its result in the completion closure.
- [ ] `execute` on `PyAsyncPostgresCursor` and the single-call methods share `run_statement_cancellable`.
- [ ] The stub diff adds eight methods and no class, and `mypy.stubtest` passes with the allow-list unchanged.
- [ ] Existing async cursor tests for cancel, concurrent execute, and lazy begin pass unchanged.

## Notes & Discoveries

- `SessionHandle::claim_cancel_on_shared` unconditionally marks an in-flight cancelled session for retirement (`retire_on_finish = true`, `retire_pending = true`), so cancelling an in-flight `AsyncPostgresClient.fetch_rows()` sends PostgreSQL `CancelRequest` and closes the session (`client.closed is True`), matching `test_async_cursor_cancellation_cancels_server_and_recovers` and `postgres-limitations.md`. Cancelling a queued single-call statement before wire submission returns `Ok(None)` from `run_statement_cancellable` and leaves the client usable for subsequent statements; both paths are covered in `test_async_single_call_cancellation`.
- All unit tests ran against in-process loopback PostgreSQL Protocol 3.0 wire servers rather than a live PostgreSQL instance.
- Under the sandboxed filesystem, `~/.cache/uv` is read-only; running `maturin develop`, `uv run --no-sync`, and `make` targets with `UV_CACHE_DIR=/tmp/uv-cache UV_NO_SYNC=1` succeeds cleanly.
