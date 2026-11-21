---
type: Task
id: "native-cursor-data-path:single:t16"
chapter_id: single-call-paths
title: "Oracle async single-call methods and completed fetch awaitables"
description: "AsyncOracleClient and AsyncOraclePool gain the four single-call methods, each returning one awaitable, and AsyncOracleCursor fetches return an awaitable that is already complete."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:single:t15"
  - "native-cursor-data-path:single:t31"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t01-ready-awaitable.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t15-oracle-bind-and-sync-single-call.md"
  - ".agents/bundles/specs/native-cursor-data-path/chapters/single-call-paths/tasks/t31-oracle-pool-release-reset.md"
  - "src/rs/vertebra-oracle/src/cursor.rs:1216-1290"
  - "src/rs/vertebra-oracle/src/cursor.rs:1377-1443"
  - "src/rs/vertebra-oracle/src/cursor.rs:1529-1594"
  - "src/rs/vertebra-oracle/src/client.rs:2174-2228"
  - "src/rs/vertebra-oracle/src/client.rs:3213-3244"
  - "src/rs/vertebra-oracle/src/pool.rs:426-502"
  - "src/rs/vertebra-oracle/src/pool.rs:1016-1028"
  - "src/rs/vertebra-oracle/src/pool.rs:1197-1272"
  - "src/rs/vertebra-db/src/py_async.rs:1-25"
  - "src/py/vertebra/_oracle.pyi:332-506"
  - "src/py/vertebra/_oracle.pyi:607-700"
files:
  - "src/rs/vertebra-oracle/src/cursor.rs"
  - "src/rs/vertebra-oracle/src/client.rs"
  - "src/rs/vertebra-oracle/src/pool.rs"
  - "src/py/vertebra/_oracle.pyi"
tests:
  - "src/py/tests/integration/db/test_oracle_native.py"
estimated_loc: 250
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
  - id: oracle-async-cursor
    resource: "src/rs/vertebra-oracle/src/cursor.rs::AsyncOracleCursor"
    title: "Async cursor whose fetch methods spawn a task through ready_awaitable"
  - id: oracle-async-pool
    resource: "src/rs/vertebra-oracle/src/pool.rs::AsyncOraclePool"
    title: "Async pool class"
tags: [oracle, rust, pyo3, async, performance]
---

# Oracle async single-call methods and completed fetch awaitables

## Objective & Scope Boundary

An async Oracle query awaits one awaitable and gets finished Python objects back. A fetch on an async cursor returns at once, without a task and without a trip through the event loop.

`estimated_loc` counts about 180 Rust lines and 70 stub lines. Tests add about 140 lines.

Out of scope:

- `ReadyAwaitable`, `ready_ok`, and `ready_err` (t01).
- The bind fast path, `prepare_execute`, `build_reply`, and the four shapers (t15).
- The release reset itself (t31). This task depends on t31 because both edit `client.rs` and `pool.rs`.
- The Python drivers (t17, t18).
- `executemany` on the async cursor. It keeps its task.

## Bounded Read Manifest

- The t01 worksheet for the helper signatures: `ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>` and `ready_err(py: Python<'_>, err: PyErr) -> PyResult<Py<ReadyAwaitable>>`. Both and `ReadyAwaitable` are imported from `py_async` in `vertebra-db`.
- The t15 worksheet for `PreparedExecute`, `prepare_execute`, `BuiltReply`, `build_reply`, the shapers, `is_fatal_transport`, and the method signature.
- `ready_awaitable` in `cursor.rs` (`:1273-1290`) wraps a ready value in a spawned task. `fetchone`, `fetchmany`, `fetchall`, and `close` (`:1529-1594`) call it.
- `execute` on `AsyncOracleCursor` in `cursor.rs` (`:1381-1443`) is the async statement run.
- `dispatch_async_raw` on `OracleWorkerHandle` in `client.rs` (`:2174-2228`) sends one command and breaks the statement when its future is dropped.
- `acquire_async` on `OraclePoolCore` in `pool.rs` (`:426-502`). `AsyncOraclePool.release` (`:1250-1272`) runs `release_sync` on a blocking thread.

## Contracts & Edge Cases

### Completed fetch awaitables on `AsyncOracleCursor`

Today these methods compute their value at the call, and a cursor state error is raised by the call itself, before any awaitable exists. That rule is kept: errors keep propagating with `?` from the call. `ready_err` is not used in this crate.

| Method | New signature | Body |
| --- | --- | --- |
| `fetchone` | `pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | Compute the row under the state lock as today; `ready_ok(py, row_or_none)` |
| `fetchmany` | `pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>`, still `#[pyo3(signature = (size = None))]` | `ensure_open`, `check_fetchmany_size`, `fetchmany`; `ready_ok` with the list |
| `fetchall` | `pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | `ready_ok` with the list |
| `close` | `pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>` | Close the state; `ready_ok(py, py.None())` |

- The Python value is built at the call, under the attached interpreter.
- `close` never does I/O on this cursor, so it always returns the completed awaitable.
- Delete `ready_awaitable`. These four were its only callers.
- A fetch does not check the executing flag, as today. A second `execute` or `executemany` while one is in flight still raises `InterfaceError` with `cursor is already executing`.

### Async runner in `cursor.rs`

Add:

```rust
pub(crate) fn spawn_single_call(
    py: Python<'_>,
    worker: Arc<OracleWorkerHandle>,
    prepared: PreparedExecute,
    shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>,
) -> PyResult<RustDbTaskAwaitable>
```

It calls `spawn_rust_db_task_with_attr` once with `ORACLE_COMPLETION_DRAINER_ATTR`. The future is `worker.dispatch_async_raw(cmd)`. The success closure runs on the loop thread and returns `shape(py, build_reply(...)?)`. The error closure is `err.to_py_err(py)`.

- Parameter and option errors are raised by the call, not by the await, because `prepare_execute` runs on the calling thread. This matches the async cursor's `execute`.
- Cancelling the awaiting task drops the future, and `dispatch_async_raw` breaks the statement, as it does for the cursor.
- No cursor state exists, so there is no in-flight rule for a single-call method.
- `execute` on `AsyncOracleCursor` calls `prepare_execute` and keeps its executing guard and its state handling.

### Python methods

On `AsyncOracleClient` (`client.rs`, next to `cursor()` at `:3213-3244`) and on `AsyncOraclePool` (`pool.rs`), four methods each, with the `#[pyo3(signature = ...)]` and `text_signature` lines from t15 and the return type `PyResult<RustDbTaskAwaitable>`.

Client body: parse `vector_return_format`, build `OracleValueOptions`, `prepare_execute`, then `spawn_single_call` with the shaper from t15.

Pool body: parse the options and call `prepare_execute` first. Then one `spawn_rust_db_task_with_attr` whose future does, in order:

1. `inner.acquire_async(None, None, None, None, None, None, None).await`.
2. `worker.dispatch_async_raw(cmd).await`.
3. `discard` is true when `worker.is_closed()`, or when the statement failed and `is_fatal_transport(&err)`.
4. Tokio's `spawn_blocking` around `inner.release_sync(&worker, None, discard)`, awaited. A panic in that task becomes the `Interface` variant of `OracleError`, as in `AsyncOraclePool.release`.

The success closure builds the reply and shapes it on the loop thread.

- A failed statement raises its own error even when the release also fails. A failed release after a successful statement raises the release error.
- The connection is returned when the awaiting task is cancelled. Hold the worker in a guard whose `Drop` spawns the blocking release with `discard` true, and disarm the guard once step 4 has started. A cancelled statement was broken mid-flight, so its connection is not reused.
- The acquire passes no session attributes and uses the pool defaults.
- The awaitable of `execute_dml` resolves to the four-item tuple of t15.

### Release without a rollback round trip (async)

Task t31 changes `release_sync` to send `ResetIfDirty` in place of an unconditional rollback. Every async release path calls `release_sync` on a blocking thread: `AsyncOraclePool.release`, `release_to_pool`, and step 4 above. No further edit to the release is needed here.

- The reset command travels through the worker channel, behind any command still queued for that worker. A statement whose awaiting task was cancelled is therefore handled before the reset decides.
- The cancel guard above releases with `discard` true, so a cancelled single call closes its connection and sends no reset.
- The rule table in t31 applies unchanged: an async pool `execute_dml` that commits releases with no round trip; an async pool query still sends the rollback.

### Stub

Add to `AsyncOracleClient` and to `AsyncOraclePool` the four methods with the parameter list from t15 and these return types, in the single-line `...` form the file uses:

| Method | Return type |
| --- | --- |
| `fetch_rows` | `Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]` |
| `fetch_dicts` | `Awaitable[list[dict[str, Any]]]` |
| `fetch_value` | `Awaitable[Any]` |
| `execute_dml` | `Awaitable[tuple[int, str \| None, dict[str, Any] \| tuple[Any, ...] \| None, bool]]` |

The stub lines of the four cursor fetch methods do not change.

## Test Matrix

Rust unit tests go in the existing `tests` module of `cursor.rs`. They build an `AsyncOracleCursor` on a worker handle that has no connection (`spawn` on `OracleWorkerHandle` on a config from `validate` on `NativeOracleConfig`, as `worker_zero_io_spawn_session_attr_staging_and_break_handle_lifecycle` in `client.rs` does), store a result with `store` on `CursorState`, and drive the returned object from Python with `__await__` and `next`. Each extension has its own copy of the awaitable class, so tests check behavior or `type(x).__name__` and never `isinstance`.

Live tests named `test_async_single_call_*` and `test_async_cursor_fetch_ready_*` go in `src/py/tests/integration/db/test_oracle_native.py` and are skipped without a database.

| Case | Input | Expected |
| --- | --- | --- |
| Completed type (Rust) | `cursor.fetchall(py)` after a stored result | Type name `ReadyAwaitable` |
| No yield (Rust) | `next(aw.__await__())` | `StopIteration` whose value is the row list, on the first `next` |
| Fetchone, fetchmany, close (Rust) | The same drive for each | Row or `None`; list; `None` |
| Error at the call (Rust) | `fetchall` on a closed cursor; `fetchmany(-1)` | The call returns `Err` with `InterfaceError`; with `ProgrammingError` |
| Fetch does not iterate the loop (live) | After `await cur.execute(sql)`: `loop.call_soon(ran.append, 1)`, then `rows = await cur.fetchall()` | `rows` correct and `ran == []` |
| Select equality (live) | `await client.fetch_rows(sql)` against the cursor path | Same rows and names |
| Dicts, value, DML (live) | `fetch_dicts`, `fetch_value`, `execute_dml` on client and pool | Same values as the sync methods of t15 |
| Not found (live) | `await client.fetch_value(select_without_rows)` | `NotFoundError` |
| Call-time error (live) | `client.fetch_rows("SELECT 1 FROM dual", "abc")` | `InterfaceError` raised by the call |
| Out-binds (live) | `await client.execute_dml("BEGIN :out := 99; END;", {"out": var})` | Item three is `{"out": 99}`; the variable holds 99 |
| One awaitable per pool call (live) | `aw = pool.fetch_value("SELECT 1 FROM dual")` | Not a coroutine; one await yields `1`; `borrowed_size` is 0 after |
| Pool error (live) | `ORA-00942`, then a select | The select succeeds; `borrowed_size` is 0 |
| Pool cancel (live) | Cancel the task awaiting a slow `pool.fetch_value` | `borrowed_size` returns to 0; a later call succeeds |
| Reset after an async begin (Rust, `client.rs` tests, worker without a connection) | Await `dispatch_async_raw` with `Begin` on the runtime, then `reset_with_timeout` | `reset_rollbacks` is 1 |
| Reset behind a queued command (Rust) | Start `dispatch_async_raw` with `Begin` on the runtime without awaiting it, then call `reset_with_timeout` from a blocking thread | `reset_rollbacks` is 1 |
| Async pool DML (live) | `await pool.execute_dml(insert)`, then count the row from a second connection | The row is visible |
| Async abandoned transaction (live) | Acquire, `begin()`, insert, release without commit; count from a second connection | The row is absent |
| Concurrent pool calls (live) | `asyncio.gather` of five `pool.fetch_value` | Five values |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make oracle-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen python -m mypy.stubtest vertebra._oracle`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_oracle_adapter.py -p no:xdist -q`
- `test -z "$(git grep -n 'fn ready_awaitable' -- src/rs/vertebra-oracle/src/cursor.rs)"`

`make oracle-test` runs the crate's `cargo test` with the interpreter exports a PyO3 crate needs. The live tests in it run only when an Oracle database is reachable.

## Acceptance Criteria

- [ ] The four fetch methods of `AsyncOracleCursor` spawn no task and return `Py<ReadyAwaitable>`, and `ready_awaitable` is gone.
- [ ] Cursor state errors of the fetch methods are still raised by the call.
- [ ] Each of the eight new methods spawns one task and builds its result in the completion closure.
- [ ] A pool method returns its connection on success, on error, and on cancellation.
- [ ] The worker-level async tests count the reset rollbacks the t31 table predicts, and the live tests show the pool-level result.
- [ ] The stub diff adds eight methods and no class, and `mypy.stubtest` passes with the allow-list unchanged.
