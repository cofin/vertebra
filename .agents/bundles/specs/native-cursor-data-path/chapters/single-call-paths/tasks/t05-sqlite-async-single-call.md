---
type: Task
id: "native-cursor-data-path:single:t05"
chapter_id: single-call-paths
title: "SQLite async single-call methods and completed fetch awaitables"
description: "Add the awaitable forms of the four single-call methods and last_statement_rowid to AsyncSQLiteClient, and return completed awaitables from the async cursor's fetch and close methods."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:single:t04"
read_set:
  - "src/rs/vertebra-sqlite/src/py_client.rs:13-31"
  - "src/rs/vertebra-sqlite/src/py_client.rs:924-942"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1136-1179"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1394-1398"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1406-1515"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1881-1893"
  - "src/rs/vertebra-db/src/py_async.rs:14-21"
  - "src/py/vertebra/_sqlite.pyi:203-240"
  - "src/py/vertebra/_sqlite.pyi:320-378"
  - "src/py/tests/unit/db/test_sqlite_driver.py:948-1035"
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/py/vertebra/_sqlite.pyi
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 240
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: sqlite-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs
    title: "SQLite client and cursor PyO3 classes"
  - id: async-bridge
    resource: src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable
    title: "Awaitable backed by a Tokio task and a loop completion queue"
tags: [sqlite, pyo3, asyncio, single-call]
---

# SQLite async single-call methods and completed fetch awaitables

## Objective & Scope Boundary

An async SQLite query is one awaitable. `AsyncSQLiteClient` gains `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`, each returning one awaitable whose result objects are built when the statement completes.

`AsyncSQLiteCursor.fetchone`, `fetchmany`, `fetchall`, and `close` return an awaitable that is already complete. They no longer spawn a task.

The sync client, the cursor's `execute` and `executemany`, and the Python drivers are not changed.

## Bounded Read Manifest

- `py_client.rs:13-31` — the import from the `py_async` module of `vertebra_db`, which this task extends.
- `py_client.rs:924-942` — `ready_awaitable`, the spawn-based helper this task deletes.
- `py_client.rs:1136-1179` — the four async cursor methods.
- `py_client.rs:1394-1398` — `SyncSQLiteClient.to_async`, which constructs an `AsyncSQLiteClient`.
- `py_client.rs:1406-1515` — `AsyncSQLiteClient`, its constructor, and `execute`, the spawn and cancel pattern to follow.
- `py_client.rs:1881-1893` — `to_sync` and `cursor`; the new methods go after `cursor`.
- The converters `outcome_to_rows`, `outcome_to_dicts`, `outcome_to_value`, and `outcome_to_dml` that t04 adds after `description_tuple` in `py_client.rs`.
- `vertebra-db/src/py_async.rs:14-21` — the re-export list with `ReadyAwaitable` and `ready_ok` from t01.
- `_sqlite.pyi:203-240`, `320-378` — the `AsyncSQLiteClient` stub and the async cursor stub.
- `test_sqlite_driver.py:948-1035` — the async cursor fetch tests, including `test_async_cursor_state_errors_raise_at_the_call`.

## Symbol & Signature Contracts

### Client state

```rust
pub struct AsyncSQLiteClient {
    pool: Arc<SqlitePool>,
    last_statement_rowid: Arc<AtomicI64>,
}
```

The two places that build an `AsyncSQLiteClient`, the `#[new]` constructor and `SyncSQLiteClient.to_async`, set the field to `Arc::new(AtomicI64::new(0))`. The value is not shared with the sync client it came from.

### Shared spawn helper

```rust
impl AsyncSQLiteClient {
    fn spawn_single_call<'py, M>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        map_ok: M,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
    where
        M: FnOnce(Python<'_>, StatementOutcome) -> PyResult<Py<PyAny>> + Send + 'static;
}
```

In order:

1. Convert parameters with `from_py` on `BoundParams`. An error is raised at the call.
2. Copy the SQL text to an owned `String`, take `op_id` from `pool.next_op_id()`, and clone the pool for the cancel callback.
3. Call `spawn_rust_db_task_with_attr_and_cancel` with `SQLITE_COMPLETION_DRAINER_ATTR`. The future runs `pool.run_with_op_id(op_id, &sql_owned, bound)` inside `spawn_blocking` from `tokio` and maps a join error to the `Operational` error that `execute` builds.
4. The success mapper stores `outcome.lastrowid.unwrap_or(0)` in `last_statement_rowid` with `Release` ordering, then returns `map_ok(py, outcome)`.
5. The error mapper is `|py, err| err.into_py_err(py)`.
6. The cancel callback is `cancel_pool.cancel_operation(op_id)`.

### Python-visible members on `AsyncSQLiteClient`

```rust
#[getter]
pub fn last_statement_rowid(&self) -> Option<i64>;

#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn fetch_rows<'py>(
    &self,
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>>;
```

`fetch_dicts`, `fetch_value`, and `execute_dml` have the same attribute line, parameters, and return type.

| Method | `map_ok` |
| --- | --- |
| `fetch_rows` | `outcome_to_rows`, then the pair converted to one Python tuple |
| `fetch_dicts` | `outcome_to_dicts` |
| `fetch_value` | `outcome_to_value`, with its error mapped through `into_py_err` |
| `execute_dml` | `outcome_to_dml`, converted to a Python tuple |

### Async cursor fetch methods

```rust
pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;

#[pyo3(signature = (size = None))]
pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>;

pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;

pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;
```

- Each takes its rows from the state under the lock, as today, releases the lock, builds the Python value with `row_to_py_tuple` or `rows_to_py_list`, and returns `ready_ok(py, value)`.
- `fetchone` with no row and `close` return `ready_ok(py, py.None())`.
- State errors keep today's timing: a closed cursor and a negative `fetchmany` size raise at the call, before any awaitable exists. `ready_err` is not used in this crate.
- Delete the module-level `ready_awaitable` function.
- Extend the import to `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, ..., ready_ok};`.

### Stub, in `src/py/vertebra/_sqlite.pyi`

Inside `AsyncSQLiteClient`, add the property after `last_insert_rowid` and the four methods after `cursor`, each with a one-line docstring:

```python
@property
def last_statement_rowid(self) -> int | None: ...

def fetch_rows(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> Awaitable[tuple[list[tuple[Any, ...]], tuple[str, ...]]]: ...

def fetch_dicts(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> Awaitable[list[dict[str, Any]]]: ...

def fetch_value(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> Awaitable[Any]: ...

def execute_dml(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> Awaitable[tuple[int, int | None]]: ...
```

The async cursor stub does not change. Its fetch methods already return `Awaitable[...]`.

## Edge Cases & Error Semantics

- Results match the sync methods in t04 for every case in its edge list.
- A parameter conversion error raises at the call. An engine error, and the `NotFoundError` from `fetch_value`, raise when the awaitable is awaited.
- `last_statement_rowid` is updated in the completion mapper, so it is set before `await` returns. A cancelled or failed statement leaves it unchanged.
- Thread rule: the value is the last statement completed through this client's single-call methods.
- Cancelling the awaiting task cancels the operation through `cancel_operation`, as `execute` does.
- An awaitable dropped without being awaited follows the rule `execute` has today: the bridge cancels its operation.
- Awaiting a fetch does not yield to the event loop. A callback scheduled with `loop.call_soon` before the await has not run when the await returns.
- A fetch awaitable works without a running loop being required at creation. It can be awaited more than once and returns the same list object.
- `asyncio.create_task(cursor.fetchall())` is rejected, as it is today for every native awaitable.
- The object returned by a fetch has the type name `ReadyAwaitable`. It is not an instance of `vertebra._vertebra.ReadyAwaitable`, because each extension holds its own copy of the class.

## Public-Seam Test Blueprint

New tests in `src/py/tests/unit/db/test_sqlite_driver.py`, marked `anyio` like the file's other async tests, on `AsyncSQLiteClient(":memory:")`.

| Case | Steps | Expected |
| --- | --- | --- |
| Results equal sync | each of the four methods on the queries of the t04 blueprint rows "Select equals cursor", "Dicts", "Empty result", "No result set", "Value", "DML", "Returning" | the same values the sync client returns |
| One awaitable | `aw = client.fetch_rows("SELECT 1 AS a")` | `await aw == ([(1,)], ("a",))`; the call itself did not raise |
| Bind error at the call | `client.fetch_rows("SELECT ?", (object(),))` without `await` | `InterfaceError` |
| Engine error on await | `aw = client.fetch_rows("SELEC 1")`; `await aw` | creating `aw` does not raise; awaiting raises `ProgrammingError` |
| `fetch_value` not found | `await client.fetch_value("SELECT 1 WHERE 0")` | `NotFoundError` |
| Last statement rowid | fresh client; awaited insert through `execute_dml`; awaited select; awaited failing statement | `None`; the rowid; `None`; unchanged |
| Fetch does not iterate the loop | `await cursor.execute(...)`; `ran = []`; `loop.call_soon(ran.append, 1)`; `rows = await cursor.fetchall()`; then `await asyncio.sleep(0)` | `rows` correct; `ran == []` right after the fetch; `ran == [1]` after the sleep |
| Same for `fetchone`, `fetchmany`, `close` | the same callback check around each | `ran == []` after each await |
| Fetch awaitable type | `type(cursor.fetchall()).__name__` | `"ReadyAwaitable"` |
| State errors at the call | existing `test_async_cursor_state_errors_raise_at_the_call` | passes unchanged |
| Fetch walk | existing `test_async_cursor_fetch_walk_and_sizes` | passes unchanged |
| Cancel | task awaiting `client.fetch_rows` of the recursive CTE, cancelled after 20 ms | `CancelledError` in the task; a later `fetch_value("SELECT 1")` returns `1` |

## Verification

- Rebuild: `make dev`
- Red then green: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q -k "async and (single_call or ready or last_statement_rowid)"`
- Whole file: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._sqlite`
- Rust, with the interpreter exports from the `rs-test` recipe: `(cd src/rs && cargo test -p vertebra-sqlite --locked && cargo clippy -p vertebra-sqlite --all-targets --locked -- -D warnings && cargo fmt --check -p vertebra-sqlite)`
- Removed helper, must print nothing: `git grep -n "fn ready_awaitable" -- src/rs/vertebra-sqlite/src`

Name the new tests with `single_call`, `ready`, or `last_statement_rowid` in the function name so the `-k` filter selects them.

## Acceptance Criteria

- [ ] Every row of the blueprint passes.
- [ ] `AsyncSQLiteCursor.fetchone`, `fetchmany`, `fetchall`, and `close` spawn no task.
- [ ] `mypy.stubtest vertebra._sqlite` passes with no allow-list change.
- [ ] The stub diff adds four methods and one property to `AsyncSQLiteClient` and nothing else.
- [ ] The removed-helper check prints nothing.
- [ ] No existing test in `test_sqlite_driver.py` changes.

## Notes & Discoveries

- Root spec traceability says fetch errors surface on await. For SQLite the state errors have always been raised at the call, and an existing test pins it, so this task keeps that timing.
