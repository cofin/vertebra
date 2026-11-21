---
type: Task
id: "native-cursor-data-path:single:t08"
chapter_id: single-call-paths
title: "DuckDB async single-call methods and completed fetch awaitables"
description: "Add the awaitable forms of the four single-call methods to AsyncDuckDBClient and return completed awaitables from the async cursor's fetch and close methods."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t01"
  - "native-cursor-data-path:single:t07"
read_set:
  - "src/rs/vertebra-duckdb/src/lib.rs:36-38"
  - "src/rs/vertebra-duckdb/src/lib.rs:2522-2550"
  - "src/rs/vertebra-duckdb/src/lib.rs:2854-2860"
  - "src/rs/vertebra-duckdb/src/lib.rs:2950-2978"
  - "src/rs/vertebra-duckdb/src/lib.rs:3261-3270"
  - "src/rs/vertebra-duckdb/src/cursor.rs:14-24"
  - "src/rs/vertebra-duckdb/src/cursor.rs:132-150"
  - "src/rs/vertebra-duckdb/src/cursor.rs:413-430"
  - "src/rs/vertebra-duckdb/src/cursor.rs:623-658"
  - "src/rs/vertebra-db/src/py_async.rs:14-21"
  - "src/py/vertebra/_duckdb.pyi:206-262"
  - "src/py/vertebra/_duckdb.pyi:322-390"
  - "src/py/tests/unit/db/test_duckdb_driver.py:1340-1392"
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-duckdb/src/cursor.rs
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 210
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:02Z
sources:
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "DuckDB pool, bind conversion, and client PyO3 classes"
  - id: duckdb-cursor
    resource: src/rs/vertebra-duckdb/src/cursor.rs
    title: "DuckDB cursor PyO3 classes and row converters"
tags: [duckdb, pyo3, asyncio, single-call]
---

# DuckDB async single-call methods and completed fetch awaitables

## Objective & Scope Boundary

An async DuckDB query is one awaitable. `AsyncDuckDBClient` gains `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`, each returning one awaitable whose result objects are built when the statement completes.

`AsyncDuckDBCursor.fetchone`, `fetchmany`, `fetchall`, and `close` return an awaitable that is already complete. They no longer spawn a task.

The sync client, the cursor's `execute` and `executemany`, and the Python drivers are not changed.

## Bounded Read Manifest

- `lib.rs:36-38` — the import from the `py_async` module of `vertebra_db`.
- `lib.rs:2522-2550` — `CancelOnDropGuard`.
- `lib.rs:2854-2860`, `2950-2978` — `AsyncDuckDBClient` and its `execute`, the spawn pattern to follow.
- `lib.rs:3261-3270` — `to_sync` and `cursor`; the new methods go after `cursor`.
- `cursor.rs:14-24` — imports. `cursor.rs:132-150` — row converters; the outcome converters from t07 follow them.
- `cursor.rs:413-430` — `ready_awaitable`, the spawn-based helper this task deletes.
- `cursor.rs:623-658` — the four async cursor methods.
- `vertebra-db/src/py_async.rs:14-21` — the re-export list with `ReadyAwaitable` and `ready_ok` from t01.
- `_duckdb.pyi:206-262`, `322-390` — the `AsyncDuckDBClient` stub and the async cursor stub.
- `test_duckdb_driver.py:1340-1392` — async cursor fetch tests, including `test_duckdb_async_native_cursor_closed_cursor_raises_interface_error`.

## Symbol & Signature Contracts

### Shared spawn helper, in `lib.rs`

```rust
impl AsyncDuckDBClient {
    fn spawn_single_call<M>(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        map_ok: M,
    ) -> PyResult<RustDbTaskAwaitable>
    where
        M: FnOnce(Python<'_>, StatementOutcome) -> PyResult<Py<PyAny>> + Send + 'static;
}
```

In order, following `AsyncDuckDBClient.execute`:

1. Convert parameters with `extract_bound_params`. An error is raised at the call.
2. Clone `inner`, copy the SQL text, register a cancel flag, and build a `CancelOnDropGuard`.
3. Call `spawn_rust_db_task_with_attr` with `DUCKDB_COMPLETION_DRAINER_ATTR`. The future runs `inner.run_statement(sql_owned, bound, &cancel_flag)` inside `spawn_blocking` from `tokio`, maps a join error to the `Operational` variant of `DuckDBClientError`, and calls `drop_guard.mark_completed()` before it returns the result.
4. The success mapper is `map_ok`. The error mapper is `|py, err| err.into_py_err(py)`.

### Python-visible members on `AsyncDuckDBClient`

```rust
#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn fetch_rows(
    &self,
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<RustDbTaskAwaitable>;
```

`fetch_dicts`, `fetch_value`, and `execute_dml` have the same attribute line, parameters, and return type.

| Method | `map_ok` |
| --- | --- |
| `fetch_rows` | `outcome_to_rows`, then the pair converted to one Python tuple |
| `fetch_dicts` | `outcome_to_dicts` |
| `fetch_value` | `outcome_to_value` |
| `execute_dml` | `outcome_to_dml`, converted to a Python tuple |

### Async cursor fetch methods, in `cursor.rs`

```rust
pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;

#[pyo3(signature = (size = None))]
pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>>;

pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;

pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>>;
```

- Each takes its rows from the state under the lock, as today, releases the lock, builds the Python value with `row_to_py_tuple` or `rows_to_py_list`, and returns `ready_ok(py, value)`.
- `fetchone` with no row and `close` return `ready_ok(py, py.None())`.
- State errors keep today's timing: a closed cursor and a negative `fetchmany` size raise at the call. `ready_err` is not used in this crate.
- Delete the `ready_awaitable` function in `cursor.rs`.
- The `cursor.rs` import becomes `use vertebra_db::py_async::{ReadyAwaitable, RustDbTaskAwaitable, ready_ok, spawn_rust_db_task_with_attr};`.

### Stub, in `src/py/vertebra/_duckdb.pyi`

Inside `AsyncDuckDBClient`, after `cursor`, each with a one-line docstring:

```python
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

The async cursor stub does not change.

## Edge Cases & Error Semantics

- Results match the sync methods in t07 for every case in its edge list.
- A parameter conversion error raises at the call. An engine error, and the `NotFoundError` from `fetch_value`, raise on await.
- An awaitable dropped before it completes interrupts its statement through `CancelOnDropGuard`, as `execute` does.
- Awaiting a fetch does not yield to the event loop.
- A fetch awaitable can be awaited more than once and returns the same object.
- The object returned by a fetch has the type name `ReadyAwaitable`. It is not an instance of `vertebra._vertebra.ReadyAwaitable`, because each extension holds its own copy of the class.

## Public-Seam Test Blueprint

New tests in `src/py/tests/unit/db/test_duckdb_driver.py`, marked `anyio`, on `AsyncDuckDBClient(":memory:")`.

| Case | Steps | Expected |
| --- | --- | --- |
| Results equal sync | each of the four methods on the queries of the t07 rows "Select equals cursor", "Dicts", "Empty result", "No result set", "Value", "DML", "Returning" | the same values the sync client returns |
| One awaitable | `aw = client.fetch_rows("SELECT 1 AS a")` | `await aw == ([(1,)], ("a",))` |
| Bind error at the call | `client.fetch_rows("SELECT ?", 5)` without `await` | `InterfaceError` |
| Engine error on await | `aw = client.fetch_rows("SELEC 1")`; `await aw` | creating `aw` does not raise; awaiting raises the class the async cursor raises |
| `fetch_value` not found | `await client.fetch_value("SELECT 1 WHERE false")` | `NotFoundError` |
| Fetch does not iterate the loop | `await cursor.execute(...)`; `ran = []`; `loop.call_soon(ran.append, 1)`; `rows = await cursor.fetchall()`; then `await asyncio.sleep(0)` | `rows` correct; `ran == []` right after the fetch; `ran == [1]` after the sleep |
| Same for `fetchone`, `fetchmany`, `close` | the same callback check around each | `ran == []` after each await |
| Fetch awaitable type | `type(cursor.fetchall()).__name__` | `"ReadyAwaitable"` |
| State errors at the call | existing `test_duckdb_async_native_cursor_closed_cursor_raises_interface_error` | passes unchanged |
| Existing async cursor tests | every `test_duckdb_async_native_cursor_*` test | passes unchanged |

## Verification

- Rebuild: `make dev`
- Red then green: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_duckdb_driver.py -q -k "async and (single_call or ready)"`
- Whole file: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_duckdb_driver.py -q`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._duckdb`
- Rust, with the interpreter exports from the `rs-test` recipe: `(cd src/rs && cargo test -p vertebra-duckdb --locked && cargo clippy -p vertebra-duckdb --all-targets --locked -- -D warnings && cargo fmt --check -p vertebra-duckdb)`
- Removed helper, must print nothing: `git grep -n "fn ready_awaitable" -- src/rs/vertebra-duckdb/src`

Name the new tests with `single_call` or `ready` in the function name so the `-k` filter selects them.

## Acceptance Criteria

- [ ] Every row of the blueprint passes.
- [ ] `AsyncDuckDBCursor.fetchone`, `fetchmany`, `fetchall`, and `close` spawn no task.
- [ ] `mypy.stubtest vertebra._duckdb` passes with no allow-list change.
- [ ] The stub diff adds four methods to `AsyncDuckDBClient` and nothing else.
- [ ] The removed-helper check prints nothing.
- [ ] No existing test in `test_duckdb_driver.py` changes.

## Notes & Discoveries

- DuckDB async cursor state errors are raised at the call today, and an existing test pins it. This task keeps that timing.
