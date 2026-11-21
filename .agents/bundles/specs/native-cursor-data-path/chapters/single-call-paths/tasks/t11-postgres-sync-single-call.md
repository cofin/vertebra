---
type: Task
id: "native-cursor-data-path:single:t11"
chapter_id: single-call-paths
title: "PostgreSQL sync single-call methods on client and pool"
description: "SyncPostgresClient and SyncPostgresPool gain fetch_rows, fetch_dicts, fetch_value, and execute_dml over the statement runner the cursor uses."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t10"
  - "native-cursor-data-path:cleanup:t15"
read_set:
  - ".agents/bundles/specs/native-cursor-data-path/spec.md"
  - "src/rs/vertebra-postgres/src/cursor.rs:29-34"
  - "src/rs/vertebra-postgres/src/cursor.rs:203-366"
  - "src/rs/vertebra-postgres/src/cursor.rs:453-578"
  - "src/rs/vertebra-postgres/src/sync_client.rs:1128-1233"
  - "src/rs/vertebra-postgres/src/python_pool.rs:186-196"
  - "src/rs/vertebra-postgres/src/python_pool.rs:321-342"
  - "src/rs/vertebra-postgres/src/pool.rs:296-336"
  - "src/rs/vertebra-postgres/src/errors.rs:128-225"
  - "src/rs/vertebra-postgres/src/result.rs:94-105"
  - "src/py/vertebra/_postgres.pyi:24-62"
  - "src/py/vertebra/_postgres.pyi:240-252"
  - "src/py/vertebra/_postgres.pyi:594-644"
  - "src/py/tests/unit/db/postgres_ports.py:666-1000"
  - "src/py/tests/unit/db/postgres_data_peers.py:240-278"
  - "src/py/tests/unit/db/test_postgres_native_queries.py:898-1260"
files:
  - "src/rs/vertebra-postgres/src/cursor.rs"
  - "src/rs/vertebra-postgres/src/sync_client.rs"
  - "src/rs/vertebra-postgres/src/python_pool.rs"
  - "src/rs/vertebra-postgres/src/errors.rs"
  - "src/py/vertebra/_postgres.pyi"
tests:
  - "src/py/tests/unit/db/test_postgres_native_queries.py"
  - "src/py/tests/unit/db/test_postgres_native_pools.py"
  - "src/py/tests/unit/db/postgres_ports.py"
estimated_loc: 340
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
    title: "Root spec, section Single-call methods (R16)"
  - id: pg-cursor
    resource: "src/rs/vertebra-postgres/src/cursor.rs::PySyncPostgresCursor"
    title: "Sync cursor whose execute holds the statement runner today"
  - id: pg-pool
    resource: "src/rs/vertebra-postgres/src/python_pool.rs::PySyncPostgresPool"
    title: "Sync pool class"
tags: [postgres, rust, pyo3, performance]
---

# PostgreSQL sync single-call methods on client and pool

## Objective & Scope Boundary

One call on `SyncPostgresClient` or `SyncPostgresPool` runs a statement and returns finished Python objects. The caller creates no cursor and reads no cursor attribute.

`estimated_loc` is above 300. The extensions are built without PyO3's `multiple-pymethods` feature, so the client methods and the pool methods have to sit in the files that hold those classes, and the shared runner has to sit in `cursor.rs`. The count is about 200 Rust lines, 60 stub lines, and 80 lines in the class doubles. Tests add about 160 lines.

Out of scope:

- The async classes (t12) and the Python drivers (t13, t14).
- `executemany`. It stays on the cursor.
- Wire frames. Task t10 owns them.

## Bounded Read Manifest

- Root spec, section "Single-call methods (R16)", for the four return shapes.
- `execute` on `PySyncPostgresCursor` in `cursor.rs` (`:458-578`) holds the statement run inline today. `build_cursor_result` (`:350-366`) and `row_to_tuple` (`:204-223`) build the Python values.
- `sync_client.rs:1229-1233` is `cursor()` on the client. The new methods go next to it, inside the same `#[pymethods]` block.
- `python_pool.rs:332-342` is `cursor()` on the pool. `pool()` on `PySyncPostgresPool` (`:192`) returns the `Arc<PostgresPool>`.
- `PostgresPyExceptionTypes` in `errors.rs` (`:128-141`) has no `NotFoundError` entry.

## Contracts & Edge Cases

### Shared runner in `cursor.rs`

Extract the statement run from `execute` on `PySyncPostgresCursor` into these `pub(crate)` items. The cursor then calls them, so the cursor and the single-call methods share one runner.

| Item | Contract |
| --- | --- |
| `struct StatementCall { sql: String, params: Vec<Parameter>, oids: Option<Vec<u32>>, timeout: Option<Duration> }` | A statement ready to run. All fields `pub(crate)`. |
| `fn prepare_statement_call(py, sql: &str, params: Option<&Bound<'_, PyAny>>, parameter_oids: Option<&Bound<'_, PyAny>>, timeout: Option<&Bound<'_, PyAny>>) -> PyResult<StatementCall>` | Runs, in the order `execute` uses now: `extract_strict_opt_f64`, `validate_optional_seconds` on `CommandBudget`, `plan_for`, `extract_cursor_parameters`. Error mapping is unchanged. `sql` in the result is `plan.sql`. |
| `fn ensure_source_ready(py, source: &CursorSource, closed_pool_message: &str) -> PyResult<()>` | `Session`: `ensure_ordinary_reservable`. `Pool`: `InterfaceError` with `closed_pool_message` when the pool is closed. |
| `async fn run_statement(source: CursorSource, call: StatementCall, lazy_begin: bool) -> Result<QueryResult, PostgresError>` | The body of the `match source` in `execute` today: lazy begin, generation, budget, and `query_as_with_generation_and_budget` for a session; budget and `query_oneshot` for a pool. |
| `fn run_single_call(py, source: CursorSource, closed_pool_message: &str, sql: &str, params: Option<&Bound<'_, PyAny>>, parameter_oids: Option<&Bound<'_, PyAny>>, timeout: Option<&Bound<'_, PyAny>>, build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>>` | `prepare_statement_call`, `ensure_source_ready`, then `py.detach` around `block_on_db(run_statement(source, call, false))`, then `postgres_error_to_py` on failure or `build` on success. |

The cursor keeps its own execute lock and its state handling around `run_statement`. The cursor passes the message `cannot execute on a cursor whose pool is closed`. The pool methods pass `cannot run a statement on a closed pool`.

### Result builders in `cursor.rs`

| Item | Returns |
| --- | --- |
| `fn cell_to_py(py, cell: &PgValue) -> PyResult<Py<PyAny>>` | One cell. Extracted from the `match` in `row_to_tuple`, which then calls it. |
| `fn rows_and_names(py, result: &QueryResult) -> PyResult<Py<PyAny>>` | `(list_of_tuples, tuple_of_names)`. `([], ())` when `result.columns` is empty. |
| `fn row_dicts(py, result: &QueryResult) -> PyResult<Py<PyAny>>` | A list with one `dict` per row. The key strings are created once per call and reused for every row. With a repeated column name the last column wins. `[]` when there are no columns. |
| `fn first_value(py, result: &QueryResult) -> PyResult<Py<PyAny>>` | First cell of the first row. No row: `NotFoundError` with the message `Query returned no rows`. A row with no cell: `None`. |
| `fn dml_outcome(py, result: &QueryResult) -> PyResult<Py<PyAny>>` | `(rowcount, None)`. `rowcount` is `rowcount_for_result(result)` with a negative value replaced by `0`. |
| `fn cursor_not_found_err(py, message: &str) -> PyErr` | Built like `cursor_interface_err`, from the new cache entry below. |

`errors.rs`: add `pub(crate) not_found: Py<PyType>` to `PostgresPyExceptionTypes`. `load_py_exceptions_from_core` reads `NotFoundError` from `vertebra._vertebra`. `build_stand_in_py_exceptions` creates a stand-in named `NotFoundError` whose base is the `DataError` stand-in. This is the only edit to `errors.rs`.

### Python methods

On `PySyncPostgresClient` (`sync_client.rs`) and on `PySyncPostgresPool` (`python_pool.rs`), four methods each. Every one has this attribute and these parameters:

```rust
#[pyo3(
    signature = (sql, params = None, *, parameter_oids = None, timeout = None),
    text_signature = "($self, sql, params=(), *, parameter_oids=None, timeout=None)"
)]
pub fn fetch_rows(
    &self,
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    parameter_oids: Option<&Bound<'_, PyAny>>,
    timeout: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>>
```

| Method | Builder |
| --- | --- |
| `fetch_rows` | `rows_and_names` |
| `fetch_dicts` | `row_dicts` |
| `fetch_value` | `first_value` |
| `execute_dml` | `dml_outcome` |

- The client passes the `Session` variant of `CursorSource` with a clone of `self.session`. The pool passes the `Pool` variant of `CursorSource` with a clone of `self.pool()`.
- Each body is one call to `run_single_call` in the crate's `cursor` module.
- No cursor object is created and no cursor state is stored.

### Transactions

- A client method runs on that client's session. Inside an open transaction it runs in the transaction. It never begins one.
- A pool method runs on the one-shot path in autocommit. It is not used for a statement that belongs to a transaction; the driver calls the pinned client instead (t13).
- A one-shot statement that leaves its connection inside a transaction follows the existing `query_oneshot` rule: the connection is reset on release and not handed back as clean.

### Errors

- Errors are the classes the cursor raises, through `postgres_error_to_py`.
- A `TypeError` or `ValueError` from parameter conversion is an `InterfaceError`, through the existing `convert_param_err`.
- A timeout raises the class the cursor raises for the same statement.
- A closed pool raises `InterfaceError`.

### Stub

Add to `SyncPostgresClient` and to `SyncPostgresPool` in `src/py/vertebra/_postgres.pyi`:

```python
def fetch_rows(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
    *,
    parameter_oids: Sequence[int] | None = None,
    timeout: float | None = None,
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
    """Run one statement and return its rows and column names."""
    ...

def fetch_dicts(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
    *,
    parameter_oids: Sequence[int] | None = None,
    timeout: float | None = None,
) -> list[dict[str, Any]]:
    """Run one statement and return one dictionary per row."""
    ...

def fetch_value(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
    *,
    parameter_oids: Sequence[int] | None = None,
    timeout: float | None = None,
) -> Any:
    """Run one statement and return the first column of the first row."""
    ...

def execute_dml(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
    *,
    parameter_oids: Sequence[int] | None = None,
    timeout: float | None = None,
) -> tuple[int, None]:
    """Run one statement and return its row count and no last row id."""
    ...
```

### Class doubles

`src/py/tests/unit/db/postgres_ports.py` mirrors the native classes. Add the four methods to `FakeSyncPostgresClient`, `FakeSyncPostgresPool`, `FakeAsyncPostgresClient`, and `FakeAsyncPostgresPool`, with the stub signatures. The async forms are `async def`. Each method runs the statement through the same double's `cursor()` and shapes the result as the table in the root spec says, so the doubles cannot drift from their cursors. The async doubles are added here so that t12, t13, and t14 do not edit this file.

## Test Matrix

Tests named `test_sync_single_call_*` go in `test_postgres_native_queries.py` for the client and in `test_postgres_native_pools.py` for the pool. A second run of the same SQL on one connection is a cached run (t10). Tests that compare the cursor path with a single-call method use the per-frame responder in `postgres_data_peers.py`, which answers only the frames it receives and encodes rows in the formats `Bind` asked for.

| Case | Input | Expected |
| --- | --- | --- |
| Select equality | `fetch_rows(sql)` against `cursor().execute(sql)` | Same rows; names equal the description names |
| Dicts | `fetch_dicts("SELECT 1 AS a, 'x' AS b")` | `[{"a": 1, "b": "x"}]` |
| Empty result | Select with no row | `([], ("a",))`, `[]`, `NotFoundError` from `fetch_value` |
| No result set | `CREATE TABLE` | `([], ())`, `[]`, `NotFoundError`, `(0, None)` |
| DML | `UPDATE` of two rows through `execute_dml` | `(2, None)` |
| Returning | `INSERT ... RETURNING id` | `fetch_rows` gives the row; `execute_dml` gives `(1, None)` |
| Null value | `fetch_value("SELECT NULL")` | `None` |
| Named parameters | `fetch_value("SELECT %(n)s::int", {"n": 7})` | `7` |
| Empty parameters | `fetch_value("SELECT 1", ())` | `1` |
| Bad parameters | Positional parameters for SQL without placeholders | `ProgrammingError` |
| Unsupported value | A parameter whose conversion raises `TypeError` | `InterfaceError` |
| Timeout | `timeout` shorter than the scripted reply delay | The class the cursor raises |
| Server error | Unique violation | `IntegrityError` with `sqlstate` `23505` |
| In a transaction | `client.begin()`, `client.execute_dml(insert)`, `client.rollback()` | The insert is rolled back |
| Pool returns the connection | 100 sequential `pool.fetch_value` on a pool of size 1 | All succeed |
| Pool error | A failing statement, then `pool.fetch_value("SELECT 1")` | `1` |
| Closed pool | `pool.close()` then `pool.fetch_rows(sql)` | `InterfaceError` |
| Builders (Rust unit tests in `cursor.rs`) | `QueryResult` values built in the test | Shapes for no columns, no rows, `INSERT 0 3`, and `CREATE TABLE` |

## Verification

- `export PATH="$HOME/.cargo/bin:$PATH"`
- `make postgres-test`
- `cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`
- `make dev`
- `uv run --no-sync --frozen python -m mypy.stubtest vertebra._postgres`
- `uv run --no-sync --frozen pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_postgres_native_pools.py src/py/tests/unit/db/test_postgres_native_adapters.py -p no:xdist -q`
- `uv run --no-sync --frozen ruff check src/py/tests/unit/db/postgres_ports.py src/py/vertebra/_postgres.pyi`

## Acceptance Criteria

- [ ] `execute` on `PySyncPostgresCursor` and the eight new methods call `run_statement`. The statement run exists once in the crate's sync path.
- [ ] The stub diff adds eight methods and no class, and `mypy.stubtest` passes with the allow-list unchanged.
- [ ] Every matrix case is covered for the client and, where it applies, for the pool.
- [ ] Existing cursor tests pass unchanged.
