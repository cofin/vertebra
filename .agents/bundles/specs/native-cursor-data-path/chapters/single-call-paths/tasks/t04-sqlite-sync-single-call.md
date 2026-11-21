---
type: Task
id: "native-cursor-data-path:single:t04"
chapter_id: single-call-paths
title: "SQLite sync single-call methods"
description: "Add fetch_rows, fetch_dicts, fetch_value, and execute_dml to SyncSQLiteClient, with the last_statement_rowid getter and the stub lines."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:single:t02"
  - "native-cursor-data-path:single:t03"
read_set:
  - "src/rs/vertebra-sqlite/src/py_client.rs:1-31"
  - "src/rs/vertebra-sqlite/src/py_client.rs:648-683"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1181-1263"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1383-1404"
  - "src/rs/vertebra-sqlite/src/py_client.rs:1881-1893"
  - "src/rs/vertebra-sqlite/src/value.rs:259-270"
  - "src/rs/vertebra-sqlite/src/errors.rs:148-153"
  - "src/py/vertebra/_sqlite.pyi:27-66"
  - "src/py/vertebra/_sqlite.pyi:145-156"
  - "src/py/tests/unit/db/test_sqlite_driver.py:684-725"
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/py/vertebra/_sqlite.pyi
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 260
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
tags: [sqlite, pyo3, single-call, native-driver]
---

# SQLite sync single-call methods

## Objective & Scope Boundary

`SyncSQLiteClient` answers a query in one call. `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml` each run one statement and return finished Python objects. The client also reports the rowid of the last statement run this way.

The methods use `run` on `SqlitePool`, the runner the cursor uses. They create no cursor and store no cursor state.

This task adds the outcome converters that t05 reuses. It does not change the cursor, the async client, or the Python drivers.

## Bounded Read Manifest

- `py_client.rs:1-31` — imports.
- `py_client.rs:648-683` — `row_to_py_tuple`, `rows_to_py_list`, `description_tuple`; the new converters go after them.
- `py_client.rs:1181-1263` — `SyncSQLiteClient`, its constructor, getters, and `execute`.
- `py_client.rs:1383-1404` — `cancel`, `close`, `to_async`, `cursor`; the new methods go after `cursor`.
- `py_client.rs:1881-1893` — `AsyncSQLiteClient.to_sync`, which constructs a `SyncSQLiteClient`.
- `value.rs:259-270` — `StatementOutcome`.
- `errors.rs:148-153` — the `NotFound` variant.
- `_sqlite.pyi:27-66`, `145-156` — the start and end of the `SyncSQLiteClient` stub.
- `test_sqlite_driver.py:684-725` — the cursor outcome test to mirror.

## Symbol & Signature Contracts

### Converters, module level in `py_client.rs`

```rust
fn names_to_py_tuple<'py>(py: Python<'py>, columns: &[String]) -> PyResult<Bound<'py, PyTuple>>;

fn outcome_to_rows<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)>;

fn outcome_to_dicts<'py>(py: Python<'py>, outcome: StatementOutcome) -> PyResult<Bound<'py, PyList>>;

fn outcome_to_value(py: Python<'_>, outcome: StatementOutcome) -> Result<Py<PyAny>, SqliteClientError>;

const fn outcome_to_dml(outcome: &StatementOutcome) -> (i64, Option<i64>);
```

- `names_to_py_tuple` builds one tuple of interned strings with `intern` on `PyString`.
- `outcome_to_rows`: with `columns` `None`, an empty list and an empty tuple. Otherwise `rows_to_py_list` and `names_to_py_tuple`.
- `outcome_to_dicts`: with `columns` `None`, an empty list. Otherwise it builds the interned key tuple once, then one `PyDict` per row, set in column order with keys borrowed from that tuple. With a repeated column name the later column wins.
- `outcome_to_value`: with no row, or `columns` `None`, `Err(SqliteClientError::NotFound("Query returned no rows".to_string()))`. Otherwise the first value of the first row converted with `into_py`, or `None` when that row is empty.
- `outcome_to_dml`: `(rowcount, lastrowid)` with a negative `rowcount` turned into `0`. Rows are dropped.

### Client state

```rust
pub struct SyncSQLiteClient {
    pool: Arc<SqlitePool>,
    last_statement_rowid: AtomicI64,
}
```

The two places that build a `SyncSQLiteClient`, the `#[new]` constructor and `AsyncSQLiteClient.to_sync`, set the field to `AtomicI64::new(0)`. `0` means no rowid.

Private helper:

```rust
impl SyncSQLiteClient {
    fn run_once(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<StatementOutcome>;
}
```

`run_once` converts parameters with `from_py` on `BoundParams`, calls `self.pool.run(sql, bound)` inside `py.detach` with `&self.pool` and the borrowed `sql`, and maps both error sites through `into_py_err`. On success it stores `outcome.lastrowid.unwrap_or(0)` in `last_statement_rowid` with `Release` ordering before returning. On failure it stores nothing.

### Python-visible members

```rust
#[getter]
pub fn last_statement_rowid(&self) -> Option<i64>;

#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn fetch_rows<'py>(
    &self,
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)>;

#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn fetch_dicts<'py>(
    &self,
    py: Python<'py>,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyList>>;

#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn fetch_value(
    &self,
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>>;

#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
pub fn execute_dml(
    &self,
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<(i64, Option<i64>)>;
```

- Each method is `run_once` followed by its converter. `fetch_value` maps the converter's error with `into_py_err`.
- `last_statement_rowid` loads the atomic with `Acquire` ordering and returns `None` for `0`.
- The SQLite cursor's `execute` takes no keyword-only extras, so these methods take none.

### Stub, in `src/py/vertebra/_sqlite.pyi`

Inside `SyncSQLiteClient`, add the property after `last_insert_rowid` and the four methods after `cursor`. Each carries a one-line docstring in the style of the file.

```python
@property
def last_statement_rowid(self) -> int | None: ...

def fetch_rows(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]: ...

def fetch_dicts(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> list[dict[str, Any]]: ...

def fetch_value(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> Any: ...

def execute_dml(
    self,
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = (),
) -> tuple[int, int | None]: ...
```

## Edge Cases & Error Semantics

- A statement with no result set: `fetch_rows` returns `([], ())`, `fetch_dicts` returns `[]`, and `fetch_value` raises `NotFoundError`. The statement has run in each case.
- An empty result set: `fetch_rows` returns `([], names)` with the column names.
- DML with a returning clause: `fetch_rows` and `fetch_dicts` return the rows. `execute_dml` drops them and returns `(changes, rowid)`.
- `execute_dml` on a read returns `(0, None)`.
- `last_statement_rowid` after an insert that changed rows is that rowid, through any of the four methods. After any other successful statement it is `None`. After a failed statement it keeps its value.
- Thread rule: the value belongs to the client. It is the last statement any thread ran through this client's single-call methods. Cursor statements, `execute`, `execute_many`, and the pipeline do not change it.
- Errors are the classes the cursor raises, through `into_py_err`. A closed client raises `OperationalError` from the pool, as `cursor().execute` does on a closed client.
- A parameter conversion error raises before the statement runs.
- No transaction is opened. Inside an explicit transaction a read goes to the writer, as the pool decides.
- The methods hold no lock of their own. Two threads calling one client run one after the other on the writer connection, or side by side on reader connections.

## Public-Seam Test Blueprint

New tests in `src/py/tests/unit/db/test_sqlite_driver.py`, on `SyncSQLiteClient(":memory:")` seeded with `CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)`. "Cursor" below means `client.cursor()` run with the same SQL and parameters on a second, equally seeded client.

| Case | Steps | Expected |
| --- | --- | --- |
| Select equals cursor | `fetch_rows("SELECT id, label FROM items ORDER BY id")` after two inserts | `(cursor.fetchall(), names from cursor.description)`; rows are tuples; names is a tuple of `str` |
| Dicts | `fetch_dicts` on the same query | `[{"id": 1, "label": "a"}, {"id": 2, "label": "b"}]` |
| Empty result | `fetch_rows`, `fetch_dicts`, `fetch_value` with a false predicate | `([], ("id", "label"))`; `[]`; `NotFoundError` |
| No result set | `fetch_rows`, `fetch_dicts` on `CREATE TABLE other (x)` | `([], ())`; `[]` |
| Value | `fetch_value("SELECT count(*) FROM items")`; `fetch_value("SELECT NULL")` | `2`; `None` |
| DML | `execute_dml` insert, update of two rows, update matching nothing, select | `(1, 3)`; `(2, None)`; `(0, None)`; `(0, None)` |
| Returning | `fetch_rows("INSERT ... RETURNING id")`; `execute_dml("INSERT ... RETURNING id")` | `([(n,)], ("id",))`; `(1, n + 1)` |
| Last statement rowid | fresh client; insert through `execute_dml`; select through `fetch_rows`; returning insert through `fetch_rows`; failing statement | `None`; the rowid; `None`; the new rowid; unchanged after the failure |
| Rowid not touched by cursor | insert through `client.cursor().execute` | `last_statement_rowid` keeps its earlier value |
| Duplicate names | `fetch_dicts("SELECT 1 AS a, 2 AS a")` | `[{"a": 2}]` |
| Parameters | each method with `(1,)`, `[1]`, `{"v": 1}`, `()`, and `None` | same results as the cursor |
| Error classes | duplicate key; `SELEC 1`; `SELECT ?` with `(object(),)` | `IntegrityError` with `sqlstate` set; `ProgrammingError`; `InterfaceError`; each is the class the cursor raises for the same input |
| Closed client | `close()`, then each method | the class `client.cursor().execute` raises on a closed client |
| No transaction | `execute_dml` insert | `client.in_transaction` is `False`; a second client on the same file sees the row |

## Verification

- Rebuild: `make dev`
- Red then green: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q -k "single_call or last_statement_rowid"`
- Whole file: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_sqlite_driver.py -q`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._sqlite`
- Rust, with the interpreter exports from the `rs-test` recipe: `(cd src/rs && cargo test -p vertebra-sqlite --locked && cargo clippy -p vertebra-sqlite --all-targets --locked -- -D warnings && cargo fmt --check -p vertebra-sqlite)`

Name the new tests with `single_call` or `last_statement_rowid` in the function name so the `-k` filter selects them.

## Acceptance Criteria

- [ ] Every row of the blueprint passes.
- [ ] `mypy.stubtest vertebra._sqlite` passes with no allow-list change.
- [ ] The stub diff adds four methods and one property to `SyncSQLiteClient` and nothing else.
- [ ] The four methods call `run` on `SqlitePool` and create no cursor.
- [ ] No existing test in `test_sqlite_driver.py` changes.

## Notes & Discoveries

- `last_statement_rowid` exists because `fetch_rows` returns no rowid and the SQLite driver reports one after every statement. t06 reads it from `driver.lastrowid`.
