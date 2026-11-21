---
type: Task
id: "native-cursor-data-path:single:t07"
chapter_id: single-call-paths
title: "DuckDB bind fast path and sync single-call methods"
description: "Check exact tuple, list, and dict types first in DuckDB parameter extraction, bind a non-dict mapping by name, and add fetch_rows, fetch_dicts, fetch_value, and execute_dml to SyncDuckDBClient with their stub lines."
worksheet_format: full
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - "src/rs/vertebra-duckdb/src/lib.rs:12-47"
  - "src/rs/vertebra-duckdb/src/lib.rs:60-83"
  - "src/rs/vertebra-duckdb/src/lib.rs:122-133"
  - "src/rs/vertebra-duckdb/src/lib.rs:622-651"
  - "src/rs/vertebra-duckdb/src/lib.rs:2110-2140"
  - "src/rs/vertebra-duckdb/src/lib.rs:2202-2249"
  - "src/rs/vertebra-duckdb/src/lib.rs:2553-2560"
  - "src/rs/vertebra-duckdb/src/lib.rs:2646-2660"
  - "src/rs/vertebra-duckdb/src/lib.rs:2815-2825"
  - "src/rs/vertebra-duckdb/src/lib.rs:3999-4005"
  - "src/rs/vertebra-duckdb/src/cursor.rs:14-24"
  - "src/rs/vertebra-duckdb/src/cursor.rs:132-150"
  - "src/rs/vertebra-duckdb/src/cursor.rs:216-256"
  - "src/rs/vertebra-duckdb/src/cursor.rs:305-320"
  - "src/rs/vertebra-duckdb/src/errors.rs:153-156"
  - "src/py/vertebra/_duckdb.pyi:26-82"
  - "src/py/vertebra/_duckdb.pyi:142-160"
  - "src/py/tests/unit/db/test_duckdb_driver.py:954-1010"
files:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/rs/vertebra-duckdb/src/cursor.rs
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 270
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
tags: [duckdb, pyo3, binds, single-call]
---

# DuckDB bind fast path and sync single-call methods

## Objective & Scope Boundary

`SyncDuckDBClient` answers a query in one call through `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml`. Parameter extraction looks at the exact container type first, and an empty container is the no-parameter value.

One defect is fixed here: a `Mapping` that is not a `dict` is bound by name. Today it falls through to iteration and its keys are bound by position.

The methods use `run_statement` on `DuckDBInner`, the runner the cursor uses. They store no cursor state. `py_to_duck_value` is not changed; it already tests concrete types. The cursor, the async client, and the Python drivers are not changed.

## Bounded Read Manifest

- `lib.rs:12-47` — imports, including the `types` import from `pyo3` and the `cursor` module.
- `lib.rs:60-83` — `BoundParams`. `lib.rs:122-133` — `StatementOutcome`.
- `lib.rs:622-651` — `run_statement`.
- `lib.rs:2110-2140` — the head of `py_to_duck_value`, for the order it already has.
- `lib.rs:2202-2249` — `extract_bound_params`.
- `lib.rs:2553-2560`, `2646-2660` — `SyncDuckDBClient` and its `execute`.
- `lib.rs:2815-2825` — `to_async` and `cursor`; the new methods go after `cursor`.
- `lib.rs:3999-4005` — the `run` helper of the test module.
- `cursor.rs:14-24` — imports. `cursor.rs:132-150` — `row_to_py_tuple`, `rows_to_py_list`.
- `cursor.rs:216-256`, `305-320` — `run_and_install` and `execute`: how the cursor registers a cancel flag and calls `run_statement`.
- `errors.rs:153-156` — the `NotFound` variant.
- `_duckdb.pyi:26-82`, `142-160` — the `SyncDuckDBClient` stub.
- `test_duckdb_driver.py:954-1010` — cursor bind tests to mirror.

## Symbol & Signature Contracts

### `extract_bound_params`, new order

The signature does not change.

1. Absent, or `None`: `Empty`.
2. `cast_exact::<PyTuple>()` succeeds: `Empty` when its length is `0`. Otherwise `Positional` from `iter()`.
3. `cast_exact::<PyList>()` succeeds: `Empty` when its length is `0`. Otherwise `Positional` from `iter()`.
4. `cast_exact::<PyDict>()` succeeds: `Empty` when its length is `0`. Otherwise `Named`, filled with `locked_for_each`. A key that is not `str` fails with `InvalidInput`, as today.
5. The existing `cast::<PyDict>()`, `cast::<PyList>()`, and `cast::<PyTuple>()` branches, which now serve subclasses only. Each returns `Empty` for an empty container.
6. New: `cast::<PyMapping>()` succeeds: read `keys()`, and for each key read the value with `get_item`. A key that is not `str` fails with `InvalidInput`. An empty mapping is `Empty`; otherwise `Named`.
7. The existing `try_iter` branch and its error message, unchanged.

Add `PyMapping` to the `types` import from `pyo3`.

### Converters, in `cursor.rs`

```rust
pub(crate) fn names_to_py_tuple<'py>(
    py: Python<'py>,
    columns: &[String],
) -> PyResult<Bound<'py, PyTuple>>;

pub(crate) fn outcome_to_rows<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)>;

pub(crate) fn outcome_to_dicts<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<Bound<'py, PyList>>;

pub(crate) fn outcome_to_value(py: Python<'_>, outcome: StatementOutcome) -> PyResult<Py<PyAny>>;

pub(crate) const fn outcome_to_dml(outcome: &StatementOutcome) -> (i64, Option<i64>);
```

- `names_to_py_tuple` builds one tuple of interned strings with `intern` on `PyString`.
- `outcome_to_rows`: with `columns` `None`, an empty list and an empty tuple. Otherwise `rows_to_py_list` and `names_to_py_tuple`.
- `outcome_to_dicts`: with `columns` `None`, an empty list. Otherwise one interned key tuple for the call and one `PyDict` per row, set in column order through `duck_value_to_py`. With a repeated column name the later column wins.
- `outcome_to_value`: with no row, or `columns` `None`, the error `DuckDBClientError::NotFound("Query returned no rows".to_string())` converted with `into_py_err`. Otherwise the first value of the first row through `duck_value_to_py`, or `None` when that row is empty.
- `outcome_to_dml`: `(rowcount, None)` with a negative `rowcount` turned into `0`. DuckDB has no rowid, so the second item is always `None`.

### Client methods, in `lib.rs`

Private helper:

```rust
impl SyncDuckDBClient {
    fn run_once(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<StatementOutcome>;
}
```

`run_once` converts parameters with `extract_bound_params`, copies the SQL text to an owned `String`, registers a cancel flag with `register_cancel_flag`, and calls `inner.run_statement(sql_owned, bound, &cancel_flag)` inside `py.detach`. Both error sites go through `into_py_err`. `run_statement` keeps its owned `String` parameter.

```rust
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

Each method is `run_once` followed by its converter. The DuckDB cursor's `execute` takes no keyword-only extras, so these methods take none. No `last_statement_rowid` is added; the DuckDB driver has no `lastrowid`.

### Stub, in `src/py/vertebra/_duckdb.pyi`

Inside `SyncDuckDBClient`, after `cursor`, each with a one-line docstring:

```python
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

- `()`, `[]`, and `{}` give `Empty`. Before, an empty list or tuple gave an empty positional set; both bind nothing.
- A named tuple and a `list` subclass bind by position. A `dict` subclass binds by name.
- `types.MappingProxyType({"v": 1})` binds by name. This is the defect fix.
- A generator binds by position through `try_iter`, as today.
- A statement with no result set: `fetch_rows` returns `([], ())`, `fetch_dicts` returns `[]`, and `fetch_value` raises `NotFoundError`. The statement has run.
- An empty result set: `fetch_rows` returns `([], names)`.
- Mutating DML with a returning clause: `fetch_rows` returns the rows. `execute_dml` drops them and returns `(row count, None)`.
- `execute_dml` on a read returns `(0, None)`.
- Transaction-control statements go through `run_statement` and behave as they do through the cursor.
- Errors are the classes the cursor raises, through `into_py_err`. A closed client raises what `cursor().execute` raises on a closed client.
- No transaction is opened, and no cursor lock is taken.
- A statement interrupted by `client.cancel()` raises the error the cursor raises for an interrupted statement.

## Public-Seam Test Blueprint

Rust tests, added to the `tests` module of `lib.rs`, attaching the interpreter with `attach` on `Python` and building inputs with `py.eval`:

| Test | Input | Expected |
| --- | --- | --- |
| `test_extract_bound_params_exact_containers` | `(1, 'a')`, `[1, 'a']`, `{'k': 1}` | positional with `BigInt(1)` and `Text("a")` twice; named with `k -> BigInt(1)` |
| `test_extract_bound_params_empty_containers_are_empty` | `()`, `[]`, `{}`, `None`, no object | the `Empty` variant each time |
| `test_extract_bound_params_subclasses_and_mappings` | a named tuple; a `list` subclass; a `dict` subclass; `types.MappingProxyType({'k': 1})` | positional, positional, named, named |
| `test_extract_bound_params_generator_and_bad_key` | `(x for x in [1])`; `{1: 'a'}`; the integer `5` | positional with one value; `InvalidInput`; `InvalidInput` with the sequence-or-mapping message |

Python tests, added to `src/py/tests/unit/db/test_duckdb_driver.py`, on `SyncDuckDBClient(":memory:")`. "Cursor" means `client.cursor()` run with the same SQL and parameters.

| Case | Steps | Expected |
| --- | --- | --- |
| Select equals cursor | `fetch_rows("SELECT 1 AS a, 'x' AS b")` | `(cursor.fetchall(), names from cursor.description)` |
| Dicts | `fetch_dicts` on a two-row query | one `dict` per row keyed by column name |
| Empty result | `fetch_rows`, `fetch_dicts`, `fetch_value` on `SELECT 1 AS a WHERE false` | `([], ("a",))`; `[]`; `NotFoundError` |
| No result set | `fetch_rows`, `fetch_dicts` on `CREATE TABLE t (id INTEGER)` | `([], ())`; `[]` |
| Value | `fetch_value("SELECT 42")`; `fetch_value("SELECT NULL")` | `42`; `None` |
| DML | `execute_dml` insert of two rows, update matching nothing, select | `(2, None)`; `(0, None)`; `(0, None)` |
| Returning | `fetch_rows("INSERT INTO t VALUES (9) RETURNING id")`; `execute_dml` of the same shape | `([(9,)], ("id",))`; `(1, None)` |
| Rich values | the values of `test_duckdb_native_cursor_rich_binds_match_client_execute` through `fetch_rows` | the rows the cursor returns |
| Parameter styles | `SELECT ?`, `SELECT $1`, `SELECT $v`, `SELECT :v` with a tuple, a list, a `dict`, and `types.MappingProxyType` | the rows the cursor returns; the mapping proxy binds by name |
| Empty parameters | each method with `()`, `[]`, `{}`, and `None` | same results as with no parameters |
| Duplicate names | `fetch_dicts("SELECT 1 AS a, 2 AS a")` | one key, the later value |
| Error classes | duplicate key; `SELEC 1`; missing table | the class and `sqlstate` the cursor raises for the same input |
| Closed client | `close()`, then each method | the class the cursor raises on a closed client |
| No transaction | `execute_dml` insert | `client.in_transaction` is `False` |

## Verification

- Exports for raw `cargo test`: copy the interpreter exports from the `rs-test` recipe in `Makefile`.
- Red then green, Rust: `(cd src/rs && cargo test -p vertebra-duckdb --lib extract_bound_params --locked)`
- Whole crate: `(cd src/rs && cargo test -p vertebra-duckdb --locked)`
- Lint: `(cd src/rs && cargo fmt --check -p vertebra-duckdb && cargo clippy -p vertebra-duckdb --all-targets --locked -- -D warnings)`
- Rebuild: `make dev`
- Red then green, Python: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_duckdb_driver.py -q -k single_call`
- Whole file: `uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/db/test_duckdb_driver.py -q`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._duckdb`

Name the new Python tests with `single_call` in the function name so the `-k` filter selects them.

## Acceptance Criteria

- [x] The four Rust tests and every Python row pass.
- [x] A `Mapping` that is not a `dict` binds by name.
- [x] `mypy.stubtest vertebra._duckdb` passes with no allow-list change.
- [x] The stub diff adds four methods to `SyncDuckDBClient` and nothing else.
- [x] The four methods call `run_statement` and create no cursor.
- [x] No existing test in `test_duckdb_driver.py` or the `lib.rs` test module changes.

## Notes & Discoveries

- The DuckDB statement itself costs hundreds of microseconds, so the SQL text copy inside `run_once` stays. Borrowing it would change `run_statement`, `execute_sql`, and `query_sql` for no measurable gain.
- `duckdb` 1.20000.1 panics in `Row::value_ref_internal` (`unreachable!("invalid value: Null")`) when reading an untyped `SQLNULL` column (Arrow `DataType::Null`, e.g., `SELECT NULL`) because `NullArray::is_null` returns `false`. `query_rows_on_conn` now inspects `row.as_ref().schema()` once per query and emits `DuckValue::Null` directly for `DataType::Null` columns so `fetch_value("SELECT NULL")` and cursor reads return `None` without panicking.
- Workspace-wide `make dev` and `make lint` (which auto-fix all crates/files) were scoped to `maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml`, `cargo fmt --check` + `cargo clippy -p vertebra-duckdb`, `ruff`, `mypy`, `pyright`, and `mypy.stubtest vertebra._duckdb` on the owned files so parallel workers' edits are not modified; `UV_CACHE_DIR=/tmp/uv-cache` was used inside the sandbox because `~/.cache/uv` is read-only.
