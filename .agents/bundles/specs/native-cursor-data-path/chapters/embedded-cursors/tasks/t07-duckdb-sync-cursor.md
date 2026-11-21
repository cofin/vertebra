---
type: Task
id: "native-cursor-data-path:embedded:t07"
chapter_id: embedded-cursors
title: "SyncDuckDBCursor and SyncDuckDBClient.cursor"
description: "Add the synchronous native DuckDB cursor with the PEP 249 shape in a new cursor module, its factory on the sync client, and its stub."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t01"
  - "native-cursor-data-path:embedded:t06"
read_set:
  - "src/rs/vertebra-duckdb/src/lib.rs:26-66"
  - "src/rs/vertebra-duckdb/src/lib.rs:462-472"
  - "src/rs/vertebra-duckdb/src/lib.rs:2037-2100"
  - "src/rs/vertebra-duckdb/src/lib.rs:2221-2230"
  - "src/rs/vertebra-duckdb/src/lib.rs:2345-2356"
  - "src/rs/vertebra-duckdb/src/lib.rs:2455-2462"
  - "src/rs/vertebra-duckdb/src/lib.rs:2596-2613"
  - "src/rs/vertebra-duckdb/src/lib.rs:2826-2833"
  - "src/rs/vertebra-duckdb/src/lib.rs:3874-3895"
  - "src/rs/vertebra-sqlite/src/py_client.rs"
  - "src/py/vertebra/_duckdb.pyi:1-30"
  - "src/py/vertebra/_duckdb.pyi:180-198"
  - "src/py/vertebra/_sqlite.pyi"
files:
  - src/rs/vertebra-duckdb/src/cursor.rs
  - src/rs/vertebra-duckdb/src/lib.rs
  - src/py/vertebra/_duckdb.pyi
tests:
  - src/py/tests/unit/db/test_duckdb_driver.py
estimated_loc: 250
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "DuckDBInner pool, bind conversion, and client PyO3 classes"
tags: [duckdb, pyo3, cursor, dbapi]
---

# SyncDuckDBCursor and SyncDuckDBClient.cursor

## Objective & Scope Boundary

`SyncDuckDBClient.cursor()` returns a `SyncDuckDBCursor` with the same members and rules as `SyncSQLiteCursor`. `lastrowid` is always `None`.

The cursor lives in a new file, `src/rs/vertebra-duckdb/src/cursor.rs`, declared as `mod cursor;` in `lib.rs`. As a child of the crate root it reads the private helpers it needs through `super::`. The Python drivers and the Python cursor classes are not touched here.

## Bounded Read Manifest

- `lib.rs:26-66` — imports and `BoundParams`.
- `lib.rs:462-472` — `register_cancel_flag`.
- `lib.rs:2037-2100` — `extract_bound_params`, `extract_parameter_sets`.
- `lib.rs:2221-2230`, `2345-2356` — `duck_value_to_py`, `result_set_to_py_tuples`.
- `lib.rs:2455-2462`, `2596-2613`, `2826-2833` — `SyncDuckDBClient`, its `inner` field, the detach pattern, and `to_async`; `cursor` goes after `to_async`.
- `lib.rs:3874-3895` — module registration and `__all__`.
- `src/rs/vertebra-sqlite/src/py_client.rs` — search for the `CursorState` struct and the sync cursor class from t03, and read those two blocks only. They are the shapes to mirror.
- `_duckdb.pyi:1-30`, `180-198` — stub header and the end of `SyncDuckDBClient`.
- `src/py/vertebra/_sqlite.pyi` — the sync cursor class from t03 is the stub shape to mirror.

## Contracts & Edge Cases

`cursor.rs`:

```rust
pub(crate) enum CursorStateError { Closed, NoResultSet, InvalidArraysize, NegativeFetchSize }

pub(crate) struct CursorState {
    closed: bool,
    columns: Option<Vec<String>>,
    description: Option<Py<PyTuple>>,
    rows: VecDeque<Vec<DuckValue>>,
    rowcount: i64,
    arraysize: usize,
}

#[pyclass(name = "SyncDuckDBCursor", frozen, immutable_type)]
pub struct SyncDuckDBCursor { inner: Arc<DuckDBInner>, state: Mutex<CursorState>, execute_lock: Mutex<()>, lazy_begin: bool }

impl SyncDuckDBCursor { pub(crate) fn new(inner: Arc<DuckDBInner>, lazy_begin: bool) -> Self; }
```

`CursorState` has the same methods as the SQLite one in t03: `new`, `require_open`, `install(StatementOutcome)`, `clear_result`, `take_one`, `take_many`, `take_all`, `close`. The file also holds `cursor_state_err`, `row_to_py_tuple`, `rows_to_py_list`, and `description_tuple`, built on `duck_value_to_py` and the existing `into_py_err` on `DuckDBClientError`. `cursor_state_err` raises `Closed` as the `InvalidInput` variant, which is `InterfaceError`. It raises `NoResultSet`, `InvalidArraysize`, and `NegativeFetchSize` as the `Programming` variant, which is `ProgrammingError`. Messages are the four in chapter spec section 2. A failed `execute` or `executemany` calls `clear_result` before the error is raised. No error function, class table, or import of the DB-API module is added.

`lib.rs` changes: `mod cursor;`, `pub use cursor::SyncDuckDBCursor;`, `cursor` on `SyncDuckDBClient` with the PyO3 signature `(*, lazy_begin = false)`, `m.add_class::<SyncDuckDBCursor>()`, and `"SyncDuckDBCursor"` in `__all__`.

Members and rules are those of chapter spec section 2 and of t03, with these engine differences:

- `execute` registers a cancel flag with `inner.register_cancel_flag()` and calls `inner.run_statement` inside `py.detach`.
- `executemany` converts with `extract_parameter_sets` and calls `inner.run_statement_many`.
- `lastrowid` is a getter that returns `None`.
- Serialization and lazy begin follow t03. The execute lock is taken inside `py.detach`. With `lazy_begin` true and `inner.in_transaction` false, the cursor first runs the begin path `SyncDuckDBClient.begin` runs, then the statement.
- A row value that fails conversion in `duck_value_to_py` raises that Python error unchanged from the fetch call. The rows taken for that call are consumed.
- `execute` uses the same PyO3 signature and `text_signature` as the SQLite cursor, so the Python signature shows `params=()`.
- DML without a returning clause sets `description` to `None`. DuckDB's own one-column count result is not exposed.

Stub: add `"SyncDuckDBCursor"` to `__all__`, add `def cursor(self, *, lazy_begin: bool = False) -> SyncDuckDBCursor` to `SyncDuckDBClient`, and add a `SyncDuckDBCursor` class with the same members as `SyncSQLiteCursor` in `_sqlite.pyi`.

## Test Matrix

New tests in `test_duckdb_driver.py` build `vertebra._duckdb.SyncDuckDBClient(":memory:")` directly. Exception names in this worksheet are the `vertebra._vertebra` classes. `vertebra.db.dbapi` exposes the same class objects under the same names.

| Case | Steps | Expected |
| --- | --- | --- |
| Fresh cursor | `client.cursor()` | `description is None`, `rowcount == -1`, `lastrowid is None`, `arraysize == 1` |
| Query | `execute("SELECT 1 AS a, 'x' AS b")` | returns the cursor; `fetchall() == [(1, "x")]`; `description` names `a`, `b`; `rowcount == -1` |
| DDL | `CREATE TABLE` | `description is None`, `rowcount == 0` |
| Insert | two rows in one statement | `rowcount == 2`, `description is None`, `lastrowid is None` |
| Returning | `INSERT ... RETURNING id` | `description` set, `rowcount == 1` |
| Empty result | false predicate | `fetchall() == []`, `description` names the columns |
| Rich binds | `Decimal`, `datetime`, `date`, `UUID`, and `dict` parameters into typed columns | values read back equal to what `SyncDuckDBDriver.execute` stores today |
| Named parameters | `SELECT $v` with `{"v": 5}` | `[(5,)]` |
| Fetch walk | three rows | `fetchone`, `fetchmany(1)`, `fetchall`, then `fetchone() is None` |
| Discard | two `execute` calls without fetching | second result only |
| `executemany` | three sets, then empty | `rowcount == 3`, then `0` |
| Fetch before execute, fetch after DML | `fetchall()` | `ProgrammingError` |
| Closed | `close()` twice, then `execute` | `InterfaceError` |
| `arraysize` and `fetchmany` sizes | set `arraysize` to `0`; `fetchmany(-1)`; `fetchmany(0)` | `ProgrammingError`; `ProgrammingError`; `[]` |
| Lazy begin | `client.cursor(lazy_begin=True)`; insert; read `client.in_transaction`; `client.rollback()`; count rows | `True`; zero rows |
| Lazy begin after commit | same cursor: insert, `client.commit()`, insert, `client.rollback()` | one row; the second insert opened a new transaction |
| Default cursor | `client.cursor()`; insert | `client.in_transaction` is `False`; the row is committed |
| Two threads, one cursor | two threads call `execute` on one cursor behind a barrier | both return; each call ran alone; no error |
| Failed execute clears | successful query, then `execute("SELEC 1")` | the error is raised; `description is None`, `rowcount == -1`; `fetchall()` raises `ProgrammingError` |
| Failed executemany clears | successful query, then `executemany` with a duplicate key | `IntegrityError`; `description is None`, `rowcount == -1` |
| Constraint | duplicate primary key | `IntegrityError` with `sqlstate == "23000"`; caught by both `except vertebra._vertebra.IntegrityError` and `except vertebra.db.dbapi.IntegrityError` |
| Syntax | `SELEC 1` | `ProgrammingError` |

## Verification

- Build: `uv run maturin develop --uv -m src/rs/vertebra-duckdb/Cargo.toml`
- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_duckdb_driver.py -q -k native_cursor`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._duckdb`
- Rust: `(cd src/rs && cargo test -p vertebra-duckdb --locked && cargo clippy -p vertebra-duckdb --all-targets -- -D warnings)`

Name the new tests with the `native_cursor` infix so the `-k` filter selects them.

## Acceptance Criteria

- [ ] Every row of the test matrix passes.
- [ ] `mypy.stubtest vertebra._duckdb` passes with no allow-list change.
- [ ] The stub diff adds `SyncDuckDBCursor`, `SyncDuckDBClient.cursor`, and one `__all__` entry only.
- [ ] No existing test in `test_duckdb_driver.py` changes.

## Notes & Discoveries

- `parking_lot` is not a dependency of `vertebra-duckdb` and `Cargo.toml` is outside this task's files, so the cursor uses `std::sync::Mutex`. A `std` guard cannot leave `py.detach`, so the execute lock is held inside the detached closure through the state update. The cached `description` is taken out there and dropped once attached.
- `executemany` with an empty sequence returns a zero outcome in the cursor without calling `run_statement_many`.
- `TIMESTAMP` reads back as a UTC-aware `datetime` and `UUID` reads back as `str`, the same as `SyncDuckDBClient.execute_tuples`. The rich-binds test compares the cursor against `execute_tuples` and uses a `VARCHAR` column for the `dict` parameter.
- `make lint` stops at Pyright with one error outside this task: `src/py/vertebra/_typing.py:360` cannot resolve `numpy`, which is not installed in `.venv`. Biome, Ruff, rustfmt, Clippy, and Mypy passed before it; `check_extras` and the 13-extension stubtest stage did not run. `mypy.stubtest vertebra._duckdb` passes on its own.
- `uv` needs `UV_CACHE_DIR` set to a writable directory on this host; the default cache is read-only.

