---
type: Task
id: "native-cursor-data-path:embedded:t03"
chapter_id: embedded-cursors
title: "SyncSQLiteCursor and SyncSQLiteClient.cursor"
description: "Add the synchronous native SQLite cursor with the PEP 249 shape, its factory on the sync client, and its stub."
worksheet_format: compact
status: draft
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-cursor-data-path:embedded:t01"
  - "native-cursor-data-path:embedded:t02"
read_set:
  - "src/rs/vertebra-sqlite/src/py_client.rs:1-31"
  - "src/rs/vertebra-sqlite/src/py_client.rs:538-680"
  - "src/rs/vertebra-sqlite/src/py_client.rs:757-775"
  - "src/rs/vertebra-sqlite/src/lib.rs:1-41"
  - "src/rs/vertebra-sqlite/src/value.rs:51-63"
  - "src/rs/vertebra-sqlite/src/value.rs:141-160"
  - "src/py/vertebra/_sqlite.pyi:1-30"
  - "src/py/vertebra/_sqlite.pyi:150-160"
  - "src/py/tests/unit/db/test_sqlite_driver.py:1-40"
files:
  - src/rs/vertebra-sqlite/src/py_client.rs
  - src/rs/vertebra-sqlite/src/lib.rs
  - src/py/vertebra/_sqlite.pyi
  - src/rs/vertebra-sqlite/src/errors.rs
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
estimated_loc: 250
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-20T15:22:45Z
sources:
  - id: sqlite-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs
    title: "SyncSQLiteClient and AsyncSQLiteClient PyO3 classes"
tags: [sqlite, pyo3, cursor, dbapi]
---

# SyncSQLiteCursor and SyncSQLiteClient.cursor

## Objective & Scope Boundary

`SyncSQLiteClient.cursor()` returns a `SyncSQLiteCursor`. One `execute` call runs the statement in Rust and leaves rows, `description`, `rowcount`, and `lastrowid` on the cursor.

This task adds the shared cursor state that t04 reuses. It does not change any existing client method and does not touch the Python drivers.

## Bounded Read Manifest

- `py_client.rs:1-31` — imports.
- `py_client.rs:538-680` — `SyncSQLiteClient`, its `pool` field, and how `fetch_all` detaches.
- `py_client.rs:757-775` — `cancel`, `close`, `to_async`; the new `cursor` method goes after `to_async`.
- `lib.rs:1-41` — module registration and `__all__`.
- `value.rs:51-63`, `141-160` — `into_py` on `SqliteValue` and `from_py` on `BoundParams`.
- `_sqlite.pyi:1-30`, `150-160` — stub header and the end of `SyncSQLiteClient`.
- `test_sqlite_driver.py:1-40` — imports and test style.
- Chapter spec section 2 — cursor state rules and error mapping.

## Contracts & Edge Cases

Rust, in `py_client.rs`:

```rust
enum CursorStateError { Closed, NoResultSet, InvalidArraysize, NegativeFetchSize }

struct CursorState {
    closed: bool,
    columns: Option<Vec<String>>,
    description: Option<Py<PyTuple>>,
    rows: VecDeque<Vec<SqliteValue>>,
    rowcount: i64,
    lastrowid: Option<i64>,
    arraysize: usize,
}

impl CursorState {
    fn new() -> Self;                                  // rowcount -1, arraysize 1
    fn require_open(&self) -> Result<(), CursorStateError>;
    fn install(&mut self, outcome: StatementOutcome);  // replaces rows, clears cached description
    fn clear_result(&mut self);                        // no rows, columns None, rowcount -1, lastrowid None
    fn take_one(&mut self) -> Result<Option<Vec<SqliteValue>>, CursorStateError>;
    fn take_many(&mut self, size: Option<i64>) -> Result<Vec<Vec<SqliteValue>>, CursorStateError>;
    fn take_all(&mut self) -> Result<Vec<Vec<SqliteValue>>, CursorStateError>;
    fn close(&mut self);                               // idempotent; drops rows, columns None, rowcount -1
}

fn cursor_state_err(py: Python<'_>, err: CursorStateError) -> PyErr;
fn row_to_py_tuple<'py>(py: Python<'py>, row: Vec<SqliteValue>) -> PyResult<Bound<'py, PyTuple>>;
fn rows_to_py_list<'py>(py: Python<'py>, rows: Vec<Vec<SqliteValue>>) -> PyResult<Bound<'py, PyList>>;
fn description_tuple<'py>(py: Python<'py>, columns: &[String]) -> PyResult<Bound<'py, PyTuple>>;

#[pyclass(name = "SyncSQLiteCursor", frozen, immutable_type)]
pub struct SyncSQLiteCursor { pool: Arc<SqlitePool>, state: Mutex<CursorState>, execute_lock: Mutex<()>, lazy_begin: bool }
```

`SyncSQLiteClient` gains `cursor`, with the PyO3 signature `(*, lazy_begin = false)`, returning `SyncSQLiteCursor`. `lib.rs` registers the class and adds `"SyncSQLiteCursor"` to `__all__` in sorted position.

Python-visible members of `SyncSQLiteCursor`:

| Member | PyO3 form |
| --- | --- |
| `execute(sql, params=())` | `#[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]`; returns the same cursor object |
| `executemany(sql, seq_of_params)` | returns the same cursor object |
| `fetchone()` | `tuple` or `None` |
| `fetchmany(size=None)` | `list` of tuples |
| `fetchall()` | `list` of tuples |
| `close()` | `None` |
| `description`, `rowcount`, `lastrowid` | `#[getter]` |
| `arraysize` | `#[getter]` and `#[setter]` |

- `execute`: check open, convert parameters with `from_py` on `BoundParams`, call `pool.run` inside `py.detach`, then lock the state and `install`. The state lock is not held during `py.detach`.
- Serialization: `execute` and `executemany` take `execute_lock` inside the `py.detach` closure, before the pool call, and release it after the result is installed or cleared. A waiting thread never holds the interpreter.
- Lazy begin: when `lazy_begin` is true and the pool reports no open transaction, `execute` and `executemany` first call the pool function that `SyncSQLiteClient.begin` calls with its default mode `IMMEDIATE`, inside the same `py.detach` closure. A failed begin clears the result and raises, and the statement does not run. With `lazy_begin` false nothing is opened.
- `executemany`: check open, convert with the existing `extract_params_sequence`, call `pool.run_many` inside `py.detach`, then `install`.
- Engine and parameter errors go through the existing `into_py_err` on `SqliteClientError`, the conversion the client methods use. Specific classes such as `QueryCancelledError` and `NotFoundError` are raised unchanged.
- `cursor_state_err` builds a `SqliteClientError` and converts it with `into_py_err`. `Closed` uses the `InvalidInput` variant, which raises `InterfaceError`. `NoResultSet`, `InvalidArraysize`, and `NegativeFetchSize` use the `Programming` variant with `code: None` and `sqlstate: None`, which raises `ProgrammingError`. Messages are the four in chapter spec section 2.
- No error function, class table, or import of the DB-API module is added. The only edit to `src/rs/vertebra-sqlite/src/errors.rs` is the parse-failure match arm described in the notes.
- `take_*` return `NoResultSet` when `columns` is `None`. That covers a fetch before any `execute` and a fetch after a statement with no result set.
- `take_many(None)` uses `arraysize`. A size of zero returns an empty vector. A negative size returns `NegativeFetchSize`.
- `description` returns `None` when `columns` is `None`. Otherwise it builds the tuple once, stores it in `state.description`, and returns the stored object on later reads.
- A failed `execute` or `executemany` calls `clear_result` and then raises. This applies to every error after the open check: parameter conversion, the engine, and cancellation. A fetch after a failed `execute` raises `ProgrammingError`.
- The `arraysize` setter rejects values below `1` with `InvalidArraysize`.

Stub, in `_sqlite.pyi`: add `"SyncSQLiteCursor"` to `__all__`, add `def cursor(self, *, lazy_begin: bool = False) -> SyncSQLiteCursor` to `SyncSQLiteClient`, and add:

```python
@final
class SyncSQLiteCursor:
    arraysize: int
    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...] | None: ...
    @property
    def rowcount(self) -> int: ...
    @property
    def lastrowid(self) -> int | None: ...
    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = ()) -> SyncSQLiteCursor: ...
    def executemany(self, sql: str, seq_of_params: Sequence[Sequence[Any] | Mapping[str, Any]]) -> SyncSQLiteCursor: ...
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...
    def close(self) -> None: ...
```

Each stub member carries a one-line docstring in the style of the existing file.

## Test Matrix

New tests in `test_sqlite_driver.py` build `vertebra._sqlite.SyncSQLiteClient(":memory:")` directly. Exception names in this worksheet are the `vertebra._vertebra` classes. `vertebra.db.dbapi` exposes the same class objects under the same names.

| Case | Steps | Expected |
| --- | --- | --- |
| Fresh cursor | `client.cursor()` | `description is None`, `rowcount == -1`, `lastrowid is None`, `arraysize == 1` |
| Query | `execute("SELECT 1 AS a, 'x' AS b")` | returns the cursor; `description` names `a`, `b` with seven items each; `fetchall() == [(1, "x")]`; `rowcount == -1` |
| DDL | `CREATE TABLE` | `description is None`, `rowcount == 0` |
| Insert | `INSERT` with `(1, "a")` | `rowcount == 1`, `lastrowid == 1`, `description is None` |
| Named parameters | `SELECT :v` with `{"v": 5}` | `[(5,)]` |
| Returning | `INSERT ... RETURNING id` | `description` set, `rowcount == 1`, one row |
| Empty result | `SELECT` with false predicate | `fetchall() == []`, `description is not None` |
| Fetch walk | three rows | `fetchone()` then `fetchmany(1)` then `fetchall()` then `fetchone() is None` |
| `arraysize` | set to `2`, `fetchmany()` | two rows; setting `0` or `-1` raises `ProgrammingError` and leaves `arraysize` at `2` |
| `fetchmany` sizes | `fetchmany(0)`, then `fetchmany(-1)` | `[]`, then `ProgrammingError` |
| Lazy begin | `client.cursor(lazy_begin=True)`; insert; read `client.in_transaction`; `client.rollback()`; count rows | `True`; zero rows |
| Lazy begin after commit | same cursor: insert, `client.commit()`, insert, `client.rollback()` | one row; the second insert opened a new transaction |
| Default cursor | `client.cursor()`; insert | `client.in_transaction` is `False`; the row is committed |
| Two threads, one cursor | two threads call `execute` on one cursor behind a barrier | both return; each call ran alone; no error |
| Failed execute clears | successful query, then `execute("SELEC 1")` | the error is raised; `description is None`, `rowcount == -1`, `lastrowid is None`; `fetchall()` raises `ProgrammingError` |
| Failed bind clears | successful insert, then `execute("SELECT ?", (object(),))` | `InterfaceError`; `rowcount == -1`, `lastrowid is None` |
| Failed executemany clears | successful query, then `executemany` with a duplicate key | `IntegrityError`; `description is None`, `rowcount == -1` |
| Discard | `execute` twice without fetching | second result only |
| `executemany` | three sets | `rowcount == 3`, `description is None`; empty sequence gives `rowcount == 0` |
| Fetch before execute | `fetchall()` | `ProgrammingError` |
| Fetch after DML | `fetchone()` | `ProgrammingError` |
| Closed | `close()` twice, then `execute` | second `close` is silent; `execute` raises `InterfaceError` |
| Constraint | duplicate key | `IntegrityError` with `sqlstate` set; caught by both `except vertebra._vertebra.IntegrityError` and `except vertebra.db.dbapi.IntegrityError` |
| Syntax | `SELEC 1` | `ProgrammingError` |
| Unsupported bind | `execute("SELECT ?", (object(),))` | `InterfaceError` |

## Verification

- Build: `uv run maturin develop --uv -m src/rs/vertebra-sqlite/Cargo.toml`
- Red then green: `uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q -k cursor`
- Stub: `PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._sqlite`
- Rust: `(cd src/rs && cargo test -p vertebra-sqlite --locked && cargo clippy -p vertebra-sqlite --all-targets -- -D warnings)`

## Acceptance Criteria

- [ ] Every row of the test matrix passes.
- [ ] `mypy.stubtest vertebra._sqlite` passes with no allow-list change.
- [ ] The stub diff adds `SyncSQLiteCursor`, `SyncSQLiteClient.cursor`, and one `__all__` entry only.
- [ ] No existing test in `test_sqlite_driver.py` changes.

## Notes & Discoveries

- Parse failures arrive from rusqlite as its SQL-input error variant. `from_rusqlite` in `src/rs/vertebra-sqlite/src/errors.rs` routes that variant through `from_extended_code`, so a statement such as `SELEC 1` raises `ProgrammingError` like other generic engine errors. This task owns that one match arm.
- The execute lock crosses the `py.detach` boundary through `ExecuteGate`, which forgets the `parking_lot` guard and calls `force_unlock` on drop. A `parking_lot` guard is not `Send`, so it cannot be returned from the detached closure.
- `description` builds its tuple outside the state lock and stores it only when the columns are unchanged.
- With `lazy_begin`, `executemany` with an empty sequence opens no transaction, following the chapter rule that it does not call the engine.
- A result that arrives after `close()` is discarded.
- `make lint` was not run because it rewrites files across the shared checkout. Read-only checks ran on the owned files instead: `cargo fmt --check`, `cargo clippy -D warnings`, `ruff check`, `ruff format`, `mypy`, `pyright`, and `mypy.stubtest`.
- The default uv cache is read-only here; commands ran with `UV_CACHE_DIR=/tmp/uv-cache-t03`.
- The pre-run check for this chapter reports a missing-path error on this worksheet. The likely cause is the rusqlite enum variant quoted in the first note being read as a file reference; this was not confirmed by rewording the note.
- The worksheet build command fails here because plain `uv run` tries to resolve build requirements from the package index and gets HTTP 403. The same command with `uv run --no-sync` builds and installs the extension.

