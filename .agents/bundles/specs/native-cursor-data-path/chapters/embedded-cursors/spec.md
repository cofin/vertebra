---
type: Chapter
flow_id: native-cursor-data-path
chapter_id: embedded-cursors
title: "Native cursor in the SQLite and DuckDB extensions and their native drivers"
description: "Add the sync and async native cursor to vertebra-sqlite and vertebra-duckdb, move both native drivers onto it, and remove the per-query Python work the cursor replaces."
status: draft
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
replan_trigger: null
plan_revision: 3
plan_commit: 06e99fb
state_revision: 1
current_task: null
last_reviewed_commit: null
created_at: 2026-11-20T15:22:45Z
tags: [performance, database, dbapi, pyo3, sqlite, duckdb]
sources:
  - id: sqlite-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs
    title: "SyncSQLiteClient and AsyncSQLiteClient PyO3 classes"
  - id: sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs
    title: "SqlitePool, worker tasks, and statement execution"
  - id: sqlite-errors
    resource: src/rs/vertebra-sqlite/src/errors.rs
    title: "SQLite error classification and Python exception table"
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "DuckDBInner pool, bind conversion, and client PyO3 classes"
  - id: duckdb-errors
    resource: src/rs/vertebra-duckdb/src/errors.rs
    title: "DuckDB error classification and Python exception table"
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver
    title: "SQLite native driver query methods"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver
    title: "DuckDB native driver query methods, secret refresh, and Python cursor"
  - id: duckdb-manifest
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "vertebra-duckdb manifest with the duckdb crate features"
  - id: dbapi-errors
    resource: src/py/vertebra/db/dbapi/_sync.py::Error
    title: "DB-API exception names, bound to the native classes in t01"
---

# Native cursor in the SQLite and DuckDB extensions and their native drivers

<!-- truth: start -->
- `client.cursor()` on the four embedded clients returns a native cursor with the contract in root spec section 2.[^sqlite-client][^duckdb-lib]
- The cursor buffers Rust values and builds tuple rows when a fetch method is called.
- SQLite reads `description` from the prepared statement's column count and `rowcount` from the connection's change counters.[^sqlite-worker]
- DuckDB reads `description` and routing from statement facts cached per SQL text in Rust, because the DuckDB Rust binding does not expose the result kind. This is the one exception to the root rule that the engine reply answers.[^duckdb-lib]
- Statement facts are computed once per SQL text and kept in a 256-entry cache in each pool.
- The DB-API exception names are bindings to the `vertebra._vertebra` classes. `Warning` is the only exception class the DB-API module defines.[^dbapi-errors]
- The cursor raises through each crate's existing error conversion, the one its client methods use. Each instance carries `sqlstate`, `vendor_code`, and `is_transient`.[^sqlite-errors][^duckdb-errors]
- A cancelled statement through the cursor is a `QueryCancelledError`. `NotFoundError`, `SerializationFailure`, and `PoolExhaustedError` are raised unchanged.
- The SQLite and DuckDB native drivers create one cursor per call, take no broker lease, and do not touch the shared-memory statement cache.[^sqlite-driver][^duckdb-driver]
- DuckDB dynamic secrets refresh at connection open, after a credential rejection, and on an explicit `refresh_secrets` call.
- The Python `_SyncDuckDBCursor` and `_AsyncDuckDBCursor` classes are deleted. `driver.cursor()` returns the native cursor on all four embedded drivers.
- DuckDB work starts after the SQLite benchmark sample is recorded. Its first task builds the bundled engine with the `bundled-cmake` feature, so it uses jemalloc on 64-bit glibc Linux.[^duckdb-manifest]
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter is step 1 of the root spec. It delivers requirement R1 for the two embedded engines, the embedded parts of R2, R4, R5, R6, and R8, and R15.

### Findings from the source read

| Finding | Evidence | Effect on this chapter |
| --- | --- | --- |
| The DB-API classes and the native-driver classes are unrelated today. | `issubclass(dbapi.IntegrityError, _vertebra.DatabaseError)` is `False` in both directions. `_ErrorTranslator` copies one into the other per call. | Task t01 binds the DB-API names to the native classes. The extensions need no second error table. |
| The native classes are ordinary Python classes that take a message. | They are built with `type(name, bases, dict)` on top of `VertebraError`. `ProgrammingError("x")` constructs from Python and accepts attribute assignment. | Every place the DB-API module raises one of these names from Python keeps working after t01. |
| The SQLite and DuckDB crates resolve exception classes from `vertebra._vertebra` and build stand-ins when it is missing.[^sqlite-errors][^duckdb-errors] | `resolve_exception_types` in both `errors.rs` files. | The cursor calls the same `into_py_err` as the client methods. Neither `errors.rs` file is edited. |
| The SQLite pool already decides reader or writer routing by scanning SQL text on every `fetch_all`.[^sqlite-worker] | `is_read_only_sql(&sql)` in `fetch_all_with_op_id`. | The scan moves behind the statement facts cache. |
| `py_to_duck_value` already converts every type that `DuckDBCore.normalize_param` converts.[^duckdb-lib] | `Decimal` and `UUID` reach the `str()` branch, date and time types reach `isoformat`, and `dict`, `list`, and `tuple` reach `encode_json`. | The Python normalization is deleted with no Rust change to value conversion. |
| The bundled DuckDB engine uses the standard allocator.[^duckdb-manifest] | The `duckdb` dependency enables `bundled`. The built extension holds no jemalloc marker text; the PyPI `duckdb` module does. Sync `SELECT 1` is 426 us against 245 us. Preloading jemalloc cut a 314 us sample to 250 us. | t11 switches the feature to `bundled-cmake` before t06. |
| The DuckDB driver ships a Python cursor class per mode.[^duckdb-driver] | `_SyncDuckDBCursor` and `_AsyncDuckDBCursor`, exported from `db/adapters/duckdb/driver.py`. | Both are superseded and deleted in t09 and t10. |
| The async bridge has no helper that returns an already-finished awaitable. | `vertebra_core::py_async` exports only the `spawn_rust_task_*` family. | Async fetch methods spawn a future that is ready at once. `vertebra-core` is not edited here. |

### Scope boundary

In scope: the files in the ownership table of root spec section 2 for this chapter, plus two files that later chapters also edit:

- `src/py/vertebra/db/dbapi/_sync.py`: the exception name bindings and the removal of `_translate_native_error` (t01).
- `src/py/tests/unit/db/test_dbapi_standalone.py`: one new test (t01).

Four edits have exactly one owning task each:

| Edit | Task |
| --- | --- |
| `cursor()` on `SyncSQLiteDriver` and `AsyncSQLiteDriver` | t05 |
| Deleting `_SyncDuckDBCursor` and its export in `src/py/vertebra/db/adapters/duckdb/driver.py` | t09 |
| Deleting `_AsyncDuckDBCursor`, its export in `driver.py`, and the unused helpers in `src/py/vertebra/db/adapters/duckdb/core.py` | t10 |
| The `bundled-cmake` feature in `src/rs/vertebra-duckdb/Cargo.toml` and the `src/rs/Cargo.lock` update | t11 |

Out of scope: `client.execute`, `client.fetch_all`, and the other existing client methods stay, because the pipelines, the SQLSpec adapters, and the benchmark harness still call them. The `cleanup-and-checks` audit decides which ones have no caller left. The Arrow and bulk-load paths keep their Rust code and lose only the lease and statement-cache calls in the drivers.

## 2. Requirements, Concrete Contracts & Architecture

### Class and module layout

| Extension | Classes | Rust location | Created by |
| --- | --- | --- | --- |
| `vertebra._sqlite` | `SyncSQLiteCursor`, `AsyncSQLiteCursor` | `src/rs/vertebra-sqlite/src/py_client.rs` | `SyncSQLiteClient.cursor()`, `AsyncSQLiteClient.cursor()` |
| `vertebra._duckdb` | `SyncDuckDBCursor`, `AsyncDuckDBCursor` | `src/rs/vertebra-duckdb/src/cursor.rs` (new) | `SyncDuckDBClient.cursor()`, `AsyncDuckDBClient.cursor()` |

All four are `#[pyclass(frozen, immutable_type)]` with no `#[new]`. State sits behind one `parking_lot::Mutex`, which is never held while a statement runs. A sync cursor also has an execute lock that serializes `execute` and `executemany`. An async cursor has an in-flight flag instead.

### Statement outcome

Each pool gains one entry point that runs a statement and returns everything the cursor needs.

| Field | SQLite | DuckDB |
| --- | --- | --- |
| `columns: Option<Vec<String>>` | `Some` when `stmt.column_count() > 0` | `Some` when the cached fact `returns_rows` is true |
| `rows` | all rows, stepped to completion | all rows from `query_rows_on_conn` |
| `rowcount: i64` | `-1` when `stmt.readonly()`; `0` when `total_changes` did not move; otherwise `conn.changes()` | changed-row count for statements without rows; `rows.len()` for mutating statements with rows; `-1` otherwise |
| `lastrowid: Option<i64>` | `last_insert_rowid()` when the cached fact `is_insert` is true, rows changed, and the id is positive | always `None` |

### Statement facts cache (R4)

| Engine | Facts | Computed by |
| --- | --- | --- |
| SQLite | `read_only`, `is_insert` | existing `is_read_only_sql`; `vertebra_core::text::classify_sql_statement(sql).1` |
| DuckDB | `read_only`, `mutating`, `returns_rows`, `named_sql` | existing `is_read_only_sql`, `is_mutating_sql`, `sql_returns_rows`, `normalize_named_sql` |

The cache is a `parking_lot::RwLock<HashMap<Box<str>, Facts>>` owned by the pool. A hit takes the read lock. A miss computes the facts, takes the write lock, clears the map when it holds 256 entries, and inserts.

### Cursor state rules

These rules apply to all four classes and extend the edge cases in root spec section 2.

- `execute` and `executemany` accept `None` as well as an empty sequence for parameters. The Python signature shows `params=()`.
- `executemany` with an empty sequence sets `rowcount` to `0` and `description` to `None` without calling the engine.
- `fetchmany(0)` returns `[]`. A negative `fetchmany` size raises `ProgrammingError`.
- Setting `arraysize` below `1` raises `ProgrammingError`.
- A failed `execute` or `executemany` clears the result before the error is raised: `description` is `None`, `rowcount` is `-1`, `lastrowid` is `None`, and no rows are buffered. This covers parameter conversion errors, engine errors, and a cancelled statement.
- `description` is a tuple of `(name, None, None, None, None, None, None)` tuples, built on first read and cached until the next `execute`.
- After `close`, every method except `close` raises `InterfaceError`. `description` reads `None` and `rowcount` reads `-1`.
- The cursor has no context-manager methods. Callers use `contextlib.closing` or an explicit `close`.
- Two threads that call `execute` on one sync cursor run one after the other. The second waits on the execute lock with the interpreter detached.
- A second `execute` or `executemany` on an async cursor while one is in flight raises `InterfaceError` with the message `cursor is already executing`, at the call.
- `client.cursor()` and `driver.cursor()` take keyword-only `lazy_begin: bool = False`. With it set, `execute` and `executemany` open a transaction through the code path the client's `begin()` uses when `in_transaction` is false, then run the statement. SQLite uses the default begin mode `IMMEDIATE`. A failed begin clears the result and raises.

### Error mapping (R5)

The cursor raises through `into_py_err`, the conversion each crate's client methods already use. Every class below is the `vertebra._vertebra` class of that name. `vertebra.db.dbapi` exposes the same class objects after t01.

| Rust error kind | Class raised |
| --- | --- |
| `InvalidInput` | `InterfaceError` |
| `Programming` | `ProgrammingError` |
| `Integrity` | `IntegrityError` |
| `Data` | `DataError` |
| `NotFound` | `NotFoundError` |
| `Operational`, `ShmArrow` | `OperationalError` |
| `Cancelled` (SQLite), `QueryTimeout` (DuckDB) | `QueryCancelledError` |
| `Transaction` (DuckDB) | `SerializationFailure` |
| `PoolExhausted` (DuckDB) | `PoolExhaustedError` |
| `NotSupported` (DuckDB) | `NotSupportedError` |
| Fetch before `execute`, or after a statement with no result set; `arraysize` below `1`; negative `fetchmany` size | `ProgrammingError` |
| Any method after `close` | `InterfaceError` |

Cursor state errors are built as the `InvalidInput` or `Programming` error kind and converted the same way. No crate adds an error function, a second class table, or an import of the DB-API module.

| State error | Message |
| --- | --- |
| Closed cursor | `cursor is closed` |
| No result set | `no result set; call execute() with a row-returning statement first` |
| Invalid `arraysize` | `arraysize must be at least 1` |
| Negative fetch size | `fetchmany size must not be negative` |
| Async cursor busy | `cursor is already executing` |

### Native driver mapping (R2, R6, R8)

Each driver method creates a cursor with `client.cursor()`, uses it once, and lets it go out of scope.

| Driver method | Cursor calls | Return value |
| --- | --- | --- |
| `execute_with_description` | `execute`, `fetchall`, `description` | `(rows, tuple([c[0] for c in description]))`; `([], ())` when `description` is `None` |
| `execute_tuples`, `select`, `select_one`, `select_value` | through `execute_with_description` in the driver base | unchanged |
| `execute` | `execute`, `rowcount` | `rowcount` when positive, otherwise `0` |
| `execute_dml` (SQLite) | `execute`, `rowcount`, `lastrowid` | `(rowcount or 0, lastrowid)` |
| `execute_many` | `executemany`, `rowcount` | `rowcount`; `0` for an empty sequence without a native call |
| `cursor(*, lazy_begin=False)` (SQLite and DuckDB) | `client.cursor(lazy_begin=lazy_begin)` | the native cursor; a new one per call |

Removed from the embedded query path: `classify_sql_statement`, `_lookup_or_cache_statement`, `_acquire_broker_lease`, `_release_broker_lease`, `_sqlite_param_count`, the broker-less SQLite branch, `DuckDBCore.normalize_param`, `DuckDBCore.normalize_parameters`, `DuckDBCore.param_len`, `DuckDBCore.is_write_statement`, `DuckDBCore._scan_sql_keywords_outside_literals`, and the per-query `_refresh_dynamic_secrets()` call. The `broker` constructor argument stays accepted and unused on the query path.

### DuckDB allocator (R15)

t11 changes the `duckdb` crate feature from `bundled` to `bundled-cmake`. The build then needs a `cmake` binary on every host that compiles the extension. jemalloc is active on 64-bit glibc Linux. macOS, Windows, and musl builds keep the system allocator. The worksheet lists each build site and the check that proves jemalloc is linked.

### DuckDB secret refresh

- At open: `_refresh_dynamic_secrets(force=True)` in the constructor, as today.
- On error: every cursor-backed and Arrow method wraps its native call in `try/except Exception`. The handler calls `DuckDBCore.invalidate_rejected_credentials`. When that call removed at least one tracked generation, the handler runs `_refresh_dynamic_secrets(force=True)`, adds a note to the original exception if the refresh fails, and re-raises the original exception.
- On request: `refresh_secrets(force=...)` is unchanged, including its interval and generation checks when `force` is false.
- Time-based rotation no longer happens as a side effect of a query. The `cleanup-and-checks` knowledge update records this.

## 3. Documentation & OKF Knowledge Plan

No knowledge page is edited in this chapter. Hand-offs for `cleanup-and-checks`:

- `domains/data/sqlite-limitations.md`: the SQLite native driver takes no lease and no statement-cache entry.
- `domains/data/database-driver-limitations.md`: DuckDB secret rotation timing, the DuckDB sample numbers from t11 and t10, and the allocator scope by platform.
- The Windows deep-path caveat of the CMake build, noted in t11, for the release notes.
- The decision record cites the single exception hierarchy from t01 and its deviation from PEP 249: `InterfaceError` is a subclass of `DatabaseError`, and `Error` is `DatabaseError`.
- Docstrings on changed driver methods are updated in the task that changes them.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Tasks | Verification |
| --- | --- | --- |
| R1 One primitive (SQLite, DuckDB) | t03, t04, t05, t07, t08, t09, t10 | Cursor tests in `test_sqlite_driver.py` and `test_duckdb_driver.py`; `mypy.stubtest` for `vertebra._sqlite` and `vertebra._duckdb` |
| R2 The engine answers | t02, t05, t06, t09, t10 | Outcome tests in `tests/sqlite_client.rs` and `lib.rs`; `git grep classify_sql_statement` returns nothing under `db/adapters/sqlite` |
| R4 Per-statement work cached in Rust | t02, t06 | Rust tests that a repeated SQL text adds one cache entry and that entry 257 clears the cache |
| R5 Errors raised once | t01, t03, t04, t07, t08 | Identity test that each DB-API name is the native class; exception class per failure kind |
| R6 Leases follow connections (none taken) | t05, t09, t10 | Driver tests with a `LeaseBroker` attached assert zero leases acquired |
| R8 Delete what is superseded | t05, t09, t10 | `make lint-all`; `git grep` for each removed name in the task's acceptance list |
| R13 Native SQLite `select_one` row | t05 | Adapter matrix capture for SQLite |
| R13 Native DuckDB row, sampled | t11, t10 | Adapter matrix capture for DuckDB against the PyPI comparator; enforced in `cleanup-and-checks` |
| R15 DuckDB allocator | t11 | jemalloc marker in the built extension and wheel; `make duckdb-test`; `make duckdb-wheel-check` |

## 5. Implementation Plan

- [x] t01 — DB-API exception names bind to the native classes (tasks/t01-dbapi-exception-bindings.md)
- [x] t02 — SQLite statement outcome and statement facts cache (tasks/t02-sqlite-statement-outcome.md)
- [x] t03 — SyncSQLiteCursor and SyncSQLiteClient.cursor (tasks/t03-sqlite-sync-cursor.md)
- [x] t04 — AsyncSQLiteCursor and AsyncSQLiteClient.cursor (tasks/t04-sqlite-async-cursor.md)
- [x] t05 — SQLite native drivers call the cursor and expose cursor() (tasks/t05-sqlite-drivers-on-cursor.md)
- [x] t06 — DuckDB statement outcome and statement facts cache (tasks/t06-duckdb-statement-outcome.md)
- [x] t07 — SyncDuckDBCursor and SyncDuckDBClient.cursor (tasks/t07-duckdb-sync-cursor.md)
- [x] t08 — AsyncDuckDBCursor and AsyncDuckDBClient.cursor (tasks/t08-duckdb-async-cursor.md)
- [x] t09 — DuckDB sync driver calls the cursor (tasks/t09-duckdb-sync-driver.md)
- [x] t10 — DuckDB async driver calls the cursor and unused helpers are deleted (tasks/t10-duckdb-async-driver.md)
- [-] t11 — Build the bundled DuckDB engine with jemalloc (tasks/t11-duckdb-jemalloc-build.md)

### Run order

| Step | Tasks | Why |
| --- | --- | --- |
| 1 | t01, t02 | Separate files. Both can run at once. |
| 2 | t03 | Needs the outcome from t02 and the name bindings from t01. |
| 3 | t04 | Edits the same three files as t03. |
| 4 | t05 | Needs both SQLite cursors. Ends with the SQLite benchmark sample. |
| 5 | t06 | DuckDB follows once SQLite has proven the design. |
| 6 | t11 | The allocator change is independent of the cursor work and waits on a decision about the source of the DuckDB CMake tree. |
| 7 to 10 | t07, t08, t09, t10 | Each edits a file the previous one edited. |

### Chapter check

Run after t10, in this order:

```bash
make lint-all
(cd src/rs && cargo test -p vertebra-sqlite -p vertebra-duckdb --locked)
PYTHONPATH="src/py:." uv run --no-sync python -m mypy.stubtest vertebra._sqlite vertebra._duckdb
uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_duckdb_driver.py src/py/tests/unit/db/test_dbapi_standalone.py -q
make db-conformance ENGINE=sqlite
make db-conformance ENGINE=duckdb
```

The stub diff for each extension adds the two cursor classes, the `cursor` method on each client, and nothing else.

### Local build notes

- Put the Cargo bin directory on `PATH` before calling `uv`, `cargo`, or `maturin` (`export PATH="$HOME/.cargo/bin:$PATH"`).
- When the default uv cache is read-only, point `UV_CACHE_DIR` at a writable temporary directory.
- Build `vertebra-core` first (`maturin develop -m src/rs/vertebra-py/Cargo.toml` through `make dev`), then the companion extension; a companion build without the root package fails dependency resolution.
- Set `PYO3_PYTHON` to the project virtual environment interpreter so extensions link the same Python as the tests.
- Raw `cargo test` for a PyO3 crate needs the interpreter library path that the `make rs-test` recipe exports (`LD_LIBRARY_PATH`, `PYTHONHOME`, `PYTHONPATH`); copy those exports or run the crate through the recipe.
- Pass `--frozen` with `uv run --no-sync` so `uv.lock` is not rewritten.
- Run the database unit tests serially (`-p no:xdist`); the shared-memory cleanup fixture collides across parallel workers until the cleanup chapter fixes it.
- A task changes only its listed files. Leave commits to the coordinator.

## 6. Current State & Remaining Plan Snapshot

- Chapter complete (`10` completed, `1` deferred by user decision; uncommitted on `feat/integrated-benchmarks`).
- Completed (`t01`–`t10`): DB-API exception names bind directly to `vertebra._vertebra` classes, `SyncSQLiteCursor` / `AsyncSQLiteCursor` and `SyncDuckDBCursor` / `AsyncDuckDBCursor` ship with statement facts caches and updated `.pyi` stubs, embedded drivers call their native cursors with no broker lease on the query path, and `fetchone()` / `fetchmany()` / `fetchall()` return `None` / `[]` when `description is None`.
- Deferred (`t11`): DuckDB `bundled-cmake` / jemalloc is deferred by user approval because `libduckdb-sys` `1.20000.1` on crates.io excludes `duckdb-sources`. The build stays on `bundled` `cc`, and `vertebra-duckdb-sync` / `vertebra-sqlspec-duckdb-sync` are listed in `COMPARATOR_ALLOW_LIST`.
- Pending stamps: task commit SHAs and `last_reviewed_commit` are unrecorded until the branch is committed and reviewed.

[^sqlite-client]: `src/rs/vertebra-sqlite/src/py_client.rs`
[^sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs`
[^sqlite-errors]: `src/rs/vertebra-sqlite/src/errors.rs`
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs`
[^duckdb-errors]: `src/rs/vertebra-duckdb/src/errors.rs`
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver`
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver`
[^dbapi-errors]: `src/py/vertebra/db/dbapi/_sync.py::Error`
[^duckdb-manifest]: `src/rs/vertebra-duckdb/Cargo.toml`
