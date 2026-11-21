---
type: Chapter
flow_id: native-cursor-data-path
chapter_id: single-call-paths
title: "One native call per query: single-call methods, completed awaitables, the SQLSpec cache-hit executor, and bind fast paths"
description: "Add the four single-call methods and the completed awaitable, move the native drivers, the SQLSpec bases, and the DB-API cursor factory onto them, and check exact parameter types first in every extension."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: [network-engines]
replan_trigger: null
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
current_task: null
last_reviewed_commit: null
created_at: 2026-11-21T16:23:02Z
tags: [performance, database, pyo3, sqlspec, dbapi, native-driver]
sources:
  - id: hot-path-research
    resource: .agents/bundles/research/native-cursor-hot-paths_20261121.md
    title: "Measured per-call costs of the cursor path and the single-call design"
  - id: async-bridge
    resource: src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable
    title: "Awaitable backed by a Tokio task and a loop completion queue"
  - id: sqlite-binds
    resource: src/rs/vertebra-sqlite/src/value.rs::BoundParams
    title: "SQLite parameter extraction from Python sequences and mappings"
  - id: sqlite-pool
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: "SQLite pool, statement facts cache, and interrupt bookkeeping"
  - id: sqlite-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs
    title: "SQLite client and cursor PyO3 classes"
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "DuckDB pool, bind conversion, and client PyO3 classes"
  - id: duckdb-cursor
    resource: src/rs/vertebra-duckdb/src/cursor.rs
    title: "DuckDB cursor PyO3 classes and row converters"
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver
    title: "SQLite native driver query methods"
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver
    title: "DuckDB native driver query methods and secret refresh"
  - id: driver-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: "Native driver select family built on execute_with_description"
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec adapter base hooks called by the upstream cache-hit path"
  - id: dbapi-conn
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: "DB-API connection cursor factory"
  - id: bench-adapters
    resource: tools/benchmarks/adapters/sql.py
    title: "Adapter benchmark matrix with capture and compare commands"
---

# One native call per query: single-call methods, completed awaitables, the SQLSpec cache-hit executor, and bind fast paths

<!-- truth: start -->
- A native-driver query method on SQLite, DuckDB, PostgreSQL, and Oracle makes one native call, or awaits one awaitable.[^hot-path-research][^driver-base]
- The leased route is the one exception: it adds the checkout and the release that the lease contract requires around the one statement call.
- Each client of those four extensions, and each PostgreSQL and Oracle pool, has `fetch_rows`, `fetch_dicts`, `fetch_value`, and `execute_dml` with the return contracts in root spec section 2.[^sqlite-client][^duckdb-lib]
- A single-call method runs its statement through the runner the cursor uses and stores no cursor state.[^sqlite-pool]
- `vertebra-core` has one completed awaitable, `ReadyAwaitable`, built by `ready_ok` and `ready_err`. Awaiting it does not yield to the event loop.[^async-bridge]
- Extension crates import the completed awaitable from `vertebra_db::py_async`. Each extension holds its own copy of the class, so tests check behavior and never `isinstance`.
- `fetchone`, `fetchmany`, `fetchall`, and `close` on every async cursor return a completed awaitable. Stub return types stay `Awaitable[T]`.[^duckdb-cursor]
- A cursor state error keeps the timing it has today. SQLite and DuckDB raise it at the call.
- Parameter extraction tests the exact types `tuple`, `list`, and `dict` first. An empty container is the no-parameter value. Protocol checks run only for other types.[^sqlite-binds]
- The SQLite sync cursor passes the SQL text to the pool as a borrowed `&str` across `py.detach`.
- A SQLite statement takes the interrupt-state lock once on the success path. `cancel()` and per-operation cancel keep their results.
- SQLite and DuckDB row buffers stay `Vec<Vec<_>>`. A single-call method turns the outcome straight into Python objects.
- The SQLite and DuckDB drivers bind the four native methods when the client is set and swap in a raising callable on `close()`. No query method checks the closed flag in Python.[^sqlite-driver]
- The DuckDB error-path secret refresh wraps each single-call method exactly as it wraps the cursor-backed methods.[^duckdb-driver]
- Each SQLite client records the rowid of the last statement run through a single-call method and exposes it as `last_statement_rowid`. `driver.lastrowid` reads it, so a row method stays at one native call.
- The SQLSpec bases register a cache-hit executor whose state holds a row fetch and a statement run chosen at construction.[^sqlspec-base]
- The DB-API connections hold `cursor` as an instance attribute bound to the driver's `cursor` method. PostgreSQL keeps a per-call decision.[^dbapi-conn]
- The last task captures the adapter matrix for every acceptance row and states whether the `wire-parity` trigger fired.[^bench-adapters]
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter delivers requirements R16 to R21 of the root spec. The contracts in root spec section 2 ("Single-call methods", "Completed awaitables", "SQLSpec cache-hit executor", "Bind fast path and cursor factory") are authoritative. This chapter adds the per-engine detail that the root leaves to the source read.

The cause of each cost is measured in the hot-path research brief.[^hot-path-research]

### Findings from the source read

| Finding | Evidence | Effect on this chapter |
| --- | --- | --- |
| The crate's existing awaitable is exported from `vertebra._vertebra` only, and every extension compiles its own copy of the class.[^async-bridge] | Module doc of `py_async.rs`; `async_bridge::register` in `vertebra-py`; no awaitable class is registered in `vertebra._sqlite`. | t01 registers `ReadyAwaitable` on `vertebra._vertebra` and re-exports it from `vertebra_db::py_async`. Tests in extension test files check behavior, not class identity. |
| SQLite and DuckDB async cursors raise state errors at the call. | `take_one` and `take_many` run before the awaitable is built; `test_async_cursor_state_errors_raise_at_the_call` and `test_duckdb_async_native_cursor_closed_cursor_raises_interface_error` pin it. | t05 and t08 keep it. `ready_err` serves engines whose fetch errors surface on await today. |
| `BoundParams::from_py` in SQLite casts to `PyMapping`, then `PySequence`, before it looks at the concrete type.[^sqlite-binds] | `value.rs`, the two `cast_into` calls. | t02 adds exact checks ahead of both and keeps both as the fallback. |
| `SqliteValue::from_py` tries `extract::<i64>` on every value that is not `None` or `bool`. A `str` or `float` pays for a failed extraction. | `value.rs`, the conversion chain. | t02 adds an exact-type dispatch ahead of the chain. The chain and its order stay as the fallback. |
| `SqlitePool::run` takes the SQL text by value, and the statement runs on the calling thread.[^sqlite-pool] | `WorkerSlot::send` runs the task under the connection lock; the `mpsc` channel is written and read by one thread. | t03 changes `run` and `run_with_op_id` to take `&str` and to call the slot directly. |
| A SQLite statement locks the interrupt state three times on success. | `ensure_open_and_not_cancelled`, `begin_op`, `end_op`. | t03 keeps the lock in `begin_op` only. |
| The DuckDB bind code already checks concrete types, but an empty list or tuple is not the no-parameter value, and a `Mapping` that is not a `dict` is bound as its keys.[^duckdb-lib] | `extract_bound_params` falls through to `try_iter`. | t07 adds exact checks, the empty rule, and a mapping fallback. The mapping case is a defect fix with its own test. |
| `fetch_rows` returns no `lastrowid`. The SQLite driver sets `driver.lastrowid` from the cursor after every statement today. | Root spec return table; `test_sqlite_driver.py`, the returning-insert assertion in `test_sync_sqlite_driver_cursor_backed_query_results`. | t04 and t05 add `last_statement_rowid` to the SQLite clients. t06 turns `driver.lastrowid` into a property that reads it. The assertion passes unchanged. |

### Scope boundary

In scope: the files in the ownership table of root spec section 2 for this chapter.

Out of scope:

- The cursor classes keep every member and every state rule from `embedded-cursors` and `network-engines`. Only the return type of the async fetch methods changes.
- Existing client methods such as `execute` and `fetch_all` stay. The `cleanup-and-checks` audit decides which have no caller left.
- `executemany` paths keep an owned SQL text. They are not on the per-query path.
- Arrow, bulk-load, and pipeline paths are not edited.

## 2. Requirements, Concrete Contracts & Architecture

### Completed awaitable (R17)

t01 owns the class. Every later task uses it through `vertebra_db::py_async`.

```rust
pub fn ready_ok(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<ReadyAwaitable>>;
pub fn ready_err(py: Python<'_>, err: PyErr) -> PyResult<Py<ReadyAwaitable>>;
```

- The class is its own iterator. `__await__` returns the object, and the first `__next__` raises `StopIteration(value)` or the stored error.
- It holds no lock and no mutable state, so it can be awaited more than once and from any thread.
- A fetch method builds its Python value at the call and wraps it with `ready_ok`. It needs no running event loop.

### Single-call methods (R16)

The return contracts are in the root spec. Per engine in this chapter's first nine tasks:

| Case | SQLite | DuckDB |
| --- | --- | --- |
| `fetch_rows` on a statement with no result set | `([], ())` | `([], ())` |
| `fetch_rows` on DML with a returning clause | the returned rows and names | the returned rows and names |
| `fetch_dicts` on a statement with no result set | `[]` | `[]` |
| `fetch_value` with no row, or no result set | `NotFoundError`, message `Query returned no rows` | same |
| `execute_dml` on a statement with a result set | rows are dropped; `(0, None)` for a read; `(changes, rowid)` for a returning insert | rows are dropped; `(0, None)` for a read; `(row count, None)` for mutating DML with rows |
| `execute_dml` `lastrowid` | the engine rowid for an insert that changed rows | always `None` |
| Duplicate column names in `fetch_dicts` | the later column wins | same |
| Parameter errors | raised at the call, sync and async | same |

- The Python signature of all four is `(sql, params=())`. `None` is accepted as no parameters.
- A single-call method never opens a transaction and takes no cursor lock.
- The async forms build their result objects in the completion mapper and return one awaitable. Cancelling the awaitable cancels the operation the way the async cursor does.

### Leased route (R16 exception)

A driver query makes exactly one native call, or awaits one awaitable, on every route but one.

- The leased route applies when a broker or a client factory is attached to the driver, or when a pooled Oracle driver has a broker or pending session attributes.
- On that route a driver query makes one native statement call, plus the checkout and the release that the lease contract in root spec section 2 requires.
- SQLite and DuckDB never take this route. They take no lease.

### Oracle pool release

A pooled Oracle release sends a rollback only when the connection may hold a transaction.

| State of the connection at release | Rollback round trip |
| --- | --- |
| Nothing has run | no |
| The last server call was a successful commit or rollback | no |
| Any query has run since | yes |
| A statement failed | yes |
| `begin()` is open | yes |
| The state is unknown | yes |

The worksheet t31 holds the state tracking.

### Oracle single-call shapes

- The four Oracle single-call methods take `out_binds` and the four value options of the Oracle cursor's `execute`, as keyword-only arguments.
- The native Oracle `execute_dml` returns `(rowcount, lastrowid, out_binds, touches_session_context)`.
- The Oracle driver's `execute_dml` returns `(rowcount, lastrowid)`, the shape every other driver returns.
- The worksheets t15 to t18 hold the signatures.

### SQLite last statement rowid

`fetch_rows` returns no `lastrowid`, and the SQLite drivers report one after every statement. The client carries it instead.

- `SyncSQLiteClient` and `AsyncSQLiteClient` each hold one atomic. Every single-call method stores the statement outcome's `lastrowid` in it after a successful statement, and `0` when the outcome has none.
- A failed statement leaves the stored value as it was.
- The read-only getter `last_statement_rowid` returns `int | None`. It is `None` before any single-call statement.
- The async client stores the value in the completion mapper, before the awaitable resolves.
- Cursor statements, `execute_many`, and the older client methods do not change it.
- Thread rule: the value belongs to the client and is the last statement any thread ran through it. The driver attribute it replaces has the same rule today.
- A client made by `to_async()` or `to_sync()` starts with no value.
- DuckDB has no such getter. Its driver has no `lastrowid`.

### Bind fast path (R19)

| Step | SQLite (`BoundParams::from_py`) | DuckDB (`extract_bound_params`) |
| --- | --- | --- |
| 1 | absent or `None`: no parameters | same |
| 2 | exact `tuple`: empty is no parameters; otherwise positional | same |
| 3 | exact `list`: same rule | same |
| 4 | exact `dict`: empty is no parameters; otherwise named | same |
| 5 | `PyMapping` protocol, as today | `dict` subclass, `list` subclass, `tuple` subclass, as today |
| 6 | `PySequence` protocol, as today | `PyMapping` protocol: named (defect fix) |
| 7 | error `parameters must be a sequence or mapping` | any other iterable: positional, as today |

A generator is rejected by SQLite and accepted by DuckDB, as today.

### SQLite statement run

- `SqlitePool::run` and `run_with_op_id` take `sql: &str`. The sync cursor and the sync single-call methods pass the `&str` that PyO3 borrows from the Python `str` argument.
- The interrupt state tracks the running operation per connection in an atomic. `begin_op` is the one locked step on the success path. The end of a statement clears the atomic without the lock.
- The statement facts lookup runs before dispatch only when a reader connection could take the statement. Otherwise it runs after the statement, and only when rows changed.
- The prepared-statement cache still hashes the SQL text once. Its entry point takes the text and no precomputed key.
- Rows stay `Vec<Vec<SqliteValue>>`. Each row is moved into its tuple without a copy. A flat buffer would save one allocation per row and would change the outcome type, both cursor states, and six tests. It is recorded as a follow-up if the closing capture misses.

### Native drivers on the single-call methods (R16)

The SQLite and DuckDB drivers hold four bound callables in slots: `_fetch_rows`, `_fetch_dicts`, `_fetch_value`, `_run_dml`.

- `_bind_client(client)` sets them from the client. The constructor and `connect()` call it.
- `close()` sets all four to a module-level function that raises `InterfaceError` with the driver's existing closed message.
- A query method calls the slot directly. It does not call `_require_client()`.

| Driver method | `schema_type` is `None` or `dict` | `schema_type` set |
| --- | --- | --- |
| `select` | `fetch_dicts` | `fetch_rows`, then `hydrate_rows` |
| `select_one`, `select_one_or_none` | `fetch_dicts`, then the first item | `fetch_rows`, then `hydrate_rows`, then the first item |
| `select_value` | `fetch_value`, then `to_value_type` when `value_type` is set | |
| `execute_with_description` | `fetch_rows` | |
| `execute_tuples` | `fetch_rows`, first item | |
| `execute`, `execute_dml` | `execute_dml` | |

`cursor()` and `execute_many` are unchanged. The SQLite drivers drop the `_lastrowid` slot.

### DuckDB secret refresh

The three rules in `embedded-cursors` ("DuckDB secret refresh") hold. Each single-call driver method wraps its native call in `try/except Exception`, calls `_on_native_error`, and re-raises. The async driver keeps its `_initial_secrets_applied` check before the call.

### Cursor factory (R20)

- `SyncDBAPIConnection` and `AsyncDBAPIConnection` hold `cursor` as an instance attribute. It is a callable bound to the driver's `cursor` method, rebuilt when `autocommit` changes.
- PostgreSQL keeps a per-call decision. Its driver accepts `lazy_begin=True` only while a session is pinned, so the connection chooses the argument on each call. Implicit-transaction behavior is unchanged.
- The worksheets t25 and t26 hold the signatures.

### SQLSpec executor callables (R18)

Each SQLSpec adapter supplies the row fetch and the statement run for the executor state through `_cache_hit_callables` in its `driver.py`. The base in `_driver.py` registers the executor and reads those callables once at construction.

### File ownership inside the chapter

| File | Tasks, in order |
| --- | --- |
| `src/rs/vertebra-core/src/py_async.rs`, `src/rs/vertebra-db/src/py_async.rs`, `src/rs/vertebra-py/src/async_bridge.rs`, `src/py/vertebra/_vertebra.pyi` | t01 |
| `src/rs/vertebra-sqlite/src/worker.rs`, `src/rs/vertebra-sqlite/tests/sqlite_client.rs` | t03, then t02 (tests only) |
| `src/rs/vertebra-sqlite/src/value.rs` | t02 |
| `src/rs/vertebra-sqlite/src/py_client.rs` | t03 (two call sites), t02, t04, t05 |
| `src/py/vertebra/_sqlite.pyi` | t04, t05 |
| `src/py/vertebra/db/adapters/sqlite/{_sync,_async}.py` | t06 |
| `src/py/tests/unit/db/test_sqlite_driver.py` | t02, t04, t05, t06 |
| `src/rs/vertebra-duckdb/src/{lib,cursor}.rs`, `src/py/vertebra/_duckdb.pyi` | t07, t08 |
| `src/py/vertebra/db/adapters/duckdb/{_sync,_async}.py` | t09 |
| `src/py/tests/unit/db/test_duckdb_driver.py` | t07, t08, t09 |
| `src/py/tests/unit/ffi/test_ffi.py` | t01 |
| `src/py/vertebra/extensions/sqlspec/adapters/{sqlite,duckdb}/driver.py` | t23, then t24 |
| `src/py/vertebra/extensions/sqlspec/adapters/oracle/driver.py` | t28 |
| `src/py/vertebra/extensions/sqlspec/adapters/spanner/driver.py` | t29 |
| `src/py/vertebra/extensions/sqlspec/adapters/bigquery/driver.py` | t30 |

## 3. Documentation & OKF Knowledge Plan

No knowledge page is edited in this chapter. Hand-offs for `cleanup-and-checks`:

- `patterns/rust/ffi-and-gil-discipline.md`: the completed awaitable, the per-extension class copy, and the exact-type bind check.
- `domains/data/database-drivers-and-dialers.md`: the four single-call methods, which driver methods use them, and the bound-callable pattern for a closed driver.
- `domains/data/sqlite-limitations.md`: `driver.lastrowid` reads the client's `last_statement_rowid`, with its thread rule.
- `domains/data/database-driver-limitations.md`: the medians recorded by t06, t09, and t27.
- Docstrings on changed driver methods are updated in the task that changes them.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Tasks | Verification |
| --- | --- | --- |
| R16 single-call methods, SQLite | t03, t04, t05 | Result equality with the cursor in `test_sqlite_driver.py`; `mypy.stubtest vertebra._sqlite` |
| R16 single-call methods, DuckDB | t07, t08 | Result equality with the cursor in `test_duckdb_driver.py`; `mypy.stubtest vertebra._duckdb` |
| R16 single-call methods, PostgreSQL | t11, t12 | Client and pool tests in the PostgreSQL unit test files; `mypy.stubtest vertebra._postgres` |
| R16 single-call methods, Oracle | t15, t16 | Client and pool tests in the Oracle unit test files; `mypy.stubtest vertebra._oracle` |
| R16 per-statement round trips on the pooled Oracle route | t31 | Wire or call-count test: no rollback round trip on release after a successful commit or rollback, or when nothing ran; a rollback after a query, a failed statement, an open `begin()`, or an unknown state |
| R16 one native call per driver method | t06, t09, t13, t14, t17, t18 | Counting client double: one native call per driver method; on the leased route, one statement call plus one checkout and one release |
| R17 completed awaitable | t01 | Rust tests in `py_async.rs`: value, error, no loop iteration, repeat await |
| R17 async cursor fetches | t05, t08, t12, t16, t19, t20, t21, t22 | Each engine's test awaits a fetch and asserts a scheduled callback has not run |
| R18 SQLSpec cache-hit executor | t23, t24, t28, t29, t30 | One driver call per cached statement, sync and async; error mapping tests |
| R19 bind fast path | t02, t07, t10, t15, t19, t20, t21, t22 | Tuple, list, dict, empty, subclass, and non-dict mapping inputs per extension |
| R20 cursor factory | t25, t26 | DB-API tests for autocommit changes and a driver without `cursor()` |
| R21 cached PostgreSQL statements | t10 | Wire-frame tests asserting `Bind`, `Execute`, `Sync` |
| R13 acceptance rows | t06, t09 (samples), t27 (every row) | Adapter matrix capture written outside the repository |
| R8 delete what is superseded | t05, t08 (spawn-based ready helpers), t06, t09 (per-call closed checks) | `git grep` lists in each worksheet |

## 5. Implementation Plan

- [x] t01 — Completed awaitable in vertebra-core (tasks/t01-ready-awaitable.md)
- [x] t02 — SQLite bind fast path and borrowed SQL text (tasks/t02-sqlite-bind-fast-path.md)
- [x] t03 — SQLite statement run: one interrupt lock, single facts lookup, flat row buffer (tasks/t03-sqlite-statement-run.md)
- [x] t04 — SQLite sync single-call methods (tasks/t04-sqlite-sync-single-call.md)
- [x] t05 — SQLite async single-call methods and completed fetch awaitables (tasks/t05-sqlite-async-single-call.md)
- [x] t06 — SQLite native drivers on the single-call methods (tasks/t06-sqlite-drivers-single-call.md)
- [x] t07 — DuckDB bind fast path and sync single-call methods (tasks/t07-duckdb-sync-single-call.md)
- [x] t08 — DuckDB async single-call methods and completed fetch awaitables (tasks/t08-duckdb-async-single-call.md)
- [x] t09 — DuckDB native drivers on the single-call methods (tasks/t09-duckdb-drivers-single-call.md)
- [x] t10 — PostgreSQL bind fast path and cached-statement frames (tasks/t10-postgres-bind-and-cached-frames.md)
- [x] t11 — PostgreSQL sync single-call methods on client and pool (tasks/t11-postgres-sync-single-call.md)
- [x] t12 — PostgreSQL async single-call methods on client and pool, completed fetch awaitables (tasks/t12-postgres-async-single-call.md)
- [x] t13 — PostgreSQL sync driver on the single-call methods (tasks/t13-postgres-sync-driver.md)
- [x] t14 — PostgreSQL async driver on the single-call methods (tasks/t14-postgres-async-driver.md)
- [x] t15 — Oracle bind fast path and sync single-call methods on client and pool (tasks/t15-oracle-bind-and-sync-single-call.md)
- [x] t16 — Oracle async single-call methods and completed fetch awaitables (tasks/t16-oracle-async-single-call.md)
- [x] t17 — Oracle sync driver on the single-call methods (tasks/t17-oracle-sync-driver.md)
- [x] t18 — Oracle async driver on the single-call methods (tasks/t18-oracle-async-driver.md)
- [x] t19 — MySQL bind fast path and completed fetch awaitables (tasks/t19-mysql-bind-and-ready-fetch.md)
- [x] t20 — SQL Server bind fast path and completed fetch awaitables (tasks/t20-mssql-bind-and-ready-fetch.md)
- [x] t21 — Spanner bind fast path and completed fetch awaitables (tasks/t21-spanner-bind-and-ready-fetch.md)
- [x] t22 — BigQuery bind fast path and completed fetch awaitables (tasks/t22-bigquery-bind-and-ready-fetch.md)
- [x] t23 — SQLSpec sync cache-hit executor and `execute_dml` on the driver bases (tasks/t23-sqlspec-sync-cache-hit-executor.md)
- [x] t24 — SQLSpec async cache-hit executor (tasks/t24-sqlspec-async-cache-hit-executor.md)
- [x] t25 — Sync DB-API cursor factory bound to the native factory (tasks/t25-dbapi-sync-cursor-factory.md)
- [x] t26 — Async DB-API cursor factory bound to the native factory (tasks/t26-dbapi-async-cursor-factory.md)
- [x] t27 — Chapter close: adapter matrix capture for every acceptance row and the wire-parity trigger statement (tasks/t27-chapter-close-capture.md)
- [x] t28 — Oracle SQLSpec cache-hit executor (tasks/t28-sqlspec-oracle-cache-hit-executor.md)
- [x] t29 — Spanner SQLSpec cache-hit executor (tasks/t29-sqlspec-spanner-cache-hit-executor.md)
- [x] t30 — BigQuery SQLSpec cache-hit executor (tasks/t30-sqlspec-bigquery-cache-hit-executor.md)
- [x] t31 — Oracle pool release rolls back only a connection that may hold a transaction (tasks/t31-oracle-pool-release-reset.md)

The title of t03 is fixed. Its worksheet keeps the nested row buffer and gives the reason.

### Dependencies

| Task | Depends on |
| --- | --- |
| t01, t03, t07, t10, t15 | none |
| t02 | t03 |
| t04 | t02, t03 |
| t05 | t01, t04 |
| t06 | t05 |
| t08 | t01, t07 |
| t09 | t08 |
| t11 | t10 |
| t12 | t01, t11 |
| t13 | t11 |
| t14 | t12, t13 |
| t16 | t01, t15, t31 |
| t17 | t15 |
| t18 | t16, t17 |
| t19, t20, t21, t22 | t01 |
| t23 | t06, t09, t13, t17 |
| t24 | t23, t14, t18 |
| t25 | t06 |
| t26 | t25 |
| t28 | t23, t24, t17, t18 |
| t29 | t23, t24, t21 |
| t30 | t23, t24, t22 |
| t31 | t15 |
| t27 | every other task, t28 to t31 included |

t14 follows t13, and t18 follows t17, because each pair edits one test file.

t02 depends on t03 because both edit the `vertebra-sqlite` crate and t02 passes a borrowed `&str` to the pool signature that t03 introduces. Neither can be compiled or checked alone while the other is half done in the same checkout.

### Run order for the PostgreSQL and Oracle tasks

| Step | Tasks | Why |
| --- | --- | --- |
| 1 | t10, t15 | Separate crates. |
| 2 | t11, then t12 | Each edits the PostgreSQL client, pool, and stub files. t12 also needs t01. |
| 2 | t31, after t15 | Edits the Oracle sources t15 edited. |
| 3 | t16, after t31 and t01 | Edits the same Oracle sources again. |
| 4 | t13, then t14 | The two PostgreSQL driver tasks share one test file. |
| 4 | t17, then t18 | The two Oracle driver tasks share one test file. |

### Run order for the closing tasks

| Step | Tasks | Why |
| --- | --- | --- |
| after t01 | t19, t20, t21, t22 | One extension crate each. |
| after t06 | t25, then t26 | The DB-API connections; t26 follows t25. |
| after t06, t09, t13, t17 | t23 | The sync executor needs the four sync drivers. |
| after t23, t14, t18 | t24 | The async executor edits the file t23 edited. |
| after t24 | t28, t29, t30 | One adapter `driver.py` each. t28 also needs t17 and t18, t29 needs t21, and t30 needs t22. |
| last | t27 | Captures every acceptance row after every other task. |

### Run order for the first nine tasks

| Step | Tasks | Why |
| --- | --- | --- |
| 1 | t01, t03, t07 | Separate crates. t01 only adds symbols, so crates that compile against `vertebra-core` at the same time keep building. |
| 2 | t02 | Needs the `&str` pool signature from t03. t08 can start here once t01 is done. |
| 3 | t04, then t05, then t06 | Each edits a file the one before it edited. |
| alongside | t08, then t09 | The DuckDB chain shares no file with the SQLite chain. |

### Chapter check for the first nine tasks

Run after t06 and t09, in this order:

```bash
make lint-all
(cd src/rs && cargo test -p vertebra-core --features pyo3 --locked && cargo test -p vertebra-sqlite -p vertebra-duckdb --locked)
PYTHONPATH="src/py:." uv run --no-sync --frozen python -m mypy.stubtest vertebra._vertebra vertebra._sqlite vertebra._duckdb
uv run --no-sync --frozen pytest -p no:xdist src/py/tests/unit/ffi/test_ffi.py src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_duckdb_driver.py -q
make db-conformance ENGINE=sqlite
make db-conformance ENGINE=duckdb
```

### Local build notes

- Put the Cargo bin directory on `PATH` before calling `uv`, `cargo`, or `maturin` (`export PATH="$HOME/.cargo/bin:$PATH"`).
- Use `uv run --no-sync --frozen` so the environment and `uv.lock` are not rewritten.
- Raw `cargo test` for a PyO3 crate needs the interpreter exports that the `make rs-test` recipe sets (`PYO3_PYTHON`, `PYTHONHOME`, `PYTHONPATH`, `LD_LIBRARY_PATH`). Copy those exports or run the crate through the recipe.
- Rebuild the extensions with `make dev` before any Python test that needs a new native method.
- Run the database unit tests with `-p no:xdist`.
- A task changes only its listed files. Leave commits to the coordinator.
- Every benchmark capture writes its report to a temporary directory outside the repository.

## 6. Current State & Remaining Plan Snapshot

- Measured on 2026-11-21 under 64-bit glibc Linux, release build (`maturin develop --release --offline` across all 13 extensions after release workspace build), Python 3.12, loopback, tmpfs containers from `FIXTURE_SPECS`, `--iterations 2000 --warmup 200` for embedded engines and `--iterations 500 --warmup 50` for network engines.

| Row | Vertebra driver | Comparator | Run 1 | Run 2 | Run 3 | Median ratio | Vertebra median | Comparator median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| SQLSpec SQLite sync | `vertebra-sqlspec-sqlite-sync` | `sqlspec-sqlite-sync` | 0.746 | 0.749 | 0.756 | 0.749 | 2.21 us | 2.95 us |
| SQLAlchemy `sqlite+vtb` | `sqlalchemy-vtb-sync` | `sqlalchemy-vanilla-sync` | 1.024 | 1.000 | 1.003 | 1.003 | 26.61 us | 26.54 us |
| Native SQLite `select_one` | `vertebra-sqlite-sync` | `sqlite3` | 0.604 | 0.616 | 0.613 | 0.613 | 0.94 us | 1.54 us |
| PostgreSQL async pooled | `vertebra-async` | `asyncpg` | 1.394 | 1.241 | 0.843 | 1.241 noisy | 90.12 us | 72.60 us |
| SQLSpec PostgreSQL async | `vertebra-sqlspec-postgres-async` | `sqlspec-asyncpg` | 4.246 | 3.887 | 4.328 | 4.246 noisy | 356.05 us | 83.86 us |
| Oracle sync, relay off | `vertebra-oracle-sync` | `oracledb-sync` | 1.024 | 1.069 | 1.427 | 1.069 noisy | 74.12 us | 69.33 us |
| Native DuckDB sync | `vertebra-duckdb-sync` | `duckdb` | 1.429 | 1.580 | 1.451 | 1.451 | 365.77 us | 252.15 us |

- PostgreSQL async pooled median ratio is `1.241`, which is above `1.0`.
- Oracle sync, relay off median ratio is `1.069`, which is above `1.0`.
- The `wire-parity` trigger fired because both the PostgreSQL async pooled row and the Oracle sync, relay off row are above `1.0`.

[^hot-path-research]: `.agents/bundles/research/native-cursor-hot-paths_20261121.md`
[^async-bridge]: `src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable`
[^sqlite-binds]: `src/rs/vertebra-sqlite/src/value.rs::BoundParams`
[^sqlite-pool]: `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool`
[^sqlite-client]: `src/rs/vertebra-sqlite/src/py_client.rs`
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs`
[^duckdb-cursor]: `src/rs/vertebra-duckdb/src/cursor.rs`
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver`
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/_sync.py::SyncDuckDBDriver`
[^driver-base]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver`
[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver`
[^dbapi-conn]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection`
[^bench-adapters]: `tools/benchmarks/adapters/sql.py`
