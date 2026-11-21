---
type: Research
title: "Native Cursor Hot Paths: Measured Per-Call Costs and Single-Call Design"
description: "Layer-by-layer timings of the native cursor data path on release builds, the causes of each remaining gap against third-party drivers, and the single-call design that closes them."
status: stable
tags:
  - research
  - performance
  - database
  - sqlspec
  - sqlalchemy
  - pyo3
sources:
  - id: sqlite-cursor
    resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor
    title: "Synchronous SQLite cursor: execute, fetch, and result buffering"
  - id: sqlite-binds
    resource: src/rs/vertebra-sqlite/src/value.rs::BoundParams
    title: "SQLite parameter extraction from Python sequences and mappings"
  - id: sqlite-pool
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: "SQLite worker pool, statement facts cache, and interrupt bookkeeping"
  - id: driver-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: "Native driver select family built on execute_with_description"
  - id: sqlspec-base
    resource: src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver
    title: "SQLSpec adapter base hooks called by the upstream cache-hit path"
  - id: async-bridge
    resource: src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable
    title: "Awaitable backed by a Tokio task and a loop completion queue"
  - id: pg-cursor
    resource: src/rs/vertebra-postgres/src/cursor.rs
    title: "PostgreSQL cursors; async fetch methods spawn a task"
  - id: pg-session
    resource: src/rs/vertebra-postgres/src/session.rs
    title: "Extended-protocol framing including Describe and Close for the unnamed portal"
  - id: pg-driver
    resource: src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver
    title: "Async PostgreSQL driver query path"
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver
    title: "Sync Oracle driver query path"
  - id: dbapi-conn
    resource: src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection
    title: "DB-API connection cursor factory"
---

# Native Cursor Hot Paths: Measured Per-Call Costs and Single-Call Design

<!-- truth: start -->
- The native SQLite cursor matches `sqlite3` when one cursor is reused: `execute` plus `fetchone` is 1.33 us against 1.38 us.[^sqlite-cursor]
- The gaps sit in call count. A native-driver `select_one` makes five native calls and three Python frames; an async query awaits two awaitables.[^driver-base][^pg-cursor]
- Passing an empty tuple as parameters costs 0.35 us more than passing nothing, because parameter extraction runs two abstract-base-class checks before it looks at the concrete type.[^sqlite-binds]
- Every async cursor fetch spawns a Tokio task and crosses the event loop twice, although the rows are already in memory.[^async-bridge][^pg-cursor]
- Upstream SQLSpec exposes a per-driver cache-hit executor. A driver that registers one runs a cached statement in one call instead of five hook calls.[^sqlspec-base]
- The design that follows: one native call per native-driver query, one awaitable per async query, completed awaitables for in-memory fetches, and a registered SQLSpec cache-hit executor.
<!-- truth: end -->

## 1. Run conditions

Release extensions, Python 3.12, 64-bit glibc Linux, in-memory SQLite, best of seven loops of 5000 calls. Network-engine figures come from the acceptance captures in the `native-cursor-data-path` spec (loopback containers on tmpfs).

## 2. SQLite native path

| Call | Latency |
| --- | --- |
| `sqlite3` `conn.execute(sql, args).fetchone()` | 1.38 us |
| `sqlite3` reused cursor `execute(sql, args).fetchall()` | 1.13 us |
| `sqlite3` `cur.execute("SELECT 1")` | 0.70 us |
| Native `client.cursor()` | 0.12 us |
| Native reused cursor `execute("SELECT 1")` | 0.87 us |
| Native reused cursor `execute("SELECT 1", ())` | 1.22 us |
| Native reused cursor `execute("SELECT 1", {})` | 0.94 us |
| Native reused cursor `execute("SELECT ?", (1,))` | 1.23 us |
| Native reused cursor `execute` plus `fetchall` | 1.39 us |
| Native new cursor `execute` plus `fetchall` | 1.54 us |
| Cursor attribute read (`description`, `rowcount`, `lastrowid`) | 0.07 us each |
| Driver `execute_with_description` | 2.05 us |
| Driver `hydrate_rows` for one row | 0.71 us |
| Driver `select` | 3.02 us |
| Driver `select_one` | 3.37 us |
| Driver `select_value` | 2.27 us |

Causes, in order of size:

1. Row hydration in Python: 0.71 us for one row. It runs two `isinstance` checks against abstract base classes per call.
2. Three interpreted frames (`select_one`, `select`, `execute_with_description`) and five native calls (`cursor`, `execute`, `description`, `lastrowid`, `fetchall`): about 1.0 us.
3. Parameter extraction: `BoundParams::from_py` casts to `PyMapping`, then to `PySequence`, then reads items by index. A tuple fails the mapping check through `collections.abc`, which costs 0.35 us even when the tuple is empty.[^sqlite-binds]
4. Statement run overhead inside the extension, 0.17 us over `sqlite3` with no parameters: the SQL text is copied to an owned `String`; the cursor takes its state lock twice and its execute lock once; the pool takes the interrupt-state lock four times and inserts and removes a map entry per statement; the statement facts cache and the prepared-statement cache each hash the SQL text; rows are buffered as `Vec<Vec<SqliteValue>>` and converted to tuples on fetch.[^sqlite-cursor][^sqlite-pool]

## 3. SQLSpec SQLite path

| Call | Latency |
| --- | --- |
| Upstream SQLite adapter `select_one("SELECT 1")` | 2.71 us |
| Upstream adapter through an interpreted subclass with no overrides | 2.69 us |
| Vertebra SQLite adapter `select_one("SELECT 1")` | 4.68 us |
| Same with `collect_rows` replaced by a constant | 4.33 us |

- The upstream adapter registers a cache-hit executor (`_cache_hit_executor`, `_cache_hit_state` on the driver). `run_cached_execution` calls it as `executor(connection, sql, params, cached, statement_config, state)`. It returns the result, or `None` to decline. An async executor returns an awaitable.
- The Vertebra adapter registers none, so a cached statement goes through `_execute_cache_hit` and calls back into interpreted code ten times: `handle_database_exceptions`, `with_cursor`, the cursor context `__init__`, `__enter__`, driver `cursor`, `_require_client`, `collect_rows`, `__exit__`, cursor `close`, and `len`.[^sqlspec-base]
- A callable that is not a descriptor works as a class-level hook: with `resolve_rowcount` set to `operator.attrgetter("rowcount")` an insert reports one affected row. The compiled base looks hooks up by name on interpreted subclasses.

## 4. SQLAlchemy SQLite path

| Call | Latency |
| --- | --- |
| `sqlite` dialect `execute` plus `fetchone` | 18.23 us |
| `sqlite+vtb` dialect `execute` plus `fetchone` | 20.18 us |
| `sqlite3` `conn.cursor()` | 0.20 us |
| DB-API `connection.cursor()` | 0.30 us |
| DB-API cursor `execute("SELECT 1", ())` | 1.21 us |

SQLAlchemy core accounts for about 17 us on both sides. The 1.95 us difference is the cursor factory (two Python frames before the native call), the empty-tuple parameter cost, and the statement run overhead from section 2.[^dbapi-conn]

## 5. PostgreSQL async path

Acceptance captures: 117 to 137 us against `asyncpg` at 72 us for the pooled native driver; 146 to 205 us against 84 to 148 us through SQLSpec.

1. Two awaitables per query. `execute_with_description` awaits `cursor.execute`, then awaits `cursor.fetchall`. Each await spawns a Tokio task and completes through the loop completion queue, so the query crosses threads four times. The rows are already buffered when `fetchall` runs.[^pg-cursor][^async-bridge]
2. Three coroutine frames per query before the native call: `_execute_cursor`, `_ensure_pool_async`, `_execute_on_pool`.[^pg-driver]
3. A cached statement still sends `Describe` for the unnamed portal and `Close` for it. The row description is known from the cached statement, and the unnamed portal is replaced by the next `Bind`.[^pg-session]
4. One thread crossing each way remains after the items above. `asyncpg` reads and writes on the event loop thread and has none.

## 6. Oracle sync path

Acceptance captures with the relay off: 84 to 89 us against `oracledb` at 68 us. The extension call alone measures about 73 us.

1. The driver wraps every statement in `checkout_cursor`, a generator-based context manager, then makes five native calls and reads the session-context flag.[^oracle-driver]
2. About 5 us sits inside the extension call. Its cause is not measured. A packet-level comparison against `oracledb` is the next step.

## 7. Design

| Rule | Applies to |
| --- | --- |
| A native-driver query method makes one native call, or awaits one awaitable. Rows, column names, and row dictionaries are built in Rust. | SQLite, DuckDB, PostgreSQL, Oracle native drivers |
| A fetch that needs no I/O returns an awaitable that is already complete. | Every async cursor |
| A cached SQLSpec statement runs through a registered cache-hit executor that makes one driver call. | Every SQLSpec adapter |
| Parameter extraction checks the exact type (`tuple`, `list`, `dict`) first and treats an empty container as no parameters. | Every extension |
| The DB-API connection hands out cursors through a callable bound to the native factory. | DB-API module |
| A cached PostgreSQL statement sends `Bind`, `Execute`, and `Sync` only. | `vertebra-postgres` |

Expected effect on SQLite, from the measured parts: native `select_one` about 1.2 us (one frame, one call near 1.0 us); SQLSpec `select_one` near the upstream figure, because the executor replaces ten interpreted calls with one native call; SQLAlchemy within about 0.5 us of the upstream dialect before the statement run overhead is trimmed.

PostgreSQL async drops to two thread crossings per query. Whether that reaches `asyncpg` is measured after the change. If it does not, the remaining cost is the Tokio-to-loop crossing, and the next step is to write the request on the calling thread and read the reply on the event loop thread.

[^sqlite-cursor]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor`
[^sqlite-binds]: `src/rs/vertebra-sqlite/src/value.rs::BoundParams`
[^sqlite-pool]: `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool`
[^driver-base]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver`
[^sqlspec-base]: `src/py/vertebra/extensions/sqlspec/_driver.py::BaseSyncDriver`
[^async-bridge]: `src/rs/vertebra-core/src/py_async.rs::RustTaskAwaitable`
[^pg-cursor]: `src/rs/vertebra-postgres/src/cursor.rs`
[^pg-session]: `src/rs/vertebra-postgres/src/session.rs`
[^pg-driver]: `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver`
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver`
[^dbapi-conn]: `src/py/vertebra/db/dbapi/_sync.py::SyncDBAPIConnection`
