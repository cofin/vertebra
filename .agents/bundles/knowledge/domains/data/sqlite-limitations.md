---
type: Gotcha
title: SQLite Native Driver Limitations
description: Operational boundaries for SQLite single-writer WAL serialization, in-memory worker affinity, dynamic VTBARROW schema inference, and extension loading security.
status: stable
tags:
  - data
  - database
  - sqlite
  - limitations
generated:
  by: flow/0.30.1
  at: "2026-11-16T00:30:00Z"
verified:
  - by: agent:bb-executor
    at: "2026-11-21T01:40:00Z"
    commit: 06e99fbf
  - by: agent:flow-executor
    at: "2026-11-17T01:28:00Z"
    commit: 5a833832
  - by: agent:sqlite-exec
    at: "2026-11-16T00:30:00Z"
    commit: 473e1f2e
sources:
  - id: sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqliteWorkerPool
    title: WAL multi-reader single-writer OS worker pool
  - id: sqlite-py-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteClient
    title: Native SyncSQLiteClient, AsyncSQLiteClient, and broker_engine_capsule wrappers
  - id: sqlite-py-cursor
    resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor
    title: Native SyncSQLiteCursor and AsyncSQLiteCursor execution, classification, and result-set description
  - id: sqlite-arrow
    resource: src/rs/vertebra-sqlite/src/arrow_bridge.rs::select_to_arrow
    title: SQLite ArrowCursor streaming and dynamic schema inference
  - id: sqlite-sync
    resource: src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver
    title: Sync/async SQLite adapter layout, autocommit execute_tuples, and native-cursor query path
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/driver.py
    title: Python SQLite driver barrel exporting SyncSQLiteDriver and AsyncSQLiteDriver
---

# SQLite Native Driver Limitations

<!-- truth: start -->
- Adapter layout and single-call query path: `SyncSQLiteDriver` (`_sync.py`) and `AsyncSQLiteDriver` (`_async.py`) sit behind `driver.py` as a barrel and bind the native client's single-call methods (`fetch_dicts`, `fetch_rows`, `fetch_value`, `execute_dml`) onto driver slots at connect (`_raise_closed` on `close()`). Each of the eight query methods makes one native call, and `lastrowid` reads `client.last_statement_rowid`. `cursor()` and `execute_many` remain on the native cursor path; SQLite queries take no broker lease and do not touch the shared-memory statement cache. `execute_tuples` and `execute_with_description` open no implicit transaction, so statements outside an explicit caller transaction (`in_transaction == False`) run in SQLite autocommit mode [^sqlite-sync] [^sqlite-driver] [^sqlite-py-cursor].
- SQLite in WAL mode permits concurrent readers alongside writes, but write transactions are strictly serialized across OS threads and processes; concurrent writers exceeding `busy_timeout_ms` surface a transient `OperationalError` (`SQLITE_BUSY`, sqlstate `55P03`) [^sqlite-worker] [^sqlite-py-client].
- A private `:memory:` database pins execution to a single worker connection because each distinct connection opens an independent in-memory catalog; multi-reader in-memory concurrency requires shared-cache URI syntax `file:...?mode=memory&cache=shared` [^sqlite-worker] [^sqlite-sync].
- Dynamic SQLite expression columns without declared column affinity infer their `VTBARROW` arrow schema from the first non-null row encountered; expression columns evaluating entirely to NULL default to `DataType::Utf8` [^sqlite-arrow].
- Runtime SQLite extension loading (`load_extension`) is disabled by default for FFI safety and memory isolation across worker threads [^sqlite-worker].
<!-- truth: end -->

## Operational Boundaries

- **WAL Write Serialization**: While WAL mode decouples read transactions from write locks, SQLite enforces single-writer mutual exclusion at the database file level. Applications issuing bursts of write operations should tune `busy_timeout_ms` on [`SQLiteConfig`](../../../../../src/py/vertebra/db/adapters/sqlite/config.py) to accommodate lock contention rather than relying on unbounded retry loops.
- **Explicit Transaction Scope**: Because `execute_tuples` and `execute_with_description` do not open implicit transactions, multi-statement atomic writes must use explicit `begin()` / `commit()` / `rollback()` boundaries or `pipeline()`, which enters `BEGIN IMMEDIATE` when `in_transaction` is false and a `SAVEPOINT` when already inside a caller transaction [^sqlite-sync] [^sqlite-worker].
- **In-Memory Thread Affinity**: Standard `:memory:` databases create a transient database tied to the opening connection's memory space. In [`SqliteWorkerPool`](../../../../../src/rs/vertebra-sqlite/src/worker.rs)[^sqlite-worker], `:memory:` databases route all operations through the dedicated writer thread to preserve state across queries. To enable concurrent reader pools against in-memory databases, supply a named URI with `mode=memory&cache=shared`.
- **Dynamic Expression Schema Inference**: SQLite dynamically types values at the row level rather than enforcing column-level schemas on expressions. [`select_to_arrow`](../../../../../src/rs/vertebra-sqlite/src/arrow_bridge.rs)[^sqlite-arrow] inspects declared table column types when available; computed columns without declared affinity infer Arrow field types from the first non-null row.
- **Native Extension Loading Security**: For memory safety and sandbox containment, the native SQLite worker disables dynamic library loading (`sqlite3_enable_load_extension`). Extensions must be compiled directly into custom runtime binaries rather than loaded via SQL at runtime.
- **Statement Outcome Reporting**: `SqlitePool::run` in [`worker.rs`](../../../../../src/rs/vertebra-sqlite/src/worker.rs)[^sqlite-worker] picks the reader or writer connection from a lexical read-only check when reader connections are configured and no transaction is open, so a `SELECT` whose text contains a write keyword (for example a `created_at` column) runs on the writer; with no reader connections or inside a transaction, `statement_facts` is consulted only when a statement changes rows. `rowcount` is `-1` for statements SQLite reports as read-only. `run_many` reports the summed changed-row count only: `lastrowid` is always `None` and the pool's `last_insert_rowid()` is not updated. The per-SQL statement facts cache holds 256 texts and is emptied in full when a new text arrives at capacity. Pre-cancelled operations are rejected at `begin_op` after acquiring the connection lock rather than before dispatch, and a closed connection inside an open pool leaves a pre-cancelled id bounded by the 4096-entry eviction queue.
- **Cursor Execute Lock**: `SyncSQLiteCursor` in [`py_client.rs`](../../../../../src/rs/vertebra-sqlite/src/py_client.rs)[^sqlite-py-cursor] serializes `execute` and `executemany` per cursor, not per client. Two cursors on one client still interleave on the writer thread. With `lazy_begin`, `executemany` with an empty sequence opens no transaction. A statement that finishes after `close()` on another thread is discarded.
- **Async Cursor In-Flight Rules**: `AsyncSQLiteCursor` in [`py_client.rs`](../../../../../src/rs/vertebra-sqlite/src/py_client.rs)[^sqlite-py-client] accepts one `execute` or `executemany` at a time and raises `InterfaceError` (`cursor is already executing`) for a second call. Cancelling the awaitable, or dropping it without awaiting it, interrupts the statement and clears the result at once; the cursor keeps rejecting new statements until the worker reports the interrupted statement finished. A transaction opened by `lazy_begin` stays open when its statement fails or is cancelled. Each `fetchone`, `fetchmany`, `fetchall`, and `close` costs one completion round trip, and rows are taken from the buffer when the method is called, so rows handed to a fetch awaitable that is never awaited or is cancelled are lost.
- **Driver Query Path on Single-Call Native Methods**: `SyncSQLiteDriver` and `AsyncSQLiteDriver` bind `fetch_dicts`, `fetch_rows`, `fetch_value`, and `execute_dml` directly onto driver slots in `_bind_client` and swap them to `_raise_closed` on `close()`, so each of the eight query methods runs in one native call without touching `_closed` or `_require_client()`. Test doubles passed via `_client=` must expose `fetch_rows`, `fetch_dicts`, `fetch_value`, `execute_dml`, and `last_statement_rowid`. `lastrowid` reads `client.last_statement_rowid` (reporting the last single-call statement across threads; `execute_many` and `cursor()` do not change it, and failed statements preserve it). `cursor()` on either driver still returns a new native cursor and is a plain call on the async driver. The SQLite query path takes no broker lease and does not touch the shared-memory statement cache [^sqlite-py-cursor]. Release-build sample (2026-11-21, three captures, 2000 iterations, release `vertebra-core` + `vertebra-sqlite` only via `maturin develop --uv -r`): `point_select_1` sync median `0.000934 ms` vs `sqlite3` `0.001558 ms` (`0.599x`), async `0.042792 ms` vs `aiosqlite` `0.084696 ms` (`0.505x`); `multi_row_select_10` sync `0.004812 ms` vs `sqlite3` `0.013220 ms` (`0.364x`), async `0.047608 ms` vs `aiosqlite` `0.100213 ms` (`0.475x`); `cursor_point_select_1` sync `0.000736 ms` vs `sqlite3` `0.001331 ms` (`0.553x`).
- **SQLAlchemy `AUTOCOMMIT` Isolation Level**: `SyncSQLiteDialect` and `AsyncSQLiteDialect` in [`sqlite.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py) inherit directly from SQLAlchemy's `BaseSQLiteDialect`, whose allowed isolation levels are `READ UNCOMMITTED` and `SERIALIZABLE`. Passing `isolation_level="AUTOCOMMIT"` to `create_engine` / `create_async_engine` raises `sqlalchemy.exc.ArgumentError`; enable autocommit on SQLite engines via `connect_args={"autocommit": True}`.

[^sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqliteWorkerPool` — WAL multi-reader single-writer OS worker pool.
[^sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteClient` — Native `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule` wrappers.
[^sqlite-py-cursor]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor` — Native `SyncSQLiteCursor` and `AsyncSQLiteCursor` execution, classification, and result-set description.
[^sqlite-arrow]: `src/rs/vertebra-sqlite/src/arrow_bridge.rs::select_to_arrow` — SQLite `ArrowCursor` streaming and dynamic schema inference.
[^sqlite-sync]: `src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver` — Sync/async SQLite adapter layout, autocommit `execute_tuples`, and native-cursor query path.
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Python SQLite driver barrel exporting `SyncSQLiteDriver` and `AsyncSQLiteDriver`.
