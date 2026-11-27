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
- **Adapter Layout & Single-Call Path**: `SyncSQLiteDriver` (`_sync.py`) and `AsyncSQLiteDriver` (`_async.py`) sit behind `driver.py` and bind the native client's single-call methods (`fetch_dicts`, `fetch_rows`, `fetch_value`, `execute_dml`) onto driver slots at connect (`_raise_closed` on close). `cursor()` and `execute_many` stay on the native cursor path, queries take no broker lease or SHM statement cache entry, and `execute_tuples` / `execute_with_description` run in autocommit mode outside an explicit transaction [^sqlite-sync] [^sqlite-driver] [^sqlite-py-cursor].
- **Single-Writer WAL Serialization**: WAL mode permits concurrent readers, but write transactions serialize strictly; writers exceeding `busy_timeout_ms` raise `OperationalError` (`SQLITE_BUSY`, sqlstate `55P03`) [^sqlite-worker] [^sqlite-py-client].
- **In-Memory Worker Affinity**: Plain `:memory:` pins all queries to one worker connection; multi-reader in-memory concurrency requires `file:...?mode=memory&cache=shared` [^sqlite-worker] [^sqlite-sync].
- **Dynamic Arrow Schema Inference**: Expression columns without declared table affinity infer their `VTBARROW` type from the first non-null row, defaulting all-NULL columns to `DataType::Utf8` [^sqlite-arrow].
- **Extension Loading Disabled**: Runtime `load_extension` is disabled by default for FFI and worker thread isolation [^sqlite-worker].
<!-- truth: end -->

## Operational Boundaries

### Concurrency, Transactions & Schema Inference

- **WAL Write Serialization**: Tune `busy_timeout_ms` on [`SQLiteConfig`](../../../../../src/py/vertebra/db/adapters/sqlite/config.py) to absorb write-lock contention rather than running unbounded retry loops.
- **Explicit Transaction Scope**: Because `execute_tuples` and `execute_with_description` open no implicit transaction, wrap multi-statement atomic writes in `begin()` / `commit()` / `rollback()` or `pipeline()` (which enters `BEGIN IMMEDIATE`, or `SAVEPOINT` if already inside a transaction) [^sqlite-sync] [^sqlite-worker].
- **In-Memory Thread Affinity**: [`SqliteWorkerPool`](../../../../../src/rs/vertebra-sqlite/src/worker.rs)[^sqlite-worker] routes plain `:memory:` databases through the single writer thread so state persists across queries. Use a shared-cache URI (`file:...?mode=memory&cache=shared`) for concurrent readers.
- **Dynamic Expression Schema Inference**: [`select_to_arrow`](../../../../../src/rs/vertebra-sqlite/src/arrow_bridge.rs)[^sqlite-arrow] reads declared table column types when present and infers computed expressions from the first non-null row.
- **Extension Loading Security**: `sqlite3_enable_load_extension` stays disabled; custom SQLite extensions must be compiled into the binary rather than loaded via SQL.

### Statement Routing & Cursor Semantics

- **Lexical Reader/Writer Routing (`worker.rs`)**: When reader connections exist and no transaction is open, `SqlitePool::run` uses a lexical read-only check—so a `SELECT` mentioning a write keyword (e.g., a `created_at` column) runs on the writer. `rowcount` is `-1` for read-only statements; `run_many` returns the summed changed-row count only (`lastrowid` is `None`, and `last_insert_rowid()` is unchanged).
- **Facts Cache & Cancellation Queue**: The per-SQL statement facts cache holds 256 entries and clears in full at capacity. Pre-cancelled ops fail at `begin_op` after acquiring the connection lock, and pre-cancelled IDs for closed connections sit in a 4,096-entry eviction queue [^sqlite-worker].
- **Sync Cursor Locking (`py_client.rs`)**: `SyncSQLiteCursor` serializes `execute` / `executemany` per cursor (two cursors on one client still interleave on the writer). With `lazy_begin`, empty `executemany` opens no transaction. Results completing after `close()` on another thread are discarded [^sqlite-py-cursor].
- **Async Cursor In-Flight Rules (`py_client.rs`)**: `AsyncSQLiteCursor` rejects concurrent `execute` / `executemany` with `InterfaceError("cursor is already executing")`. Cancelling or dropping an unawaited awaitable interrupts the statement and clears results, rejecting new statements until the worker finishes unwinding. Transactions opened by `lazy_begin` stay open on error or cancellation. Fetch methods drain buffered rows when called, so rows handed to a cancelled or unawaited fetch awaitable are lost [^sqlite-py-client].

### Single-Call Driver Path & SQLAlchemy Autocommit

- **Single-Call Slot Binding**: `SyncSQLiteDriver` and `AsyncSQLiteDriver` bind `fetch_dicts`, `fetch_rows`, `fetch_value`, and `execute_dml` directly in `_bind_client` and swap them to `_raise_closed` on `close()`. Custom `_client=` test doubles must expose those four methods plus `last_statement_rowid`. `lastrowid` reads `client.last_statement_rowid` (untouched by `execute_many`, `cursor()`, or failed statements) [^sqlite-py-cursor].
- **Release Benchmark Sample (2026-11-21, 2,000 iterations)**: `point_select_1` sync `0.93 us` vs `sqlite3` `1.56 us`, async `42.8 us` vs `aiosqlite` `84.7 us`; `multi_row_select_10` sync `4.81 us` vs `13.22 us`, async `47.6 us` vs `100.2 us`; `cursor_point_select_1` sync `0.74 us` vs `1.33 us`.
- **SQLAlchemy `AUTOCOMMIT` Isolation**: [`sqlite.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py) subclasses SQLAlchemy's `BaseSQLiteDialect`, which allows only `READ UNCOMMITTED` and `SERIALIZABLE`. Passing `isolation_level="AUTOCOMMIT"` raises `ArgumentError`; enable autocommit via `connect_args={"autocommit": True}`.

[^sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqliteWorkerPool` — WAL multi-reader single-writer OS worker pool.
[^sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteClient` — Native `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule` wrappers.
[^sqlite-py-cursor]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteCursor` — Native `SyncSQLiteCursor` and `AsyncSQLiteCursor` execution, classification, and result-set description.
[^sqlite-arrow]: `src/rs/vertebra-sqlite/src/arrow_bridge.rs::select_to_arrow` — SQLite `ArrowCursor` streaming and dynamic schema inference.
[^sqlite-sync]: `src/py/vertebra/db/adapters/sqlite/_sync.py::SyncSQLiteDriver` — Sync/async SQLite adapter layout, autocommit `execute_tuples`, and native-cursor query path.
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/driver.py` — Python SQLite driver barrel exporting `SyncSQLiteDriver` and `AsyncSQLiteDriver`.
