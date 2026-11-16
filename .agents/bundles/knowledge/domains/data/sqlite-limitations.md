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
  - by: agent:sqlite-exec
    at: "2026-11-16T00:30:00Z"
    commit: 473e1f2e
sources:
  - id: sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqliteWorkerPool
    title: WAL multi-reader single-writer OS worker pool
  - id: sqlite-py-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteClient
    title: Native SyncSQLiteClient and AsyncSQLiteClient wrappers
  - id: sqlite-arrow
    resource: src/rs/vertebra-sqlite/src/arrow_bridge.rs::select_to_arrow
    title: SQLite ArrowCursor streaming and dynamic schema inference
  - id: sqlite-driver
    resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
    title: Python SQLite driver cutover and configuration validation
---

# SQLite Native Driver Limitations

<!-- truth: start -->
- SQLite in WAL mode permits concurrent readers alongside writes, but write transactions are strictly serialized across OS threads and processes; concurrent writers exceeding `busy_timeout_ms` surface a transient `OperationalError` (`SQLITE_BUSY`, sqlstate `55P03`) [^sqlite-worker] [^sqlite-py-client].
- A private `:memory:` database pins execution to a single worker connection because each distinct connection opens an independent in-memory catalog; multi-reader in-memory concurrency requires shared-cache URI syntax `file:...?mode=memory&cache=shared` [^sqlite-worker] [^sqlite-driver].
- Dynamic SQLite expression columns without declared column affinity infer their `VTBARROW` arrow schema from the first non-null row encountered; expression columns evaluating entirely to NULL default to `DataType::Utf8` [^sqlite-arrow].
- Runtime SQLite extension loading (`load_extension`) is disabled by default for FFI safety and memory isolation across worker threads [^sqlite-worker].
<!-- truth: end -->

## Operational Boundaries

- **WAL Write Serialization**: While WAL mode decouples read transactions from write locks, SQLite enforces single-writer mutual exclusion at the database file level. Applications issuing bursts of write operations should tune `busy_timeout_ms` on [`SQLiteConfig`](../../../../../src/py/vertebra/db/adapters/sqlite/config.py) to accommodate lock contention rather than relying on unbounded retry loops.
- **In-Memory Thread Affinity**: Standard `:memory:` databases create a transient database tied to the opening connection's memory space. In [`SqliteWorkerPool`](../../../../../src/rs/vertebra-sqlite/src/worker.rs)[^sqlite-worker], `:memory:` databases route all operations through the dedicated writer thread to preserve state across queries. To enable concurrent reader pools against in-memory databases, supply a named URI with `mode=memory&cache=shared`.
- **Dynamic Expression Schema Inference**: SQLite dynamically types values at the row level rather than enforcing column-level schemas on expressions. [`select_to_arrow`](../../../../../src/rs/vertebra-sqlite/src/arrow_bridge.rs)[^sqlite-arrow] inspects declared table column types when available; computed columns without declared affinity infer Arrow field types from the first non-null row.
- **Native Extension Loading Security**: For memory safety and sandbox containment, the native SQLite worker disables dynamic library loading (`sqlite3_enable_load_extension`). Extensions must be compiled directly into custom runtime binaries rather than loaded via SQL at runtime.

[^sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqliteWorkerPool` — WAL multi-reader single-writer OS worker pool.
[^sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::SyncSQLiteClient` — Native SyncSQLiteClient and AsyncSQLiteClient wrappers.
[^sqlite-arrow]: `src/rs/vertebra-sqlite/src/arrow_bridge.rs::select_to_arrow` — SQLite ArrowCursor streaming and dynamic schema inference.
[^sqlite-driver]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SQLite driver cutover and configuration validation.
