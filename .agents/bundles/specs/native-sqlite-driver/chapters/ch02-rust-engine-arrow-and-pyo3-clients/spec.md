---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch02-rust-engine-arrow-and-pyo3-clients
title: 'Chapter 02: Rust SQLite Config, WAL Worker Pool, VTBARROW SHM, and PyO3 Clients'
description: Implement Rust SQLiteConfig/pragmas/extended errors, 1-writer/N-reader WAL worker pool with JSONB/UDF/Backup/Interrupts, VTBARROW SHM streaming and Arrow ingest, and SyncSQLiteClient/AsyncSQLiteClient + broker_engine_capsule.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlite
- wheel
- errors
- pragmas
- wal
- arrow
- pyo3
- freethreaded
sources:
- id: duckdb-pyproj
  resource: src/rs/vertebra-duckdb/pyproject.toml
  title: Maturin standalone wheel isolation manifest precedent
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
- id: sqlite-rs
  resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
  title: Shared SQLite pragma helper in vertebra-db
- id: vertebra-py-db
  resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
  title: Legacy sqlite_initialization_pragmas binding in vertebra-py
- id: py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: AsyncCompletionQueue and Notifier PyO3 bridge
- id: arrow-shm
  resource: src/rs/vertebra-db/src/arrow_shm.rs
  title: ShmArrowStreamWriter and VTBARROW ShmCapsule constructor
- id: duckdb-lib
  resource: src/rs/vertebra-duckdb/src/lib.rs
  title: Embedded worker-thread and VTBARROW PyO3 extension precedent
- id: ft-gil-test
  resource: src/py/tests/unit/test_freethreaded_gil.py
  title: Free-threaded Python 3.14t/3.15t GIL_USED verification test
---

# Chapter 02: Rust SQLite Config, WAL Worker Pool, VTBARROW SHM, and PyO3 Clients

<!-- truth: start -->
- Every sibling maturin `pyproject.toml` (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) excludes `"**/py/vertebra/_sqlite.pyi"` [^duckdb-pyproj] [^wheel-tests].
- `test_optional_driver_wheels.py` enforces reciprocal `.pyi` isolation across `_oracle.pyi`, `_postgres.pyi`, and `_sqlite.pyi` [^wheel-tests].
- `SqliteClientConfig` computes WAL, `synchronous`, `mmap_size`, `busy_timeout`, `temp_store = MEMORY`, and `foreign_keys` pragmas and detects `:memory:` vs shared-URI vs file paths [^sqlite-rs].
- `errors.rs` maps extended `rusqlite::ffi::Error` codes (`2067`, `1555`, `2579`, `787`, `1299`, `275`, `5`, `6`, `9`, `1`, `20`) to typed `vertebra._vertebra.DatabaseError` subclasses with `vendor_code`, `sqlstate`, and `is_transient` [^vertebra-py-db].
- `worker.rs` runs 1 serialized writer thread (`SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NO_MUTEX`) + up to `max_readers` read-only WAL worker threads (`SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX`), pinning plain `:memory:` and active transactions (`BEGIN IMMEDIATE`/`DEFERRED` + `SAVEPOINT`) to the writer [^duckdb-lib].
- Bundled SQLite 3.45+ supports `JSONB` (`jsonb()`, `json_extract()`), per-op `rusqlite::InterruptHandle` (`sqlite3_interrupt()`) cancellation, and online `rusqlite::backup::Backup` `backup_to` / `restore_from` snapshots.
- `arrow_bridge.rs` streams `rusqlite::Row::get_ref` columns directly into `arrow-array 60.1.0` builders and `ShmArrowStreamWriter` (`VTBARROW` `ShmCapsule` `ArrowCursor`) and ingests Arrow IPC streams in `load_from_arrow` [^arrow-shm].
- `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`) execute off the GIL (`py.detach`) and register in `BrokerEngineDiscovery` and `test_freethreaded_gil.py` [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Maturin workspace wheels share `src/py` as `python-source`, so every sibling wheel must exclude `_sqlite.pyi` to avoid file collisions on installation [^duckdb-pyproj] [^wheel-tests]. Inside `vertebra-sqlite`, `config.rs` and `errors.rs` provide deterministic WAL pragma generation and extended SQLite constraint/transient error classification before worker threads land in `ch03`.

**Non-Goals:** Worker thread pool and `VTBARROW` builder (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel archives |
| SQLite config & errors | `src/rs/vertebra-sqlite/src/{config,errors,lib}.rs` [^sqlite-rs] | `SqliteClientConfig` + extended FFI code -> `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

Python stdlib `sqlite3` serializes behind a Python `RLock`, materializes row tuples on the GIL heap before Arrow conversion, and lacks async event-loop completion queues. `vertebra-sqlite` runs dedicated Rust OS worker threads over `SyncCompletionQueue` and `AsyncCompletionQueue` + `Notifier`, enabling true parallel WAL reads under free-threaded Python `3.14t`/`3.15t`, `sqlite3_interrupt()` cancellation, online `rusqlite::backup` snapshots, and zero-copy `VTBARROW` SHM cursors [^py-async] [^arrow-shm].

**Non-Goals:** Python `SyncSQLiteDriver` / `AsyncSQLiteDriver` cutover (`ch04`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| WAL pool & backup | `src/rs/vertebra-sqlite/src/{value,worker}.rs` | 1 writer + `max_readers` WAL pool, `JSONB`, `InterruptHandle`, `rusqlite::backup` |
| Zero-copy Arrow | `src/rs/vertebra-sqlite/src/arrow_bridge.rs` [^arrow-shm] | `sqlite3_column_*` -> `ShmArrowStreamWriter` (`VTBARROW`) + `load_from_arrow` |
| PyO3 & capsule | `src/rs/vertebra-sqlite/src/{py_client,lib}.rs`, `_sqlite.pyi`, `broker/engines.py` [^py-async] [^ft-gil-test] | `SyncSQLiteClient`, `AsyncSQLiteClient`, `broker_engine_capsule()` (`tag=3`) |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5a**: Exclude `_sqlite.pyi` in `bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, and `storage` `pyproject.toml` manifests and enforce in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `SqliteClientConfig` and extended `rusqlite::Error` -> `vertebra._vertebra` exception mapping (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `OperationalError` `is_transient`, `ProgrammingError`, `DataError`) (`t04`).

```rust
pub struct SqliteClientConfig {
    pub path: String,
    pub wal_mode: bool,
    pub synchronous: SqliteSynchronous,
    pub mmap_size: u64,
    pub busy_timeout_ms: u32,
    pub max_readers: usize,
    pub foreign_keys: bool,
    pub enable_jsonb: bool,
}
// Extended SQLite error codes -> (Python exception class, sqlstate, is_transient):
// 2067 | 1555 | 2579 -> UniqueViolationError ("23505", false)
// 787                -> ForeignKeyViolationError ("23503", false)
// 1299               -> NotNullViolationError ("23502", false)
// 275                -> CheckViolationError ("23514", false)
// 5 | 261 | 517 | 6  -> OperationalError ("55P03", true)
// 9 (SQLITE_INTERRUPT)-> OperationalError ("57014", true)
```

**Integration Flow:**
- `make sqlite-wheel-check` + `pytest test_optional_driver_wheels.py` validate wheel payload boundaries.
- `worker.rs` (`ch03`) consumes `SqliteClientConfig::initialization_pragmas()` and `map_rusqlite_error(py, err)`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid synchronous mode | `synchronous = "BOGUS"` | Raises `ValueError` immediately at client construction | SQL injection-free pragma validation |
| Constraint violation | Extended code `2067` (`SQLITE_CONSTRAINT_UNIQUE`) | `UniqueViolationError` (`vendor_code=2067`, `sqlstate="23505"`) | Dialect-neutral ORM error handling |
| Busy / interrupt | `SQLITE_BUSY` (`5`) or `SQLITE_INTERRUPT` (`9`) | `OperationalError` (`is_transient=True`) | Retryable lock/cancel classification |

- **R6**: Implement `value.rs` and `worker.rs`: single-writer + `max_readers` WAL pool, `:memory:` single-worker pinning, `BEGIN`/`SAVEPOINT` transaction pinning, full-step `RETURNING`, SQLite 3.45+ `JSONB`, prepared statement cache, `InterruptHandle` `cancel()`, and online `backup_to` / `restore_from` (`t01`).
- **R7**: Implement `arrow_bridge.rs` encoding `rusqlite::ValueRef` directly into `VTBARROW` `ShmCapsule` `ArrowCursor` batches (`batch_size = 8192`) and transactional prepared `load_from_arrow` (`t02`).
- **R8**: Implement `SyncSQLiteClient`, `AsyncSQLiteClient` (`submit_*`, `cancel_operation`, `drain_completions`, `notifier_fd`), batched `execute_pipeline`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`), synced with `_sqlite.pyi` (`t03`).
- **R9**: Register `"vertebra._sqlite"` in `BrokerEngineDiscovery.ENGINE_MODULES` and verify concurrent WAL reads, `< 50ms` recursive-CTE `InterruptHandle` abort, backup/restore, and `3.14t`/`3.15t` `GIL_USED == False` (`t04`).

```python
# src/py/vertebra/_sqlite.pyi
def broker_engine_capsule() -> object: ...

class SyncSQLiteClient:
    def __init__(self, path: str = ":memory:", *, wal_mode: bool = True, synchronous: str = "NORMAL",
                 mmap_size: int = 268435456, busy_timeout_ms: int = 5000, max_readers: int = 8,
                 foreign_keys: bool = True, enable_jsonb: bool = True) -> None: ...
    @property
    def path(self) -> str: ...
    @property
    def in_transaction(self) -> bool: ...
    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int: ...
    def execute_many(self, sql: str, params_seq: Sequence[Sequence[Any] | Mapping[str, Any]]) -> int: ...
    def execute_script(self, sql_script: str) -> None: ...
    def execute_pipeline(self, operations: Sequence[tuple[bool, str, Sequence[Any] | Mapping[str, Any] | None]],
                         *, use_savepoint: bool = False) -> list[Any]: ...
    def fetch_all(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> tuple[list[str], list[tuple[Any, ...]]]: ...
    def select_to_arrow(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None, *, batch_size: int = 8192) -> ArrowCursor: ...
    def load_from_arrow(self, table_name: str, arrow_ipc_bytes: bytes) -> int: ...
    def backup_to(self, target_path: str, *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...
    def restore_from(self, source_path: str, *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...
    def begin(self, mode: str = "IMMEDIATE") -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...

class AsyncSQLiteClient:
    # Mirrors SyncSQLiteClient constructor + submit_<op>(...) -> int, cancel_operation(op_id: int) -> bool,
    # cancel() -> None, drain_completions() -> list[tuple[int, bool, Any]], notifier_fd() -> int, close() -> None.
    ...
```

**Integration Flow:**
- Read-only `SELECT` outside a transaction dispatches to the `max_readers` WAL reader pool; writes, `RETURNING`, `execute_pipeline`, and explicit transactions pin to the single writer thread.
- `AsyncSQLiteClient.cancel_operation(op_id)` marks `AsyncCompletionQueue` cancelled and fires `InterruptHandle::interrupt()` on the active worker.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Recursive CTE hang | `cancel()` / `cancel_operation(op_id)` invoked | `InterruptHandle::interrupt()` aborts execution within `< 50ms` (`SQLITE_INTERRUPT` -> transient `OperationalError`) | Out-of-band C-API cancel |
| Plain `:memory:` | Multiple reads + writes on `:memory:` | Pinned to 1 read-write worker so all queries share the same in-memory DB | No empty reader databases |
| `INSERT ... RETURNING` | Called via `execute()` | Steps all returned rows to completion before committing | Full DML side-effects guaranteed |

## 3. Documentation & OKF Knowledge Plan

Pragma defaults and error mappings are documented in `ch03:t04`.

`vertebra._sqlite` stub docs and `sqlite-limitations.md` execute in `ch03:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust SQLiteConfig, Initialization Pragma Generator, and Extended Errors | `src/rs/vertebra-sqlite/src/config.rs`, `src/rs/vertebra-sqlite/src/errors.rs`, `src/rs/vertebra-sqlite/src/lib.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t02` | Implement 1-Writer/N-Reader WAL Pool, JSONB, Backup API, and Interrupt Registry | `src/rs/vertebra-sqlite/src/value.rs`, `src/rs/vertebra-sqlite/src/worker.rs`, `src/rs/vertebra-sqlite/src/lib.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t03` | Implement VTBARROW Arrow IPC Streaming and Bulk Arrow Table Ingestion | `src/rs/vertebra-sqlite/src/arrow_bridge.rs`, `src/rs/vertebra-sqlite/src/worker.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t04` | Expose SyncSQLiteClient, AsyncSQLiteClient, Broker Capsule, and Free-Threaded Verification | `src/rs/vertebra-sqlite/src/py_client.rs`, `src/rs/vertebra-sqlite/src/lib.rs`, `src/py/vertebra/_sqlite.pyi` (+1 more) | `src/rs/vertebra-sqlite/tests/sqlite_client.rs`, `src/py/tests/unit/test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-sqlite-config-pragmas-and-errors.md](tasks/t01-sqlite-config-pragmas-and-errors.md) — Implement SQLiteConfig, initialization_pragmas, URI/SQLCipher builder, and extended SQLite error mapping in vertebra-sqlite.
- [ ] [t02-wal-pool-jsonb-and-interrupts.md](tasks/t02-wal-pool-jsonb-and-interrupts.md) — Build dedicated writer/reader worker threads, JSONB blob detection, online backup/restore, and InterruptHandle cancellation.
- [ ] [t03-vtbarrow-shm-and-arrow-ingest.md](tasks/t03-vtbarrow-shm-and-arrow-ingest.md) — Stream SQLite result sets into VTBARROW Arrow IPC frames with ShmArena spill and ingest Arrow IPC streams via prepared transactions.
- [ ] [t04-pyo3-clients-capsule-and-freethreaded.md](tasks/t04-pyo3-clients-capsule-and-freethreaded.md) — Bind SyncSQLiteClient, AsyncSQLiteClient, and broker_engine_capsule (tag 3) in vertebra._sqlite and verify GIL_USED = False and interrupt cancellation.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Extension loading (`load_extension`) stays disabled by default; every blocking SQLite FFI call executes off the Python GIL.
- **Fast Chapter Gate (`< 60s`)**: `make sqlite-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py`

[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — ShmArrowStreamWriter and VTBARROW ShmCapsule constructor.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED verification test.
