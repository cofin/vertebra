---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch02-rust-engine-arrow-and-pyo3-clients
title: 'Chapter 02: Rust SQLite Config, WAL Worker Pool, VTBARROW SHM, and PyO3 Clients'
description: Implement Rust SqliteClientConfig/pragmas/extended errors, 1-writer/N-reader WAL worker pool with JSONB/Backup/InterruptHandle, VTBARROW SHM streaming and Arrow ingest, and SyncSQLiteClient/AsyncSQLiteClient + broker_engine_capsule.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-wheel-scaffold-and-isolation
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:10:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-14T23:10:00Z'
    commit: 1b191388
tags:
  - sqlite
  - errors
  - pragmas
  - wal
  - arrow
  - pyo3
  - freethreaded
sources:
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: Shared SQLite pragma helper in vertebra-db
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: Legacy sqlite_initialization_pragmas binding in vertebra-py
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: DbCompletionDrainer, spawn_rust_db_task_with_attr_and_cancel, and construct_py_arrow_cursor PyO3 async bridge
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: ShmArrowStreamWriter and VTBARROW ShmCapsule constructor
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: Embedded worker-thread and VTBARROW PyO3 extension precedent
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: Free-threaded Python 3.14t/3.15t GIL_USED verification test
current_task: "native-sqlite-driver:ch02-rust-engine-arrow-and-pyo3-clients:t01"
---

# Chapter 02: Rust SQLite Config, WAL Worker Pool, VTBARROW SHM, and PyO3 Clients

<!-- truth: start -->
- `SqliteClientConfig` (`src/rs/vertebra-sqlite/src/config.rs`) computes WAL, `synchronous`, `mmap_size`, `busy_timeout`, `temp_store = MEMORY`, and `foreign_keys` pragmas and distinguishes plain `:memory:`, shared-cache memory URIs, and filesystem WAL paths [^sqlite-rs].
- `errors.rs` maps extended `rusqlite::ffi::Error` codes (`2067`, `1555`, `2579`, `787`, `1299`, `275`, `5`, `261`, `517`, `6`, `9`, `1`, `20`) to typed `vertebra._vertebra.DatabaseError` subclasses with `vendor_code`, `sqlstate`, and `is_transient` [^vertebra-py-db].
- `worker.rs` runs 1 serialized writer thread (`SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NO_MUTEX`) + up to `max_readers` read-only WAL worker threads (`SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX`), pinning plain `:memory:` and active transactions (`BEGIN IMMEDIATE`/`DEFERRED` + `SAVEPOINT`) to the writer [^duckdb-lib].
- Bundled SQLite 3.45+ supports `JSONB` (`jsonb()`, `json_extract()`), per-op `rusqlite::InterruptHandle` (`sqlite3_interrupt()`) cancellation, and online `rusqlite::backup::Backup` `backup_to` / `restore_from` snapshots.
- `arrow_bridge.rs` streams `rusqlite::Row::get_ref` columns directly into `arrow-array 60.1.0` builders and `ShmArrowStreamWriter` (`VTBARROW` `ShmCapsule` `ArrowCursor`) and ingests Arrow IPC streams in `load_from_arrow` [^arrow-shm].
- `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`) execute off the GIL (`py.detach`) and register in `BrokerEngineDiscovery` and `test_freethreaded_gil.py` [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Python stdlib `sqlite3` serializes behind a Python `RLock`, allocates intermediate row tuples on the GIL heap before Arrow conversion, and lacks async event-loop completion queues. Inside the scaffolded `vertebra-sqlite` crate (`ch01`), this chapter implements deterministic WAL/URI configuration (`config.rs`), extended FFI error mapping (`errors.rs`), dedicated Rust OS worker threads (`value.rs`, `worker.rs`), direct `VTBARROW` SHM streaming (`arrow_bridge.rs`), and GIL-free `SyncSQLiteClient` / `AsyncSQLiteClient` + `broker_engine_capsule()` (`py_client.rs`, `lib.rs`) [^py-async] [^arrow-shm] [^ft-gil-test].

**Non-Goals:** Python `SyncSQLiteDriver` / `AsyncSQLiteDriver` cutover, delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline`, and CI conformance updates (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Config & errors | `src/rs/vertebra-sqlite/src/{config,errors}.rs` [^sqlite-rs] [^vertebra-py-db] | `SqliteClientConfig`, initialization pragmas, extended FFI error classes |
| WAL pool & backup | `src/rs/vertebra-sqlite/src/{value,worker}.rs` [^duckdb-lib] | 1 writer + `max_readers` WAL pool, `JSONB`, `InterruptHandle`, `rusqlite::backup` |
| Zero-copy Arrow | `src/rs/vertebra-sqlite/src/arrow_bridge.rs` [^arrow-shm] | `sqlite3_column_*` -> `ShmArrowStreamWriter` (`VTBARROW`) + `load_from_arrow` |
| PyO3 & capsule | `src/rs/vertebra-sqlite/src/{py_client,lib}.rs`, `_sqlite.pyi`, `broker/engines.py` [^py-async] [^ft-gil-test] | `SyncSQLiteClient`, `AsyncSQLiteClient`, `broker_engine_capsule()` (`tag=3`) |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md), [PyO3 FFI and Batch Dispatch](../../../../knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R5b**: Implement `SqliteClientConfig` and extended `rusqlite::Error` -> `vertebra._vertebra` exception mapping onto the existing hierarchy (`IntegrityError` carrying `sqlstate` `23505`/`23503`/`23502`/`23514`, `OperationalError` with `is_transient`, `ProgrammingError`, `DataError`; no new exception classes) (`t01`).
- **R6**: Implement `value.rs` and `worker.rs`: 1 writer + `max_readers` WAL pool, `:memory:` single-worker pinning, `BEGIN`/`SAVEPOINT` transaction pinning, full-step `RETURNING`, SQLite 3.45+ `JSONB`, prepared statement cache, `InterruptHandle` `cancel()`, and online `backup_to` / `restore_from` (`t02`).
- **R7**: Implement `arrow_bridge.rs` encoding `rusqlite::ValueRef` directly into `VTBARROW` `ShmCapsule` `ArrowCursor` batches (`batch_size = 8192`) and transactional prepared `load_from_arrow` (`t03`).
- **R8 / R9**: Implement `SyncSQLiteClient`, `AsyncSQLiteClient` (awaitables produced by `spawn_rust_db_task_with_attr_and_cancel` and drained through the `_vertebra_sqlite_completion_drainer` loop attribute, matching `vertebra-duckdb`; no polling `submit_*`/`drain_completions`/`notifier_fd` surface), batched `execute_pipeline`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`), register `"vertebra._sqlite"` in `BrokerEngineDiscovery.ENGINE_MODULES`, update `_sqlite.pyi` and `test_freethreaded_gil.py`, and verify `< 50ms` recursive-CTE cancellation (`t04`).

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
// 2067 | 1555 | 2579 -> IntegrityError ("23505", false)
// 787                -> IntegrityError ("23503", false)
// 1299               -> IntegrityError ("23502", false)
// 275                -> IntegrityError ("23514", false)
// 5 | 261 | 517 | 6  -> OperationalError ("55P03", true)
// 9 (SQLITE_INTERRUPT)-> OperationalError ("57014", true)
```

```python
def broker_engine_capsule() -> object: ...

class SyncSQLiteClient:
    def __init__(
        self,
        path: str = ":memory:",
        *,
        wal_mode: bool = True,
        synchronous: str = "NORMAL",
        mmap_size: int = 268435456,
        busy_timeout_ms: int = 5000,
        max_readers: int = 8,
        foreign_keys: bool = True,
        enable_jsonb: bool = True,
    ) -> None: ...
    @property
    def path(self) -> str: ...
    @property
    def in_transaction(self) -> bool: ...
    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int: ...
    def execute_many(self, sql: str, params_seq: Sequence[Sequence[Any] | Mapping[str, Any]]) -> int: ...
    def execute_script(self, sql_script: str) -> None: ...
    def execute_pipeline(
        self,
        operations: Sequence[tuple[bool, str, Sequence[Any] | Mapping[str, Any] | None]],
        *,
        use_savepoint: bool = False,
    ) -> list[Any]: ...
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
    def notifier_fd(self) -> int: ...
    def drain_completions(self) -> list[tuple[int, bool, Any]]: ...
    def cancel_operation(self, op_id: int) -> bool: ...
    def cancel(self) -> None: ...
    def close(self) -> None: ...
```

**Integration Flow:**
- Read-only `SELECT` outside a transaction dispatches to the `max_readers` WAL reader pool; writes, `RETURNING`, `execute_pipeline`, and explicit transactions pin to the single writer thread.
- `AsyncSQLiteClient.cancel_operation(op_id)` marks `AsyncCompletionQueue` cancelled and fires `InterruptHandle::interrupt()` on the active worker.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid `synchronous` | `synchronous = "BOGUS"` | Raises `ValueError` immediately at construction | Pragma injection blocked |
| Constraint violation | Extended codes `2067`/`787`/`1299`/`275` | `UniqueViolationError` / `ForeignKeyViolationError` / `NotNullViolationError` / `CheckViolationError` | Typed error hierarchy |
| Recursive CTE hang | `cancel()` / `cancel_operation(op_id)` invoked | `InterruptHandle::interrupt()` aborts within `< 50ms` (`SQLITE_INTERRUPT` -> transient `OperationalError`) | Out-of-band C-API cancel |
| Plain `:memory:` | Concurrent reads + writes on `:memory:` | Pinned to 1 read-write worker so all queries share the same in-memory DB | No isolated reader DBs |
| `INSERT ... RETURNING` | Executed via `execute()` | Steps all returned rows to completion before committing | Full DML side-effects |

## 3. Documentation & OKF Knowledge Plan

Pragma defaults, `JSONB`, `backup_to`/`restore_from`, and `vertebra._sqlite` stub API docs execute in `ch03:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Implement Rust SqliteClientConfig, Initialization Pragma Generator, and Extended Errors | `src/rs/vertebra-sqlite/src/{config,errors,lib}.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t02` | Implement 1-Writer/N-Reader WAL Pool, JSONB, Backup API, and Interrupt Registry | `src/rs/vertebra-sqlite/src/{value,worker,lib}.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t03` | Implement VTBARROW Arrow IPC Streaming and Bulk Arrow Table Ingestion | `src/rs/vertebra-sqlite/src/{arrow_bridge,worker}.rs` | `src/rs/vertebra-sqlite/tests/sqlite_client.rs` |
| `t04` | Expose SyncSQLiteClient, AsyncSQLiteClient, Broker Capsule, and Free-Threaded Verification | `src/rs/vertebra-sqlite/src/{py_client,lib}.rs`, `src/py/vertebra/_sqlite.pyi`, `src/py/vertebra/extensions/broker/engines.py` | `make sqlite-test`, `mypy.stubtest vertebra._sqlite`, `src/py/tests/unit/test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-sqlite-config-pragmas-and-errors.md](tasks/t01-sqlite-config-pragmas-and-errors.md) — Implement `SqliteClientConfig`, `initialization_pragmas`, URI/WAL builder, and extended SQLite error mapping in `vertebra-sqlite`.
- [ ] [t02-wal-pool-jsonb-and-interrupts.md](tasks/t02-wal-pool-jsonb-and-interrupts.md) — Build dedicated writer/reader worker threads, `JSONB` blob detection, online `backup_to`/`restore_from`, and `InterruptHandle` cancellation.
- [ ] [t03-vtbarrow-shm-and-arrow-ingest.md](tasks/t03-vtbarrow-shm-and-arrow-ingest.md) — Stream SQLite result sets into `VTBARROW` Arrow IPC frames with `ShmArena` spill and ingest Arrow IPC streams via prepared transactions.
- [ ] [t04-pyo3-clients-capsule-and-freethreaded.md](tasks/t04-pyo3-clients-capsule-and-freethreaded.md) — Bind `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule` (`tag 3`) in `vertebra._sqlite` and verify `GIL_USED = False` and interrupt cancellation.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Extension loading (`load_extension`) stays disabled by default; every blocking SQLite FFI call executes off the Python GIL.
- **Fast Chapter Gate (`< 60s`)**: `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite --locked && uv run --no-sync python -m mypy.stubtest vertebra._sqlite --concise && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py -q`

[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — ShmArrowStreamWriter and VTBARROW ShmCapsule constructor.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED verification test.
