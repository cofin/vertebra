---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch03-worker-pool-arrow-and-pyo3-clients
title: "Chapter 03: WAL Single-Writer/Multi-Reader Pool, JSONB, InterruptHandle, Backup, VTBARROW, and PyO3 Clients"
description: "Implement the 3.14t/3.15t-safe single-writer + multi-reader WAL worker pool, SQLite 3.45+ JSONB, InterruptHandle cancel(), online backup/snapshot hooks, VTBARROW ShmCapsule ArrowCursor + load_from_arrow, SyncSQLiteClient/AsyncSQLiteClient, and broker_engine_capsule."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlite
  - wal
  - arrow
  - pyo3
  - freethreaded
sources:
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: "ShmArrowStreamWriter and VTBARROW ShmCapsule constructor"
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "Embedded worker-thread and VTBARROW PyO3 extension precedent"
  - id: ft-gil-test
    resource: src/py/tests/unit/test_freethreaded_gil.py
    title: "Free-threaded Python 3.14t/3.15t GIL_USED verification test"
---

# Chapter 03: WAL Single-Writer/Multi-Reader Pool, JSONB, InterruptHandle, Backup, VTBARROW, and PyO3 Clients

<!-- truth: start -->
- `worker.rs` runs 1 serialized writer thread (`SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NO_MUTEX`) + up to `max_readers` read-only WAL worker threads (`SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_NO_MUTEX`), pinning plain `:memory:` and active transactions (`BEGIN IMMEDIATE`/`DEFERRED` + `SAVEPOINT`) to the writer [^duckdb-lib].
- Bundled SQLite 3.45+ supports `JSONB` (`jsonb()`, `json_extract()`), per-op `rusqlite::InterruptHandle` (`sqlite3_interrupt()`) cancellation, and online `rusqlite::backup::Backup` `backup_to` / `restore_from` snapshots.
- `arrow_bridge.rs` streams `rusqlite::Row::get_ref` columns directly into `arrow-array 60.1.0` builders and `ShmArrowStreamWriter` (`VTBARROW` `ShmCapsule` `ArrowCursor`) and ingests Arrow IPC streams in `load_from_arrow` [^arrow-shm].
- `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`) execute off the GIL (`py.detach`) and register in `BrokerEngineDiscovery` and `test_freethreaded_gil.py` [^py-async] [^ft-gil-test].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Python stdlib `sqlite3` serializes behind a Python `RLock`, materializes row tuples on the GIL heap before Arrow conversion, and lacks async event-loop completion queues. `vertebra-sqlite` runs dedicated Rust OS worker threads over `SyncCompletionQueue` and `AsyncCompletionQueue` + `Notifier`, enabling true parallel WAL reads under free-threaded Python `3.14t`/`3.15t`, `sqlite3_interrupt()` cancellation, online `rusqlite::backup` snapshots, and zero-copy `VTBARROW` SHM cursors [^py-async] [^arrow-shm].

**Non-Goals:** Python `SyncSQLiteDriver` / `AsyncSQLiteDriver` cutover (`ch04`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| WAL pool & backup | `src/rs/vertebra-sqlite/src/{value,worker}.rs` | 1 writer + `max_readers` WAL pool, `JSONB`, `InterruptHandle`, `rusqlite::backup` |
| Zero-copy Arrow | `src/rs/vertebra-sqlite/src/arrow_bridge.rs` [^arrow-shm] | `sqlite3_column_*` -> `ShmArrowStreamWriter` (`VTBARROW`) + `load_from_arrow` |
| PyO3 & capsule | `src/rs/vertebra-sqlite/src/{py_client,lib}.rs`, `_sqlite.pyi`, `broker/engines.py` [^py-async] [^ft-gil-test] | `SyncSQLiteClient`, `AsyncSQLiteClient`, `broker_engine_capsule()` (`tag=3`) |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Benchmarks and Free-Threaded Python](../../../../knowledge/testing/benchmarks-and-free-threaded.md).

## 2. Requirements, Concrete Contracts & Architecture

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

`vertebra._sqlite` stub docs and `sqlite-limitations.md` execute in `ch05`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R6` | `t01` | `src/rs/vertebra-sqlite/src/{value,worker,lib}.rs` | `cargo test -p vertebra-sqlite` |
| `R7` | `t02` | `src/rs/vertebra-sqlite/src/{arrow_bridge,worker}.rs` | `cargo test -p vertebra-sqlite` |
| `R8` | `t03` | `src/rs/vertebra-sqlite/src/{py_client,lib}.rs`, `src/py/vertebra/_sqlite.pyi` | `mypy.stubtest vertebra._sqlite` |
| `R9` | `t04` | `src/py/vertebra/extensions/broker/engines.py` | `make sqlite-test && pytest test_freethreaded_gil.py` |

## 5. Implementation Plan

- [ ] [t01-wal-pool-jsonb-and-interrupts.md](tasks/t01-wal-pool-jsonb-and-interrupts.md) — Implement value.rs and worker.rs with WAL reader/writer threads, JSONB, InterruptHandle, savepoints, and online backup_to/restore_from.
- [ ] [t02-vtbarrow-shm-and-arrow-ingest.md](tasks/t02-vtbarrow-shm-and-arrow-ingest.md) — Stream SQLite rows directly into arrow-array builders and ShmArrowStreamWriter, and ingest Arrow IPC streams.
- [ ] [t03-pyo3-clients-and-broker-capsule.md](tasks/t03-pyo3-clients-and-broker-capsule.md) — Expose SyncSQLiteClient, AsyncSQLiteClient, and broker_engine_capsule() (tag 3) in vertebra._sqlite and sync _sqlite.pyi.
- [ ] [t04-freethreaded-and-interrupt-verification.md](tasks/t04-freethreaded-and-interrupt-verification.md) — Add vertebra._sqlite to BrokerEngineDiscovery and verify 3.14t/3.15t GIL safety, <50ms CTE cancellation, and WAL concurrency.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Extension loading (`load_extension`) stays disabled by default; every blocking SQLite FFI call executes off the Python GIL.
- **Fast Chapter Gate (`< 60s`)**: `make sqlite-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py`

[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — ShmArrowStreamWriter and VTBARROW ShmCapsule constructor.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
[^ft-gil-test]: `src/py/tests/unit/test_freethreaded_gil.py` — Free-threaded Python 3.14t/3.15t GIL_USED verification test.
