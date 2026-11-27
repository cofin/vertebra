---
type: Spec
flow_id: db-pool-broker-perf
title: SQLite SQLAlchemy Fastpath, Lock-Free Async SHM Broker, and Local asyncpg Parity
description: Eliminate SQLite lazy_begin read-lock serialization and nested pool overhead under SQLAlchemy, replace SyncPool broker contention with per-loop local handoff and single-leader futex waits, and run 1:1 local asyncpg/psycopg PostgreSQL benchmarks.
status: stable
state: active
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-sqlite-sqlalchemy-fastpath
  - ch02-async-shm-broker-contention
  - ch03-asyncpg-parity-and-verification
current_chapter: ch03-asyncpg-parity-and-verification
current_task: t07
parent_flow: null
chapter_id: null
planning_status: complete
depends_on_chapters: []
replan_trigger: null
plan_revision: 2
plan_commit: "16ccb56e"
last_verified_checkpoint: "f1496e28"
last_reviewed_chapter: ch02-async-shm-broker-contention
last_reviewed_commit: "f1496e28"
generated:
  by: flow/0.26.0
  at: "2026-11-27T18:30:00Z"
verified:
  - by: user:cofin
    at: "2026-11-27T18:48:00Z"
    commit: "16ccb56e"
sources:
  - id: src-sqlite-py-client
    resource: src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor
    title: Native SQLite sync and async cursor lazy_begin transaction handling
  - id: src-sqlite-worker
    resource: src/rs/vertebra-sqlite/src/worker.rs::SqlitePool
    title: SQLite WAL connection pool and WorkerSlot execution
  - id: src-sa-sqlite-dialect
    resource: src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::AsyncSQLiteDialect
    title: SQLAlchemy 2.1 SQLite +vtb and +vtb_async dialects
  - id: src-core-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Cross-process shared-memory lease table and futex wakeup
  - id: src-py-broker
    resource: src/rs/vertebra-py/src/broker.rs::LeaseBroker
    title: PyO3 LeaseBroker bindings and async bridge
  - id: src-py-broker-transport
    resource: src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport
    title: Asynchronous SHM broker transport acquire and release paths
  - id: src-bench-drivers
    resource: tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver
    title: Native and stdlib SQLite/PostgreSQL benchmark WorldDriver implementations
  - id: src-bench-targets
    resource: tools/benchmarks/targets.py::ALL_TARGETS
    title: HTTP data and baseline benchmark target definitions
tags:
  - performance
  - sqlite
  - sqlalchemy
  - broker
  - shm
  - postgres
  - benchmarks
---

# SQLite SQLAlchemy Fastpath, Lock-Free Async SHM Broker, and Local asyncpg Parity

<!-- truth: start -->
- Skip implicit `BEGIN IMMEDIATE` for read-only statements in `SyncSQLiteCursor` and `AsyncSQLiteCursor` (`lazy_begin=True`), default `max_readers=0` in `SyncSQLiteDialect` and `AsyncSQLiteDialect` `create_connect_args`, and remove dead `mpsc::channel()` allocations in `SqlitePool` so `sqlite+vtb_async` eliminates cross-worker write-lock serialization and 9× connection fan-out under SQLAlchemy.
- Replace `wake_all()` thundering herds in single-slot `ShmLeaseTable::{release,dispose_quarantined}` with `wake_one_waiter_seqcst()` (using `wake_one()` when `partition_count <= 1`), expose `LeaseBroker::{wake_seq,wait_for_wake_async,partition_count}` over Tokio `spawn_blocking`, and add per-loop local handoff plus single-leader cross-process waiting in `AsyncSHMBrokerTransport` to eliminate `SyncPool` thread contention under `c=128`.
- Align `NativePostgresWorldDriver` multi-query requests to single-checkout connection/lease reuse matching `StdlibPostgresWorldDriver`, add `granian-rsgi-data-stdlib` (`asyncpg`), install `--group postgres-bench`, and capture live 1:1 SQLite and Docker PostgreSQL 17 benchmarks against `asyncpg`, `psycopg`, Granian RSGI/ASGI, and Uvicorn.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Live Suite B (SQLite) and Suite C (PostgreSQL 17) benchmark runs on the 12-core Linux host (`c=128`, 4 workers) exposed two concrete runtime bottlenecks and one comparative measurement gap:

1. **`sqlite+vtb_async` trails `sqlite+aiosqlite` by 2.0×–3.4× under SQLAlchemy concurrency**: `vtb-data-aa-native-4w` reached `2,258 RPS` (`/db`), `120 RPS` (`queries_20`), and `242 RPS` (`updates_5`) versus `granian-asgi-data-aa` (`sqlite+aiosqlite`) at `4,550` / `406` / `409 RPS`. Root causes verified in live code:
   - `SyncSQLiteCursor::run_and_install` and `AsyncSQLiteCursor::spawn_statement` unconditionally emit `BEGIN IMMEDIATE` before every statement when `lazy_begin=True` [^src-sqlite-py-client], acquiring SQLite's exclusive write lock on pure `SELECT` queries across all 4 workers and forcing `ROLLBACK` on pool return.
   - `SyncSQLiteDialect` and `AsyncSQLiteDialect` leave `SQLiteConfig.max_readers` at its standalone default (`8`), creating 9 `rusqlite::Connection`s per SQLAlchemy pool slot (`360` connections across 4 workers × 10 pool slots) even though SQLAlchemy already pools connections and serializes each connection under `_execute_mutex` [^src-sa-sqlite-dialect] [^src-sqlite-worker].
   - `SqlitePool` allocates a synchronous `mpsc::channel()` per statement in `WorkerSlot::send` callers even though `WorkerSlot` executes directly on the calling thread under `self.conn.lock()` [^src-sqlite-worker].
2. **`AsyncSHMBrokerTransport` drops `shm_broker` throughput by ~51% under `c=128`**: `vtb-data-shm-native-4w-broker` hit `15,351 RPS` (`/db`) and `798 RPS` (`queries_20`) versus `31,637` / `2,022 RPS` for `per_worker`. Root causes verified in live code:
   - Every single-slot `ShmLeaseTable::release` and `dispose_quarantined` invokes `wake_waiters_seqcst()` (`hdr.wake_seq.0.wake_all()`), causing a kernel futex thundering herd across all ~80 waiting tasks on every release [^src-core-broker].
   - Every contended `AsyncSHMBrokerTransport.acquire()` submits blocking `self._native.acquire` to Python's `SyncPool.default()` (`ThreadPoolExecutor`, 24 threads) with `contextvars.copy_context()` and Python closure/lock churn [^src-py-broker-transport] [^src-py-broker].
   - `AsyncSHMBrokerTransport` recomputes `stable_hash64("default")` and acquires `self._lock` in `_resolve_partition` even when cross-process `partition_count() == 0` [^src-py-broker-transport].
3. **Missing local 1:1 `asyncpg` / `psycopg` PostgreSQL baseline and multi-query checkout parity**:
   - `.venv` lacks `--group postgres-bench` (`asyncpg`, `psycopg[binary]`), preventing local Suite C runs of `granian-asgi-data-{stdlib,sqlspec,aa}`, `uvicorn-httptools-data-*`, and standalone external Postgres comparators.
   - `tools/benchmarks/targets.py` lacks a `granian-rsgi-data-stdlib` (`asyncpg`) target matching the top TechEmpower Round 23 Python stack [^src-bench-targets].
   - `NativePostgresWorldDriver.fetch_worlds_async` and `update_worlds_async` acquire/release a pool connection (and SHM broker lease) on every query inside `queries_20` / `updates_5`, whereas `StdlibPostgresWorldDriver` (`asyncpg`) holds one checked-out connection per request [^src-bench-drivers].

Objectives:
- Make `vtb-data-aa-native-4w` (`sqlite+vtb_async`) match or beat `granian-asgi-data-aa` (`sqlite+aiosqlite`) on SQLite `/db`, `queries_20`, and `updates_5` with zero errors.
- Eliminate `SyncPool` usage and `wake_all()` thundering herds in contended `AsyncSHMBrokerTransport.acquire()`, substantially closing the `shm_broker` vs `per_worker` gap under `c=128`.
- Capture complete, reproducible local PostgreSQL 17 numbers comparing Vertebra (`native`, `sqlspec`, `advanced_alchemy` × `per_worker`/`shm_broker`) 1:1 against `asyncpg` and `psycopg` (Granian RSGI/ASGI, Uvicorn, and standalone comparators).

### 1.2 Upstream Execution Baseline

- Branch: `perf/runtime-remediation` (base commit `16ccb56e`, after `runtime-perf-remediation` archival).
- Active sibling specs: None (`runtime-perf-remediation` archived into OKF knowledge bundle).
- Baseline measurements from `runtime-perf-remediation`:
  - SQLite 4w HTTP: `vtb-data-aa-native-4w` `2,258` / `120` / `242 RPS` vs `granian-asgi-data-aa` `4,550` / `406` / `409 RPS`.
  - Postgres 17 4w HTTP: `vtb-data-shm-native-4w` `31,637` / `2,022` / `3,680 RPS` vs `vtb-data-shm-native-4w-broker` `15,351` / `798` / `1,577 RPS`.

### 1.3 Code Analysis & Interface Summary

| Area | Files Examined (`path::Symbol`) | Public Interface & Dependency Category | Affected Docs / OKF Chapters |
| :--- | :--- | :--- | :--- |
| SQLite cursor & worker | `src/rs/vertebra-sqlite/src/py_client.rs::{SyncSQLiteCursor,AsyncSQLiteCursor}`, `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool` | In-process PyO3 FFI (`cursor(lazy_begin=...)`, `SqlitePool`) | `domains/data/sqlite-limitations.md`, `domains/extensions/sqlspec-alchemy-and-dialects.md` |
| SQLAlchemy SQLite dialect | `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::{SyncSQLiteDialect,AsyncSQLiteDialect}` | In-process SQLAlchemy 2.1 dialect (`create_connect_args`) | `domains/extensions/sqlspec-alchemy-and-dialects.md` |
| SHM broker core & PyO3 | `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable`, `src/rs/vertebra-py/src/broker.rs::LeaseBroker`, `src/py/vertebra/_vertebra.pyi` | Cross-process SHM ABI (unchanged) + PyO3 FFI (`wake_seq`, `wait_for_wake_async`) | `domains/data/broker-and-shm-leases.md` |
| Async SHM broker transport | `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` | In-process async lease source (`acquire`, `release`, `dispose_quarantined`) | `domains/data/broker-and-shm-leases.md` |
| Postgres benchmark drivers & targets | `tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver`, `tools/benchmarks/targets.py::ALL_TARGETS` | Remote-owned Docker fixture (`postgres:17-alpine`) + benchmark harness | `testing/benchmarks-and-free-threaded.md` |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge chapters: `domains/data/sqlite-limitations.md`, `domains/data/broker-and-shm-leases.md`, `domains/extensions/sqlspec-alchemy-and-dialects.md`, `testing/benchmarks-and-free-threaded.md`.
- Project skills: `runtime-ipc`, `pyo3-bridge`, `python-framework`, `review-performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional & Performance Requirements

- **R1 (SQLite read-only `lazy_begin` alignment)**: Replace naive substring scanning in `is_read_only_sql` with comment/quote-aware whole-token lexical scanning (handling `--`/`/* */` comments, audit column names like `created_at`/`updated_at`/`deleted_at`, and rejecting `REPLACE` and other write keywords only as standalone tokens). When `lazy_begin=True` and `!pool.in_transaction()`, `SyncSQLiteCursor::execute` and `AsyncSQLiteCursor::execute` must skip implicit `BEGIN IMMEDIATE` for read-only statements (`pool.statement_facts(sql).read_only`), matching Python stdlib `sqlite3` / `aiosqlite` semantics. Write/DDL statements and non-empty `executemany` continue opening `BEGIN IMMEDIATE` transactions.
- **R2 (SQLAlchemy SQLite single-connection pool slots)**: `SyncSQLiteDialect` and `AsyncSQLiteDialect` must override `create_connect_args` to extract any case-insensitive `max_readers` entry from `url.query`, strip it via `url.difference_update_query([...])`, convert it to `int` in `kwargs["max_readers"]`, and otherwise inject `kwargs["max_readers"] = 0` (so SQLAlchemy `connect_args` still overrides afterward).
- **R3 (Direct `SqlitePool` execution without dead channels)**: `SqlitePool` (`src/rs/vertebra-sqlite/src/worker.rs`) must execute `WorkerSlot` operations directly under `self.conn.lock()` without per-statement `mpsc::channel()` heap allocations, preserving cancellation (`op_id` + `InterruptState`) and `last_rowid` behavior.
- **R4 (Zero-ABI-break single-waiter SHM wakeup & async wake primitive)**: `ShmLeaseTable` must add `wake_one_waiter_seqcst()` calling `wake_one()` when `partition_count <= 1` and falling back to `wake_all()` when `partition_count > 1`, used by single-slot `release()` and `dispose_quarantined()`. `ShmLeaseTable` must expose `wake_seq() -> u32`, `partition_count() -> u32`, and `wait_for_wake(expected_seq, timeout) -> u32`, bridged to Python as `LeaseBroker.wake_seq() -> int`, `LeaseBroker.partition_count() -> int`, and `LeaseBroker.wait_for_wake_async(expected_seq: int, timeout_ms: int = 25) -> Awaitable[int]` via `crate::async_bridge::spawn_rust_db_task`.
- **R5 (Lock-free local handoff & single-leader async broker waiting)**: `AsyncSHMBrokerTransport` must maintain per-event-loop `collections.deque` waiter queues keyed by partition (`int | None`) so `_release_impl` and `_dispose_quarantined_impl` (using the exact `int | None` partition returned by `_record_pool_release` / `_record_pool_dispose`) wake one local waiter directly on the event loop without touching `SyncPool.default()`. At most one leader task per `(loop, partition)` awaits `self._native.wait_for_wake_async(...)` for cross-process releases. Cache `"default"` bind hash and skip `self._lock` / `lookup_partition` in `_resolve_partition` only when cross-process `self._native.partition_count() == 0`.
- **R6 (1:1 PostgreSQL multi-query checkout parity & `granian-rsgi-data-stdlib`)**: `NativePostgresWorldDriver` must check out one physical client via `driver.pool.acquire()` (and one SHM broker lease when `shm_broker` is active, without double-acquiring) per `fetch_worlds_async` / `update_worlds_async` call, closing via `await client.close()` under `CancelScope(shield=True)` and sorting `row_id` updates inside the transaction in both `NativePostgresWorldDriver` and `StdlibPostgresWorldDriver` to avoid `40P01` deadlocks under `c=128`. Register `granian-rsgi-data-stdlib` (port `8170`) in `tools/benchmarks/targets.py`.
- **R7 (Live `postgres-bench` & SQLite verification gate)**: Install `--group postgres-bench` (`asyncpg`, `psycopg[binary]`), run release `make build`, and record live Suite B (SQLite) and Suite C (Docker PostgreSQL 17 HTTP + standalone external comparators) benchmarks plus updated OKF knowledge chapters.

### 2.2 Shared Concrete Contracts

```rust
// src/rs/vertebra-core/src/broker.rs
impl ShmLeaseTable {
    pub fn wake_seq(&self) -> u32;
    pub fn partition_count(&self) -> u32;
    pub fn wait_for_wake(&self, expected_seq: u32, timeout: std::time::Duration) -> u32;
}

// src/rs/vertebra-py/src/broker.rs
#[pymethods]
impl LeaseBroker {
    pub fn wake_seq(&self) -> u32;
    pub fn partition_count(&self) -> u32;
    pub fn record_timeout(&self);

    #[pyo3(signature = (expected_seq, timeout_ms=25))]
    pub fn wait_for_wake_async(
        &self,
        py: Python<'_>,
        expected_seq: u32,
        timeout_ms: u64,
    ) -> PyResult<crate::async_bridge::TaskAwaitable>;
}
```

```python
# src/py/vertebra/_vertebra.pyi
class LeaseBroker:
    def wake_seq(self) -> int: ...
    def partition_count(self) -> int: ...
    def wait_for_wake_async(self, expected_seq: int, timeout_ms: int = 25) -> Awaitable[int]: ...

# src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py
class _SQLiteDialectMixin:
    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]: ...
```

### 2.3 Caller -> Interface -> Collaborator Map

- `AdvancedAlchemyWorldDriver` -> `AsyncSQLiteDialect.create_connect_args` (`max_readers=0`) -> `AsyncDBAPIConnection` -> `AsyncSQLiteCursor::execute` (skips `BEGIN IMMEDIATE` on `read_only` SQL) -> `SqlitePool::run_with_op_id` (direct `WorkerSlot` lock, no `mpsc::channel`).
- `AsyncPostgreSQLDriver` / `AsyncBrokerPool` -> `AsyncSHMBrokerTransport.acquire` -> `LeaseBroker.try_acquire[_in]` fastpath -> per-loop waiter deque + single-leader `LeaseBroker.wait_for_wake_async` -> `ShmLeaseTable::{wait_for_wake, wake_one_waiter_seqcst}`.
- `tools.benchmarks.cli` -> `NativePostgresWorldDriver` / `StdlibPostgresWorldDriver` (`asyncpg`) / `granian-rsgi-data-stdlib` -> Docker `postgres:17-alpine` + `tools.benchmarks.adapters.sql`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| Read-then-write SQLite session (`SELECT` then `UPDATE` under `lazy_begin=True`) | `SELECT` runs without `BEGIN IMMEDIATE`; subsequent `UPDATE` emits `BEGIN IMMEDIATE`, honoring `busy_timeout` without WAL snapshot-upgrade `SQLITE_BUSY`. |
| Explicit `driver.begin()` followed by `SELECT` on `lazy_begin=True` cursor | `pool.in_transaction()` is already `True`; `SELECT` routes to `pool.writer` inside the active transaction snapshot unchanged. |
| User explicitly passes `max_readers=4` in SQLite URL query or `connect_args` | `create_connect_args` extracts and strips `max_readers` from `url.query` as an `int` (or injects `0` when absent), so URL overrides work and SQLAlchemy `connect_args` still overrides afterward. |
| Multiple broker partitions (`partition_count > 1`) during single-slot release | `wake_one_waiter_seqcst()` falls back to `wake_all()` when `partition_count > 1`, preventing cross-partition waiter starvation while preserving SHM ABI layout. |
| Leader waiter cancelled or timed out while follower coroutines remain queued | Leader `finally` block promotes the next non-done follower in the deque to become leader or immediately triggers retry so no waiter ever hangs. |
| Cross-thread or cross-loop release wakes waiting loop in `AsyncSHMBrokerTransport` | `_notify_local_waiters` dispatches via `loop.call_soon_threadsafe` when called from a different thread or loop, and schedules directly on the active loop otherwise. |
| Cross-process partition registration after worker transport initialization | `_resolve_partition` checks cross-process `self._native.partition_count() == 0` instead of a local boolean, so partitions registered later by another worker are always discovered. |
| `asyncpg` or `psycopg` absent in minimal CI environment | Existing `import_optional` guards keep offline unit tests green without `--group postgres-bench`. |

### 2.5 Acceptance Scenarios

1. `vtb-data-aa-native-4w` (`sqlite+vtb_async`) achieves `>= 4,500 RPS` on `/db` (matching or beating `granian-asgi-data-aa`) and improves `queries_20` and `updates_5` by `>= 2×` over baseline (`120` / `242 RPS`) with `0` errors.
2. `AsyncSHMBrokerTransport.acquire()` under `c=128` concurrency over a 10-slot pool completes with zero `SyncPool.default().submit_async` calls and zero timeouts/leaks across single-process and multi-process contention tests.
3. `vtb-data-shm-native-4w-broker` closes the `per_worker` gap significantly on Docker PostgreSQL 17 `/db` and `queries_20` with `0` errors.
4. Full local PostgreSQL 17 benchmark matrix (`granian-rsgi-data-stdlib`, `granian-asgi-data-{stdlib,sqlspec,aa}`, `uvicorn-httptools-data-*`, Vertebra `per_worker` & `shm_broker`, plus standalone `--include-external` SQL/SQLSpec/Advanced Alchemy comparators) succeeds with `0` errors and produces a 1:1 comparison table.
5. `make lint-all`, `make rs-test`, `mypy.stubtest`, and targeted Python unit suites pass with zero regressions.

### 2.6 Alternatives Considered

- **Emit `BEGIN DEFERRED` on SQLite `SELECT` under `lazy_begin=True`**: Rejected because SQLite WAL immediately returns `SQLITE_BUSY` (bypassing `busy_timeout`) when a deferred read transaction upgrades to a write lock after a concurrent commit, which breaks concurrent `/updates` sessions.
- **Add per-partition `WaitWord` fields to `ShmPartitionEntry` in SHM**: Rejected to avoid breaking the fixed 64-byte `ShmPartitionEntry` and `ShmLeaseHeader` ABI (`size_of == 4544` / `4992`); checking `partition_count <= 1` delivers exact `wake_one()` semantics for unpartitioned and single-partition `AsyncBrokerPool` workloads with zero ABI change.
- **Keep `SyncPool.default()` with larger thread pool size for `AsyncSHMBrokerTransport`**: Rejected because spawning 64–128 OS threads per worker still pays `contextvars.copy_context()`, GIL contention, and futex thundering herds on every contended acquire.

## 3. Documentation & OKF Knowledge Plan

- `[0/4 files documented]`
  - `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` (`ch03:t09`) — Document read-only `lazy_begin` skip semantics for `SyncSQLiteCursor` and `AsyncSQLiteCursor`.
  - `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` (`ch03:t09`) — Document SQLite `lazy_begin` read-only skip semantics and SQLAlchemy SQLite dialect `max_readers=0` default.
  - `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md` (`ch03:t09`) — Document single-waiter `wake_one_waiter_seqcst()`, `LeaseBroker::{wake_seq,wait_for_wake_async,partition_count}`, and `AsyncSHMBrokerTransport` per-loop local handoff + single-leader watcher.
  - `.agents/bundles/knowledge/testing/benchmarks-and-free-threaded.md` (`ch03:t09`) — Document `granian-rsgi-data-stdlib` target, `NativePostgresWorldDriver` single-checkout parity, and `--group postgres-bench` 1:1 `asyncpg`/`psycopg` execution.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Chapter | Verification Strategy |
| :--- | :--- | :--- |
| R1, R2, R3 | `ch01-sqlite-sqlalchemy-fastpath` | `behavior_tdd`, `regression_tdd` |
| R4, R5 | `ch02-async-shm-broker-contention` | `behavior_tdd`, `regression_tdd` |
| R6, R7 | `ch03-asyncpg-parity-and-verification` | `integration_acceptance`, `documentation_validation` |

## 5. Implementation Plan

- [x] [b14aa2c3] ch01-sqlite-sqlalchemy-fastpath — Skip `BEGIN IMMEDIATE` on read-only SQLite `lazy_begin`, default SQLAlchemy SQLite `max_readers=0`, and remove dead `mpsc::channel()` allocations in `SqlitePool` (chapters/ch01-sqlite-sqlalchemy-fastpath/spec.md)
- [x] [f1496e28] ch02-async-shm-broker-contention — Single-waiter SHM futex wake, `LeaseBroker.wait_for_wake_async`, and per-loop local handoff + single-leader watcher in `AsyncSHMBrokerTransport` (chapters/ch02-async-shm-broker-contention/spec.md)
- [~] ch03-asyncpg-parity-and-verification — `NativePostgresWorldDriver` checkout parity, `granian-rsgi-data-stdlib` target, live `--group postgres-bench` Suite B/C verification, and OKF doc sync (chapters/ch03-asyncpg-parity-and-verification/spec.md)

### Chapter / Phase Recovery Checkpoint

Each chapter runs a `< 60s` targeted Rust + Python lint and unit test gate (`make dev-ext EXT=<ext>`, targeted `cargo test`, targeted `uv run --no-sync pytest`, `mypy.stubtest`, and `ruff`). Full `make build`, live SQLite/PostgreSQL benchmark runs, and `make lint-all` run in `ch03-asyncpg-parity-and-verification`.

## 6. Current State & Remaining Plan Snapshot

- **Flow State**: `active` (`ch01` and `ch02` completed and approved through `f1496e28`; executing `ch03-asyncpg-parity-and-verification`).
- **Remaining Chapters**: `ch03-asyncpg-parity-and-verification`.

[^src-sqlite-py-client]: `src/rs/vertebra-sqlite/src/py_client.rs::AsyncSQLiteCursor` — Native SQLite sync and async cursor lazy_begin transaction handling.
[^src-sqlite-worker]: `src/rs/vertebra-sqlite/src/worker.rs::SqlitePool` — SQLite WAL connection pool and WorkerSlot execution.
[^src-sa-sqlite-dialect]: `src/py/vertebra/extensions/sqlalchemy/dialects/sqlite.py::AsyncSQLiteDialect` — SQLAlchemy 2.1 SQLite +vtb and +vtb_async dialects.
[^src-core-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Cross-process shared-memory lease table and futex wakeup.
[^src-py-broker]: `src/rs/vertebra-py/src/broker.rs::LeaseBroker` — PyO3 LeaseBroker bindings and async bridge.
[^src-py-broker-transport]: `src/py/vertebra/extensions/broker/transport.py::AsyncSHMBrokerTransport` — Asynchronous SHM broker transport acquire and release paths.
[^src-bench-drivers]: `tools/benchmarks/apps/_db_drivers.py::NativePostgresWorldDriver` — Native and stdlib SQLite/PostgreSQL benchmark WorldDriver implementations.
[^src-bench-targets]: `tools/benchmarks/targets.py::ALL_TARGETS` — HTTP data and baseline benchmark target definitions.
