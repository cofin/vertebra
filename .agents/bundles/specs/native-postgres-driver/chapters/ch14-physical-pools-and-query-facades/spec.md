---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch14-physical-pools-and-query-facades
title: "Chapter 14: Physical Rust Connection Pools, Reset Draining, and Typed Python Query Facades"
description: Pool real authenticated PostgreSQL sockets with bounded leases, conservative DISCARD ALL reset and draining shutdown, lossless Python value extraction/projection, and typed sync/async query and transaction client facades.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch13-transactions-portals-and-statement-cache
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 0147d80c
state_revision: 0
last_verified_checkpoint: e7c0a42f
last_reviewed_commit: e7c0a42f
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: 0147d80c
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL pool, value record, and query result contracts
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow that deletes vertebra_db::postgres::PostgresPool
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared Tokio database runtime and per-extension AsyncCompletionQueue drainer
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical vertebra._vertebra DatabaseError hierarchy and SQLSTATE attributes
tags:
  - postgres
  - pool
  - pyo3
  - codecs
  - queries
---

# Chapter 14: Physical Rust Connection Pools, Reset Draining, and Typed Python Query Facades

<!-- truth: start -->
- **Physical Socket Leases (`PostgresPool` / `PooledConnection`)**: `vertebra-postgres` pools real authenticated `PostgresConnection` sockets (`vertebra-db` carries no PostgreSQL pool type once `driver-fail-loud` deletes `vertebra_db::postgres::PostgresPool` [^fail-loud-baseline]), enforcing no-I/O construction, lazy `min_size` warmup on first `acquire`, bounded `opening + idle + borrowed <= max_size` reservations, and canonical full-config + preparation-policy comparison without caller-supplied pool keys [^pg-contracts].
- **Conservative Reset & Bounded Drain**: Returning a lease drains bounded responses, rolls back `T`/`E`, closes portals, runs `DISCARD ALL` after `I` (in `direct`/`session` modes), restores configured `session_gucs`, and invalidates `StatementCache`/`TypeCatalog` generations; uncertain or failed resets retire the socket and release its permit once [^pg-contracts].
- **Lossless Python Value Bridge & Query Facades**: `python_values.rs` extracts owned Rust parameters while attached and projects `QueryResult` into frozen `msgspec` adapter records (`PostgresQueryResult`, `PostgresColumn`, `PgInterval`, `PgArray`, `PgComposite`, `PgTemporal`, `PgJson`, `PgBitString`, `PgVector`) outside session locks, powering `SyncPostgresClient` and `AsyncPostgresClient` (`query`, `query_multi`, `begin`/`commit`/`rollback`/savepoints) with canonical `vertebra._vertebra` SQLSTATE exceptions and `.completed_result` diagnostics [^pg-contracts] [^py-async-rs] [^db-errors-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`driver-fail-loud` deletes the SHM-admission `vertebra_db::postgres::PostgresPool`, so `vertebra-db` provides no PostgreSQL pool [^fail-loud-baseline], and `SyncPostgresClient` / `AsyncPostgresClient` from Chapter 07 establish raw physical sessions without typed query execution or pool lease management. Chapter 14 builds the concrete Rust `PostgresPool` and `PooledConnection` (`ch14:t01`), implements rollback/`DISCARD ALL` session reset and deadline-bounded draining shutdown (`ch14:t02`), bridges the complete lossless `PgValue` / `QueryResult` model and canonical SQLSTATE/`AfterCompletion` exception mapper to Python (`ch14:t03`), and exposes typed `query`, `query_multi`, and transaction/savepoint methods on `SyncPostgresClient` and `AsyncPostgresClient` (`ch14:t04`) [^pg-contracts] [^py-async-rs].

### 1.2 Non-Goals

- Python `SyncPostgresPool` / `AsyncPostgresPool` PyO3 classes (owned by `ch15:t01`).
- Single-`Sync` pipeline execution, protocol `CancelRequest`, command deadlines, and Python portal wrappers (owned by Chapters 15, 16, and 17).
- Adapter-owned `%s` / `%(name)s` SQL bind lowering and high-level `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` cutover (owned by Chapters 25–27).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and frozen contracts: [contracts.md](../../contracts.md). Engineering skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge chapters: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) [^py-async-rs] [^db-errors-rs].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R14 (`ch14:t01`)**: Implement `pool.rs::{PostgresPool, PostgresPoolOptions, PooledConnection, PoolStats}` over `PostgresConfig` with no-I/O construction, lazy `min_size` startup on first `acquire`, `opening + idle + borrowed` capacity accounting, independent `acquire_timeout` vs `connect_timeout` enforcement, canonical full-configuration comparison (`validate_configuration`), idle/lifetime/broken eviction, and concrete `rlib` exports without trait-object ABI across extensions [^pg-contracts] [^fail-loud-baseline].
- **R14 (`ch14:t02`)**: Implement `PooledConnection::release`, `Drop` return scheduling, `SessionHandle::from_pooled(PooledConnection)`, conservative reset (`ROLLBACK` -> close portals -> `DISCARD ALL` at `I` -> restore `session_gucs` -> invalidate `StatementCache`/`TypeCatalog` generations), single-permit socket retirement on any uncertain/failed cleanup, and `PostgresPool::close(deadline)` draining [^pg-contracts].
- **R6, R7, R11 (`ch14:t03`)**: Implement `python_values.rs::{extract_parameters, extract_pg_value, pg_value_to_python, query_result_to_python}` and `errors.rs::postgres_error_to_py` (`PyOnceLock`-cached adapter `msgspec.Struct` records and `vertebra._vertebra` exception classes, exact `Decimal` tuple extraction, `PgJson(None)` vs `None`, `PgTemporal` `wire_text` `DataError` refusal on typed bind, and `AfterCompletion.completed_result` attachment) [^pg-contracts] [^db-errors-rs].
- **R6, R7, R12, R13 (`ch14:t04`)**: Extend `SyncPostgresClient` and `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`, `src/py/vertebra/_postgres.pyi`) with `query`, `query_multi`, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, and `rollback_to_savepoint` using detached `block_on_db` and guarded `spawn_rust_db_task_with_attr` completion delivery [^pg-contracts] [^py-async-rs].

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator**:
  - Pool borrowers -> `PostgresPool::acquire` -> `PooledConnection` -> `SessionHandle::from_pooled` -> `PooledConnection::release` / `PostgresPool::close`.
  - Python callers -> `SyncPostgresClient` / `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`) -> `python_values::extract_parameters` (GIL held) -> detached `SessionHandle` execution -> `python_values::query_result_to_python` / `errors::postgres_error_to_py` (on caller/event-loop thread after locks release).
- **Rust & Python Signatures**:
  - `pub struct PostgresPoolOptions { pub pooler_mode: PoolerMode, pub protocol_level_pooler_cache: bool, pub statement_cache_size: usize, pub min_size: usize, pub max_size: usize, pub acquire_timeout: Duration, pub reset_timeout: Duration, pub close_timeout: Duration }`
  - `pub struct PoolStats { pub connecting: usize, pub idle: usize, pub borrowed: usize, pub resetting: usize, pub retired: u64 }`
  - `PostgresPool::new(config: PostgresConfig, options: PostgresPoolOptions) -> Result<Arc<Self>, PostgresError>`
  - `PostgresPool::acquire(self: &Arc<Self>) -> Result<PooledConnection, PostgresError>`
  - `PostgresPool::validate_configuration(&self, candidate_config: &PostgresConfig, candidate_options: &PostgresPoolOptions) -> Result<(), PostgresError>`
  - `PostgresPool::close(&self, deadline: Duration) -> Result<(), PostgresError>`
  - `SyncPostgresClient.query(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *, parameter_oids: Sequence[int] | None = None) -> PostgresQueryResult`
  - `SyncPostgresClient.query_multi(self, sql: str) -> tuple[PostgresQueryResult, ...]`
  - `AsyncPostgresClient.query(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *, parameter_oids: Sequence[int] | None = None) -> Awaitable[PostgresQueryResult]`
  - `AsyncPostgresClient.query_multi(self, sql: str) -> Awaitable[tuple[PostgresQueryResult, ...]]`

### 2.3 Edge-Case Matrix & Alternatives

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| Arbitrary `SELECT` mutating session state | Borrower runs `SELECT set_config('work_mem', '64MB', false)` or `pg_advisory_lock(1)` and returns lease | Never skip reset via SQL-text `SELECT` heuristics; run full `DISCARD ALL` + `session_gucs` restoration before idle republication [^pg-contracts]. |
| Reset failure or EOF during return | `DISCARD ALL` times out or hits EOF | Retire physical socket, decrement capacity once, and never publish the broken lease to waiting borrowers [^pg-contracts]. |
| Hostile Python `decimal.Context(prec=2)` | Extract or project a 38-digit `Decimal("12345678901234567890.123456789012345678")` | Construct via `Decimal.as_tuple()` / exact string spelling without context rounding; all 38 digits and trailing scale survive [^pg-contracts]. |
| Top-level JSON `null` vs SQL `NULL` | `SELECT 'null'::jsonb, NULL::jsonb, '[null]'::jsonb` | Raw `query` / `query_multi` projects `(PgJson(None), None, [None])`; rebinding preserves distinct wire JSON null vs SQL `NULL` [^pg-contracts]. |
| Post-`ReadyForQuery` metadata transport failure | Side-effecting `INSERT ... RETURNING custom_val` succeeds (`ReadyForQuery`), then `TypeCatalog` socket drops | Raise canonical `ConnectionFailure` with `.completed_result` holding original `PostgresQueryResult` (fallback text + affected count); zero SQL replay [^pg-contracts]. |

- **Alternatives Rejected**: Skipping `DISCARD ALL` after `SELECT` queries leaks session GUCs, temp tables, and advisory locks set inside SQL functions; passing `Py<PyAny>` into Tokio tasks forces worker-thread `Python::attach` and risks GIL contention on 3.14t/3.15t [^py-async-rs].

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented in ch14]` (`src/py/vertebra/_postgres.pyi` type stubs and docstrings land in `ch14:t04`; user guide and OKF knowledge chapters consolidate in `ch30:t01`–`t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R14 | [t01](tasks/t01-rust-physical-pool.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/physical_pool.rs` |
| R14 | [t02](tasks/t02-pool-reset-and-drain.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/pool_reset.rs` |
| R6, R7, R11 | [t03](tasks/t03-python-value-extraction-bridge.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/python_value_contract.rs` |
| R6, R7, R12, R13 | [t04](tasks/t04-python-query-and-transaction-facades.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_queries.py` |

## 5. Implementation Plan

- [x] [e7c0a42f] native-postgres-driver:ch14:t01 — Implement concrete Rust physical connection pool and bounded leases (tasks/t01-rust-physical-pool.md)
- [x] [e7c0a42f] native-postgres-driver:ch14:t02 — Implement rollback/DISCARD ALL pool reset and draining shutdown (tasks/t02-pool-reset-and-drain.md)
- [x] [e7c0a42f] native-postgres-driver:ch14:t03 — Bridge lossless native PostgreSQL values and SQLSTATE errors to Python (tasks/t03-python-value-extraction-bridge.md)
- [x] [e7c0a42f] native-postgres-driver:ch14:t04 — Expose typed sync and async query and transaction client facades (tasks/t04-python-query-and-transaction-facades.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test physical_pool --test pool_reset --test python_value_contract && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: 0147d80c`; chapter `state: completed` at `e7c0a42f`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: None (`4/4` completed at `e7c0a42f`).
- **Downstream Dependents**: Unblocks `ch15-pool-facades-and-single-sync-pipeline` (`SyncPostgresPool` / `AsyncPostgresPool` and single-`Sync` pipelines), `ch16`–`ch17` (pipeline contexts, cancellation, deadlines, portals), and `ch18`–`ch26` (COPY, Arrow, and permanent Python drivers).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pool options, `PostgresQueryResult`, lossless `Pg*` value records, and error diagnostics.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed upstream flow that deletes `vertebra_db::postgres::PostgresPool`.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio runtime and independent event-loop completion queue.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `vertebra._vertebra` database exception hierarchy.
