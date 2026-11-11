---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch16-pipeline-scope-and-cancellation
title: "Chapter 16: Transactional Pipeline Contexts, Protocol Cancellation, and Session Retirement"
description: Retain transaction or savepoint scopes across Python pipeline result conversion, expose SyncPostgresPipeline and AsyncPostgresPipeline, and implement generation-fenced protocol CancelRequest cancellation with mandatory connection retirement.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch15-pool-facades-and-single-sync-pipeline
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 0147d80c
state_revision: 1
last_verified_checkpoint: e8b397b4
last_reviewed_commit: e8b397b4
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
    title: Frozen native PostgreSQL pipeline transaction scope, capability context, and cancellation retirement contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL protocol 3.0/3.2 CancelRequest semantics, PgBouncer cancel forwarding, and delayed-cancel retirement
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared PyO3 Tokio completion bridge and future cancellation callback registration
  - id: pg-conn-rs
    resource: src/rs/vertebra-db/src/connection.rs::CancelHandle
    title: Shared Rust CancelHandle and Connection cancellation trait
tags:
  - postgres
  - pipeline
  - cancellation
  - transactions
  - pyo3
---

# Chapter 16: Transactional Pipeline Contexts, Protocol Cancellation, and Session Retirement

<!-- truth: start -->
- **Retained Pipeline Transaction Scope (`SessionAccess::Pipeline`)**: `SessionHandle::{pipeline_begin, pipeline_results, pipeline_finish}` opens `BEGIN` at `I` or a savepoint at `T`, blocks every ordinary raw session method while active (even for the same owner), persists failure across caught preflight/server errors so `finish(true)` still rolls back, and commits/releases only when caller result conversion exits cleanly [^pg-contracts].
- **Enqueue-Snapshot Python Pipeline Contexts**: `SyncPostgresPipeline` and `AsyncPostgresPipeline` snapshot nested mutable parameters at `execute`/`select` time, project DML counts (`int`) and `SELECT` rows (`list[dict[str, Any]]`), raise the first canonical `DatabaseError` / `InterfaceError` / `NotSupportedError` with `.outcomes` and `.failing_index`, and roll back before invoking managed lease release on any body or conversion error [^pg-contracts].
- **Generation-Fenced `CancelRequest` & Mandatory Retirement**: `PostgresCancelHandle` sends `CancelRequest` over a fresh endpoint/TLS/cloud transport without `StartupMessage` or passwords (Protocol 3.0 `32`-bit PID/secret across PG14–18, with owned secret storage extensible to Protocol 3.2 `256`-bit variable-length keys without Python exposure), marks every cancel-sent physical socket `retire_on_finish` so delayed cancels never hit a next operation/borrower, and routes `asyncio` `Future` cancellation via `spawn_rust_db_task_with_attr_and_cancel` without aborting native drain/retirement [^pg-contracts] [^pg-protocol-research] [^py-async-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 15 (`ch15-pool-facades-and-single-sync-pipeline`) submits single-`Sync` pipeline batches and demultiplexes ordered outcomes, and `vertebra-db` (`py_async.rs`) currently aborts Tokio worker tasks immediately when a Python `asyncio.Future` is cancelled [^py-async-rs]. Two critical safety gaps remain: (1) pipeline contexts must hold an explicit transaction or savepoint open across caller-side Python row/cardinality conversion (`zip(strict=True)`, `SQLResult` hydration) while preventing same-owner raw `commit`/`query` escapes and caught-guard commits; and (2) out-of-band `client.cancel()` and `asyncio` cancellation must send a protocol `CancelRequest` on a separate connection, drain the original stream boundedly without aborting the cleanup worker, and retire any physical connection for which `CancelRequest` may have been sent because PostgreSQL cancellation has no wire acknowledgement [^pg-contracts] [^pg-protocol-research]. Chapter 16 closes both gaps across `ch16:t01`–`t04`.

### 1.2 Non-Goals

- Per-command client I/O deadlines (`timeout=`), Python portal wrappers, and `jiff` timezone text fallback (owned by Chapter 17).
- High-level `Driver.pipeline()` / `Driver.cancel()` active-operation registry integration on `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` (owned by Chapters 25 and 26).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and frozen contracts: [contracts.md](../../contracts.md). Engineering skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge chapters: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) [^py-async-rs] [^pg-conn-rs].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R13, R17 (`ch16:t01`)**: Implement `crate::session::pipeline_scope::PipelineScopeToken` (`#[path = "pipeline_scope.rs"] pub mod pipeline_scope;` in `session.rs`, never duplicated in `lib.rs`), `SessionHandle::{pipeline_begin, pipeline_results, pipeline_finish}` with central `SessionAccess::{Ordinary, Pipeline(token), Cancel, ForcedCleanup}` gating (including `with_connection` / `acquire_operation`) and persistent failed-scope state, plus pure-Python `pipeline.py::{PreparePostgresQuery, SyncPostgresPipeline, AsyncPostgresPipeline}` with enqueue-time deep parameter snapshots and persistent failure tracking [^pg-contracts].
- **R13, R17 (`ch16:t02`)**: Expose `pipeline_begin() -> int`, `pipeline_results(scope_id, operations) -> tuple[PostgresPipelineOutcome, ...]`, and `pipeline_finish(scope_id, success) -> None` (and async `Awaitable` counterparts) on `SyncPostgresClient` and `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`, `_postgres.pyi`), attaching `.outcomes` and `.failing_index` to canonical exceptions [^pg-contracts].
- **R7, R18 (`ch16:t03`)**: Implement `crate::session::cancel::{PostgresCancelHandle, OperationGeneration, CancelDisposition}` (`#[path = "cancel.rs"] pub mod cancel;` in `session.rs`, never duplicated in `lib.rs`) and `Connection::cancel_handle` (`session.rs`) sending fresh-socket `CancelRequest` (Protocol 3.0 `32`-bit PID/secret via `frontend::cancel_request(pid, key)`, keeping variable-length / `256`-bit Protocol 3.2 secret framing Rust-private) with mandatory `retire_on_finish` and bounded drain, plus `py_async.rs::spawn_rust_db_task_with_attr_and_cancel` [^pg-contracts] [^pg-protocol-research] [^py-async-rs] [^pg-conn-rs].
- **R7, R18 (`ch16:t04`)**: Expose out-of-band `SyncPostgresClient.cancel() -> None` and `AsyncPostgresClient.cancel() -> Awaitable[None]`, route `AsyncPostgresClient` operations through `spawn_rust_db_task_with_attr_and_cancel` preserving `asyncio.CancelledError` while native drain/retirement completes, and update `_postgres.pyi` [^pg-contracts] [^py-async-rs].

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator**:
  - Python `with SyncPostgresPipeline(client)` / `async with AsyncPostgresPipeline(client)` (`pipeline.py`) -> `client.{pipeline_begin, pipeline_results, pipeline_finish}` (`sync_client.rs`, `async_client.rs`) -> `SessionHandle` `SessionAccess::Pipeline(token)` (`pipeline_scope.rs`, `session.rs`).
  - Caller `client.cancel()` or `Future.cancel()` -> `spawn_rust_db_task_with_attr_and_cancel` (`py_async.rs`) -> `PostgresCancelHandle` (`cancel.rs`) -> fresh endpoint/TLS/cloud `CancelRequest` + original reader drain & `retire_on_finish`.
- **Rust & Python Signatures**:
  - `pub struct PipelineScopeToken(u64)`
  - `SessionHandle::pipeline_begin(&self) -> Result<PipelineScopeToken, PostgresError>`
  - `SessionHandle::pipeline_results(&self, token: PipelineScopeToken, batch: PipelineBatch) -> Result<Vec<PipelineOutcome>, PostgresError>`
  - `SessionHandle::pipeline_finish(&self, token: PipelineScopeToken, success: bool) -> Result<(), PostgresError>`
  - `SyncPostgresPipeline(client: SyncPostgresClient, *, prepare: PreparePostgresQuery | None = None, release: Callable[[], None] | None = None)`
  - `AsyncPostgresPipeline(client: AsyncPostgresClient, *, prepare: PreparePostgresQuery | None = None, release: Callable[[], Awaitable[None]] | None = None)`
  - `pub struct OperationGeneration(u64)`
  - `pub enum CancelDisposition { IdleNoOp, SentAndDrained, SentAndRetiredOnError, CancelledBeforeSend }`
  - `pub fn spawn_rust_db_task_with_attr_and_cancel<F, T, E, MapOk, MapErr, OnCancel>(...) -> PyResult<Bound<'_, RustDbTaskAwaitable>>`
  - `SyncPostgresClient.cancel(self) -> None` / `AsyncPostgresClient.cancel(self) -> Awaitable[None]`

### 2.3 Edge-Case Matrix & Alternatives

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| Caught guard / server error inside `with` block | Queue `[INSERT, COMMIT]`, catch `InterfaceError` inside `with SyncPostgresPipeline(client):` and exit cleanly | Persistent Python + native failed flags force `pipeline_finish(token, False)` (`ROLLBACK`); zero inserted rows survive [^pg-contracts]. |
| Same-owner raw escape mid-pipeline | Inside `with SyncPostgresPipeline(client):`, caller invokes `client.commit()` or `client.query("SELECT 1")` before or after `results()` | Central `SessionAccess::Pipeline(token)` gate rejects with `InterfaceError` before touching the wire [^pg-contracts]. |
| Delayed `CancelRequest` losing race to query completion | Query finishes just before `CancelRequest` lands on server; second borrower acquires from `max_size=1` pool | `CancelRequest`-sent session is marked `retire_on_finish` and closed; second borrower dials a fresh backend PID and never receives `57014` [^pg-protocol-research]. |
| `asyncio.wait_for` cancels `AsyncPostgresClient.query` | Caller awaits `wait_for(client.query("SELECT pg_sleep(10)"), 0.05)` and drops `Future` | Caller receives `asyncio.CancelledError`; custom cancel callback triggers `CancelRequest` while Tokio cleanup worker stays alive to drain/retire and return the pool permit [^py-async-rs]. |

- **Alternatives Rejected**: Aborting the Tokio query worker via `AbortHandle::abort()` on `Future.cancel()` drops the socket mid-query without sending `CancelRequest` (leaving `pg_sleep`/locks running on PostgreSQL) or strands pool permits; reusing a connection after sending `CancelRequest` exposes the next query to a delayed asynchronous `SIGINT` on the backend [^pg-protocol-research].

## 3. Documentation & OKF Knowledge Plan

Progress: `[2/2 files documented in ch16]` (`src/py/vertebra/db/adapters/postgres/pipeline.py` and `src/py/vertebra/_postgres.pyi` docstrings land in `ch16:t01`, `ch16:t02`, and `ch16:t04`; user/OKF guides consolidate in `ch30`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R13, R17 | [t01](tasks/t01-pipeline-transaction-scope.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/pipeline_scope.rs`, `src/py/tests/unit/db/test_postgres_native_pipeline.py` |
| R13, R17 | [t02](tasks/t02-python-pipeline-contexts.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_pipeline.py` |
| R7, R18 | [t03](tasks/t03-cancel-engine-and-retirement.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/cancellation_protocol.rs`, `src/rs/vertebra-db/src/py_async.rs` |
| R7, R18 | [t04](tasks/t04-python-and-asyncio-cancellation.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_cancel.py` |

## 5. Implementation Plan

- [x] [e8b397b4] native-postgres-driver:ch16:t01 — Implement retained pipeline transaction scope and Python pipeline context classes (tasks/t01-pipeline-transaction-scope.md)
- [x] [e8b397b4] native-postgres-driver:ch16:t02 — Wire PyO3 client pipeline scope methods and end-to-end transactional pipeline tests (tasks/t02-python-pipeline-contexts.md)
- [x] [e8b397b4] native-postgres-driver:ch16:t03 — Implement protocol CancelRequest engine, connection retirement, and non-aborting async cancel callback (tasks/t03-cancel-engine-and-retirement.md)
- [x] [e8b397b4] native-postgres-driver:ch16:t04 — Expose explicit and asyncio client cancellation with independent native cleanup (tasks/t04-python-and-asyncio-cancellation.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features pyo3 --lib py_async::tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_scope --test cancellation_protocol && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py src/py/tests/unit/db/test_postgres_native_cancel.py -q && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: 0147d80c`; chapter `state: completed` at `e8b397b4`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: None (`4/4` completed at `e8b397b4`).
- **Downstream Dependents**: Unblocks `ch17-command-deadlines-portals-and-tz-fallback` (command I/O deadlines reusing `OperationGeneration` + `PostgresCancelHandle`, Python portals, and `jiff` timezone fallback) and `ch26:t02` (permanent driver `.pipeline()` factories).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pipeline transaction scope, `SyncPostgresPipeline` / `AsyncPostgresPipeline`, and cancellation retirement contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Protocol 3.0/3.2 `CancelRequest` framing, PgBouncer forwarding, and delayed-cancel race analysis.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared PyO3 Tokio completion bridge and task cancellation hook.
[^pg-conn-rs]: `src/rs/vertebra-db/src/connection.rs::CancelHandle` — Shared Rust `CancelHandle` and `Connection` traits.
