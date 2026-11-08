---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch15-pool-facades-and-single-sync-pipeline
title: "Chapter 15: Python Pool Facades, Pipeline SQL-Head Guard, and Single-Sync Pipeline Execution"
description: Expose SyncPostgresPool and AsyncPostgresPool over real Rust leases, enforce pre-emission SQL-head transaction/COPY guards, submit cold and warm pipeline batches under one Sync and one flush, and demultiplex ordered result/error/skipped outcomes.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch14-physical-pools-and-query-facades
replan_trigger: null
current_task: "native-postgres-driver:ch15:t01"
plan_revision: 9
plan_commit: b0de4dcb
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen native PostgreSQL pool facade, SQL-head guard, and pipeline outcome contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Single-Sync cold/warm pipeline framing, duplex backpressure, and skipped outcome recovery
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared Tokio completion drainer for async pool acquisition and shutdown
  - id: db-caps-py
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: Public Vertebra pipeline capability contract
tags:
  - postgres
  - pool
  - pipeline
  - protocol
  - pyo3
---

# Chapter 15: Python Pool Facades, Pipeline SQL-Head Guard, and Single-Sync Pipeline Execution

<!-- truth: start -->
- **Native Python Pool Facades (`SyncPostgresPool` / `AsyncPostgresPool`)**: Both pool classes accept the complete connection constructor kwargs plus 8 pool/preparation kwargs, transfer concrete `PooledConnection` leases into `SessionHandle::from_pooled` (recovering leases safely on cancelled `Future`s), and enforce `validate_configuration` without caller keys or network I/O [^pg-contracts] [^py-async-rs].
- **Pre-Emission SQL-Head Pipeline Guard**: Before encoding or flushing any frame of a pipeline batch, a bounded linear UTF-8 SQL-head scanner rejects all unquoted transaction-control aliases (`BEGIN`/`COMMIT`/`END`/`ROLLBACK`/`ABORT`/`SAVEPOINT`/`RELEASE`, `START`/`PREPARE TRANSACTION`, `SET TRANSACTION`/`SET SESSION CHARACTERISTICS` -> `InterfaceError`) and `COPY` (`NotSupportedError`) with `outcomes = ()` and `failing_index` [^pg-contracts].
- **Single-`Sync` Outbound & Ordered Demultiplexing**: Non-empty cold and warm batches encode all `N` extended operations followed by **one** `Sync` and **one** logical socket flush under duplex read-while-writing backpressure, demultiplexing ordered `Result`, `Error`, and synthesized `Skipped { failed_index }` outcomes drained to the trailing `ReadyForQuery` (`I`/`T`/`E`) without fabricated SQLSTATEs [^pg-contracts] [^pg-protocol-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

With Rust `PostgresPool` (`ch14:t01`–`t02`) and typed client query facades (`ch14:t04`) established, Python callers need `SyncPostgresPool` and `AsyncPostgresPool` carrying real `PooledConnection` leases, and the native Rust engine needs true single-`Sync` pipeline batching (which `tokio-postgres` cannot provide because its query methods append `Sync` per statement [^pg-protocol-research]). Chapter 15 delivers `SyncPostgresPool` / `AsyncPostgresPool` (`ch15:t01`), the private native SQL-head guard (`ch15:t02`), single-`Sync` cold/warm outbound pipeline submission with duplex backpressure (`ch15:t03`), and ordered `Result` / `Error` / `Skipped` outcome demultiplexing plus frozen Python pipeline outcome records (`ch15:t04`) [^pg-contracts] [^db-caps-py].

### 1.2 Non-Goals

- Retaining pipeline transaction/savepoint scopes across Python caller result conversion and `SyncPostgresPipeline` / `AsyncPostgresPipeline` context managers (owned by `ch16:t01`–`t02`).
- Protocol `CancelRequest` cancellation and per-command I/O deadlines (owned by `ch16:t03`–`t04` and `ch17:t01`–`t02`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and frozen contracts: [contracts.md](../../contracts.md). Engineering skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md), and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge chapters: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md) [^pg-protocol-research] [^db-caps-py].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R6, R7, R14 (`ch15:t01`)**: Expose `SyncPostgresPool` and `AsyncPostgresPool` (`python_pool.rs`, `lib.rs`, `_postgres.pyi`) with full `contracts.md` connection kwargs + 8 pool kwargs, `acquire()`, `close()`, `validate_configuration(**options) -> None`, cancelled-acquire lease recovery, and real server-PID lease transfer via `SessionHandle::from_pooled` [^pg-contracts] [^py-async-rs].
- **R13, R17 (`ch15:t02`)**: Implement private `pipeline_guard.rs::{validate_pipeline_command, validate_pipeline_commands}` and `PostgresError::PipelineValidation { source, failing_index }`, rejecting transaction-control heads (`InterfaceError`) and `COPY` (`NotSupportedError`) across leading `;`, whitespace, `--`, and nested `/* ... */` comments while permitting `PREPARE name AS ...`, quoted identifiers, string/dollar literals, and identifier-continuation words (`COMMIT_x`, `COMMITé`) [^pg-contracts].
- **R13, R17 (`ch15:t03`)**: Implement `pipeline.rs::{PipelineOperation, PipelineBatch, PipelineLimits}` and `PostgresConnection::submit_pipeline(batch)` validating the whole batch up front (`outcomes = ()`, zero wire bytes on failure), encoding cold all-text and warm binary/mixed `Parse`/`Describe`/`Bind`/`Execute` operations before exactly one `Sync` and one logical socket flush, and servicing reads concurrently while writing large batches [^pg-contracts] [^pg-protocol-research].
- **R17 (`ch15:t04`)**: Implement `pipeline.rs::{PipelineOutcome, PipelineFailure}` and `PostgresConnection::pipeline_results(batch) -> Result<Vec<PipelineOutcome>, PipelineFailure>` demultiplexing parse/bind/describe/execute errors, synthesizing `Skipped { failed_index }` after the first `ErrorResponse`, draining to trailing `ReadyForQuery`, and adding `PostgresError::PipelineAborted` plus frozen `PostgresErrorInfo`, `PostgresPipelineOperation`, and `PostgresPipelineOutcome` in `src/py/vertebra/db/adapters/postgres/types.py` [^pg-contracts].

### 2.2 Concrete Type & Signature Contracts

- **Caller -> Seam -> Collaborator**:
  - Python pool callers -> `SyncPostgresPool` / `AsyncPostgresPool` (`python_pool.rs`) -> `PostgresPool::acquire` -> `SessionHandle::from_pooled` -> `SyncPostgresClient` / `AsyncPostgresClient`.
  - Pipeline callers -> `PostgresConnection::pipeline_results` (`pipeline.rs`) -> `pipeline_guard::validate_pipeline_commands` (`pipeline_guard.rs`) -> single-`Sync` `submit_pipeline` -> ordered `Vec<PipelineOutcome>` / `PipelineFailure` -> `PostgresPipelineOutcome` (`types.py`).
- **Python & Rust Signatures**:
  - `SyncPostgresPool(..., *, pooler_mode: str = "direct", protocol_level_pooler_cache: bool = False, statement_cache_size: int = 256, min_size: int = 1, max_size: int = 10, acquire_timeout: float = 30.0, reset_timeout: float = 10.0, close_timeout: float = 30.0)`
  - `SyncPostgresPool.acquire(self) -> SyncPostgresClient` / `AsyncPostgresPool.acquire(self) -> Awaitable[AsyncPostgresClient]`
  - `SyncPostgresPool.validate_configuration(self, **connection_options: Any) -> None`
  - `pub(crate) fn validate_pipeline_commands<'a>(sqls: impl IntoIterator<Item = &'a str>) -> Result<(), PostgresError>`
  - `pub enum PipelineOutcome { Result(QueryResult), Error(ServerError), Skipped { failed_index: usize } }`
  - `pub struct PipelineFailure { pub first_error: PostgresError, pub failing_index: usize, pub outcomes: Vec<PipelineOutcome>, pub transaction_status: TransactionStatus }`
  - `PostgresConnection::pipeline_results(&mut self, batch: &PipelineBatch) -> Result<Vec<PipelineOutcome>, PipelineFailure>`

### 2.3 Edge-Case Matrix & Alternatives

| Scenario | Input / Wire Condition | Required Behavior |
| --- | --- | --- |
| Cancelled `AsyncPostgresPool.acquire()` | Python `Future` cancelled just as `PostgresPool::acquire` wins a `PooledConnection` | Worker/completion cleanup immediately releases the `PooledConnection` back to the pool; zero leaked permits [^pg-contracts]. |
| Valid `INSERT` followed by `;/*c*/COMMIT` in batch | `[INSERT INTO t VALUES (1), ";/*nested/*c*/*/--line\nEnD WORK"]` | `validate_pipeline_commands` rejects at `failing_index = 1` with `InterfaceError`, `outcomes == ()`, and **zero** `Parse`/`Bind`/`Execute` frames or flushes for `INSERT` [^pg-contracts]. |
| DDL followed by dependent DML in one cold batch | `[CREATE TEMP TABLE p(x int), INSERT INTO p VALUES ($1), SELECT x FROM p]` | Emits zero pre-batch catalog queries; sends all 3 cold text extended sequences + 1 `Sync` + 1 flush and decodes all 3 outcomes in order [^pg-protocol-research]. |
| Middle `Bind` / `Execute` error in 3-op batch | `[SELECT 1, SELECT 1/0, SELECT 3]` | Returns `Err(PipelineFailure)` with `[Result(1), Error("22012"), Skipped { failed_index: 1 }]`, drained through trailing `Sync` `ReadyForQuery` [^pg-protocol-research]. |

- **Alternatives Rejected**: Emitting `Flush` per pipeline statement or `write_all` before reading causes extra syscalls or TCP deadlocks when server `DataRow` buffers fill; fabricating synthetic SQLSTATE codes on skipped operations misstates server execution [^pg-protocol-research].

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/2 files documented in ch15]` (`src/py/vertebra/_postgres.pyi` and `src/py/vertebra/db/adapters/postgres/types.py` docstrings update inline in `ch15:t01` and `ch15:t04`; user/OKF guides consolidate in `ch30`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R6, R7, R14 | [t01](tasks/t01-python-pool-facades.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_pools.py` |
| R13, R17 | [t02](tasks/t02-pipeline-sql-head-guard.md) | `behavior_tdd` | `src/rs/vertebra-postgres/src/pipeline_guard.rs` |
| R13, R17 | [t03](tasks/t03-single-sync-pipeline-outbound.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/pipeline_wire.rs` |
| R17 | [t04](tasks/t04-ordered-pipeline-outcomes.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/pipeline_outcomes.rs`, `src/py/tests/unit/db/test_postgres_types.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch15:t01 — Expose SyncPostgresPool and AsyncPostgresPool carrying real session leases (tasks/t01-python-pool-facades.md)
- [ ] native-postgres-driver:ch15:t02 — Implement native pipeline SQL-head transaction and COPY command guard (tasks/t02-pipeline-sql-head-guard.md)
- [ ] native-postgres-driver:ch15:t03 — Implement single-Sync cold and warm outbound pipeline submission (tasks/t03-single-sync-pipeline-outbound.md)
- [ ] native-postgres-driver:ch15:t04 — Demultiplex ordered pipeline results, errors, and skipped outcomes (tasks/t04-ordered-pipeline-outcomes.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --lib pipeline_guard::tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire --test pipeline_outcomes && make dev && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pools.py src/py/tests/unit/db/test_postgres_types.py -q && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch16-pipeline-scope-and-cancellation` (retained pipeline transaction scopes, `SyncPostgresPipeline` / `AsyncPostgresPipeline`, and protocol cancellation).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pool constructor options, SQL-head guard rules, and pipeline outcome/exception contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Single-`Sync` cold/warm batching, duplex backpressure, and skipped operation demultiplexing.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio completion ring for async PyO3 methods.
[^db-caps-py]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — Public Vertebra pipeline capability protocol.
