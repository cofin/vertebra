---
type: Task
id: "native-postgres-driver:ch15:t03"
chapter_id: ch15-pool-facades-and-single-sync-pipeline
title: "Implement single-Sync cold and warm outbound pipeline submission"
description: "Encode and flush N extended pipeline operations in one single-Sync batch with whole-batch SQL-head preflight and duplex read-while-writing backpressure."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch15:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/pipeline_guard.rs
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/session.rs
files:
  - src/rs/vertebra-postgres/src/pipeline.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/pipeline_wire.rs
estimated_loc: 245
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen single-Sync pipeline submission, cold text discovery, and whole-batch guard contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Single-Sync wire batching, logical flush counting, and duplex backpressure deadlock avoidance
tags:
  - postgres
  - pipeline
  - wire
  - backpressure
  - rust
---

# Task t03: Implement Single-`Sync` Cold and Warm Outbound Pipeline Submission

## Objective & Scope Boundary

Implement `PipelineOperation`, `PipelineBatch`, `PipelineLimits`, and `PostgresConnection::submit_pipeline(batch)` in `vertebra-postgres` so every non-empty batch validates all SQL heads before emitting a single byte, encodes all `N` cold or warm extended operations before **one** trailing `Sync` and **one** logical `AsyncWrite::poll_flush`, and services inbound backend frames concurrently under write backpressure [^pg-contracts] [^pg-protocol-research]. Full `Result` / `Error` / `Skipped` outcome demultiplexing lands in `ch15:t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 15 single-`Sync` outbound framing and duplex backpressure invariants.
- [contracts.md](../../../contracts.md) — Whole-batch SQL-head preflight (`outcomes = ()`, zero wire bytes on failure) and cold/warm pipeline format rules [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Verified cold single-`Sync` prototype (`Parse`+`DescribeS`+`Bind`-text+`DescribeP`+`Execute` x `N` + `Sync`), independent `Sync` vs `poll_flush` counters, and duplex deadlock prevention [^pg-protocol-research].
- `src/rs/vertebra-postgres/src/pipeline_guard.rs` — `validate_pipeline_commands` from `ch15:t02`.
- `src/rs/vertebra-postgres/src/extended.rs` — `Parameter`, `encode_bind`, and parameter/result format selection.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection` transport split/duplex pump, `StatementCache`, and `TypeCatalog` snapshot.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/pipeline.rs` (exported via `src/rs/vertebra-postgres/src/lib.rs`):
  - `pub struct PipelineLimits { pub max_operations: usize, pub max_sql_bytes: usize, pub max_parameter_bytes: usize, pub max_result_bytes: usize }` (with explicit sensible defaults, e.g. `max_operations = 4096`, `max_result_bytes = 268_435_456`).
  - `pub struct PipelineOperation { pub sql: String, pub parameters: Vec<Parameter>, pub parameter_oids: Option<Vec<u32>> }`
  - `pub struct PipelineBatch { pub operations: Vec<PipelineOperation>, pub limits: PipelineLimits }`
  - `pub struct SubmittedPipeline` (outbound plan + captured inbound raw frame buffer / duplex exchange handle consumed by `ch15:t04` `pipeline_results`).
- `src/rs/vertebra-postgres/src/session.rs`:
  - `PostgresConnection::submit_pipeline(&mut self, batch: &PipelineBatch) -> impl Future<Output = Result<SubmittedPipeline, PostgresError>> + Send`:
    1. **Empty batch**: when `batch.operations.is_empty()`, emit zero frames and zero socket flushes, returning an empty `SubmittedPipeline`.
    2. **Whole-batch preflight before ANY wire byte**: validate `batch.limits` (`max_operations`, total SQL bytes, parameter counts/OIDs, `TemporalText` `DataError` refusal) AND invoke `pipeline_guard::validate_pipeline_commands(batch.operations.iter().map(|op| op.sql.as_str()))` across all `0..N` operations in order BEFORE encoding or writing operation `0`. If operation `k` (`0 <= k < N`) is `COMMIT`, `END`, `ROLLBACK`, `BEGIN`, `SAVEPOINT`, `PREPARE TRANSACTION`, `SET TRANSACTION`, or `COPY`, return `Err(PostgresError::PipelineValidation { source, failing_index: k })` (`outcomes = ()`) with **zero** `Parse`/`Bind`/`Execute` frames and **zero** socket flushes.
    3. **Snapshot & encode all `N` ops + 1 `Sync`**: snapshot `TypeCatalog` and `StatementCache` before send (never issuing catalog `Query` frames before or inside the batch, even for DDL-dependent operations).
       - Cold / uncached op: encode `Parse` + `DescribeStatement` + `Bind`(text parameters, all-text result format `[0]`) + `DescribePortal` + `Execute(max_rows = 0)`.
       - Warm cached op: encode `Bind`(cached statement name, supported binary/text parameter formats, per-column binary/text result formats) + `DescribePortal` + `Execute(max_rows = 0)` (or `Parse` + `Bind` when cache capacity is `0`).
       - Append **zero** per-operation `Flush` frames and **exactly one** trailing `frontend::sync(&mut buf)` at the end of the entire batch.
    4. **Duplex write + concurrent read**: write bounded outbound chunks while concurrently draining inbound backend frames into bounded memory (`limits.max_result_bytes`) via `tokio::select!` so large batches with large `DataRow` responses never deadlock when kernel TCP send/recv buffers saturate, finishing outbound transmission with **exactly one** logical `AsyncWrite::poll_flush` completion. Partial-send or framing errors mark the connection broken/retired immediately.

## Edge Cases & Error Semantics

- **DDL followed by dependent `INSERT`/`SELECT` in one batch**: Because cold ops use `Parse` + `DescribeStatement` + text `Bind` + `DescribePortal` + `Execute` inside the single-`Sync` stream with zero pre-batch round trip, operation `0` (`CREATE TEMP TABLE t(x int)`) executes on the server before operation `1` (`INSERT INTO t VALUES ($1)`) is parsed/bound by PostgreSQL [^pg-protocol-research].
- **Duplicate SQL in same batch**: Repeated cold statements in the same single-`Sync` batch use unnamed `""` (or distinct prepared names) cleanly without colliding before `ParseComplete` arrives.
- **Clean pre-send rejection vs partial-send retirement**: Guard/limit/parameter validation failures happen before writing bytes, leaving the connection clean; transport/limit failures after bytes enter the stream retire the physical connection.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/pipeline_wire.rs` wrapping the duplex stream in an instrumentation layer that counts exact `Parse`, `Describe`, `Bind`, `Execute`, `Flush`, and `Sync` wire frames independently from `AsyncWrite::poll_flush` completions.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Empty batch (`0` ops) | `submit_pipeline(PipelineBatch { operations: vec![], .. })` | `0` wire bytes, `0` `Sync` frames, `0` `poll_flush` calls. |
| Cold & warm `N`-op single-`Sync` batch | 3 cold ops (including `CREATE TEMP TABLE`, `INSERT` with typed `NULL` hint, `SELECT`), then 2 warm cached ops | Batch 1: `3` `Parse`/`Bind`/`Execute` groups, `0` `Flush` frames, **`1`** `Sync` frame, **`1`** `poll_flush` completion, `0` pre-batch catalog queries; Batch 2: warm `Bind` uses binary formats with **`1`** `Sync` and **`1`** `poll_flush`. |
| Later forbidden command blocks whole batch | `[INSERT INTO t VALUES ($1), ";/*c*/COMMIT"]` and `[INSERT INTO t VALUES ($1), "--x\nCOPY t TO STDOUT"]` | Returns `Err(PipelineValidation { failing_index: 1, .. })` (`InterfaceError` / `NotSupportedError`, `outcomes = ()`); wire counter records **`0`** `Parse`/`Bind`/`Execute` frames and **`0`** data-batch flushes. |
| Duplex backpressure under tiny socket buffer | Duplex channel (`max_buf_size = 1024`) with 200 operations and 512 KiB inbound `DataRow` frames echoed before final `Sync` arrives | Completes without deadlock within bounded memory while `poll_flush` count remains `1`. |
| Partial-send EOF retires connection | Peer drops write half mid-batch after 2 KiB written | `submit_pipeline` returns `Err(PostgresError::ConnectionClosed)` and `conn.is_broken() == true`. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire
  ```
- Final Green, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] Every non-empty cold or warm `PipelineBatch` emits all `N` operation sequences with zero `Flush` frames, **one** trailing `Sync`, and **one** logical `poll_flush` completion, with zero pre-batch catalog round trips (R17).
- [ ] Whole-batch `validate_pipeline_commands` runs before encoding/sending operation `0`, producing zero operation frames, zero flushes, `outcomes = ()`, and exact `failing_index` when any operation head is forbidden (R13, R17).
- [ ] Duplex read-while-writing prevents deadlocks under backpressure, bounds buffered bytes via `PipelineLimits`, and retires connections on partial-send failures (R17).
- [ ] `pipeline_wire` integration tests, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen single-`Sync` pipeline batching and pre-emission guard invariants.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL extended pipeline protocol flow, cold text framing, and duplex backpressure analysis.
