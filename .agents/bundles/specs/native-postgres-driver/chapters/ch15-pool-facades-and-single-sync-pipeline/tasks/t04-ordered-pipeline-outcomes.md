---
type: Task
id: "native-postgres-driver:ch15:t04"
chapter_id: ch15-pool-facades-and-single-sync-pipeline
title: "Demultiplex ordered pipeline results, errors, and skipped outcomes"
description: "Demultiplex cold and warm single-Sync pipeline frame streams into ordered Result, Error, and Skipped outcomes, drain through ReadyForQuery, and define frozen Python pipeline outcome records."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch15:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch15-pool-facades-and-single-sync-pipeline/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/pipeline.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/py/vertebra/db/adapters/postgres/types.py
files:
  - src/rs/vertebra-postgres/src/pipeline.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/py/vertebra/db/adapters/postgres/types.py
tests:
  - src/rs/vertebra-postgres/tests/pipeline_outcomes.rs
  - src/py/tests/unit/db/test_postgres_types.py
estimated_loc: 245
plan_revision: 5
plan_commit: fd571e3a
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen PostgresErrorInfo, PostgresPipelineOperation, PostgresPipelineOutcome, and first-error outcome invariants
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL pipeline ErrorResponse frame discard until Sync and ReadyForQuery status rules
tags:
  - postgres
  - pipeline
  - outcomes
  - msgspec
  - rust
---

# Task t04: Demultiplex Ordered Pipeline Results, Errors, and Skipped Outcomes

## Objective & Scope Boundary

Demultiplex single-`Sync` cold and warm pipeline responses into ordered `PipelineOutcome::{Result, Error, Skipped}` entries in `vertebra-postgres`, drain boundedly to the trailing `Sync` `ReadyForQuery` (`I`/`T`/`E`) without committing or releasing the session scope, and define the frozen `PostgresErrorInfo`, `PostgresPipelineOperation`, and `PostgresPipelineOutcome` `msgspec.Struct` records in `src/py/vertebra/db/adapters/postgres/types.py` [^pg-contracts] [^pg-protocol-research]. Retaining transaction/savepoint scopes across Python result conversion and `SyncPostgresPipeline` / `AsyncPostgresPipeline` contexts belongs to `ch16:t01`–`t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 15 ordered outcome demultiplexing and error-draining contracts.
- [contracts.md](../../../contracts.md) — Exact `PostgresErrorInfo`, `PostgresPipelineOperation`, and `PostgresPipelineOutcome` `msgspec.Struct` definitions and discriminant invariants [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Server discard-until-`Sync` behavior verified on `[SELECT 1, SELECT 1/0, SELECT 3]` [^pg-protocol-research].
- `src/rs/vertebra-postgres/src/pipeline.rs` — `PipelineBatch`, `PipelineOperation`, and `submit_pipeline` from `ch15:t03`.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError`, `ServerError`, and `postgres_error_to_py`.
- `src/py/vertebra/db/adapters/postgres/types.py` — Existing frozen `msgspec.Struct` PostgreSQL value and result records from `ch09:t01`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/pipeline.rs` and `src/rs/vertebra-postgres/src/errors.rs`:
  - `pub enum PipelineOutcome`:
    - `Result(QueryResult)`
    - `Error(ServerError)`
    - `Skipped { failed_index: usize }`
  - `pub struct PipelineFailure`:
    - `pub first_error: PostgresError`
    - `pub failing_index: usize`
    - `pub outcomes: Vec<PipelineOutcome>`
    - `pub transaction_status: TransactionStatus`
  - `PostgresConnection::pipeline_results(&mut self, batch: &PipelineBatch) -> impl Future<Output = Result<Vec<PipelineOutcome>, PipelineFailure>> + Send`:
    - Tracks per-operation expected backend phase sequence (`ParseComplete` when `Parse` was emitted, `ParameterDescription` + statement `RowDescription`/`NoData` when `DescribeStatement` was emitted, `BindComplete`, portal `RowDescription`/`NoData` when `DescribePortal` was emitted, `DataRow*`, and `CommandComplete` / `EmptyQueryResponse`).
    - Uses portal `RowDescription` formats (never statement `Describe` text-format placeholders) to decode `DataRow` cells with the captured `CodecContext` and snapshotted `TypeCatalog`, updating each completed `QueryResult.transaction_status` to the actual final batch `ReadyForQuery` status (`I` / `T` / `E`) rather than assuming commit from `CommandComplete`.
    - On the **first** `ErrorResponse` at operation `k`, records `PipelineOutcome::Error(server_err)` at index `k`, synthesizes `PipelineOutcome::Skipped { failed_index: k }` for every remaining operation `k + 1 .. N` (expecting zero further `ErrorResponse`/`CommandComplete` frames for discarded ops), drains boundedly through the single trailing `Sync` `ReadyForQuery`, updates `PostgresConnection` `TransactionStatus`, and returns `Err(PipelineFailure)` without committing, rolling back, or releasing any scope/lease.
    - Add `PostgresError::PipelineAborted(Box<PipelineFailure>)` in `errors.rs` delegating category/SQLSTATE to `first_error` so `ch16:t01`–`t02` project `.outcomes` and `.failing_index` onto the canonical `vertebra._vertebra` `DatabaseError` subclass.
- `src/py/vertebra/db/adapters/postgres/types.py`:
  - Add frozen `gc=False` `msgspec.Struct` classes matching `contracts.md` field order and defaults [^pg-contracts]:
    - `PostgresErrorInfo(sqlstate: str | None, message: str, severity: str | None, diagnostics: tuple[tuple[str, str], ...])`
    - `PostgresPipelineOperation(sql: str, parameters: tuple[PostgresValue | PgParameter, ...] = (), parameter_oids: tuple[int, ...] | None = None, kind: Literal["execute", "select"] = "execute")`
    - `PostgresPipelineOutcome(index: int, status: Literal["result", "error", "skipped"], result: PostgresQueryResult | None = None, error: PostgresErrorInfo | None = None, failed_index: int | None = None)`
    Validate in `__post_init__`: non-negative integer `index` (rejecting `bool`), equal `len(parameters) == len(parameter_oids)` when `parameter_oids` is supplied, and exact mutual exclusion on `PostgresPipelineOutcome` (`status == "result"` requires `result is not None` and `error is None and failed_index is None`; `status == "error"` requires `error is not None` and `result is None and failed_index is None`; `status == "skipped"` requires non-negative integer `failed_index < index` and `result is None and error is None`).

## Edge Cases & Error Semantics

- **Phase-accurate failure attribution**: An `ErrorResponse` arriving during operation `k`'s `Parse` (`42601`), `DescribeStatement` (`42P01`), or `Bind` (`22P02`) before `Execute` attributes `PipelineOutcome::Error` to operation `k` and marks `k + 1 .. N` as `Skipped { failed_index: k }`.
- **Decode failure mid-batch**: Retains the first decode `PostgresError::Data` at operation `k`, drains boundedly to `ReadyForQuery` (retiring the socket on malformed framing), and never fabricates successful rows or fake SQLSTATEs for skipped operations.
- **Interleaved async frames**: `NoticeResponse`, `ParameterStatus` (updating `CodecContext` for subsequent operations in the batch), and `NotificationResponse` are processed cleanly during demultiplexing.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/pipeline_outcomes.rs` and `src/py/tests/unit/db/test_postgres_types.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Heterogeneous `Result` batch | 4-op batch: `[INSERT (affected 2), SELECT 2 rows (mixed binary/text), CREATE TEMP TABLE (NoData), "" (EmptyQueryResponse)]` ending in `ReadyForQuery('T')` | `Ok(vec![Result(..), Result(..), Result(..), Result(..)])` preserving exact order, rows, `affected_rows` (`2, 2, 0, 0`), and `transaction_status == InTransaction`. |
| Middle `Execute` failure (`SELECT 1/0`) | `[SELECT 1, SELECT 1/0, SELECT 3]` -> `CommandComplete`, `ErrorResponse("22012")`, `ReadyForQuery('E')` | `Err(PipelineFailure)` with `failing_index == 1`, `outcomes == [Result(1), Error("22012"), Skipped { failed_index: 1 }]`, `transaction_status == Failed`; connection remains reusable after `rollback()`. |
| `Parse` / `Bind` failure before `Execute` | Op `0` succeeds; Op `1` fails during `Parse` (`42601`) or `Bind` (`22P02`); Op `2` discarded by server | `failing_index == 1`, `outcomes[0]` is `Result`, `outcomes[1]` is `Error("42601" | "22P02")`, `outcomes[2]` is `Skipped { failed_index: 1 }` with no fabricated SQLSTATE. |
| Interleaved `ParameterStatus` & `NoticeResponse` | Op `0` `SET LOCAL TimeZone = 'UTC'` emits `ParameterStatus` before Op `1` cold `timestamptz` `DataRow` + `NoticeResponse` | Op `1` decodes with updated `TimeZone` context; `NoticeResponse` is absorbed without shifting operation indices. |
| Python `PostgresPipeline*` record invariants | Construct valid `result` / `error` / `skipped` `PostgresPipelineOutcome`s vs invalid combinations (`status="skipped"` with `error=...`, `failed_index >= index`, `index=True`, mismatched `parameter_oids` length) | Valid frozen `msgspec.Struct` instances succeed; every invalid discriminant/bound/bool combination raises `ValueError` or `TypeError`. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_outcomes
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q
  ```
- Final Green, Ruff, Mypy, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_outcomes
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_wire
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_types.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/types.py src/py/tests/unit/db/test_postgres_types.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/types.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [x] `PostgresConnection::pipeline_results` produces one ordered `Result` / `Error` / `Skipped { failed_index }` per submitted operation, accurately attributing `Parse`/`Describe`/`Bind`/`Execute` failures without fabricated SQLSTATEs (R17).
- [x] First server error drains boundedly through the single trailing `Sync` `ReadyForQuery` (`I`/`T`/`E`) without committing, rolling back, or releasing caller scopes (R17).
- [x] `PostgresErrorInfo`, `PostgresPipelineOperation`, and `PostgresPipelineOutcome` in `types.py` enforce frozen `gc=False` discriminant, length, and integer invariants (R17).
- [x] `pipeline_outcomes`, `pipeline_wire`, `test_postgres_types.py`, Ruff, Mypy, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen `PostgresPipelineOperation`, `PostgresPipelineOutcome`, `PostgresErrorInfo`, and pipeline outcome invariants.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL extended pipeline `ErrorResponse` discard-until-`Sync` protocol behavior.
