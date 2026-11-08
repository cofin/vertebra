---
type: Task
id: "native-postgres-driver:ch12:t02"
chapter_id: ch12-pgvector-and-extended-query
title: Implement cold-query pre-Parse parameter discovery and post-ReadyForQuery owned result normalization
description: Resolve required custom parameter metadata before Parse, normalize owned cold text results after ReadyForQuery with ambient-transaction savepoint isolation, and attach AfterCompletion diagnostics on unrecoverable post-completion errors.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch12:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch12-pgvector-and-extended-query/spec.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/errors.rs
files:
  - src/rs/vertebra-postgres/src/metadata.rs
  - src/rs/vertebra-postgres/src/errors.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/metadata_normalization.rs
estimated_loc: 250
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
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cold ordinary-query metadata ordering, ambient T enrichment savepoint, and AfterCompletion completed_result contract
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL unnamed prepared statement destruction on Query and transaction-pooler Sync backend rotation
  - id: py-db-errors
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical shared DatabaseError hierarchy preserved by AfterCompletion
tags:
  - postgres
  - rust
  - metadata
  - pooler
---

# Task t02: Implement Cold-Query Pre-`Parse` Parameter Discovery and Post-`ReadyForQuery` Owned Result Normalization

## Objective & Scope Boundary

Implement `metadata.rs::{ResolvedParameters, OwnedTextResult, OwnedTextField, resolve_parameter_metadata, normalize_materialized_result}` and `errors.rs::PostgresError::AfterCompletion` so ordinary cold queries resolve custom parameter serialization metadata before `Parse` and enrich already-completed owned text results only after `ReadyForQuery` — with zero `Parse`->`Query`->`Bind` hazard, zero implicit `BEGIN`, and zero SQL replay [^pg-contracts] [^pg-research] [^py-db-errors]. Extended wire `Parse`/`Bind`/`Execute` framing (`t03`), user `TransactionScope` (`ch13:t01`), and Python `.completed_result` attribute conversion (`ch14:t04`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Pre-`Parse` parameter lookup, post-`ReadyForQuery` owned text normalization, ambient-`T` enrichment savepoint, and `.completed_result` diagnostics [^pg-contracts].
- [spec.md](../spec.md) — Chapter 12 `metadata.rs` and `AfterCompletion` contracts.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection` (`type_catalog`, `unnamed_statement_valid`, `transaction_status`, `parameters()`, broken-session retirement).
- `src/rs/vertebra-postgres/src/query.rs` — `PostgresConnection::simple_query` (used for catalog lookups and internal metadata savepoint commands).
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog::discover` and `lookup_extension_type`.
- `src/rs/vertebra-postgres/src/errors.rs` — `PostgresError` variants, `sqlstate()`, `is_transient()`, `Display`, and `postgres_error_to_py`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/errors.rs`**:
  - Add `AfterCompletion { source: Box<PostgresError>, completed_result: crate::result::QueryResult }` to `PostgresError` without introducing a competing Python exception class.
  - Update every exhaustive match in `errors.rs` (`Display`, `std::error::Error::source`, `sqlstate`, `is_transient`, `postgres_error_to_py`) to delegate to `source`; `Display` formats `source` without logging `completed_result` row values.
- **`src/rs/vertebra-postgres/src/metadata.rs` (exported via `pub mod metadata;` in `src/rs/vertebra-postgres/src/lib.rs`)**:
  - `#[derive(Clone, Debug, PartialEq)] pub struct ParameterCandidate { pub value: PgValue, pub explicit_oid: Option<u32> }` (or shared parameter input descriptor consumed by `t03::Parameter`) and `pub struct ResolvedParameters { pub parameter_oids: Vec<u32>, pub all_formats_known: bool }`.
  - `pub struct OwnedTextField { pub bytes: Option<Vec<u8>> }` and `pub struct OwnedTextResult`: stores ordered `Vec<Column>`, owned `Vec<Vec<OwnedTextField>>`, `affected_rows: u64`, `command_tag: String`, `transaction_status: TransactionStatus`, shared `Arc<CodecContext>` snapshot captured when each row/result generation was received (before any later catalog exchange can alter `DateStyle`/`IntervalStyle`/`TimeZone`), and an explicit `exchange_completed: bool` gate (`OwnedTextResult::mark_completed(...)`). Never holds borrowed backend frame slices.
  - `pub async fn resolve_parameter_metadata(conn: &mut PostgresConnection, params: &[(PgValue, Option<u32>)], parameter_oids: Option<&[u32]>) -> Result<ResolvedParameters, PostgresError>`:
    - Built-in or server-inferable parameters perform zero catalog queries and emit zero `BEGIN`.
    - When a custom wrapper (`PgValue::Vector`, `PgValue::Composite`, or `PgValue::Array` with unknown custom OID) requires catalog metadata before serialization, runs `TypeCatalog::discover` before any `Parse` is sent (using an internal metadata-only savepoint `SAVEPOINT _vtb_meta` / `ROLLBACK TO SAVEPOINT _vtb_meta; RELEASE SAVEPOINT _vtb_meta` if `conn.transaction_status() == TransactionStatus::InTransaction`).
    - If required parameter encoding metadata remains unavailable or discovery fails, returns the error before any user query is emitted (with NO `AfterCompletion` wrapper and NO invented OID).
  - `pub async fn normalize_materialized_result(conn: &mut PostgresConnection, raw: OwnedTextResult) -> Result<QueryResult, PostgresError>`:
    - Refuses `!raw.exchange_completed` or non-idle session exchange state with `PostgresError::Interface`.
    - Collects only unresolved `Column.type_oid`s; if none exist, decodes owned fields directly through `codecs::decode_value_with_catalog` using the captured `Arc<CodecContext>` with zero network I/O.
    - When unresolved OIDs exist: at `TransactionStatus::Idle`, runs `TypeCatalog::discover` directly (never issuing `BEGIN`); at `TransactionStatus::InTransaction`, wraps `TypeCatalog::discover` in `SAVEPOINT _vtb_meta` -> `RELEASE SAVEPOINT _vtb_meta`, rolling back and releasing `_vtb_meta` on recoverable `PostgresError::Server` so ambient `T` and prior user work survive intact; at `TransactionStatus::Failed`, skips catalog `Query` and retains exact `PgValue::Text` fallback.
    - Consumes `OwnedTextResult` progressively (moving row vectors without a second whole-result clone): newly discovered OIDs decode into typed `PgValue`s using the original captured `CodecContext`, while unknown or recoverably failed catalog OIDs remain `PgValue::Text` with original `Column.type_oid`.
    - If post-completion catalog/savepoint cleanup encounters an unrecoverable transport, protocol, or broken-cleanup failure, marks `conn` broken, constructs the fallback `QueryResult` from the original `Columns` + remaining owned `PgValue::Text` cells + original `command_tag`/`affected_rows`/`transaction_status`, and returns `Err(PostgresError::AfterCompletion { source: Box::new(err), completed_result })` — NEVER replaying the original SQL.

## Edge Cases & Error Semantics

- **Stateful unnamed-statement & backend-rotation safety**: Catalog `Query` clears `conn.unnamed_statement_valid()`, and an idle `Sync` may rotate the backend under a transaction pooler; `normalize_materialized_result` runs strictly after `ReadyForQuery` and never issues a `Bind` after catalog `Query` or assumes backend affinity at `I`.
- **Session GUC drift during post-query catalog exchange**: If a peer sends `ParameterStatus("DateStyle", "German, DMY")` during the post-result catalog `Query`, `normalize_materialized_result` still decodes the original result's temporal text using the `Arc<CodecContext>` captured when the original rows arrived.
- **Single-`Sync` pipelines excluded**: Pipeline execution (`ch15`) never calls `normalize_materialized_result` and keeps undiscovered custom OIDs as `PgValue::Text`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/metadata_normalization.rs` (stateful loopback peer tracking unnamed-statement destruction on every `Query`, backend ID rotation on every idle `ReadyForQuery`, and ambient `T` pinning).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Built-in parameters & results emit zero catalog `Query` or `BEGIN` | `resolve_parameter_metadata` + `normalize_materialized_result` with `INT4` (`23`) and `TEXT` (`25`) | Zero `Query` or `BEGIN` frames written to peer; returns typed `QueryResult`. |
| Pre-`Parse` `PgVector` discovery & unavailable extension failure | `PgValue::Vector` (`HalfVec`) when `"halfvec"` is discovered (`42002`) vs absent from `pg_extension` | Discovered case resolves `42002` before caller's `Parse`; missing extension fails before caller SQL emission with plain `PostgresError::Data` (not `AfterCompletion`). |
| Post-`ReadyForQuery` custom OID enrichment across backend rotation & GUC change | Stateful peer rotates backend on original `ReadyForQuery(b'I')`, clears unnamed statement on catalog `Query`, and emits `ParameterStatus("DateStyle", "German, DMY")` during catalog lookup | Original `INSERT ... RETURNING custom_comp, ts_col` executes once, zero `Bind` follows catalog `Query`, `custom_comp` normalizes to `PgValue::Composite`, and `ts_col` uses original captured `ISO` `CodecContext`. |
| Recoverable catalog denial (`42501`) at `I` and inside ambient `T` | Peer returns `ErrorResponse("42501")` on `pg_type` lookup at `I` vs at `T` | At `I`, drains to `I` and returns `PgValue::Text` with original `Column.type_oid`; at `T`, issues `ROLLBACK TO SAVEPOINT _vtb_meta; RELEASE SAVEPOINT _vtb_meta`, preserves `conn.transaction_status() == InTransaction`, and returns `PgValue::Text` fallback. |
| Unrecoverable transport drop or failed savepoint cleanup after side-effecting query | Peer drops socket mid-catalog `Query` (or fails `ROLLBACK TO SAVEPOINT`) after `INSERT 0 1` completed | Marks `conn` broken, emits zero replay of `INSERT`, and returns `Err(PostgresError::AfterCompletion { source, completed_result })` preserving `affected_rows == 1`, original `Columns`, fallback `PgValue::Text`, and `Display` without row values. |
| Uncompleted exchange gate | Call `normalize_materialized_result` on `OwnedTextResult` with `exchange_completed == false` | Fails immediately with `Err(PostgresError::Interface(_))`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test metadata_normalization`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test metadata_normalization && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test type_catalog && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Required custom parameter metadata resolves before `Parse`, and cold result normalization runs strictly after `ReadyForQuery` using owned text fields and captured `Arc<CodecContext>` snapshots without re-executing or re-binding user SQL (R12, R13, R15).
- [ ] Zero implicit `BEGIN` is emitted at `I`, and ambient `T` catalog lookups isolate recoverable errors inside an internal metadata savepoint while preserving exact `PgValue::Text` + `Column.type_oid` fallback (R12, R15).
- [ ] Unrecoverable post-completion catalog/cleanup failures mark the session broken and surface `PostgresError::AfterCompletion` retaining the original `completed_result` without logging row contents in `Display` (R12, R13).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cold ordinary-query metadata ordering, ambient `T` enrichment savepoint, and `AfterCompletion` `completed_result` contract.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL unnamed prepared statement destruction on `Query` and transaction-pooler `Sync` backend rotation.
[^py-db-errors]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical shared `DatabaseError` hierarchy preserved by `AfterCompletion`.
