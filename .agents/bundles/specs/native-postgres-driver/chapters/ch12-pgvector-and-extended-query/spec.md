---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch12-pgvector-and-extended-query
title: "Chapter 12: Validated pgvector/halfvec/Bit Codecs, Cold Metadata Normalization, and Mixed-Format Extended Queries"
description: Repair shared pgvector/halfvec/bit CPU helpers, integrate dynamic extension vector and bitstring codecs, normalize cold-query parameter and result metadata at safe protocol boundaries, and execute mixed-format extended queries.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch11-type-catalog-arrays-and-composites
replan_trigger: null
current_task: "native-postgres-driver:ch12:t01"
plan_revision: 5
plan_commit: fd571e3a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
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
    title: PgVector 1..=16000 and half 2.7.1 contracts, PgBitString rules, cold-query metadata boundaries, and AfterCompletion diagnostics
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 extended query flow (54.2.3), unnamed statement lifecycle, and pgvector 0.8.1 wire layouts
  - id: pg-vector-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16
    title: Existing pgvector f32/f16/bit CPU helpers and PostgreSQL extended frame encoders in vertebra-db
  - id: pg-embed-py
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: Python PostgresEmbeddingPushdown consumer of pgvector encoding and decoding
tags:
  - postgres
  - rust
  - pgvector
  - codecs
  - protocol
---

# Chapter 12: Validated pgvector/halfvec/Bit Codecs, Cold Metadata Normalization, and Mixed-Format Extended Queries

<!-- truth: start -->
- **pgvector `0.8.1` (`1..=16000`) & `half = 2.7.1` Fidelity**: Shared `vertebra_db::postgres` CPU helpers and dynamic `TypeCatalog`-bound `vector`/`halfvec` codecs enforce `1..=16000` dimensions, IEEE 754 ties-to-even `half::f16::from_f32` quantization, pre/post-narrowing finiteness (`65520.0 -> Inf` rejected), and `BIT`/`VARBIT` (`1560`/`1562`) `0`-length + zero low-bit padding invariants [^pg-contracts] [^pg-vector-rs].
- **Pooler-Safe Cold Metadata Boundaries**: Custom parameter metadata resolves before `Parse`, and cold text results normalize only after the original `Execute` + `ReadyForQuery` completes using captured `CodecContext` snapshots — never inserting `Query` between `Parse` and `Bind`, opening implicit `BEGIN`, or replaying completed SQL (surfacing `PostgresError::AfterCompletion` on unrecoverable post-completion failures) [^pg-contracts] [^pg-research].
- **Mixed-Format Extended Execution**: `PostgresConnection::query` emits single-roundtrip `Parse` + `Describe` + `Bind` + `Execute` + `Sync` groups with typed `NULL`s, canonical `42P18` server inference errors, pre-send `TemporalText` rejection, and per-parameter / per-column binary/text format negotiation [^pg-contracts] [^pg-research].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Two critical gaps remain before `vertebra-postgres` can execute parameterized queries end to end: first, the existing `vertebra_db::postgres` vector/bit helpers round `f16` midpoints away from zero, overflow `65520.0_f32` silently to `Infinity`, accept dimensions up to `u16::MAX` (instead of pgvector `0.8.1`'s `16000` ceiling), reject valid zero-length `bit`/`varbit`, and ignore nonzero padding bits [^pg-contracts] [^pg-vector-rs]; second, PostgreSQL's extended query protocol (`Parse`/`Describe`/`Bind`/`Execute`/`Sync`) requires custom parameter serialization metadata before `Parse` and destroys the unnamed statement if any `pg_catalog` simple `Query` runs between `Parse` and `Bind`, while transaction-mode poolers rotate backends on idle `Sync` and commands like `VACUUM` forbid an implicit `BEGIN` [^pg-contracts] [^pg-research]. Chapter 12 repairs the shared vector/bit codecs (`t01`), builds boundary-safe cold metadata normalization with `AfterCompletion` diagnostics (`t02`), and implements `PostgresConnection::query` with mixed binary/text format negotiation (`t03`).

### 1.2 Non-Goals

- `sparsevec` codecs (explicitly outside the driver charter [^pg-contracts]).
- Pinned explicit `TransactionScope` / savepoint user API (`ch13:t01`), bounded portal `max_rows` streaming (`ch13:t02`), and named prepared-statement caching (`ch13:t03`).
- Single-`Sync` multi-operation pipeline submission (`ch15`–`ch16`) and Python `PgVector` / `PgBitString` / `.completed_result` projection (`ch14:t03`–`ch14:t04`).

### 1.3 Upstream Execution Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface authority: [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Depends on `ch11-type-catalog-arrays-and-composites` (`TypeCatalog`, `SimpleQueryCollector`, and scalar/numeric/temporal/JSON/array/composite codecs).

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Vector, Bitstring, & Cold-Query Contracts | `.agents/bundles/specs/native-postgres-driver/contracts.md` [^pg-contracts] | Defines `PgVector` (`1..=16000`, `half 2.7.1`), `PgBitString` (`(b"", 0)` valid, zero padding), cold text-group flow, `TemporalText` bind rejection, and `.completed_result`. |
| Extended Protocol & Pooler Lifecycle | `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` [^pg-research] | Documents Protocol 3.0 section 54.2.3, unnamed statement invalidation on `Query`, and transaction-pooler rotation on idle `Sync`. |
| Shared Vector & Bit CPU Helpers | `src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16` [^pg-vector-rs] | Houses `encode/decode_pgvector_{f32,f16,bit}` whose rounding, `1..=16000` dimension, finiteness, and `0`-bit rules are repaired in `t01`. |
| Embedding Pushdown Consumer | `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` [^pg-embed-py] | Downstream Python consumer relying on exact `decode_pgvector_f32` and pgvector parameter semantics. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).
- Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11 (`ch12:t01`)**: Repair `vertebra_db::postgres::{encode_pgvector_f32, decode_pgvector_f32, encode_pgvector_f16, decode_pgvector_f16, encode_pgvector_bit, decode_pgvector_bit}` using `half = "=2.7.1"` (`1..=16000` vector/halfvec dimensions, ties-to-even quantization, pre/post-narrowing finiteness, valid `0`-bit `bit`/`varbit`, zero low-bit padding) and wire `TypeCatalog`-resolved `vector`/`halfvec` plus built-in `BIT` (`1560`) / `VARBIT` (`1562`) binary and text codecs into `vertebra-postgres`.
- **R12, R13, R15 (`ch12:t02`)**: Resolve required custom parameter metadata before `Parse` and normalize owned cold text results only after `ReadyForQuery` using captured `Arc<CodecContext>` snapshots, isolating ambient `T` catalog queries behind an internal metadata savepoint and wrapping unrecoverable post-completion failures in `PostgresError::AfterCompletion { source, completed_result }` without SQL replay.
- **R12 (`ch12:t03`)**: Execute single-statement extended queries via `PostgresConnection::query` with `Parameter` / `parameter_oids` validation, typed `NULL`s, pre-send `TemporalText` rejection, cold all-text single-group execution (no implicit `BEGIN`, `VACUUM`-safe), and warm mixed per-parameter / per-column binary/text format negotiation.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- **Shared Vector & Bit CPU Helpers (`src/rs/vertebra-db/src/postgres/mod.rs`) & Dispatcher (`src/rs/vertebra-postgres/src/codecs/mod.rs`)**:
  - Preserves exact public signatures: `encode_pgvector_f32(&[f32]) -> Result<Vec<u8>, String>`, `decode_pgvector_f32(&[u8]) -> Result<Vec<f32>, String>`, `encode_pgvector_f16(&[f32]) -> Result<Vec<u8>, String>`, `decode_pgvector_f16(&[u8]) -> Result<Vec<f32>, String>`, `encode_pgvector_bit(&[u8], usize) -> Result<Vec<u8>, String>`, `decode_pgvector_bit(&[u8]) -> Result<(Vec<u8>, usize), String>`.
  - Built-in OIDs in `codecs/mod.rs`: `BIT_OID = 1560`, `VARBIT_OID = 1562`, `BIT_ARRAY_OID = 1561`, `VARBIT_ARRAY_OID = 1563`; dynamic `"vector"` and `"halfvec"` resolved exclusively through `TypeCatalog`.
- **Cold Metadata Normalization & Diagnostics (`src/rs/vertebra-postgres/src/{metadata,errors}.rs`)**:
  - `PostgresError::AfterCompletion { source: Box<PostgresError>, completed_result: QueryResult }` (`Display`, `sqlstate()`, `is_transient()`, and `postgres_error_to_py` delegate to `source`; `Display` never logs completed row payloads).
  - `pub struct ResolvedParameters { pub parameters: Vec<Parameter>, pub parameter_oids: Vec<u32>, pub all_formats_known: bool }`
  - `pub struct OwnedTextField { pub bytes: Option<Vec<u8>> }`, `pub struct OwnedTextResult { ... }` (completion-gated).
  - `pub async fn resolve_parameter_metadata(conn: &mut PostgresConnection, parameters: &[Parameter], explicit_oids: Option<&[u32]>) -> Result<ResolvedParameters, PostgresError>`
  - `pub async fn normalize_materialized_result(conn: &mut PostgresConnection, raw_result: OwnedTextResult) -> Result<QueryResult, PostgresError>`
- **Extended Query Execution (`src/rs/vertebra-postgres/src/{extended,session}.rs`)**:
  - `pub(crate) type RowBody<'a> = postgres_protocol::message::backend::DataRowBody<'a>;`
  - `#[derive(Clone, Debug, PartialEq)] pub struct Parameter { pub value: PgValue, pub type_oid: Option<u32> }`
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct PreparedDescription { pub parameter_oids: Vec<u32>, pub result_columns: Vec<Column> }`
  - `pub fn encode_bind(...) -> Result<(), PostgresError>`, `pub struct ExtendedQueryCollector`
  - `impl PostgresConnection { pub async fn query(&mut self, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>) -> Result<QueryResult, PostgresError> }`
  - `impl PostgresConnection { pub(crate) async fn query_with_row_sink<F>(&mut self, sql: &str, params: &[Parameter], result_formats: &[Format], on_columns: impl FnOnce(&[Column]) -> Result<(), PostgresError>, on_row: F) -> Result<String, PostgresError> where F: FnMut(&RowBody<'_>) -> Result<(), PostgresError> }` (shared zero-materialization streaming seam consumed by `ch20:t03` `execute_select_to_arrow` without touching `extended.rs` or `session.rs` in `ch20`).

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `PostgresConnection::query` (`session.rs`) -> `metadata::resolve_parameter_metadata` (`metadata.rs`, optional pre-`Parse` `TypeCatalog::discover`) -> `extended::encode_bind` + `ExtendedQueryCollector` (`extended.rs`) -> warm path decodes mixed binary/text via `codecs::decode_value_with_catalog` (`codecs/mod.rs` + `vertebra_db::postgres` vector/bit helpers) OR cold all-text path finishes `ReadyForQuery` then calls `metadata::normalize_materialized_result` (`metadata.rs`).

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| `halfvec` ties-to-even & finite overflow | `1.00048828125_f32` (`0x3f80_1000`) vs `65520.0_f32` (`> 65504.0`) vs `dim = 16001` | `1.00048828125` rounds to even `1.0` (`0x3c00`); `65520.0` and `16001`-dim fail before send with `PostgresError::Data` / `Err(String)` | Exact pgvector `0.8.1` IEEE 754 binary16 parity |
| `bit`/`varbit` zero length & nonzero tail padding | `(b"", 0)` vs `(b"\xa0", 3)` vs `(b"\xa8", 3)` | `(b"", 0)` and `(b"\xa0", 3)` (`"101"`) succeed; `(b"\xa8", 3)` fails with `PostgresError::Data` / `Err(String)` | Exact `PgBitString` canonical padding invariant |
| Cold query across transaction pooler & `VACUUM` | `conn.query("VACUUM", &[], None)` or cold custom composite/vector query when `Sync` rotates backends | Emits zero `BEGIN` frames and zero `Query` between `Parse` and `Bind`; result OID discovery runs after `ReadyForQuery` | Transaction-pooler & non-transactional SQL compatibility |
| Post-completion enrichment failure & ambient `T` | Catalog permission error (`42501`) inside `T`, or socket drop during post-`ReadyForQuery` catalog `Query` after `INSERT ... RETURNING` | `42501` at `T` rolls back only internal metadata savepoint and returns exact `PgValue::Text` fallback; socket drop marks session broken and returns `PostgresError::AfterCompletion { source, completed_result }` without re-running `INSERT` | Zero ambient-transaction poisoning and zero side-effect replay |
| Unresolved `TemporalText` or ambiguous `NULL` bind | `Parameter { value: PgValue::TemporalText { .. }, .. }` or untyped `NULL` rejected by server with `42P18` | `TemporalText` fails locally with `PostgresError::Data` before socket write even under matching `DateStyle`/`TimeZone`; server `42P18` drains to `ReadyForQuery` as `PostgresError::Server` | Zero ambiguous instant or invented `NULL` type coercion |

### 2.5 Acceptance Scenarios (Given / When / Then)

- **Given** a cold `PostgresConnection` connected to a stateful peer that clears the unnamed statement on every `Query` and rotates backend IDs on every idle `Sync`, **When** `conn.query("SELECT $1::vector, custom_col FROM items", &[Parameter { value: PgValue::Vector { values: vec![0.5, -1.25], kind: VectorKind::Vector }, type_oid: None }], None)` executes, **Then** `"vector"` extension metadata is discovered before `Parse`, the extended group runs once with zero intermediate `Query` or `BEGIN`, and `custom_col` is normalized after `ReadyForQuery` into its typed `PgValue` (or `PgValue::Text` if unresolvable).
- **Given** a warm `TypeCatalog` with known `INT4` (`23`), `UUID` (`2950`), `vector` (`42001`), and an unknown custom column (`59999`), **When** `conn.query(...)` executes, **Then** `Bind` negotiates `Format::Binary` (`1`) for the known columns and `Format::Text` (`0`) for `59999` in a single extended exchange.

### 2.6 Alternatives Considered

| Alternative | Why Rejected | Chosen Seam |
| --- | --- | --- |
| Keeping handwritten `f32_to_f16_bits` in `vertebra-db` | Rounded midpoints away from zero and turned finite `65520.0_f32` into `+Infinity`. | Use `half = "=2.7.1"` (already declared in `vertebra-db` and `vertebra-postgres` in `ch01:t01`–`t02` and locked in `Cargo.lock`) with pre/post-conversion finiteness checks and zero lockfile churn. |
| `Parse` + `DescribeStatement` -> `Sync` -> catalog `Query` -> `Bind` | `Sync` allows PgBouncer backend rotation and `Query` destroys the unnamed prepared statement. | Pre-`Parse` parameter discovery + single all-text extended group + post-`ReadyForQuery` owned-result normalization. |
| Opening an implicit `BEGIN` around cold queries to pin PgBouncer | Breaks `VACUUM`, `CREATE INDEX CONCURRENTLY`, and read-only pooler routing, and adds extra roundtrips. | Zero implicit `BEGIN`; internal metadata savepoint is used strictly when the caller is already inside ambient `T`. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch12]` (user docs and OKF data chapters update in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11 (`pgvector`, `halfvec`, & `bit`/`varbit` codecs) | [t01](tasks/t01-pgvector-halfvec-and-bit.md) | `regression_tdd` | `src/rs/vertebra-db/tests/postgres_vector_tests.rs`, `src/rs/vertebra-postgres/tests/vector_codecs.rs` |
| R12, R13, R15 (cold parameter discovery & result normalization) | [t02](tasks/t02-cold-query-metadata-normalization.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/metadata_normalization.rs` |
| R12 (extended query mixed formats & typed `NULL`s) | [t03](tasks/t03-extended-query-mixed-formats.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/extended_protocol.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch12:t01 — Repair shared pgvector/halfvec/bit CPU helpers and integrate dynamic vector and bitstring codecs (tasks/t01-pgvector-halfvec-and-bit.md)
- [ ] native-postgres-driver:ch12:t02 — Implement cold-query pre-Parse parameter discovery and post-ReadyForQuery owned result normalization (tasks/t02-cold-query-metadata-normalization.md)
- [ ] native-postgres-driver:ch12:t03 — Implement extended-query execution with typed NULLs and mixed binary/text format negotiation (tasks/t03-extended-query-mixed-formats.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib postgres::tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --test postgres_vector_tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `e8901de4`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Downstream Dependents**: Unblocks `ch13-transactions-portals-and-statement-cache` (`TransactionScope`, portals, prepared cache), `ch14` (pools and Python query facades), `ch15`–`ch17` (pipelines, cancellation, deadlines), and `ch18`–`ch20` (`COPY` and `VTBARROW`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgVector` `1..=16000` and `half 2.7.1` contracts, `PgBitString` rules, cold-query metadata boundaries, and `AfterCompletion` diagnostics.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 extended query flow (`54.2.3`), unnamed statement lifecycle, and pgvector `0.8.1` wire layouts.
[^pg-vector-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16` — Existing `pgvector` `f32`/`f16`/`bit` CPU helpers and PostgreSQL extended frame encoders in `vertebra-db`.
[^pg-embed-py]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — Python `PostgresEmbeddingPushdown` consumer of `pgvector` encoding and decoding.
