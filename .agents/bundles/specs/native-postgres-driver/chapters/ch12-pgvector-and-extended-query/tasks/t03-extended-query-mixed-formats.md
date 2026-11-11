---
type: Task
id: "native-postgres-driver:ch12:t03"
chapter_id: ch12-pgvector-and-extended-query
title: Implement extended-query execution with typed NULLs and mixed binary/text format negotiation
description: Implement extended.rs (Parameter, PreparedDescription, encode_bind, ExtendedQueryCollector) and PostgresConnection::query plus query_with_row_sink with cold all-text single-group safety and warm mixed binary/text format negotiation.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch12:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch12-pgvector-and-extended-query/spec.md
  - src/rs/vertebra-postgres/src/metadata.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/extended.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/extended_protocol.rs
estimated_loc: 250
plan_revision: 5
plan_commit: 936daa9f
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
commit: cae857ba
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
    title: Extended query single-statement contract, PgParameter typed NULLs, TemporalText bind rejection, and mixed format rules
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL Protocol 3.0 Parse/Describe/Bind/Execute/Sync framing, portal RowDescription formats, and SQLSTATE 42P18
  - id: pg-bind-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_bind_frame
    title: Reference PostgreSQL extended protocol message layout in vertebra-db
tags:
  - postgres
  - rust
  - protocol
  - extended-query
---

# Task t03: Implement Extended-Query Execution with Typed `NULL`s and Mixed Binary/Text Format Negotiation

## Objective & Scope Boundary

Implement `extended.rs::{RowBody, PreparedDescription, Parameter, encode_bind, ExtendedQueryCollector}`, `PostgresConnection::query`, and the streaming `pub(crate) async fn query_with_row_sink` seam (`session.rs`), executing single-statement extended queries in one `Sync`/flush roundtrip with exact parameter OIDs, typed `NULL`s, pre-send `TemporalText` rejection, cold all-text execution (`VACUUM`- and pooler-safe via `ch12:t02`), and warm per-parameter / per-column binary/text format negotiation [^pg-contracts] [^pg-research] [^pg-bind-rs]. Named statement caching (`ch13:t03`), suspended multi-fetch portals (`ch13:t02`), and multi-operation pipelines (`ch15`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgParameter` / `parameter_oids` conflict & `0`-inference rules, `TemporalText` (`special="wire_text"`) bind refusal, and cold vs warm extended execution invariants [^pg-contracts].
- [spec.md](../spec.md) — Chapter 12 `extended.rs`, `PostgresConnection::query`, and `query_with_row_sink` contracts.
- `src/rs/vertebra-postgres/src/metadata.rs` — `resolve_parameter_metadata`, `OwnedTextResult`, and `normalize_materialized_result` from `ch12:t02`.
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, `type_catalog`, `unnamed_statement_valid`, and active-exchange guard.
- `src/rs/vertebra-postgres/src/query.rs` — `parse_command_tag_affected_rows` and `parse_row_description` helpers (`pub mod query;` already mounted in `lib.rs` by `ch09:t02`).
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `format_for_oid_with_catalog`, `encode_value_with_catalog`, and `decode_value_with_catalog`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/extended.rs` & `src/rs/vertebra-postgres/src/lib.rs`**:
  - Remove `#[path = "types.rs"] pub mod types;` from `session.rs` and declare `pub mod types;` and `pub mod extended;` directly in `src/rs/vertebra-postgres/src/lib.rs` (alongside `pub mod query;` from `ch09:t02`).
  - `pub(crate) type RowBody<'a> = postgres_protocol::message::backend::DataRowBody<'a>;`
  - `#[derive(Clone, Debug, PartialEq)] pub struct Parameter { pub value: PgValue, pub type_oid: Option<u32> }` (`Parameter::new(value: PgValue) -> Self`, `Parameter::with_oid(value: PgValue, oid: u32) -> Result<Self, PostgresError>` requiring `oid >= 1`; `Parameter { value: PgValue::Null, type_oid: Some(oid) }` represents a typed SQL `NULL`).
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct PreparedDescription { pub parameter_oids: Vec<u32>, pub result_columns: Vec<Column> }` (captures `ParameterDescription` OIDs and `DescribeStatement` text-format column metadata).
  - `pub fn encode_bind(portal: &str, statement: &str, parameters: &[Parameter], param_oids: &[u32], param_formats: &[Format], result_formats: &[Format], ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>`:
    - Enforces `parameters.len() <= i16::MAX as usize`, `result_formats.len() <= i16::MAX as usize`, `param_oids.len() == parameters.len()`, and `param_formats.len() == parameters.len()`.
    - Rejects any `PgValue::TemporalText { .. }` (at top level or nested inside `PgValue::Array` / `PgValue::Composite`) with `PostgresError::Data` before writing wire bytes in EVERY format, even when `date_style` and `time_zone` match `ctx`.
    - Encodes each parameter via `encode_value_with_catalog`, checking serialized payload length `<= i32::MAX as usize` (`None` / `PgValue::Null` -> `-1_i32`), and writes the `Bind` frontend message via `postgres_protocol::message::frontend::bind`.
  - `pub struct ExtendedQueryCollector`: correlates `ParseComplete`, `ParameterDescription`, `RowDescription` / `NoData` (for both statement `Describe('S')` and portal `Describe('P')`), `BindComplete`, `DataRow`, `CommandComplete` / `EmptyQueryResponse`, `CloseComplete`, and `ReadyForQuery`, validating `ParameterDescription` count and portal `RowDescription` negotiated per-column `Format` codes before invoking row callbacks or materializing cells.
- **`src/rs/vertebra-postgres/src/session.rs`**:
  - `pub(crate) async fn query_with_row_sink<F>(&mut self, sql: &str, params: &[Parameter], result_formats: &[Format], on_columns: impl FnOnce(&[Column]) -> Result<(), PostgresError>, on_row: F) -> Result<String, PostgresError> where F: FnMut(&RowBody<'_>) -> Result<(), PostgresError>`:
    - Resolves required custom parameter metadata before `Parse`, emits the single-group extended exchange (`Parse + Describe('S') + Bind + Describe('P') + Execute("", 0) + Close('P') + Sync`; when `result_formats` is empty `&[]`, negotiates warm formats if cached or requests `[Format::Text]`), invokes `on_columns(&portal_columns)` once before the first `DataRow` (or `&[]` on `NoData`), streams each `DataRow` directly into `on_row(&row_body)` without allocating `Vec<Vec<PgValue>>`, drains boundedly through `ReadyForQuery` (updating `TransactionStatus` even if `on_columns` or `on_row` returns `Err`), and returns the final `command_tag: String` so `ch20:t03` (`execute_select_to_arrow`) can stream directly into Arrow batch builders without editing `extended.rs` or `session.rs` in `ch20`.
  - `pub async fn query(&mut self, sql: &str, parameters: &[Parameter], parameter_oids: Option<&[u32]>) -> Result<QueryResult, PostgresError>` (implemented over `query_with_row_sink` / `ExtendedQueryCollector`):
    1. Validates `parameter_oids` length (`== parameters.len()` when `Some`), rejects conflicting positive `parameter_oids[i]` vs `parameters[i].type_oid` with `PostgresError::Data` (`0` in `parameter_oids` means server inference), and rejects `TemporalText` or embedded NUL in `sql` before I/O.
    2. Calls `metadata::resolve_parameter_metadata` before `Parse` for any custom wrapper needing catalog metadata.
    3. **Cold route** (no warm `PreparedDescription` for `sql`): emits `Parse("", sql, &resolved_oids) + Describe('S', "") + Bind("", "", TEXT param formats, all-TEXT [Format::Text] result formats) + Describe('P', "") + Execute("", 0) + Close('P', "") + Sync` in a single outbound buffer and single flush — with zero implicit `BEGIN` and zero `Query` between `Parse` and `Bind` — collects `OwnedTextResult` through `ReadyForQuery`, and runs `metadata::normalize_materialized_result`.
    4. **Warm route** (when warm/explicit parameter & result OID metadata is supplied, e.g. via `query_with_description` or cached `PreparedDescription` in `ch13:t03`; note that when statement cache capacity is `0`, a fresh unnamed `Parse` is still emitted in every group because local metadata never implies unnamed-statement survival): selects `format_for_oid_with_catalog` independently per parameter and per result column (`Binary` for known codecs, `Text` for unknown custom OIDs), emits one `Parse + Bind + Describe('P') + Execute + Close('P') + Sync` group, and decodes mixed binary/text columns directly into `QueryResult`.
    5. Drains recoverable server `ErrorResponse` (including `42601` multi-command `Parse` rejection and `42P18` indeterminate `NULL` parameter datatype) cleanly through `ReadyForQuery`, updating `TransactionStatus`; protocol framing errors mark the session broken.

## Edge Cases & Error Semantics

- **Typed `NULL` vs untyped ambiguous `NULL`**: `Parameter { value: PgValue::Null, type_oid: Some(23) }` sends OID `23` in `Parse` and length `-1` in `Bind`; untyped `Parameter { value: PgValue::Null, type_oid: None }` sends OID `0` in `Parse`, preserving the server's canonical `SQLSTATE 42P18` (`ProgrammingError`) when PostgreSQL cannot infer the type.
- **Unresolved `TemporalText` bind blocked unconditionally**: Passing `PgValue::TemporalText { kind: TimestampTz, wire_text: "Wed Jul 01 12:00:00 2026 CST", date_style: "Postgres, MDY", time_zone: "Asia/Shanghai" }` fails with `PostgresError::Data` before any `Parse` or `Bind` bytes hit the socket, even when `conn.parameters()` has matching `DateStyle` and `TimeZone`.
- **`VACUUM` & non-transactional commands**: `conn.query("VACUUM", &[], None)` emits no `BEGIN`, executes cleanly in one extended group, and returns `QueryResult { command_tag: "VACUUM", affected_rows: 0, transaction_status: Idle, .. }`.
- **Statement `Describe` vs Portal `Describe` & streaming sink errors**: `Describe('S')` reports `Format::Text` (`0`) column metadata, whereas `Describe('P')` reports the actual negotiated per-column `Format` (`0` or `1`) requested in `Bind`; `ExtendedQueryCollector` / `query_with_row_sink` delivers portal `RowDescription` formats to `on_columns` and drains remaining frames boundedly through `ReadyForQuery` if `on_columns` or `on_row` returns an application/codec `PostgresError::Data`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/extended_protocol.rs` (scripted loopback transcripts + stateful unnamed-statement / backend-rotation peer + row-sink streaming verification).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Cold single-group transcript & `VACUUM` (no `BEGIN`) | `conn.query("VACUUM", &[], None)` and `conn.query("SELECT $1, custom_val FROM t", &[Parameter::with_oid(PgValue::Int4(9), 23)?], None)` on stateful rotating peer | `VACUUM` emits exact `P+D(S)+B+D(P)+E+C(P)+S` (zero `BEGIN`); cold custom query runs one extended group, finishes `ReadyForQuery`, then normalizes `custom_val` via post-completion catalog lookup without re-binding. |
| Warm mixed binary/text per-column & per-parameter formats | Known `INT4` (`23`), `UUID` (`2950`), `vector` (`42001`) + unknown `59999` result column (with fresh unnamed `Parse` under cache capacity `0`) | `Bind` requests `[1, 1, 1, 0]` result formats; `Describe('S')` has format `0` while `Describe('P')` confirms `[1, 1, 1, 0]`; returns typed `Int4`, `Uuid`, `Vector`, and `PgValue::Text` in `Column(type_oid=59999)`. |
| Streaming row-sink execution & mid-stream sink error drain | Streaming extended query over 3 `DataRow` frames, plus mid-stream sink error on row 2 | Delivers negotiated portal `&[Column]` once and each `&RowBody` incrementally; mid-stream sink error drains remaining row + `CommandComplete` + `ReadyForQuery` leaving session reusable at `Idle`. |
| Empty params, typed `NULL`, & ambiguous untyped `NULL` (`42P18`) | `parameters = &[]`; `Parameter { value: Null, type_oid: Some(3802) }`; untyped `Null` (`oid = 0`) rejected by peer with `ErrorResponse(C="42P18")` | Empty and typed `NULL` (`Parse` OID `3802`, `Bind` len `-1`) succeed; untyped `NULL` drains through `ReadyForQuery` and returns `PostgresError::Server` with `sqlstate() == Some("42P18")`. |
| Conflicting `parameter_oids`, `> i16::MAX` params, & multi-statement `Parse` | `Parameter::with_oid(Int4(1), 23)` with `parameter_oids = Some(&[20])`; `32_768` parameters; `"SELECT 1; SELECT 2"` (`ErrorResponse("42601")`) | OID conflict and `> 32767` count fail locally with `PostgresError::Data` before send; multi-statement `Parse` drains cleanly and surfaces `42601`. |
| Unresolved `TemporalText` rejected before socket write | `Parameter::new(PgValue::TemporalText { .. })` with matching session `DateStyle` and `TimeZone` | Returns `Err(PostgresError::Data(_))` with `0` bytes written to peer socket. |
| Decode error vs malformed framing | Corrupted `INT4` payload (`3` bytes) in `DataRow` vs unexpected `DataRow` before `BindComplete` | Cell decode failure drains boundedly through `ReadyForQuery` and returns `PostgresError::Data`; framing violation marks `conn` broken with `PostgresError::Protocol`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test extended_protocol`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test extended_protocol && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `PostgresConnection::query` and `pub(crate)` `PostgresConnection::query_with_row_sink` execute extended `Parse`/`Describe`/`Bind`/`Execute`/`Close`/`Sync` exchanges with validated parameter counts/OIDs, typed `NULL`s, canonical `42P18` / `42601` server error draining, and mixed binary/text column formats matching portal `RowDescription` without materializing `Vec<Vec<PgValue>>` on the streaming sink path (R12).
- [x] Cold queries emit one extended group with zero implicit `BEGIN` (`VACUUM`-compatible) and zero `Parse`->`Query`->`Bind` window, resolving custom parameter metadata before `Parse` and normalizing cold text results after `ReadyForQuery` without SQL replay (R12, R13, R15).
- [x] Every `PgValue::TemporalText` parameter is rejected with `PostgresError::Data` before submission regardless of session `DateStyle`/`TimeZone` (R11, R12).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Extended query single-statement contract, `PgParameter` typed `NULL`s, `TemporalText` bind rejection, and mixed format rules.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL Protocol 3.0 `Parse`/`Describe`/`Bind`/`Execute`/`Sync` framing, portal `RowDescription` formats, and `SQLSTATE 42P18`.
[^pg-bind-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_bind_frame` — Reference PostgreSQL extended protocol message layout in `vertebra-db`.
