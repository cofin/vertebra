---
type: Task
id: "native-postgres-driver:ch11:t01"
chapter_id: ch11-type-catalog-arrays-and-composites
title: Implement session-local PostgreSQL type discovery and format negotiation
description: Seed built-in PostgreSQL OIDs and resolve transitive array, composite, domain, and extension metadata per physical session via PostgresConnection::simple_query at idle boundaries.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch10:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch11-type-catalog-arrays-and-composites/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/session.rs
tests:
  - src/rs/vertebra-postgres/tests/type_catalog.rs
estimated_loc: 240
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
    title: Column OID invariants, extension vector/halfvec resolution, and cold-pipeline text fallback rules
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL pg_catalog metadata queries, attisdropped filtering, and unnamed statement invalidation
tags:
  - postgres
  - rust
  - catalog
  - codecs
---

# Task t01: Implement Session-Local PostgreSQL Type Discovery and Format Negotiation

## Objective & Scope Boundary

Implement `TypeCatalog`, `TypeInfo`, `TypeKind`, and `CompositeField` in `src/rs/vertebra-postgres/src/types.rs`, attach a session-local `TypeCatalog` to `PostgresConnection` (`session.rs`), and extend `codecs/mod.rs` with catalog-aware format negotiation and domain/fallback dispatch [^pg-contracts] [^pg-research]. Binary/text array codecs (`t02`–`t03`), composite wire codecs (`t04`), `pgvector`/`bit` codecs (`ch12:t01`), and cold-query pre-`Parse` / post-`ReadyForQuery` orchestration (`ch12:t02`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Positive OID invariants, dynamic `vector`/`halfvec` lookup by extension identity, unknown-OID text fallback, and single-`Sync` cold pipeline rules [^pg-contracts].
- [spec.md](../spec.md) — Chapter 11 `TypeCatalog` signatures and boundary constraints.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `pg_type`/`pg_attribute`/`pg_extension` catalog layout and simple-`Query` unnamed statement destruction [^pg-research].
- `src/rs/vertebra-postgres/src/session.rs` — `PostgresConnection`, `unnamed_statement_valid`, and connection generation tracking.
- `src/rs/vertebra-postgres/src/query.rs` — `PostgresConnection::simple_query` and `SimpleQueryCollector`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Built-in scalar/numeric/temporal/JSON OID registry and `format_for_oid` / `decode_value` / `encode_value`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/types.rs` (mounted via `#[path = "types.rs"] pub mod types;` in `session.rs`, re-exported from `codecs/mod.rs` now and at crate root `lib.rs` in `t02`)**:
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct CompositeField { pub name: String, pub type_oid: u32, pub type_modifier: i32 }`.
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub enum TypeKind { Scalar, Array { element_oid: u32, delimiter: u8 }, Composite { fields: Vec<CompositeField> }, Domain { base_oid: u32 }, Enum, Range, Extension { schema: String, name: String } }`.
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct TypeInfo { pub oid: u32, pub schema: String, pub name: String, pub kind: TypeKind, pub delimiter: u8 }`.
  - `pub struct TypeCatalog` pre-seeded (`TypeCatalog::new_with_builtins()`) with all built-in scalar/numeric/temporal/JSON/bit/record OIDs (`16`, `17`, `19`, `20`, `21`, `23`, `25`, `26`, `114`, `700`, `701`, `1042`, `1043`, `1082`, `1083`, `1114`, `1184`, `1186`, `1560`, `1562`, `1700`, `2249`, `2950`, `3802`) and their built-in array counterparts (`1000`, `1001`, `1002`, `1005`, `1007`, `1009`, `1028`, `199`, `1021`, `1022`, `1014`, `1015`, `1182`, `1183`, `1115`, `1185`, `1187`, `1561`, `1563`, `1231`, `2287`, `2951`, `3807`, plus `603` `box` / `1020` `_box` with delimiter `b';'`).
  - Methods on `TypeCatalog`:
    - `pub fn type_info(&self, oid: u32) -> Option<&TypeInfo>`
    - `pub fn resolve_domain_base(&self, oid: u32) -> Result<u32, PostgresError>` (detects cycles and caps chain depth `<= 16`)
    - `pub fn lookup_extension_type(&self, ext_type_name: &str) -> Option<&TypeInfo>` (namespace-aware `pg_extension` identity for `"vector"` and `"halfvec"`, tracks `search_path`/generation)
    - `pub fn invalidate(&mut self)` & `pub fn invalidate_oid(&mut self, oid: u32)`
    - `pub fn snapshot_for_batch(&self) -> Arc<TypeCatalog>`
    - `pub async fn discover(&mut self, conn: &mut PostgresConnection, missing_oids: &[u32], extension_names: &[&str]) -> Result<(), PostgresError>`: queries `pg_catalog.pg_type`, `pg_catalog.pg_namespace`, `pg_catalog.pg_attribute` (`attnum > 0 AND NOT attisdropped ORDER BY attnum`), `pg_catalog.pg_class`, and `pg_catalog.pg_extension` via `conn.simple_query(...)` with bounded rounds (`<= 8` transitive hops, `<= 1024` total catalog rows per discovery) to resolve transitive element/attribute/domain base OIDs. Interpolates ONLY validated positive `u32` decimal literals and fixed allowlisted extension type names (`"vector"`, `"halfvec"`); NEVER concatenates arbitrary SQL strings or identifiers.
- **`src/rs/vertebra-postgres/src/session.rs` & `src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - `PostgresConnection` owns `type_catalog: TypeCatalog`, `unnamed_statement_valid: bool`, and an active-exchange guard (`Idle` vs `InExtendedExchange` / `PortalActive` / `CopyActive` / `PipelineActive`); `TypeCatalog::discover` refuses with `PostgresError::Interface` whenever `conn` is not at a clean idle command boundary, and every `simple_query` marks `unnamed_statement_valid = false`. Reconnect/new session starts with a fresh `TypeCatalog` generation; `ParameterStatus("search_path", ...)` invalidates unqualified extension name bindings while retaining verified OID entries.
  - `codecs/mod.rs` re-exports `{CompositeField, TypeCatalog, TypeInfo, TypeKind}` and provides `format_for_oid_with_catalog`, `decode_value_with_catalog`, and `encode_value_with_catalog`: domains delegate to their resolved base OID while preserving outer `Column.type_oid`, enums decode/encode UTF-8 labels (`Format::Text` or UTF-8 binary), and unknown/undiscovered custom OIDs return `Format::Text` and `PgValue::Text` without speculative binary decoding.

## Edge Cases & Error Semantics

- **Forbidden discovery windows**: Attempting `discover` while an extended query (`Parse` before `ReadyForQuery`), portal, `COPY`, or pipeline batch is active fails immediately with `PostgresError::Interface` and emits zero wire bytes. Cold DDL-dependent pipelines use `snapshot_for_batch()` and leave undiscovered OIDs as `Format::Text` / `PgValue::Text`.
- **Dropped composite attributes (`ALTER TYPE ... DROP ATTRIBUTE`)**: Catalog rows with `attisdropped = true` or `attnum <= 0` are excluded; surviving attributes preserve exact `attnum` order, `attname`, `atttypid`, and `atttypmod`.
- **Recursive / cyclic types & hostile catalog sizes**: Self-referential composites/arrays or domain cycles terminate via visited-OID tracking and depth/row caps (`> 16` hops or `> 1024` catalog rows -> `PostgresError::Data` / `Protocol`), preventing infinite discovery loops or unbounded memory growth.
- **Invalid OIDs & schema drift**: `0` in `missing_oids` is ignored/rejected before SQL emission; schema mismatch during decode/encode invalidates only the stale OID entry (`invalidate_oid`) and returns `PostgresError::Data` without replaying user SQL.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/type_catalog.rs` (loopback scripted-peer server fixtures testing `TypeCatalog`, `PostgresConnection`, and `format_for_oid_with_catalog` / `decode_value_with_catalog`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Built-in scalar/array lookup with zero I/O | `catalog.type_info(23)`, `catalog.type_info(1007)`, `catalog.type_info(1020)` (`_box`) | Resolves immediately with zero `simple_query` frames; `_int4` has `element_oid = 23, delimiter = b','`, `_box` has `delimiter = b';'`. |
| Transitive custom domain, array, composite (with dropped column), & `pgvector` discovery | `discover(&mut conn, &[41002], &["vector", "halfvec"])` against peer returning composite `41002` -> dropped col + domain `41001` -> `_numeric` `1231`, plus `pg_extension` `vector` (`42001`) and `halfvec` (`42002`) | Resolves all transitive types in bounded `simple_query` rounds, drops `attisdropped` attribute, binds `lookup_extension_type("vector") -> 42001`, and marks `conn.unnamed_statement_valid() == false`. |
| Domain format negotiation & outer `Column` OID | Domain `pos_int` (`41010` over `INT4` `23`) decoded via `decode_value_with_catalog(41010, Format::Binary, Some(&42_i32.to_be_bytes()), ...)` | Negotiates `Format::Binary` and decodes `PgValue::Int4(42)` while `Column.type_oid` remains `41010`. |
| Unknown OID fallback & cold pipeline snapshot | Unregistered OID `99999` in `snapshot_for_batch()` | `format_for_oid_with_catalog(99999, &snap) == Format::Text`; `decode_value_with_catalog` returns `PgValue::Text("raw".into())` with zero catalog queries; `Format::Binary` returns `Err(PostgresError::Data(_))`. |
| Discovery blocked during non-idle state & hostile catalog cap | Call `discover` while session exchange guard is non-idle, or peer returns cyclic domain (`50001 -> 50002 -> 50001`) / `> 1024` rows | Non-idle returns `Err(PostgresError::Interface(_))` with zero wire writes; cyclic/oversized catalog returns `Err(PostgresError::Data(_))`. |
| DDL invalidation & reconnect isolation | `catalog.invalidate_oid(41002)` after `ALTER TYPE`, or open second `PostgresConnection` | Invalidated OID re-discovers updated field list; second connection has an independent empty custom cache. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test type_catalog`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test type_catalog && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `TypeCatalog` resolves built-ins, transitive custom arrays, non-dropped composite attributes, domains, and extension `vector`/`halfvec` OIDs under bounded `simple_query` discovery using only validated `u32` literals (R11, R15).
- [ ] Catalog discovery is refused whenever an extended exchange, portal, `COPY`, or single-`Sync` pipeline is active, and every catalog `Query` invalidates local unnamed prepared-statement tracking (R11, R15).
- [ ] Undiscovered custom OIDs negotiate `Format::Text` and preserve owned `PgValue::Text` without speculative binary decoding, and invalidation remains physical-session-local (R11, R15).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `Column` OID invariants, extension `vector`/`halfvec` resolution, and cold-pipeline text fallback rules.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `pg_catalog` metadata queries, `attisdropped` filtering, and unnamed statement invalidation.
