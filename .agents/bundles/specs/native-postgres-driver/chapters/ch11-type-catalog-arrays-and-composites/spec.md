---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch11-type-catalog-arrays-and-composites
title: "Chapter 11: Session Type Catalog, Dimensioned Arrays, and Ordered Composite Codecs"
description: Implement session-local PostgreSQL pg_catalog type discovery and format negotiation alongside lossless binary and text codecs for multidimensional arrays and ordered composite records.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch10-temporal-interval-and-json-codecs
replan_trigger: null
current_task: "native-postgres-driver:ch11:t01"
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
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
    title: PgArray, PgComposite, Column OID invariants, and cold-pipeline unknown-OID text fallback contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL pg_catalog metadata queries, unnamed statement invalidation, and container wire layouts
  - id: db-drivers-guide
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Per-session database driver metadata boundaries and FFI ownership rules
tags:
  - postgres
  - rust
  - codecs
  - arrays
  - composites
---

# Chapter 11: Session Type Catalog, Dimensioned Arrays, and Ordered Composite Codecs

<!-- truth: start -->
- **Boundary-Safe Session `TypeCatalog`**: `TypeCatalog` seeds built-in scalar, temporal, JSON, bit, record, and array OIDs and discovers custom domains, arrays, non-dropped composite attributes, and extension types (`vector`, `halfvec`) via `PostgresConnection::simple_query` only when idle between exchanges — never inside receive loops, portals, `COPY`, `Parse`->`Bind` windows, or single-`Sync` pipelines [^pg-contracts] [^pg-research].
- **Dimensioned `PgArray` Fidelity (`0..=6` Rank)**: Binary and text array codecs preserve flat row-major values, exact `dimensions`, arbitrary `i32` `lower_bounds` (`[lower:upper]=`), custom `pg_type.typdelim` (`;` for `box`), `element_oid`, and exact distinctions among SQL `NULL`, `"NULL"`, and `""` [^pg-contracts] [^pg-research].
- **Ordered `PgComposite` & Anonymous `record` (`2249`) Safety**: Named composites reconcile against ordered non-dropped `TypeCatalog` attributes while anonymous binary records preserve transmitted field OIDs with `f1, f2, ...` labels; unknown text records stay `PgValue::Text` at their outer `Column` OID without speculative CSV splitting [^pg-contracts].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

PostgreSQL assigns dynamic OIDs per database to extension types (`vector`, `halfvec`), user-defined domains, custom arrays, and named composites, while encoding multidimensional arrays with explicit per-axis lower bounds (`[lower:upper]=`), type-specific delimiters (`pg_type.typdelim`), and distinct array vs composite text `NULL` grammars [^pg-contracts] [^pg-research]. Furthermore, executing a simple `Query` against `pg_catalog` destroys the backend's unnamed prepared statement and cannot run safely mid-stream or inside a single-`Sync` pipeline [^pg-research]. Chapter 11 implements session-local `TypeCatalog` discovery and format negotiation (`t01`), dimensioned binary arrays (`t02`), escaped multidimensional text arrays with a shared bounded container scanner (`t03`), and ordered binary/text composite codecs (`t04`).

### 1.2 Non-Goals

- Dynamic `pgvector`/`halfvec` and `bit`/`varbit` element payload codecs (`ch12:t01`).
- Pre-`Parse` parameter discovery and post-`ReadyForQuery` cold text result normalization (`ch12:t02`) and extended-query `Parse`/`Bind`/`Execute` (`ch12:t03`).
- Nested Arrow `ListArray` / `StructArray` builders (`ch19:t03`) and Python `PgArray` / `PgComposite` hydration (`ch14:t03`).

### 1.3 Upstream Execution Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface authority: [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Depends on `ch10-temporal-interval-and-json-codecs` (`PgValue`, `CodecContext`, `SimpleQueryCollector`, and scalar/numeric/temporal/JSON codecs).

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Array & Composite Value Contracts | `.agents/bundles/specs/native-postgres-driver/contracts.md` [^pg-contracts] | Specifies `PgArray` (`0..=6` rank, `dimensions`, `lower_bounds`, `element_oid`), `PgComposite` (`type_oid`, `field_names`, `field_oids`, `values`), `record` (`2249`), and unknown-OID text fallback. |
| Catalog & Wire Protocol Probes | `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` [^pg-research] | Confirms `pg_type`/`pg_attribute` (`attnum > 0 AND NOT attisdropped`), unnamed statement destruction on `Query`, `typdelim` `;` on `box`, and array vs composite `NULL` grammars. |
| Adapter Ownership Rules | `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` [^db-drivers-guide] | Keeps catalog caches physical-session-local with zero cross-session global OID assumptions. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).
- Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11, R15 (`ch11:t01`)**: Seed built-in PostgreSQL OIDs and discover transitive array, composite, domain, and extension (`vector`, `halfvec`) metadata per session via `PostgresConnection::simple_query` only at idle exchange boundaries, marking unnamed prepared state invalid on every `Query` and preserving exact text fallback for undiscovered OIDs.
- **R11 (`ch11:t02`)**: Encode and decode binary PostgreSQL arrays (`0..=6` dimensions) preserving `dimensions`, `lower_bounds`, `element_oid`, and `PgValue::Null` elements while guarding pre-allocation dimension products against hostile headers.
- **R11 (`ch11:t03`)**: Decode and encode multidimensional PostgreSQL text arrays (`[lower:upper]={...}`) with `pg_type.typdelim` support, exact `NULL` vs `"NULL"` vs `""` semantics, and a bounded quoted/escaped container scanner shared internally with composite text codecs.
- **R11 (`ch11:t04`)**: Encode and decode binary and text PostgreSQL composites and anonymous binary `record` (`OID 2249`) values, reconciling non-dropped field metadata, enforcing composite empty-unquoted `NULL` rules only when field metadata is known, and rejecting stale `PgComposite` schemas without query replay.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- **Session Type Catalog (`src/rs/vertebra-postgres/src/types.rs`, re-exported from `codecs` and `lib.rs`)**:
  - `pub struct CompositeField { pub name: String, pub type_oid: u32, pub type_modifier: i32 }`
  - `pub enum TypeKind { Scalar, Array { element_oid: u32, delimiter: u8 }, Composite { fields: Vec<CompositeField> }, Domain { base_oid: u32 }, Enum, Range, Extension { schema: String, name: String } }`
  - `pub struct TypeInfo { pub oid: u32, pub schema: String, pub name: String, pub kind: TypeKind, pub delimiter: u8 }`
  - `pub struct TypeCatalog` with `new_with_builtins() -> Self`, `type_info(&self, oid: u32) -> Option<&TypeInfo>`, `lookup_extension_type(&self, type_name: &str) -> Option<&TypeInfo>`, `invalidate(&mut self)`, `invalidate_oid(&mut self, oid: u32)`, `snapshot_for_batch(&self) -> Arc<TypeCatalog>`, and `pub async fn discover(&mut self, conn: &mut PostgresConnection, oids: &[u32], extension_names: &[&str]) -> Result<(), PostgresError>`.
- **Array & Composite Codecs (`src/rs/vertebra-postgres/src/codecs/{array,array_text,composite,mod}.rs`)**:
  - `pub fn format_for_oid_with_catalog(oid: u32, catalog: &TypeCatalog) -> Format`
  - `pub fn decode_value_with_catalog(oid: u32, format: Format, raw: Option<&[u8]>, ctx: &CodecContext, catalog: &TypeCatalog) -> Result<PgValue, PostgresError>`
  - `pub fn encode_value_with_catalog(value: &PgValue, target_oid: u32, format: Format, out: &mut BytesMut, ctx: &CodecContext, catalog: &TypeCatalog) -> Result<bool, PostgresError>`
  - `array.rs`: `decode_array_binary`, `encode_array_binary`.
  - `array_text.rs`: `decode_array_text`, `encode_array_text`, plus `pub(crate) enum ContainerNullRule { ArrayUnquotedNullKeyword, CompositeEmptyUnquoted }` and `pub(crate) fn scan_delimited_container(input: &str, open: u8, close: u8, delimiter: u8, null_rule: ContainerNullRule, expected_arity: Option<usize>, max_depth: usize) -> Result<Vec<ScannedToken>, PostgresError>` (supporting empty unquoted slots before/between/after `,` under `CompositeEmptyUnquoted`, both `\"`/`\\` and doubled-quote `""` unescaping inside quotes, and `0`-vs-`1`-attribute `"()"` disambiguation via `expected_arity`).
  - `composite.rs`: `decode_composite_binary`, `encode_composite_binary`, `decode_composite_text`, `encode_composite_text`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `PostgresConnection` (`session.rs`) owns session-local `TypeCatalog` and `exchange_state` guard -> `TypeCatalog::discover` (`types.rs`) queries `pg_catalog` via `PostgresConnection::simple_query` (`query.rs`, invalidating `unnamed_statement_valid`) -> `SimpleQueryCollector` / future `metadata.rs`, `extended.rs`, `copy`, and `arrow` pass `&TypeCatalog` + `&CodecContext` into `codecs::{decode_value_with_catalog, encode_value_with_catalog}` -> recurse boundedly across `array.rs`, `array_text.rs`, and `composite.rs`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Mid-exchange or pipeline catalog lookup | `TypeCatalog::discover` called while an extended exchange, portal, `COPY`, or pipeline is active | Refuses immediately with `PostgresError::Interface` without writing frames; cold pipelines snapshot current `TypeCatalog` and keep unknown custom OIDs as `PgValue::Text` | Single-`Sync` pipeline & unnamed-statement safety |
| Hostile binary array dimensions | `ndim = 3`, `dims = [65536, 65536, 65536]` with 20-byte payload, or `ndim = 7` | Checked `i32` product & byte-minimum validation rejects with `PostgresError::Data` before `Vec` allocation | Zero OOM from untrusted wire headers |
| Array vs composite text `NULL` rules | Array `{NULL,"NULL",""}` vs known composite `(,"",NULL,"NULL")` | Array yields `[Null, Text("NULL"), Text("")]`; composite yields `[Null, Text(""), Text("NULL"), Text("NULL")]` | Exact PostgreSQL server container grammar |
| Unknown text record & `ALTER TYPE` drift | Undiscovered composite/anonymous `record` (`2249`) in `Format::Text`, or stale `PgComposite` bind after `ALTER TYPE DROP ATTRIBUTE` | Undiscovered text record returns `PgValue::Text` at outer `Column.type_oid`; mismatched bind/wire attribute count invalidates stale OID and fails with `PostgresError::Data` without query replay | Zero guessed CSV parsing or duplicate side effects |

### 2.5 Acceptance Scenarios (Given / When / Then)

- **Given** a session whose `TypeCatalog` discovers a composite type `pair_t` with a dropped middle column (`attisdropped = true`) and a 2D array `[0:1][-2:-1]={{1,NULL},{"a,b",""}}`, **When** `decode_value_with_catalog` processes both binary and text representations, **Then** `PgComposite` retains only the active ordered attributes and `PgArray` preserves `dimensions = [2, 2]`, `lower_bounds = [0, -2]`, and identical element values across both wire formats.
- **Given** an anonymous `ROW(1::int4, 'x'::text)` returned in binary (`OID 2249`) vs text (`OID 2249`), **When** decoded, **Then** binary returns `PgValue::Composite { type_oid: 2249, field_names: ["f1", "f2"], field_oids: [23, 25], .. }` whereas text returns `PgValue::Text("(1,x)")` with `Column.type_oid == 2249`.

### 2.6 Alternatives Considered

| Alternative | Why Rejected | Chosen Seam |
| --- | --- | --- |
| Global process-wide OID cache | Extension and user OIDs differ per database and across `DROP`/`CREATE TYPE` migrations. | Physical-session-local `TypeCatalog` with `Arc<TypeCatalog>` batch snapshots and explicit invalidation. |
| Flattening PostgreSQL arrays into nested Python `list`s on decode | Destroys empty-array element OIDs, 0D vs 1D rank, and non-`1` lower bounds (`[0:2]=`). | Lossless `PgValue::Array` / `PgArray` on all decoded results, keeping rectangular sequences as input bind conveniences. |
| Speculative CSV splitting for unknown `record` (`2249`) text | Cannot distinguish `""` from `NULL` vs typed scalars or nested composites without `field_oids`. | Preserve `PgValue::Text` with `Column.type_oid` whenever composite field metadata is unknown. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch11]` (user docs and OKF data chapters update in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11, R15 (session type catalog & format negotiation) | [t01](tasks/t01-session-type-catalog.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/type_catalog.rs` |
| R11 (dimensioned binary arrays) | [t02](tasks/t02-binary-arrays.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/array_codecs.rs` |
| R11 (escaped multidimensional text arrays) | [t03](tasks/t03-multidimensional-text-arrays.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/array_text.rs` |
| R11 (ordered binary & text composites) | [t04](tasks/t04-composite-codecs.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/composite_codecs.rs`, `src/rs/vertebra-postgres/tests/array_text.rs` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch11:t01 — Implement session-local PostgreSQL type discovery and format negotiation (tasks/t01-session-type-catalog.md)
- [ ] native-postgres-driver:ch11:t02 — Implement dimensioned binary array codecs preserving rank, lower bounds, and element OIDs (tasks/t02-binary-arrays.md)
- [ ] native-postgres-driver:ch11:t03 — Implement escaped multidimensional text array codecs and bounded container scanning (tasks/t03-multidimensional-text-arrays.md)
- [ ] native-postgres-driver:ch11:t04 — Implement typed ordered composite binary and text codecs (tasks/t04-composite-codecs.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch12-pgvector-and-extended-query`, `ch14:t03` (`python_values.rs`), `ch18` (`COPY`), `ch19` (`VTBARROW` nested schemas), and `ch22` (`pgoutput` tuple decoding).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgArray`, `PgComposite`, `Column` OID invariants, and cold-pipeline unknown-OID text fallback contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `pg_catalog` metadata queries, unnamed statement invalidation, and container wire layouts.
[^db-drivers-guide]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Per-session database driver metadata boundaries and FFI ownership rules.
