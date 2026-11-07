---
type: Task
id: "native-postgres-driver:ch19:t01"
chapter_id: ch19-native-arrow-batches-and-shm
title: Define lossless PostgreSQL Arrow schemas over locked workspace Arrow crates
description: Implement PostgresArrowSchema::from_columns in vertebra-postgres over the locked arrow-* (=60.1.0) and half (=2.7.1) dependencies from ch01 with full-range PostgreSQL metadata and recursion guards.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch18:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch19-native-arrow-batches-and-shm/spec.md
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/result.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/arrow.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
estimated_loc: 215
plan_revision: 3
plan_commit: dd432f18
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
    title: Lossless PostgreSQL value, temporal, numeric, vector, and Arrow schema contracts
  - id: rs-workspace
    resource: src/rs/Cargo.toml
    title: Rust workspace dependency versions and locked Arrow 60.1.0 graph
  - id: adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: Optional driver wheel isolation and pure-Rust Arrow dependency rules
tags:
  - postgres
  - rust
  - arrow
  - schema
---

# Task t01: Define Lossless PostgreSQL Arrow Schemas over Locked Workspace Arrow Crates

## Objective & Scope Boundary

Implement `PostgresArrowSchema::from_columns` in `src/rs/vertebra-postgres/src/arrow.rs` (exported via `src/rs/vertebra-postgres/src/lib.rs`) over the pure-Rust `arrow-array`, `arrow-buffer`, `arrow-ipc`, `arrow-schema` (`=60.1.0`, `default-features = false`) and `half = "=2.7.1"` dependencies already declared in `src/rs/vertebra-postgres/Cargo.toml` and locked in `src/rs/Cargo.lock` from `ch01:t01`–`t02`, with zero manifest/lockfile churn, zero duplicate Arrow major version, and zero `arrow-pyarrow` FFI crate [^pg-contracts] [^rs-workspace] [^adr-004]. `RecordBatch` value builders and `VTBARROW` IPC serialization follow in `ch19:t02`–`t04`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 19 lossless Arrow schema, `RecordBatch`, and `VTBARROW` contracts.
- `src/rs/vertebra-postgres/Cargo.toml` — Read-only verification that `arrow-array`, `arrow-buffer`, `arrow-ipc`, `arrow-schema` (`=60.1.0`, `default-features = false`) and `half = "=2.7.1"` are already declared and locked in `src/rs/Cargo.lock` from `ch01:t01`–`t02` [^rs-workspace].
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog`, `TypeInfo`, `TypeKind`, `CompositeField` (ordered non-dropped attributes), domains, enums, `vector`/`halfvec` dynamic OID lookup, and built-in PostgreSQL OID constants (`ch11:t01`).
- `src/rs/vertebra-postgres/src/result.rs` — `crate::result::Column` (`name`, `type_oid`, `format`, `type_modifier`, `table_oid`, `attribute_number`; `PostgresColumn` is Python-only in `src/py/vertebra/db/adapters/postgres/types.py`).
- [contracts.md](../../../contracts.md) — Full-range temporal, canonical numeric, `PgArray`, `PgComposite`, `PgVector`, and `PgBitString` invariants [^pg-contracts].
- [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Standalone wheel isolation rules [^adr-004].

## Symbol & Signature Contracts

- Dependency baseline (`ch01:t01`–`t02`): `arrow-array`, `arrow-buffer`, `arrow-ipc`, `arrow-schema` (`=60.1.0`, `default-features = false`) and `half = "=2.7.1"` are already declared in `src/rs/vertebra-postgres/Cargo.toml` and locked in `src/rs/Cargo.lock`; do NOT edit `Cargo.toml` or `Cargo.lock`.
- `src/rs/vertebra-postgres/src/lib.rs`:
  - Export `pub mod arrow;`.
- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const POSTGRES_ARROW_VERSION_KEY: &str = "postgres.arrow.version";` (value `"1"` on `Schema::metadata`)
  - Field metadata keys: `"postgres.oid"`, `"postgres.typmod"`, `"postgres.type_name"`, `"postgres.encoding"`.
  - `pub const MAX_ARROW_NESTING_DEPTH: usize = 32;`
  - `pub struct PostgresArrowSchema;` with `pub fn from_columns(columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Arc<Schema>, PostgresError>`:
    - Preserves column order, duplicate names, and empty names (`""`) verbatim without `HashMap` deduplication; all fields are nullable (`true`) so empty and all-`NULL` results preserve exact `DataType` and metadata.
    - Scalars: `bool` (`16`) -> `Boolean`; `int2` (`21`) -> `Int16`; `int4` (`23`) -> `Int32`; `int8` (`20`) -> `Int64`; `oid`/`regproc`/`xid`/`cid` (`26`, `24`, `28`, `29`) -> `UInt32`; `float4` (`700`) -> `Float32`; `float8` (`701`) -> `Float64`; `text`/`varchar`/`bpchar`/`name`/`xml`/`unknown` -> `Utf8`; `bytea` (`17`) -> `Binary`; `uuid` (`2950`) -> `FixedSizeBinary(16)`; `json` (`114`) / `jsonb` (`3802`) -> `Utf8` (`"postgres.encoding" = "json"` / `"jsonb"`).
    - `numeric` (`1700`) -> `Utf8` with `"postgres.encoding" = "numeric-decimal"` (retaining canonical decimal digits, trailing scale zeros, and `NaN`/`Infinity`/`-Infinity`; never `Decimal128` or generic `str(value)`).
    - Deliberately PostgreSQL full-range temporal/interval schemas (not standard Arrow temporal arrays):
      - `date` (`1082`) -> `Struct(kind: Int8, value: Int32, wire_text: Utf8, date_style: Utf8, time_zone: Utf8)` (`kind`: `-1` negative infinity, `0` finite PG days since `2000-01-01`, `1` positive infinity, `2` `wire_text` with null `value`).
      - `timestamp` (`1114`) / `timestamptz` (`1184`) -> `Struct(kind: Int8, value: Int64, wire_text: Utf8, date_style: Utf8, time_zone: Utf8)` (`value` in PG microseconds since `2000-01-01`, `wire_text`/`date_style`/`time_zone` nullable `Utf8`).
      - `time` (`1083`) -> `Int64` (`"postgres.encoding" = "pg-time-micros"`, preserving `0..=86_400_000_000` including `24:00:00`).
      - `interval` (`1186`) -> `Struct(months: Int32, days: Int32, microseconds: Int64)`.
    - Nested & extension types:
      - PostgreSQL arrays -> `Struct(dimensions: List<Int32>, lower_bounds: List<Int32>, values: List<element_field>)` (`"postgres.encoding" = "pg-array"`).
      - Known catalog composites -> `Struct` of ordered non-dropped `pg_attribute` fields (`"postgres.encoding" = "pg-composite"`); anonymous `record` (`2249`) without fixed field schema requires text format (`0`) and maps to `Utf8`.
      - `vector` / `halfvec` (resolved via `TypeCatalog` by type name, never hardcoded OID) -> `FixedSizeList<Float32>(dim)` when `type_modifier` encodes positive dimension `1..=16_000`, else `List<Float32>`, with `"postgres.type_name" = "vector" | "halfvec"` and `"postgres.encoding" = "pgvector" | "pghalfvec"`.
      - `bit` (`1560`) / `varbit` (`1562`) -> `Struct(bytes: Binary, bit_length: Int32)` (`"postgres.encoding" = "pg-bits"`).
      - Domains keep resolved base `DataType` while recording the outer domain OID in `"postgres.oid"` (plus `"postgres.base_oid"`); enums map to `Utf8` (`"postgres.encoding" = "pg-enum"`).
      - Unknown OIDs require `column.format == 0` (text) and map to `Utf8` (`"postgres.encoding" = "unknown-text"`).

## Edge Cases & Error Semantics

- Unknown OID with `column.format == 1` (binary) fails immediately with `PostgresError::Data` rather than guessing UTF-8 over binary wire bytes.
- Invalid `vector` / `halfvec` typmod dimension (`<= 0` when present or `> 16_000`) fails with `PostgresError::Data`.
- Recursive catalog resolution tracks active composite/domain/array lineage and current depth: cycles or nesting `> 32` fail closed with `PostgresError::Data` before execution.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` (`schema_*` unit/integration tests)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Duplicate & empty column names + empty/all-NULL preservation | `[Column { name: "id".into(), type_oid: 23, .. }, Column { name: "id".into(), type_oid: 20, .. }, Column { name: "".into(), type_oid: 25, .. }]` | `Schema` has 3 fields `("id": Int32, "id": Int64, "": Utf8)` with `"postgres.arrow.version" == "1"` and exact `"postgres.oid"` metadata. |
| Scalars, `numeric(38, 10)`, `uuid`, `jsonb`, and full-range temporals | Columns for `bool`, `int2/4/8`, `oid`, `float4/8`, `bytea`, `uuid`, `numeric(typmod)`, `jsonb`, `date`, `time`, `timestamp`, `timestamptz`, `interval` | Exact `Boolean`, `Int16/32/64`, `UInt32`, `Float32/64`, `Binary`, `FixedSizeBinary(16)`, `Utf8("numeric-decimal")` with typmod preserved, `Utf8("jsonb")`, `Int64("pg-time-micros")`, and temporal/interval `Struct` child fields. |
| Nested array, ordered composite, domain, enum, `vector(3)`, dynamic `halfvec`, `varbit` | Catalog with composite `(a int4, [dropped], b numeric[])`, domain over `uuid`, enum `mood`, `vector` (`typmod = 3`), `halfvec` (`typmod = -1`), `varbit` | Composite `Struct` omits dropped attribute and nests array `Struct(dimensions, lower_bounds, values)`; domain keeps `FixedSizeBinary(16)` + domain OID; `vector(3)` -> `FixedSizeList<Float32>(3)`; `halfvec` -> `List<Float32>`; `varbit` -> `Struct(bytes: Binary, bit_length: Int32)`. |
| Unknown OID text vs binary format | OID `99_999` with `format = 0` vs `format = 1` | `format = 0` -> `Ok(Utf8)` with `"postgres.oid" = "99999"`; `format = 1` -> `Err(PostgresError::Data)`. |
| Catalog cycle & depth `> 32` guard | Self-referential composite OID cycle and `33`-deep nested array/composite chain | Both return `Err(PostgresError::Data)` without stack overflow. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests schema_`
- Final Green + dependency-tree + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests schema_
  cargo tree --manifest-path src/rs/Cargo.toml -p vertebra-postgres -i arrow-array
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): define lossless Arrow schemas`

## Acceptance Criteria

- [ ] `cargo tree -p vertebra-postgres -i arrow-array` resolves exclusively to `arrow-array v60.1.0` with zero duplicate Arrow major versions or Python FFI Arrow crates (R20).
- [ ] `PostgresArrowSchema::from_columns` produces lossless `postgres.arrow.version = "1"` schemas for every scalar, `numeric`, full-range temporal/interval, array, composite, domain, enum, `vector`/`halfvec`, `bit`/`varbit`, and text-format unknown OID while preserving duplicate/empty column names (R20).
- [ ] Unknown binary OIDs, invalid vector dimensions, catalog cycles, and nesting depths `> 32` fail closed with `PostgresError::Data` before execution (R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Lossless PostgreSQL value, temporal, numeric, vector, and Arrow schema contracts.
[^rs-workspace]: `src/rs/Cargo.toml` — Rust workspace dependency versions and locked Arrow 60.1.0 graph.
[^adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Optional driver wheel isolation and pure-Rust Arrow dependency rules.
