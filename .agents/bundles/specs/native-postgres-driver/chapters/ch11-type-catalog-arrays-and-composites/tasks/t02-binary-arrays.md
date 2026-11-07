---
type: Task
id: "native-postgres-driver:ch11:t02"
chapter_id: ch11-type-catalog-arrays-and-composites
title: Implement dimensioned binary array codecs preserving rank, lower bounds, and element OIDs
description: Encode and decode PostgreSQL binary arrays (rank 0..=6) in codecs/array.rs with checked dimension products, arbitrary i32 lower bounds, element OID validation, and recursive value dispatch.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch11:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch11-type-catalog-arrays-and-composites/spec.md
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/array.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/array_codecs.rs
estimated_loc: 235
plan_revision: 5
plan_commit: 399e068c
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
    title: PgArray rank 0..=6, dimensions, lower_bounds, element_oid, and convenience sequence bind rules
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL array_send/array_recv binary header layout and MAXDIM=6 bounds
tags:
  - postgres
  - rust
  - codecs
  - arrays
---

# Task t02: Implement Dimensioned Binary Array Codecs Preserving Rank, Lower Bounds, and Element OIDs

## Objective & Scope Boundary

Implement PostgreSQL `array_send` / `array_recv` binary codecs in `src/rs/vertebra-postgres/src/codecs/array.rs`, register all known and `TypeCatalog`-discovered array OIDs in `codecs/mod.rs`, and re-export `types` (`TypeCatalog`, `TypeInfo`, `TypeKind`, `CompositeField`) at the `vertebra-postgres` crate root (`lib.rs`) [^pg-contracts] [^pg-research]. Multidimensional text array parsing/escaping (`t03`) and composite element codecs (`t04`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgArray` rank `0..=6`, row-major flat values, `lower_bounds` + `dimensions` `i32` invariants, and convenience sequence bind rules [^pg-contracts].
- [spec.md](../spec.md) — Chapter 11 binary array signatures and hostile-header bounds.
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog`, `TypeKind::Array { element_oid, delimiter }`, and `resolve_domain_base`.
- `src/rs/vertebra-postgres/src/result.rs` — `PgValue::Array { values, dimensions, lower_bounds, element_oid }`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext`, `format_for_oid_with_catalog`, `decode_value_with_catalog`, and `encode_value_with_catalog`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/array.rs`**:
  - `pub const MAX_ARRAY_DIMS: usize = 6;`
  - `pub fn decode_array_binary(array_oid: u32, expected_element_oid: u32, raw: &[u8], ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<PgValue, PostgresError>`:
    - Parses header `[i32 ndim BE][i32 flags BE][u32 wire_element_oid BE]` (`12` bytes minimum).
    - Enforces `0 <= ndim <= 6`, `flags == 0 || flags == 1` (`has_null`), and `wire_element_oid == expected_element_oid` (or domain-resolved base OID matching `TypeCatalog`).
    - When `ndim == 0`: requires exact `12`-byte payload (`raw.len() == 12`) and returns `PgValue::Array { values: vec![], dimensions: vec![], lower_bounds: vec![], element_oid: wire_element_oid }`.
    - When `1 <= ndim <= 6`: reads `ndim` pairs of `[i32 dim BE][i32 lbound BE]`, requires `dim >= 1` and `lbound.checked_add(dim - 1).is_some()`, computes checked `usize` element product, verifies remaining wire bytes `>= product * 4` before `Vec::with_capacity(product)`, and decodes each `[i32 elem_len BE]` (`-1` -> `PgValue::Null` only if `flags == 1`; `>= 0` -> slice of exact `elem_len` passed to `decode_value_with_catalog(..., Format::Binary, ...)` at `depth + 1`), rejecting any trailing bytes or nested codec failure with contextual path `array[idx]` diagnostics.
  - `pub fn encode_array_binary(value: &PgValue, target_array_oid: u32, expected_element_oid: u32, out: &mut BytesMut, ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<(), PostgresError>` and `pub fn normalize_convenience_array(...) -> Result<PgValue, PostgresError>`:
    - Validates `PgValue::Array` (`dimensions.len() == lower_bounds.len() <= 6`, `ndim == 0` iff `values.is_empty()`, positive `dimensions`, checked `i32` upper bounds, `product == values.len()`, `element_oid == expected_element_oid`, and binary support for `expected_element_oid`).
    - Sets `flags = 1` iff any element is `PgValue::Null`, writes `[ndim][flags][element_oid]`, `[dim][lbound]` pairs, and length-prefixed binary element payloads (`-1_i32` for `PgValue::Null`), rejecting any element payload `> i32::MAX` or unsupported binary element OID (never silently coercing unsupported binary bytes to text).
    - Helper `normalize_convenience_array` converts rectangular nested convenience sequences into canonical `PgValue::Array` with `lower_bounds = vec![1; rank]` when `expected_element_oid != 0`, rejecting ragged shapes, nested composites masquerading as arrays, and untyped-empty / all-`Null` ambiguity without an explicit target array OID.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs` & `src/rs/vertebra-postgres/src/lib.rs`**:
  - `format_for_oid_with_catalog` returns `Format::Binary` for `TypeKind::Array { element_oid, .. }` iff `format_for_oid_with_catalog(element_oid, catalog) == Format::Binary` (otherwise `Format::Text`).
  - `lib.rs` exports `pub use session::types;` and `pub use session::types::{CompositeField, TypeCatalog, TypeInfo, TypeKind};`.

## Edge Cases & Error Semantics

- **Pre-allocation DoS guard**: A crafted 20-byte header claiming `ndim = 3` and `dims = [10_000, 10_000, 10_000]` fails the minimum-remaining-bytes check (`product * 4 > remaining_bytes`) with `PostgresError::Data` before allocating element storage.
- **Rank `0` vs SQL `NULL` vs array containing `NULL`**: SQL `NULL` array cell (`raw == None`) decodes to `PgValue::Null`; binary `{}` (`ndim = 0`) decodes to `PgValue::Array { values: [], dimensions: [], lower_bounds: [], element_oid }`; `{NULL}` (`ndim = 1, dim = 1, lbound = 1, flags = 1, elem_len = -1`) decodes to `PgValue::Array { values: [PgValue::Null], dimensions: [1], lower_bounds: [1], element_oid }`.
- **Invalid headers & flags**: `ndim < 0`, `ndim > 6`, `dim <= 0` when `ndim > 0`, `lbound + dim - 1` overflowing `i32`, `flags` outside `{0, 1}`, `elem_len == -1` when `flags == 0`, `elem_len < -1`, mismatched `wire_element_oid`, truncated element bytes, trailing bytes, or recursion `> ctx.max_nesting_depth` returns `Err(PostgresError::Data(_))`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/array_codecs.rs` (independent binary wire fixtures covering `0D`/`1D`/`2D`/`3D` arrays, domains, and adversarial headers).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `0D` empty, `1D` nullable, & `2D`/`3D` negative/non-one `lower_bounds` | `0D` `_uuid` (`2951`); `2D` `_int4` (`1007`) with `dims=[2, 3], lbounds=[-1, 5]` and `NULL` cell; `_numeric`, `_timestamptz`, `_jsonb` | Exact binary round-trip preserving `dimensions`, `lower_bounds`, `element_oid`, `PgNumeric` `dscale`, and `PgValue::Null` vs non-null elements. |
| SQL `NULL` parent vs empty `0D` array vs `[NULL]` | `raw = None` vs `ndim = 0` vs `ndim = 1, elem = -1` on `_text` (`1009`) | Yields `PgValue::Null`, empty `PgValue::Array` (`rank = 0`), and `1`-element `PgValue::Array` (`[PgValue::Null]`) respectively. |
| Convenience sequence normalization | Rectangular `[[1, 2], [3, 4]]` with target `_int4` (`1007`) vs ragged `[[1, 2], [3]]` or untyped `[]` (`oid = 0`) | Rectangular succeeds (`dims=[2, 2], lbounds=[1, 1], element_oid=23`); ragged and untyped empty/all-null fail with `Err(PostgresError::Data(_))`. |
| Hostile header counts & `i32` bound overflow | `dims=[65536, 65536, 65536]` on 36-byte buffer; `ndim = 7`; `lbound = i32::MAX, dim = 2`; `flags = 2`; `elem_len = -1` with `flags = 0` | All five return `Err(PostgresError::Data(_))` without large allocation or panic. |
| Nested element codec error path & domain array | Corrupted second `UUID` (15 bytes) in `2x2` `_uuid` array; array over custom domain `pos_int[]` | Invalid UUID fails with `PostgresError::Data` naming index path (`[0][1]` / element `1`); domain array resolves base element codec cleanly. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_codecs`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Binary array encode/decode preserves `0..=6` dimensions, negative/non-one `lower_bounds`, `PgValue::Null` elements, and `element_oid` across all supported scalar, numeric, temporal, JSON, and domain element types (R11).
- [ ] Convenience sequence normalization infers `1`-based rectangular dimensions when a target array OID is known and rejects ragged or untyped-empty/all-null inputs with `PostgresError::Data` (R11).
- [ ] Hostile dimension counts, `i32` bound overflows, invalid `has_null` flags, mismatched element OIDs, and truncated/trailing bytes fail before unbounded allocation (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgArray` rank `0..=6`, `dimensions`, `lower_bounds`, `element_oid`, and convenience sequence bind rules.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `array_send`/`array_recv` binary header layout and `MAXDIM=6` bounds.
