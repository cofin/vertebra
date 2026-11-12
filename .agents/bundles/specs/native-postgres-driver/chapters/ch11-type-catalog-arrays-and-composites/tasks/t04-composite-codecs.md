---
type: Task
id: "native-postgres-driver:ch11:t04"
chapter_id: ch11-type-catalog-arrays-and-composites
title: Implement typed ordered composite binary and text codecs
description: Encode and decode named PostgreSQL composite types and anonymous binary records (OID 2249) with ordered field metadata, composite NULL rules, and stale-schema invalidation.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch11:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch11-type-catalog-arrays-and-composites/spec.md
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/array_text.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/composite.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/types.rs
tests:
  - src/rs/vertebra-postgres/tests/composite_codecs.rs
estimated_loc: 245
plan_revision: 5
plan_commit: c8722c76
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
    commit: c8722c76
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PgComposite ordered field metadata, anonymous record OID 2249, and unknown text record fallback
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL record_send/record_recv wire framing and record_out/record_in composite quoting rules
tags:
  - postgres
  - rust
  - codecs
  - composites
---

# Task t04: Implement Typed Ordered Composite Binary and Text Codecs

## Objective & Scope Boundary

Implement PostgreSQL `record_send` / `record_recv` and `record_out` / `record_in` codecs in `src/rs/vertebra-postgres/src/codecs/composite.rs`, preserving composite type identity (`type_oid`), ordered non-dropped field names and field OIDs, parent SQL `NULL` vs field `NULL` distinctions, anonymous binary `record` (`OID 2249`) positional names, and explicit `PgValue::Text` fallback for unknown text records [^pg-contracts] [^pg-research]. Python `PgComposite` projection (`python_values.rs`) belongs to `ch14:t03`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgComposite` invariants, anonymous `record` (`OID 2249`) `f1, f2, ...` names, parent `None` vs field `None`, and no-guessed-CSV rule for unknown text records [^pg-contracts].
- [spec.md](../spec.md) — Chapter 11 `composite.rs` signatures and schema-mismatch invalidation rules.
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog`, `TypeKind::Composite { fields }`, `CompositeField`, and `invalidate_oid`.
- `src/rs/vertebra-postgres/src/result.rs` — `PgValue::Composite { type_oid, field_names, field_oids, values }`.
- `src/rs/vertebra-postgres/src/codecs/array_text.rs` — `scan_delimited_container` and `ContainerNullRule::CompositeEmptyUnquoted` from `t03`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Recursive `format_for_oid_with_catalog`, `decode_value_with_catalog`, and `encode_value_with_catalog`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/composite.rs`**:
  - `pub const RECORD_OID: u32 = 2249;`, `pub const RECORD_ARRAY_OID: u32 = 2287;`.
  - `pub fn decode_composite_binary(type_oid: u32, raw: &[u8], ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<PgValue, PostgresError>`:
    - Reads `[i32 nfields BE]` (`>= 0`, checked against remaining `raw.len() >= nfields * 8` before `Vec::with_capacity`) followed by `nfields` entries of `[u32 field_oid BE][i32 field_len BE][bytes]`.
    - For a known named `TypeKind::Composite { fields }`: requires `nfields as usize == fields.len()` and reconciles each `field_oid` against `fields[i].type_oid` (allowing domain-compatible match only when `fields[i].type_oid == field_oid`, otherwise returning `Err(PostgresError::Data(_))` so callers can invalidate stale catalog entries); preserves `fields[i].name` and actual positive `field_oid`, decoding each field via `decode_value_with_catalog(field_oid, Format::Binary, ...)` at `depth + 1`.
    - For anonymous `RECORD_OID` (`2249`): requires every `field_oid >= 1`, assigns deterministic positional names `f1`, `f2`, ..., `f{n}` without guessing from outer `RowDescription`, and decodes each field by its transmitted `field_oid` in `Format::Binary` (rejecting trailing bytes or unsupported binary `field_oid`).
  - `pub fn encode_composite_binary(value: &PgValue, target_oid: u32, out: &mut BytesMut, ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<(), PostgresError>`:
    - Validates `PgValue::Composite` against `TypeCatalog` for `target_oid` (rejecting `RECORD_OID` bind unless `target_oid` is a known concrete composite OID matching `field_names`/`field_oids` count and types; stale/dropped-attribute mismatches fail before send with `PostgresError::Data`), then writes `[i32 nfields BE]` and `[u32 field_oid BE][i32 len BE (-1 for Null)][bytes]`.
  - `pub fn decode_composite_text(type_oid: u32, fields: &[CompositeField], text: &str, ctx: &CodecContext, catalog: &TypeCatalog, depth: usize) -> Result<PgValue, PostgresError>` & `pub fn encode_composite_text(...)`:
    - Uses `scan_delimited_container(text, b'(', b')', b',', ContainerNullRule::CompositeEmptyUnquoted, Some(fields.len()), 1)` (handling `""` -> `Value("")`, leading/inner/trailing empty unquoted slots `(,)` / `(a,)` / `(,b)` -> `Null`, `0`-vs-`1`-attribute `"()"` via `Some(fields.len())`, unquoted `NULL` -> `Value("NULL")`, literal unquoted backslashes `a\b`, and PostgreSQL `""` / `\"` / `\\` double-quote escapes), verifies `tokens.len() == fields.len()`, and dispatches each field via `decode_value_with_catalog(field.type_oid, Format::Text, ...)` at `depth + 1`.
    - `encode_composite_text` writes `(` + comma-separated fields + `)`: empty unquoted slot for `PgValue::Null`, `""` for empty string, and double-quoted field (doubling `"` and `\`) whenever the encoded text is empty or contains `(`, `)`, `,`, `"`, `\`, or ASCII whitespace.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs` & `src/rs/vertebra-postgres/src/types.rs`**:
  - `format_for_oid_with_catalog` returns `Format::Binary` for a known `TypeKind::Composite { fields }` iff every field's `type_oid` supports `Format::Binary` (otherwise `Format::Text`), and returns `Format::Text` for `RECORD_OID` (`2249`) and undiscovered composite OIDs so unknown records stay exact `PgValue::Text` at their outer `Column.type_oid` unless the server already sent binary with known field OIDs.
  - Add helper on `TypeCatalog` (`validate_composite_schema`) used to detect and invalidate stale OIDs on DDL mismatch without implicit query replay.

## Edge Cases & Error Semantics

- **Parent `NULL` vs all-`NULL` composite vs empty string**: Wire `raw == None` -> `PgValue::Null`; text `(,)` on a 2-field `(int4, text)` composite -> `PgValue::Composite { values: [Null, Null], .. }`; text `(,"",NULL)` on `(int4, text, text)` -> `PgValue::Composite { values: [Null, Text(""), Text("NULL")], .. }`.
- **Unknown named or anonymous `record` (`2249`) in text format**: Never split speculatively as CSV; `decode_value_with_catalog(2249, Format::Text, Some(b"(1,a)"), ...)` and undiscovered `type_oid = 49999` in `Format::Text` return `PgValue::Text("(1,a)".into())` while preserving `Column.type_oid`.
- **Schema drift (`ALTER TYPE ... ADD/DROP ATTRIBUTE`) & malformed frames**: Mismatched `nfields`, mismatched `field_oid`, `field_oid == 0`, negative `nfields`, `field_len < -1`, truncated/trailing bytes, or `depth > ctx.max_nesting_depth` fail with `PostgresError::Data` (and never replay an executed statement).

## Public-Seam Test Blueprint

- **Canonical test files**:
  - `src/rs/vertebra-postgres/tests/composite_codecs.rs`
  - `src/rs/vertebra-postgres/tests/array_text.rs` (verifies array-of-composites `person_t[]` round-trips now that `composite.rs` is wired into `codecs/mod.rs`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Named composite with dropped middle attribute, nested array, `JSONB`, & `NUMERIC` | `order_t` (`41050`: `id int4`, *[dropped]*, `tags text[]`, `meta jsonb`, `total numeric`) in both binary and text | Both formats produce identical `PgValue::Composite` with `field_names = ["id", "tags", "meta", "total"]`, `field_oids = [23, 1009, 3802, 1700]`, and lossless nested values. |
| Composite text `NULL` vs `""` vs `"NULL"` & embedded punctuation | `(,"",NULL,"a,b(c)\"d\\e")` on 4-text-field composite (`41051`) | Yields `[Null, Text(""), Text("NULL"), Text("a,b(c)\"d\\e")]` and round-trips cleanly. |
| Parent SQL `NULL` vs composite of `NULL`s | `raw = None` vs binary/text `(,)` on 2-field composite (`41052`) | `None` -> `PgValue::Null`; `(,)` -> `PgValue::Composite { values: [Null, Null], .. }`. |
| Anonymous `record` (`OID 2249`) binary vs text & undiscovered composite text | `2249` binary `[2 fields: (23, 7), (25, "ok")]` vs `2249` text `"(7,ok)"` vs unknown `49999` text `"(7,ok)"` | `2249` binary -> `Composite { type_oid: 2249, field_names: ["f1", "f2"], field_oids: [23, 25], values: [Int4(7), Text("ok")] }`; both text cases -> `PgValue::Text("(7,ok)")`. |
| Nested `composite[]` & `composite` inside `composite` | `person_t[]` (`{"(1,\"a,b\")",NULL,"(,)" }`) in text and binary | Decodes `PgValue::Array` containing `[Composite(..), Null, Composite([Null, Null])]`. |
| Stale schema bind, mismatched `field_oid`, trailing bytes, & deep recursion | Bind 2-field `PgComposite` after catalog updated to 3 fields; wire binary with wrong `field_oid` or trailing byte `0x00`; 40-deep nested record | All fail with `Err(PostgresError::Data(_))`; catalog invalidation test confirms stale entry is cleared without query replay. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test composite_codecs`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test composite_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test array_text && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] Named composites preserve `type_oid`, ordered non-dropped `field_names`/`field_oids`, and exact parent `NULL` vs field `NULL` vs `""` distinctions across binary and text formats, including nested arrays and composites (R11).
- [x] Anonymous binary `record` (`OID 2249`) retains transmitted `field_oids` with `f1, f2, ...` names, while unknown named or anonymous text records stay `PgValue::Text` at their outer `Column.type_oid` without guessed CSV parsing (R11).
- [x] Stale or mismatched `PgComposite` schemas, invalid field counts/OIDs/lengths, trailing bytes, and excessive recursion fail explicitly with `PostgresError::Data` without replaying queries (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgComposite` ordered field metadata, anonymous `record` `OID 2249`, and unknown text record fallback.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `record_send`/`record_recv` wire framing and `record_out`/`record_in` composite quoting rules.
