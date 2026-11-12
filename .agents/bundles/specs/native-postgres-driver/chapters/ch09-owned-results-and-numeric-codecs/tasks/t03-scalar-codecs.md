---
type: Task
id: "native-postgres-driver:ch09:t03"
chapter_id: ch09-owned-results-and-numeric-codecs
title: Implement checked built-in scalar binary and text codecs with per-column fallback
description: Build CodecContext, decode_value, encode_value, and format_for_oid in codecs/mod.rs for built-in PostgreSQL scalar types and wire per-column scalar decoding into SimpleQueryCollector.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch09:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch09-owned-results-and-numeric-codecs/spec.md
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/lib.rs
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
files:
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/query.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests:
  - src/rs/vertebra-postgres/tests/scalar_codecs.rs
estimated_loc: 240
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
commit: ef33a380
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
    title: Scalar value mapping, unknown OID text fallback, and null invariants
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: postgres-protocol 0.6.12 scalar codecs and per-column format negotiation
tags:
  - postgres
  - rust
  - codecs
  - scalars
---

# Task t03: Implement Checked Built-In Scalar Binary and Text Codecs with Per-Column Fallback

## Objective & Scope Boundary

Create the central PostgreSQL codec dispatcher (`src/rs/vertebra-postgres/src/codecs/mod.rs`: `CodecContext`, `format_for_oid`, `decode_value`, `encode_value`) covering built-in scalar OIDs (`bool`, `int2`, `int4`, `int8`, `oid`, `float4`, `float8`, `text`, `varchar`, `bpchar`, `name`, `bytea`, `uuid`) in both binary and text wire formats, wire per-column `decode_value` into `SimpleQueryCollector` (`query.rs`), and declare `pub mod codecs;` in `lib.rs` [^pg-contracts] [^pg-research]. `NUMERIC` (`ch09:t04`), temporal/interval/JSON (`ch10`), and container/vector codecs (`ch11`–`ch12`) plug into this dispatcher in subsequent tasks.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Scalar `PostgresValue` fidelity, SQL `NULL` vs empty string/bytes, and unknown-OID text fallback [^pg-contracts].
- [spec.md](../spec.md) — Chapter 09 codec dispatcher signatures and scalar OID table.
- `src/rs/vertebra-postgres/src/result.rs` — `Format`, `Column`, `PgValue`, and `QueryResult`.
- `src/rs/vertebra-postgres/src/query.rs` — `SimpleQueryCollector` cell decoding integration point (`pub mod query;` mounted in `lib.rs` in `ch09:t02`).
- `src/rs/vertebra-postgres/src/lib.rs` — Explicit module exports (`pub mod codecs;`).
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `postgres-protocol =0.6.12` `types` helpers and unknown-OID format rules [^pg-research].

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Built-in OID constants: `BOOL_OID = 16`, `BYTEA_OID = 17`, `NAME_OID = 19`, `INT8_OID = 20`, `INT2_OID = 21`, `INT4_OID = 23`, `TEXT_OID = 25`, `OID_OID = 26`, `FLOAT4_OID = 700`, `FLOAT8_OID = 701`, `BPCHAR_OID = 1042`, `VARCHAR_OID = 1043`, `UUID_OID = 2950`.
  - `#[derive(Clone, Debug, PartialEq, Eq)] pub struct CodecContext { pub date_style: String, pub interval_style: String, pub time_zone: String, pub max_field_bytes: usize, pub max_nesting_depth: usize }` with `CodecContext::from_parameters(params: &HashMap<String, String>) -> Self` (defaulting to `"ISO, MDY"`, `"postgres"`, `"UTC"`, `max_field_bytes = 16 * 1024 * 1024`, `max_nesting_depth = 32`).
  - `pub fn format_for_oid(oid: u32) -> Format`: returns `Format::Binary` for supported built-in scalar OIDs and `Format::Text` for unknown OIDs (never `Binary` for `0` or unregistered OIDs).
  - `pub fn decode_value(oid: u32, format: Format, raw: Option<&[u8]>, ctx: &CodecContext) -> Result<PgValue, PostgresError>`:
    - `None` -> `Ok(PgValue::Null)`.
    - Enforces `raw.len() <= ctx.max_field_bytes` before allocation.
    - Binary decoding via `postgres_protocol::types` checked widths (`bool` exact 1 byte `0`/`1`; `int2` 2B BE; `int4` 4B BE; `int8` 8B BE; `oid` 4B BE `u32` widened losslessly to `PgValue::Int8(u32 as i64)`; `float4` 4B IEEE-754; `float8` 8B IEEE-754; `text`/`varchar`/`bpchar`/`name` checked UTF-8 without embedded NUL; `bytea` owned `Vec<u8>`; `uuid` exact 16B `[u8; 16]`).
    - Text decoding for every scalar OID (`"t"`/`"f"`/`"true"`/`"false"`/`"1"`/`"0"` for `bool`; checked base-10 integer bounds for `int2`/`int4`/`int8`/`oid`; `"NaN"`, `"Infinity"`, `"-Infinity"`, `-0.0` for `float4`/`float8`; both `\\x` hex and legacy PostgreSQL octal `\\NNN` / `\\\\` escape forms for `bytea`; canonical 36-char hyphenated or 32-hex UUIDs).
    - Unknown `oid` in `Format::Text` validates UTF-8 and returns `PgValue::Text(String)` (preserving `Column.type_oid` on the enclosing column); unknown `oid` in `Format::Binary` returns `Err(PostgresError::Data(...))`.
  - `pub fn encode_value(value: &PgValue, target_oid: u32, format: Format, out: &mut BytesMut, ctx: &CodecContext) -> Result<bool, PostgresError>`:
    - `PgValue::Null` -> `Ok(false)`.
    - Rejects `target_oid == 0` in `Format::Binary` (`PostgresError::Data`); rejects embedded `\0` in `PgValue::Text` (`PostgresError::Data`); encodes `bytea` text as canonical `\x<hex>` and UUID text as lowercase hyphenated 36-byte ASCII.
- **`src/rs/vertebra-postgres/src/query.rs` & `src/rs/vertebra-postgres/src/lib.rs`**:
  - `SimpleQueryCollector` builds `CodecContext::from_parameters(conn.parameters())` per result description and decodes each column cell using that `Column`'s exact `type_oid` and `format` (never assuming one global row format).
  - `lib.rs` (which already mounts `pub mod query;` from `ch09:t02`) declares `pub mod codecs;` and re-exports `CodecContext`, `format_for_oid`, `decode_value`, and `encode_value`.

## Edge Cases & Error Semantics

- **Invalid fixed binary widths & bool bytes**: `BOOL` byte `2`, `INT2` of 1 or 3 bytes, `INT4` of 3 bytes, `INT8` of 7 bytes, `FLOAT4` of 8 bytes, or `UUID` of 15/17 bytes returns `Err(PostgresError::Data(_))`.
- **Float signed zero & IEEE specials**: `-0.0_f32` (`0x8000_0000`) and `-0.0_f64` (`0x8000_0000_0000_0000`) preserve negative sign bits (`val.is_sign_negative()`); `NaN`, `+Infinity`, and `-Infinity` round-trip in both binary and text (`"NaN"`, `"Infinity"`, `"-Infinity"`).
- **Malformed `bytea` text**: Odd-length hex `r"\xabc"`, non-hex digits `r"\xzz"`, truncated octal escape `r"\01"`, or out-of-byte octal `r"\777"` fails with `Err(PostgresError::Data(_))`.
- **NUL bytes & UTF-8**: PostgreSQL text columns cannot contain `0x00`; both text `encode_value` and `decode_value` reject embedded NUL bytes and invalid UTF-8 sequences with `PostgresError::Data`, whereas `BYTEA` preserves arbitrary `0x00..=0xFF` bytes including empty `b""` distinct from `PgValue::Null`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/scalar_codecs.rs` (golden PostgreSQL server `*_send` / text fixtures + scripted `PostgresConnection::simple_query` typed scalar rows).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Integer & OID boundary golden bytes | Binary & text `i16::MIN` (`0x8000`), `i32::MAX` (`0x7fff_ffff`), `i64::MIN` (`0x8000_0000_0000_0000`), `OID` `4_000_000_000` (`0xee6b_2800`) | Exact `PgValue::{Int2, Int4, Int8}` values; `int2` text `"40000"` returns `Err(PostgresError::Data)`. |
| IEEE-754 float specials & `-0.0` | `FLOAT4`/`FLOAT8` binary `0x8000_0000` (`-0.0`), `0x7f80_0000` (`+Inf`), `0xff80_0000` (`-Inf`), `0x7fc0_0000` (`NaN`) and text `"Infinity"`, `"-Infinity"`, `"NaN"`, `"-0.0"` | Preserves `is_sign_negative()` on `-0.0`, `is_infinite()`, and `is_nan()` across both formats. |
| `bytea` hex & octal text + empty vs `NULL` | Text `r"\x00ff41"`, `r"a\000\377\\b"`, `r"\x"`, and SQL `NULL` (`None`); malformed `r"\x0g"` | Decodes to `Bytes([0x00, 0xff, 0x41])`, `Bytes([b'a', 0x00, 0xff, b'\\', b'b'])`, `Bytes([])`, and `PgValue::Null`; `r"\x0g"` returns `Err(PostgresError::Data)`. |
| `UUID` binary & text validation | Binary 16B `a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11` and text `"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"` vs 15-byte binary / `"not-a-uuid"` | Valid inputs yield identical `PgValue::Uuid([0xa0, 0xee, ...])`; invalid inputs return `Err(PostgresError::Data)`. |
| Embedded NUL in `TEXT` vs `BYTEA` | `encode_value(&PgValue::Text("ab\0cd".into()), TEXT_OID, ...)` vs `PgValue::Bytes(vec![0, 1])` | `TEXT` rejects NUL with `Err(PostgresError::Data)`; `BYTEA` succeeds. |
| Unknown OID per-column fallback | Column 0 `INT4_OID` (`Format::Binary`, `0x0000_002a`), Column 1 `OID 99999` (`Format::Text`, `b"custom_val"`), Column 2 `OID 99999` (`Format::Binary`, `b"\x01\x02"`) | Columns 0 & 1 decode to `[Int4(42), Text("custom_val")]`; Column 2 binary unknown OID fails with `Err(PostgresError::Data)`. |

## Verification

- Initial Red baseline:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test scalar_codecs`
- Final Green + Regression + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test scalar_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test query_protocol && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] Golden binary and text fixtures for `bool`, `int2`/`int4`/`int8`/`oid`, `float4`/`float8`, `text`/`varchar`/`bpchar`/`name`, `bytea`, and `uuid` round-trip losslessly and reject malformed widths/escapes/NULs (R11).
- [x] `SimpleQueryCollector` dispatches recognized scalar OIDs through `codecs::decode_value` using each column's own `Format` (R11).
- [x] Unknown OIDs negotiate `Format::Text` and remain `PgValue::Text` with `Column.type_oid` intact, while unexpected binary payloads for unknown OIDs fail explicitly with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Scalar value mapping, unknown OID text fallback, and null invariants.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — `postgres-protocol 0.6.12` scalar codecs and per-column format negotiation.
