---
type: Task
id: "native-postgres-driver:ch10:t04"
chapter_id: ch10-temporal-interval-and-json-codecs
title: Implement strict JSON and version-validated JSONB binary and text codecs
description: Encode and decode PostgreSQL JSON (OID 114) and JSONB (OID 3802, version 0x01) values, preserving large integer precision and distinguishing JSON null from SQL NULL.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch10:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch10-temporal-interval-and-json-codecs/spec.md
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/json.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/json_codecs.rs
estimated_loc: 205
plan_revision: 5
plan_commit: 73c8a6ff
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
commit: d3bb0d14
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
    title: JSON and JSONB OIDs, PgJson(None) vs SQL NULL, and integer precision contracts
  - id: pg-jsonb-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::strip_pg_jsonb_header
    title: Shared strip_pg_jsonb_header and encode_pg_jsonb helpers in vertebra-db
tags:
  - postgres
  - rust
  - codecs
  - json
  - jsonb
---

# Task t04: Implement Strict `JSON` and Version-Validated `JSONB` Binary and Text Codecs

## Objective & Scope Boundary

Implement PostgreSQL `JSON` (`OID 114`) and `JSONB` (`OID 3802`) binary and text codecs in `src/rs/vertebra-postgres/src/codecs/json.rs`, validating the mandatory 1-byte `0x01` `JSONB` binary version marker via `vertebra_db::postgres::{strip_pg_jsonb_header, encode_pg_jsonb}`, enforcing strict RFC 8259 UTF-8 JSON syntax without rounding numbers through `f64`, and keeping JSON `null` (`PgValue::Json(b"null")`) distinct from SQL `NULL` (`PgValue::Null`) [^pg-contracts] [^pg-jsonb-rs]. Python `msgspec.json` hydration into `PgJson(None)` / Python primitives belongs to `ch14:t03`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `JSON` (`114`) vs `JSONB` (`3802`), `PgJson(None)` vs `PgParameter(None, 3802)`, `PgJson("null")` string distinction, and large integer preservation [^pg-contracts].
- [spec.md](../spec.md) — Chapter 10 `json.rs` contracts and edge cases.
- `src/rs/vertebra-db/src/postgres/mod.rs::{strip_pg_jsonb_header,encode_pg_jsonb}` — Existing `0x01` binary `JSONB` header helpers [^pg-jsonb-rs].
- `src/rs/vertebra-postgres/src/result.rs` — `PgValue::Json(Vec<u8>)` and `PgValue::Null`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext` (`max_nesting_depth`, `max_field_bytes`), `format_for_oid`, `decode_value`, and `encode_value`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/json.rs`**:
  - OID constants: `pub const JSON_OID: u32 = 114;`, `pub const JSONB_OID: u32 = 3802;`.
  - `pub fn validate_json_bytes(raw: &[u8], max_depth: usize) -> Result<&str, PostgresError>`: checks valid UTF-8, no embedded `\0` escape inside `JSONB` when prohibited or invalid surrogate halves (`\uD800` without low surrogate), strict RFC 8259 grammar (objects, arrays, strings, `true`/`false`/`null`, and JSON numbers `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` validated lexically without parsing into `f64` or `i64`), no trailing tokens, and container nesting depth `<= max_depth`.
  - `pub fn decode_json_binary(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError>` & `pub fn decode_json_text(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError>`: validates `raw` via `validate_json_bytes` and returns owned `raw.to_vec()`.
  - `pub fn decode_jsonb_binary(raw: &[u8], ctx: &CodecContext) -> Result<Vec<u8>, PostgresError>`: strips the mandatory `0x01` version byte via `vertebra_db::postgres::strip_pg_jsonb_header` (mapping empty payload or version `!= 0x01` to `PostgresError::Data`), validates the remaining UTF-8 JSON slice via `validate_json_bytes`, and returns owned JSON payload bytes (without the `0x01` header).
  - `pub fn encode_json_binary(json: &[u8], out: &mut BytesMut, ctx: &CodecContext) -> Result<(), PostgresError>` & `pub fn encode_jsonb_binary(json: &[u8], out: &mut BytesMut, ctx: &CodecContext) -> Result<(), PostgresError>`: validates `json` via `validate_json_bytes`; `JSON` writes raw UTF-8 JSON, while `JSONB` prepends `0x01` (matching `encode_pg_jsonb` byte-for-byte). Text encoding for both `114` and `3802` writes validated UTF-8 JSON without a `0x01` prefix.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Declare `pub mod json;`, return `Format::Binary` in `format_for_oid` for `JSON_OID` (`114`) and `JSONB_OID` (`3802`), and wire `PgValue::Json` into `decode_value` and `encode_value` (never coercing `PgValue::Text("null")` into JSON `null` unless wrapped in `PgValue::Json(b"\"null\"".to_vec())`).

## Edge Cases & Error Semantics

- **SQL `NULL` vs JSON `null` vs JSON string `"null"`**:
  - Wire `raw == None` (`len == -1`) -> `PgValue::Null`.
  - `JSONB` binary `b"\x01null"` or text `b"null"` -> `PgValue::Json(b"null".to_vec())`.
  - `JSONB` binary `b"\x01\"null\""` -> `PgValue::Json(b"\"null\"".to_vec())`.
  - All three remain distinct in `PgValue` and round-trip to their exact wire representations.
- **64-bit / arbitrary-precision JSON integers**: `{"id": 18446744073709551615, "big": 99999999999999999999999999999}` preserves every digit verbatim in `PgValue::Json(Vec<u8>)` without rounding to `18446744073709551616.0`.
- **Invalid `JSONB` headers & malformed JSON**: Empty `b""` `JSONB` binary, `b"\x00{}"` or `b"\x02{}"` unknown version bytes, `b"\x01"` (version byte with zero JSON payload), `NaN`/`Infinity` literals, leading-zero numbers (`01`), trailing commas (`{"a":1,}`), lone UTF-16 surrogates (`"\uD800"`), invalid UTF-8 (`b"\x01\xff"`), or nesting deeper than `ctx.max_nesting_depth` return `Err(PostgresError::Data(_))`.
- **Fragmented TCP delivery**: Scripted loopback `simple_query` / protocol delivery split 1 byte at a time across a multi-KiB Unicode `JSONB` / `JSON` `DataRow` reassembles the full frame before `decode_value` runs.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/json_codecs.rs` (golden `JSON`/`JSONB` binary & text fixtures + bytewise-fragmented `PostgresConnection::simple_query` loopback delivery).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SQL `NULL` vs JSON `null` vs `"null"` | `JSONB` `None`, `b"\x01null"`, `b"\x01\"null\""`, `b"\x01[null,{},[]]"` | Decodes to `PgValue::Null`, `Json(b"null")`, `Json(b"\"null\"")`, and `Json(b"[null,{},[]]")`; re-encodes with `0x01` prefix on `3802` and raw UTF-8 on `114`. |
| Integer precision beyond `f64` (`> 2^53`) & Unicode escapes | `b"\x01{\"u64\":18446744073709551615,\"s\":\"\\uD83D\\uDE80 \\\"ok\\\"\"}"` | Preserves exact bytes `18446744073709551615` and valid surrogate pair without `f64` loss. |
| Invalid `JSONB` version & truncated payloads | `b""`, `b"\x01"`, `b"\x00{}"`, `b"\x02{\"a\":1}"` on `JSONB_OID` (`Format::Binary`) | All four return `Err(PostgresError::Data(_))`. |
| Malformed RFC 8259 JSON & excessive nesting | `b"{\"a\":01}"`, `b"{\"a\":NaN}"`, `b"[1,]"`, `b"\"\\uD800\""`, or `[` repeated `64` times (`max_nesting_depth = 32`) | Returns `Err(PostgresError::Data(_))`. |
| Bytewise-fragmented `DataRow` over loopback socket | Scripted server dribbling 1 byte per write for a `RowDescription(JSON_OID, JSONB_OID)` + `DataRow` | `PostgresConnection::simple_query` returns intact `PgValue::Json` cells. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test json_codecs`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test json_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] `JSON` (`OID 114`) and `JSONB` (`OID 3802`, `0x01` binary header) validate UTF-8 RFC 8259 JSON in both binary and text formats while keeping unknown extension OIDs as `PgValue::Text` (R11).
- [x] SQL `NULL`, top-level JSON `null`, and JSON string `"null"` remain distinct in `PgValue`, and integers exceeding `2^53` suffer zero `f64` precision loss (R11).
- [x] Empty/truncated `JSONB`, non-`0x01` version markers, invalid UTF-8, and malformed JSON fail explicitly with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `JSON` and `JSONB` OIDs, `PgJson(None)` vs SQL `NULL`, and integer precision contracts.
[^pg-jsonb-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::strip_pg_jsonb_header` — Shared `strip_pg_jsonb_header` and `encode_pg_jsonb` helpers in `vertebra-db`.
