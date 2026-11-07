---
type: Task
id: "native-postgres-driver:ch12:t01"
chapter_id: ch12-pgvector-and-extended-query
title: Repair shared pgvector/halfvec/bit CPU helpers and integrate dynamic vector and bitstring codecs
description: Fix half 2.7.1 ties-to-even rounding, finite overflow, 1..=16000 dimension bounds, and 0-length/padding bit rules in vertebra-db, and wire TypeCatalog-resolved vector/halfvec and BIT/VARBIT codecs into vertebra-postgres.
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "native-postgres-driver:ch11:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch12-pgvector-and-extended-query/spec.md
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-postgres/src/types.rs
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-db/src/postgres/mod.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
tests:
  - src/rs/vertebra-db/tests/postgres_vector_tests.rs
  - src/rs/vertebra-postgres/tests/vector_codecs.rs
estimated_loc: 240
plan_revision: 4
plan_commit: 45ff9563
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
    title: PgVector 1..=16000 and half 2.7.1 ties-to-even rules, PgBitString zero-padding invariants, and dynamic extension OID lookup
  - id: pg-vector-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16
    title: Shared encode_pgvector_f32/f16/bit and decode_pgvector_f32/f16/bit helpers in vertebra-db
  - id: pg-embed-py
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: Existing pgvector consumer in vertebra.db.adapters.postgres.embeddings
tags:
  - postgres
  - rust
  - pgvector
  - codecs
---

# Task t01: Repair Shared `pgvector`/`halfvec`/`bit` CPU Helpers and Integrate Dynamic Vector and Bitstring Codecs

## Objective & Scope Boundary

Reproduce and fix the rounding, finite-overflow, `1..=16000` dimension, and `bit`/`varbit` zero-length/padding defects in `vertebra_db::postgres::{encode_pgvector_f32, decode_pgvector_f32, encode_pgvector_f16, decode_pgvector_f16, encode_pgvector_bit, decode_pgvector_bit}` using `half` (`=2.7.1`, already declared on `vertebra-db` and `vertebra-postgres` in `ch01:t01`–`t02`), and integrate `TypeCatalog`-resolved `"vector"` / `"halfvec"` and built-in `BIT` (`1560`) / `VARBIT` (`1562`) binary and text codecs into `src/rs/vertebra-postgres/src/codecs/mod.rs` [^pg-contracts] [^pg-vector-rs] [^pg-embed-py]. Do not mutate any `Cargo.toml` or `Cargo.lock` file; `sparsevec` is out of scope, and Python `PgVector` / `PgBitString` wrapper hydration belongs to `ch14:t03`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgVector` (`1..=16000`, `half 2.7.1` ties-to-even, pre/post-narrowing finiteness) and `PgBitString` (`(b"", 0)`, MSB-first zero low-bit padding, `1560`/`1562`) [^pg-contracts].
- [spec.md](../spec.md) — Chapter 12 `t01` `regression_tdd` boundaries.
- `src/rs/vertebra-db/src/postgres/mod.rs` — Existing `f32_to_f16_bits`, `f16_bits_to_f32`, and `encode/decode_pgvector_{f32,f16,bit}` helpers [^pg-vector-rs].
- `src/rs/vertebra-postgres/src/types.rs` — `TypeCatalog` (`TypeKind::Extension { schema, name }` for `"vector"` and `"halfvec"`, plus built-in `BIT` `1560` / `VARBIT` `1562` and arrays `1561` / `1563`).
- `src/rs/vertebra-postgres/src/result.rs` — `PgValue::{Vector, BitString}` and `VectorKind`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `PgValue::{Vector, BitString}` binary/text dispatch and target-OID validation.

## Symbol & Signature Contracts

- **`src/rs/vertebra-db/src/postgres/mod.rs`**:
  - Replace handwritten `f32_to_f16_bits` / `f16_bits_to_f32` with `half::f16::from_f32` / `half::f16::from_bits(...).to_f32()` (consuming `half = "=2.7.1"` already declared in `src/rs/vertebra-db/Cargo.toml` and locked in `src/rs/Cargo.lock` by `ch01:t01`–`t02`, with zero `Cargo.toml` or `Cargo.lock` edits).
  - Preserve exact public signatures while enforcing pgvector `0.8.1` and PostgreSQL `varbit` wire rules:
    - `pub const PGVECTOR_MAX_DIM: usize = 16_000;` (value limit, NOT ANN index limit `2000`/`4000`).
    - `encode_pgvector_f32(values: &[f32]) -> Result<Vec<u8>, String>` & `decode_pgvector_f32(buf: &[u8]) -> Result<Vec<f32>, String>`: require `1 <= dim <= 16_000`, `unused == 0`, exact byte length `4 + dim * 4`, and `v.is_finite()` on every element in both encode and decode (preserving `-0.0` and subnormals).
    - `encode_pgvector_f16(values: &[f32]) -> Result<Vec<u8>, String>` & `decode_pgvector_f16(buf: &[u8]) -> Result<Vec<f32>, String>`: require `1 <= dim <= 16_000`, `unused == 0`, exact byte length `4 + dim * 2`, `v.is_finite()` before conversion, `let h = half::f16::from_f32(v);` with `h.is_finite()` verified after ties-to-even quantization (rejecting `65520.0_f32 -> ±Inf`), and `h.is_finite()` verified on decode before `h.to_f32()`.
    - `encode_pgvector_bit(packed_bytes: &[u8], bit_len: usize) -> Result<Vec<u8>, String>` & `decode_pgvector_bit(buf: &[u8]) -> Result<(Vec<u8>, usize), String>`: allow `bit_len == 0` (`packed_bytes == b""`, wire `[0, 0, 0, 0]`), require `0 <= bit_len <= i32::MAX as usize`, `packed_bytes.len() == bit_len.div_ceil(8)`, exact wire length `4 + expected_bytes`, and zero unused low bits in the final byte (`let rem = bit_len % 8; if rem != 0 && (last & ((1u8 << (8 - rem)) - 1)) != 0 { return Err(...) }`). Vector `1..=16_000` limits do NOT apply to SQL `bit`/`varbit`.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Built-in constants `pub const BIT_OID: u32 = 1560;`, `pub const VARBIT_OID: u32 = 1562;`.
  - Negotiate `Format::Binary` for `1560`, `1562`, and `TypeCatalog`-discovered `TypeKind::Extension { name, .. }` when `name == "vector" || name == "halfvec"` (plus arrays over them); never hard-code dynamic extension OIDs, and keep undiscovered extension OIDs at `Format::Text` / `PgValue::Text`.
  - Decode/encode `PgValue::Vector { values, kind }` in binary (via `vertebra_db::postgres::{encode/decode_pgvector_f32, encode/decode_pgvector_f16}`) and text (`[v1,v2,...]` bracketed ASCII with no `NaN`/`Inf`, `1..=16_000` elements, `half::f16::from_f32` quantization check for `HalfVec`). Reject `VectorKind::Vector` bound to a `halfvec` OID or `VectorKind::HalfVec` bound to a `vector` OID with `PostgresError::Data` before send; never decode vectors into plain lists or `bytea` into `BitString`.
  - Decode/encode `PgValue::BitString { data, bit_length }` in binary (via `encode/decode_pgvector_bit`) and text (`'0'`/`'1'` MSB-first string of exact `bit_length`, including `""` for `bit_length == 0`).

## Edge Cases & Error Semantics

- **Existing defects captured in initial RED (`regression_tdd`)**:
  1. `encode_pgvector_f16(&[1.00048828125_f32])` (`1.0 + 2^-11`, midpoint with even LSB `0x3c00`) previously produced `0x3c01` instead of IEEE ties-to-even `0x3c00`.
  2. `encode_pgvector_f16(&[65520.0_f32])` previously succeeded with `0x7c00` (`+Inf`) instead of failing on finite-to-half overflow.
  3. `encode_pgvector_f32(&vec![0.0; 16_001])` and `decode_pgvector_f32(&[0, 0, 0, 0])` (`dim = 0`) previously succeeded instead of enforcing `1..=16_000`.
  4. `encode_pgvector_bit(b"", 0)` / `decode_pgvector_bit(&[0, 0, 0, 0])` previously failed on valid `0`-length `varbit`, while `encode_pgvector_bit(b"\xa8", 3)` / `decode_pgvector_bit(&[0, 0, 0, 3, 0xa8])` previously accepted nonzero low padding bit `0x08`.
- **Signed zero, subnormals, & wrapper-kind mismatch**: `-0.0_f32` (`0x8000` in `f16`, `0x8000_0000` in `f32`), smallest positive `f16` subnormal `2^-24` (`5.9604645e-8_f32` -> `0x0001`), and largest finite `f16` `65504.0_f32` (`0x7bff`) round-trip bit-exactly; binding `VectorKind::HalfVec` against dynamic `vector` OID `42001` fails with `PostgresError::Data`.

## Public-Seam Test Blueprint

- **Canonical test files**:
  - `src/rs/vertebra-db/tests/postgres_vector_tests.rs` (behavioral RED reproduction against `vertebra_db::postgres` public helpers)
  - `src/rs/vertebra-postgres/tests/vector_codecs.rs` (`TypeCatalog`-resolved `vector`/`halfvec`, `vector[]`, and `BIT`/`VARBIT` binary + text fixtures)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `halfvec` ties-to-even, signed zero, subnormal, & max finite | `[1.00048828125_f32, 1.00146484375_f32, -0.0_f32, 5.9604645e-8_f32, 65504.0_f32]` | Encodes `[0x3c00, 0x3c02, 0x8000, 0x0001, 0x7bff]` matching pgvector `0.8.1` `halfvec_send`; preserves `-0.0` sign bit on decode. |
| Finite-to-half overflow & `NaN`/`Inf` wire payloads | `encode_pgvector_f16(&[65520.0])`, `decode_pgvector_f16(&[0, 1, 0, 0, 0x7c, 0x00])`, `decode_pgvector_f32(&[0, 1, 0, 0, 0x7f, 0xc0, 0, 0])` | All three fail (`Err` / `PostgresError::Data`). |
| pgvector `1..=16_000` dimension & `unused == 0` bounds | `vec![1.0; 16_000]` vs `[]` (`dim = 0`), `vec![1.0; 16_001]`, `unused = 1`, or trailing byte | `16_000`-dim (including `> 2000` ANN index limit) succeeds; `0`-dim, `16_001`-dim, `unused != 0`, and wrong byte length fail. |
| `BIT`/`VARBIT` zero length, non-aligned MSB bits, & padding check | `(b"", 0)` (`""`), `(b"\xa0", 3)` (`"101"`), `20_000`-bit `varbit`, vs `(b"\xa8", 3)` or `bit_len = -1` | First three round-trip in binary and text (`PgValue::BitString`); nonzero padding `0xa8` and negative/mismatched lengths fail. |
| Dynamic `TypeCatalog` `vector`/`halfvec`, `vector[]`, & kind mismatch | Discovered `vector = 42001`, `halfvec = 42002`, `_vector = 42003`; unknown `91000` `"[1,2]"` | `42001`/`42002`/`42003` round-trip in binary and text; binding `HalfVec` to `42001` fails with `PostgresError::Data`; `91000` stays `PgValue::Text("[1,2]")`. |

## Verification

- Initial Red baseline (must fail on buggy rounding/overflow/dimension/zero-bit assertions, not missing imports):
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --test postgres_vector_tests`
- Final Green + Regression + Lockfile + Clippy gate:
  - `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --lib postgres::tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --test postgres_vector_tests && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test vector_codecs && cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `vertebra_db::postgres` `f32`/`f16`/`bit` helpers preserve their public signatures while enforcing `half` (`2.7.1`) ties-to-even rounding, finite-to-half overflow rejection, `1..=16000` vector dimensions, valid `0`-length `bit`/`varbit`, and zero unused low-bit padding with zero `Cargo.toml` or `Cargo.lock` modifications (R11).
- [ ] `vertebra-postgres` selects `vector` and `halfvec` codecs strictly via session `TypeCatalog` extension identity, rejects wrapper/target-OID mismatches before send, and keeps undiscovered extension OIDs as `PgValue::Text` (R11).
- [ ] Existing `vertebra-db` unit tests, new `postgres_vector_tests` + `vector_codecs` suites, and `--locked` workspace checks pass with zero warnings (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgVector` `1..=16000` and `half 2.7.1` ties-to-even rules, `PgBitString` zero-padding invariants, and dynamic extension OID lookup.
[^pg-vector-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::encode_pgvector_f16` — Shared `encode_pgvector_f32`/`f16`/`bit` and `decode_pgvector_f32`/`f16`/`bit` helpers in `vertebra-db`.
[^pg-embed-py]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — Existing `pgvector` consumer in `vertebra.db.adapters.postgres.embeddings`.
