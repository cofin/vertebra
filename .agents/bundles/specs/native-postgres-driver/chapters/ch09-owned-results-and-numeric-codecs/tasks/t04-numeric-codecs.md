---
type: Task
id: "native-postgres-driver:ch09:t04"
chapter_id: ch09-owned-results-and-numeric-codecs
title: Implement arbitrary-precision base-10000 numeric binary and text codecs
description: Implement PostgreSQL NUMERIC (OID 1700) binary base-10000 and text codecs preserving exact precision, declared dscale, NaN, and positive/negative infinity without float or fixed-width narrowing.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch09:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch09-owned-results-and-numeric-codecs/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/numeric.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/result.rs
tests:
  - src/rs/vertebra-postgres/tests/numeric_codecs.rs
estimated_loc: 235
plan_revision: 6
plan_commit: 8833b064
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
    title: Arbitrary-precision Decimal scale, special values, and sNaN rejection contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL numeric_send/numeric_recv base-10000 wire format and PG18.3 golden bytes
tags:
  - postgres
  - rust
  - codecs
  - numeric
---

# Task t04: Implement Arbitrary-Precision Base-10000 Numeric Binary and Text Codecs

## Objective & Scope Boundary

Implement PostgreSQL `NUMERIC` (`OID 1700`) binary and text codecs in `src/rs/vertebra-postgres/src/codecs/numeric.rs`, preserving arbitrary precision (beyond `f64` and `rust_decimal` 96-bit limits), declared display scale (`dscale`), `NaN`, `+Infinity`, and `-Infinity` using checked base-10000 digit groups, and register `OID 1700` in `codecs/mod.rs` [^pg-contracts] [^pg-research]. `NUMERIC[]` (`OID 1231`) container framing belongs to `ch11:t02`–`ch11:t03`, and Python `decimal.Decimal` hydration belongs to `ch14:t03`.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `Decimal` precision/scale preservation and `DataError` pre-submission rules [^pg-contracts].
- [spec.md](../spec.md) — Chapter 09 `PgNumeric` and `numeric.rs` codec contracts.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Verified `numeric_send` wire layout (`1.2300` -> `0002000000000004000108fc`) and PostgreSQL `numeric.c` limits [^pg-research].
- `src/rs/vertebra-postgres/src/result.rs` — `PgNumeric` and `PgValue::Numeric`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext`, `format_for_oid`, `decode_value`, and `encode_value`.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/result.rs`**:
  - Ensure `PgNumeric` provides checked construction helpers from `(negative: bool, base10_digits: &[u8], exponent: i32, signaling_nan: bool) -> Result<Self, PostgresError>` (mirroring Python `Decimal.as_tuple()` without touching thread `decimal.Context` precision/rounding, and rejecting `signaling_nan == true` with `PostgresError::Data`) plus exact `to_canonical_string(&self) -> String`.
- **`src/rs/vertebra-postgres/src/codecs/numeric.rs`**:
  - Wire sign constants matching PostgreSQL `src/backend/utils/adt/numeric.c`: `NUMERIC_POS: u16 = 0x0000`, `NUMERIC_NEG: u16 = 0x4000`, `NUMERIC_NAN: u16 = 0xC000`, `NUMERIC_PINF: u16 = 0xD000`, `NUMERIC_NINF: u16 = 0xF000`, `MAX_DSCALE: u16 = 0x3FFF` (`16_383`), and `MAX_NUMERIC_DIGITS: usize = 32_768` (covering PostgreSQL's `131_072` integer digits + `16_383` fractional digits).
  - `pub fn decode_numeric_binary(raw: &[u8]) -> Result<PgNumeric, PostgresError>`: parses 8-byte header `[i16 ndigits BE][i16 weight BE][u16 sign BE][u16 dscale BE]` followed by `ndigits` `i16` base-10000 values (`0..=9999`), checking exact byte length `8 + 2 * (ndigits as usize)` before allocation.
  - `pub fn encode_numeric_binary(num: &PgNumeric, out: &mut BytesMut) -> Result<(), PostgresError>`: normalizes leading/trailing all-zero base-10000 groups from `digits` while preserving `dscale` independently (zero normalizes to `ndigits = 0, weight = 0, sign = NUMERIC_POS, dscale = preserved_dscale`).
  - `pub fn decode_numeric_text(text: &str) -> Result<PgNumeric, PostgresError>`: parses optional sign, integer/fractional digits, scientific exponent (`e`/`E` with checked `i32` bounds against `MAX_DSCALE` and `MAX_NUMERIC_DIGITS`), `"NaN"`, `"Infinity"`/`"+Infinity"`, and `"-Infinity"` (case-insensitive matching PostgreSQL output) without float or fixed-width integer intermediaries; rejects `"sNaN"` / `"-sNaN"`.
  - `pub fn encode_numeric_text(num: &PgNumeric) -> String`: renders exact decimal text preserving `dscale` trailing zeros (`1.2300` -> `"1.2300"`, `0` with `dscale = 3` -> `"0.000"`, `NaN` -> `"NaN"`, `PosInf` -> `"Infinity"`, `NegInf` -> `"-Infinity"`).
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Declare `pub mod numeric;`, add `pub const NUMERIC_OID: u32 = 1700;`, return `Format::Binary` in `format_for_oid(NUMERIC_OID)`, and wire `PgValue::Numeric` into `decode_value` and `encode_value`.

## Edge Cases & Error Semantics

- **Trailing zero scale preservation**: `1.2300` has base-10000 groups `[1, 2300]`, `weight = 0`, `dscale = 4` (`0x0002_0000_0000_0004_0001_08fc`). Even when fractional digits end in `0000` (e.g., `5.0000` -> `ndigits = 1, weight = 0, dscale = 4, digits = [5]`), `dscale = 4` is preserved in both binary and text (`"5.0000"`).
- **Zero sign normalization with scale retention**: `-0.00` normalizes sign to `NUMERIC_POS` (`0x0000`, `ndigits = 0`, `weight = 0`) while retaining `dscale = 2` (`"0.00"`), matching PostgreSQL `numeric_in` / `numeric_send`.
- **Signaling `NaN` refusal**: PostgreSQL has no signaling-NaN representation; `PgNumeric::from_decimal_parts(..., signaling_nan = true)` and text `"sNaN"` fail with `PostgresError::Data` before wire submission.
- **Malformed binary packets**: Header `< 8` bytes, `ndigits < 0`, `ndigits as usize > MAX_NUMERIC_DIGITS`, unknown `sign` word (`0x1234`), non-zero `ndigits`/`weight`/`dscale` on `NUMERIC_NAN`/`NUMERIC_PINF`/`NUMERIC_NINF`, `dscale > 16383`, any base-10000 digit `< 0` or `>= 10000`, or trailing/truncated bytes returns `Err(PostgresError::Data(_))` without panicking or over-allocating.
- **Extreme precision beyond `rust_decimal` / `f64`**: 60-digit financial/scientific values and `1e1000` round-trip byte-for-byte in binary and character-for-character in text.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/numeric_codecs.rs` (golden `numeric_send` byte vectors from PostgreSQL + negative wire fuzzing).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| PG18.3 golden `1.2300` with trailing zeros | Binary `hex!("0002000000000004000108fc")` & text `"1.2300"` | Decodes to `Finite { negative: false, weight: 0, dscale: 4, digits: [1, 2300] }`; re-encodes to exact `0002000000000004000108fc` and `"1.2300"`. |
| Negative sub-unit weight & zero scale retention | Text `"-0.000056"` (`weight = -2, dscale = 6, digits = [5600]`) and `"-0.000"` | `"-0.000056"` round-trips negative weight `-2`; `"-0.000"` normalizes sign to positive with `ndigits = 0, dscale = 3` (`"0.000"`). |
| 50-digit high precision beyond `Decimal128` / `rust_decimal` | `"123456789012345678901234567890.00009876543210987654"` & `"1e1000"` | Exact base-10000 binary & canonical text round-trip with zero rounding. |
| Special values (`NaN`, `+Infinity`, `-Infinity`) & `sNaN` rejection | Binary `0000_0000_c000_0000`, `0000_0000_d000_0000`, `0000_0000_f000_0000`; `signaling_nan = true` / `"sNaN"` | Specials map to `PgNumeric::{NaN, PosInf, NegInf}`; `sNaN` returns `Err(PostgresError::Data)`. |
| Malformed binary headers & out-of-range digits | `digit = 10000` (`0x2710`), `sign = 0x8000`, `ndigits = -1`, `dscale = 20000`, `NaN` with `ndigits = 1`, or 1 trailing byte | Every case returns `Err(PostgresError::Data(_))` within bounded memory. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test numeric_codecs`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test numeric_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `NUMERIC` (`OID 1700`) binary and text codecs preserve arbitrary precision, declared `dscale`, `NaN`, `+Infinity`, and `-Infinity` without narrowing through `f64` or fixed-width decimals (R11).
- [ ] Signaling `NaN` inputs are rejected with `PostgresError::Data` (`DataError`) before wire emission (R11).
- [ ] Malformed headers, out-of-range base-10000 digits (`< 0` or `>= 10000`), invalid sign flags, `dscale > 16383`, and truncated/trailing frames fail cleanly within bounded allocation limits (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Arbitrary-precision Decimal scale, special values, and sNaN rejection contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `numeric_send`/`numeric_recv` base-10000 wire format and PG18.3 golden bytes.
