---
type: Task
id: "native-postgres-driver:ch10:t03"
chapter_id: ch10-temporal-interval-and-json-codecs
title: Implement lossless interval text codecs across all four PostgreSQL IntervalStyle formats
description: Decode and encode PostgreSQL INTERVAL (OID 1186) text across postgres, postgres_verbose, sql_standard, and iso_8601 session styles without month flattening or float rounding.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch10:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch10-temporal-interval-and-json-codecs/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/interval_text.rs
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/interval_text.rs
estimated_loc: 230
plan_revision: 4
plan_commit: e8901de4
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
    title: PgInterval independent signed (months, days, microseconds) and infinity contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL IntervalStyle output grammars and PG18.3 mixed-sign/infinity probes
tags:
  - postgres
  - rust
  - codecs
  - interval
---

# Task t03: Implement Lossless Interval Text Codecs Across All Four PostgreSQL `IntervalStyle` Formats

## Objective & Scope Boundary

Implement bounded, style-aware `INTERVAL` (`OID 1186`) text decoding and canonical ISO-8601 text encoding in `src/rs/vertebra-postgres/src/codecs/interval_text.rs` across all four PostgreSQL `IntervalStyle` settings (`postgres`, `postgres_verbose`, `sql_standard`, `iso_8601`), preserving independent `(months: i32, days: i32, microseconds: i64)` fields, mixed signs, 6-digit fractional microseconds, and PG18 `±infinity` sentinels [^pg-contracts] [^pg-research].

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgInterval(months, days, microseconds)` independent signs and infinity sentinels [^pg-contracts].
- [spec.md](../spec.md) — Chapter 10 `interval_text.rs` contracts.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — PostgreSQL `IntervalStyle` output forms and PG18.3 mixed-sign probe (`1 month, -2 days, 11045000006 microseconds`) [^pg-research].
- `src/rs/vertebra-postgres/src/codecs/temporal.rs` — `INTERVAL_OID` (`1186`), `PG_INTERVAL_POS_INF`, `PG_INTERVAL_NEG_INF`, and binary interval helpers.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext::interval_style` (`ParameterStatus("IntervalStyle")`) and `decode_value` / `encode_value` text dispatch.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/interval_text.rs`**:
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum IntervalFormatStyle { Postgres, PostgresVerbose, SqlStandard, Iso8601 }` with `pub fn parse_interval_style(raw: &str) -> Result<IntervalFormatStyle, PostgresError>`.
  - `pub fn decode_interval_text(text: &str, ctx: &CodecContext) -> Result<(i32, i32, i64), PostgresError>`:
    - Handles `"infinity"` / `"+infinity"` -> `(i32::MAX, i32::MAX, i64::MAX)` and `"-infinity"` -> `(i32::MIN, i32::MIN, i64::MIN)`.
    - **`postgres` style**: e.g. `"1 year 2 mons -3 days -04:05:06.123456"`, `"00:00:00"`, `"-00:00:00.5"`; accumulates `years * 12 + mons` with `i32::checked_add`/`checked_mul`, `days` in `i32`, and signed `±HH:MM[:SS[.ffffff]]` in `i64` microseconds (applying leading `-` on the time group to hours, minutes, seconds, and fractional microseconds alike!).
    - **`postgres_verbose` style**: leading `"@ "` with unit pairs (`year`/`years`, `mon`/`mons`, `day`/`days`, `hour`/`hours`, `min`/`mins`, `sec`/`secs`), `"@ 0"`, and optional trailing `" ago"` which negates all three accumulated components via checked negation (`i32::checked_neg` / `i64::checked_neg`).
    - **`sql_standard` style**: `"0"`, `[+|-]Y-M` (`"-1-2"` -> `-14` months), `[+|-]D [H:M:S[.f]]`, or combined `[+|-]Y-M [+|-]D [+|-]H:M:S[.f]` where each signed group distributes its sign across the fields inside that group (`+1-2 -3 +4:05:06.7` -> `months = 14, days = -3, microseconds = 14_706_700_000`).
    - **`iso_8601` style**: `"PT0S"`, `P[<y>Y][<mo>M][<d>D][T[<h>H][<mi>M][<s>[.f]S]]` where each designator carries its own independent sign (`"P1Y2M-3DT-4H-5M-6.5S"`).
  - `pub fn encode_interval_text(months: i32, days: i32, microseconds: i64) -> String`:
    - Renders `"infinity"` / `"-infinity"` for exact PG18 infinity triples, `"PT0S"` for `(0, 0, 0)`, and unambiguous ISO-8601 `P<months>M<days>DT<hours>H<mins>M<secs>.<frac>S` preserving independent component signs without floating-point division.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Declare `pub mod interval_text;`, capture `ParameterStatus("IntervalStyle")` in `CodecContext::from_parameters`, and wire `INTERVAL_OID` (`Format::Text`) in `decode_value` and `encode_value`.

## Edge Cases & Error Semantics

- **Negative fractional sub-minute times**: `"-00:00:00.000006"` in `postgres` / `sql_standard` or `"@ 0.000006 secs ago"` in `postgres_verbose` must parse sign separately from integer `0` (`hours == 0 && mins == 0 && secs == 0` still negates the `6` microseconds to `-6`!).
- **Checked overflow bounds**: `years = i32::MAX` (`years * 12` overflow), `i32::MIN` negated by `"ago"` (except the exact infinity sentinel), >6 fractional microsecond digits or unparsable unit designators (`"P5Q"`, `"1 fortnight"`, empty `""`) fail with `Err(PostgresError::Data(_))` before entering a `COPY` stream or pipeline.
- **Warm binary vs cold text equivalence**: Every finite and PG18 infinite fixture produces identical `(months, days, microseconds)` tuples whether decoded via `temporal::decode_interval_binary` or `interval_text::decode_interval_text`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/interval_text.rs` (golden server output fixtures across all 4 `IntervalStyle` formats + binary-vs-text equivalence + `ParameterStatus` switching).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Mixed-sign triple across all 4 `IntervalStyle` grammars | `postgres`: `"1 mon -2 days +03:04:05.000006"`; `postgres_verbose`: `"@ 1 mon -2 days 3 hours 4 mins 5.000006 secs"`; `sql_standard`: `"+0-1 -2 +3:04:05.000006"`; `iso_8601`: `"P1M-2DT3H4M5.000006S"` | All four decode to `PgValue::Interval { months: 1, days: -2, microseconds: 11_045_000_006 }`, identical to `decode_interval_binary`. |
| Negative zero-hour fractional seconds & verbose `ago` | `postgres`: `"-00:00:00.5"`; `postgres_verbose`: `"@ 1 year -2 days 0.5 secs ago"` | Decodes to `(0, 0, -500_000)` and `(-12, 2, -500_000)`. |
| Zero spellings & PG18 `±infinity` | `"00:00:00"`, `"@ 0"`, `"0"`, `"PT0S"`, `"infinity"`, `"-infinity"` | Zeroes yield `(0, 0, 0)`; infinities yield `(i32::MAX, i32::MAX, i64::MAX)` and `(i32::MIN, i32::MIN, i64::MIN)`. |
| Large multi-hundred-thousand-hour intervals & canonical encode | `encode_interval_text(-14, 3, -900_000_000_001)` -> `decode_interval_text(..., iso_8601)` | Exact round-trip `(-14, 3, -900_000_000_001)` with zero float loss. |
| Malformed designators & `i32` month overflow | `"P10X"`, `"@ 2000000000 years"`, `""`, `"+1-2-3"` | Returns `Err(PostgresError::Data(_))`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test interval_text`
- Final Green + Regression + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test interval_text && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test temporal_codecs && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] `postgres`, `postgres_verbose`, `sql_standard`, and `iso_8601` `IntervalStyle` outputs preserve independent `(months, days, microseconds)` fields, mixed signs, negative sub-second fractions, and PG18 infinities (R11).
- [ ] Warm binary and cold text interval paths produce identical `PgValue::Interval` tuples without `f64` or `timedelta` conversion (R11).
- [ ] Malformed interval strings and integer overflows fail with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `PgInterval` independent signed `(months, days, microseconds)` and infinity contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `IntervalStyle` output grammars and PG18.3 mixed-sign/infinity probes.
