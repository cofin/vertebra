---
type: Task
id: "native-postgres-driver:ch10:t01"
chapter_id: ch10-temporal-interval-and-json-codecs
title: Implement lossless PostgreSQL 2000-01-01 epoch temporal and interval binary codecs
description: Encode and decode binary DATE (1082), TIME (1083), TIMESTAMP (1114), TIMESTAMPTZ (1184), and INTERVAL (1186) values with exact PostgreSQL 2000-01-01 epoch units, 24:00 bounds, and infinity sentinels.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch09:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch10-temporal-interval-and-json-codecs/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/result.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/result.rs
tests:
  - src/rs/vertebra-postgres/tests/temporal_codecs.rs
estimated_loc: 230
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
    title: Temporal units, 2000-01-01 epoch, 24:00 end_of_day, and PgInterval contracts
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL date/timestamp/interval binary send layouts and PG18.3 infinity sentinels
tags:
  - postgres
  - rust
  - codecs
  - temporal
---

# Task t01: Implement Lossless PostgreSQL `2000-01-01` Epoch Temporal and Interval Binary Codecs

## Objective & Scope Boundary

Implement fixed-width big-endian binary codecs in `src/rs/vertebra-postgres/src/codecs/temporal.rs` for `DATE` (`OID 1082`), `TIME` (`OID 1083`), `TIMESTAMP` (`OID 1114`), `TIMESTAMPTZ` (`OID 1184`), and `INTERVAL` (`OID 1186`) using exact PostgreSQL `2000-01-01` epoch integer units, checked proleptic Gregorian calendar range boundaries, `24:00:00` (`86_400_000_000` microseconds), and PG18 interval infinity triples [^pg-contracts] [^pg-research]. Session-style text parsing (`ch10:t02`–`ch10:t03`) and Python `datetime`/`PgTemporal` hydration (`ch14:t03`) are out of scope.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — Exact `2000-01-01` epoch units, `PgTemporal` specials, and `PgInterval` field invariants [^pg-contracts].
- [spec.md](../spec.md) — Chapter 10 temporal OIDs, binary signatures, and server range bounds.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — Verified `interval_send` byte order (`[i64 micros][i32 days][i32 months]`) and PG18.3 infinity probes [^pg-research].
- `src/rs/vertebra-postgres/src/result.rs` — `PgValue::{Date, Time, Timestamp, TimestampTz, Interval}` and `TemporalKind`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `format_for_oid`, `decode_value`, and `encode_value` registration.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/result.rs`**:
  - Add pure helper constants and predicates on temporal/interval values: `PG_TIME_MAX_MICROS: i64 = 86_400_000_000`, `PG_DATE_POS_INF: i32 = i32::MAX`, `PG_DATE_NEG_INF: i32 = i32::MIN`, `PG_TS_POS_INF: i64 = i64::MAX`, `PG_TS_NEG_INF: i64 = i64::MIN`, `PG_INTERVAL_POS_INF: (i32, i32, i64) = (i32::MAX, i32::MAX, i64::MAX)`, `PG_INTERVAL_NEG_INF: (i32, i32, i64) = (i32::MIN, i32::MIN, i64::MIN)`.
- **`src/rs/vertebra-postgres/src/codecs/temporal.rs`**:
  - OID constants: `pub const DATE_OID: u32 = 1082;`, `pub const TIME_OID: u32 = 1083;`, `pub const TIMESTAMP_OID: u32 = 1114;`, `pub const TIMESTAMPTZ_OID: u32 = 1184;`, `pub const INTERVAL_OID: u32 = 1186;` (note `TIMETZ` `1266` stays unregistered so unknown-OID text fallback applies cleanly without lossy binary timezone-offset assumptions).
  - Checked proleptic Gregorian calendar conversion helpers (`ymd_to_pg_days(year_astro: i32, month: u8, day: u8) -> Result<i32, PostgresError>` and `pg_days_to_ymd(days: i32) -> (i32, u8, u8)`, where `2000-01-01` = `0`, `1970-01-01` = `-10_957`, `4713-11-24 BC` = `-2_451_545`, `5874897-12-31 AD` = `2_147_483_493`) reused by `ch10:t02` and `VTBARROW`.
  - `pub fn decode_date_binary(raw: &[u8]) -> Result<i32, PostgresError>` & `pub fn encode_date_binary(days: i32, out: &mut BytesMut) -> Result<(), PostgresError>`: exact 4-byte BE `i32`; validates `days == i32::MIN || days == i32::MAX` or finite PostgreSQL `date` bounds (`-2_451_545..=2_147_483_493`).
  - `pub fn decode_time_binary(raw: &[u8]) -> Result<i64, PostgresError>` & `pub fn encode_time_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError>`: exact 8-byte BE `i64`; enforces `0 <= micros <= 86_400_000_000` (rejecting `< 0`, `> 86_400_000_000`, and `i64::MIN`/`i64::MAX`).
  - `pub fn decode_timestamp_binary(raw: &[u8]) -> Result<i64, PostgresError>` & `pub fn encode_timestamp_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError>`: exact 8-byte BE `i64`; accepts `i64::MIN` / `i64::MAX` infinities or legal PostgreSQL finite `timestamp`/`timestamptz` microsecond range (`-211_813_488_000_000_000..=9_223_371_331_200_000_000`, i.e., `4713-11-24 00:00:00 BC` through `294276-12-31 23:59:59.999999 AD`); rejects illegal finite integers outside server range rather than mislabeling them `out_of_range`.
  - `pub fn decode_interval_binary(raw: &[u8]) -> Result<(i32, i32, i64), PostgresError>` & `pub fn encode_interval_binary(months: i32, days: i32, microseconds: i64, out: &mut BytesMut) -> Result<(), PostgresError>`: exact 16-byte BE `[i64 microseconds][i32 days][i32 months]`, preserving mixed signs and PG18 `(i32::MAX, i32::MAX, i64::MAX)` / `(i32::MIN, i32::MIN, i64::MIN)` infinity triples while rejecting partial-component infinity sentinel combinations (if PostgreSQL rejects mixed `i32::MIN`/`i64::MIN` half-sentinels, validate consistently).
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Declare `pub mod temporal;`, return `Format::Binary` in `format_for_oid` for `1082`, `1083`, `1114`, `1184`, `1186`, and dispatch binary encode/decode between `PgValue::{Date, Time, Timestamp, TimestampTz, Interval}` (enforcing `Timestamp` vs `TimestampTz` target-OID agreement).

## Edge Cases & Error Semantics

- **Naive `TIMESTAMP` vs `TIMESTAMPTZ` distinction**: `PgValue::Timestamp` encodes only to `TIMESTAMP_OID` (`1114`) and `PgValue::TimestampTz` encodes only to `TIMESTAMPTZ_OID` (`1184`); cross-binding mismatched variants returns `Err(PostgresError::Data(_))` without consulting machine local timezone.
- **`TIME` `24:00:00` vs overflow**: `86_400_000_000` (`24:00:00.000000`) succeeds; `86_400_000_001` or `-1` fails with `Err(PostgresError::Data(_))`.
- **Illegal finite server `DATE`/`TIMESTAMP` integers**: Raw finite `i32` / `i64` values beyond PostgreSQL's `Julian` / server calendar domain (e.g., `i32::MAX - 1` or `i64::MAX - 1`) fail with `Err(PostgresError::Data(_))` rather than masquerading as valid `out_of_range` Python dates.
- **No nanosecond widening**: `i64::MAX` or large multi-million-hour intervals never multiply `microseconds * 1000`.
- **Wrong byte widths**: 3-byte `DATE`, 4-byte `TIME`, 12-byte `INTERVAL`, or 17-byte `INTERVAL` returns `Err(PostgresError::Data(_))`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/temporal_codecs.rs` (golden PostgreSQL binary byte vectors + boundary arithmetic assertions).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Epoch zero & pre-epoch / leap day | `DATE` `0x0000_0000` (`2000-01-01`), `-10_957` (`1970-01-01`), `60` (`2000-03-01` after `2000-02-29`); `TIMESTAMP` `-1` (`1999-12-31 23:59:59.999999`) | Exact round-trip of `PgValue::Date(0)`, `Date(-10957)`, `Date(60)`, and `Timestamp(-1)`. |
| `TIME` `00:00:00`, `24:00:00`, and overflow | Binary `0`, `86_400_000_000` (`0x0000_0014_1dd7_6000`), `-1`, and `86_400_000_001` | `0` and `86_400_000_000` round-trip; `-1` and `86_400_000_001` return `Err(PostgresError::Data)`. |
| `DATE` / `TIMESTAMP` / `TIMESTAMPTZ` infinities & BC / year > 9999 | `DATE` `0x7fff_ffff` (`+inf`), `0x8000_0000` (`-inf`), `-730_850` (`0001-01-01 BC`), `3_652_425` (`12000-01-01 AD`); `TIMESTAMPTZ` `0x7fff_ffff_ffff_ffff` | Exact round-trip as `PgValue::{Date, TimestampTz}`; illegal finite `i32::MAX - 1` returns `Err(PostgresError::Data)`. |
| `INTERVAL` mixed signs & PG18 infinity sentinels | `16B` `[micros = 11_045_000_006 i64 BE][days = -2 i32 BE][months = 1 i32 BE]` and `[i64::MAX][i32::MAX][i32::MAX]` | Decodes to `PgValue::Interval { months: 1, days: -2, microseconds: 11_045_000_006 }` and PG18 `+infinity` triple with exact 16-byte re-encode. |
| Wrong payload widths & `Timestamp` vs `TimestampTz` OID mismatch | 8-byte buffer on `DATE_OID`, 12-byte buffer on `INTERVAL_OID`, or `encode_value(&PgValue::Timestamp(0), TIMESTAMPTZ_OID, ...)` | Returns `Err(PostgresError::Data(_))`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test temporal_codecs`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test temporal_codecs && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [x] Binary `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, and `INTERVAL` codecs match PostgreSQL `2000-01-01` epoch units and wire byte layouts (R11).
- [x] `PgValue::Interval` preserves independent `(months, days, microseconds)` signs and PG18 infinity sentinels without `timedelta` or nanosecond conversion (R11).
- [x] `24:00:00`, `±infinity`, `BC`, and years `> 9999` round-trip while illegal finite ranges and wrong widths fail with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Temporal units, `2000-01-01` epoch, `24:00` `end_of_day`, and `PgInterval` contracts.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `date`/`timestamp`/`interval` binary send layouts and PG18.3 infinity sentinels.
