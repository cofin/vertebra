---
type: Task
id: "native-postgres-driver:ch10:t02"
chapter_id: ch10-temporal-interval-and-json-codecs
title: Implement session-aware DateStyle and TimeZone temporal text codecs
description: Decode PostgreSQL date, time, timestamp, and timestamptz text across ISO, SQL, Postgres, and German DateStyle formats and MDY/DMY/YMD orderings, capturing unresolved abbreviation displays as TemporalText.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch10:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch10-temporal-interval-and-json-codecs/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
files:
  - src/rs/vertebra-postgres/src/codecs/datetime_text.rs
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
tests:
  - src/rs/vertebra-postgres/tests/datetime_text.rs
estimated_loc: 245
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
    title: DateStyle/TimeZone text decoding and TemporalText output-only fallback rules
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL datetime.c server output styles, BC/large-year arithmetic, and timezone_abbreviations constraints
tags:
  - postgres
  - rust
  - codecs
  - datetime
---

# Task t02: Implement Session-Aware `DateStyle` and `TimeZone` Temporal Text Codecs

## Objective & Scope Boundary

Implement server-output temporal text decoding and canonical ISO text encoding in `src/rs/vertebra-postgres/src/codecs/datetime_text.rs` across all four PostgreSQL `DateStyle` styles (`ISO`, `SQL`, `Postgres`, `German`) and all three field orderings (`MDY`, `DMY`, `YMD`), converting unambiguous outputs to exact `2000-01-01` epoch integers and preserving abbreviation-only `timestamptz` displays as `PgValue::TemporalText { kind, wire_text, date_style, time_zone }` [^pg-contracts] [^pg-research]. Named/POSIX IANA zone database resolution via `jiff` belongs to `ch17:t04`; this task never guesses an offset for non-`UTC`/`GMT` abbreviations.

## Bounded Read Manifest

- [contracts.md](../../../contracts.md) — `PgTemporal` raw-epoch vs `wire_text` invariants and `DateStyle`/`TimeZone` rules [^pg-contracts].
- [spec.md](../spec.md) — Chapter 10 `datetime_text.rs` contracts.
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — PostgreSQL `datetime.c` output styles, `BC`/large-year arithmetic, and `timezone_abbreviations` hazard [^pg-research].
- `src/rs/vertebra-postgres/src/codecs/temporal.rs` — Checked `ymd_to_pg_days`, `pg_days_to_ymd`, and temporal OID constants (`1082`, `1083`, `1114`, `1184`).
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `CodecContext` (`date_style`, `time_zone`) and `decode_value` / `encode_value` text dispatch.

## Symbol & Signature Contracts

- **`src/rs/vertebra-postgres/src/codecs/datetime_text.rs`**:
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum DateFormatStyle { Iso, Sql, Postgres, German }` and `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub enum DateOrder { Mdy, Dmy, Ymd }` with `pub fn parse_date_style(raw: &str) -> (DateFormatStyle, DateOrder)` (parsing `"ISO, MDY"`, `"SQL, DMY"`, `"Postgres, MDY"`, `"German, DMY"`, etc.).
  - `pub fn decode_date_text(text: &str, ctx: &CodecContext) -> Result<i32, PostgresError>`: handles `"infinity"` / `"+infinity"` (`i32::MAX`), `"-infinity"` (`i32::MIN`), trailing `" BC"` (`year_astro = 1 - bc_year`), 5+ digit years (`"12000-01-01"`), `"ISO"` (`YYYY-MM-DD`), `"SQL"` (`MM/DD/YYYY` or `DD/MM/YYYY`), `"Postgres"` (`MM-DD-YYYY` or `DD-MM-YYYY`), and `"German"` (`DD.MM.YYYY`) using `temporal::ymd_to_pg_days`.
  - `pub fn decode_time_text(text: &str) -> Result<i64, PostgresError>`: parses `HH:MM:SS[.ffffff]` (`1..=6` fractional digits right-padded to microseconds), accepting `"24:00:00"` / `"24:00:00.000000"` (`86_400_000_000`) and rejecting `24:00:00.000001`, `minutes >= 60`, or `seconds >= 60`.
  - `pub fn decode_timestamp_text(text: &str, kind: TemporalKind, ctx: &CodecContext) -> Result<PgValue, PostgresError>`:
    - Handles `"infinity"` / `"-infinity"`, `"ISO"` (`YYYY-MM-DD HH:MM:SS[.f][±HH[:MM[:SS]]][ BC]`), `"SQL"` (`MM/DD/YYYY HH:MM:SS[.f] [TZ][ BC]`), `"Postgres"` (`Dy Mon DD HH:MM:SS[.f] YYYY [TZ][ BC]` or `Dy DD Mon ...` under `DMY`), and `"German"` (`DD.MM.YYYY HH:MM:SS[.f] [TZ][ BC]`).
    - For `TemporalKind::Timestamp`: ignores no-offset ambiguity and computes exact naive microseconds from `2000-01-01 00:00:00`.
    - For `TemporalKind::TimestampTz`: explicit signed numeric UTC offsets (`+00`, `-04:00`, `+05:30:15`, `-00:44:30`) and unambiguous `"UTC"` / `"GMT"` / `"Z"` labels subtract the offset in microseconds to produce `PgValue::TimestampTz(utc_micros)`. Any other alphabetic abbreviation (`"CST"`, `"BST"`, `"PDT"`, `"LMT"`) validates the local calendar/time grammar first and returns `PgValue::TemporalText { kind: TemporalKind::TimestampTz, wire_text: text.to_owned(), date_style: ctx.date_style.clone(), time_zone: ctx.time_zone.clone() }` (prepared for `ch17:t04`).
  - `pub fn encode_temporal_text(value: &PgValue) -> Result<String, PostgresError>`: emits canonical unambiguous ISO-8601 strings (`"2026-11-05"`, `"24:00:00"`, `"0044-03-15 12:00:00 BC"`, `"2026-11-05 14:30:00.123456+00"`, `"infinity"`, `"-infinity"`), and rejects `PgValue::TemporalText` with `Err(PostgresError::Data(_))` before wire submission.
- **`src/rs/vertebra-postgres/src/codecs/mod.rs`**:
  - Declare `pub mod datetime_text;` and wire `Format::Text` branches for `DATE_OID`, `TIME_OID`, `TIMESTAMP_OID`, and `TIMESTAMPTZ_OID` in `decode_value` and `encode_value`.

## Edge Cases & Error Semantics

- **Astronomical BC year numbering**: PostgreSQL has no year `0` (`"0001-01-01 BC"` immediately precedes `"0001-01-01"` AD by 1 day, and `"0004-02-29 BC"` is a leap day because astronomical year `-3` is not divisible by 4 whereas `"0005-02-29 BC"` = astronomical year `-4` IS a leap year!). Verify `0001-01-01 BC` (`-730_850` days) and `0005-02-29 BC` explicitly.
- **Seconds-precision UTC offsets**: Historical zones emit signed second offsets such as `+00:53:28` or `-04:56:02`; numeric offset parser accepts `±HH`, `±HH:MM`, `±HHMM`, and `±HH:MM:SS`.
- **Live `ParameterStatus` switching**: Updating `DateStyle` between results in a multi-statement `simple_query` (`SET DateStyle = 'SQL, DMY'; SELECT ...; SET DateStyle = 'German, DMY'; SELECT ...`) updates `CodecContext` immediately without rejecting valid `session_gucs`.
- **Malformed unambiguous server text**: Invalid calendar dates (`"2025-02-29"`, `"31.04.2026"`), malformed times (`"25:00:00"`), or garbage tokens fail with `Err(PostgresError::Data(_))` even if followed by a timezone abbreviation.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/datetime_text.rs` (golden server output fixtures across all 4 `DateStyle` x 3 `DateOrder` combinations + scripted `simple_query` `ParameterStatus` transitions).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| All 4 `DateStyle` x `MDY`/`DMY`/`YMD` date forms | `"2026-07-04"` (`ISO`), `"07/04/2026"` (`SQL, MDY`), `"04/07/2026"` (`SQL, DMY`), `"07-04-2026"` (`Postgres, MDY`), `"04-07-2026"` (`Postgres, DMY`), `"04.07.2026"` (`German, DMY`) | Every input decodes to the identical `PgValue::Date(9681)` (`2026-07-04`). |
| `BC` leap years & years `> 9999` | `"0001-01-01 BC"`, `"0005-02-29 BC"`, `"4713-11-24 BC"`, `"294276-12-31 23:59:59.999999+00"` | Exact raw `PgValue::Date` / `PgValue::TimestampTz` matching `temporal::decode_*_binary` fixtures; `"0004-02-29 BC"` returns `Err(PostgresError::Data)`. |
| `Postgres` & `SQL` `timestamptz` with numeric/UTC vs abbreviation | `"Wed Jul 01 17:30:15.25 2026 +05:30:15"`, `"Wed Jul 01 12:00:00 2026 UTC"` vs `"Wed Jul 01 12:00:00 2026 CST"` (`TimeZone="Asia/Shanghai"`) | First two yield exact `PgValue::TimestampTz` (`2026-07-01T12:00:00Z` / `.25Z`); `"CST"` yields `PgValue::TemporalText { wire_text: "Wed Jul 01 12:00:00 2026 CST", date_style: "Postgres, MDY", time_zone: "Asia/Shanghai", .. }`. |
| `24:00:00`, infinities, & `TemporalText` rebind refusal | `"24:00:00"`, `"infinity"`, `"-infinity"`; `encode_temporal_text(&PgValue::TemporalText { .. })` | Decode to `Time(86_400_000_000)` and `±infinity` sentinels; `encode_temporal_text` on `TemporalText` returns `Err(PostgresError::Data)`. |
| Mid-session `ParameterStatus` `DateStyle` transition | `simple_query` stream emitting `ParameterStatus("DateStyle", "German, DMY")` before Result 1 (`"05.11.2026"`) and `ParameterStatus("DateStyle", "SQL, MDY")` before Result 2 (`"11/05/2026"`) | Both results decode to identical `PgValue::Date(9805)`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test datetime_text`
- Final Green + Clippy gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test datetime_text && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] All four `DateStyle` output formats (`ISO`, `SQL`, `Postgres`, `German`) across `MDY`, `DMY`, and `YMD` decode to exact PostgreSQL `2000-01-01` epoch integers (R11).
- [ ] Explicit numeric offsets (including seconds) and `UTC`/`GMT` resolve exact instants, while abbreviation-only `timestamptz` outputs remain tagged as `PgValue::TemporalText` with captured `DateStyle` and `TimeZone` (R11).
- [ ] `ParameterStatus` updates take effect immediately per result set, `BC`/large-year calendar bounds work without `chrono` clipping, and `TemporalText` typed rebinds fail with `PostgresError::Data` (R11).

## Notes & Discoveries

- None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — `DateStyle`/`TimeZone` text decoding and `TemporalText` output-only fallback rules.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL `datetime.c` server output styles, `BC`/large-year arithmetic, and `timezone_abbreviations` constraints.
