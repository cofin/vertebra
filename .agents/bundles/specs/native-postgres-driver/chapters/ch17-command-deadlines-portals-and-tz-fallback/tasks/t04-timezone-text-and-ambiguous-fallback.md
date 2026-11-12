---
type: Task
id: "native-postgres-driver:ch17:t04"
chapter_id: ch17-command-deadlines-portals-and-tz-fallback
title: "Resolve vetted jiff timezone text and enforce output-only ambiguous temporal fallback"
description: "Pin jiff =0.2.39 with bundled tzdb in vertebra-postgres to resolve unique in-range session timezone instants, capture ambiguous/out-of-range outputs as TemporalText, and reject wire_text typed rebinds with DataError."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch17:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md
  - .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
  - src/rs/Cargo.toml
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/src/codecs/datetime_text.rs
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/Cargo.lock
  - src/rs/vertebra-postgres/src/codecs/datetime_text.rs
tests:
  - src/rs/vertebra-postgres/tests/timezone_text.rs
  - src/py/tests/unit/db/test_postgres_native_queries.py
estimated_loc: 230
plan_revision: 5
plan_commit: c8722c76
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T20:21:59Z"
last_verified_commit: c8722c76
verification_evidence: "cargo test -p vertebra-postgres --test timezone_text --test datetime_text --locked (10 passed), pytest test_postgres_native_queries.py (2 passed), and cargo check --locked passed"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: f7535c47
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen PgTemporal wire_text invariants and unconditional DataError typed-rebind refusal contract
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL timezone_abbreviations input configuration asymmetry and LMT/DST ambiguity constraints
  - id: cargo-workspace
    resource: src/rs/Cargo.toml
    title: Rust workspace dependency manifest
tags:
  - postgres
  - codecs
  - timezones
  - jiff
  - rust
---

# Task t04: Resolve Vetted `jiff` Timezone Text and Enforce Output-Only Ambiguous Temporal Fallback

## Objective & Scope Boundary

Pin `jiff = "=0.2.39"` (`std`, `tzdb-bundle-always`) in `vertebra-postgres` and implement `resolve_or_capture_temporal` in `src/rs/vertebra-postgres/src/codecs/datetime_text.rs` so non-ISO `timestamptz` abbreviation outputs resolve to an exact PostgreSQL UTC epoch integer **only** when the active session `TimeZone` (IANA, POSIX, or fixed numeric offset) and displayed abbreviation uniquely identify one instant strictly within `jiff`'s supported civil/timestamp range, retaining lossless `PgValue::TemporalText` (`PgTemporal(special="wire_text")`) otherwise and rejecting every typed `wire_text` bind with `DataError` before submission across text, binary, pipeline, and COPY paths [^pg-contracts] [^pg-protocol-research].

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 17 `jiff = "=0.2.39"` resolution and `wire_text` bind refusal rules.
- [contracts.md](../../../contracts.md) — `PgTemporal` `special="wire_text"` invariants and unconditional `DataError` refusal across text, binary, pipeline (including first operation), and COPY paths [^pg-contracts].
- [protocol-and-execution_20261105.md](../../../research/protocol-and-execution_20261105.md) — `timezone_abbreviations` input/output asymmetry, DST fold/gap, and historical LMT rules [^pg-protocol-research].
- `src/rs/Cargo.toml` — Rust workspace Cargo configuration [^cargo-workspace].
- `src/rs/vertebra-postgres/Cargo.toml` — `vertebra-postgres` crate manifest.
- `src/rs/vertebra-postgres/src/codecs/datetime_text.rs` — Existing `decode_timestamp_text`, `encode_temporal_text`, and `CodecContext` (`DateStyle` / `TimeZone`) from `ch10:t02`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/Cargo.toml` and `src/rs/Cargo.lock`:
  - Add exact pinned dependency `jiff = { version = "=0.2.39", default-features = false, features = ["std", "tzdb-bundle-always"] }` and update `src/rs/Cargo.lock` (`cargo check -p vertebra-postgres --locked`).
- `src/rs/vertebra-postgres/src/codecs/datetime_text.rs`:
  - Expose `pub fn resolve_or_capture_temporal(...) -> Result<PgValue, PostgresError>` (and re-export via `pub mod timezone_text { pub use super::resolve_or_capture_temporal; }` so `codecs::datetime_text::timezone_text::resolve_or_capture_temporal` is addressable directly):
    1. **Explicit fixed numeric offsets & canonical `UTC`/`GMT`**: continue resolving directly over the full PostgreSQL year range via `ch10:t02`'s checked Gregorian arithmetic.
    2. **Named IANA or POSIX `TimeZone` within `jiff`'s documented civil/timestamp range** (`-9999..=9999` civil years / `jiff::Timestamp::MIN..=MAX`):
       - Look up the active `CodecContext` `time_zone` in `jiff::tz::db()` (or parse POSIX TZ string via `jiff::tz::TimeZone::posix` / fixed offset).
       - Resolve candidate zoned instants for the parsed local civil date/time (`jiff::tz::TimeZone::to_ambiguous_zoned`):
         - `AmbiguousZoned::Unambiguous(zdt)`: verify the zone's active abbreviation at that instant matches the wire abbreviation; if exact match, convert to PostgreSQL `2000-01-01T00:00:00Z` `i64` microseconds (`PgValue::Timestamptz`).
         - `AmbiguousZoned::Fold(before, after)`: ONLY if `before` and `after` carry **distinct** zone abbreviations (e.g., `"EDT"` vs `"EST"` in `"America/New_York"`) AND the wire abbreviation matches **exactly one** branch, select that unique instant; if both fold branches share the same abbreviation (or neither matches), treat as unresolvable and capture `PgValue::TemporalText`!
         - `AmbiguousZoned::Gap(_, _)`, unknown/unparsed `TimeZone`, historical LMT with non-unique/mismatched label, abbreviation mismatch, or civil year outside `jiff`'s range (`BC` years / years `> 9999`): NEVER guess via a global abbreviation map and NEVER extrapolate POSIX/IANA rules outside `jiff`'s vetted range — return `Ok(PgValue::TemporalText { kind: TemporalKind::Timestamptz, wire_text, date_style, time_zone })` [^pg-contracts] [^pg-protocol-research].
    3. **Unconditional `DataError` refusal on typed `TemporalText` bind**:
       - Ensure `encode_temporal_text` (and binary/pipeline/COPY parameter validation) rejects every `PgValue::TemporalText` (`PgTemporal(special="wire_text")`) with canonical `PostgresError::Data` (`DataError`) BEFORE wire submission — even when captured `date_style` and `time_zone` equal the current session's `ParameterStatus`, and even in the first operation of a pipeline batch. Matching output `DateStyle`/`TimeZone` cannot prove input invertibility because PostgreSQL's `timezone_abbreviations` setting independently controls input parsing [^pg-contracts]. Callers either resolve the value to a finite/raw temporal instant or explicitly bind a `str` (`TEXT`) parameter.

## Edge Cases & Error Semantics

- **Ambiguous regional abbreviations (`CST`, `BST`, `AST`) without matching active `TimeZone`**: Under a session `TimeZone` whose rule at that local time does not produce that abbreviation (or when `TimeZone` is unresolvable), no global table guesses `+08:00` vs `-06:00` vs `-05:00`; `PgValue::TemporalText` preserves exact `wire_text`, `date_style`, and `time_zone`.
- **Binary temporal values unaffected**: Binary `timestamptz` wire frames carry exact `i64` UTC microseconds and never produce `TemporalText` regardless of `DateStyle`, `TimeZone`, or `timezone_abbreviations`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/timezone_text.rs` and `src/py/tests/unit/db/test_postgres_native_queries.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Unique IANA abbreviation & DST fold with distinct abbrs | `DateStyle = "Postgres, MDY"`, `TimeZone = "America/New_York"`: `"Sun Nov 01 01:30:00 2026 EDT"` vs `"Sun Nov 01 01:30:00 2026 EST"` | Resolves uniquely to `2026-11-01T05:30:00Z` (`EDT`, `-04:00`) and `2026-11-01T06:30:00Z` (`EST`, `-05:00`) as UTC `datetime` / exact PG `i64`. |
| Same-abbreviation fold, DST gap, & historical LMT | Fold where both offsets share `"CST"` / `"BST"`, nonexistent spring-forward gap local time, or pre-standardization `"LMT"` output | Returns `PgValue::TemporalText` -> `PgTemporal(kind="timestamptz", value=None, special="wire_text", wire_text=..., date_style=..., time_zone=...)` without guessing. |
| Out-of-`jiff`-range PG years (`BC` / `> 9999`) with abbreviation | `"Wed Jul 09 07:30:00 12000 EDT"` or `"0044-03-15 12:00:00 EST BC"` under `"America/New_York"` | Never extrapolates `jiff` beyond its range; preserves lossless `TemporalText` / `PgTemporal(special="wire_text")` (while numeric-offset `"12000-07-09 07:30:00-04"` still resolves via checked PG arithmetic). |
| Global abbreviation guess forbidden | Wire `"Wed Jul 09 07:30:00 2026 CST"` when `TimeZone = "UTC"` (mismatched abbreviation) | Captures `TemporalText` rather than guessing China (`+08:00`), US Central (`-06:00`/`-05:00`), or Cuba (`-05:00`/`-04:00`). |
| Matching-context typed rebind refused everywhere | Receive `val = PgTemporal(kind="timestamptz", value=None, special="wire_text", ...)` from `query` / `pipeline`, then pass `val` to `client.query("SELECT $1::timestamptz", [val])` and `pipeline` op `0` under identical `DateStyle` & `TimeZone` (and altered `timezone_abbreviations`) | Raises `vertebra._vertebra.DataError` BEFORE emitting any wire bytes in both text/binary `query` and `pipeline` op `0`; explicit `str(val.wire_text)` remains caller's intentional raw-text choice. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test timezone_text
  ```
- Final Green, `--locked` Cargo check, Python query/stubtest, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test timezone_text
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test datetime_text
  cargo check --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [x] `jiff = "=0.2.39"` (`std`, `tzdb-bundle-always`) is pinned in `vertebra-postgres/Cargo.toml` and `src/rs/Cargo.lock`, passing `cargo check --locked` (R11).
- [x] `resolve_or_capture_temporal` resolves non-ISO `timestamptz` abbreviation outputs strictly within `jiff`'s range only when local fields + abbreviation identify a unique instant, capturing ambiguous folds/gaps, LMT, mismatched abbreviations, and out-of-range years as `TemporalText` / `PgTemporal(special="wire_text")` without global abbreviation guessing (R11).
- [x] Every `TemporalText` / `PgTemporal(special="wire_text")` typed bind raises `DataError` before submission across text, binary, pipeline (including op `0`), and COPY paths, even when `DateStyle` and `TimeZone` match (R11).
- [x] `timezone_text`, `datetime_text`, `test_postgres_native_queries.py`, `mypy.stubtest vertebra._postgres`, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen `PgTemporal` `wire_text` discriminant and `DataError` typed-bind refusal contract.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL 18 `timezone_abbreviations` input configuration and non-ISO display text semantics.
[^cargo-workspace]: `src/rs/Cargo.toml` — Rust workspace Cargo configuration.
