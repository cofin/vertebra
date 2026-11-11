---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch10-temporal-interval-and-json-codecs
title: "Chapter 10: Lossless Temporal, Session-Style Datetime/Interval Text, and JSON/JSONB Codecs"
description: Implement PostgreSQL 2000-01-01 epoch binary temporal and interval codecs, session-aware DateStyle and IntervalStyle text decoders, and strict JSON and version-checked JSONB wire codecs.
status: stable
state: completed
planning_status: complete
depends_on_chapters:
  - ch09-owned-results-and-numeric-codecs
replan_trigger: null
current_task: null
plan_revision: 5
plan_commit: 936daa9f
state_revision: 0
last_verified_checkpoint: ef33a380
last_reviewed_commit: ef33a380
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: 936daa9f
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgreSQL 2000-01-01 temporal epoch units, PgTemporal/PgInterval/PgJson contracts, and typed NULL rules
  - id: pg-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL temporal/interval wire layout, DateStyle/IntervalStyle output grammars, and PG18.3 probes
  - id: pg-jsonb-rs
    resource: src/rs/vertebra-db/src/postgres/mod.rs::strip_pg_jsonb_header
    title: Existing 0x01 PostgreSQL binary JSONB version header helpers in vertebra-db
tags:
  - postgres
  - rust
  - codecs
  - temporal
  - jsonb
---

# Chapter 10: Lossless Temporal, Session-Style Datetime/Interval Text, and JSON/JSONB Codecs

<!-- truth: start -->
- **Exact `2000-01-01` Epoch & Independent `Interval` Wire Units**: Binary `DATE` (`1082`), `TIME` (`1083`), `TIMESTAMP` (`1114`), `TIMESTAMPTZ` (`1184`), and `INTERVAL` (`1186`) codecs preserve `i32` days, `i64` microseconds (including `24:00:00 = 86_400_000_000` and `±infinity` sentinels), and independent `(months: i32, days: i32, microseconds: i64)` triples without float epochs, nanosecond overflow, or `timedelta` month flattening [^pg-contracts] [^pg-research].
- **Session-Aware `DateStyle` & `IntervalStyle` Text Decoding**: Cold and simple-query text decoders honor active `ParameterStatus` `DateStyle` (`ISO`, `SQL`, `Postgres`, `German` across `MDY`/`DMY`/`YMD`), `TimeZone`, and `IntervalStyle` (`postgres`, `postgres_verbose`, `sql_standard`, `iso_8601`), capturing abbreviation-only `timestamptz` displays as `PgValue::TemporalText` rather than guessing offsets [^pg-contracts] [^pg-research].
- **Validated `JSON` & Version-Checked `JSONB`**: `JSON` (`114`) and `JSONB` (`3802`, mandatory `0x01` version byte) codecs validate strict RFC 8259 UTF-8 JSON without rounding large integers through `f64`, keeping JSON `null` (`PgValue::Json(b"null")`) distinct from SQL `NULL` (`PgValue::Null`) [^pg-contracts] [^pg-jsonb-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

PostgreSQL binary temporal values span `4713 BC` to `5874897 AD` (`date`) and `294276 AD` (`timestamp`/`timestamptz`), allow `24:00:00` (`time`) and `±infinity`, and encode `interval` as three independent signed fields (`[i64 microseconds][i32 days][i32 months]`); meanwhile, cold pipelines and simple queries emit text formatted under session-configurable `DateStyle`, `TimeZone`, and `IntervalStyle` GUCs, and `JSONB` prepends a `0x01` binary version byte while distinguishing JSON `null` from SQL `NULL` [^pg-contracts] [^pg-research] [^pg-jsonb-rs]. Chapter 10 implements lossless binary and session-aware text codecs for all temporal, interval, and JSON/JSONB types in `vertebra-postgres`.

### 1.2 Non-Goals

- Named/POSIX IANA timezone abbreviation resolution via `jiff` and `PgTemporal(special="wire_text")` rebind rejection across `COPY`/pipelines (finalized in `ch17:t04`; `ch10:t02` captures unresolved abbreviation-only `timestamptz` text into `PgValue::TemporalText` without guessing offsets).
- Container codecs (`_date`, `_timestamptz`, `_interval`, `_jsonb` arrays and composites), owned by Chapter 11.
- Python `datetime`/`Decimal`/`msgspec` hydration (`python_values.rs`), owned by Chapter 14.

### 1.3 Upstream Execution Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface authority: [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Depends on `ch09-owned-results-and-numeric-codecs` (`PgValue`, `TemporalKind`, `CodecContext`, `SimpleQueryCollector`, and `codecs/mod.rs` dispatcher).

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Temporal & JSON Value Contracts | `.agents/bundles/specs/native-postgres-driver/contracts.md` [^pg-contracts] | Defines `2000-01-01` epoch units, `PgTemporal` specials (`positive_infinity`, `negative_infinity`, `out_of_range`, `end_of_day`, `wire_text`), `PgInterval`, and `PgJson(None)` vs SQL `NULL`. |
| Server Formatting & Wire Probes | `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` [^pg-research] | Confirms `date` `0x7fffffff`, `timestamp` `0x7fffffffffffffff`, `time` `24:00 = 86_400_000_000`, PG18 interval infinity `(i64::MAX, i32::MAX, i32::MAX)`, and 4 `DateStyle` / 4 `IntervalStyle` grammars. |
| Binary `JSONB` Header Helpers | `src/rs/vertebra-db/src/postgres/mod.rs::strip_pg_jsonb_header` [^pg-jsonb-rs] | Existing `0x01`-version-checked `strip_pg_jsonb_header` and `encode_pg_jsonb` CPU helpers in `vertebra-db`. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).
- Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R11 (`ch10:t01`)**: Encode and decode binary `DATE` (`1082`), `TIME` (`1083`), `TIMESTAMP` (`1114`), `TIMESTAMPTZ` (`1184`), and `INTERVAL` (`1186`) using exact PostgreSQL `2000-01-01` epoch integers, `24:00:00` time bounds, `±infinity` sentinels, and independent `(months, days, microseconds)` interval fields.
- **R11 (`ch10:t02`)**: Decode temporal text across all four `DateStyle` output styles (`ISO`, `SQL`, `Postgres`, `German`) and `MDY`/`DMY`/`YMD` orderings using owned proleptic Gregorian arithmetic across PostgreSQL's full year range, updating `CodecContext` live on `ParameterStatus` and tagging abbreviation-only non-ISO `timestamptz` strings as `PgValue::TemporalText`.
- **R11 (`ch10:t03`)**: Decode and encode interval text across `postgres`, `postgres_verbose`, `sql_standard`, and `iso_8601` styles without float rounding or flattening months into days.
- **R11 (`ch10:t04`)**: Encode and decode `JSON` (`114`) and versioned `JSONB` (`3802`, `0x01` prefix), validating RFC 8259 UTF-8 JSON without `f64` integer precision loss and keeping JSON `null` separate from SQL `NULL`.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- **Binary Temporal & Interval Codecs (`src/rs/vertebra-postgres/src/codecs/temporal.rs`)**:
  - OID constants: `DATE_OID = 1082`, `TIME_OID = 1083`, `TIMESTAMP_OID = 1114`, `TIMESTAMPTZ_OID = 1184`, `INTERVAL_OID = 1186`.
  - `pub fn decode_date_binary(raw: &[u8]) -> Result<i32, PostgresError>`, `pub fn encode_date_binary(days: i32, out: &mut BytesMut) -> Result<(), PostgresError>`
  - `pub fn decode_time_binary(raw: &[u8]) -> Result<i64, PostgresError>`, `pub fn encode_time_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError>` (`0 <= micros <= 86_400_000_000`)
  - `pub fn decode_timestamp_binary(raw: &[u8]) -> Result<i64, PostgresError>`, `pub fn encode_timestamp_binary(micros: i64, out: &mut BytesMut) -> Result<(), PostgresError>`
  - `pub fn decode_interval_binary(raw: &[u8]) -> Result<(i32, i32, i64), PostgresError>`, `pub fn encode_interval_binary(months: i32, days: i32, microseconds: i64, out: &mut BytesMut) -> Result<(), PostgresError>` (wire order: `microseconds: i64 BE`, `days: i32 BE`, `months: i32 BE`).
- **Session-Style Datetime Text Codecs (`src/rs/vertebra-postgres/src/codecs/datetime_text.rs`)**:
  - `pub enum DateFormatStyle { Iso, Sql, Postgres, German }`, `pub enum DateOrder { Mdy, Dmy, Ymd }`
  - `pub fn decode_date_text(text: &str, ctx: &CodecContext) -> Result<i32, PostgresError>`
  - `pub fn decode_time_text(text: &str) -> Result<i64, PostgresError>`
  - `pub fn decode_timestamp_text(text: &str, kind: TemporalKind, ctx: &CodecContext) -> Result<PgValue, PostgresError>`
  - `pub fn encode_temporal_text(value: &PgValue) -> Result<String, PostgresError>`
- **Session-Style Interval Text Codecs (`src/rs/vertebra-postgres/src/codecs/interval_text.rs`)**:
  - `pub enum IntervalFormatStyle { Postgres, PostgresVerbose, SqlStandard, Iso8601 }`
  - `pub fn decode_interval_text(text: &str, ctx: &CodecContext) -> Result<(i32, i32, i64), PostgresError>`
  - `pub fn encode_interval_text(months: i32, days: i32, microseconds: i64) -> String`
- **JSON & JSONB Codecs (`src/rs/vertebra-postgres/src/codecs/json.rs`)**:
  - OID constants: `JSON_OID = 114`, `JSONB_OID = 3802`.
  - `pub fn decode_json_binary(raw: &[u8]) -> Result<Vec<u8>, PostgresError>`, `pub fn encode_json_binary(json: &[u8], out: &mut BytesMut) -> Result<(), PostgresError>`
  - `pub fn decode_jsonb_binary(raw: &[u8]) -> Result<Vec<u8>, PostgresError>`, `pub fn encode_jsonb_binary(json: &[u8], out: &mut BytesMut) -> Result<(), PostgresError>`
  - `pub fn validate_json_bytes(raw: &[u8]) -> Result<&str, PostgresError>`

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SimpleQueryCollector` / future `ExtendedQueryCollector` & `COPY` -> sample `CodecContext::from_parameters(conn.parameters())` per result description -> invoke `codecs::decode_value` / `codecs::encode_value` (`codecs/mod.rs`) -> dispatch `1082`/`1083`/`1114`/`1184`/`1186` to `temporal.rs`, `datetime_text.rs`, and `interval_text.rs`, and `114`/`3802` to `json.rs` (delegating `0x01` header framing to `vertebra_db::postgres::{strip_pg_jsonb_header, encode_pg_jsonb}`).

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Out-of-`chrono` / `BC` years & `24:00:00` | `"4713-11-24 BC"`, `"294276-12-31 23:59:59.999999"`, `"24:00:00"` | Owned proleptic Gregorian math computes exact `2000-01-01` epoch integers; `"24:00:00"` -> `Time(86_400_000_000)`; `"24:00:00.000001"` -> `PostgresError::Data` | Full PostgreSQL server temporal domain without library clipping |
| Ambiguous `timestamptz` abbreviation | `"Wed Jul 01 12:00:00 2026 CST"` under `DateStyle = "Postgres, MDY"` | Returns `PgValue::TemporalText { kind: TimestampTz, wire_text, date_style, time_zone }` without global abbreviation guessing | Zero silent timezone corruption |
| Mixed-sign & PG18 infinite `INTERVAL` | `"@ 1 year -2 days 3 hours 4 mins 5.000006 secs ago"` or `(i64::MAX, i32::MAX, i32::MAX)` | Preserves `months = -12, days = 2, microseconds = -11_045_000_006` and PG18 infinity triple identically across binary and text | Zero `timedelta` month loss or `i64 * 1000` nanosecond overflow |
| `JSONB` version & 64-bit integer precision | `0x01` + `{"id": 9007199254740993}` vs `0x02` + `{}` vs JSON `null` | `0x01` preserves exact ASCII digits `9007199254740993`; `0x02` fails with `PostgresError::Data`; `b"\x01null"` -> `PgValue::Json(b"null") != PgValue::Null` | Exact integer fidelity and SQL `NULL` vs JSON `null` separation |

### 2.5 Acceptance Scenarios (Given / When / Then)

- **Given** `ParameterStatus` updates switching `DateStyle` from `"ISO, MDY"` to `"German, DMY"` and `IntervalStyle` from `"postgres"` to `"sql_standard"` between statements, **When** `SimpleQueryCollector` decodes consecutive result sets, **Then** `"17.11.2026"` and `"-1-2 +3 -04:05:06.5"` produce identical raw `PgValue::Date` and `PgValue::Interval` values to warm binary `DATE` and `INTERVAL` frames.
- **Given** a `JSONB` binary payload `b"\x01null"` and a SQL `NULL` `JSONB` cell in the same row, **When** `decode_value` runs, **Then** cell `0` yields `PgValue::Json(b"null".to_vec())` and cell `1` yields `PgValue::Null`.

### 2.6 Alternatives Considered

| Alternative | Why Rejected | Chosen Seam |
| --- | --- | --- |
| Relying solely on `chrono::NaiveDate` for temporal text | `chrono` caps years around `±262,143`, failing valid PostgreSQL dates up to `5,874,897 AD`. | Checked integer Hinnant/proleptic Gregorian calendar conversion over PostgreSQL's exact `2000-01-01` epoch. |
| Parsing JSON via `serde_json::Value` with float numbers | Allocates recursive AST maps on Rust worker threads and risks `f64` coercion if configured loosely. | Bounded single-pass RFC 8259 UTF-8 validator retaining exact owned JSON bytes for `msgspec` hydration in Python. |
| Rejecting non-`ISO` `DateStyle` / `IntervalStyle` `session_gucs` | Violates user-configured legacy schemas and `SET DateStyle` sessions. | Live `ParameterStatus`-driven output parsers for all 4 `DateStyle` and 4 `IntervalStyle` server formats. |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/0 files documented in ch10]` (public user docs and OKF data chapters update in `ch30:t01` and `ch30:t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R11 (binary temporal & interval) | [t01](tasks/t01-temporal-and-interval-binary.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/temporal_codecs.rs` |
| R11 (`DateStyle` & `TimeZone` text) | [t02](tasks/t02-datetime-text-styles.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/datetime_text.rs` |
| R11 (`IntervalStyle` text) | [t03](tasks/t03-interval-text-styles.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/interval_text.rs`, `src/rs/vertebra-postgres/tests/temporal_codecs.rs` |
| R11 (`JSON` & `JSONB` codecs) | [t04](tasks/t04-json-and-jsonb-codecs.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/json_codecs.rs` |

## 5. Implementation Plan

- [x] [ef33a380] native-postgres-driver:ch10:t01 — Implement lossless PostgreSQL 2000-01-01 epoch temporal and interval binary codecs (tasks/t01-temporal-and-interval-binary.md)
- [x] [ef33a380] native-postgres-driver:ch10:t02 — Implement session-aware DateStyle and TimeZone temporal text codecs (tasks/t02-datetime-text-styles.md)
- [x] [ef33a380] native-postgres-driver:ch10:t03 — Implement lossless interval text codecs across all four PostgreSQL IntervalStyle formats (tasks/t03-interval-text-styles.md)
- [x] [ef33a380] native-postgres-driver:ch10:t04 — Implement strict JSON and version-validated JSONB binary and text codecs (tasks/t04-json-and-jsonb-codecs.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: 936daa9f`; chapter `state: completed` at `ef33a380`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: None (`4/4` completed at `ef33a380`).
- **Downstream Dependents**: Unblocks `ch11-type-catalog-arrays-and-composites`, `ch12-pgvector-and-extended-query`, `ch17:t04` (`timezone_text.rs`), and `ch18`–`ch20` (`COPY` and `VTBARROW`).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgreSQL 2000-01-01 temporal epoch units, PgTemporal/PgInterval/PgJson contracts, and typed NULL rules.
[^pg-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL temporal/interval wire layout, DateStyle/IntervalStyle output grammars, and PG18.3 probes.
[^pg-jsonb-rs]: `src/rs/vertebra-db/src/postgres/mod.rs::strip_pg_jsonb_header` — Existing `0x01` PostgreSQL binary JSONB version header helpers in `vertebra-db`.
