---
type: Task
id: "native-postgres-driver:ch19:t02"
chapter_id: ch19-native-arrow-batches-and-shm
title: Build bounded native Arrow scalar RecordBatches in Rust
description: Implement PostgresArrowBatchBuilder in vertebra-postgres to decode raw DataRow and PgValue scalar fields directly into Arrow columns with 65536-row and 8 MiB value-buffer flush bounds.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch19:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch19-native-arrow-batches-and-shm/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - src/rs/vertebra-postgres/src/codecs/numeric.rs
  - src/rs/vertebra-postgres/src/codecs/temporal.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/arrow.rs
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
estimated_loc: 225
plan_revision: 3
plan_commit: dd432f18
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
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Exact PostgreSQL scalar, numeric, JSON null, and temporal discriminant contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Native Rust DataRow decoding and bounded Arrow batch sizing
tags:
  - postgres
  - rust
  - arrow
  - codecs
  - streaming
---

# Task t02: Build Bounded Native Arrow Scalar RecordBatches in Rust

## Objective & Scope Boundary

Implement `PostgresArrowBatchBuilder` in `src/rs/vertebra-postgres/src/arrow.rs` to decode raw `DataRow` fields (and `Portal` `PgValue` rows) directly into Arrow `RecordBatch` columns in pure Rust without Python objects, tuples, or callbacks on Tokio workers, flushing at `max_rows` (default `65_536`) or `max_bytes` (default `8 MiB`) [^pg-contracts] [^pg-protocol-research]. Recursive array, composite, `pgvector`, and bitstring column builders extend `PostgresArrowBatchBuilder` in `ch19:t03`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 19 bounded `RecordBatch` sizing and lossless scalar Arrow contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — `PostgresArrowSchema::from_columns` (`&[crate::result::Column]`) and field metadata (`ch19:t01`).
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Shared `codecs::decode_value_with_catalog` and `CodecContext` (`DateStyle`, `TimeZone`, `IntervalStyle`).
- `src/rs/vertebra-postgres/src/codecs/numeric.rs` — Canonical PostgreSQL `numeric` decimal formatting preserving trailing scale zeros and `NaN`/`±Infinity` (`ch09:t04`).
- `src/rs/vertebra-postgres/src/codecs/temporal.rs` — Binary/text temporal, `24:00:00`, `±infinity`, out-of-Python-range, `wire_text`, and `PgInterval` codecs (`ch10:t01`–`t03`, `ch17:t04`).
- [contracts.md](../../../contracts.md) — Temporal `kind` / `wire_text`, `PgJson(None)` vs SQL `NULL`, and duplicate-column invariants [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const DEFAULT_ARROW_BATCH_ROWS: usize = 65_536;`
  - `pub const DEFAULT_ARROW_BATCH_BYTES: usize = 8 * 1024 * 1024;`
  - `pub struct PostgresArrowBatchBuilder`:
    - `pub fn try_new(columns: &[crate::result::Column], catalog: &TypeCatalog, max_rows: usize, max_bytes: usize) -> Result<Self, PostgresError>` (validates `max_rows > 0` and `max_bytes > 0`, constructs `schema` via `PostgresArrowSchema::from_columns`, and pre-sizes typed column builders without column-name maps so duplicate names remain positional)
    - `pub fn schema(&self) -> &Arc<Schema>`
    - `pub fn buffered_rows(&self) -> usize`
    - `pub fn buffered_bytes(&self) -> usize`
    - `pub fn would_exceed_bytes(&self, row_bytes: usize) -> Result<bool, PostgresError>` (raises `PostgresError::Data` before `reserve` if a single row/value exceeds `max_bytes`; returns `Ok(true)` when `buffered_rows > 0 && buffered_bytes + row_bytes > max_bytes` so callers flush the current batch before appending the new row)
    - `pub fn append_data_row(&mut self, fields: &[Option<&[u8]>], ctx: &CodecContext, catalog: &TypeCatalog) -> Result<Option<RecordBatch>, PostgresError>` (decodes each field via shared `codecs::decode_value_with_catalog(oid, format, raw, ctx, catalog)`, validates column width, flushes prior batch first if adding this row crosses `max_bytes`, appends to positional column builders, and flushes when `buffered_rows == max_rows`)
    - `pub fn append_pg_row(&mut self, row: &[PgValue]) -> Result<Option<RecordBatch>, PostgresError>` (direct `PgValue` path used by `Portal::next_event` in `ch20:t03`/`t04` without re-encoding or Python tuple conversion)
    - `pub fn flush_batch(&mut self) -> Result<Option<RecordBatch>, PostgresError>` (emits `Some(RecordBatch)` when `buffered_rows > 0`, resetting builders in place while retaining only one active batch)
    - `pub fn finish_empty(&self) -> RecordBatch` (`RecordBatch::new_empty(Arc::clone(&self.schema))` so `0`-row queries publish an explicit zero-row batch with full schema)

## Edge Cases & Error Semantics

- SQL `NULL` appends a null slot at the outer Arrow column (including null parent `Struct` for `date` / `timestamp` / `timestamptz` / `interval` with matching null children). Top-level JSON null (`PgJson(Null)`) is NOT SQL `NULL` and appends valid `"null"` UTF-8 text.
- Temporal `Struct` values:
  - Finite (`kind = 0`): exact signed PG epoch integer in `value`, nulls in `wire_text` / `date_style` / `time_zone`.
  - Negative/positive infinity (`kind = -1` / `1`): `value = 0` (non-null raw placeholder) + sentinel `kind = -1 | 1`, nulls in `wire_text` / `date_style` / `time_zone`.
  - Unresolved display `wire_text` (`kind = 2`): null `value` + non-null `wire_text`, `date_style`, and `time_zone` preserving exact server output and session context without inventing an epoch.
- Single scalar payload (`text`, `bytea`, `jsonb`, `numeric`) larger than `max_bytes` fails with `PostgresError::Data` before growing Arrow `BufferBuilder` / `StringBuilder` offsets; `i32` string/binary offset overflow is checked before append.
- Preserves ordered duplicate and empty column names across `RecordBatch::try_new` (`Schema` fields and `Vec<ArrayRef>` align positionally).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` (`scalar_*` and `batch_bounds_*` tests)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Signed/float/UUID/bytea/Unicode scalar fidelity | Rows containing `i16::MIN/MAX`, `i32::MIN/MAX`, `i64::MIN/MAX`, `u32::MAX` OID, `f32::NAN`, `f64::NEG_INFINITY`, `""` vs `NULL` `text`, `b""` vs `NULL` `bytea`, and `UUID` bytes | Exact typed Arrow values and validity bitmaps; `f32`/`f64` `is_nan()` and `-inf` preserved; empty `b""` valid vs `NULL` invalid. |
| Canonical `numeric` & JSON `null` vs SQL `NULL` | `numeric` values `0.0000`, `-123456789012345678901234567890.0010`, `NaN`, `Infinity`, `-Infinity`; `jsonb` row `1 = PgJson(Null)` vs row `2 = NULL` | `StringArray` yields `"0.0000"`, exact `34`-digit string with trailing `0`, `"NaN"`, `"Infinity"`, `"-Infinity"`; `jsonb` row `1` is valid `"null"` while row `2` is `is_null(1) == true`. |
| Full-range temporals, `24:00:00`, `interval`, and `wire_text` | `date` `-infinity` & `4713 BC`, `timestamptz` `+infinity` & `294276 AD`, `timestamptz` `wire_text("Wed Jul 09 07:30:00 2026 MET", "Postgres, MDY", "Europe/Paris")`, `time` `24:00:00`, `interval` `(-14, 3, -900_000)` vs SQL `NULL` | `kind` = `-1`/`0`/`1`/`2`; infinity stores `value = 0` + `kind = ±1`; `wire_text` has `kind = 2`, `value` null, exact context strings, and non-null parent `Struct` (distinct from SQL `NULL` parent); `time` = `86_400_000_000`; `interval` keeps `(-14, 3, -900_000)`. |
| Row-count & byte-cap multi-batch flushes + oversize single value | (a) `5` rows with `max_rows = 2`; (b) `4` rows of `3 KiB` each with `max_bytes = 7 KiB`; (c) `1` row of `9 KiB` with `max_bytes = 8 KiB`; (d) `0` rows + duplicate `"dup"` columns | (a) Batches of `2, 2, 1` rows; (b) batches of `2, 2` rows without exceeding `7 KiB`; (c) `Err(PostgresError::Data)` before allocation; (d) `0`-row `RecordBatch` with two `"dup"` columns. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests scalar_`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): build bounded native Arrow scalar batches`

## Acceptance Criteria

- [ ] `PostgresArrowBatchBuilder` decodes raw `DataRow` and `PgValue` scalar rows directly in Rust with zero Python object/tuple materialization, preserving duplicate column names and emitting explicit `0`-row batches for empty results (R20).
- [ ] Numeric scale/specials, JSON `"null"` vs SQL `NULL`, full-range temporal `kind = -1/0/1/2` (`wire_text` context), `24:00:00` `time`, and signed `interval` triples roundtrip losslessly into their `ch19:t01` schemas (R20).
- [ ] Batch flushes honor `max_rows` (`65_536` default) and `max_bytes` (`8 MiB` default), retaining at most one incomplete row and one active batch while rejecting single values above `max_bytes` before allocation (R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Exact PostgreSQL scalar, numeric, JSON null, and temporal discriminant contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Native Rust DataRow decoding and bounded Arrow batch sizing.
