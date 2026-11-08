---
type: Task
id: "native-postgres-driver:ch20:t01"
chapter_id: ch20-arrow-copy-import-and-portals
title: Convert Arrow RecordBatches into binary COPY fields with prevalidated target plans and IPC batch iterators
description: Implement PostgresArrowImportPlan, SessionHandle::copy_from_arrow, CopyFromStream::write_arrow_ipc, and bounded Python Arrow IPC batch iterators in adapters/postgres/arrow.py without Python row materialization.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch19:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch20-arrow-copy-import-and-portals/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/codecs/mod.rs
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/copy.rs
  - src/py/vertebra/db/adapters/postgres/arrow.py
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs
estimated_loc: 245
plan_revision: 8
plan_commit: 8c540809
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
    title: PostgreSQL binary COPY, temporal wire_text refusal, numeric, array, composite, and vector contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Bound pg_catalog column discovery, identifier quoting, and CopyFail rollback rules
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor buffer_view consumption without row decoding
tags:
  - postgres
  - rust
  - python
  - arrow
  - copy
---

# Task t01: Convert Arrow RecordBatches into Binary COPY Fields with Prevalidated Target Plans and IPC Batch Iterators

## Objective & Scope Boundary

Implement `PostgresArrowImportPlan` in `src/rs/vertebra-postgres/src/arrow.rs`, `SessionHandle::copy_from_arrow` + `CopyFromStream::write_arrow_ipc` in `src/rs/vertebra-postgres/src/copy.rs`, and bounded Python IPC batch chunkers (`iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync`, `load_arrow_async`) in `src/py/vertebra/db/adapters/postgres/arrow.py` [^pg-contracts] [^pg-protocol-research] [^arrow-py]. PyO3 client bindings (`SyncPostgresClient` / `AsyncPostgresClient` `copy_to`, `copy_from`, `copy_from_arrow`, and `write_arrow_ipc`) land in `ch20:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 `PostgresArrowImportPlan`, `copy_from_arrow`, and bounded Python Arrow IPC batch streaming contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — `PostgresArrowSchema` (`postgres.arrow.version = "1"`) and Arrow `60.1.0` array types (`ch19:t01`–`t04`).
- `src/rs/vertebra-postgres/src/copy.rs` — `BinaryCopyEncoder`, `CopyFromStream`, and `CopyFromStream::write_encoded_payload` (`ch18:t01`–`t03`).
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`'s `pub(crate)` operation reservation (`ch13:t01`) borrowing `&mut PostgresConnection` and `PostgresConnection::query_with_row_sink` (`ch12:t03`) so `copy_from_arrow` runs bound catalog discovery + `start_copy_in` without editing `session.rs` or `extended.rs`.
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — Shared `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)` and `codecs::decode_value_with_catalog`.
- [contracts.md](../../../contracts.md) — `wire_text` refusal, precision/timezone loss guards, `ArrowCursor.buffer_view()` zero-copy IPC slice consumption, and `PgArray`/`PgComposite`/`PgVector`/`PgBitString` invariants [^pg-contracts] [^arrow-py].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const MAX_ARROW_COPY_CALL_BYTES: usize = 16 * 1024 * 1024;`
  - `pub const MAX_ARROW_COPY_BATCH_BYTES: usize = 8 * 1024 * 1024;`
  - `pub struct PostgresArrowImportPlan`:
    - `pub fn from_schema(schema: &Schema, target_columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Self, PostgresError>` (`PostgresColumn` is Python-only in `types.py`):
      - Rejects column-count mismatch, missing/unknown/duplicate target columns, or incompatible Arrow `DataType` / `"postgres.oid"` identity with `PostgresError::Data`.
      - When `"postgres.arrow.version" == "1"` metadata is present, inverts every binary-representable `ch19:t01` schema with exact OID/typmod/dimension/lower-bound/bit-length/vector fidelity; `numeric` (`"numeric-decimal"`) parses through the dedicated PostgreSQL numeric parser preserving scale and `NaN`/`±Infinity`.
      - For ordinary Arrow schemas without PostgreSQL metadata, maps primitives with checked signed/unsigned narrowing (`UInt8/16/32/64`, `Int8/16/32/64`, `Float16/32/64`, `Boolean`, `Utf8`/`LargeUtf8`/`Utf8View`, `Binary`/`LargeBinary`/`FixedSizeBinary`), `Decimal128`/`Decimal256`, `Date32`/`Date64` (checked PG epoch conversion), `Timestamp(Second|Millisecond|Microsecond|Nanosecond, tz)` (requiring zero sub-microsecond remainder `ns % 1000 == 0`, normalizing `tz`-aware values to UTC for `timestamptz`, and rejecting `tz`-aware `Timestamp` -> naive `timestamp` loss), `Time32`/`Time64` (zero sub-microsecond remainder), `List`/`LargeList`/`FixedSizeList` -> 1D lower-bound-`1` `PgArray` (or `PgVector` when target OID is `vector`/`halfvec`), `Struct` matching ordered non-dropped composite attributes without missing/extra fields, and direct Rust `DictionaryArray` value lookup.
    - `pub fn encode_batch(&self, batch: &RecordBatch, encoder: &mut BinaryCopyEncoder, ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>`:
      - Validates batch schema compatibility and `batch.get_array_memory_size() <= MAX_ARROW_COPY_BATCH_BYTES` before encoding via `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)`; rejects any temporal `kind == 2` (`wire_text`) row with `PostgresError::Data` before writing that batch to `CopyFromStream`.
  - `pub fn decode_bounded_ipc_batches(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), PostgresError>` (enforces `ipc_bytes.len() <= MAX_ARROW_COPY_CALL_BYTES` before `StreamReader` allocation).
- `src/rs/vertebra-postgres/src/copy.rs`:
  - Identifier validation & quoting helper: parses `table_name` as `table` or `schema.table` (rejecting empty parts, embedded `\0`, or `> 2` dot-separated parts), double-quote-escapes identifiers (`"` -> `""`), resolves target `crate::result::Column` metadata once via bound `pg_catalog` query (`pg_class` / `pg_namespace` / `pg_attribute` non-dropped `attnum > 0` without `attisdropped`), and never interpolates values.
  - `SessionHandle::copy_from_arrow(self: &Arc<Self>, table_name: &str, first_batch_ipc: &[u8], columns: Option<&[String]>) -> Result<CopyFromStream, PostgresError>`:
    - Borrows `&mut PostgresConnection` via `SessionHandle`'s `pub(crate)` operation reservation (`ch13:t01`) and drives `PostgresConnection::query_with_row_sink` (`ch12:t03`) so `RowDescription` + `DataRow` frames stream directly during target column / `TypeCatalog` lookup without materializing `QueryResult.rows` and without editing `session.rs` or `extended.rs`; decodes `first_batch_ipc`, builds `PostgresArrowImportPlan`, and pre-encodes all rows in `first_batch_ipc` BEFORE issuing `COPY <quoted_target> FROM STDIN (FORMAT binary)` so schema/first-batch errors leave zero `COPY` side effects; writes pre-encoded bytes once `COPY` opens and stores `PostgresArrowImportPlan` inside `CopyFromStream`.
  - `CopyFromStream::write_arrow_ipc(&mut self, ipc_bytes: &[u8]) -> Result<(), PostgresError>`:
    - Validates entire `ipc_bytes` (`<= 16 MiB`) against stored `PostgresArrowImportPlan` and encodes each batch before calling `write_encoded_payload`; on failure, aborts `CopyFromStream` (`CopyFail` + rollback of COPY-owned transaction) and returns original `PostgresError`.
- `src/py/vertebra/db/adapters/postgres/arrow.py`:
  - `iter_arrow_ipc_batches(source, *, max_rows=65_536, max_bytes=8 * 1024 * 1024) -> Iterator[bytes]` and `aiter_arrow_ipc_batches(...) -> AsyncIterator[bytes]`:
    - Calls `ensure_module("pyarrow", extra="postgres")` before `COPY` side effects; accepts `pa.Table` (`to_reader(max_chunksize=max_rows)`), `pa.RecordBatch`, `ArrowCursor` (`pa.ipc.open_stream(cursor.buffer_view())`), `pa.RecordBatchReader`, and `__arrow_c_stream__` providers (`pa.RecordBatchReader.from_stream(source)`), closing readers in `finally`.
    - Serializes each bounded `RecordBatch` to a self-contained IPC stream via `pa.BufferOutputStream()` + `pa.ipc.new_stream(...)`; halves batch slices if serialized stream exceeds `max_bytes` (raising `DataError` if a `1`-row slice still exceeds `max_bytes`); yields a schema-only `0`-row IPC stream when input has `0` rows; NEVER calls `to_pylist()` or `to_pydict()`.
  - `load_arrow_sync` and `load_arrow_async` orchestrating `copy_from_arrow` on the first yielded IPC chunk, sequential `write_arrow_ipc` on remaining chunks, and `finish()` / `close()` (`aclose()`) in `finally`.

## Edge Cases & Error Semantics

- First-batch overflow, `UInt64 > i64::MAX`, `Decimal128` target mismatch, `Timestamp(Nanosecond)` with `123ns` remainder, `tz`-aware `Timestamp` -> naive `timestamp`, `kind = 2` `wire_text`, or missing/duplicate/unknown target column fails BEFORE `COPY ... FROM STDIN` is sent.
- Second-or-later batch value failure aborts the active `CopyFromStream` via `CopyFail` and rolls back the COPY-owned transaction (or leaves an ambient transaction in `E`) without committing batch `1`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs`
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full `ch19:t01` lossless roundtrip (`Arrow IPC -> COPY -> codecs::decode_value_with_catalog`) | IPC batches covering scalars, scaled/special `numeric`, finite/infinite temporals, `24:00:00` `time`, `interval`, negative-lower-bound 2D `PgArray`, `PgComposite`, `vector(3)`, `halfvec`, and `varbit` | Decoded `PgValue` rows match original dimensions, lower bounds, validity bitmaps, numeric scale, and float/bit precision bit-for-bit. |
| Ordinary Arrow types & dictionary arrays | `UInt16 -> int4`, `Decimal128(18, 4) -> numeric`, `Date32 -> date`, `Timestamp(Nanosecond, Some("America/New_York"))` with `.000_000_000` -> `timestamptz`, `DictionaryArray<Int8, Utf8> -> text`, `List<Int32> -> int4[]` | Encodes exact binary `COPY` fields; `List<Int32>` produces 1D `lower_bounds = [1]` `PgArray`; dictionary values resolve directly in Rust. |
| Pre-`COPY` validation rejections | `UInt64(u64::MAX) -> int8`, `Timestamp(Nanosecond)` with `500ns` remainder, `Timestamp(..., Some("UTC")) -> timestamp` (naive), `kind = 2` `wire_text`, unknown/duplicate column, or `"bad\0table"` / `"a.b.c"` | Returns `Err(PostgresError::Data)` before emitting any `COPY` command on the scripted wire peer. |
| Later-batch value failure rollback & identifier quoting | Table `"my ""schema"""."odd.table"`; batch 1 valid (`2` rows), batch 2 contains `kind = 2` `wire_text` (or `UInt32 > i16::MAX` for `int2`) | Emits `COPY "my ""schema"""."odd.table" (...) FROM STDIN (FORMAT binary)`, sends batch 1, rejects batch 2 before sending its bytes, sends `CopyFail` + `ROLLBACK`, and returns `PostgresError::Data`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests`
- Final Green + Clippy gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  ```
- Planned local commit: `feat(postgres): encode typed Arrow batches for COPY`

## Acceptance Criteria

- [ ] `PostgresArrowImportPlan` roundtrips all binary-representable `postgres.arrow.version = "1"` schemas and ordinary Arrow scalar/decimal/temporal/dictionary/list/struct types into exact `PGCOPY` binary fields without Python row conversion (R20).
- [ ] Target metadata resolves once via bound `pg_catalog` queries with double-quoted identifiers, rejecting NUL/unsupported multipart names, `wire_text` (`kind = 2`), and lossy narrowing/timestamp conversions before `COPY` startup (or aborting via `CopyFail` on later batches) (R19, R20).
- [ ] `src/py/vertebra/db/adapters/postgres/arrow.py` serializes bounded `<= 8 MiB` self-contained IPC streams from `Table`, `RecordBatch`, `ArrowCursor`, and `__arrow_c_stream__` sources without calling `to_pylist()` or `to_pydict()` (R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgreSQL binary COPY, temporal wire_text refusal, numeric, array, composite, and vector contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Bound pg_catalog column discovery, identifier quoting, and CopyFail rollback rules.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor `buffer_view` consumption without row decoding.
