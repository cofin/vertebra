---
type: Task
id: "native-postgres-driver:ch20:t01"
chapter_id: ch20-arrow-copy-import-and-portals
title: Convert Arrow RecordBatches into binary COPY fields with prevalidated target plans and IPC batch iterators
description: Implement parse_postgres_qualified_table, post-ch19 PostgresArrowImportPlan, generation-fenced SessionHandle::copy_from_arrow_with_generation_as preserving BinaryCopyEncoder header state, CopyFromStream::write_arrow_ipc, and schema_name-aware Python Arrow IPC batch iterators without Python row materialization.
status: stable
state: completed
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
  - src/rs/vertebra-postgres/src/session.rs
  - src/py/vertebra/db/adapters/postgres/arrow.py
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs
estimated_loc: 245
plan_revision: 6
plan_commit: ef86844f
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T16:58:55Z"
last_verified_commit: 34a428d7
verification_evidence: "cargo test -p vertebra-postgres --test postgres_arrow_import_tests (10 passed), cargo clippy -p vertebra-postgres --all-targets -- -D warnings, ruff check, and mypy on postgres/arrow.py exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T16:58:55Z"
commit: 34a428d7
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: ef86844f
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

Implement `parse_postgres_qualified_table` and post-`ch19` `PostgresArrowImportPlan` in `src/rs/vertebra-postgres/src/arrow.rs`, `SessionHandle::{copy_from_arrow, copy_from_arrow_as, copy_from_arrow_with_generation_as}` + an internal `CopyFromStream` constructor preserving `BinaryCopyEncoder.header_written` + `CopyFromStream::write_arrow_ipc` in `src/rs/vertebra-postgres/src/copy.rs`, and `schema_name`-aware bounded Python IPC batch chunkers (`iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync`, `load_arrow_async`) in `src/py/vertebra/db/adapters/postgres/arrow.py` [^pg-contracts] [^pg-protocol-research] [^arrow-py]. PyO3 client bindings (`SyncPostgresClient` / `AsyncPostgresClient` `copy_to`, `copy_from`, `copy_from_arrow`, and `write_arrow_ipc`) land in `ch20:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 `PostgresArrowImportPlan`, `copy_from_arrow(..., schema_name=...)`, encoder header preservation, and bounded Python Arrow IPC batch streaming contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — Post-`ch19` `PostgresArrowSchema` (`postgres.arrow.version = "1"`, `"pg-enum"`, `"pg-timestamptz"`, `"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `"pg-array"`, `"pg-composite"`) and Arrow `60.1.0` array types (`ch19:t01`–`t04`).
- `src/rs/vertebra-postgres/src/copy.rs` — `BinaryCopyEncoder` (`header_written`), `CopyFromStream`, `CopyFromStream::flush_encoded_slice_internal`, and `abort_copy_in_lease` (`ch18:t01`–`t03`).
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle::run_cancellable_operation`, `SessionHandle::copy_from_with_generation_as`, `CopySessionLease`, `OperationGeneration`, `TransactionOwner`, and `TypeCatalog::discover` (`ch13:t01`, `ch16:t03`, `ch18:t03`).
- `src/rs/vertebra-postgres/src/codecs/mod.rs` — `codecs::supports_binary_with_catalog`, `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)`, and OID constants (`OID_OID`, `REGPROC_OID`, `XID_OID`, `CID_OID`, `XML_OID`, `UNKNOWN_OID`, `BOX_OID`, `RECORD_OID`).
- [contracts.md](../../../contracts.md) — `copy_from_arrow` / `write_arrow_ipc` signatures, `wire_text` refusal, precision/timezone loss guards, `ArrowCursor.buffer_view()` zero-copy IPC slice consumption, and `PgArray`/`PgComposite`/`PgVector`/`PgBitString` invariants [^pg-contracts] [^arrow-py].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const MAX_ARROW_COPY_CALL_BYTES: usize = 16 * 1024 * 1024;`
  - `pub const MAX_ARROW_COPY_BATCH_BYTES: usize = 8 * 1024 * 1024;`
  - `pub fn parse_postgres_qualified_table(table_name: &str, schema_name: Option<&str>) -> Result<(Option<String>, String), PostgresError>`:
    - When `schema_name` is `Some(s)`: validates `s` and `table_name` directly as literal PostgreSQL identifiers (rejecting empty strings or embedded `\0` with `PostgresError::Data`, quoting each as `"..."` with `"` escaped to `""`, and NEVER splitting dots so literal `table_name = "odd.table"` with `schema_name = Some("analytics")` resolves `("analytics", "odd.table")`).
    - When `schema_name` is `None`: runs a strict PostgreSQL double-quote-aware splitter rejecting unclosed `"`, empty segments (`a..b`, `.a`, `a.`, `""`), embedded `\0`, or `> 2` segments while parsing `"my ""schema"""."odd.table"` into `(Some("my \"schema\"".into()), "odd.table".into())`.
  - `pub struct PostgresArrowImportPlan`:
    - `pub fn from_schema(schema: &Schema, target_columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Self, PostgresError>`:
      - Resolves domain chains via `catalog.resolve_domain_base(col.type_oid)` and rejects column-count mismatch, missing/unknown/duplicate target columns, or incompatible Arrow `DataType` / `"postgres.oid"` identity with `PostgresError::Data`.
      - Accepts post-`ch19` (`3c7a9355`) `"postgres.arrow.version" == "1"` encodings: binary scalars/temporals/intervals/vectors/varbits, `"numeric-decimal"` via the dedicated PostgreSQL numeric parser (`NaN`/`±Infinity`/scale preserved), top-level `"pg-enum"` (`DataType::Utf8` -> `PgValue::Text`), `DataType::UInt32` `"scalar"` for `oid` (`26`), binary `regproc` (`24`), `xid` (`28`), and `cid` (`29`) via `PgValue::Int8(i64::from(u32_val))`, `xml` (`142`) `"scalar"` `Utf8` -> `PgValue::Text`, `"unknown"` (`705`) `Utf8` targeting text-family columns (`text`/`varchar`/`bpchar`/`name`/`xml`) while rejecting target pseudo-column `705`, and `"pg-timestamptz"` (`temporal_struct_data_type(DataType::Int64)`).
      - Rejects at schema-plan construction time (including `0`-row schema-only first batches) with `PostgresError::Data` before `COPY` starts: `"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `BOX_OID` (`603`), `RECORD_OID` (`2249`), and any `"pg-array"` or `"pg-composite"` column where `codecs::supports_binary_with_catalog(base_target_oid, catalog)` returns `false` (such as arrays or composites containing enums, ranges, `xml`, `box`, or anonymous records).
      - For ordinary Arrow schemas without PostgreSQL metadata, maps primitives with checked signed/unsigned narrowing (`UInt8/16/32/64`, `Int8/16/32/64`, `Float16/32/64`, `Boolean`, `Utf8`/`LargeUtf8`/`Utf8View`, `Binary`/`LargeBinary`/`FixedSizeBinary`), `Decimal128`/`Decimal256`, `Date32`/`Date64` (checked PG epoch conversion), `Timestamp(Second|Millisecond|Microsecond|Nanosecond, tz)` (requiring `ns % 1000 == 0`, normalizing `tz`-aware values to UTC for `timestamptz`, and rejecting `tz`-aware `Timestamp` -> naive `timestamp` loss), `Time32`/`Time64` (`ns % 1000 == 0`), `List`/`LargeList`/`FixedSizeList` -> 1D lower-bound-`1` `PgArray` (when `supports_binary_with_catalog` is `true`, or `PgVector` when target OID is `vector`/`halfvec`), `Struct` matching ordered non-dropped composite attributes without missing/extra fields (when `supports_binary_with_catalog` is `true`), and direct Rust `DictionaryArray` value lookup.
    - `pub fn encode_batch(&self, batch: &RecordBatch, encoder: &mut BinaryCopyEncoder, ctx: &CodecContext, catalog: &TypeCatalog, out: &mut BytesMut) -> Result<(), PostgresError>`:
      - Validates batch schema compatibility and `batch.get_array_memory_size() <= MAX_ARROW_COPY_BATCH_BYTES` before encoding via `codecs::encode_value_with_catalog(..., Format::Binary, out, ctx, catalog)`; rejects any temporal or `"pg-timestamptz"` row with `kind == 2` (`wire_text`) with `PostgresError::Data` before any wire write.
  - `pub fn decode_bounded_ipc_batches(ipc_bytes: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), PostgresError>` (enforces `ipc_bytes.len() <= MAX_ARROW_COPY_CALL_BYTES` before `StreamReader` allocation).
- `src/rs/vertebra-postgres/src/copy.rs`:
  - Bound `pg_catalog` column resolver + custom OID discovery: resolves target `crate::result::Column` metadata in ONE bound `pg_catalog` query (`pg_class JOIN pg_namespace ON relnamespace = pg_namespace.oid JOIN pg_attribute ON attrelid = pg_class.oid`, `attnum > 0 AND NOT attisdropped`, using `current_schemas(false)` + `array_position` ordering when unqualified, with zero correlated scalar subqueries) and discovers custom array/composite/domain/enum/vector target OIDs into `conn.type_catalog` (`TypeCatalog::discover`) before calling `PostgresArrowImportPlan::from_schema`.
  - `SessionHandle::copy_from_arrow(self: &Arc<Self>, table_name: &str, schema_name: Option<&str>, first_batch_ipc: &[u8], columns: Option<&[String]>) -> Result<CopyFromStream, PostgresError>`, `SessionHandle::copy_from_arrow_as(..., owner: Option<TransactionOwner>)`, and `pub(crate) async fn copy_from_arrow_with_generation_as(self: &Arc<Self>, op_gen: OperationGeneration, table_name: &str, schema_name: Option<&str>, first_batch_ipc: &[u8], columns: Option<&[String]>, owner: Option<TransactionOwner>) -> Result<CopyFromStream, PostgresError>`:
    - Fences both the bound `pg_catalog` + `TypeCatalog::discover` resolution query (`run_cancellable_operation(..., SessionAccess::Ordinary, owner, ...)`) and `CopySessionLease` checkout / `start_copy_in` (`copy_from_with_generation_as`) under `op_gen` while joining any ambient `TransactionOwner`.
    - Decodes `first_batch_ipc`, builds `PostgresArrowImportPlan`, and pre-encodes all rows of `first_batch_ipc` into `payload` using a fresh `BinaryCopyEncoder` BEFORE issuing `COPY <quoted_target> [(<quoted_cols>)] FROM STDIN (FORMAT binary)` so schema or first-batch failures leave zero `COPY` side effects.
    - Constructs `CopyFromStream` through an internal constructor accepting the already-initialized `BinaryCopyEncoder` (preserving `header_written` from `first_batch_ipc` so batch `2` or `finish()` NEVER emits a duplicate `19`-byte `PGCOPY` header) plus `Some(plan)`, then flushes non-empty `payload` via `CopyFromStream::flush_encoded_slice_internal(&payload, false)`.
  - `CopyFromStream::write_arrow_ipc(&mut self, batch_ipc: &[u8]) -> Result<(), PostgresError>`:
    - Validates `batch_ipc.len() <= MAX_ARROW_COPY_CALL_BYTES` against the stored `PostgresArrowImportPlan`, encodes each `RecordBatch` into `payload` via `self.encoder`, and flushes via `self.flush_encoded_slice_internal(&payload, false)`; on any failure, aborts `CopyFromStream` (`CopyFail` + rollback of COPY-owned transaction) and returns the original `PostgresError`.
- `src/py/vertebra/db/adapters/postgres/arrow.py`:
  - Top-level module imports and zero inline `#` comments:
    - `def iter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> Iterator[bytes]`
    - `async def aiter_arrow_ipc_batches(source: Any, *, max_rows: int = 65_536, max_bytes: int = 8 * 1024 * 1024) -> AsyncIterator[bytes]`
    - `def load_arrow_sync(client: SyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> int`
    - `async def load_arrow_async(client: AsyncPostgresClient, table_name: str, source: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> int`
  - Calls `ensure_pyarrow()` from `vertebra.utils.module_loader` (`extra="arrow"`) before `COPY` side effects; accepts `pa.Table` (`to_reader(max_chunksize=max_rows)`), `pa.RecordBatch`, `ArrowCursor` (`pa.ipc.open_stream(cursor.buffer_view())`), `pa.RecordBatchReader`, and `__arrow_c_stream__` providers (`pa.RecordBatchReader.from_stream(source)`), closing readers in `finally`; halves slices exceeding `max_bytes` (raising `DataError` if `1` row exceeds `max_bytes`), yields a `0`-row schema-only IPC stream for empty inputs, forwards `columns=columns, schema_name=schema_name` to `copy_from_arrow`, calls `write_arrow_ipc(batch_ipc)` for remaining chunks, and NEVER calls `to_pylist()` or `to_pydict()`.

## Edge Cases & Error Semantics

- Schema-plan rejections (`"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `BOX_OID`, `RECORD_OID`, non-binary `"pg-array"`/`"pg-composite"` such as enum/range/record arrays or composites, target pseudo-column `705`), `0`-row schema-only invalid types, `UInt64 > i64::MAX`, `Timestamp(Nanosecond)` with non-zero `ns % 1000`, `tz`-aware `Timestamp` -> naive `timestamp`, temporal/`"pg-timestamptz"` `kind == 2` `wire_text`, or invalid identifiers (`a..b`, `.a`, `a.`, `""`, unclosed `"`, embedded `\0`, `> 2` segments) fail with `PostgresError::Data` BEFORE `COPY ... FROM STDIN` is sent.
- Multi-batch `copy_from_arrow` + `write_arrow_ipc` + `finish()` emits exactly one `19`-byte `PGCOPY\n\xff\r\n\0` header across the entire stream (including when `first_batch_ipc` has `0` rows followed by batch `2` or immediate `finish()`).
- Second-or-later batch value failure aborts the active `CopyFromStream` via `CopyFail` and rolls back the COPY-owned transaction (or leaves an ambient `TransactionOwner` transaction in `E`) without committing batch `1`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/rs/vertebra-postgres/tests/postgres_arrow_import_tests.rs`
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full post-`ch19` lossless roundtrip & single `PGCOPY` header across batches | Batch 1 + batch 2 + `finish()` covering scalars, scaled/special `numeric`, finite/infinite temporals (`"pg-timestamptz"` `kind = 0/±1`), `24:00:00` `time`, `interval`, top-level `"pg-enum"`, `UInt32` `oid`/`regproc`/`xid`/`cid`, `xml`, `"unknown"` -> `text`/`varchar`/`bpchar`/`name`/`xml`, domain chain over custom base OID, negative-lower-bound 2D `PgArray`, `PgComposite`, `vector(3)`, `halfvec`, and `varbit` | Wire capture starts with exactly one `19`-byte `PGCOPY` header and ends with `-1i16`; decoded `PgValue` rows match original values, lower bounds, null bitmaps, numeric scale, and bit precision bit-for-bit. |
| Ordinary Arrow types, dictionary arrays, and `0`-row -> batch-2 header preservation | `UInt16 -> int4`, `Decimal128(18, 4) -> numeric`, `Date32 -> date`, `Timestamp(Nanosecond, Some("America/New_York"))` (`.000_000_000`) -> `timestamptz`, `DictionaryArray<Int8, Utf8> -> text`, `List<Int32> -> int4[]`, plus `0`-row `first_batch_ipc` followed by `write_arrow_ipc(batch_2)` and `finish()` | Encodes exact binary `COPY` fields with a single `19`-byte `PGCOPY` header across `0`-row + non-empty batches; `List<Int32>` produces 1D `lower_bounds = [1]` `PgArray`. |
| Pre-`COPY` schema & value rejections (including `0`-row schema-only batches) | `"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `BOX_OID` (`603`), `RECORD_OID` (`2249`), enum-array / range-composite (`supports_binary_with_catalog == false`), target `UNKNOWN_OID` (`705`), `"pg-timestamptz"` `kind = 2` `wire_text`, `UInt64(u64::MAX) -> int8`, `500ns` remainder, `tz -> timestamp`, or `"a..b"` / unclosed `"` / `"bad\0table"` | Returns `Err(PostgresError::Data)` before emitting any `COPY` command on the scripted wire peer (even when `first_batch_ipc` contains `0` rows). |
| Literal `schema_name` vs quote-aware splitting, generation cancel, & batch-2 rollback | (a) `table_name = "odd.table"`, `schema_name = Some("analytics")`; (b) `table_name = "\"my \"\"schema\"\"\".\"odd.table\""`, `schema_name = None` with batch-2 `"pg-timestamptz"` `kind = 2`; (c) cancel `op_gen` during `pg_catalog` lookup and during `start_copy_in` | (a) Emits `COPY "analytics"."odd.table" ...` without splitting dots; (b) emits `COPY "my ""schema"""."odd.table" ...`, rejects batch 2 before sending its bytes, sends `CopyFail` + `ROLLBACK`; (c) fences `CancelRequest` and retires/recovers cleanly. |

## Verification

- Initial Red baseline: `PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests`
- Final Green + lint + type-check gate:
  ```bash
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/arrow.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/arrow.py
  ```
- Planned local commit: `feat(postgres): encode typed Arrow batches for COPY`

## Acceptance Criteria

- [x] `PostgresArrowImportPlan` discovers custom target OIDs and domain bases, roundtrips post-`ch19` `postgres.arrow.version = "1"` schemas (including `"pg-enum"`, `UInt32` `oid`/`regproc`/`xid`/`cid`, `xml`, and `"unknown"` -> text-family targets) plus ordinary Arrow types, and rejects `"regproc-text"`, `"pg-range"`, `"record-text"`, `"unknown-text"`, `BOX_OID`, `RECORD_OID`, target `705`, non-binary `"pg-array"`/`"pg-composite"`, and `"pg-timestamptz"` `kind = 2` before `COPY` startup (R20).
- [x] `parse_postgres_qualified_table`, the single bound `pg_catalog` `JOIN` query (zero correlated subqueries), `SessionHandle::copy_from_arrow_with_generation_as`, and the encoder-preserving `CopyFromStream` constructor fence async cancellation + ambient `TransactionOwner`, support literal `schema_name` with dotted table names, emit exactly one `19`-byte `PGCOPY` header across batches via `flush_encoded_slice_internal(&payload, false)`, and abort batch `>= 2` errors via `CopyFail` (R19, R20).
- [x] `src/py/vertebra/db/adapters/postgres/arrow.py` passes `ruff` and `mypy`, accepts and forwards `schema_name: str | None = None`, and serializes bounded `<= 8 MiB` self-contained IPC streams from `Table`, `RecordBatch`, `ArrowCursor`, and `__arrow_c_stream__` sources without calling `to_pylist()` or `to_pydict()` (R20).

## Notes & Discoveries

`OperationMeta::rereserve_op_gen_on_ok` atomically re-reserves `op_gen` inside `run_cancellable_operation`'s completion lock block so `copy_from_arrow_with_generation_as` bridges the bound `pg_catalog` resolution query into `CopySessionLease` checkout with zero cancellation gap.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgreSQL binary COPY, temporal wire_text refusal, numeric, array, composite, and vector contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Bound pg_catalog column discovery, identifier quoting, and CopyFail rollback rules.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor `buffer_view` consumption without row decoding.
