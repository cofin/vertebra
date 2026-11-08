---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch19-native-arrow-batches-and-shm
title: "Chapter 19: Lossless Native Arrow Schemas, Bounded RecordBatches, and Zero-Copy VTBARROW SHM"
description: Define lossless PostgreSQL-to-Arrow schemas, build bounded scalar and nested RecordBatches directly in Rust, and serialize adaptive inline or zero-copy VTBARROW shared-memory IPC streams without Python row materialization.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch18-binary-copy-streaming
replan_trigger: null
current_task: "native-postgres-driver:ch19:t01"
plan_revision: 8
plan_commit: 8c540809
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Lossless PostgreSQL value, Arrow result, and VTBARROW capsule contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL binary/text wire decoding and Arrow batch ownership invariants
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Shared VTBARROW 16-byte header, 8-byte frame padding, and unregistered SHM writer
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor
    title: Cross-extension PyCapsule ShmCapsule handoff to vertebra._vertebra.ArrowCursor
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Engine-agnostic public ArrowCursor wrapper over inline bytes and native SHM cursors
tags:
  - postgres
  - rust
  - arrow
  - shm
  - ipc
---

# Chapter 19: Lossless Native Arrow Schemas, Bounded RecordBatches, and Zero-Copy VTBARROW SHM

<!-- truth: start -->
- **Lossless Full-Range Arrow Schema (`postgres.arrow.version = "1"`)**: `PostgresArrowSchema` maps every PostgreSQL scalar, canonical decimal `numeric` (`Utf8`), full-range temporal `Struct` (`kind`, raw PG epoch `value`, `wire_text`, `date_style`, `time_zone`), `interval`, multi-dimensional array, ordered catalog composite, `vector`/`halfvec`, and `bit`/`varbit` without lossy Arrow temporal clamping or float rounding [^pg-contracts] [^pg-protocol-research].
- **Bounded Rust `RecordBatch` Construction**: `PostgresArrowBatchBuilder` decodes raw `DataRow` fields directly in Rust with zero Python tuple/dict materialization, flushing at `65_536` rows or `8 MiB` value bytes and bounding recursive catalog nesting at depth `32` [^pg-contracts].
- **Single-Stream `VTBARROW` SHM Ownership**: `PostgresArrowIpcWriter` owns one `arrow_ipc::writer::StreamWriter<Vec<u8>>`, promotes complete aligned message groups above `threshold_rows` into `ShmArrowStreamWriter::finish_unregistered`, and hands `Arc<ShmRegion>` across `.so` boundaries via `construct_py_arrow_cursor` [^arrow-shm-rs] [^py-async-rs] [^arrow-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Legacy PostgreSQL Arrow selection materializes Python row tuples before building Arrow tables or coerces PostgreSQL `numeric`, `interval`, `±infinity` timestamps, `24:00:00` `time`, negative-lower-bound multidimensional arrays, composites, and `pgvector` columns into lossy shapes [^pg-contracts] [^arrow-py]. Chapter 19 builds the pure-Rust Arrow data plane in `src/rs/vertebra-postgres/src/arrow.rs` over the `arrow-array`, `arrow-buffer`, `arrow-ipc`, `arrow-schema` (`=60.1.0`, `default-features = false`) and `half = "=2.7.1"` crates already declared in `src/rs/vertebra-postgres/Cargo.toml` and locked in `src/rs/Cargo.lock` from `ch01:t01`–`t02`, pairs it with `ShmArrowStreamWriter` and `construct_py_arrow_cursor` for zero-copy `VTBARROW` POSIX shared-memory transfer, defines `PostgresArrowResult` in `src/py/vertebra/db/adapters/postgres/types.py`, and adds engine-agnostic `row_count` metadata validation to `src/py/vertebra/db/arrow.py::ArrowCursor` [^arrow-shm-rs] [^py-async-rs] [^arrow-py].

### 1.2 Non-Goals

- Arrow-to-`COPY FROM STDIN` batch import (`PostgresArrowImportPlan`) and `SyncPostgresClient` / `AsyncPostgresClient` `select_to_arrow` and `stream_arrow_batches` portal facades (owned by Chapter 20).
- High-level `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` `select_to_arrow` and `load_arrow` adapter wiring (owned by `ch26:t03`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Protocol research: [protocol-and-execution_20261105.md](../../research/protocol-and-execution_20261105.md). Skills: [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R20.1 (`ch19:t01`)**: Consume the `arrow-array`, `arrow-buffer`, `arrow-ipc`, `arrow-schema` (`=60.1.0`, `default-features = false`) and `half = "=2.7.1"` crates already declared in `src/rs/vertebra-postgres/Cargo.toml` and locked in `src/rs/Cargo.lock` from `ch01:t01`–`t02` (with zero manifest/lockfile churn, zero second Arrow major, and zero `arrow-pyarrow` FFI crate) and implement `PostgresArrowSchema::from_columns(columns: &[crate::result::Column], catalog: &TypeCatalog)` with `"postgres.arrow.version" = "1"`, per-field `"postgres.oid"` / `"postgres.typmod"` / `"postgres.type_name"` / `"postgres.encoding"` metadata, lossless full-range PostgreSQL structural types, unknown-OID text-format requirement, and depth-`32` / cycle validation.
- **R20.2 (`ch19:t02`)**: Implement `PostgresArrowBatchBuilder` decoding raw `DataRow` fields via shared `codecs::decode_value_with_catalog` directly into Arrow builders in Rust, flushing at `max_rows` (default `65_536`) or `max_batch_bytes` (default `8 MiB`), rejecting single values above `8 MiB` before `reserve`, preserving ordered duplicate column names, distinguishing SQL `NULL` from `PgJson(Null)` and temporal `wire_text`, and emitting an explicit `0`-row `RecordBatch` for empty results.
- **R20.3 (`ch19:t03`)**: Extend `PostgresArrowBatchBuilder` recursively up to depth `32` for PostgreSQL arrays (`dimensions`, `lower_bounds`, flat row-major `values`, `0`-D empty arrays, negative lower bounds), ordered non-dropped catalog composites, `vector` / `halfvec` (`FixedSizeList<Float32>` when typmod dimension is fixed else `List<Float32>`), `bit` / `varbit` (`Struct(bytes: Binary, bit_length: Int32)`), domains, and enums across multiple batches and `arrow-ipc` roundtrips.
- **R20.4 (`ch19:t04`)**: Implement `PostgresArrowIpcWriter` owning one `StreamWriter<Vec<u8>>` across `Adaptive { threshold_rows }` and `AlwaysShm` modes under `max_result_bytes` (default `256 MiB`), promoting complete aligned IPC message groups to `ShmArrowStreamWriter` and finalizing via `finish_unregistered()`; convert `ArrowOutput` on the Python loop thread into `PostgresArrowResult` (`types.py`) via `construct_py_arrow_cursor`; extend `vertebra.db.arrow.ArrowCursor` with validated keyword-only `row_count: int | None = None`.

### 2.2 Concrete Type & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const DEFAULT_ARROW_BATCH_ROWS: usize = 65_536;`
  - `pub const DEFAULT_ARROW_BATCH_BYTES: usize = 8 * 1024 * 1024;`
  - `pub const DEFAULT_MAX_ARROW_RESULT_BYTES: usize = 256 * 1024 * 1024;`
  - `pub const MAX_ARROW_NESTING_DEPTH: usize = 32;`
  - `pub struct PostgresArrowSchema` with `pub fn from_columns(columns: &[crate::result::Column], catalog: &TypeCatalog) -> Result<Arc<Schema>, PostgresError>` (`PostgresColumn` is Python-only in `types.py`).
  - `pub struct PostgresArrowBatchBuilder` with `pub fn try_new(columns: &[crate::result::Column], catalog: &TypeCatalog, max_rows: usize, max_bytes: usize) -> Result<Self, PostgresError>`, `pub fn append_data_row(&mut self, fields: &[Option<&[u8]>], ctx: &CodecContext, catalog: &TypeCatalog) -> Result<Option<RecordBatch>, PostgresError>`, `pub fn append_pg_row(&mut self, row: &[PgValue]) -> Result<Option<RecordBatch>, PostgresError>`, `pub fn flush_batch(&mut self) -> Result<Option<RecordBatch>, PostgresError>`, and `pub fn finish_empty_or_tail(&mut self) -> Result<Option<RecordBatch>, PostgresError>`.
  - `pub enum ArrowStorageMode { Adaptive { threshold_rows: usize }, AlwaysShm }`
  - `pub enum ArrowOutput { Inline { ipc_bytes: Vec<u8>, row_count: usize, columns: Vec<String> }, Shm { shm_name: String, byte_len: usize, row_count: usize, columns: Vec<String>, region: Arc<ShmRegion> } }`
  - `pub struct PostgresArrowIpcWriter` with `pub fn try_new(schema: Arc<Schema>, columns: Vec<String>, mode: ArrowStorageMode, max_result_bytes: usize) -> Result<Self, PostgresError>`, `pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PostgresError>`, and `pub fn finish(self) -> Result<ArrowOutput, PostgresError>`.
  - `pub fn arrow_output_into_py_result(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>>` and `pub fn shm_output_into_py_cursor(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>>`.
- `src/py/vertebra/db/adapters/postgres/types.py`:
  - `class PostgresArrowResult(msgspec.Struct, frozen=True, gc=False): buffer: bytes | Any; row_count: int; columns: tuple[str, ...]; used_shm: bool` (where `buffer` is `bytes` when `used_shm` is `False` and `vertebra._vertebra.ArrowCursor` when `used_shm` is `True`).
- `src/py/vertebra/db/arrow.py`:
  - `ArrowCursor.__init__(self, buffer: Buffer | Any, *, rows: Sequence[tuple[Any, ...]] = (), columns: Sequence[str] = (), used_shm: bool = False, mmap_obj: mmap.mmap | None = None, shm_cursor: Any = None, row_count: int | None = None) -> None`.

### 2.3 Caller -> Seam -> Collaborator Map

```
Raw DataRow / Portal PgValue stream
  -> PostgresArrowSchema::from_columns + PostgresArrowBatchBuilder (src/rs/vertebra-postgres/src/arrow.rs)
    -> codecs::decode_value_with_catalog + TypeCatalog (src/rs/vertebra-postgres/src/types.rs; pure Rust, zero Python GIL/tuple materialization)
    -> PostgresArrowIpcWriter (single arrow_ipc::writer::StreamWriter<Vec<u8>>)
      -> Inline Vec<u8> (<= threshold_rows) OR ShmArrowStreamWriter::finish_unregistered (> threshold_rows / AlwaysShm)
        -> arrow_output_into_py_result on loop/caller thread -> construct_py_arrow_cursor (PyCapsule ShmCapsule)
          -> PostgresArrowResult (types.py) / vertebra.db.arrow.ArrowCursor (arrow.py)
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Failure / Edge Condition | Expected Behavior |
| --- | --- |
| Unknown extension OID returned in binary format (`format == 1`) | `PostgresArrowSchema::from_columns` rejects with `PostgresError::Data` instead of guessing UTF-8 over opaque binary bytes; text format (`format == 0`) maps to `Utf8` with exact OID metadata. |
| Recursive composite/array/domain catalog cycle or depth `> 32` | Fails closed with `PostgresError::Data` before query execution or builder allocation. |
| `numeric` scale `1000`, `12.3400`, `NaN`, `Infinity`, `-Infinity` | Stored in canonical decimal `Utf8` (`"postgres.encoding" = "numeric-decimal"`), preserving exact trailing zeros and specials without float or `Decimal128` overflow. |
| Temporal `±infinity`, BC/far-future out-of-Python range, `24:00:00`, or `wire_text` | Preserved in full-range PostgreSQL `Struct` (`kind = -1/0/1` with PG epoch integer, or `kind = 2` + null `value` + `wire_text`/`date_style`/`time_zone`); `time` `Int64` keeps `86_400_000_000`. |
| Unaligned variable-width batch frames written into `VTBARROW` SHM | `StreamWriter<Vec<u8>>` emits complete 8-byte-aligned schema and dictionary+batch groups drained via `get_mut()`, avoiding arbitrary `Write` fragment padding corruption in `ShmArrowStreamWriter`. |
| Retained `memoryview` / `pyarrow.Table` after `ArrowCursor.close()` or `del cursor` | `ShmCapsule` transfers `Arc<ShmRegion>` ownership to `vertebra._vertebra.ArrowCursor`, keeping the mmap alive until all exported buffer views release and then dropping the producer mapping cleanly. |
| Negative, `bool`, or conflicting `row_count` passed to `vertebra.db.arrow.ArrowCursor` | Raises `ValueError` immediately (including `row_count=0` when native `_shm_cursor.row_count > 0` or `len(rows) > 0`). |

### 2.5 Alternatives Considered

| Decision | Chosen Seam & Rejected Alternative |
| --- | --- |
| PostgreSQL full-range temporal/interval/numeric Arrow representation vs standard Arrow `Timestamp`/`Decimal128` | Structural full-range schemas (`Struct` for date/timestamp/interval, canonical decimal `Utf8` for `numeric`) preserve PG `±infinity`, `4713 BC..294276 AD`, `wire_text`, mixed-sign month intervals, and `1000`-digit scale. Standard Arrow temporal/decimal types overflow or drop PostgreSQL sentinels. |
| Single `StreamWriter<Vec<u8>>` message-group drain vs custom `std::io::Write` wrapper over `ShmArrowStreamWriter` | Draining complete schema and batch message groups from one `StreamWriter<Vec<u8>>` preserves dictionary delta state and 8-byte IPC framing, whereas forwarding partial `Write::write` calls to `ShmArrowStreamWriter` corrupts FlatBuffers alignment because every append pads to 8 bytes. |
| Cross-`.so` SHM handoff | Existing `construct_py_arrow_cursor` `c"vertebra.ShmCapsule"` C ABI callback contract (`ADR-004`). Reconstructing `Arc<ShmRegion>` across `cdylib` allocators or using a global name registry would cause UB or leak segments. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch19]` (public Arrow data-plane docs in `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, and OKF chapters in `.agents/bundles/knowledge/domains/data/` are updated in `ch30:t01` and `ch30:t02`; docstrings on `ArrowCursor`, `PostgresArrowResult`, and Rust `arrow.rs` types land inline in `ch19:t01`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R20.1 | [t01](tasks/t01-lossless-arrow-schemas.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |
| R20.2 | [t02](tasks/t02-bounded-arrow-scalar-batches.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |
| R20.3 | [t03](tasks/t03-nested-arrow-arrays-composites-vectors.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` |
| R20.4 | [t04](tasks/t04-arrow-ipc-to-shm.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs`, `src/py/tests/unit/db/test_postgres_native_data.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch19:t01 — Define lossless PostgreSQL Arrow schemas over locked workspace Arrow crates (tasks/t01-lossless-arrow-schemas.md)
- [ ] native-postgres-driver:ch19:t02 — Build bounded native Arrow scalar RecordBatches in Rust (tasks/t02-bounded-arrow-scalar-batches.md)
- [ ] native-postgres-driver:ch19:t03 — Build recursive Arrow builders for arrays, composites, pgvector, and bitstrings (tasks/t03-nested-arrow-arrays-composites-vectors.md)
- [ ] native-postgres-driver:ch19:t04 — Serialize adaptive inline and VTBARROW SHM IPC streams with validated ArrowCursor counts (tasks/t04-arrow-ipc-to-shm.md)

**Chapter Recovery Checkpoint:** `make dev && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests && uv run --no-sync python -m mypy.stubtest vertebra._postgres && uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k arrow_count -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `plan_commit: dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch20-arrow-copy-import-and-portals` (Arrow `COPY` import plans, streaming `COPY` input bridges, and sync/async Arrow selection and portal streams).

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Lossless PostgreSQL value, Arrow result, and VTBARROW capsule contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL binary/text wire decoding and Arrow batch ownership invariants.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Shared VTBARROW 16-byte header, 8-byte frame padding, and unregistered SHM writer.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — Cross-extension PyCapsule ShmCapsule handoff to `vertebra._vertebra.ArrowCursor`.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Engine-agnostic public ArrowCursor wrapper over inline bytes and native SHM cursors.
