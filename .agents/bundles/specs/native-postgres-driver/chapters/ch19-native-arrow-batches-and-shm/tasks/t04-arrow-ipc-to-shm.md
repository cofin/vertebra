---
type: Task
id: "native-postgres-driver:ch19:t04"
chapter_id: ch19-native-arrow-batches-and-shm
title: Serialize adaptive inline and VTBARROW SHM IPC streams with validated ArrowCursor counts
description: Implement PostgresArrowIpcWriter over a single StreamWriter<Vec<u8>> with ShmArrowStreamWriter promotion, cross-extension ShmCapsule PostgresArrowResult conversion, and validated row_count metadata on vertebra.db.arrow.ArrowCursor.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch19:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch19-native-arrow-batches-and-shm/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-db/src/arrow_shm.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/db/arrow.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/py/vertebra/db/arrow.py
  - src/py/vertebra/db/adapters/postgres/types.py
tests:
  - src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 245
plan_revision: 5
plan_commit: 79eea133
state_revision: 2
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T04:06:23Z"
last_verified_commit: fe925db7
verification_evidence: "make postgres-test -> 0; cargo clippy -p vertebra-postgres --all-targets -- -D warnings -> 0; uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k arrow_count -q -> 28 passed; uv run --no-sync python -m mypy.stubtest vertebra._postgres -> 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T04:10:58Z"
commit: df033deb
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: PostgresArrowResult and ArrowCursor row_count and VTBARROW ownership contracts
  - id: arrow-shm-rs
    resource: src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter
    title: Page-aligned VTBARROW 8-byte padded frame writer and finish_unregistered
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor
    title: Cross-cdylib PyCapsule ShmCapsule handoff to vertebra._vertebra.ArrowCursor
  - id: shm-core-rs
    resource: src/rs/vertebra-core/src/shm.rs::ShmCapsule
    title: C-ABI producer drop callback contract for shared-memory regions
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Engine-agnostic public ArrowCursor wrapper
tags:
  - postgres
  - rust
  - python
  - arrow
  - shm
---

# Task t04: Serialize Adaptive Inline and VTBARROW SHM IPC Streams with Validated ArrowCursor Counts

## Objective & Scope Boundary

Implement `PostgresArrowIpcWriter` and cross-extension `ArrowOutput` conversion in `src/rs/vertebra-postgres/src/arrow.rs`, add frozen `PostgresArrowResult` in `src/py/vertebra/db/adapters/postgres/types.py`, and add validated keyword-only `row_count: int | None = None` support to `src/py/vertebra/db/arrow.py::ArrowCursor` [^pg-contracts] [^arrow-shm-rs] [^py-async-rs] [^arrow-py]. Client `select_to_arrow` and `stream_arrow_batches` exports follow in `ch20:t03` and `ch20:t04`, so Rust integration tests exercise the production `arrow_output_into_py_result` converter directly against the independently loaded `vertebra._vertebra` extension without exposing a test-only Python factory.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 19 single-stream `PostgresArrowIpcWriter`, `VTBARROW` SHM promotion, and `PostgresArrowResult` contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — `PostgresArrowSchema` and `PostgresArrowBatchBuilder` (`ch19:t01`–`t03`).
- `src/rs/vertebra-db/src/arrow_shm.rs` — `ShmArrowStreamWriter::create`, `append_ipc_frame`, and `finish_unregistered` (`8`-byte padding invariants) [^arrow-shm-rs].
- `src/rs/vertebra-db/src/py_async.rs` — `construct_py_arrow_cursor` and `wrap_shm_region_capsule` (`c"vertebra.ShmCapsule"`) [^py-async-rs].
- `src/py/vertebra/db/arrow.py` — `ArrowCursor.__slots__`, `__init__`, `__len__`, `num_rows`, `row_count` [^arrow-py].
- [contracts.md](../../../contracts.md) — `PostgresArrowResult` (`src/py/vertebra/db/adapters/postgres/types.py` frozen `msgspec.Struct(frozen=True, gc=False)`) and `ArrowCursor` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - `pub const DEFAULT_MAX_ARROW_RESULT_BYTES: usize = 256 * 1024 * 1024;`
  - `pub enum ArrowStorageMode { Adaptive { threshold_rows: usize }, AlwaysShm }`
  - `pub enum ArrowOutput { Inline { ipc_bytes: Vec<u8>, row_count: usize, columns: Vec<String> }, Shm { shm_name: String, byte_len: usize, row_count: usize, columns: Vec<String>, region: Arc<ShmRegion> } }`
  - `pub struct PostgresArrowIpcWriter`:
    - Owns ONE `arrow_ipc::writer::StreamWriter<Vec<u8>>` across its entire lifetime so dictionary trackers stay coherent across batches; drains complete 8-byte-aligned schema and dictionary+batch message groups via `stream_writer.get_mut().clear()` and NEVER passes arbitrary `std::io::Write` fragments to `ShmArrowStreamWriter` (because every `ShmArrowStreamWriter` append pads to 8 bytes).
    - `pub fn try_new(schema: Arc<Schema>, columns: Vec<String>, mode: ArrowStorageMode, max_result_bytes: usize) -> Result<Self, PostgresError>` (validates `max_result_bytes > 0`, initializes `StreamWriter::try_new(Vec::new(), &schema)`, drains complete `schema_ipc_frame`; in `AlwaysShm` mode immediately creates `ShmArrowStreamWriter::create` with unique `<= 31`-byte `/vtb-pg-{pid:x}-{seq:x}` name, while `Adaptive` buffers `schema_ipc_frame` + complete batch groups inline while `total_rows <= threshold_rows`)
    - `pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), PostgresError>` (writes `batch` into `StreamWriter`, drains complete batch message group, enforces `total_ipc_bytes <= max_result_bytes` before copying/promoting, promotes already-encoded schema + batch groups to `ShmArrowStreamWriter` as soon as `total_rows > threshold_rows`, then appends subsequent complete groups via `append_ipc_frame`)
    - `pub fn finish(self) -> Result<ArrowOutput, PostgresError>` (calls `StreamWriter::finish()` once; inline retains `StreamWriter`'s single 8-byte EOS marker, whereas SHM discards `StreamWriter`'s trailing EOS and calls `ShmArrowStreamWriter::finish_unregistered()` so `VTBARROW` header + sole EOS are written once without touching `ACTIVE_ARROW_SHM_REGIONS`)
  - `pub fn arrow_output_into_py_result(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>>` and `pub fn shm_output_into_py_cursor(py: Python<'_>, output: ArrowOutput) -> PyResult<Py<PyAny>>`:
    - `arrow_output_into_py_result` imports `PostgresArrowResult` from `vertebra.db.adapters.postgres.types`; for `Inline`, passes `PyBytes` + `used_shm = false`; for `Shm`, builds `py_cols` from `columns`, constructs `vertebra._vertebra.ArrowCursor` via `vertebra_db::py_async::construct_py_arrow_cursor` (`ShmCapsule` producer drop callback), and passes `used_shm = true`, while `shm_output_into_py_cursor` provides standalone `ArrowOutput::Shm -> vertebra._vertebra.ArrowCursor` conversion for streaming batch handoff (rejecting `ArrowOutput::Inline`) [^shm-core-rs].
- `src/py/vertebra/db/adapters/postgres/types.py`:
  - Add `PostgresArrowResult` to `__all__`:
    ```python
    class PostgresArrowResult(msgspec.Struct, frozen=True, gc=False):
        buffer: bytes | Any
        row_count: int
        columns: tuple[str, ...]
        used_shm: bool
    ```
    with `__post_init__` validating non-`bool` `row_count >= 0`, `used_shm` is `bool`, and `isinstance(buffer, bytes)` when `not used_shm`.
- `src/py/vertebra/db/arrow.py`:
  - Extend `ArrowCursor.__slots__` with `"_row_count"` and add keyword-only `row_count: int | None = None` to `ArrowCursor.__init__`:
    - Remains 100% engine-agnostic (zero PostgreSQL imports).
    - Rejects `isinstance(row_count, bool)` or `row_count < 0` (`TypeError` / `ValueError`).
    - When `rows` is non-empty and `row_count is not None and row_count != len(rows)`, raises `ValueError`.
    - When `_shm_cursor` exposes `row_count` with `int(_shm_cursor.row_count) > 0` and `row_count is not None and row_count != int(_shm_cursor.row_count)` (including explicit `row_count=0` against non-zero native count), raises `ValueError`.
    - `__len__` (and thus `num_rows` / `row_count`) returns `len(self._rows)` if `self._rows`, else native `int(self._shm_cursor.row_count)` when `> 0`, else `self._row_count` in `O(1)` without decoding IPC rows when `self._row_count is not None`, else falls back to `self._ipc_table()` (`0` when absent).

## Edge Cases & Error Semantics

- `threshold_rows = 0`: `0`-row result (`0 <= 0`) stays inline `bytes` with valid schema + single EOS; `1`-row result (`1 > 0`) promotes to `VTBARROW` SHM. Exact-boundary (`total_rows == threshold_rows`) stays inline; `total_rows == threshold_rows + 1` promotes to SHM.
- Unaligned string/binary payload lengths (e.g., `1`, `3`, `7` bytes) across multiple batches and forced SHM page growth (`ensure_capacity` doubling) decode cleanly via `arrow_ipc::reader::StreamReader` and `pyarrow.ipc.open_stream` with exact row count and exactly one trailing `ARROW_IPC_EOS_MARKER`.
- Breaching `max_result_bytes` fails with `PostgresError::Data` / `Operational` and drops any owned `ShmRegion` immediately without leaking `/dev/shm` mappings.

## Public-Seam Test Blueprint

- **Canonical test files**:
  - `src/rs/vertebra-postgres/tests/postgres_arrow_tests.rs` (`ipc_*` tests including embedded-Python cross-`.so` `construct_py_arrow_cursor` lifetime test)
  - `src/py/tests/unit/db/test_postgres_native_data.py` (`test_arrow_count_*` suite)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zero/multi-batch unaligned IPC & forced SHM growth | (a) `0` batches; (b) `4` batches with `1`/`3`/`7`-byte strings + `128 KiB` payload forcing `ShmArrowStreamWriter` resize | Valid IPC stream decodable by `StreamReader` with exact schema, values, row count, and single 8-byte EOS marker (never double EOS or 8-byte pad corruption). |
| Adaptive `threshold_rows` `0` / exact / `+1` & `max_result_bytes` cap | `threshold_rows = 0` (`0` vs `1` row), `threshold_rows = 4` (`4` vs `5` rows), and `max_result_bytes = 1024` with `4 KiB` batch | `0` and `4` rows -> `ArrowOutput::Inline`; `1` and `5` rows -> `ArrowOutput::Shm`; `4 KiB > 1024` -> `Err` with zero leaked `/dev/shm` files. |
| Cross-`.so` `PostgresArrowResult` & retained `memoryview` / `pyarrow.Table` lifetime | Rust test calls `arrow_output_into_py_result` into loaded `vertebra._vertebra`, takes `mv = result.buffer.buffer_view()` and `tbl = pa.ipc.open_stream(mv).read_all()`, drops `result` and runs `gc.collect()` | `PostgresArrowResult(used_shm=True, row_count=N)` is frozen; `mv` and `tbl` remain readable after `result` drop; dropping `mv` + `tbl` invokes `ShmCapsule` drop callback and releases producer `Arc<ShmRegion>`. |
| Generic `vertebra.db.arrow.ArrowCursor` `row_count` validation | Valid inline IPC `bytes` with `row_count=0` and `row_count=3`; negative `-1`, `row_count=True`, `rows=[(1,)]` + `row_count=2`, and native SHM cursor (`row_count=5`) + `row_count=0` or `row_count=4` | Valid inline counts return `len(c) == c.num_rows == c.row_count` without row hydration; negative, `bool`, and conflicting row/SHM counts raise `TypeError` / `ValueError`. |

## Verification

- Initial Red baseline: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests ipc_`
- Final Green + stubtest + pytest gate:
  ```bash
  make dev
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests ipc_
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k arrow_count -q
  ```
- Planned local commit: `feat(postgres): preserve adaptive Arrow IPC output ownership`

## Acceptance Criteria

- [x] `PostgresArrowIpcWriter` owns one `StreamWriter<Vec<u8>>`, promotes complete aligned IPC message groups into `ShmArrowStreamWriter::finish_unregistered()` above `threshold_rows` (or in `AlwaysShm` mode), enforces `max_result_bytes`, and emits a single EOS marker (R20).
- [x] `arrow_output_into_py_result` constructs frozen `PostgresArrowResult` on the Python thread using `construct_py_arrow_cursor` (`c"vertebra.ShmCapsule"`), keeping retained `memoryview` / `pyarrow` views readable after cursor release and freeing the producer mapping on final drop (R20).
- [x] `vertebra.db.arrow.ArrowCursor` remains engine-agnostic, supports validated keyword-only `row_count: int | None = None` for inline IPC buffers without decoding rows, and rejects negative, `bool`, or conflicting row/native counts (R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresArrowResult and ArrowCursor row_count and VTBARROW ownership contracts.
[^arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::ShmArrowStreamWriter` — Page-aligned VTBARROW 8-byte padded frame writer and `finish_unregistered`.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::construct_py_arrow_cursor` — Cross-cdylib PyCapsule ShmCapsule handoff to `vertebra._vertebra.ArrowCursor`.
[^shm-core-rs]: `src/rs/vertebra-core/src/shm.rs::ShmCapsule` — C-ABI producer drop callback contract for shared-memory regions.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Engine-agnostic public ArrowCursor wrapper.
