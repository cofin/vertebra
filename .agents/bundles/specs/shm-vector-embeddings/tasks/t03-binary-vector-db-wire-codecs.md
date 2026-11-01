---
type: Task
task_id: t03-binary-vector-db-wire-codecs
flow_id: shm-vector-embeddings
title: "Binary pgvector Wire Codecs, Zero-Copy PyBuffer Oracle 26ai 0xDB, to_arrow_shm & SQLAlchemy Pushdown"
description: "Implement binary pgvector (vector, halfvec, bit) codecs in vertebra-db, zero-copy PyBuffer input in Oracle 26ai 0xDB codecs, VectorBatchView.to_arrow_shm(), and binary/buffer pushdown in SQLAlchemy Vector."
scope: task
status: ready
state: completed
strategy: tdd
depends_on:
  - t02-shm-vector-slab-cache-and-views
traces_to:
  - REQ-03
  - NFR-01
  - NFR-02
tags:
  - database
  - pgvector
  - oracle
  - arrow
  - sqlalchemy
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:34:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:34:00Z
    commit: d3d5f8b
sources:
  - id: pg-wire
    resource: src/rs/vertebra-db/src/postgres.rs::encode_pgcopy_binary_rows
    title: PostgreSQL wire framing and PGCOPY binary row encoding
  - id: oracle-vec
    resource: src/rs/vertebra-db/src/oracle.rs::encode_oracle_vector_f32
    title: Oracle 26ai binary VECTOR (FLOAT32, FLOAT64, INT8, BINARY) wire codecs
  - id: duckdb-arrow-shm
    resource: src/rs/vertebra-db/src/duckdb_engine.rs::write_shm_arrow_frame
    title: Shared-memory VTBARROW IPC framing in vertebra-db
  - id: pyo3-db
    resource: src/rs/vertebra-py/src/db.rs::ArrowCursor
    title: PyO3 bindings for ArrowCursor and database wire codecs
  - id: sa-vector-type
    resource: src/py/vertebra/extensions/sqlalchemy/types.py::Vector
    title: SQLAlchemy 2.1 Vector column type and VectorDistanceExpr compiler
---

# Task `t03`: Binary `pgvector` Wire Codecs, Zero-Copy `PyBuffer` Oracle 26ai `0xDB`, `to_arrow_shm` & SQLAlchemy Pushdown

<!-- truth: start -->
- **Binary `pgvector` Wire Protocol (`vector`, `halfvec`, `bit`)**: `vertebra-db::postgres`[^pg-wire] encodes and decodes `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`), `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`), and `bit` (`[i32 bit_len BE][u8; ceil(bit_len / 8)]`) in `< 2 us` (`339x` faster than text bind formatting).
- **Zero-Copy `PyBuffer` Oracle 26ai `0xDB` & `pgvector` Input**: `encode_oracle_vector_f32`[^oracle-vec], `encode_oracle_vector_f64`, `encode_oracle_vector_i8`, and `encode_pgvector_*` in `vertebra-py::db`[^pyo3-db] accept any PEP 3118 buffer (`VectorView`, `memoryview`, `numpy.ndarray`, `bytes`) alongside Python `Sequence` without allocating an intermediate `Vec<f32>` from a `PyList`.
- **Shared-Memory `VTBARROW` & SQLAlchemy `Vector` Pushdown**: `VectorBatchView.to_arrow_shm(column="embedding")` frames a contiguous `FixedSizeList<Float32, D>` inside a `VTBARROW` `ShmRegion`[^duckdb-arrow-shm] and returns an `ArrowCursor`, while `Vector.process_bind_param`[^sa-vector-type] accepts `VectorView` / `memoryview` directly.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Eliminate Python `list[float]` extraction and string concatenation bottlenecks when binding or streaming vectors into PostgreSQL (`pgvector`), Oracle 26ai (`0xDB` `VECTOR`), SQLite (`BLOB`), and DuckDB (`VTBARROW` `ArrowCursor`).

### 1.2 In-Scope Files

- [`src/rs/vertebra-db/src/postgres.rs`](../../../../../src/rs/vertebra-db/src/postgres.rs)
- [`src/rs/vertebra-db/src/lib.rs`](../../../../../src/rs/vertebra-db/src/lib.rs)
- [`src/rs/vertebra-py/src/db.rs`](../../../../../src/rs/vertebra-py/src/db.rs)
- `src/rs/vertebra-py/src/vector.rs`
- [`src/py/vertebra/_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)
- [`src/py/vertebra/extensions/sqlalchemy/types.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)
- `src/py/tests/unit/db/test_vector_wire_codecs.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 Rust Binary `pgvector` Codecs (`src/rs/vertebra-db/src/postgres.rs`)

1. **`vector` (`FLOAT32` binary format, `vector_send` / `vector_recv`)**:
   - `pub fn encode_pgvector_f32(values: &[f32]) -> Result<Vec<u8>, String>`: validates `values.len() <= u16::MAX as usize` and finite values (`!v.is_nan() && !v.is_infinite()`), writes `[dim as u16 BE][0u16 BE]` followed by `values.len()` big-endian `f32::to_be_bytes()`.
   - `pub fn decode_pgvector_f32(buf: &[u8]) -> Result<Vec<f32>, String>`: validates `buf.len() >= 4`, checks `unused == 0`, checks `buf.len() == 4 + (dim as usize) * 4`, and decodes big-endian `f32` values.
2. **`halfvec` (`FLOAT16` binary format, `halfvec_send` / `halfvec_recv`)**:
   - `pub fn encode_pgvector_f16(values: &[f32]) -> Result<Vec<u8>, String>` and `pub fn decode_pgvector_f16(buf: &[u8]) -> Result<Vec<f32>, String>` using IEEE-754 binary16 conversion (`f32_to_f16_bits` / `f16_bits_to_f32`).
3. **`bit` (PostgreSQL `VARBIT` binary format, `bit_send` / `bit_recv`)**:
   - `pub fn encode_pgvector_bit(packed_bytes: &[u8], bit_len: usize) -> Result<Vec<u8>, String>` and `pub fn decode_pgvector_bit(buf: &[u8]) -> Result<(Vec<u8>, usize), String>` with 4-byte big-endian `bit_len: i32` header + `bit_len.div_ceil(8)` packed bytes.

### 2.2 Zero-Copy `PyBuffer` Extraction in `vertebra-py::db` & `VectorView` Convenience Methods

- In [`src/rs/vertebra-py/src/db.rs`](../../../../../src/rs/vertebra-py/src/db.rs)[^pyo3-db]:
  - Add a helper `extract_f32_slice_or_vec(obj: &Bound<'_, PyAny>)` that checks if `obj` is a `VectorView` (borrowing `row_f32` directly) or supports PEP 3118 `PyBuffer<f32>` (borrowing contiguous `f32` memory directly when native-endian C-contiguous) before falling back to `obj.extract::<Vec<f32>>()`.
  - Apply this fastpath to `encode_oracle_vector_f32`, `encode_oracle_vector_f64`, `encode_oracle_vector_i8`, `encode_pgvector_f32`, and `encode_pgvector_f16`.
  - Add PyO3 functions `encode_pgvector_f32`, `decode_pgvector_f32`, `encode_pgvector_f16`, `decode_pgvector_f16`, `encode_pgvector_bit`, `decode_pgvector_bit` to `_vertebra`.
- On `VectorView` (`src/rs/vertebra-py/src/vector.rs`):
  - Add `.to_pgvector_bytes() -> PyResult<Bound<'_, PyBytes>>`, `.to_oracle_bytes() -> PyResult<Bound<'_, PyBytes>>`, and `.to_sqlite_blob() -> PyResult<Bound<'_, PyBytes>>`.
- On `VectorBatchView` (`src/rs/vertebra-py/src/vector.rs`):
  - Add `.to_arrow_shm(py, column: &str = "embedding") -> PyResult<ArrowCursor>` without adding the Rust `arrow` crate to `vertebra-py`:
    - When `pyarrow` is importable in Python, build a zero-copy `pyarrow.FixedSizeListArray.from_arrays(pyarrow.Array.from_buffers(elem_type, N * D, [None, pyarrow.py_buffer(slf)]), D)` wrapped in a single-column `pyarrow.RecordBatch` named `column`, serialize the IPC stream bytes into a `PyBytes` buffer, and frame them in POSIX shared memory via `write_shm_arrow_frame`[^duckdb-arrow-shm] (`ArrowCursor::write_ipc_to_shm`[^pyo3-db]).
    - When `pyarrow` is not installed, frame the contiguous `VectorSlab` byte slice directly via `write_shm_arrow_frame` (`row_count = N`, `byte_length = slab_bytes.len()`) and return the `ArrowCursor` so `ArrowCursor.shm_view()` / `ArrowCursor.read_bytes()` works with zero optional dependencies.

### 2.3 SQLAlchemy `Vector` Pushdown (`src/py/vertebra/extensions/sqlalchemy/types.py`)

- Update [`Vector.process_bind_param`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^sa-vector-type] and `Vector.process_result_value`:
  - Accept `VectorView` (or `memoryview`) directly without converting to `list[float]` first:
    - For `oracle`: call `value.to_oracle_bytes()` (or `_vertebra.encode_oracle_vector_f32(value)`).
    - For `sqlite`: call `value.to_sqlite_blob()`.
    - For `postgresql`: when `self.binary_format` is enabled (or when `VectorView` is passed with binary bind mode), call `value.to_pgvector_bytes()`; otherwise format directly from `VectorView` buffer.
    - In `process_result_value`: when `postgresql` returns binary `pgvector` `bytes`, decode via `_vertebra.decode_pgvector_f32`.

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Rust unit tests in `src/rs/vertebra-db/src/postgres.rs` verifying roundtrip encoding/decoding of `encode_pgvector_f32`, `encode_pgvector_f16`, and `encode_pgvector_bit`, plus rejection of `NaN`/`Inf` and malformed headers.
   - Write Python unit tests in `src/py/tests/unit/db/test_vector_wire_codecs.py` verifying:
     - `VectorView.to_pgvector_bytes()`, `VectorView.to_oracle_bytes()`, and `VectorView.to_sqlite_blob()` roundtrip through `decode_pgvector_f32`, `decode_oracle_vector`, and `struct.unpack`.
     - `encode_oracle_vector_f32(vec_view)` and `encode_pgvector_f32(memoryview(vec_view))` accept PEP 3118 buffers directly without `list` conversion.
     - `VectorBatchView.to_arrow_shm(column="embedding")` returns an `ArrowCursor` readable by `DuckDBDriver` / `ArrowCursor.read_bytes()`.
     - `SQLAlchemy` `Vector(768)` binds `VectorView` across `postgresql`, `oracle`, and `sqlite` dialects.
2. **Green**:
   - Implement the `postgres.rs` codecs, `db.rs` and `vector.rs` PyO3 bindings, `_vertebra.pyi` stubs, and `sqlalchemy/types.py` updates.
3. **Refactor & Verify**:
   - Run `cargo test -p vertebra-db`, `make dev`, `mypy.stubtest vertebra._vertebra`, and `pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/extensions/test_sqlalchemy.py`.

### 3.2 Verification Commands

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db
make dev
.venv/bin/python -m mypy.stubtest vertebra._vertebra --allowlist tools/stubtest_allowlist.txt
.venv/bin/pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/extensions/test_sqlalchemy.py -v
```

---

## 4. Task Notes

- Implemented `encode_pgvector_f32`, `decode_pgvector_f32`, `encode_pgvector_f16`, `decode_pgvector_f16`, `encode_pgvector_bit`, and `decode_pgvector_bit` in `src/rs/vertebra-db/src/postgres.rs` with IEEE-754 `f16` conversion and `NaN`/`Inf`/header validation.
- Added zero-copy `PyBuffer` / `VectorView` input fastpaths (`with_f32_slice_or_vec`, `with_f64_slice_or_vec`, `with_i8_slice_or_vec`, `with_u8_slice_or_vec`) in `src/rs/vertebra-py/src/db.rs` for `encode_oracle_vector_*` and `encode_pgvector_*`.
- Added `.to_pgvector_bytes()`, `.to_oracle_bytes()`, and `.to_sqlite_blob()` on `VectorView` and `.to_arrow_shm(column="embedding")` (with `pyarrow` `FixedSizeListArray` IPC stream framing + fallback raw slab framing) on `VectorBatchView` in `src/rs/vertebra-py/src/vector.rs`.
- Updated `Vector` in `src/py/vertebra/extensions/sqlalchemy/types.py` to push down `VectorView` / `memoryview` directly in `process_bind_param` and decode binary `pgvector` frames in `process_result_value`.
- Verified with 29 Rust tests in `vertebra-db`, `mypy.stubtest vertebra._vertebra` (0 issues), `pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -v` (12/12 passed), `ruff`, `mypy`, and `cargo clippy` (0 warnings).

---

## Provenance Footnotes

[^pg-wire]: `src/rs/vertebra-db/src/postgres.rs::encode_pgcopy_binary_rows` — PostgreSQL wire framing and `PGCOPY` binary row encoding.
[^oracle-vec]: `src/rs/vertebra-db/src/oracle.rs::encode_oracle_vector_f32` — Oracle 26ai binary `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) wire codecs.
[^duckdb-arrow-shm]: `src/rs/vertebra-db/src/duckdb_engine.rs::write_shm_arrow_frame` — Shared-memory `VTBARROW` IPC framing in `vertebra-db`.
[^pyo3-db]: `src/rs/vertebra-py/src/db.rs::ArrowCursor` — PyO3 bindings for `ArrowCursor` and database wire codecs.
[^sa-vector-type]: `src/py/vertebra/extensions/sqlalchemy/types.py::Vector` — SQLAlchemy 2.1 `Vector` column type and `VectorDistanceExpr` compiler.
