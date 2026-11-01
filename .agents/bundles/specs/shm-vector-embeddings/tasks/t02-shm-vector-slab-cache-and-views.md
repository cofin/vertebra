---
type: Task
task_id: t02-shm-vector-slab-cache-and-views
flow_id: shm-vector-embeddings
title: "Shared-Memory VectorSlab, VectorCache, SIMD Kernels, StaticEmbedder & PEP 3118 Views"
description: "Implement 64-byte-aligned VectorSlab, seqlock VectorCache with SIMD top-k search, Matryoshka L2 normalization, scalar/binary quantization, Gemma task-prefix splicer, StaticEmbedder in vertebra-core, and PEP 3118 VectorView/VectorBatchView in vertebra-py."
scope: task
status: draft
state: pending
strategy: tdd
depends_on: []
traces_to:
  - REQ-02
  - NFR-01
  - NFR-02
tags:
  - vector
  - shared-memory
  - simd
  - pyo3
  - pep3118
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:33:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:33:00Z
    commit: d3d5f8b
sources:
  - id: shm-region
    resource: src/rs/vertebra-core/src/shm.rs::ShmRegion
    title: Cross-platform shared memory region allocation and overflow fallback
  - id: shm-broker
    resource: src/rs/vertebra-core/src/broker.rs::ShmLeaseTable
    title: Cross-process SHM connection lease broker and seqlock statement cache
  - id: pyo3-request
    resource: src/rs/vertebra-py/src/request.rs::Request
    title: Anchored PEP 3118 Py_buffer zero-copy memoryview implementation
---

# Task `t02`: Shared-Memory `VectorSlab`, `VectorCache`, SIMD Kernels, `StaticEmbedder` & PEP 3118 Views

<!-- truth: start -->
- **64-Byte Cache-Line Aligned `VectorSlab`**: `VectorSlab` stores `(count, dimensions, dtype)` matrices in a `ShmRegion`[^shm-region] (or in-process 64-byte aligned buffer for small standalone views) with 64-byte aligned row strides for `Float32`, `Float64`, `Float16`, `Int8`, and packed 1-bit `Binary`.
- **Zero-Copy PEP 3118 `VectorView` & `VectorBatchView`**: Declared in `vertebra-py` as `#[pyclass(frozen, immutable_type)]` following `Request.__getbuffer__`[^pyo3-request], `VectorView` (1D `shape=[D]`) and `VectorBatchView` (2D `shape=[N, D]`) expose read-only (`readonly = 1`) C-buffers consumable by `memoryview(v)` and `numpy.asarray(v)` in `< 1 us` with `0` byte copies across both GIL and free-threaded `3.14t`/`3.15t` builds.
- **Pure-Rust SIMD Kernel & `VectorCache`**: `vertebra-core::vector` provides MRL prefix truncation (`768d -> 512d/256d/128d`) + SIMD L2 renormalization, `int8` and `1-bit` binary quantization, Gemma task-prefix token splicer, `StaticEmbedder` static token lookup, and seqlock-guarded `VectorCache`[^shm-broker] with off-GIL SIMD `cosine_top_k`, `dot_top_k`, and `POPCNT` `hamming_top_k`.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Implement the pure-Rust shared-memory vector foundation in `vertebra-core` (`src/rs/vertebra-core/src/vector.rs`) and its zero-copy PEP 3118 Python bindings in `vertebra-py` (`src/rs/vertebra-py/src/vector.rs`) with full type stubs in [`src/py/vertebra/_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi).

### 1.2 In-Scope Files

- `src/rs/vertebra-core/src/vector.rs` (registered in [`src/rs/vertebra-core/src/lib.rs`](../../../../../src/rs/vertebra-core/src/lib.rs))
- `src/rs/vertebra-py/src/vector.rs` (registered in [`src/rs/vertebra-py/src/lib.rs`](../../../../../src/rs/vertebra-py/src/lib.rs))
- [`src/py/vertebra/_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)
- `src/py/tests/unit/embeddings/test_vector_views.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 Rust `vertebra-core::vector` (`src/rs/vertebra-core/src/vector.rs`)

1. **`VectorDtype` & `VectorSlabHeader` (`64 bytes`, `#[repr(C, align(64))]`)**:
   - `VectorDtype`: `Float32 = 1`, `Float64 = 2`, `Float16 = 3`, `Int8 = 4`, `Binary = 5`.
   - `VectorSlabHeader`:
     - `magic: [u8; 8]` (`b"VTBVSLAB"`)
     - `version: u32` (`1`)
     - `dtype: u8`
     - `flags: u8` (`bit 0 = l2_normalized`)
     - `reserved: u16`
     - `count: u32` (`N`)
     - `dimensions: u32` (`D`)
     - `row_stride_bytes: u32` (rounded up to multiple of `64` bytes when `padded = true`, or packed `D * elem_bytes` / `D.div_ceil(8)` for contiguous 2D C-buffer views)
     - `scales_offset: u64` (optional per-row `f32` quantization scales for `Int8`)
   - `VectorSlab`:
     - Backed by either `ShmRegion`[^shm-region] (`create(name, count, dimensions, dtype)` / `open(name)`) or an `Arc<AlignedBuffer>` for single-process `Topology::Embedded`.
     - Provides safe `row_bytes(idx) -> &[u8]`, `row_f32(idx) -> &[f32]`, `row_f32_mut(idx) -> &mut [f32]`, `as_bytes() -> &[u8]`, `shm_name() -> Option<&str>`.
2. **SIMD Post-Processing & Quantization Kernels**:
   - `l2_normalize_in_place(slice: &mut [f32]) -> f32`: 8-lane unrolled sum-of-squares accumulator ($\|v\|_2 = \sqrt{\sum v_i^2}$); if norm $> 1\text{e-}12$, scales `slice` by $1.0 / \|v\|_2$ and returns the original norm.
   - `mrl_truncate_and_normalize(src: &[f32], target_dim: usize, dst: &mut [f32])`: copies `src[..target_dim]` into `dst` and runs `l2_normalize_in_place(dst)`.
   - `quantize_f32_to_i8(src: &[f32], dst: &mut [i8]) -> f32`: computes `max_abs = src.iter().fold(0.0f32, |m, &x| m.max(x.abs()))`, `scale = if max_abs > 0.0 { max_abs / 127.0 } else { 1.0 }`, and maps each element to `(x / scale).round().clamp(-127.0, 127.0) as i8`.
   - `quantize_f32_to_binary(src: &[f32], dst: &mut [u8])`: packs `src.len()` floats into `src.len().div_ceil(8)` bytes (`bit (7 - (i & 7)) = 1` if `src[i] > 0.0`).
   - `cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32`, `dot_product_f32(a: &[f32], b: &[f32]) -> f32`, `dot_product_i8(a: &[i8], b: &[i8]) -> i32`, `hamming_distance_binary(a: &[u8], b: &[u8]) -> u32` (using `u64::from_ne_bytes` + `.count_ones()`).
   - `top_k_search(query: &[u8], slab: &VectorSlab, metric: SimilarityMetric, k: usize) -> Vec<SimilarityMatch>`: maintains a bounded binary min-heap of size `k` over rows `0..slab.count()`.
3. **Gemma Task-Prefix Splicer (`EmbeddingTask`)**:
   - Enum `EmbeddingTask`: `SearchQuery`, `SearchDocument`, `QuestionAnswering`, `FactVerification`, `Classification`, `Clustering`, `SemanticSimilarity`, `CodeRetrieval`.
   - `format_gemma_prompt(task: EmbeddingTask, text: &str, title: Option<&str>) -> String` and `splice_prefix_tokens(prefix_ids: &[u32], doc_ids: &[u32], eos_id: Option<u32>, max_seq_len: usize, out: &mut Vec<u32>)`: zero-Python-string-allocation token ID splicer.
4. **Seqlock `VectorCache` & Pure-Rust `StaticEmbedder`**:
   - `VectorCache`: set-associative seqlock table in `ShmRegion`[^shm-broker] keyed by `fnv1a_64(&(model_id, task_tag, dims, dtype, text_bytes))`. Lookups spin-check even sequence numbers (`seq1 == seq2 && (seq1 & 1) == 0`) and return a `VectorSlab` row slice in `< 200 ns`; also supports `similarity_search(query_vec, top_k, metric)` over cached entries.
   - `StaticEmbedderCore`: loads a static token embedding matrix `(vocab_size, dims)` into `VectorSlab`, splits or tokenizes input text off-GIL, averages token rows with SIMD accumulation, truncates to `target_dim`, and L2-normalizes in `< 10 us`.

### 2.2 PyO3 `VectorView` & `VectorBatchView` (`src/rs/vertebra-py/src/vector.rs`)

- **`#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)] pub struct VectorView`**:
  - Holds `slab: Arc<VectorSlab>`, `row_index: usize`, `shape_storage: [isize; 1]`, `stride_storage: [isize; 1]`.
  - For `VectorDtype::Binary` (`1-bit` packed), `shape_storage[0] = D.div_ceil(8) as isize` (`packed_bytes_per_row`) and `stride_storage[0] = 1` so `Py_buffer.len` equals the exact packed byte length (`D.div_ceil(8)` bytes, preventing any out-of-bounds buffer overread), while `dimensions` (`#[getter]`) returns the logical bit dimension `D`.
  - Implements `unsafe fn __getbuffer__(slf: Bound<'_, Self>, view: *mut ffi::Py_buffer, flags: c_int) -> PyResult<()>` and `unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer)` following [`Request::__getbuffer__`](../../../../../src/rs/vertebra-py/src/request.rs)[^pyo3-request]:
    - Obtains `this: &Self = slf.get()` so `this.shape_storage` and `this.stride_storage` reside inside the heap `PyObject` payload rather than on a temporary stack wrapper.
    - Calls `ffi::PyBuffer_FillInfo(view, slf.as_ptr(), ptr, byte_len, 1, flags)` (which increments `(*view).obj` **once**; never call an extra manual `ffi::Py_INCREF` alongside `PyBuffer_FillInfo`), then sets `(*view).itemsize = elem_size`, `(*view).format` (`c"f"` for `Float32`, `c"d"` for `Float64`, `c"e"` for `Float16`, `c"b"` for `Int8`, `c"B"` for `Binary`), `(*view).ndim = 1`, `(*view).shape = (&raw const this.shape_storage[0]).cast_mut()`, and `(*view).strides = (&raw const this.stride_storage[0]).cast_mut()`.
  - Methods: `dimensions: usize` (`#[getter]`), `dtype: &str` (`#[getter]`), `nbytes: usize` (`#[getter]`), `__len__() -> usize`, `norm() -> f32`, `truncate(dimensions: usize) -> PyResult<VectorView>`, `quantize_int8() -> PyResult<VectorView>`, `quantize_binary() -> PyResult<VectorView>`, `cosine_similarity(other: &VectorView) -> PyResult<f32>`, `dot(other: &VectorView) -> PyResult<f32>`, `hamming_distance(other: &VectorView) -> PyResult<u32>`, `to_list() -> PyResult<Vec<f32>>`, `to_bytes() -> &[u8]`.
- **`#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)] pub struct VectorBatchView`**:
  - Holds `slab: Arc<VectorSlab>`, `shape_storage: [isize; 2]`, `stride_storage: [isize; 2]`.
  - For `Float32`/`Float64`/`Float16`/`Int8`, `shape_storage = [N as isize, D as isize]` and `stride_storage = [row_stride_bytes as isize, elem_size as isize]`; for `VectorDtype::Binary`, `shape_storage = [N as isize, D.div_ceil(8) as isize]` and `stride_storage = [row_stride_bytes as isize, 1]`, while `dimensions` (`#[getter]`) returns logical bit dimension `D`.
  - Implements PEP 3118 2D `__getbuffer__(slf: Bound<'_, Self>, view: *mut ffi::Py_buffer, flags: c_int)` using `slf.get()`, `ffi::PyBuffer_FillInfo` (single `(*view).obj` refcount increment), `ndim = 2`, `shape = (&raw const this.shape_storage[0]).cast_mut()`, `strides = (&raw const this.stride_storage[0]).cast_mut()`, and `readonly = 1`.
  - Methods: `count: usize` (`#[getter]`), `dimensions: usize` (`#[getter]`), `dtype: &str` (`#[getter]`), `nbytes: usize` (`#[getter]`), `shm_name: Option<String>` (`#[getter]`), `__len__() -> usize`, `__getitem__(index: isize) -> PyResult<VectorView>`, `truncate(dimensions: usize) -> PyResult<VectorBatchView>`, `quantize_int8() -> PyResult<VectorBatchView>`, `quantize_binary() -> PyResult<VectorBatchView>`, `top_k(query: &VectorView, k: usize, metric: &str) -> PyResult<Vec<SimilarityMatch>>`, `to_list() -> PyResult<Vec<Vec<f32>>>`.

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Rust unit tests in `src/rs/vertebra-core/src/vector.rs` verifying:
     - `VectorSlab` creation in `ShmRegion`, cross-handle reopen via `VectorSlab::open`, 64-byte row alignment, and bounds checks.
     - `mrl_truncate_and_normalize` produces unit L2 norm ($\|\hat{v}\|_2 = 1.0 \pm 1\text{e-}6$) at `512d`, `256d`, and `128d`.
     - `quantize_f32_to_i8` and `quantize_f32_to_binary` accuracy and `hamming_distance_binary` `POPCNT` exactness.
     - `VectorCache` seqlock concurrent read/write consistency and `top_k_search` exact ordering.
   - Write Python unit tests in `src/py/tests/unit/embeddings/test_vector_views.py` verifying:
     - `memoryview(vec)` and `memoryview(batch)` expose 1D (`shape == (768,)`, `format == "f"`, `readonly is True`) and 2D (`shape == (256, 768)`, `format == "f"`, `readonly is True`) buffers with zero byte copies.
     - `batch[i]` returns a `VectorView` sharing the underlying `VectorSlab`.
     - MRL `.truncate(256)`, `.quantize_int8()`, `.quantize_binary()`, `.cosine_similarity()`, and `batch.top_k(query, k=5)` match reference calculations.
2. **Green**:
   - Implement `src/rs/vertebra-core/src/vector.rs`, `src/rs/vertebra-py/src/vector.rs`, register classes/functions in `src/rs/vertebra-py/src/lib.rs`, and update `src/py/vertebra/_vertebra.pyi`.
3. **Refactor & Verify**:
   - Run `cargo test -p vertebra-core`, `make dev`, `mypy.stubtest vertebra._vertebra`, and `pytest src/py/tests/unit/embeddings/test_vector_views.py`.

### 3.2 Verification Commands

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core
cargo check --manifest-path src/rs/Cargo.toml --target x86_64-pc-windows-msvc --no-default-features
make dev
.venv/bin/python -m mypy.stubtest vertebra._vertebra --allowlist tools/stubtest_allowlist.txt
.venv/bin/pytest src/py/tests/unit/embeddings/test_vector_views.py -v
```

---

## 4. Task Notes

*(Populated during `/flow:implement` execution.)*

---

## Provenance Footnotes

[^shm-region]: `src/rs/vertebra-core/src/shm.rs::ShmRegion` — Cross-platform shared memory region allocation and overflow fallback.
[^shm-broker]: `src/rs/vertebra-core/src/broker.rs::ShmLeaseTable` — Cross-process SHM connection lease broker and seqlock statement cache.
[^pyo3-request]: `src/rs/vertebra-py/src/request.rs::Request` — Anchored PEP 3118 `Py_buffer` zero-copy `memoryview` implementation.
