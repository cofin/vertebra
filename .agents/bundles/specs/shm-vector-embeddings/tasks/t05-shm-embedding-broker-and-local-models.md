---
type: Task
task_id: t05-shm-embedding-broker-and-local-models
flow_id: shm-vector-embeddings
title: "Single-Instance EmbeddingBroker, GemmaEmbedder, FastEmbedder & Free-Threaded 3.14t/3.15t Quarantine"
description: "Implement EmbeddingBroker over MpmcRing and VectorSlab with token-budgeted length-sorted batching, GemmaEmbedder, FastEmbedder, StaticEmbedder, free-threaded GIL quarantine, and optional [embeddings] packaging."
scope: task
status: ready
state: completed
strategy: tdd
depends_on:
  - t02-shm-vector-slab-cache-and-views
  - t04-google-grpc-and-sql-pushdown-embedders
traces_to:
  - REQ-05
  - NFR-01
  - NFR-02
tags:
  - embeddings
  - broker
  - shared-memory
  - gemma
  - fastembed
  - free-threaded
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:36:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:36:00Z
    commit: d3d5f8b
sources:
  - id: ring-channels
    resource: src/rs/vertebra-core/src/channel.rs::MpmcRing
    title: Lock-free SpscRing and MpmcRing shared-memory slot views
  - id: tiered-rings
    resource: src/rs/vertebra-core/src/tiered_ring.rs::TieredRingConfig
    title: Four-tier response ring slot capacities (4 KiB to 2 MiB)
  - id: request-batcher
    resource: src/rs/vertebra-http/src/batcher.rs::RequestBatcher
    title: Double-buffered cooperative request batching in vertebra-http
  - id: gil-audit
    resource: src/py/vertebra/server/_gil_audit.py::GilAuditReport
    title: Free-threaded Python 3.14t/3.15t GIL re-enablement auditor
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional dependency importer and MissingDependencyError guard in vertebra.utils.module_loader
---

# Task `t05`: Single-Instance `EmbeddingBroker`, `GemmaEmbedder`, `FastEmbedder` & Free-Threaded `3.14t`/`3.15t` Quarantine

<!-- truth: start -->
- **`1x` Model RAM Across Multi-Worker SHM (`--workers N`)**: `EmbeddingBroker` loads local model weights (`GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder`) in a single host instance (`1 x 300 MB` instead of `N x 300 MB`) and serves all worker processes over `MpmcRing`[^ring-channels] + `VectorSlab`.
- **Token-Budgeted & Length-Sorted Dynamic Padding**: `EmbeddingBroker` drains queued requests up to `max_batch_requests` (`64`) or `max_batch_tokens` (`16,384`), sorts inputs by token count to pad each mini-batch only to its local maximum sequence length, scatters outputs back in original caller order, and yields between bulk sub-batches so singular `embed_query` calls are never starved.
- **Gemma 4 `EmbeddingGemma 2` Precision Guard & `3.14t`/`3.15t` GIL Quarantine**: `GemmaEmbedder` enforces `bf16` / `fp32` / `int8` / `q4` precision (raising `ValueError` on `fp16` to prevent `NaN`/`Inf` activation overflow), and `EmbeddingBroker` isolates non-`Py_MOD_GIL_NOT_USED` Python C-extensions in a sidecar subprocess when running under free-threaded Python `3.14t`/`3.15t` (`GilAuditReport`[^gil-audit]).
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Implement `EmbeddingBroker`, `GemmaEmbedder`, `FastEmbedder`, and `StaticEmbedder` in `src/py/vertebra/embeddings/`, plus the optional `[project.optional-dependencies] embeddings` extra in [`pyproject.toml`](../../../../../pyproject.toml).

### 1.2 In-Scope Files

- [`pyproject.toml`](../../../../../pyproject.toml)
- `src/py/vertebra/embeddings/_broker.py`
- `src/py/vertebra/embeddings/_local.py`
- `src/py/vertebra/embeddings/_models.py`
- `src/py/vertebra/embeddings/__init__.py`
- `src/py/tests/unit/embeddings/test_embedding_broker.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 Optional Packaging in `pyproject.toml`

- Add `embeddings = ["fastembed>=0.6.0"]` under `[project.optional-dependencies]` in [`pyproject.toml`](../../../../../pyproject.toml) while keeping `[project] dependencies` unchanged (zero new base dependencies).
- All imports of `fastembed` or `onnxruntime` in `src/py/vertebra/embeddings/_local.py` MUST go through `ensure_module("fastembed", extra="embeddings")` / `ensure_module("onnxruntime", extra="embeddings")` (or `import_optional`) in [`src/py/vertebra/utils/module_loader.py`](../../../../../src/py/vertebra/utils/module_loader.py)[^module-loader] (`PY-08`, `PY-12`), which raises `MissingDependencyError(VertebraError)` instructing the user to install `vertebra-core[embeddings]` when missing.

### 2.2 Model Registry & `GemmaEmbedder`, `FastEmbedder`, `StaticEmbedder` (`_models.py`, `_local.py`)

1. **`ModelSpec` Registry (`src/py/vertebra/embeddings/_models.py`)**:
   - `class ModelSpec(msgspec.Struct, frozen=True, gc=False)`:
     - Fields: `name: str`, `provider: str`, `default_dimensions: int`, `supported_dimensions: tuple[int, ...]`, `max_tokens: int`, `quantizations: tuple[str, ...]`, `mirror_url: str | None = None`, `forbid_fp16: bool = False`, `task_prefixes: bool = False`.
   - Built-in catalog entries:
     - `gemini-embedding-001` (`provider="vertex"`, `default_dimensions=768`, `supported_dimensions=(128, 256, 512, 768, 1536, 3072)`, `max_tokens=2048`)
     - `gemini-embedding-2` (`provider="gemini"`, `default_dimensions=768`, `supported_dimensions=(128, 256, 512, 768, 1536, 3072)`, `max_tokens=8192`)
     - `text-embedding-005` (`provider="vertex"`, `default_dimensions=768`, `supported_dimensions=(128, 256, 512, 768)`, `max_tokens=2048`)
     - `embeddinggemma-2-270m` (`provider="gemma"`, `default_dimensions=768`, `supported_dimensions=(128, 256, 512, 768)`, `max_tokens=8192`, `quantizations=("int8", "q4", "bf16", "fp32")`, `forbid_fp16=True`, `task_prefixes=True`)
     - `embeddinggemma-300m` (`provider="gemma"`, `default_dimensions=768`, `supported_dimensions=(128, 256, 512, 768)`, `max_tokens=2048`, `quantizations=("int8", "q4", "fp32")`, `mirror_url="https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX/resolve/main"`, `task_prefixes=True`)
     - `BAAI/bge-small-en-v1.5` (`provider="fastembed"`, `default_dimensions=384`, `supported_dimensions=(128, 256, 384)`, `max_tokens=512`, `mirror_url="https://storage.googleapis.com/qdrant-fastembed/fast-bge-small-en-v1.5.tar.gz"`)
     - `nomic-ai/nomic-embed-text-v1.5` (`provider="fastembed"`, `default_dimensions=768`, `supported_dimensions=(64, 128, 256, 512, 768)`, `max_tokens=8192`, `mirror_url="https://storage.googleapis.com/qdrant-fastembed/fast-nomic-embed-text-v1.5.tar.gz"`)
     - `minishlab/potion-base-8M` (`provider="static"`, `default_dimensions=256`, `supported_dimensions=(64, 128, 256)`, `max_tokens=8192`, `mirror_url="https://huggingface.co/minishlab/potion-base-8M/resolve/main"`)
2. **`GemmaEmbedder` (`src/py/vertebra/embeddings/_local.py`)**:
   - Subclasses `Embedder`.
   - Validates `precision`/`quantization`: if `fp16` / `float16` is passed for `embeddinggemma-2*`, immediately raises `ValueError("EmbeddingGemma 2 does not support float16 (fp16) due to activation overflow; use 'int8', 'q4', 'bf16', or 'fp32'.")`.
   - Applies Gemma task-steered prefixes (`format_gemma_prompt` in Rust) for `embed_query` (`search_query`), `embed_document` (`search_document` with `title`), and custom `EmbeddingTask` modes.
   - Supports `source="gs://..."` or local/SHM path staged via `vtb ai models pull|stage|warm` (`local_files_only=True` when local cache or `/dev/shm/vtb-model-*` exists) and delegates multi-worker execution to `EmbeddingBroker`.
3. **`FastEmbedder` & `StaticEmbedder` (`src/py/vertebra/embeddings/_local.py`)**:
   - `FastEmbedder`: wraps `fastembed.TextEmbedding` (`specific_model_path`, `threads`, `cache_dir`) behind `EmbeddingBroker` and writes output matrices directly into `VectorSlab` with MRL truncation + SIMD L2 normalization.
   - `StaticEmbedder`: pure-Rust static token embedding lookup (`StaticEmbedderCore` from `t02`) backed by a `VectorSlab` in `ShmRegion` (`< 10 us` per query with zero Python ML dependencies).

### 2.3 `EmbeddingBroker` (`src/py/vertebra/embeddings/_broker.py`)

1. **Token-Budgeted & Length-Sorted Micro-Batcher**:
   - `class EmbeddingBrokerConfig(msgspec.Struct, frozen=True, gc=False)`:
     - `max_batch_requests: int = 64`
     - `max_batch_tokens: int = 16_384`
     - `coalesce_window_ms: float = 1.0`
     - `bulk_chunk_size: int = 128`
     - `isolate_gil_subprocess: bool = True`
   - When multiple coroutines, `3.14t`/`3.15t` threads, or multi-worker processes submit singular `embed_query` / `embed_document` requests:
     - Drains pending items until `len(batch) >= max_batch_requests` or `sum(estimated_tokens) >= max_batch_tokens` (waiting up to `coalesce_window_ms` when `in_flight > 0`).
     - Sorts batch indices by estimated token length (`sorted_order = sorted(range(n), key=lambda i: token_counts[i])`), executes inference in tight length-homogeneous sub-batches, scatters results into the output `VectorSlab` at the original indices, and resolves each caller's `VectorView`.
   - For bulk `embed_many` (`N` texts):
     - Allocates a single `(N, D)` `VectorSlab` up front.
     - Processes length-sorted chunks of size `bulk_chunk_size`, yielding to any pending singular query micro-batch between chunks (priority preemption).
2. **Free-Threaded `3.14t`/`3.15t` Subprocess Quarantine**:
   - When `is_free_threaded_build()` (`sys._is_gil_enabled() is False` from [`_gil_audit.py`](../../../../../src/py/vertebra/server/_gil_audit.py)[^gil-audit]) and `isolate_gil_subprocess=True` for a local Python C-extension backend (`FastEmbedder` / `GemmaEmbedder`), `EmbeddingBroker` spawns a single dedicated sidecar subprocess that loads the model and exchanges `VectorSlab` SHM names over IPC so `sys._is_gil_enabled()` remains `False` in the main server process.

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Python unit tests in `src/py/tests/unit/embeddings/test_embedding_broker.py` verifying:
     - Token-budgeted micro-batching coalesces `20` concurrent `embed_query` calls into `1` backend forward pass when under `max_batch_tokens`, and splits into `2` passes when token count exceeds `max_batch_tokens`.
     - Length-sorted dynamic batching groups short queries separately from a `2,000`-token outlier while preserving exact caller output order (`results[i]` matches `inputs[i]`).
     - Bulk `embed_many` yields between chunks so a concurrent `embed_query` completes before the remaining bulk chunks finish.
     - `GemmaEmbedder("embeddinggemma-2-270m", precision="fp16")` raises `ValueError` explaining `fp16` activation overflow, while `int8`, `q4`, `bf16`, and `fp32` succeed.
     - `StaticEmbedder` executes in pure Rust without `fastembed` installed and returns unit-normalized `VectorView` / `VectorBatchView`.
     - Subprocess quarantine mode exchanges `VectorSlab` in `/dev/shm` across processes and returns a zero-copy `VectorBatchView` in the parent process.
2. **Green**:
   - Update `pyproject.toml`, implement `_models.py`, `_local.py`, and `_broker.py`, and export all classes in `src/py/vertebra/embeddings/__init__.py`.
3. **Refactor & Verify**:
   - Run `pytest src/py/tests/unit/embeddings/test_embedding_broker.py` and `python -m tools.scripts.review_setup validate`.

### 3.2 Verification Commands

```bash
.venv/bin/pytest src/py/tests/unit/embeddings/test_embedding_broker.py -v
.venv/bin/python -m tools.scripts.review_setup validate
```

---

## 4. Task Notes

- Added `embeddings = ["fastembed>=0.6.0"]` under `[project.optional-dependencies]` in `pyproject.toml`.
- Implemented `ModelSpec`, `MODEL_CATALOG`, `get_model_spec`, and `list_model_specs` in `src/py/vertebra/embeddings/_models.py`.
- Implemented `EmbeddingBrokerConfig` and `EmbeddingBroker` in `src/py/vertebra/embeddings/_broker.py` with token-budgeted (`max_batch_tokens=16384`), length-sorted dynamic batching, bulk chunk preemption for interactive queries, and free-threaded `3.14t`/`3.15t` subprocess GIL quarantine over `/dev/shm` `VectorSlab` regions.
- Implemented `GemmaEmbedder` (with `fp16` activation-overflow guard for `embeddinggemma-2*` and `format_gemma_prompt` task prefixes), `FastEmbedder` (guarded via `ensure_module("fastembed", extra="embeddings")`), and `StaticEmbedder` (`StaticEmbedderCore`) in `src/py/vertebra/embeddings/_local.py`.
- Verified with `pytest src/py/tests/unit/embeddings/test_embedding_broker.py -v` (5/5 passed), `ruff`, `mypy`, and `python -m tools.scripts.review_setup validate` (0 errors).

---

## Provenance Footnotes

[^ring-channels]: `src/rs/vertebra-core/src/channel.rs::MpmcRing` — Lock-free `SpscRing` and `MpmcRing` shared-memory slot views.
[^tiered-rings]: `src/rs/vertebra-core/src/tiered_ring.rs::TieredRingConfig` — Four-tier response ring slot capacities (4 KiB to 2 MiB).
[^request-batcher]: `src/rs/vertebra-http/src/batcher.rs::RequestBatcher` — Double-buffered cooperative request batching in `vertebra-http`.
[^gil-audit]: `src/py/vertebra/server/_gil_audit.py::GilAuditReport` — Free-threaded Python `3.14t`/`3.15t` GIL re-enablement auditor.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional dependency importer and `MissingDependencyError` guard in `vertebra.utils.module_loader`.
