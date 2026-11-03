---
type: Concept
title: "Shared-Memory Vector Slabs, Zero-Weight Cloud & SQL Embeddings, and Local Broker"
description: "The `vertebra-core::vector`, `vertebra-py::vector`, and `vertebra.embeddings` stack: POSIX shared-memory `VectorSlab` and `VectorCache`, PEP 3118 `VectorView` and `VectorBatchView`, SIMD MRL truncation and quantization, native HTTP/2 gRPC Vertex AI and Gemini embedders, AlloyDB and Oracle 26ai in-database SQL pushdown, and `1x` RAM `EmbeddingBroker` with free-threaded 3.14t/3.15t GIL quarantine."
status: stable
tags: [domains, agent, embeddings, vector, shm, grpc, simd]
sources:
  - resource: src/rs/vertebra-core/src/vector.rs
  - resource: src/rs/vertebra-py/src/vector.rs
  - resource: src/rs/vertebra-http/src/grpc.rs
  - resource: src/py/vertebra/embeddings/__init__.py
  - resource: src/py/vertebra/embeddings/_base.py
  - resource: src/py/vertebra/embeddings/_google.py
  - resource: src/py/vertebra/embeddings/_remote.py
  - resource: src/py/vertebra/embeddings/_models.py
  - resource: src/py/vertebra/embeddings/_broker.py
  - resource: src/py/vertebra/embeddings/_local.py
  - resource: src/py/vertebra/cli/ai.py
  - resource: src/py/tests/unit/embeddings/test_vector_views.py
  - resource: src/py/tests/unit/embeddings/test_google_embedders.py
  - resource: src/py/tests/unit/embeddings/test_embedding_broker.py
---

# Shared-Memory Vector Slabs, Zero-Weight Cloud & SQL Embeddings, and Local Broker

<!-- truth: start -->
- **Shared-Memory Vector Arena (`vertebra-core::vector`, `vertebra-py::vector`)**: [`VectorSlab`](../../../../../src/rs/vertebra-core/src/vector.rs) (`VECTOR_SLAB_MAGIC = *b"VTBVSLAB"`) stores 64-byte-aligned dense vectors (`Float32`, `Float64`, `Float16`, `Int8`, `Binary`) in a POSIX shared-memory `ShmRegion` with a 64-byte [`VectorSlabHeader`](../../../../../src/rs/vertebra-core/src/vector.rs). [`VectorView`](../../../../../src/rs/vertebra-py/src/vector.rs) (1D) and [`VectorBatchView`](../../../../../src/rs/vertebra-py/src/vector.rs) (2D) expose zero-copy PEP 3118 buffers (`memoryview`, `numpy.asarray`), Apache Arrow `FixedSizeListArray` streams (`to_arrow_shm`), and binary database wire frames (`to_pgvector_bytes`, `to_oracle_bytes`, `to_sqlite_blob`).
- **Google-First Zero-Weight Cloud & SQL Pushdown (`_google.py`, `_remote.py`)**: [`Embedder`](../../../../../src/py/vertebra/embeddings/_base.py) defaults to [`VertexEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) (`gemini-embedding-001`, 768 dims) over native Rust HTTP/2 gRPC (`encode_batch_embed_grpc_request`, `decode_batch_embed_grpc_response_into_slab` in [`vertebra-core/src/vector.rs`](../../../../../src/rs/vertebra-core/src/vector.rs) and [`vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs), exposed to Python via `encode_gemini_embed_grpc_frame`, `encode_gemini_embed_grpc_response_frame`, and `decode_gemini_embed_grpc_frame_to_batch` in [`vertebra-py/src/vector.rs`](../../../../../src/rs/vertebra-py/src/vector.rs)) with [`GcpTokenCache`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs) auth, decoding Protobuf v3 packed-`f32` (`wire_type = 2`) fields directly into `VectorSlab` off-GIL. [`AlloyDbEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) and [`OracleEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) push embedding generation into database SQL (`embedding()`, `DBMS_VECTOR.UTL_TO_EMBEDDING()`) and decode binary `pgvector` / Oracle `0xDB` wire payloads into `VectorBatchView`. SQL pushdown embedders (`SpannerEmbedder`, `OracleEmbedder`, `BigQueryEmbedder`) raise `EmbeddingError(VertebraError, RuntimeError)` for missing vectors or row-count mismatches, propagate driver errors, order generated embeddings by input offset (`ORDER BY idx`), and validate interpolated identifiers with `validate_identifier`.
- **Single-Copy `1x` RAM Local Model Broker (`_broker.py`, `_local.py`)**: [`EmbeddingBroker`](../../../../../src/py/vertebra/embeddings/_broker.py) loads local weights (`GemmaEmbedder`, `FastEmbedder`) once for an entire multi-worker deployment, dynamic-batches concurrent worker requests by token budget (`max_batch_tokens = 8192`) with length-sorted grouping, and isolates `onnxruntime` / `ctranslate2` inside a dedicated child process (`submit_batch_in_subprocess`) when running on free-threaded Python `3.14t`/`3.15t` (`sys._is_gil_enabled() is False`) so C-extension loads never re-enable the GIL in HTTP workers.
<!-- truth: end -->

---

## 1. Architecture Overview

```mermaid
flowchart LR
    subgraph Workers["Python Workers (3.12-3.15 / 3.14t-3.15t)"]
        W1["Route / Worker Task"] --> Emb["Embedder Facade"]
        Emb --> VView["PEP 3118 VectorView / VectorBatchView"]
    end

    subgraph Backends["Embedding Execution Planes"]
        GGrpc["Rust HTTP/2 gRPC (VertexEmbedder / GeminiEmbedder / RemoteEmbedder)"]
        SqlPush["In-DB SQL Pushdown (AlloyDbEmbedder / OracleEmbedder)"]
        Broker["EmbeddingBroker (1x RAM GemmaEmbedder / FastEmbedder)"]
        Static["Rust Mmap StaticEmbedderCore (StaticEmbedder)"]
    end

    subgraph ShmPlane["POSIX Shared Memory (/dev/shm)"]
        Slab["VectorSlab (64B Header + 64B-Aligned Rows)"]
        Cache["VectorCache (Set-Associative Seqlock + FNV-1a Key)"]
    end

    Emb --> Cache
    Emb --> GGrpc
    Emb --> SqlPush
    Emb --> Broker
    Emb --> Static
    GGrpc --> Slab
    SqlPush --> Slab
    Broker --> Slab
    Static --> Slab
    Slab --> VView
```

---

## 2. Shared-Memory `VectorSlab`, `VectorCache` & SIMD Kernels (`vertebra-core::vector`)

| Component | Source | Responsibility |
| :--- | :--- | :--- |
| [`VectorSlabHeader`](../../../../../src/rs/vertebra-core/src/vector.rs) | `vertebra-core/src/vector.rs` | `#[repr(C, align(64))]` 64-byte header (`magic = *b"VTBVSLAB"`, `version = 1`, `dtype`, `flags`, `count`, `dimensions`, `row_stride_bytes`, `scales_offset`, `data_offset`, `total_bytes`). |
| [`VectorDtype`](../../../../../src/rs/vertebra-core/src/vector.rs) | `vertebra-core/src/vector.rs` | `Float32 = 1` (`"f"`), `Float64 = 2` (`"d"`), `Float16 = 3` (`"e"`), `Int8 = 4` (`"b"`), `Binary = 5` (`"B"`). |
| [`VectorSlab`](../../../../../src/rs/vertebra-core/src/vector.rs) | `vertebra-core/src/vector.rs` | Allocates 64-byte-aligned rows (`from_f32_batch`, `from_i8_batch`, `from_binary_batch`) and executes Matryoshka truncation (`mrl_truncate_and_normalize`), scalar quantization (`quantize_f32_to_i8`), and 1-bit packing (`quantize_f32_to_binary`). |
| [`VectorCache`](../../../../../src/rs/vertebra-core/src/vector.rs) | `vertebra-core/src/vector.rs` | Set-associative seqlock cache keyed by 64-bit FNV-1a (`compute_vector_cache_key(model_id, task, dimensions, text)`), with off-GIL SIMD `cosine_top_k`, `dot_top_k`, and `hamming_top_k`. |
| [`StaticEmbedderCore`](../../../../../src/rs/vertebra-core/src/vector.rs) | `vertebra-core/src/vector.rs` | Pure-Rust `mmap` wordpiece/unigram token table (`model2vec` / `potion-base-8M`) pooling and L2-normalizing token rows directly into `VectorSlab` in `<50 µs` with zero Python ML dependencies. |
| `format_gemma_prompt` | `vertebra-core/src/vector.rs` | Formats asymmetrical `EmbeddingGemma` prompts (`"task: search result \| query: {text}"` vs `"title: {title \| none} \| text: {text}"`). |

---

## 3. PEP 3118 `VectorView` & `VectorBatchView` (`vertebra-py::vector`)

[`VectorView`](../../../../../src/rs/vertebra-py/src/vector.rs) and [`VectorBatchView`](../../../../../src/rs/vertebra-py/src/vector.rs) pin their underlying `Arc<VectorSlab>` and implement `tp_as_buffer` (`__getbuffer__` / `__releasebuffer__`):

- **Buffer Protocol**: `memoryview(view)` and `np.asarray(view)` expose read-only 1D `(dims,)` or 2D `(rows, dims)` views with format `"f"` (`f32`), `"d"` (`f64`), `"e"` (`f16`), `"b"` (`i8`), or `"B"` (`u8` bit-packed) and zero heap copies.
- **Matryoshka & Quantization**: `.truncate(dims)` slices `[:dims]` and L2-renormalizes into a new slab allocation; `.quantize_int8()` and `.quantize_binary()` produce `Int8` and packed `Binary` views.
- **Database Wire Codecs**:
  - `.to_pgvector_bytes()`: PostgreSQL `pgvector` binary wire format (`uint16 dims`, `uint16 unused = 0`, big-endian `f32[dims]`).
  - `.to_oracle_bytes()`: Oracle 26ai `VECTOR` binary `0xDB` frame (`encode_vector_binary`).
  - `.to_sqlite_blob()`: Little-endian packed payload for `sqlite-vec`.
  - `.to_arrow_shm(column_name="embedding", *, shm_name=None)`: Apache Arrow IPC `RecordBatch` stream with a `FixedSizeListArray(Float32, dims)` column inside a `ShmRegion`.

---

## 4. Embedder Hierarchy (`vertebra.embeddings`)

| Class | Default Model | Execution Path |
| :--- | :--- | :--- |
| [`Embedder`](../../../../../src/py/vertebra/embeddings/_base.py) | Delegates to `VertexEmbedder` | Unified facade with `embed_query()`, `embed_document()`, `embed_many()`, `embed_arrow()`, and `similarity_search()`. |
| [`VertexEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) | `gemini-embedding-001` (`768` dims) | Rust HTTP/2 gRPC `PredictionService/EmbedContent` with `GcpTokenCache` Bearer token injection and packed-`f32` decoding into `VectorSlab`. |
| [`GeminiEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) | `gemini-embedding-001` (`768` dims) | Rust HTTP/2 gRPC `GenerativeService/BatchEmbedContents` with `x-goog-api-key` or ADC Bearer auth. |
| [`AlloyDbEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) | `gemini-embedding-001` (`768` dims) | Executes `SELECT embedding($1, t.txt)::vector` in AlloyDB `ai` / `google_ml_integration` and decodes binary `pgvector` into `VectorBatchView`. |
| [`OracleEmbedder`](../../../../../src/py/vertebra/embeddings/_google.py) | `gemini-embedding-001` (`768` dims) | Executes `SELECT DBMS_VECTOR.UTL_TO_EMBEDDING(COLUMN_VALUE, JSON(:params))` in Oracle 26ai and decodes binary `0xDB` `VECTOR` frames into `VectorBatchView`. |
| [`GemmaEmbedder`](../../../../../src/py/vertebra/embeddings/_local.py) | `embeddinggemma-2-270m` (`768` dims) | `EmbeddingBroker`-hosted `EmbeddingGemma` (`270M` / `300M`, `100+` languages, MRL `768`/`512`/`256`/`128`). Rejects `precision="fp16"` (`ValueError`) to prevent activation overflow; supports `int8` (default), `q4`, `bf16`, and `fp32`. |
| [`FastEmbedder`](../../../../../src/py/vertebra/embeddings/_local.py) | `BAAI/bge-small-en-v1.5` (`384` dims) | `EmbeddingBroker`-hosted ONNX Runtime execution for BGE, Nomic, Arctic, Jina, and MiniLM models. |
| [`StaticEmbedder`](../../../../../src/py/vertebra/embeddings/_local.py) | `potion-base-8M` (`512` dims) | Pure-Rust `StaticEmbedderCore` mmap lookup table with zero ML dependencies. |
| [`RemoteEmbedder`](../../../../../src/py/vertebra/embeddings/_remote.py) | `embeddinggemma-2-270m` (`768` dims) | Rust HTTP/2 gRPC `/vertebra.EmbeddingService/BatchEmbed` client and server (`create_embedding_grpc_service`). |

---

## 5. `EmbeddingBroker` & Free-Threaded `3.14t`/`3.15t` Quarantine (`_broker.py`)

- **`1x` Model RAM Across Workers**: [`EmbeddingBroker`](../../../../../src/py/vertebra/embeddings/_broker.py) loads model weights once in the supervisor/broker context and exposes `submit_single()`, `submit_batch()`, and `submit_batch_in_subprocess()`.
- **Token-Budgeted Length-Sorted Dynamic Batching (`_flush_pending_batch`)**: Collects pending items across a `batch_window_ms = 2.0` window, estimates token length (`estimate_gemma_token_count`), sorts items by ascending token length to minimize right-padding waste across attention layers, splits batches at `max_batch_tokens = 8192` or `max_batch_items = 64`, and restores caller ordering in the output `VectorBatchView`.
- **Free-Threaded `3.14t`/`3.15t` Subprocess Quarantine (`should_quarantine_subprocess`)**: When `is_free_threaded()` (`sys._is_gil_enabled() is False`) is `True`, `isolate_gil_subprocess` is enabled, and no custom in-process `backend_fn` callable was passed (`not self._has_custom_backend`), `EmbeddingBroker` runs inference in a dedicated child process (`submit_batch_in_subprocess`) exchanging results over a `/dev/shm` `VectorSlab` so HTTP worker processes remain 100% GIL-disabled.
