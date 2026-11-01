---
type: Task
task_id: t04-google-grpc-and-sql-pushdown-embedders
flow_id: shm-vector-embeddings
title: "Native HTTP/2 gRPC Packed-Float Codec & Google-First Zero-Weight Embedders"
description: "Implement compact Protobuf v3 packed-f32 gRPC wire codecs in vertebra-http and the clean-named Google-first embedder hierarchy (Embedder, VertexEmbedder, GeminiEmbedder, AlloyDbEmbedder, OracleEmbedder, RemoteEmbedder) in vertebra.embeddings."
scope: task
status: ready
state: completed
strategy: tdd
depends_on:
  - t01-gcp-adc-token-cache-and-refresh
  - t02-shm-vector-slab-cache-and-views
  - t03-binary-vector-db-wire-codecs
traces_to:
  - REQ-04
  - NFR-01
  - NFR-02
tags:
  - embeddings
  - grpc
  - vertex-ai
  - gemini
  - alloydb
  - oracle
generated:
  by: flow/0.26.0
  at: 2026-11-01T18:35:00Z
verified:
  - by: agent:plan-generator
    at: 2026-11-01T18:35:00Z
    commit: d3d5f8b
sources:
  - id: grpc-framing
    resource: src/rs/vertebra-http/src/grpc.rs::encode_grpc_frame
    title: Native 5-byte Length-Prefixed Message (LPM) HTTP/2 gRPC framing in vertebra-http
  - id: grpc-registry
    resource: src/py/vertebra/extensions/grpc/__init__.py::GrpcServiceRegistry
    title: Native gRPC service registry and bidirectional streaming in vertebra.extensions.grpc
  - id: alloydb-dialer
    resource: src/rs/vertebra-db/src/dialer.rs::AlloyDbDialer
    title: Native AlloyDB (:5433 mTLS) and Cloud SQL (:3307) database dialers
  - id: oracle-vec
    resource: src/rs/vertebra-db/src/oracle.rs::encode_oracle_vector_f32
    title: Oracle 26ai binary VECTOR (FLOAT32, FLOAT64, INT8, BINARY) wire codecs
  - id: genai-telemetry
    resource: src/py/vertebra/telemetry/genai.py::GenAiUsage
    title: OpenTelemetry GenAI semantic conventions and metrics
---

# Task `t04`: Native HTTP/2 gRPC Packed-Float Codec & Google-First Zero-Weight Embedders

<!-- truth: start -->
- **Clean Domain Naming (`PY-07`)**: `vertebra.embeddings` exposes `Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`, `StaticEmbedder`, `GemmaEmbedder`, and `FastEmbedder` with zero `*Bridge` or `*Grpc*` implementation suffixes, and calling `Embedder()` defaults to `VertexEmbedder("gemini-embedding-001", dimensions=768)`.
- **Zero-Parse Packed `f32` Protobuf v3 gRPC Wire Codec**: `vertebra-http::grpc`[^grpc-framing] encodes `BatchEmbedContentsRequest` / `EmbedContentRequest` and extracts packed `repeated float values = 1 [packed = true]` (`tag 0x0A` + `varint byte_len` + contiguous little-endian IEEE-754 `f32` bytes) directly into a 64-byte-aligned `VectorSlab` with SIMD L2 renormalization (fixing unnormalized Matryoshka slices from `gemini-embedding-001`).
- **In-Database SQL Pushdown (`AlloyDbEmbedder` & `OracleEmbedder`)**: `AlloyDbEmbedder` pushes `embedding(:model, :text)::vector` into AlloyDB AI (`google_ml_integration`[^alloydb-dialer]) and `OracleEmbedder` pushes `DBMS_VECTOR.UTL_TO_EMBEDDING` into Oracle 26ai[^oracle-vec], returning `VectorView` / `VectorBatchView` directly from binary database wire frames.
<!-- truth: end -->

---

## 1. Objective & Scope

### 1.1 Goal

Implement the zero-weight Google-first embedder suite (`VertexEmbedder`, `GeminiEmbedder`, `AlloyDbEmbedder`, `OracleEmbedder`, `RemoteEmbedder`) and the clean `vertebra.embeddings` domain module on top of native HTTP/2 5-byte LPM gRPC framing (`vertebra-http/src/grpc.rs`) and `GcpTokenCache` (`t01`).

### 1.2 In-Scope Files

- `src/rs/vertebra-core/src/vector.rs`
- [`src/rs/vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs)
- `src/rs/vertebra-py/src/vector.rs`
- [`src/py/vertebra/_vertebra.pyi`](../../../../../src/py/vertebra/_vertebra.pyi)
- `src/py/vertebra/embeddings/__init__.py`
- `src/py/vertebra/embeddings/_base.py`
- `src/py/vertebra/embeddings/_google.py`
- `src/py/vertebra/embeddings/_remote.py`
- [`src/py/vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py)
- `src/py/tests/unit/embeddings/test_google_embedders.py`

---

## 2. Public Seam & Implementation Specification

### 2.1 Compact Protobuf v3 Packed-Float Wire Codec (`src/rs/vertebra-core/src/vector.rs` & `src/rs/vertebra-http/src/grpc.rs`)

> [!IMPORTANT]
> Because `vertebra-http` already depends on `vertebra-py` (`_vertebra`), `vertebra-py` cannot depend on `vertebra-http`. The pure-Rust Protobuf v3 + 5-byte LPM wire helpers (`encode_batch_embed_grpc_request` and `decode_batch_embed_grpc_response_into_slab`) live in `src/rs/vertebra-core/src/vector.rs` (which is already a dependency of both `vertebra-http` and `vertebra-py`), and are re-exported in [`src/rs/vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs)[^grpc-framing] and exposed to Python in `src/rs/vertebra-py/src/vector.rs`.

1. **`encode_batch_embed_grpc_request` (`vertebra-core::vector`)**:
   - Encodes `BatchEmbedContentsRequest` (`model`, per-item `content.parts.text`, `task_type` enum, optional `title`, and optional `output_dimensionality`) wrapped in a 5-byte LPM gRPC frame (`[0u8][u32 len BE][protobuf_bytes]`) without requiring `protoc` or `prost-build`.
2. **`decode_batch_embed_grpc_response_into_slab` (`vertebra-core::vector`)**:
   - Parses the 5-byte LPM gRPC frame, iterates `repeated ContentEmbedding embeddings = 1` (`0x0A`), reads `repeated float values = 1 [packed = true]` (`0x0A` + `varint byte_len`), copies the little-endian `f32` bytes directly into row `i` of a `VectorSlab` (slicing to `target_dim`), and executes `l2_normalize_in_place` on each row so truncated Matryoshka slices (`768d`, `512d`, `256d`, `128d` from `gemini-embedding-001`) are guaranteed unit-norm.
   - Re-export in [`src/rs/vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs)[^grpc-framing] and expose `encode_gemini_embed_grpc_frame` and `decode_gemini_embed_grpc_frame_to_batch` in `vertebra-py` (`src/rs/vertebra-py/src/vector.rs`).

### 2.2 Clean `vertebra.embeddings` Domain Hierarchy (`src/py/vertebra/embeddings/`)

1. **`Embedder` Base Class & Default Factory (`src/py/vertebra/embeddings/_base.py`)**:
   - Calling `Embedder(model="gemini-embedding-001", *, dimensions=768, ...)` directly instantiates `VertexEmbedder` by default (`__new__` dispatches to `VertexEmbedder` when `cls is Embedder`), while subclassing `Embedder` initializes the subclass normally.
   - Core async & sync contract on `Embedder`:
     - `async def embed_query(self, text: str) -> VectorView` / `def embed_query_sync(self, text: str) -> VectorView`
     - `async def embed_document(self, text: str, *, title: str | None = None) -> VectorView` / `def embed_document_sync(self, text: str, *, title: str | None = None) -> VectorView`
     - `async def embed_many(self, texts: Sequence[str], *, task: EmbeddingTask | str = "document", titles: Sequence[str | None] | None = None) -> VectorBatchView` / `def embed_many_sync(...) -> VectorBatchView`
     - `async def embed_arrow(self, cursor_or_table: object, *, text_column: str = "text", output_column: str = "embedding") -> ArrowCursor`
     - `def similarity_search(self, query: str | VectorView, candidates: VectorBatchView, *, top_k: int = 10, metric: str = "cosine") -> list[SimilarityMatch]`
   - Integrates with `VectorCache` (optional `cache: VectorCache | None`) for `< 200 ns` exact FNV-1a text-hash deduplication and emits OpenTelemetry GenAI spans/metrics via [`GenAiUsage`](../../../../../src/py/vertebra/telemetry/genai.py)[^genai-telemetry].
2. **`VertexEmbedder` & `GeminiEmbedder` (`src/py/vertebra/embeddings/_google.py`)**:
   - `VertexEmbedder`:
     - Authenticates via `GcpTokenCache` (`t01`, supporting Cloud Run Metadata Server, local `gcloud` ADC with `x-goog-user-project`, and `target_service_account` impersonation, automatically retrying once on `401` after `cache.invalidate(gen)`).
     - Uses native gRPC framing (`encode_gemini_embed_grpc_frame` / `decode_gemini_embed_grpc_frame_to_batch`) over HTTP/2 (`application/grpc+proto`) or JSON fallback when using custom proxy endpoints, coalescing concurrent `embed_query` calls over an async micro-batcher (`max_batch_size=64`, `max_batch_tokens=16384`, `coalesce_window_ms=1.0`).
   - `GeminiEmbedder`:
     - Authenticates via `api_key` (`GEMINI_API_KEY` / `GOOGLE_API_KEY`) or `GcpTokenCache` against `generativelanguage.googleapis.com` (`gemini-embedding-001`, `gemini-embedding-2`) and supports multimodal inputs (`Part` bytes/URIs for `gemini-embedding-2`).
3. **`AlloyDbEmbedder` & `OracleEmbedder` (`src/py/vertebra/embeddings/_google.py`)**:
   - `AlloyDbEmbedder`: executes `SELECT embedding($1, t.txt)::vector FROM unnest($2::text[]) WITH ORDINALITY AS t(txt, ord) ORDER BY t.ord` (or inline SQL expression generation via `.sql_expr(column)` for zero-network-hop in-database search) and decodes binary `pgvector` frames directly into `VectorView` / `VectorBatchView`.
   - `OracleEmbedder`: executes `SELECT DBMS_VECTOR.UTL_TO_EMBEDDING(:text, JSON(:params)) FROM DUAL` (and `DBMS_VECTOR.UTL_TO_EMBEDDINGS` for batch inputs) and decodes Oracle 26ai `0xDB` binary `VECTOR` frames directly into `VectorView` / `VectorBatchView`, enforcing three Oracle 26ai `SYS.DBMS_VECTOR` invariants when `provider="vertexai"`:
     - Automatically ensures `:predict` is suffixed on the `"model"` JSON parameter (`"gemini-embedding-001"` -> `"gemini-embedding-001:predict"`) because `SYS.DBMS_VECTOR` concatenates `url || model` verbatim.
     - Enforces the `128`-character buffer ceiling on `"url"` (`https://<loc>-aiplatform.googleapis.com/v1/projects/<project>/locations/<loc>/publishers/google/models/`), preferring `project_number` when supplied and raising a descriptive `ValueError` before `ORA-06502` if the URL exceeds 128 characters.
     - Tracks `applied_token_generation: int` against `GcpTokenCache` (`t01`) when rotating `DBMS_VECTOR.CREATE_CREDENTIAL` OAuth2 `{"access_token": "..."}` credentials.
4. **`RemoteEmbedder` (`src/py/vertebra/embeddings/_remote.py`)**:
   - Connects to a remote Vertebra embedding service over `@grpc_method` (`GrpcServiceRegistry`[^grpc-registry]) and includes `create_embedding_grpc_service(embedder: Embedder) -> GrpcServiceRegistry` so any Vertebra app can expose a high-speed cluster embedding pool.
5. **Top-Level Lazy Export (`src/py/vertebra/__init__.py`)**:
   - Register `"vertebra.embeddings"` in `__lazy_modules__` (`PY-06`) and expose `Embedder`, `VectorView`, and `VectorBatchView` in `_LAZY_EXPORTS` and `__all__` in [`src/py/vertebra/__init__.py`](../../../../../src/py/vertebra/__init__.py).

---

## 3. TDD Verification Plan

### 3.1 Red-Green-Refactor Sequence

1. **Red**:
   - Write Rust unit tests in `src/rs/vertebra-core/src/vector.rs` and `src/rs/vertebra-http/src/grpc.rs` testing `encode_batch_embed_grpc_request` and `decode_batch_embed_grpc_response_into_slab` with synthetic packed `f32` Protobuf wire payloads, verifying exact `VectorSlab` population and unit L2 norm on truncated Matryoshka dimensions (`768d -> 256d`).
   - Write Python unit tests in `src/py/tests/unit/embeddings/test_google_embedders.py` verifying:
     - `isinstance(Embedder(), VertexEmbedder)` and `Embedder().model == "gemini-embedding-001"`, `Embedder().dimensions == 768`.
     - `VertexEmbedder` and `GeminiEmbedder` coalesce concurrent `embed_query` calls into a single batched gRPC frame, attach refreshed `Bearer` + `x-goog-user-project` headers from `GcpTokenCache`, retry on `401` after invalidating the stale token generation, and return unit-normalized `VectorView` / `VectorBatchView`.
     - `AlloyDbEmbedder` and `OracleEmbedder` generate parameterized SQL pushdown queries (including `:predict` suffix and `<= 128`-char URL validation for `OracleEmbedder`) and decode binary `pgvector` / Oracle 26ai `0xDB` wire responses into `VectorView` / `VectorBatchView`.
     - `RemoteEmbedder` roundtrips `embed_query` and `embed_many` through `create_embedding_grpc_service`.
2. **Green**:
   - Implement the gRPC packed-`f32` wire helpers in `vertebra-core/src/vector.rs`, `vertebra-http/src/grpc.rs`, and `vertebra-py/src/vector.rs`, implement `src/py/vertebra/embeddings/{__init__,_base,_google,_remote}.py`, and add `"vertebra.embeddings"` to `__lazy_modules__` and lazy exports in `src/py/vertebra/__init__.py`.
3. **Refactor & Verify**:
   - Run `cargo test -p vertebra-core -p vertebra-http`, `make dev`, `mypy.stubtest vertebra._vertebra`, and `pytest src/py/tests/unit/embeddings/test_google_embedders.py`.

### 3.2 Verification Commands

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-http
make dev
.venv/bin/python -m mypy.stubtest vertebra._vertebra --allowlist tools/stubtest_allowlist.txt
.venv/bin/pytest src/py/tests/unit/embeddings/test_google_embedders.py -v
```

---

## 4. Task Notes

- Re-exported `encode_batch_embed_grpc_request`, `encode_batch_embed_grpc_response_from_slab`, and `decode_batch_embed_grpc_response_into_slab` in `src/rs/vertebra-http/src/grpc.rs` and added `test_batch_embed_grpc_packed_float_reexport_and_mrl_norm`.
- Implemented `Embedder` (with `__new__` default factory returning `VertexEmbedder("gemini-embedding-001", dimensions=768)`), `MultimodalPart`, `VectorCache` integration, `embed_arrow`, and `similarity_search` in `src/py/vertebra/embeddings/_base.py`.
- Implemented `VertexEmbedder` (token-budgeted async micro-batching, `GcpTokenCache` auth + `x-goog-user-project`, and `401` retry after `invalidate`), `GeminiEmbedder` (API key + multimodal support), `AlloyDbEmbedder` (`unnest($2::text[]) WITH ORDINALITY` and `.sql_expr()`), and `OracleEmbedder` (`:predict` suffix, `128`-char URL ceiling guard, and `applied_token_generation` credential rotation) in `src/py/vertebra/embeddings/_google.py`.
- Implemented `RemoteEmbedder` and `create_embedding_grpc_service` in `src/py/vertebra/embeddings/_remote.py` and registered `"vertebra.embeddings"` in `__lazy_modules__` (`src/py/vertebra/__init__.py`).
- Verified with `cargo test -p vertebra-http`, `pytest src/py/tests/unit/embeddings/test_google_embedders.py -v` (6/6 passed), `ruff`, `mypy`, and `cargo clippy` (0 warnings).

---

## Provenance Footnotes

[^grpc-framing]: `src/rs/vertebra-http/src/grpc.rs::encode_grpc_frame` — Native 5-byte Length-Prefixed Message (LPM) HTTP/2 gRPC framing in `vertebra-http`.
[^grpc-registry]: `src/py/vertebra/extensions/grpc/__init__.py::GrpcServiceRegistry` — Native gRPC service registry and bidirectional streaming in `vertebra.extensions.grpc`.
[^alloydb-dialer]: `src/rs/vertebra-db/src/dialer.rs::AlloyDbDialer` — Native AlloyDB (`:5433` mTLS) and Cloud SQL (`:3307`) database dialers.
[^oracle-vec]: `src/rs/vertebra-db/src/oracle.rs::encode_oracle_vector_f32` — Oracle 26ai binary `VECTOR` (`FLOAT32`, `FLOAT64`, `INT8`, `BINARY`) wire codecs.
[^genai-telemetry]: `src/py/vertebra/telemetry/genai.py::GenAiUsage` — OpenTelemetry GenAI semantic conventions and metrics.
