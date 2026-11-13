---
type: Spec
flow_id: gcp-embeddings-and-identity
chapter_id: ch02-vertex-and-gemini-embedder-wire-parity
title: "Chapter 02: Vertex AI REST, Gemini Multimodal Wire Parity, and Remote Identity"
description: "Fixes the Gemini BatchEmbedContents protobuf field numbers and multimodal media encoding, introduces the shared _GoogleEmbedder runtime, moves VertexEmbedder onto Vertex AI v1 REST predict and embedContent, and adds ID token authentication to RemoteEmbedder."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch01-gcp-id-token-cache
plan_commit: 18baadcd
last_reviewed_commit: null
updated_at: "2026-11-13T05:04:45Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - embeddings
  - vertex-ai
  - gemini
  - multimodal
sources:
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: "Gemini BatchEmbedContents request encoder currently writing Part.text on field 1"
  - id: py-vector-binding
    resource: src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame
    title: "PyO3 Gemini gRPC frame encoder registered via register_vector_bindings"
  - id: py-google
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: "VertexEmbedder and GeminiEmbedder transports and micro-batching"
  - id: py-models
    resource: src/py/vertebra/embeddings/_models.py::ModelSpec
    title: "Embedding model catalog metadata"
  - id: py-remote
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: "Remote gRPC embedder client"
  - id: gemini-content-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto
    title: "Gemini Part protobuf field numbers (text=2, inline_data=3, file_data=6)"
  - id: vertex-embed-ref
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent
    title: "Vertex AI v1 predict and embedContent REST schemas"
---

# Chapter 02: Vertex AI REST, Gemini Multimodal Wire Parity, and Remote Identity

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `encode_batch_embed_grpc_request` writes `Part.text` on protobuf field `1` [^core-vector-codec]; `GeminiEmbedder.embed_multimodal` base64-encodes media into plain text strings; `VertexEmbedder` posts `GenerativeService/BatchEmbedContents` gRPC payloads to `*-aiplatform.googleapis.com` and shares one micro-batch queue across event loops [^py-google]; `RemoteEmbedder` supports only static bearer tokens [^py-remote].
- **Target Architecture**: `encode_batch_embed_contents_grpc_request` and `encode_gemini_embed_contents_grpc_frame` encode `Part.text = 2`, `Part.inline_data = 3` (`Blob{1,2}`), and `Part.file_data = 6` (`FileData{1,2}`) [^py-vector-binding] [^gemini-content-proto]. `_GoogleEmbedder` normalizes token caches to async, retries one `401`, raises `EmbeddingError`, and isolates micro-batch queues per event loop. `VertexEmbedder` posts JSON to Vertex AI v1 `:predict` and `:embedContent` [^py-models] [^vertex-embed-ref], and `RemoteEmbedder` supports `id_token_cache`.
- **Scope & Boundaries**: Covers `R5`–`R9` and `R11` across `vertebra-core`, `vertebra-py`, `_vertebra.pyi`, and `vertebra/embeddings/{_google,_models,_remote}.py` (`4` compact tasks).
<!-- truth: end -->

## 1. Specification, Context & Provenance

In `google.ai.generativelanguage.v1beta.Part`, field `1` is unused while `text = 2`, `inline_data = 3`, and `file_data = 6` [^gemini-content-proto]. Meanwhile, `VertexEmbedder` sends Gemini Developer API gRPC frames to `*-aiplatform.googleapis.com`, which serves Vertex AI REST JSON `:predict` (`gemini-embedding-001`, `text-embedding-005`, `text-multilingual-embedding-002`) and `:embedContent` (`gemini-embedding-2`) [^py-google] [^vertex-embed-ref]. Extracting `_GoogleEmbedder` gives both cloud classes clean per-loop micro-batching and `401` token refresh without coupling Gemini to Vertex project/location rules, while `RemoteEmbedder` gains audience-bound ID token auth from `ch01` [^py-remote]. Guidance: [embeddings-and-vectors.md](../../../../knowledge/domains/agent/embeddings-and-vectors.md).

## 2. Requirements, Concrete Contracts & Architecture

```rust
// src/rs/vertebra-core/src/vector.rs
pub enum GeminiContentPart<'a> {
    Text(&'a str),
    InlineData { mime_type: &'a str, data: &'a [u8] },
    FileData { mime_type: &'a str, file_uri: &'a str },
}
pub fn encode_batch_embed_contents_grpc_request(
    model: &str,
    contents: &[Vec<GeminiContentPart<'_>>],
    task_type: Option<u32>,
    titles: Option<&[Option<String>]>,
    output_dimensionality: Option<u32>,
) -> Result<Vec<u8>, String>;
```

```python
# src/py/vertebra/_vertebra.pyi
def encode_gemini_embed_contents_grpc_frame(
    model: str,
    contents: Sequence[Sequence[tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]]],
    task: str | None = None,
    titles: Sequence[str | None] | None = None,
    output_dimensionality: int | None = None,
) -> bytes: ...

# src/py/vertebra/embeddings/_models.py
class ModelSpec(msgspec.Struct, frozen=True, gc=False):
    # existing fields retained
    vertex_method: Literal["predict", "embed_content"] = "predict"
    max_instances_per_request: int = 1

# src/py/vertebra/embeddings/_google.py
class _GoogleEmbedder(Embedder):
    DEFAULT_SCOPES: ClassVar[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)
    async def _post_authorized(self, url: str, headers: dict[str, str], body: bytes) -> tuple[dict[str, str], bytes]: ...
    @staticmethod
    def _require_rows(batch: VectorBatchView, expected: int, provider: str) -> VectorBatchView: ...

class VertexEmbedder(_GoogleEmbedder):
    def __init__(
        self, model: str = "gemini-embedding-001", *, dimensions: int = 768, project: str | None = None,
        location: str = "us-central1", endpoint: str | None = None,
        token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
        target_service_account: str | None = None, quota_project_id: str | None = None,
        cache: VectorCache | None = None, max_batch_size: int = 64, max_batch_tokens: int = 16_384,
        coalesce_window_ms: float = 1.0, max_concurrent_requests: int = 8, auto_truncate: bool = False,
        transport: GrpcTransportCallable | None = None,
    ) -> None: ...
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...

# src/py/vertebra/embeddings/_remote.py
class RemoteEmbedder(Embedder):
    def __init__(
        self, endpoint: str = "http://127.0.0.1:8000", model: str = "gemini-embedding-001", *,
        dimensions: int = 768, bearer_token: str | None = None,
        id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None,
        audience: str | None = None, cache: VectorCache | None = None,
        transport: GrpcTransportCallable | None = None,
    ) -> None: ...
```

- **Key Wire & Error Contracts**:
  - `GeminiEmbedder`: sends raw bytes inside `Part.inline_data` (`0x1a`) and URIs inside `Part.file_data` (`0x32`); rejects `gs://` URIs with `ValueError` naming `VertexEmbedder`, rejects empty or dual `data`+`uri` `MultimodalPart` values, and validates returned row count via `_require_rows`.
  - `VertexEmbedder`: URL `{endpoint}/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:{predict|embedContent}` (`https://aiplatform.googleapis.com` for `location == "global"`); missing `project` raises `ImproperConfigurationError` on first request; non-`None` title outside `document`/`search_document` raises `ValueError`; `:predict` chunks by `max_instances_per_request` (`1` for `gemini-embedding-001`, `250` for `text-embedding-005` / `text-multilingual-embedding-002`) and `20,000` estimated tokens via `asyncio.TaskGroup` + `Semaphore(max_concurrent_requests)`; `:embedContent` sends `{"content": {"role": "user", "parts": [...]}, "taskType": ..., "outputDimensionality": ..., "autoTruncate": ...}` (`inlineData: {"mimeType", "data": base64}`, `fileData: {"mimeType", "fileUri"}` allowing `gs://`).
  - `RemoteEmbedder`: setting both `bearer_token` and `id_token_cache` raises `ValueError`; default `audience` is `{scheme}://{netloc}` of `endpoint`; HTTP `401` calls `invalidate(audience, generation)` and retries once.

## 3. Documentation & OKF Knowledge Plan

User documentation and OKF updates for Vertex REST, Gemini multimodal, and `RemoteEmbedder` ID tokens land in `ch04`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R5` (Gemini protobuf field-2 + media part encoder & PyO3 frame binding) | `t01` | `src/rs/vertebra-{core,py}/src/vector.rs`, `src/py/vertebra/_vertebra.pyi` | `cargo test -p vertebra-core --lib vector::` & `pytest test_vector_wire_codecs.py` |
| `R6`, `R9` (`_GoogleEmbedder` base, per-loop batching, Gemini real multimodal) | `t02` | `src/py/vertebra/embeddings/_google.py` | `pytest test_google_embedders.py -k "google or gemini or micro_batch"` |
| `R7`, `R8` (`ModelSpec` Vertex limits, Vertex REST `:predict` / `:embedContent` + multimodal) | `t03` | `src/py/vertebra/embeddings/{_models,_google}.py` | `pytest test_google_embedders.py -k vertex` |
| `R11` (`RemoteEmbedder` ID token cache authentication & `401` retry) | `t04` | `src/py/vertebra/embeddings/_remote.py` | `pytest test_google_embedders.py -k remote` |

## 5. Implementation Plan

- [x] [484568a7] [t01-gemini-content-part-wire-codec.md](tasks/t01-gemini-content-part-wire-codec.md) — Fix `Part.text` to field `2`, add `GeminiContentPart` (`inline_data=3`, `file_data=6`) and `encode_gemini_embed_contents_grpc_frame`, and update `_vertebra.pyi`.
- [x] [484568a7] [t02-google-embedder-base-and-gemini-multimodal.md](tasks/t02-google-embedder-base-and-gemini-multimodal.md) — Extract `_GoogleEmbedder` (async cache normalization, `401` retry, `EmbeddingError`, per-loop batching) and wire real media parts in `GeminiEmbedder.embed_multimodal`.
- [x] [484568a7] [t03-vertex-rest-predict-and-multimodal.md](tasks/t03-vertex-rest-predict-and-multimodal.md) — Extend `ModelSpec` and migrate `VertexEmbedder` text and multimodal embedding onto Vertex AI v1 REST `:predict` and `:embedContent`.
- [x] [484568a7] [t04-remote-embedder-id-token-auth.md](tasks/t04-remote-embedder-id-token-auth.md) — Add `id_token_cache` and `audience` with `401` invalidation retry to `RemoteEmbedder`.

## 6. Current State & Remaining Plan Snapshot

- **Status**: `t01`–`t04` are implemented and verified at `484568a7` (`cargo clippy`, `vector::` `3` passed, `vertebra-http grpc` `8` passed, `mypy.stubtest vertebra._vertebra`, `ruff`, `mypy`, and `135` Python tests pass).
- **Fast Chapter Gate (`< 35s`)**:
  `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib vector:: && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http grpc && uv run --no-sync maturin develop --uv && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync ruff check src/py/vertebra/embeddings && uv run --no-sync mypy src/py/vertebra/embeddings && uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/embeddings/test_google_embedders.py -q`

[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Gemini `BatchEmbedContents` request encoder currently writing `Part.text` on field `1`.
[^py-vector-binding]: `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame` — PyO3 Gemini gRPC frame encoder registered via `register_vector_bindings`.
[^py-google]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — `VertexEmbedder` and `GeminiEmbedder` transports and micro-batching.
[^py-models]: `src/py/vertebra/embeddings/_models.py::ModelSpec` — Embedding model catalog metadata.
[^py-remote]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — Remote gRPC embedder client.
[^gemini-content-proto]: `https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto` — Gemini `Part` protobuf field numbers (`text=2`, `inline_data=3`, `file_data=6`).
[^vertex-embed-ref]: `https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent` — Vertex AI v1 `predict` and `embedContent` REST schemas.
