---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch05-vertex-rest-and-remote-identity
title: "Chapter 05: Vertex REST and Remote Identity"
description: Move VertexEmbedder to Vertex AI REST predict and embedContent, add Vertex multimodal embedding, and authenticate RemoteEmbedder with ID tokens.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-id-token-python-surface
  - ch04-gemini-wire-and-multimodal
replan_trigger: null
current_task: null
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
generated:
  by: agent:plan-generator
  at: "2026-11-12T00:35:00Z"
sources:
  - id: py-vertex
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder._embed_batch_impl
    title: Vertex path posting a GenerativeService gRPC frame to aiplatform at HEAD
  - id: py-models
    resource: src/py/vertebra/embeddings/_models.py::ModelSpec
    title: Model catalog metadata extended with Vertex method and instance limits
  - id: py-remote
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: Remote gRPC embedder with static bearer token
  - id: vertex-embed-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent
    title: Vertex AI v1 embedContent request and response schema
  - id: vertex-predict-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api
    title: Vertex AI text embeddings predict schema and per-model instance limits
tags:
  - vertex-ai
  - embeddings
  - identity
  - rest
---

# Chapter 05: Vertex REST and Remote Identity

<!-- truth: start -->
- **Scope**: `ch05:t01` moves `VertexEmbedder` text embedding to Vertex AI REST (`:predict` for `gemini-embedding-001`, `text-embedding-005`, `text-multilingual-embedding-002`; `:embedContent` for `gemini-embedding-2`) using `ModelSpec.vertex_method` and `ModelSpec.max_instances_per_request` [^py-vertex] [^py-models] [^vertex-predict-reference]; `ch05:t02` implements `VertexEmbedder.embed_multimodal` over `:embedContent` with `inlineData` and `fileData` (`gs://` allowed) [^vertex-embed-reference]; `ch05:t03` adds `id_token_cache` and `audience` to `RemoteEmbedder` [^py-remote].
- **Invariants**: request URL `{endpoint}/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:{method}`; `global` location uses `https://aiplatform.googleapis.com`; the project is required at request time; every returned vector has exactly `dimensions` values; chunk failures cancel sibling requests and surface the first underlying error.
- **Verification Gate**: `uv run pytest src/py/tests/unit/embeddings -q && uv run mypy src/py/vertebra/embeddings`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`VertexEmbedder._embed_batch_impl` posts `google.ai.generativelanguage.v1beta.GenerativeService/BatchEmbedContents` to `https://{location}-aiplatform.googleapis.com`, which serves Vertex AI (`google.cloud.aiplatform.v1`), not the Gemini Developer API, and it never sends the project or location [^py-vertex]. Vertex AI serves text embeddings through `publishers/google/models/{model}:predict` (one instance per request for `gemini-embedding-001`, up to `250` instances and `20,000` tokens for `text-embedding-005` and `text-multilingual-embedding-002`) and multimodal embeddings through `:embedContent` (`gemini-embedding-2`) [^vertex-predict-reference] [^vertex-embed-reference].

`RemoteEmbedder` sends a static bearer token, which cannot authenticate to a Cloud Run-hosted embedding service whose tokens expire hourly [^py-remote].

### 1.2 Non-Goals

- Vertex batch prediction jobs or `text-embedding-large` style asynchronous APIs.
- `RemoteEmbedder` transport or wire format changes.
- Client-side request size limits (the API enforces them).

### 1.3 Upstream Baseline & Code Analysis

- After `ch03`, `VertexEmbedder` inherits `_GoogleEmbedder` (`_post_authorized`, `_require_rows`, per-loop micro-batching); after `ch04`, `_GoogleEmbedder._content_parts(item, allow_gcs_uris=...)` builds validated part tuples.
- `vertebra.utils.serializers` provides `encode_json_bytes` and `decode_json(data, type=...)`.
- `MODEL_CATALOG` lists `gemini-embedding-001` (`vertex`), `gemini-embedding-2` (`gemini`), and `text-embedding-005` (`vertex`).
- `ch02` provides `SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache`.

Applicable guidance: [python-framework SKILL.md](../../../../../skills/python-framework/SKILL.md), [msgspec usage in product-guidelines.md](../../../../product/product-guidelines.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

```python
class ModelSpec(msgspec.Struct, frozen=True, gc=False):
    ...
    vertex_method: Literal["predict", "embed_content"] = "predict"
    max_instances_per_request: int = 1

class VertexEmbedder(_GoogleEmbedder):
    PROVIDER: ClassVar[str] = "Vertex AI"
    MAX_PREDICT_TOKENS_PER_REQUEST: ClassVar[int] = 20_000
    VERTEX_TASK_TYPES: ClassVar[dict[str, str]]
    def __init__(self, model: str = "gemini-embedding-001", *, dimensions: int = 768, project: str | None = None,
                 location: str = "us-central1", endpoint: str | None = None,
                 token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None = None,
                 target_service_account: str | None = None, quota_project_id: str | None = None,
                 cache: VectorCache | None = None, max_batch_size: int = 64, max_batch_tokens: int = 16_384,
                 coalesce_window_ms: float = 1.0, max_concurrent_requests: int = 8, auto_truncate: bool = False,
                 transport: GrpcTransportCallable | None = None) -> None: ...
    @property
    def project(self) -> str | None: ...
    @property
    def location(self) -> str: ...
    @property
    def resource_model_name(self) -> str: ...      # raises ImproperConfigurationError without a project
    @property
    def vertex_method(self) -> Literal["predict", "embed_content"]: ...
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...

class RemoteEmbedder(Embedder):
    def __init__(self, endpoint: str = "http://127.0.0.1:8000", model: str = "gemini-embedding-001", *,
                 dimensions: int = 768, bearer_token: str | None = None,
                 id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None,
                 audience: str | None = None, cache: VectorCache | None = None,
                 transport: GrpcTransportCallable | None = None) -> None: ...
```

### Caller -> Seam -> Collaborator Map

- `VertexEmbedder._embed_batch_impl` -> `_validate_titles` -> `_chunk_requests` -> `asyncio.TaskGroup` bounded by `asyncio.Semaphore(max_concurrent_requests)` -> `_post_authorized(url, {"content-type": "application/json"}, encode_json_bytes(request))` -> `decode_json(body, type=_VertexPredictResponse | _VertexEmbedContentResponse)` -> dimension check -> `VectorBatchView.from_f32_rows(rows, normalize=True)` -> `_require_rows`.
- `VertexEmbedder.embed_multimodal` -> `_content_parts(item, allow_gcs_uris=True)` -> `_VertexContent` JSON parts -> `:embedContent` per item.
- `RemoteEmbedder._embed_batch_impl` -> `AsyncGcpIdTokenCache.get_token(audience)` -> transport -> `401` -> `invalidate(audience, generation)` -> retry once.

### Edge-Case & Invariant Matrix

| Scenario | Input | Expected | Owner |
| --- | --- | --- | --- |
| `gemini-embedding-001` batch | `3` texts | `3` `:predict` requests, one instance each, rows in order | `t01` |
| `text-embedding-005` batch | `300` short texts | `2` requests (`250` + `50`) | `t01` |
| Token budget | `2` texts of `50,000` chars on `text-embedding-005` | `2` requests (each `> 20,000` estimated tokens alone) | `t01` |
| `gemini-embedding-2` text | `"hi"` | `:embedContent` body `{"content":{"parts":[{"text":"hi"}],"role":"user"},"taskType":"RETRIEVAL_DOCUMENT","outputDimensionality":4,"autoTruncate":false}` | `t01` |
| Missing project | No project or env vars | `ImproperConfigurationError` on first request | `t01` |
| Global location | `location="global"` | Host `https://aiplatform.googleapis.com` | `t01` |
| Title misuse | `titles=["T"]` with `task="query"` | `ValueError` | `t01` |
| Dimension drift | `3072` values for `dimensions=4` | `EmbeddingError` | `t01` |
| Chunk failure | One of `3` requests returns `500` | `EmbeddingError`; siblings cancelled | `t01` |
| Multimodal on predict model | `gemini-embedding-001` + `embed_multimodal` | `ValueError` naming `gemini-embedding-2` | `t02` |
| `gs://` on Vertex | `MultimodalPart(uri="gs://b/v.mp4", mime_type="video/mp4")` | `fileData` part with `fileUri` `gs://b/v.mp4` | `t02` |
| Remote auth conflict | `bearer_token` and `id_token_cache` both set | `ValueError` | `t03` |
| Remote default audience | `endpoint="https://emb-abc.a.run.app/"` | Audience `https://emb-abc.a.run.app` | `t03` |

### Alternatives Comparison

- A Vertex gRPC `PredictionService/EmbedContent` client would reuse packed-float decoding but depends on protobuf field numbers for deprecated and nested config fields that are not verified in-repo; REST JSON matches the published reference exactly.
- Routing every Vertex model through `:embedContent` would simplify code but `gemini-embedding-001` and `text-embedding-*` are documented on `:predict`; the catalog makes the method explicit per model and defaults unknown models to `:predict` with one instance.

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/embeddings-and-vectors.md` (`ch06:t01`) documents the Vertex REST transport, `auto_truncate`, multimodal examples, and `RemoteEmbedder(id_token_cache=...)`; the OKF chapter (`ch06:t02`) records the method routing and limits.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical Test |
| --- | --- | --- | --- |
| R7 | `t01` | `regression_tdd` | `test_google_embedders.py::test_vertex_*` |
| R8 | `t02` | `behavior_tdd` | `test_google_embedders.py::test_vertex_multimodal_*` |
| R11 | `t03` | `behavior_tdd` | `test_google_embedders.py::test_remote_embedder_id_token_*` |

## 5. Implementation Plan

- [ ] t01 — Vertex AI REST `:predict` and `:embedContent` transport (tasks/t01-vertex-predict-and-embed-content.md)
- [ ] t02 — Vertex multimodal embedding (tasks/t02-vertex-multimodal-embedding.md)
- [ ] t03 — `RemoteEmbedder` ID token authentication (tasks/t03-remote-embedder-id-token-auth.md)
- **Chapter Recovery Checkpoint**: `uv run pytest src/py/tests/unit/embeddings src/py/tests/unit/storage/test_gcs_token_refresh.py -q && uv run ruff check src/py/vertebra/embeddings src/py/tests/unit/embeddings && uv run mypy src/py/vertebra/embeddings`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/3` tasks completed; depends on `ch02` (ID token classes) and `ch04` (`_content_parts`).

[^py-vertex]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder._embed_batch_impl` — Vertex path replaced here.
[^py-models]: `src/py/vertebra/embeddings/_models.py::ModelSpec` — Catalog metadata.
[^py-remote]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — Remote client.
[^vertex-embed-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent` — `embedContent` schema.
[^vertex-predict-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api` — `predict` schema and limits.
