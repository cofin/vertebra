---
type: Task
id: "gcp-embeddings-and-identity:ch05:t01"
chapter_id: ch05-vertex-rest-and-remote-identity
title: Embed text through Vertex AI REST predict and embedContent
description: Replace the VertexEmbedder gRPC path with Vertex AI REST JSON requests routed by ModelSpec.vertex_method, with per-model instance limits, bounded concurrent fan-out, project and location resolution, title and dimension validation, and an auto_truncate option defaulting to False.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch04:t02"
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_models.py
  - src/py/vertebra/utils/serializers/_json.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch05-vertex-rest-and-remote-identity/spec.md
files:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_models.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 240
plan_revision: 1
plan_commit: null
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-12T00:35:00Z"
updated_at: "2026-11-12T00:35:00Z"
commit: null
sources:
  - id: py-vertex
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: Vertex embedder after the ch03 runtime refactor
  - id: py-models
    resource: src/py/vertebra/embeddings/_models.py::MODEL_CATALOG
    title: Catalog entries for gemini-embedding-001, gemini-embedding-2, text-embedding-005
  - id: py-json
    resource: src/py/vertebra/utils/serializers/_json.py::decode_json
    title: Centralized JSON encode/decode helpers
  - id: vertex-predict-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api
    title: predict instances (content, task_type, title), parameters (outputDimensionality, autoTruncate), predictions[].embeddings.values
  - id: vertex-embed-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent
    title: embedContent body (content, taskType, title, outputDimensionality, autoTruncate) and embedding.values
tags:
  - vertex-ai
  - embeddings
  - rest
---

# Task t01: Embed Text Through Vertex AI REST `:predict` and `:embedContent`

## Objective & Scope Boundary

Rewrite `VertexEmbedder` text embedding in `src/py/vertebra/embeddings/_google.py` to post Vertex AI REST JSON [^py-vertex] [^vertex-predict-reference] [^vertex-embed-reference], and extend `src/py/vertebra/embeddings/_models.py::ModelSpec` with routing metadata [^py-models]. Requests and responses are `msgspec.Struct` types encoded and decoded through `vertebra.utils.serializers` (`encode_json_bytes`, `decode_json`) [^py-json]. The `encode_gemini_embed_grpc_frame`, `decode_gemini_embed_grpc_frame_to_batch`, and gRPC header code paths leave `VertexEmbedder` entirely.

Out of scope: `embed_multimodal` on Vertex (`t02`), `RemoteEmbedder` (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py::VertexEmbedder`, `::_GoogleEmbedder` (after `ch03`/`ch04`).
- `src/py/vertebra/embeddings/_models.py::ModelSpec`, `::MODEL_CATALOG`, `::get_model_spec`.
- `src/py/vertebra/utils/serializers/_json.py::encode_json_bytes`, `::decode_json`.
- `src/py/tests/unit/embeddings/test_google_embedders.py::test_vertex_embedder_grpc_microbatching_adc_headers_and_401_retry`, `::test_clean_domain_naming_and_default_factory`, `::test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default` (default-transport assertions for Vertex).
- Chapter spec.

## Symbol & Signature Contracts

```python
# _models.py
class ModelSpec(msgspec.Struct, frozen=True, gc=False):
    ...  # existing fields unchanged
    vertex_method: Literal["predict", "embed_content"] = "predict"
    max_instances_per_request: int = 1
# Catalog: gemini-embedding-001 -> ("predict", 1); gemini-embedding-2 -> ("embed_content", 1);
# text-embedding-005 -> ("predict", 250); new "text-multilingual-embedding-002": provider "vertex",
# default_dimensions 768, supported_dimensions (128, 256, 512, 768), max_tokens 2048, ("predict", 250).

# _google.py
class _VertexPredictInstance(msgspec.Struct, omit_defaults=True):
    content: str
    task_type: str
    title: str | None = None
class _VertexPredictParameters(msgspec.Struct, rename="camel"):
    output_dimensionality: int
    auto_truncate: bool
class _VertexPredictRequest(msgspec.Struct):
    instances: list[_VertexPredictInstance]
    parameters: _VertexPredictParameters
class _VertexEmbeddingValues(msgspec.Struct):
    values: list[float]
class _VertexPrediction(msgspec.Struct):
    embeddings: _VertexEmbeddingValues
class _VertexPredictResponse(msgspec.Struct):
    predictions: list[_VertexPrediction]
class _VertexPart(msgspec.Struct, omit_defaults=True, rename="camel"):
    text: str | None = None
    inline_data: _VertexBlob | None = None
    file_data: _VertexFileData | None = None
class _VertexBlob(msgspec.Struct, rename="camel"):
    mime_type: str
    data: str
class _VertexFileData(msgspec.Struct, rename="camel"):
    mime_type: str
    file_uri: str
class _VertexContent(msgspec.Struct):
    parts: list[_VertexPart]
    role: str = "user"
class _VertexEmbedContentRequest(msgspec.Struct, omit_defaults=True, rename="camel"):
    content: _VertexContent
    task_type: str
    output_dimensionality: int
    auto_truncate: bool
    title: str | None = None
class _VertexEmbedContentResponse(msgspec.Struct):
    embedding: _VertexEmbeddingValues

class VertexEmbedder(_GoogleEmbedder):
    PROVIDER: ClassVar[str] = "Vertex AI"
    MAX_PREDICT_TOKENS_PER_REQUEST: ClassVar[int] = 20_000
    VERTEX_TASK_TYPES: ClassVar[dict[str, str]] = {
        "query": "RETRIEVAL_QUERY", "search_query": "RETRIEVAL_QUERY",
        "document": "RETRIEVAL_DOCUMENT", "search_document": "RETRIEVAL_DOCUMENT",
        "question_answering": "QUESTION_ANSWERING", "fact_verification": "FACT_VERIFICATION",
        "classification": "CLASSIFICATION", "clustering": "CLUSTERING",
        "semantic_similarity": "SEMANTIC_SIMILARITY", "code_retrieval": "CODE_RETRIEVAL_QUERY",
    }
    def __init__(..., max_concurrent_requests: int = 8, auto_truncate: bool = False, ...) -> None: ...  # full signature in chapter spec
    @property
    def project(self) -> str | None: ...
    @property
    def resource_model_name(self) -> str: ...
    @property
    def vertex_method(self) -> Literal["predict", "embed_content"]: ...
    async def _embed_batch_impl(self, texts: list[str], *, task: str = "document", titles: list[str | None] | None = None) -> VectorBatchView: ...
    async def _post_vertex(self, method: Literal["predict", "embed_content"], payload: msgspec.Struct) -> bytes: ...
    def _vertex_task_type(self, task: str) -> str: ...
    def _chunk_texts(self, texts: list[str], titles: list[str | None]) -> list[tuple[list[str], list[str | None]]]: ...
    def _checked_rows(self, rows: list[list[float]]) -> list[list[float]]: ...
```

Resolution: `project = project or GOOGLE_CLOUD_PROJECT or GCP_PROJECT or None`; `endpoint = endpoint or ("https://aiplatform.googleapis.com" if location == "global" else f"https://{location}-aiplatform.googleapis.com")`; `resource_model_name` returns `model` unchanged when it starts with `projects/`, otherwise `projects/{project}/locations/{location}/publishers/google/models/{model}` and raises `ImproperConfigurationError("VertexEmbedder requires a Google Cloud project; pass project= or set GOOGLE_CLOUD_PROJECT")` without a project. URL: `{endpoint.rstrip('/')}/v1/{resource_model_name}:predict` or `:embedContent`. `max_concurrent_requests < 1` raises `ValueError` at construction.

Fan-out: `predict` chunks respect `spec.max_instances_per_request` (unknown model: `1`) and `MAX_PREDICT_TOKENS_PER_REQUEST` (estimated `max(1, len(text) // 4)`; an over-budget text forms its own chunk); `embed_content` sends one text per request. Chunks run inside `asyncio.TaskGroup` with an `asyncio.Semaphore(max_concurrent_requests)`; an `ExceptionGroup` is unwrapped and its first exception re-raised.

## Edge Cases & Error Semantics

- A non-`None` title with a task that does not map to `RETRIEVAL_DOCUMENT` raises `ValueError("Vertex AI accepts titles only for document embeddings")` before any request.
- Unknown task strings raise `ValueError(f"unsupported embedding task {task!r}")`.
- Each vector must have exactly `self._dimensions` values, else `EmbeddingError(f"Vertex AI returned {n} values; expected {d}")`.
- `predictions` length differing from the chunk size raises `EmbeddingError` via `_require_rows` semantics (`returned N embeddings for M inputs`).
- Malformed JSON raises `EmbeddingError("Vertex AI returned an invalid response: <detail>")` (wrapping `msgspec.ValidationError` / `msgspec.DecodeError`).
- `auto_truncate=False` is sent explicitly; over-length input yields the API's `400`, surfaced as `EmbeddingError` with the response snippet.
- Results are normalized (`VectorBatchView.from_f32_rows(rows, normalize=True)`) because MRL-truncated Vertex vectors are not unit length.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py` (injected transport returning JSON bodies; `_FakeTokenCache`). Existing Vertex tests that build gRPC frames are rewritten to JSON responses.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_vertex_predict_one_instance_per_request` | `VertexEmbedder("gemini-embedding-001", project="p", dimensions=4, coalesce_window_ms=0.0)`; `embed_many(["a","b","c"])`; transport answers `{"predictions":[{"embeddings":{"values":[k,0,0,0]}}]}` with `k` = request ordinal + 1 | `3` calls to `https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/us-central1/publishers/google/models/gemini-embedding-001:predict`; body `{"instances":[{"content":"a","task_type":"RETRIEVAL_DOCUMENT"}],"parameters":{"outputDimensionality":4,"autoTruncate":false}}`; headers include `authorization: Bearer mock-vertex-token-1` and `content-type: application/json`; rows in input order |
| `test_vertex_predict_batches_text_embedding_005` | `text-embedding-005`, `300` texts `"t"` | `2` calls with `250` and `50` instances |
| `test_vertex_predict_token_budget_splits` | `text-embedding-005`, texts `"x" * 50_000` twice | `2` calls, one instance each |
| `test_vertex_embed_content_for_gemini_embedding_2` | `gemini-embedding-2`, `embed_query("hi")`; transport answers `{"embedding":{"values":[1,0,0,0]}}` | URL ends `gemini-embedding-2:embedContent`; body `{"content":{"parts":[{"text":"hi"}],"role":"user"},"taskType":"RETRIEVAL_QUERY","outputDimensionality":4,"autoTruncate":false}` |
| `test_vertex_requires_project_at_request_time` | `monkeypatch.delenv` both env vars; `VertexEmbedder()` constructs; `await embed_query("x")` | `ImproperConfigurationError` matching `requires a Google Cloud project`; zero transport calls |
| `test_vertex_global_location_and_endpoint_override` | `location="global"`; then `endpoint="http://127.0.0.1:9"` | URLs start `https://aiplatform.googleapis.com/v1/projects/p/locations/global/` and `http://127.0.0.1:9/v1/` |
| `test_vertex_title_and_dimension_validation` | `embed_many(["a"], task="query", titles=["T"])`; transport returning `8` values for `dimensions=4` | `ValueError` matching `titles only for document`; `EmbeddingError` matching `returned 8 values; expected 4` |
| `test_vertex_chunk_failure_cancels_siblings` | `3` texts, `max_concurrent_requests=3`; request 2 returns `500`, requests 1 and 3 await an event | `EmbeddingError` matching `HTTP 500`; the waiting requests observe `asyncio.CancelledError` |
| `test_vertex_401_retry_and_quota_header` (rewritten from the existing micro-batching test) | First call `401`, then success | Second call carries `Bearer mock-vertex-token-2`; `x-goog-user-project: quota-proj-123` on both |
| `test_clean_domain_naming_and_default_factory` (existing) | `Embedder()` | Still a `VertexEmbedder`; constructs without a project |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k "vertex or default_factory"
uv run pytest src/py/tests/unit/embeddings -q
uv run ruff check src/py/vertebra/embeddings src/py/tests/unit/embeddings
uv run mypy src/py/vertebra/embeddings
```

Write the tests first (Red: requests go to `GenerativeService/BatchEmbedContents`), implement, re-run. Commit as `fix(embeddings): call Vertex AI predict and embedContent over REST`.

## Acceptance Criteria

- [ ] `VertexEmbedder` posts documented Vertex AI REST requests with project and location (R7).
- [ ] Instance limits, token budgets, and bounded concurrency follow `ModelSpec` (R7).
- [ ] Missing project, title misuse, dimension drift, and malformed responses fail loud (R7).
- [ ] `auto_truncate` defaults to `False` and is sent explicitly (R7).

## Notes & Discoveries

- [2026-11-12 00:35] `gemini-embedding-001` accepts one instance per `:predict` request, so the micro-batcher coalesces calls but each chunk fans out into single-instance requests.

[^py-vertex]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — Embedder rewritten here.
[^py-models]: `src/py/vertebra/embeddings/_models.py::MODEL_CATALOG` — Routing metadata.
[^py-json]: `src/py/vertebra/utils/serializers/_json.py::decode_json` — Serializer helpers.
[^vertex-predict-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api` — `predict` contract.
[^vertex-embed-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent` — `embedContent` contract.
