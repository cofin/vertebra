---
type: Task
id: "gcp-embeddings-and-identity:ch03:t01"
chapter_id: ch03-google-embedder-runtime
title: Share Google embedder authentication, error mapping, and row validation
description: Introduce _GoogleEmbedder as the base of VertexEmbedder and GeminiEmbedder with async token-cache normalization, default ADC caches without storage fallback, one 401 retry, EmbeddingError mapping for HTTP and gRPC failures, and row-count validation.
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t03"
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch03-google-embedder-runtime/spec.md
files:
  - src/py/vertebra/embeddings/_google.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
estimated_loc: 220
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
  - id: py-google-embedders
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder._ensure_token_cache
    title: Forced metadata option, TypeError storage fallback, and isawaitable token handling
  - id: py-gemini-dispatch
    resource: src/py/vertebra/embeddings/_google.py::GeminiEmbedder._dispatch_with_auth_retry
    title: API-key dispatch override and RuntimeError mapping
  - id: py-embedder-base
    resource: src/py/vertebra/embeddings/_base.py::EmbeddingError
    title: Embedding failure type (VertebraError, RuntimeError)
  - id: py-storage-isolation-test
    resource: src/py/tests/unit/storage/test_gcs_token_refresh.py::test_google_embedder_uses_vertebra_gcp_token_cache_without_storage_import
    title: Existing guard that embedders do not import vertebra._storage
tags:
  - embeddings
  - auth
  - fail-loud
---

# Task t01: Share Google Embedder Authentication, Error Mapping, and Row Validation

## Objective & Scope Boundary

Refactor `src/py/vertebra/embeddings/_google.py` so `VertexEmbedder` and `GeminiEmbedder` both inherit from a module-private `_GoogleEmbedder(Embedder)` that owns token-cache resolution, request authorization, the single `401` retry, error mapping, and row-count validation [^py-google-embedders] [^py-gemini-dispatch]. Both classes keep their current request formats and constructor signatures (`ch04` and `ch05` change the formats). The micro-batcher moves to the base unchanged in this task (`t02` makes it per-loop).

Delete: `VertexEmbedder._ensure_token_cache`, `VertexEmbedder._dispatch_with_auth_retry`, `GeminiEmbedder._dispatch_with_auth_retry`, the `import_optional("vertebra.storage._store")` fallback, the forced `{"use_metadata_server": "true"}` option, and the `inspect`/`import_optional` imports when unused.

Out of scope: per-loop queues (`t02`), wire formats (`ch04`, `ch05`).

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py` (whole module).
- `src/py/vertebra/embeddings/_base.py::Embedder`, `::EmbeddingError` [^py-embedder-base].
- `src/py/vertebra/_vertebra.pyi::SyncGcpTokenCache`, `::AsyncGcpTokenCache`, `::GcpTokenSnapshot`.
- `src/py/tests/unit/embeddings/test_google_embedders.py::_FakeTokenCache`, `::test_vertex_embedder_grpc_microbatching_adc_headers_and_401_retry`, `::test_gemini_embedder_api_key_multimodal_and_embed_arrow`.
- `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_google_embedder_uses_vertebra_gcp_token_cache_without_storage_import` [^py-storage-isolation-test].
- Chapter spec.

## Symbol & Signature Contracts

```python
class _GoogleEmbedder(Embedder):
    """Shared Google Cloud authentication, error mapping, and micro-batching for Vertex AI and Gemini embedders."""

    DEFAULT_SCOPES: ClassVar[tuple[str, ...]] = ("https://www.googleapis.com/auth/cloud-platform",)
    PROVIDER: ClassVar[str] = "Google"

    def __init__(self, model: str, *, dimensions: int, endpoint: str,
                 token_cache: SyncGcpTokenCache | AsyncGcpTokenCache | None,
                 target_service_account: str | None, quota_project_id: str | None,
                 cache: VectorCache | None, max_batch_size: int, max_batch_tokens: int,
                 coalesce_window_ms: float, transport: GrpcTransportCallable | None) -> None: ...
    @property
    def endpoint(self) -> str: ...
    @property
    def resource_model_name(self) -> str: ...            # subclasses override
    async def _post_authorized(self, url: str, headers: dict[str, str], body: bytes) -> tuple[dict[str, str], bytes]: ...
    async def _auth_headers(self, *, force_refresh: bool) -> tuple[dict[str, str], int | None]: ...
    def _resolve_token_cache(self) -> AsyncGcpTokenCache: ...
    @staticmethod
    def _require_rows(batch: VectorBatchView, expected: int, provider: str) -> VectorBatchView: ...

class VertexEmbedder(_GoogleEmbedder):
    PROVIDER: ClassVar[str] = "Vertex AI"
class GeminiEmbedder(_GoogleEmbedder):
    PROVIDER: ClassVar[str] = "Gemini"
    async def _auth_headers(self, *, force_refresh: bool) -> tuple[dict[str, str], int | None]: ...  # api key -> ({"x-goog-api-key": key}, None)
```

Behavior:
- `__init__` stores `token_cache.to_async()` when given a `SyncGcpTokenCache`, otherwise the object as given (duck-typed caches stay supported for tests).
- `_resolve_token_cache` lazily builds `AsyncGcpTokenCache(options, list(self.DEFAULT_SCOPES))` where `options` holds only `target_service_account` and `quota_project_id` when set; a `ValueError` becomes `ImproperConfigurationError(f"{self.PROVIDER} embedder could not resolve Google Cloud credentials: {exc}")` raised from the original.
- `_auth_headers(force_refresh)` awaits `cache.get_token(force_refresh=force_refresh)` and returns `{"authorization": f"Bearer {snap.access_token}"}` plus `x-goog-user-project` (`quota_project_id` argument first, then the snapshot's) and `snap.generation`.
- `_post_authorized` merges auth headers into `headers`, calls the transport, and on `401` with a non-`None` generation calls `cache.invalidate(generation)` and retries once with `force_refresh=False`; a non-`200` final status raises `EmbeddingError(f"{self.PROVIDER} embedding request failed with HTTP {status}: {snippet}")` where `snippet` is the first `512` bytes decoded with `errors="replace"`; returns `(resp_headers, resp_body)`.
- Both current gRPC paths check `grpc-status` after `_post_authorized` and raise `EmbeddingError(f"{self.PROVIDER} embedding gRPC status {code}: {urllib.parse.unquote(message)}")`.
- `_require_rows` raises `EmbeddingError(f"{provider} returned {batch.count} embeddings for {expected} inputs")` on mismatch; both `_embed_batch_impl` paths call it.

## Edge Cases & Error Semantics

- `GeminiEmbedder` with an API key never touches a token cache, never retries `401`, and raises `EmbeddingError` with `HTTP 401` on the first failure.
- Transport exceptions (connection errors from `gcp_http2_post_async`) propagate unchanged.
- `GoogleCloudAuthError` from `get_token` propagates unchanged.
- `Embedder()` still constructs without credentials; credential errors surface on first request.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/embeddings/test_google_embedders.py` (injected transports, `_FakeTokenCache`), `src/py/tests/unit/storage/test_gcs_token_refresh.py` (storage-isolation guard).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_google_embedder_converts_sync_token_cache` | `VertexEmbedder(token_cache=SyncGcpTokenCache({"access_token": "tok"}, []))` | `isinstance(embedder._resolve_token_cache(), AsyncGcpTokenCache)` |
| `test_google_embedder_second_401_raises_embedding_error` | Transport always returns `(401, {}, b"denied")` | `pytest.raises(EmbeddingError, match="HTTP 401: denied")`; `2` transport calls; fake cache `invalidate` called with generation `1` |
| `test_google_embedder_grpc_status_maps_to_embedding_error` | Gemini API-key embedder; transport returns `(200, {"grpc-status": "3", "grpc-message": "bad%20model"}, b"")` | `EmbeddingError("Gemini embedding gRPC status 3: bad model")` |
| `test_google_embedder_row_count_mismatch` | Transport returns a `1`-row frame for `embed_many(["a", "b"])` | `EmbeddingError` containing `returned 1 embeddings for 2 inputs` |
| `test_google_embedder_missing_credentials_is_improper_configuration` | `monkeypatch` `vertebra.embeddings._google.AsyncGcpTokenCache` to raise `ValueError("no creds")` | First `embed_query` raises `ImproperConfigurationError` containing `Vertex AI embedder could not resolve` |
| `test_gemini_api_key_skips_token_cache_and_retry` | `GeminiEmbedder(api_key="k")`; transport returns `401` | One transport call; header `x-goog-api-key: k`; `EmbeddingError` |
| `test_google_embedder_uses_vertebra_gcp_token_cache_without_storage_import` (existing) | Unchanged | Still passes; `vertebra._storage` not imported |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k "google_embedder or gemini_api_key or vertex"
uv run pytest src/py/tests/unit/storage/test_gcs_token_refresh.py -q -k google_embedder
uv run ruff check src/py/vertebra/embeddings/_google.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run ruff format --check src/py/vertebra/embeddings/_google.py
uv run mypy src/py/vertebra/embeddings
```

Write the new tests first (Red), refactor, re-run. Commit as `refactor(embeddings): share Google embedder auth and error handling`.

## Acceptance Criteria

- [ ] `VertexEmbedder` and `GeminiEmbedder` inherit `_GoogleEmbedder`; storage fallback and forced metadata option are gone (R9).
- [ ] Sync caches are converted to async; default caches use core ADC order (R9).
- [ ] HTTP, gRPC, and row-count failures raise `EmbeddingError`; at most two transport calls per request (R9).

## Notes & Discoveries

- [2026-11-12 00:35] `isinstance(emb, VertexEmbedder)` remains true for `Embedder()`; `GeminiEmbedder` stops being a `VertexEmbedder` subclass, and no in-repo code relies on that relationship.

[^py-google-embedders]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder._ensure_token_cache` — Token resolution being replaced.
[^py-gemini-dispatch]: `src/py/vertebra/embeddings/_google.py::GeminiEmbedder._dispatch_with_auth_retry` — API-key override being replaced.
[^py-embedder-base]: `src/py/vertebra/embeddings/_base.py::EmbeddingError` — Error type.
[^py-storage-isolation-test]: `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_google_embedder_uses_vertebra_gcp_token_cache_without_storage_import` — Isolation guard.
