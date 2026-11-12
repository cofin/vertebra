---
type: Task
id: "gcp-embeddings-and-identity:ch05:t03"
chapter_id: ch05-vertex-rest-and-remote-identity
title: Authenticate RemoteEmbedder with audience-bound ID tokens
description: Add id_token_cache and audience parameters to RemoteEmbedder, default the audience to the endpoint origin, retry once after invalidating on HTTP 401, reject conflicting auth options, and raise EmbeddingError for transport failures.
status: draft
state: pending
priority: P2
verification_strategy: behavior_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch05:t02"
read_set:
  - src/py/vertebra/embeddings/_remote.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch05-vertex-rest-and-remote-identity/spec.md
files:
  - src/py/vertebra/embeddings/_remote.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 120
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
  - id: py-remote
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: Remote gRPC embedder with static bearer token and RuntimeError failures
  - id: py-id-token-stub
    resource: src/py/vertebra/_vertebra.pyi::AsyncGcpIdTokenCache
    title: ID token cache stubs from ch02:t02
  - id: py-remote-test
    resource: src/py/tests/unit/embeddings/test_google_embedders.py::test_remote_embedder_and_grpc_service_roundtrip
    title: Existing remote roundtrip test through create_embedding_grpc_service
tags:
  - identity
  - embeddings
  - cloud-run
---

# Task t03: Authenticate `RemoteEmbedder` with Audience-Bound ID Tokens

## Objective & Scope Boundary

Extend `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` [^py-remote]:

- New keyword-only `id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None` and `audience: str | None = None` [^py-id-token-stub].
- `SyncGcpIdTokenCache` instances are stored as `.to_async()`.
- `audience` defaults to the endpoint origin (`scheme://host[:port]`, computed with `urllib.parse.urlsplit`) when `id_token_cache` is set.
- Each request awaits `get_token(audience)` and sends `authorization: Bearer <id_token>`; on HTTP `401`, it calls `invalidate(audience, snapshot.generation)` and retries once.
- Non-`200` responses, gRPC error statuses, and empty frame lists raise `EmbeddingError` (a `RuntimeError` subclass, so existing `RuntimeError` handlers still match).

Out of scope: default transport, wire format, server side.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_remote.py` (whole module).
- `src/py/vertebra/embeddings/_base.py::EmbeddingError`.
- `src/py/vertebra/_vertebra.pyi::SyncGcpIdTokenCache`, `::AsyncGcpIdTokenCache`, `::GcpIdTokenSnapshot`.
- `src/py/tests/unit/embeddings/test_google_embedders.py::test_remote_embedder_and_grpc_service_roundtrip` [^py-remote-test].
- Chapter spec.

## Symbol & Signature Contracts

```python
class RemoteEmbedder(Embedder):
    def __init__(self, endpoint: str = "http://127.0.0.1:8000", model: str = "gemini-embedding-001", *,
                 dimensions: int = 768, bearer_token: str | None = None,
                 id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None,
                 audience: str | None = None, cache: VectorCache | None = None,
                 transport: GrpcTransportCallable | None = None) -> None: ...
    @property
    def audience(self) -> str | None: ...
    async def _embed_batch_impl(self, texts: list[str], *, task: str = "document", titles: list[str | None] | None = None) -> VectorBatchView: ...
    async def _authorization(self, *, force_refresh: bool) -> tuple[str | None, int | None]: ...
    @staticmethod
    def _endpoint_origin(endpoint: str) -> str: ...
```

## Edge Cases & Error Semantics

- `bearer_token` and `id_token_cache` both set -> `ValueError("RemoteEmbedder accepts bearer_token or id_token_cache, not both")`.
- `audience` set without `id_token_cache` -> `ValueError("audience requires id_token_cache")`.
- Endpoint without scheme or host (for example `"localhost:8000"`) with `id_token_cache` and no explicit `audience` -> `ValueError("cannot derive an ID token audience from endpoint ...")`.
- Static bearer and unauthenticated modes never retry on `401`.
- `GoogleCloudAuthError` from `get_token` propagates unchanged.
- Responses decode as today; `decode_gemini_embed_grpc_frame_to_batch` row count is checked against `len(texts)` (`EmbeddingError` on mismatch).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py`. A `_FakeIdTokenCache` test double exposes async `get_token(audience, *, force_refresh=False)` returning `GcpIdTokenSnapshot(id_token=f"id-{generation}", audience=audience, expires_at_unix_ms=4_102_444_800_000, generation=generation)` and records `invalidate` calls.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_remote_embedder_id_token_default_audience` | `RemoteEmbedder("https://emb-abc.a.run.app/", dimensions=4, id_token_cache=fake, transport=t)`; `embed_query("q")` served by `create_embedding_grpc_service` handler | `fake` saw audience `https://emb-abc.a.run.app`; request header `authorization: Bearer id-1`; `remote.audience == "https://emb-abc.a.run.app"` |
| `test_remote_embedder_id_token_401_retry` | Transport returns `401` once, then a valid frame | `fake.invalidations == [("https://emb-abc.a.run.app", 1)]`; second request uses `Bearer id-2`; `2` transport calls |
| `test_remote_embedder_id_token_second_401_raises` | Transport always `401` | `EmbeddingError` matching `HTTP 401`; `2` transport calls |
| `test_remote_embedder_auth_option_validation` | `@pytest.mark.parametrize` over (`bearer_token="t"`, `id_token_cache=fake`), (`audience="https://x"` only), (`endpoint="localhost:8000"`, `id_token_cache=fake`) | `ValueError` matching `not both`, `requires id_token_cache`, `cannot derive` |
| `test_remote_embedder_explicit_audience` | `audience="https://custom-aud"` | `fake` saw `https://custom-aud` |
| `test_remote_embedder_and_grpc_service_roundtrip` (existing) | Unchanged static/unauthenticated flow | Still passes |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k remote_embedder
uv run ruff check src/py/vertebra/embeddings/_remote.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run mypy src/py/vertebra/embeddings
```

Commit as `feat(embeddings): authenticate RemoteEmbedder with GCP ID tokens`.

## Acceptance Criteria

- [ ] `RemoteEmbedder` sends audience-bound ID tokens from an injected cache with an endpoint-origin default audience (R11).
- [ ] One `401` retry after generation invalidation; conflicting options rejected (R11).
- [ ] Transport failures raise `EmbeddingError` (R9, R11).

## Notes & Discoveries

- None yet.

[^py-remote]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — Client extended here.
[^py-id-token-stub]: `src/py/vertebra/_vertebra.pyi::AsyncGcpIdTokenCache` — ID token cache surface.
[^py-remote-test]: `src/py/tests/unit/embeddings/test_google_embedders.py::test_remote_embedder_and_grpc_service_roundtrip` — Existing roundtrip test.
