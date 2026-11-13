---
type: Task
id: "gcp-embeddings-and-identity:ch02:t04"
flow_id: gcp-embeddings-and-identity
chapter_id: ch02-vertex-and-gemini-embedder-wire-parity
title: "Add Audience-Bound ID Token Authentication to RemoteEmbedder"
description: "Accept SyncGcpIdTokenCache or AsyncGcpIdTokenCache and optional audience on RemoteEmbedder with endpoint-origin defaults and single 401 invalidation retry."
status: stable
state: completed
commit: 484568a7
last_verified_commit: 484568a7
last_verified_at: "2026-11-13T05:04:45Z"
verification_evidence: "cargo clippy + test (vector:: 3, grpc 8) + stubtest + ruff + mypy (8 files) + pytest (135 passed)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch02:t03"
files:
  - src/py/vertebra/embeddings/_remote.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
read_set:
  - src/py/vertebra/embeddings/_remote.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 120
tags:
  - embeddings
  - identity
  - oidc
  - cloud-run
sources:
  - id: py-remote
    resource: src/py/vertebra/embeddings/_remote.py::RemoteEmbedder
    title: "RemoteEmbedder gRPC client with static bearer authentication"
---

# Task `t04`: Add Audience-Bound ID Token Authentication to `RemoteEmbedder`

## Objective & Scope Boundary

Extend `RemoteEmbedder` in `src/py/vertebra/embeddings/_remote.py` with `id_token_cache` and `audience` parameters for Cloud Run and IAP-fronted cluster embedding services per [`../spec.md` §2](../spec.md) [^py-remote]. Out of scope: changing `RemoteEmbedder`'s gRPC wire framing.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_remote.py` — `RemoteEmbedder.__init__` and `_embed_batch_impl` [^py-remote].
- `src/py/vertebra/_vertebra.pyi` — `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `GcpIdTokenSnapshot`.
- `src/py/tests/unit/embeddings/test_google_embedders.py` — `RemoteEmbedder` unit tests.

## Contracts & Edge Cases

- `RemoteEmbedder.__init__`: accept `id_token_cache: SyncGcpIdTokenCache | AsyncGcpIdTokenCache | None = None` and `audience: str | None = None`; raise `ValueError` if both `bearer_token` and `id_token_cache` are provided; normalize `SyncGcpIdTokenCache` via `.to_async()`; derive default `audience` as `{parsed.scheme}://{parsed.netloc}` from `endpoint` (raising `ValueError` when scheme or host is missing).
- Request execution: await `id_token_cache.get_token(audience)`, attach `authorization: Bearer <snapshot.id_token>`, and on HTTP `401` call `invalidate(audience, snapshot.generation)`, fetch `get_token(audience, force_refresh=True)`, and retry at most once before raising `EmbeddingError`.

## Test Matrix

- Parametrized tests in `src/py/tests/unit/embeddings/test_google_embedders.py`: default endpoint-origin audience (`"https://emb-svc.a.run.app/v1/"` -> `"https://emb-svc.a.run.app"`); explicit `audience` override; `SyncGcpIdTokenCache` `.to_async()` conversion; mutual-exclusion `ValueError` when both `bearer_token` and `id_token_cache` are set; single `401` invalidation + retry success vs double `401` `EmbeddingError`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/embeddings/_remote.py src/py/tests/unit/embeddings/test_google_embedders.py && uv run --no-sync mypy src/py/vertebra/embeddings/_remote.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py -k remote -q
```

## Acceptance Criteria

- [x] `RemoteEmbedder` authenticates outbound gRPC requests with audience-bound ID tokens and retries once on `401` (`R11`).
- [x] Zero regressions in the chapter's fast verification gate.

[^py-remote]: `src/py/vertebra/embeddings/_remote.py::RemoteEmbedder` — `RemoteEmbedder` gRPC client with static bearer authentication.
