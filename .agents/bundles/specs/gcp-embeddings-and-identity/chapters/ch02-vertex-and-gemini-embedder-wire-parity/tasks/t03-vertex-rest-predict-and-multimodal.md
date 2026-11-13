---
type: Task
id: "gcp-embeddings-and-identity:ch02:t03"
flow_id: gcp-embeddings-and-identity
chapter_id: ch02-vertex-and-gemini-embedder-wire-parity
title: "Migrate VertexEmbedder to Vertex AI REST predict and embedContent with Multimodal"
description: "Extend ModelSpec with vertex_method and max_instances_per_request, switch VertexEmbedder onto Vertex AI v1 REST JSON predict and embedContent, and implement Vertex multimodal embedding."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch02:t02"
files:
  - src/py/vertebra/embeddings/_models.py
  - src/py/vertebra/embeddings/_google.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
read_set:
  - src/py/vertebra/embeddings/_models.py
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/utils/serializers/__init__.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 245
tags:
  - vertex-ai
  - embeddings
  - rest
  - multimodal
sources:
  - id: py-models
    resource: src/py/vertebra/embeddings/_models.py::ModelSpec
    title: "MODEL_CATALOG entries for gemini-embedding-001, gemini-embedding-2, and text-embedding-005"
  - id: py-vertex
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder
    title: "VertexEmbedder transport and configuration"
---

# Task `t03`: Migrate `VertexEmbedder` to Vertex AI REST `predict` and `embedContent` with Multimodal

## Objective & Scope Boundary

Extend `ModelSpec` in `src/py/vertebra/embeddings/_models.py` and replace `VertexEmbedder`'s `BatchEmbedContents` gRPC call with Vertex AI v1 REST JSON `:predict` and `:embedContent` (both text and multimodal) per [`../spec.md` §2](../spec.md) [^py-models] [^py-vertex]. Out of scope: `RemoteEmbedder` (`t04`).

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_models.py` — `ModelSpec` and `MODEL_CATALOG` [^py-models].
- `src/py/vertebra/embeddings/_google.py` — `_GoogleEmbedder` from `t02` and `VertexEmbedder` [^py-vertex].
- `src/py/vertebra/utils/serializers/__init__.py` — centralized `encode_json_bytes` and `decode_json`.
- `src/py/tests/unit/embeddings/test_google_embedders.py` — Vertex transport tests.

## Contracts & Edge Cases

- `ModelSpec`: add `vertex_method: Literal["predict", "embed_content"] = "predict"` and `max_instances_per_request: int = 1` (`gemini-embedding-001`: `predict`/`1`; `text-embedding-005` & `text-multilingual-embedding-002`: `predict`/`250`; `gemini-embedding-2`: `embed_content`/`1`).
- `VertexEmbedder`: add `max_concurrent_requests: int = 8` and `auto_truncate: bool = False`; resolve `project` from arg -> `GOOGLE_CLOUD_PROJECT` -> `GCP_PROJECT` (raising `ImproperConfigurationError` on first request when missing, keeping `VertexEmbedder()` constructible); use `https://aiplatform.googleapis.com` when `location == "global"` and `https://{location}-aiplatform.googleapis.com` otherwise; route via `encode_json_bytes` / `decode_json` (never raw `msgspec.json`).
- Validate `titles` (non-`None` title outside `document`/`search_document` raises `ValueError` pre-network), chunk `:predict` requests by `max_instances_per_request` and `20,000` estimated tokens concurrently via `asyncio.TaskGroup` + `Semaphore(max_concurrent_requests)` preserving input order, verify every returned vector has length `self._dimensions` (`EmbeddingError` on drift), and implement `embed_multimodal` on `:embedContent` models (`inlineData: {"mimeType", "data": b64}`, `fileData: {"mimeType", "fileUri"}` including `gs://` URIs; `ValueError` naming `gemini-embedding-2` on `:predict`-only models).

## Test Matrix

- Parametrized tests in `src/py/tests/unit/embeddings/test_google_embedders.py`: `gemini-embedding-001` `3`-text `:predict` fan-out (`1` instance/request, ordered output); `text-embedding-005` `300`-text (`250`+`50`) and `20,000`-token chunking; `gemini-embedding-2` `:embedContent` JSON schema + `autoTruncate: false`; `location="global"` URL; missing `project` `ImproperConfigurationError`; `task="query"` title `ValueError`; dimension-drift and sibling-cancelling `500` `EmbeddingError`; `VertexEmbedder.embed_multimodal` with inline bytes and `gs://` URIs vs `ValueError` on `gemini-embedding-001`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/embeddings src/py/tests/unit/embeddings/test_google_embedders.py && uv run --no-sync mypy src/py/vertebra/embeddings && uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py -k vertex -q
```

## Acceptance Criteria

- [ ] `VertexEmbedder` speaks real Vertex AI v1 `:predict` and `:embedContent` JSON for text and multimodal inputs with bounded concurrency and strict dimension/title validation (`R7`, `R8`).
- [ ] Zero regressions in `test_google_embedders.py`.

[^py-models]: `src/py/vertebra/embeddings/_models.py::ModelSpec` — `MODEL_CATALOG` entries for `gemini-embedding-001`, `gemini-embedding-2`, and `text-embedding-005`.
[^py-vertex]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder` — `VertexEmbedder` transport and configuration.
