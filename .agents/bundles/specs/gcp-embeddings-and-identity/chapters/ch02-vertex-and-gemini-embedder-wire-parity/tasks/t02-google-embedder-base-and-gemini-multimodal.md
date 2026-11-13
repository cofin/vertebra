---
type: Task
id: "gcp-embeddings-and-identity:ch02:t02"
flow_id: gcp-embeddings-and-identity
chapter_id: ch02-vertex-and-gemini-embedder-wire-parity
title: "Extract _GoogleEmbedder Base and Implement Real Gemini Multimodal Embedding"
description: "Introduce _GoogleEmbedder with async token-cache normalization, 401 retry, EmbeddingError mapping, and per-loop micro-batching, and send real inline_data/file_data parts from GeminiEmbedder.embed_multimodal."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch02:t01"
files:
  - src/py/vertebra/embeddings/_google.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 245
tags:
  - gemini
  - embeddings
  - multimodal
  - asyncio
sources:
  - id: py-google
    resource: src/py/vertebra/embeddings/_google.py::GeminiEmbedder
    title: "Current VertexEmbedder/GeminiEmbedder hierarchy, token handling, and descriptor multimodal path"
  - id: py-base
    resource: src/py/vertebra/embeddings/_base.py::Embedder
    title: "Embedder facade, MultimodalPart, EmbeddingError, and sync portal loop"
---

# Task `t02`: Extract `_GoogleEmbedder` Base and Implement Real Gemini Multimodal Embedding

## Objective & Scope Boundary

Refactor `src/py/vertebra/embeddings/_google.py` so `GeminiEmbedder` and `VertexEmbedder` inherit from `_GoogleEmbedder` (shared ADC token normalization, `401` retry, `EmbeddingError` mapping, row-count check, and per-event-loop micro-batch queues) and rewrite `GeminiEmbedder` text and `embed_multimodal` over `encode_gemini_embed_contents_grpc_frame` per [`../spec.md` §2](../spec.md) [^py-google] [^py-base]. Out of scope: Vertex REST `:predict` / `:embedContent` payload migration (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py` — token resolution, micro-batching, and `GeminiEmbedder` [^py-google].
- `src/py/vertebra/embeddings/_base.py` — `Embedder`, `MultimodalPart`, and `EmbeddingError` [^py-base].
- `src/py/vertebra/_vertebra.pyi` — `encode_gemini_embed_contents_grpc_frame` and `AsyncGcpTokenCache`.
- `src/py/tests/unit/embeddings/test_google_embedders.py` — injected transport and `_FakeTokenCache` fixtures.

## Contracts & Edge Cases

- `_GoogleEmbedder`: convert `SyncGcpTokenCache` via `.to_async()` at init; build default `AsyncGcpTokenCache` using core ADC order (no `use_metadata_server` override or `vertebra.storage._store` fallback, raising `ImproperConfigurationError` on missing credentials at request time); retry once on HTTP `401` after `invalidate(generation)`; map non-`200` HTTP, non-`"0"` `grpc-status`, and `_require_rows` row-count mismatches to `EmbeddingError`; key `_MicroBatchQueue` by `asyncio.get_running_loop()` under a `threading.Lock`, cleaning drained loop entries and propagating `CancelledError`.
- `GeminiEmbedder.embed_multimodal`: encode each item (`str` or `MultimodalPart`) into real `("text", "", text)`, `("inline_data", mime_type, bytes)`, and `("file_data", mime_type, uri)` parts (optional caption appended as a trailing `text` part inside the same `Content`); reject `MultimodalPart` with both `data` and `uri`, with neither `data`/`uri`/`text`, or with `gs://` `uri` (`ValueError` naming `VertexEmbedder`); return `1` row per input item.

## Test Matrix

- Parametrized tests in `src/py/tests/unit/embeddings/test_google_embedders.py`: sync token-cache `.to_async()` conversion; single `401` retry vs double `401` `EmbeddingError`; gRPC status error decoding; row-count drift `EmbeddingError`; concurrent `embed_query_sync` (portal loop) + `embed_query` (main loop) isolation; `GeminiEmbedder.embed_multimodal` sending raw `Blob` bytes + caption on field `2` without base64-in-text descriptors; `ValueError` on `gs://`, dual `data`+`uri`, and empty `MultimodalPart`.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/embeddings/_google.py src/py/tests/unit/embeddings/test_google_embedders.py && uv run --no-sync mypy src/py/vertebra/embeddings/_google.py && uv run --no-sync pytest src/py/tests/unit/embeddings/test_google_embedders.py -q
```

## Acceptance Criteria

- [ ] `_GoogleEmbedder` enforces async token refreshes, per-loop micro-batching, and `EmbeddingError` invariants, and `GeminiEmbedder.embed_multimodal` sends real protobuf media parts (`R6`, `R9`).
- [ ] Zero regressions in `test_google_embedders.py`.

[^py-google]: `src/py/vertebra/embeddings/_google.py::GeminiEmbedder` — Current `VertexEmbedder`/`GeminiEmbedder` hierarchy, token handling, and descriptor multimodal path.
[^py-base]: `src/py/vertebra/embeddings/_base.py::Embedder` — `Embedder` facade, `MultimodalPart`, `EmbeddingError`, and sync portal loop.
