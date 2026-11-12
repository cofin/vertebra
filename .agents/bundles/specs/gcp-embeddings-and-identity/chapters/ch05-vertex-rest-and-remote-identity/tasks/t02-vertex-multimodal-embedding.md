---
type: Task
id: "gcp-embeddings-and-identity:ch05:t02"
chapter_id: ch05-vertex-rest-and-remote-identity
title: Embed multimodal content through Vertex AI embedContent
description: Implement VertexEmbedder.embed_multimodal for embed_content models with inlineData and fileData parts (gs:// and https:// URIs), one request per item under bounded concurrency, and one row per item.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch05:t01"
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch05-vertex-rest-and-remote-identity/spec.md
files:
  - src/py/vertebra/embeddings/_google.py
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
  - id: py-vertex
    resource: src/py/vertebra/embeddings/_google.py::VertexEmbedder._post_vertex
    title: Vertex REST posting helper from ch05:t01
  - id: py-content-parts
    resource: src/py/vertebra/embeddings/_google.py::_GoogleEmbedder._content_parts
    title: Shared multimodal part validation from ch04:t02
  - id: vertex-embed-reference
    resource: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent
    title: Content parts with inlineData (mimeType, data) and fileData (mimeType, fileUri)
tags:
  - vertex-ai
  - multimodal
  - embeddings
---

# Task t02: Embed Multimodal Content Through Vertex AI `:embedContent`

## Objective & Scope Boundary

Implement `VertexEmbedder.embed_multimodal(items, *, task="document")` in `src/py/vertebra/embeddings/_google.py` [^py-vertex]. Each item maps through `_GoogleEmbedder._content_parts(item, allow_gcs_uris=True)` [^py-content-parts] to `_VertexPart` values (`text`, `inlineData` with standard base64 `data`, `fileData` with `fileUri`) inside one `_VertexEmbedContentRequest` posted to `:embedContent` [^vertex-embed-reference]. Requests run under the same `TaskGroup` + semaphore fan-out as `t01`; rows return in input order and are normalized.

Out of scope: VectorCache for multimodal inputs, Gemini behavior.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py::VertexEmbedder` (`_post_vertex`, `_vertex_task_type`, `_checked_rows`, request structs), `::_GoogleEmbedder._content_parts`.
- `src/py/vertebra/embeddings/_base.py::MultimodalPart`.
- `src/py/tests/unit/embeddings/test_google_embedders.py` (Vertex tests from `t01`).
- Chapter spec.

## Symbol & Signature Contracts

```python
class VertexEmbedder(_GoogleEmbedder):
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...
    @staticmethod
    def _vertex_parts(parts: list[GeminiPartTuple]) -> list[_VertexPart]: ...
```

`_vertex_parts` maps `("text", _, s)` -> `_VertexPart(text=s)`, `("inline_data", mime, b)` -> `_VertexPart(inline_data=_VertexBlob(mime_type=mime, data=base64.b64encode(b).decode("ascii")))`, `("file_data", mime, uri)` -> `_VertexPart(file_data=_VertexFileData(mime_type=mime, file_uri=uri))`. JSON REST requires base64 for bytes; this is the wire encoding of the `bytes` field, not a text descriptor.

## Edge Cases & Error Semantics

- `self.vertex_method != "embed_content"` -> `ValueError("VertexEmbedder multimodal embedding requires a model served through :embedContent, such as gemini-embedding-2")`; zero requests.
- Empty `items`, conflicting or empty parts, and URIs that are neither `https://` nor `gs://` raise the `_content_parts` `ValueError`s.
- Titles are not sent for multimodal requests.
- Dimension, row-count, HTTP, and project errors follow `t01` semantics.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_vertex_multimodal_inline_and_gcs_parts` | `VertexEmbedder("gemini-embedding-2", project="p", dimensions=4)`; `[MultimodalPart(mime_type="image/png", data=b"\x89P", text="cap"), MultimodalPart(mime_type="video/mp4", uri="gs://b/v.mp4")]`; transport answers `{"embedding":{"values":[1,0,0,0]}}` | `2` `:embedContent` calls; first body parts `[{"inlineData":{"mimeType":"image/png","data":"iVA="}},{"text":"cap"}]`; second `[{"fileData":{"mimeType":"video/mp4","fileUri":"gs://b/v.mp4"}}]`; `taskType` `RETRIEVAL_DOCUMENT`; result `count == 2` |
| `test_vertex_multimodal_rejects_predict_models` | `gemini-embedding-001`; `embed_multimodal(["x"])` | `ValueError` matching `gemini-embedding-2`; zero transport calls |
| `test_vertex_multimodal_uri_validation` | `MultimodalPart(uri="ftp://h/f", mime_type="image/png")` | `ValueError` matching `https:// or gs://` |
| `test_vertex_multimodal_preserves_order_under_concurrency` | `4` string items, `max_concurrent_requests=2`; transport delays responses in reverse order and returns `[k,0,0,0]` keyed by request text | Output row `k` corresponds to item `k` |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k vertex_multimodal
uv run ruff check src/py/vertebra/embeddings/_google.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run mypy src/py/vertebra/embeddings
```

Commit as `feat(embeddings): add Vertex AI multimodal embeddings`.

## Acceptance Criteria

- [ ] `VertexEmbedder.embed_multimodal` sends `inlineData` and `fileData` parts to `:embedContent`, including `gs://` URIs (R8).
- [ ] Non-`embed_content` models are rejected before any request (R8).
- [ ] One normalized row per item in input order (R8).

## Notes & Discoveries

- None yet.

[^py-vertex]: `src/py/vertebra/embeddings/_google.py::VertexEmbedder._post_vertex` — Posting helper reused here.
[^py-content-parts]: `src/py/vertebra/embeddings/_google.py::_GoogleEmbedder._content_parts` — Shared part validation.
[^vertex-embed-reference]: `https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/embedContent` — Part schema.
