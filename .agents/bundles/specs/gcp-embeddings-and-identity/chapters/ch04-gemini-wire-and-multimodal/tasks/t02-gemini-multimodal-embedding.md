---
type: Task
id: "gcp-embeddings-and-identity:ch04:t02"
chapter_id: ch04-gemini-wire-and-multimodal
title: Send real media parts from GeminiEmbedder.embed_multimodal
description: Encode GeminiEmbedder text and multimodal batches through encode_gemini_embed_contents_grpc_frame, map each MultimodalPart or string to one Content, validate part combinations and URIs, and return exactly one row per item.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch04:t01"
read_set:
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_base.py
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/embeddings/test_google_embedders.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch04-gemini-wire-and-multimodal/spec.md
files:
  - src/py/vertebra/embeddings/_google.py
tests:
  - src/py/tests/unit/embeddings/test_google_embedders.py
estimated_loc: 170
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
  - id: py-gemini
    resource: src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal
    title: Base64 text-descriptor path being replaced
  - id: py-multimodal-part
    resource: src/py/vertebra/embeddings/_base.py::MultimodalPart
    title: mime_type, data, uri, text fields
  - id: py-gemini-tests
    resource: src/py/tests/unit/embeddings/test_google_embedders.py::test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default
    title: Existing test that asserts base64 text inside the request body
tags:
  - gemini
  - multimodal
  - embeddings
---

# Task t02: Send Real Media Parts from `GeminiEmbedder.embed_multimodal`

## Objective & Scope Boundary

In `src/py/vertebra/embeddings/_google.py`:

- `GeminiEmbedder._embed_batch_impl` encodes text batches with `encode_gemini_embed_contents_grpc_frame(self.resource_model_name, [[("text", "text/plain", t)] for t in texts], task=task, titles=titles, output_dimensionality=self._dimensions)`.
- `GeminiEmbedder.embed_multimodal(items, *, task="document")` builds one content per item through `_GoogleEmbedder._content_parts(item, allow_gcs_uris=False)`, splits into chunks of at most `self._max_batch_size` contents, posts each chunk through `_GoogleEmbedder._post_authorized`, checks `grpc-status`, decodes with `decode_gemini_embed_grpc_frame_to_batch(..., dimensions=self._dimensions, normalize=True)`, validates rows per chunk with `_require_rows`, and returns one `VectorBatchView` in input order (`VectorBatchView.from_f32_rows` when more than one chunk).
- Delete the `base64` import and the descriptor-string code [^py-gemini].
- `_GoogleEmbedder.embed_multimodal` is declared on the base and raises `NotImplementedError` naming the class; `ch05:t02` implements it for Vertex.

Out of scope: VectorCache for multimodal inputs (not cached), Vertex behavior.

## Bounded Read Manifest

- `src/py/vertebra/embeddings/_google.py::GeminiEmbedder` (after `ch03`), `::_GoogleEmbedder._post_authorized`, `::_GoogleEmbedder._require_rows`.
- `src/py/vertebra/embeddings/_base.py::MultimodalPart`, `::Embedder._record_usage` [^py-multimodal-part].
- `src/py/vertebra/_vertebra.pyi::encode_gemini_embed_contents_grpc_frame`, `::decode_gemini_embed_grpc_frame_to_batch`.
- `src/py/tests/unit/embeddings/test_google_embedders.py::test_gemini_embedder_api_key_multimodal_and_embed_arrow`, `::test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default` [^py-gemini-tests].
- Chapter spec.

## Symbol & Signature Contracts

```python
GeminiPartTuple = tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]

class _GoogleEmbedder(Embedder):
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...

    @staticmethod
    def _content_parts(item: MultimodalPart | str, *, allow_gcs_uris: bool) -> list[GeminiPartTuple]: ...

class GeminiEmbedder(_GoogleEmbedder):
    async def embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...
```

`_GoogleEmbedder._content_parts` (shared with `VertexEmbedder` in `ch05:t02`; `GeminiEmbedder` passes `allow_gcs_uris=False`):
- `str` -> `[("text", "text/plain", item)]`.
- `MultimodalPart` with `data` -> `[("inline_data", item.mime_type, item.data)]` plus `("text", "text/plain", item.text)` when `item.text`.
- With `uri` -> `[("file_data", item.mime_type, item.uri)]` plus the text part when `item.text`.
- With only `text` -> `[("text", "text/plain", item.text)]`.

## Edge Cases & Error Semantics

- `items` empty -> `ValueError("items must not be empty")`.
- `data` and `uri` both set -> `ValueError("MultimodalPart accepts either data or uri, not both")`.
- No `data`, `uri`, or `text` -> `ValueError("MultimodalPart requires data, uri, or text")`.
- With `allow_gcs_uris=False`, a `gs://` URI raises `ValueError("GeminiEmbedder cannot read gs:// URIs; upload through the Gemini Files API or use VertexEmbedder")` and any other non-`https://` URI raises `ValueError("file URIs must use https://")`.
- With `allow_gcs_uris=True` (Vertex), URIs must start with `https://` or `gs://`, otherwise `ValueError("file URIs must use https:// or gs://")`.
- Media with `mime_type == "text/plain"` and `data` is still sent as `inline_data` (the API validates support).
- Usage telemetry counts only text parts (`_record_usage` with the text segments).
- API errors (unsupported mime type, oversized payload) surface as `EmbeddingError` from `_post_authorized` or the gRPC status check.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/embeddings/test_google_embedders.py`. Transports record request bodies; tests decode the recorded frame with a small module-level `_walk_proto(body: bytes) -> list[tuple[int, int, bytes | int]]` helper (varint + length-delimited only) to assert field numbers instead of substring checks.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `test_gemini_multimodal_sends_inline_blob_and_text` | `GeminiEmbedder("gemini-embedding-2", dimensions=4, api_key="k")`; `[MultimodalPart(mime_type="image/png", data=b"\x89PNG\r\n", text="diagram"), "plain text"]`; transport returns a `2`-row frame | Request 1 content has parts `[field 3 Blob{1: b"image/png", 2: b"\x89PNG\r\n"}, field 2 b"diagram"]`; content 2 has `[field 2 b"plain text"]`; result `count == 2`; body does not contain `base64.b64encode(b"\x89PNG\r\n")` |
| `test_gemini_multimodal_file_uri_part` | `MultimodalPart(mime_type="video/mp4", uri="https://generativelanguage.googleapis.com/v1beta/files/abc")` | Part field `6` with `FileData{1: b"video/mp4", 2: <uri bytes>}` |
| `test_gemini_multimodal_validation` | `@pytest.mark.parametrize` over `[]`, `MultimodalPart(data=b"x", uri="https://u")`, `MultimodalPart()`, `MultimodalPart(uri="gs://b/o.png", mime_type="image/png")`, `MultimodalPart(uri="http://u", mime_type="image/png")` | `ValueError` matching `must not be empty`, `either data or uri`, `requires data, uri, or text`, `gs://`, `https://`; zero transport calls |
| `test_gemini_multimodal_chunks_preserve_order` | `max_batch_size=2`, `5` string items; transport returns rows `[i, 0, 0, 0]` per request position | `3` transport calls; output row `k` equals normalized `[k, 0, 0, 0]` order |
| `test_gemini_multimodal_row_count_mismatch` | Transport returns `1` row for `2` items | `EmbeddingError` containing `returned 1 embeddings for 2 inputs` |
| `test_gemini_text_batch_uses_text_field_two` | `embed_many(["hi"])` | Recorded part field number `2` |
| `test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default` (existing) | Rewritten to assert the raw media bytes appear inside the `Blob.data` field and the native default transport is still used | Passes |

## Verification

```bash
uv run pytest src/py/tests/unit/embeddings/test_google_embedders.py -q -k gemini
uv run ruff check src/py/vertebra/embeddings/_google.py src/py/tests/unit/embeddings/test_google_embedders.py
uv run mypy src/py/vertebra/embeddings
```

Write the tests first (Red: descriptor text appears, field `3` absent), implement, re-run. Commit as `fix(embeddings): send real media parts in Gemini multimodal embeddings`.

## Acceptance Criteria

- [ ] `embed_multimodal` sends inline-data and file-data parts and never base64-encodes media into text (R6).
- [ ] Part combinations and URIs are validated before any request (R6).
- [ ] Output has exactly one row per item in input order across chunks (R6).

## Notes & Discoveries

- None yet.

[^py-gemini]: `src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal` — Path replaced here.
[^py-multimodal-part]: `src/py/vertebra/embeddings/_base.py::MultimodalPart` — Input type.
[^py-gemini-tests]: `src/py/tests/unit/embeddings/test_google_embedders.py::test_gemini_embedder_multimodal_preserves_full_inline_data_and_rust_http2_default` — Existing test rewritten here.
