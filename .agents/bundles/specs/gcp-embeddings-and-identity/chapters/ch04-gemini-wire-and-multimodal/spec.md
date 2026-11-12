---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch04-gemini-wire-and-multimodal
title: "Chapter 04: Gemini Wire and Multimodal"
description: Fix the Gemini Part.text field number, add inline-data and file-data part encoding, and send real media from GeminiEmbedder.embed_multimodal.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch02-id-token-python-surface
  - ch03-google-embedder-runtime
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
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: Gemini BatchEmbedContents request encoder writing Part.text on field 1
  - id: py-vector-binding
    resource: src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame
    title: PyO3 binding for the request encoder
  - id: py-gemini
    resource: src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal
    title: Text-descriptor multimodal path at HEAD
  - id: gemini-content-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto
    title: Part (text=2, inline_data=3, file_data=6), Blob (mime_type=1, data=2), FileData (mime_type=1, file_uri=2)
tags:
  - gemini
  - protobuf
  - multimodal
  - embeddings
---

# Chapter 04: Gemini Wire and Multimodal

<!-- truth: start -->
- **Scope**: `ch04:t01` rewrites the Gemini request encoder in `src/rs/vertebra-core/src/vector.rs` so `Part.text` uses field `2`, adds `GeminiContentPart` and `encode_batch_embed_contents_grpc_request` for inline-data (`3`) and file-data (`6`) parts, and binds `encode_gemini_embed_contents_grpc_frame` in `vertebra-py` [^core-vector-codec] [^py-vector-binding] [^gemini-content-proto]; `ch04:t02` makes `GeminiEmbedder` encode text and multimodal requests through the new binding and return one row per item [^py-gemini].
- **Invariants**: encoders are pure and allocate once per request; media bytes are written raw into `Blob.data`; `EmbedContentRequest` fields stay `model=1`, `content=2`, `task_type=3`, `title=4`, `output_dimensionality=5`; Gemini never receives `gs://` URIs.
- **Verification Gate**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib vector:: && make dev && uv run pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/embeddings/test_google_embedders.py -q`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`encode_batch_embed_grpc_request` writes `put_bytes_field(&mut part_buf, 1, text)`. In `google.ai.generativelanguage.v1beta.Part`, field `1` is unused and `text` is field `2`, so the Gemini API sees parts with no data [^gemini-content-proto]. `GeminiEmbedder.embed_multimodal` then works around missing media support by base64-encoding bytes into a text string, which embeds the characters of the base64 text rather than the image, audio, video, or PDF [^py-gemini].

### 1.2 Non-Goals

- Vertex AI multimodal (`ch05:t02`).
- Changing `BatchEmbedContentsResponse` decoding; `decode_batch_embed_grpc_response_into_slab` already reads `embeddings=1` / `values=1` correctly.
- Client-side request size limits; the Gemini API enforces its own limits and rejections surface as `EmbeddingError`.

### 1.3 Upstream Baseline & Code Analysis

- `vertebra-core::proto` provides `put_bytes_field`, `put_varint_field`, `read_tag`, `read_length_delimited`, and `skip_field`.
- `vertebra-http/src/grpc.rs` test code and `RemoteEmbedder` servers call `encode_batch_embed_grpc_request` with unchanged arguments; only the emitted `Part` tag changes.
- `_vertebra.pyi` declares `encode_gemini_embed_grpc_frame`; `ch02:t02` already edited the stub, so `ch04:t01` appends after it.

Applicable guidance: [pyo3-bridge SKILL.md](../../../../../skills/pyo3-bridge/SKILL.md), [performance-reviewer expectations in review-performance SKILL.md](../../../../../skills/review-performance/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

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
pub fn encode_batch_embed_grpc_request(/* unchanged signature */) -> Vec<u8>; // delegates with one Text part per text
```

```python
def encode_gemini_embed_contents_grpc_frame(
    model: str,
    contents: Sequence[Sequence[tuple[Literal["text", "inline_data", "file_data"], str, str | bytes]]],
    task: str | None = None,
    titles: Sequence[str | None] | None = None,
    output_dimensionality: int | None = None,
) -> bytes: ...
async def GeminiEmbedder.embed_multimodal(self, items: Sequence[MultimodalPart | str], *, task: str = "document") -> VectorBatchView: ...
```

### Caller -> Seam -> Collaborator Map

- `GeminiEmbedder._embed_batch_impl` / `embed_multimodal` -> `GeminiEmbedder._content_parts(item)` -> `encode_gemini_embed_contents_grpc_frame` -> `encode_batch_embed_contents_grpc_request` -> `_GoogleEmbedder._post_authorized` -> `decode_gemini_embed_grpc_frame_to_batch` -> `_GoogleEmbedder._require_rows`.

### Edge-Case & Invariant Matrix

| Scenario | Input | Expected | Owner |
| --- | --- | --- | --- |
| Text part tag | `"hi"` | Part bytes `12 02 68 69` | `t01` |
| Inline part | `image/png`, `b"\x89P"` | Part bytes `1a 0f 0a 09 "image/png" 12 02 89 50` | `t01` |
| File part | `video/mp4`, `https://x/v` | Part tag `0x32` with `FileData{1: mime, 2: uri}` | `t01` |
| Empty contents | `[]` | `Err("contents must not be empty")` / `ValueError` | `t01` |
| Empty content | `[[]]` | `Err("content 0 has no parts")` / `ValueError` | `t01` |
| Empty media mime | `("inline_data", "", b"x")` | `ValueError` | `t01` |
| Unknown kind | `("audio", "audio/wav", b"x")` | `ValueError` | `t01` |
| Data and uri | `MultimodalPart(data=b"x", uri="https://u")` | `ValueError` | `t02` |
| Empty part | `MultimodalPart()` | `ValueError` | `t02` |
| `gs://` on Gemini | `MultimodalPart(uri="gs://b/o")` | `ValueError` naming `VertexEmbedder` | `t02` |
| Rows per item | `[part, "caption"]` | `count == 2` | `t02` |

### Alternatives Comparison

- Passing `MultimodalPart` objects across FFI would make the Rust binding import a Python class; plain `(kind, mime_type, value)` tuples keep the binding independent of `vertebra.embeddings`.
- Keeping the descriptor-string path for media and adding a separate media method would leave two semantics under one name; `embed_multimodal` always sends real parts.

## 3. Documentation & OKF Knowledge Plan

- Multimodal usage lands in `ch06:t01` (`docs/usage/embeddings-and-vectors.md`); encoder and binding facts land in `ch06:t02` (OKF).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Canonical Test |
| --- | --- | --- | --- |
| R5 | `t01` | `regression_tdd` | `src/py/tests/unit/db/test_vector_wire_codecs.py::test_gemini_*`; `vector.rs` `tests::gemini_*` |
| R6 | `t02` | `regression_tdd` | `src/py/tests/unit/embeddings/test_google_embedders.py::test_gemini_multimodal_*` |

## 5. Implementation Plan

- [ ] t01 — Gemini content part encoder and binding (tasks/t01-gemini-content-part-encoder.md)
- [ ] t02 — Real media in `GeminiEmbedder.embed_multimodal` (tasks/t02-gemini-multimodal-embedding.md)
- **Chapter Recovery Checkpoint**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib vector:: && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http grpc && make dev && uv run python -m mypy.stubtest vertebra._vertebra && uv run pytest src/py/tests/unit/db/test_vector_wire_codecs.py src/py/tests/unit/embeddings -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/2` tasks completed; depends on `ch02` (stub ordering) and `ch03` (`_GoogleEmbedder`).

[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Encoder being corrected.
[^py-vector-binding]: `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame` — Existing binding.
[^py-gemini]: `src/py/vertebra/embeddings/_google.py::GeminiEmbedder.embed_multimodal` — Text-descriptor path being replaced.
[^gemini-content-proto]: `https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto` — Field numbers.
