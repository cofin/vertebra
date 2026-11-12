---
type: Task
id: "gcp-embeddings-and-identity:ch04:t01"
chapter_id: ch04-gemini-wire-and-multimodal
title: Encode Gemini content parts with correct field numbers and media support
description: Rewrite the Gemini request encoder so Part.text uses field 2, add GeminiContentPart and encode_batch_embed_contents_grpc_request for inline-data and file-data parts, and bind encode_gemini_embed_contents_grpc_frame with a stub.
status: draft
state: pending
priority: P0
verification_strategy: regression_tdd
depends_on:
  - "gcp-embeddings-and-identity:ch03:t03"
read_set:
  - src/rs/vertebra-core/src/vector.rs
  - src/rs/vertebra-core/src/proto.rs
  - src/rs/vertebra-py/src/vector.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/db/test_vector_wire_codecs.py
  - .agents/bundles/specs/gcp-embeddings-and-identity/chapters/ch04-gemini-wire-and-multimodal/spec.md
files:
  - src/rs/vertebra-core/src/vector.rs
  - src/rs/vertebra-py/src/vector.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/db/test_vector_wire_codecs.py
estimated_loc: 210
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
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: Request encoder writing Part.text on field 1
  - id: core-proto
    resource: src/rs/vertebra-core/src/proto.rs::put_bytes_field
    title: Protobuf varint and length-delimited helpers
  - id: py-vector-binding
    resource: src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame
    title: Existing PyO3 encoder binding and register_vector_bindings
  - id: gemini-content-proto
    resource: https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto
    title: Part, Blob, and FileData field numbers
tags:
  - protobuf
  - gemini
  - pyo3
---

# Task t01: Encode Gemini Content Parts with Correct Field Numbers and Media Support

## Objective & Scope Boundary

In `src/rs/vertebra-core/src/vector.rs`, add `GeminiContentPart` and `encode_batch_embed_contents_grpc_request`, and reimplement `encode_batch_embed_grpc_request` as a wrapper that builds one `Text` part per text and unwraps the result (text-only input cannot hit the error cases except empty `texts`, which keeps today's behavior of encoding an empty request list) [^core-vector-codec]. Bind the new encoder in `src/rs/vertebra-py/src/vector.rs` and declare it in `src/py/vertebra/_vertebra.pyi` [^py-vector-binding].

Out of scope: `GeminiEmbedder` changes (`t02`), response decoding.

## Bounded Read Manifest

- `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request`, `::encode_batch_embed_grpc_response_from_slab`, `tests` module.
- `src/rs/vertebra-core/src/proto.rs::put_bytes_field`, `::put_varint_field`, `::read_tag`, `::read_length_delimited` [^core-proto].
- `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame`, `::register_vector_bindings`.
- `src/py/vertebra/_vertebra.pyi::encode_gemini_embed_grpc_frame`, module `__all__`.
- `src/py/tests/unit/db/test_vector_wire_codecs.py::test_gemini_grpc_proto_frame_roundtrip_and_malformed_varint_rejection`.
- Chapter spec.

## Symbol & Signature Contracts

```rust
const PART_TEXT_FIELD: u32 = 2;
const PART_INLINE_DATA_FIELD: u32 = 3;
const PART_FILE_DATA_FIELD: u32 = 6;
const BLOB_MIME_TYPE_FIELD: u32 = 1;
const BLOB_DATA_FIELD: u32 = 2;
const FILE_DATA_MIME_TYPE_FIELD: u32 = 1;
const FILE_DATA_URI_FIELD: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
```

Encoding (field numbers from `content.proto` and `generative_service.proto` [^gemini-content-proto]): `BatchEmbedContentsRequest { model=1, requests=2 (repeated EmbedContentRequest) }`; `EmbedContentRequest { model=1, content=2, task_type=3 (varint, omitted when 0/None), title=4 (omitted when None/empty), output_dimensionality=5 (omitted when 0/None) }`; `Content { parts=1 (repeated Part) }`; `Part::Text -> field 2`, `Part::InlineData -> field 3 Blob{1: mime_type, 2: data}`, `Part::FileData -> field 6 FileData{1: mime_type, 2: file_uri}`; 5-byte LPM prefix `0x00` + big-endian `u32` length. Model normalization (`models/` prefix unless `models/` or `projects/` already present) stays unchanged.

```rust
// src/rs/vertebra-py/src/vector.rs
#[derive(FromPyObject)]
enum PyPartValue { Bytes(PyBackedBytes), Text(String) }

#[pyfunction]
#[pyo3(signature = (model, contents, task=None, titles=None, output_dimensionality=None))]
pub fn encode_gemini_embed_contents_grpc_frame<'py>(
    py: Python<'py>,
    model: &str,
    contents: Vec<Vec<(String, String, PyPartValue)>>,
    task: Option<&str>,
    titles: Option<Vec<Option<String>>>,
    output_dimensionality: Option<u32>,
) -> PyResult<Bound<'py, PyBytes>>;
```

The binding maps `("text", _, Text(s))`, `("inline_data", mime, Bytes(b))`, `("file_data", mime, Text(uri))`; any other combination raises `ValueError("unsupported Gemini content part (<kind>, <value type>)")`. Task strings resolve through `EmbeddingTask::parse(..).gemini_proto_task_type()`. Encoding runs inside `py.detach` (owned `PyBackedBytes` slices are `Send`). Encoder `Err` strings become `ValueError`.

## Edge Cases & Error Semantics

- `contents` empty -> `Err("contents must not be empty")`.
- A content with zero parts -> `Err("content <i> has no parts")`.
- `InlineData`/`FileData` with empty or whitespace `mime_type` -> `Err("content <i> part <j> requires a mime_type")`; `FileData` with empty `file_uri` -> `Err("content <i> part <j> requires a file_uri")`.
- `titles` length differing from `contents` length -> `Err("titles length must match contents length")`.
- Encoded body larger than `u32::MAX` -> `Err("Gemini request exceeds the gRPC 4 GiB frame limit")`.
- `Text("")` is allowed (the API decides).

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/db/test_vector_wire_codecs.py` (literal byte assertions) and the `tests` module in `src/rs/vertebra-core/src/vector.rs` (in-process).
- **Worked Test Matrix** (frame bytes after the 5-byte prefix unless noted):

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `gemini_text_part_uses_field_two` (Rust) / `test_gemini_text_frame_literal_bytes` (Python) | `encode_gemini_embed_grpc_frame("m", ["hi"])` | Full frame `00 00 00 00 1e` + `0a 08 6d 6f 64 65 6c 73 2f 6d` + `12 12` + `0a 08 6d 6f 64 65 6c 73 2f 6d 12 06 0a 04 12 02 68 69` |
| `test_gemini_inline_part_literal_bytes` | `encode_gemini_embed_contents_grpc_frame("m", [[("inline_data", "image/png", b"\x89P")]])` | Body contains `0a 11 1a 0f 0a 09 69 6d 61 67 65 2f 70 6e 67 12 02 89 50` (Content{Part{Blob}}) |
| `test_gemini_file_part_literal_bytes` | `[[("file_data", "video/mp4", "https://x/v")]]` | Body contains `32 18 0a 09 76 69 64 65 6f 2f 6d 70 34 12 0b 68 74 74 70 73 3a 2f 2f 78 2f 76` |
| `test_gemini_mixed_parts_task_title_dims` | `[[("inline_data", "image/png", b"\x89P"), ("text", "text/plain", "cap")]]`, `task="document"`, `titles=["T"]`, `output_dimensionality=256` | Request contains, in order, `1a 0f ...` then `12 03 63 61 70` inside Content; `18 02` (task RETRIEVAL_DOCUMENT); `22 01 54` (title); `28 80 02` (256) |
| `test_gemini_contents_validation` | `[]`; `[[]]`; `[[("inline_data", "", b"x")]]`; `[[("audio", "audio/wav", b"x")]]`; `titles=[None, None]` for one content | `ValueError` matching `must not be empty`, `has no parts`, `requires a mime_type`, `unsupported Gemini content part`, `titles length` |
| `test_gemini_grpc_proto_frame_roundtrip_and_malformed_varint_rejection` (existing) | Unchanged | Still passes |

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib vector::
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-http grpc
cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-core -p vertebra-py --all-targets -- -D warnings
make dev
uv run python -m mypy.stubtest vertebra._vertebra
uv run pytest src/py/tests/unit/db/test_vector_wire_codecs.py -q
```

Write the literal-byte tests first (Red: text part shows `0a 02 68 69`), implement, re-run. Commit as `fix(vector): encode Gemini Part.text on field 2 and add media parts`.

## Acceptance Criteria

- [ ] Text parts use protobuf field `2`; inline-data and file-data parts encode per `content.proto` (R5).
- [ ] `encode_gemini_embed_contents_grpc_frame` is bound, stubbed, and passes stubtest (R5).
- [ ] Validation errors surface as `ValueError` with the specified messages (R5).

## Notes & Discoveries

- [2026-11-12 00:35] The existing Python roundtrip test only checks frame length, which is why the field-number defect passed CI; the literal-byte tests close that gap.

[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Encoder corrected here.
[^core-proto]: `src/rs/vertebra-core/src/proto.rs::put_bytes_field` — Wire helpers.
[^py-vector-binding]: `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame` — Binding pattern.
[^gemini-content-proto]: `https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/content.proto` — Field numbers.
