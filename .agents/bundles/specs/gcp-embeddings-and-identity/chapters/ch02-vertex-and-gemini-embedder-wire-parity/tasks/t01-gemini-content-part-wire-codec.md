---
type: Task
id: "gcp-embeddings-and-identity:ch02:t01"
flow_id: gcp-embeddings-and-identity
chapter_id: ch02-vertex-and-gemini-embedder-wire-parity
title: "Fix Gemini Part.text Field Number and Add Multimodal Part Wire Codec"
description: "Encode Gemini Part.text on protobuf field 2, add GeminiContentPart inline_data and file_data encoding in vertebra-core, and bind encode_gemini_embed_contents_grpc_frame in vertebra-py and _vertebra.pyi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch01:t04"
files:
  - src/rs/vertebra-core/src/vector.rs
  - src/rs/vertebra-py/src/vector.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/rs/vertebra-core/src/vector.rs
  - src/py/tests/unit/db/test_vector_wire_codecs.py
read_set:
  - src/rs/vertebra-core/src/vector.rs
  - src/rs/vertebra-core/src/proto.rs
  - src/rs/vertebra-py/src/vector.rs
  - src/py/vertebra/_vertebra.pyi
  - src/py/tests/unit/db/test_vector_wire_codecs.py
estimated_loc: 210
tags:
  - gemini
  - protobuf
  - pyo3
  - embeddings
sources:
  - id: core-vector-codec
    resource: src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request
    title: "Existing BatchEmbedContents protobuf request encoder"
  - id: py-vector-binding
    resource: src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame
    title: "Existing PyO3 LPM gRPC frame wrapper and register_vector_bindings"
---

# Task `t01`: Fix Gemini `Part.text` Field Number and Add Multimodal Part Wire Codec

## Objective & Scope Boundary

Fix `Part.text` protobuf tag encoding to field `2` (`0x12`), add `GeminiContentPart` + `encode_batch_embed_contents_grpc_request` in `vertebra-core`, expose `encode_gemini_embed_contents_grpc_frame` in `vertebra-py/src/vector.rs`, and declare its stub in `_vertebra.pyi` per [`../spec.md` §2](../spec.md) [^core-vector-codec] [^py-vector-binding]. Out of scope: Python embedder callers (`t02`–`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-core/src/vector.rs` — `encode_batch_embed_grpc_request` and `EmbeddingTask` [^core-vector-codec].
- `src/rs/vertebra-core/src/proto.rs` — wire tag helpers (`put_bytes_field`, `put_varint_field`).
- `src/rs/vertebra-py/src/vector.rs` — `encode_gemini_embed_grpc_frame` and `register_vector_bindings` [^py-vector-binding].
- `src/py/vertebra/_vertebra.pyi` — vector codec stubs.
- `src/py/tests/unit/db/test_vector_wire_codecs.py` — wire codec assertions.

## Contracts & Edge Cases

- `GeminiContentPart::Text(s)` emits field `2` (`0x12`); `InlineData { mime_type, data }` emits field `3` (`0x1a`) wrapping `Blob { mime_type = 1, data = 2 }` with raw unencoded bytes; `FileData { mime_type, file_uri }` emits field `6` (`0x32`) wrapping `FileData { mime_type = 1, file_uri = 2 }`.
- `encode_batch_embed_grpc_request` preserves its Rust signature and delegates to `encode_batch_embed_contents_grpc_request` with one `Text` part per string.
- Reject empty `contents`, empty inner part lists, empty `mime_type` on media parts, empty `file_uri`, mismatched `titles` length, or unknown part kind strings with `ValueError` in Python (`Err(String)` in Rust).

## Test Matrix

- Rust `#[cfg(test)]` in `vector.rs` and parametrized pytest cases in `src/py/tests/unit/db/test_vector_wire_codecs.py`: exact byte tags for `"hi"` (`12 02 68 69`), `("inline_data", "image/png", b"\x89P")` (`1a 0f 0a 09 ... 12 02 89 50`), and `("file_data", "video/mp4", "https://example.com/v.mp4")` (`0x32`); rejection of `[]`, `[[]]`, empty `mime_type`, and unknown part kind.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-core --lib vector:: && uv run --no-sync maturin develop --uv && uv run --no-sync python -m mypy.stubtest vertebra._vertebra && uv run --no-sync pytest src/py/tests/unit/db/test_vector_wire_codecs.py -q
```

## Acceptance Criteria

- [ ] `Part.text` encodes on field `2`, `inline_data` (`3`) and `file_data` (`6`) encode exact `content.proto` bytes, and `mypy.stubtest vertebra._vertebra` exits `0` (`R5`).
- [ ] Zero regressions in `vertebra-core` and `test_vector_wire_codecs.py`.

[^core-vector-codec]: `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_grpc_request` — Existing `BatchEmbedContents` protobuf request encoder.
[^py-vector-binding]: `src/rs/vertebra-py/src/vector.rs::encode_gemini_embed_grpc_frame` — Existing PyO3 LPM gRPC frame wrapper and `register_vector_bindings`.
