---
type: Task
id: "gcp-embeddings-and-identity:ch06:t02"
chapter_id: ch06-docs-okf-and-limitations
title: Synchronize the embeddings, object storage, and PyO3 FFI OKF chapters
description: Rewrite the embeddings OKF chapter for the shipped transports and runtime invariants, add the ID token classes and ADC order to the object storage chapter, and list gcp_identity.rs and the multimodal encoder binding in the PyO3 FFI chapter.
status: draft
state: pending
priority: P2
verification_strategy: documentation_validation
depends_on:
  - "gcp-embeddings-and-identity:ch06:t01"
read_set:
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - src/py/vertebra/embeddings/_google.py
  - src/rs/vertebra-py/src/gcp_identity.rs
  - .agents/bundles/specs/gcp-embeddings-and-identity/spec.md
files:
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
tests: []
estimated_loc: 140
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
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: Embeddings OKF chapter
  - id: okf-object-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: Object storage OKF chapter
  - id: okf-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: PyO3 FFI module surface
tags:
  - okf
  - docs
---

# Task t02: Synchronize the Embeddings, Object Storage, and PyO3 FFI OKF Chapters

## Objective & Scope Boundary

Rewrite in present tense, keeping each chapter's frontmatter, truth block (`<= 40` lines), and footnote style [^okf-embeddings] [^okf-object-storage] [^okf-ffi]:

1. `domains/agent/embeddings-and-vectors.md`
   - Truth block "Cloud and SQL Embeddings": `VertexEmbedder` posts Vertex AI REST `:predict` / `:embedContent` (routing from `ModelSpec.vertex_method`, `max_instances_per_request`), `GeminiEmbedder` posts Gemini gRPC `BatchEmbedContents` with `Part.text=2`, `inline_data=3`, `file_data=6`, `embed_multimodal` returns one row per item, `_GoogleEmbedder` owns auth/`401` retry/`EmbeddingError`/per-loop micro-batching; Oracle rotation runs only with an explicit token cache.
   - Section 4 table rows for `VertexEmbedder`, `GeminiEmbedder`, `RemoteEmbedder` (ID token option), `OracleEmbeddingPushdown` (explicit cache).
   - Add sources/footnotes for `src/py/vertebra/embeddings/_models.py::ModelSpec` and `src/rs/vertebra-core/src/vector.rs::encode_batch_embed_contents_grpc_request`; bump `verified` with the task commit.
2. `domains/storage/object-storage.md`: extend the token-cache bullet with `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot` (re-exported, no capsule) and one bullet on ADC order ending at the metadata server, citing `src/rs/vertebra-core/src/gcp_identity.rs::GcpIdTokenCache`.
3. `architecture/worker/pyo3-ffi-and-batch-dispatch.md`: add `gcp_identity.rs` (`SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot`, detached sync refresh, `RustDbTaskAwaitable` async refresh) to the module surface list and `encode_gemini_embed_contents_grpc_frame` to the vector binding list; add a source and footnote for `src/rs/vertebra-py/src/gcp_identity.rs`.

Out of scope: limitations record and index (`t03`).

## Bounded Read Manifest

- The three OKF chapters above.
- `src/py/vertebra/embeddings/_google.py` (shipped class names and methods).
- `src/rs/vertebra-py/src/gcp_identity.rs` (shipped binding names).
- Root spec truth block.

## Symbol & Signature Contracts

Every cited symbol must resolve at HEAD: `git grep -n "<Symbol>" -- <path>` returns a match for each `path::Symbol` added to `sources:`.

## Edge Cases & Error Semantics

- Remove the claims `PredictionService/EmbedContent` and "forwards inline `bytes` and `MultimodalPart` payloads alongside text parts" unless they describe shipped behavior.
- No revision history, `---` body rules, or reviewer finding identifiers.

## Public-Seam Test Blueprint

- **Gate**: `documentation_validation`.
- **Worked Check Matrix**:

| Case | Input | Expected |
| --- | --- | --- |
| Review manifest gate | `make review-check` | Exit `0` |
| Stale claim removed | `git grep -n "PredictionService/EmbedContent" .agents/bundles/knowledge` | No output |
| New binding listed | `git grep -n "gcp_identity.rs" .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` | At least one match |
| Footnote parity | Every `sources[].id` in each edited file has one `[^id]:` footnote | Manual check recorded in Notes |

## Verification

```bash
make review-check
git grep -n "PredictionService/EmbedContent" .agents/bundles/knowledge
git grep -n "gcp_identity.rs" .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
```

Commit as `docs(okf): sync embeddings, storage, and FFI chapters for GCP identity`.

## Acceptance Criteria

- [ ] Embeddings OKF chapter matches shipped transports and invariants (R12).
- [ ] Object storage and FFI chapters list the ID token classes and bindings (R12).
- [ ] `make review-check` exits `0`.

## Notes & Discoveries

- None yet.

[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Rewritten here.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Extended here.
[^okf-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — Extended here.
