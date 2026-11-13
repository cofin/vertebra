---
type: Task
id: "gcp-embeddings-and-identity:ch04:t02"
flow_id: gcp-embeddings-and-identity
chapter_id: ch04-docs-and-okf-sync
title: "Synchronize Embeddings, Storage, and PyO3 FFI OKF Chapters Within 200 Lines"
description: "Update embeddings-and-vectors.md (trimming 211L to <= 200L), object-storage.md, and pyo3-ffi-and-batch-dispatch.md with shipped transports, broker/Gemma fixes, and ID token FFI bindings."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch04:t01"
files:
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/embeddings/_broker.py
  - src/rs/vertebra-py/src/gcp_identity.rs
estimated_loc: 150
tags:
  - okf
  - embeddings
  - pyo3
  - storage
sources:
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: "Embeddings and vector slabs OKF chapter (211 lines at baseline)"
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: "Object storage OKF chapter"
  - id: okf-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: "PyO3 FFI and batch dispatch OKF chapter"
---

# Task `t02`: Synchronize Embeddings, Storage, and PyO3 FFI OKF Chapters Within 200 Lines

## Objective & Scope Boundary

Rewrite stale sections of `embeddings-and-vectors.md` (`211L -> <= 200L`), `object-storage.md` (`<= 200L`), and `pyo3-ffi-and-batch-dispatch.md` (`<= 200L`) in present tense per [`../spec.md` §2](../spec.md) [^okf-embeddings] [^okf-storage] [^okf-ffi]. Out of scope: `embeddings-limitations.md` and `gcp-bigquery-and-spanner-drivers.md` (`t03`).

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — cloud transports, SQL pushdowns, and local broker (`211L`) [^okf-embeddings].
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — GCP token cache re-exports and ADC order [^okf-storage].
- `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — `vertebra-py` FFI inventory [^okf-ffi].
- `src/py/vertebra/embeddings/_google.py`, `_broker.py`, and `src/rs/vertebra-py/src/gcp_identity.rs` — verified symbols.

## Contracts & Edge Cases

- Replace `PredictionService/EmbedContent` gRPC claims in `embeddings-and-vectors.md` with Vertex AI v1 REST `:predict` / `:embedContent`, Gemini `Part.text = 2` + `inline_data = 3` / `file_data = 6`, `RemoteEmbedder(id_token_cache=...)`, uniform SQL `embed_batch` + explicit Oracle rotation, per-loop `EmbeddingBroker` + `/vtb-qvec-*` cleanup, and `GemmaEmbedder` `precision` forwarding, keeping total file length `<= 200` lines and truth block `<= 40` lines.
- Record `GcpIdTokenSnapshot`, `SyncGcpIdTokenCache`, and `AsyncGcpIdTokenCache` re-exports + core ADC order in `object-storage.md`, and `gcp_identity.rs` + `encode_gemini_embed_contents_grpc_frame` in `pyo3-ffi-and-batch-dispatch.md` (each `<= 200` lines, expanded YAML mappings, valid `sources:` footnotes, zero standalone `---` body rules).

## Test Matrix

- Assert `wc -l` is `<= 200` on all three OKF files; assert zero `"PredictionService/EmbedContent"` matches in `.agents/bundles/knowledge/`; run `make review-check`.

## Verification

```bash
python3 -c 'from pathlib import Path; [(_ for _ in ()).throw(AssertionError(f"{p}: {len(Path(p).read_text().splitlines())}L")) for p in (".agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md", ".agents/bundles/knowledge/domains/storage/object-storage.md", ".agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md") if len(Path(p).read_text().splitlines()) > 200]' && make review-check
```

## Acceptance Criteria

- [ ] `embeddings-and-vectors.md` (`<= 200L`), `object-storage.md` (`<= 200L`), and `pyo3-ffi-and-batch-dispatch.md` (`<= 200L`) match shipped code with valid OKF v0.2 provenance (`R13`).
- [ ] `make review-check` exits `0`.

[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Embeddings and vector slabs OKF chapter (211 lines at baseline).
[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage OKF chapter.
[^okf-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — PyO3 FFI and batch dispatch OKF chapter.
