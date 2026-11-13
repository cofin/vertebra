---
type: Spec
flow_id: gcp-embeddings-and-identity
chapter_id: ch04-docs-and-okf-sync
title: "Chapter 04: User Documentation, OKF Sync, and Embeddings Limitations"
description: "Updates embeddings and GCP authentication user guides, synchronizes embeddings-and-vectors.md within 200 lines alongside storage and PyO3 FFI OKF chapters, and updates embeddings-limitations.md in place."
status: stable
state: completed
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-vertex-and-gemini-embedder-wire-parity
  - ch03-pushdown-and-local-embedding-broker
plan_commit: 18baadcd
last_reviewed_commit: b518e289
updated_at: "2026-11-13T05:52:30Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-13T05:52:30Z"
    commit: b518e289
tags:
  - docs
  - okf
  - embeddings
  - identity
sources:
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: "User guide for cloud, local, remote, and SQL pushdown embedders"
  - id: docs-storage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: "GCP credentials, ADC precedence, and token cache usage guide"
  - id: docs-storage-ref
    resource: docs/reference/storage.md
    title: "vertebra.storage API reference"
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: "Embeddings and vector slabs OKF chapter (currently 211 lines)"
  - id: okf-limitations
    resource: .agents/bundles/knowledge/domains/agent/embeddings-limitations.md
    title: "Existing embeddings limitations record updated in place"
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: "Object storage OKF chapter listing GCP token cache re-exports"
  - id: okf-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: "PyO3 FFI surface OKF chapter"
  - id: okf-gcp-drivers
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: "BigQuery and Spanner driver ADC credentials OKF chapter"
---

# Chapter 04: User Documentation, OKF Sync, and Embeddings Limitations

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `docs/usage/embeddings-and-vectors.md` cites `RefreshingGcpCredentialProvider` and Vertex gRPC [^docs-embeddings]; `embeddings-and-vectors.md` is `211` lines and claims `PredictionService/EmbedContent` [^okf-embeddings]; `embeddings-limitations.md` lists pre-fix Vertex/Gemini defects [^okf-limitations]; and storage/FFI/GCP docs omit `SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache` and canonical ADC order [^docs-storage-auth] [^okf-storage] [^okf-ffi].
- **Target Architecture**: User guides and reference pages document Vertex AI REST (`:predict`, `:embedContent`), Gemini protobuf multimodal parts, `RemoteEmbedder(id_token_cache=...)`, SQL pushdown parity, per-loop `EmbeddingBroker`, `GemmaEmbedder(precision=...)`, and audience-bound ID tokens (including `authorized_user` ADC `client_id` rules and IAP `programmaticClients`) [^docs-storage-ref]. Every touched OKF chapter stays `<= 200` lines, and `embeddings-limitations.md` is updated in place with zero duplicate limitation files [^okf-gcp-drivers].
- **Scope & Boundaries**: Covers `R13` across `docs/` and `.agents/bundles/knowledge/` (`3` compact tasks).
<!-- truth: end -->

## 1. Specification, Context & Provenance

After `ch01`–`ch03` land, public documentation and OKF chapters must state present-tense shipped behavior without historical sediment, keep `embeddings-and-vectors.md` (`211L -> <= 200L`) and all sibling OKF chapters within the `200`-line ceiling, and update the existing [embeddings-limitations.md](../../../../knowledge/domains/agent/embeddings-limitations.md) in place rather than creating a second limitations file under `domains/agent/` [^okf-embeddings] [^okf-limitations].

## 2. Requirements, Concrete Contracts & Architecture

- **Public User Docs (`t01`)**:
  - `docs/usage/embeddings-and-vectors.md`: remove stale `RefreshingGcpCredentialProvider` and Vertex gRPC references; document Vertex REST `:predict` / `:embedContent` (`auto_truncate=False` default), Gemini gRPC `embed_multimodal`, `RemoteEmbedder(id_token_cache=...)`, explicit Oracle `token_cache` rotation, Spanner `use_ai_function=True` task/dimension binding, and `GemmaEmbedder` `precision` modes; format any install commands with `pymdownx.tabbed` (`=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"`) and concise `msgspec.Struct` examples.
  - `docs/usage/storage-auth-and-signing.md` & `docs/reference/storage.md`: document canonical ADC order (explicit options -> ambient ADC file -> ambient env tokens -> GCE metadata server), `SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache` / `GcpIdTokenSnapshot`, per-credential audience rules (SA, metadata, and `target_service_account` mint any audience; `authorized_user` ADC mints its `client_id` audience, using impersonation or IAP `programmaticClients` for Cloud Run / IAP targets).
- **OKF Knowledge Sync (`t02`–`t03`)**:
  - Tighten `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` from `211` lines to `<= 200` lines (`<= 40`-line truth block) covering Vertex REST, Gemini field-2 + media parts, per-loop `_GoogleEmbedder` & `EmbeddingBroker`, `GemmaEmbedder` precision, and SQL pushdowns.
  - Update `.agents/bundles/knowledge/domains/storage/object-storage.md` and `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`<= 200` lines each) with `gcp_identity.rs` and `encode_gemini_embed_contents_grpc_frame`.
  - Rewrite `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` in place (`<= 200` lines, `verified` stamped to the active commit) recording remaining verified boundaries (loopback vs live Vertex/Gemini API verification in CI, `external_account` WIF out of scope, `authorized_user` ADC `client_id` audience rule without impersonation, DBA-managed Oracle credentials when `token_cache=None`), and sync ADC precedence in `gcp-bigquery-and-spanner-drivers.md` (`<= 200` lines).

## 3. Documentation & OKF Knowledge Plan

- `docs/usage/embeddings-and-vectors.md`, `docs/usage/storage-auth-and-signing.md`, `docs/reference/storage.md` (`t01`).
- `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md`, `.agents/bundles/knowledge/domains/storage/object-storage.md`, `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`t02`).
- `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md`, `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` (`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R13` (User usage & reference docs) | `t01` | `docs/usage/{embeddings-and-vectors,storage-auth-and-signing}.md`, `docs/reference/storage.md` | `make docs` |
| `R13` (`embeddings-and-vectors.md <= 200L`, storage & FFI OKF) | `t02` | `.agents/bundles/knowledge/{domains/agent/embeddings-and-vectors,domains/storage/object-storage,architecture/worker/pyo3-ffi-and-batch-dispatch}.md` | `make review-check` |
| `R13` (In-place `embeddings-limitations.md` & GCP drivers ADC OKF) | `t03` | `.agents/bundles/knowledge/domains/{agent/embeddings-limitations,data/gcp-bigquery-and-spanner-drivers}.md` | `make review-check` |

## 5. Implementation Plan

- [x] [01a74272] [t01-user-embeddings-and-identity-docs.md](tasks/t01-user-embeddings-and-identity-docs.md) — Update embeddings, GCP credentials/IAP/Cloud Run ID token guides, and `vertebra.storage` API reference.
- [x] [01a74272] [t02-embeddings-storage-and-ffi-okf.md](tasks/t02-embeddings-storage-and-ffi-okf.md) — Synchronize `embeddings-and-vectors.md` (`211L -> <= 200L`), `object-storage.md`, and `pyo3-ffi-and-batch-dispatch.md`.
- [x] [01a74272] [t03-embeddings-limitations-and-gcp-driver-okf.md](tasks/t03-embeddings-limitations-and-gcp-driver-okf.md) — Update `embeddings-limitations.md` in place without duplicate limitation files and align ADC order in `gcp-bigquery-and-spanner-drivers.md`.

## 6. Current State & Remaining Plan Snapshot

Completed (`3/3` tasks at `01a74272`).

- **Fast Chapter Gate (`< 45s`)**:
  `make docs && make review-check`

[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — User guide for cloud, local, remote, and SQL pushdown embedders.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — GCP credentials, ADC precedence, and token cache usage guide.
[^docs-storage-ref]: `docs/reference/storage.md` — `vertebra.storage` API reference.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Embeddings and vector slabs OKF chapter (currently 211 lines).
[^okf-limitations]: `.agents/bundles/knowledge/domains/agent/embeddings-limitations.md` — Existing embeddings limitations record updated in place.
[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage OKF chapter listing GCP token cache re-exports.
[^okf-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — PyO3 FFI surface OKF chapter.
[^okf-gcp-drivers]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — BigQuery and Spanner driver ADC credentials OKF chapter.
