---
type: Spec
parent_flow: gcp-embeddings-and-identity
chapter_id: ch06-docs-okf-and-limitations
title: "Chapter 06: Docs, OKF, and Limitations"
description: User documentation, OKF chapter updates, and a limitations record for the Google embedding transports, ADC order, and ID token caches.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch05-vertex-rest-and-remote-identity
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
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: Embeddings usage guide describing gRPC Vertex transport and RefreshingGcpCredentialProvider
  - id: docs-storage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: GCP credentials and token cache guide
  - id: docs-storage-ref
    resource: docs/reference/storage.md
    title: vertebra.storage reference with the GCS token caches section
  - id: okf-embeddings
    resource: .agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md
    title: Embeddings OKF chapter claiming PredictionService/EmbedContent and inline media
  - id: okf-object-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: Object storage OKF chapter listing token-cache re-exports
  - id: okf-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: PyO3 module surface listing gcp_auth.rs exports
  - id: okf-gcp-drivers
    resource: .agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md
    title: Scoped ADC description for BigQuery and Spanner credentials
  - id: okf-limitations-precedent
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Existing limitations record used as the format precedent
tags:
  - docs
  - okf
  - limitations
---

# Chapter 06: Docs, OKF, and Limitations

<!-- truth: start -->
- **Scope**: `ch06:t01` updates `docs/usage/embeddings-and-vectors.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/reference/storage.md` [^docs-embeddings] [^docs-storage-auth] [^docs-storage-ref]; `ch06:t02` updates the embeddings, object storage, and PyO3 FFI OKF chapters [^okf-embeddings] [^okf-object-storage] [^okf-ffi]; `ch06:t03` creates `domains/agent/embeddings-and-identity-limitations.md`, links it from `domains/index.md`, and updates the ADC statement in `gcp-bigquery-and-spanner-drivers.md` [^okf-gcp-drivers] [^okf-limitations-precedent].
- **Identity Docs**: the credentials guide states the per-credential audience rules (service account, metadata server, and `target_service_account` impersonation mint any audience; `authorized_user` ADC mints only its `client_id` audience), the exact audience-mismatch fixes, IAP `programmaticClients`, and Cloud Run usage (audience is the service URL; user ADC needs impersonation or the IAP custom-audience flow).
- **Invariants**: every claim names a symbol that exists at the chapter's HEAD; public docs keep Mermaid diagrams high level and examples short; OKF chapters carry `sources:` with matching footnotes and no `---` body rules.
- **Verification Gate**: `make docs && make review-check`.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The embeddings guide describes `VertexEmbedder` as "Rust HTTP/2 gRPC + `RefreshingGcpCredentialProvider`" and the OKF chapter claims `PredictionService/EmbedContent` plus inline media for Gemini; neither matches HEAD, and after `ch01`–`ch05` both must describe the shipped transports, ADC order, ID token caches, and the explicit Oracle rotation rule [^docs-embeddings] [^okf-embeddings].

### 1.2 Non-Goals

- `CHANGELOG.md` and `docs/changelog/index.md` (written at release time).
- New docs pages or navigation changes; content lands in existing pages.

### 1.3 Upstream Baseline & Code Analysis

- `docs/usage/embeddings-and-vectors.md` sections: `Embedder Selection Matrix`, `Google Cloud & In-Database SQL Pushdown`.
- `docs/usage/storage-auth-and-signing.md` section `Google Cloud credentials and token caches`.
- `docs/reference/storage.md` section `GCS token caches`.
- `domains/storage/object-storage-limitations.md` provides the limitations frontmatter shape (`type: Gotcha`, `status: stable`, `sources:`).

## 2. Requirements, Concrete Contracts & Architecture

### Concrete Module & Signature Contracts

- User docs reference only public import paths: `vertebra.embeddings` (`Embedder`, `VertexEmbedder`, `GeminiEmbedder`, `RemoteEmbedder`, `MultimodalPart`, `EmbeddingError`) and `vertebra.storage` (`SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenSnapshot`, `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot`).
- Package install commands, when shown, use `pymdownx.tabbed` blocks (`uv`, `pip`, `pdm`, `poetry`).

### Caller -> Seam -> Collaborator Map

- Reader -> `docs/usage/*.md` -> examples that call the shipped public APIs; maintainer -> OKF chapters -> `path::Symbol` sources.

### Edge-Case & Invariant Matrix

| Scenario | Check | Owner |
| --- | --- | --- |
| Stale transport claims | `git grep -n "RefreshingGcpCredentialProvider" docs/usage/embeddings-and-vectors.md` returns nothing | `t01` |
| Stale OKF claims | `git grep -n "PredictionService/EmbedContent" .agents/bundles/knowledge` returns nothing | `t02` |
| Broken links | `make docs` exits `0` | `t01` |
| Stale user-credential rejection | `git grep -n "cannot mint audience-bound" docs .agents/bundles/knowledge` returns nothing | `t01`, `t03` |
| IAP and Cloud Run guidance | `git grep -n "programmaticClients" docs/usage/storage-auth-and-signing.md` and `git grep -n "Cloud Run" docs/usage/storage-auth-and-signing.md` each match | `t01` |
| OKF frontmatter | `make review-check` exits `0` | `t02`, `t03` |

### Alternatives Comparison

- A new `docs/usage/gcp-identity.md` page would need navigation changes in `zensical.toml` and `docs/usage/index.md`; the existing GCP credentials section already introduces token caches, so ID tokens extend that section.

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/9 files documented]`
- `[ ]` `docs/usage/embeddings-and-vectors.md` (`t01`)
- `[ ]` `docs/usage/storage-auth-and-signing.md` (`t01`)
- `[ ]` `docs/reference/storage.md` (`t01`)
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/storage/object-storage.md` (`t02`)
- `[ ]` `.agents/bundles/knowledge/domains/agent/embeddings-and-identity-limitations.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/index.md` (`t03`)
- `[ ]` `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` (`t03`)

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Gate |
| --- | --- | --- | --- |
| R12 (user docs) | `t01` | `documentation_validation` | `make docs` |
| R12 (OKF) | `t02` | `documentation_validation` | `make review-check` |
| R12 (limitations, index, ADC) | `t03` | `documentation_validation` | `make review-check` |

## 5. Implementation Plan

- [ ] t01 — User docs for transports, ADC order, and ID tokens (tasks/t01-user-docs.md)
- [ ] t02 — Embeddings, storage, and FFI OKF chapters (tasks/t02-embeddings-and-ffi-okf.md)
- [ ] t03 — Limitations record, domain index, and ADC statement (tasks/t03-identity-okf-and-limitations.md)
- **Chapter Recovery Checkpoint**: `make docs && make review-check && make lint-all`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `c8722c76`; `0/3` tasks completed; depends on `ch05`.

[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — Embeddings guide.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — Credentials guide.
[^docs-storage-ref]: `docs/reference/storage.md` — Storage reference.
[^okf-embeddings]: `.agents/bundles/knowledge/domains/agent/embeddings-and-vectors.md` — Embeddings OKF chapter.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage OKF chapter.
[^okf-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — PyO3 module surface.
[^okf-gcp-drivers]: `.agents/bundles/knowledge/domains/data/gcp-bigquery-and-spanner-drivers.md` — Scoped ADC description.
[^okf-limitations-precedent]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations format.
