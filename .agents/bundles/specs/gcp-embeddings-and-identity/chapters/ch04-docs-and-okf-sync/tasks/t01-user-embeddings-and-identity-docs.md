---
type: Task
id: "gcp-embeddings-and-identity:ch04:t01"
flow_id: gcp-embeddings-and-identity
chapter_id: ch04-docs-and-okf-sync
title: "Update User Guides and API Reference for Embeddings and GCP Identity Tokens"
description: "Synchronize docs/usage/embeddings-and-vectors.md, docs/usage/storage-auth-and-signing.md, and docs/reference/storage.md with Vertex REST, Gemini multimodal, ADC order, and ID token caches."
status: stable
state: completed
commit: 01a74272
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "gcp-embeddings-and-identity:ch02:t04"
  - "gcp-embeddings-and-identity:ch03:t04"
files:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage-auth-and-signing.md
  - docs/reference/storage.md
tests: []
read_set:
  - docs/usage/embeddings-and-vectors.md
  - docs/usage/storage-auth-and-signing.md
  - docs/reference/storage.md
  - src/py/vertebra/embeddings/_google.py
  - src/py/vertebra/storage/__init__.py
estimated_loc: 210
tags:
  - docs
  - embeddings
  - identity
  - gcp
sources:
  - id: docs-embeddings
    resource: docs/usage/embeddings-and-vectors.md
    title: "Embeddings and vectors usage guide"
  - id: docs-storage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: "Storage authentication and GCP token caches guide"
  - id: docs-storage-ref
    resource: docs/reference/storage.md
    title: "vertebra.storage API reference"
---

# Task `t01`: Update User Guides and API Reference for Embeddings and GCP Identity Tokens

## Objective & Scope Boundary

Update `docs/usage/embeddings-and-vectors.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/reference/storage.md` in present tense per [`../spec.md` §2](../spec.md) [^docs-embeddings] [^docs-storage-auth] [^docs-storage-ref]. Out of scope: OKF chapters (`t02`–`t03`).

## Bounded Read Manifest

- `docs/usage/embeddings-and-vectors.md` — embedder matrix, cloud/multimodal examples, and SQL pushdown section [^docs-embeddings].
- `docs/usage/storage-auth-and-signing.md` — GCP credentials and token caches section [^docs-storage-auth].
- `docs/reference/storage.md` — `vertebra.storage` token-cache reference [^docs-storage-ref].
- `src/py/vertebra/embeddings/_google.py` and `src/py/vertebra/storage/__init__.py` — shipped public signatures.

## Contracts & Edge Cases

- Remove `RefreshingGcpCredentialProvider` and Vertex gRPC claims from `docs/usage/embeddings-and-vectors.md`; document Vertex AI REST (`:predict`, `:embedContent`, `auto_truncate=False`), Gemini gRPC `embed_multimodal`, `RemoteEmbedder(id_token_cache=...)`, explicit Oracle `token_cache`, Spanner `use_ai_function=True`, and `GemmaEmbedder(precision=...)`.
- Document `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `GcpIdTokenSnapshot` in `docs/usage/storage-auth-and-signing.md` and `docs/reference/storage.md`, including ADC precedence, `authorized_user` ADC `client_id` audience behavior, `target_service_account` impersonation, IAP `programmaticClients`, and Cloud Run service URL audiences.
- Keep Mermaid diagrams conceptual (`4`–`6` nodes) and any installation snippets formatted with `pymdownx.tabbed`.

## Test Matrix

- Verify zero stale `RefreshingGcpCredentialProvider` or `"cannot mint audience-bound"` occurrences in `docs/`; verify `programmaticClients` and `Cloud Run` guidance present in `docs/usage/storage-auth-and-signing.md`; build full Zensical site via `make docs`.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] User usage guides and `docs/reference/storage.md` accurately document all shipped cloud/local/SQL embedding and GCP ID token surfaces (`R13`).
- [ ] `make docs` exits `0` with zero broken links or docstring warnings.

[^docs-embeddings]: `docs/usage/embeddings-and-vectors.md` — Embeddings and vectors usage guide.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — Storage authentication and GCP token caches guide.
[^docs-storage-ref]: `docs/reference/storage.md` — `vertebra.storage` API reference.
