---
type: Task
id: "storage-completeness:ch04:t03"
chapter_id: ch04-arrow-shm-and-docs
title: Synchronize storage usage guides and OKF object-storage knowledge chapter
description: Update docs/usage/storage.md, docs/usage/storage-auth-and-signing.md, and .agents/bundles/knowledge/domains/storage/object-storage.md to match the shipped storage, auth, framework, and Arrow contracts.
status: stable
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "storage-completeness:ch04:t02"
read_set:
  - docs/usage/storage.md
  - docs/usage/storage-auth-and-signing.md
  - docs/usage/storage-parquet-and-orm.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - src/py/vertebra/_storage.pyi
  - src/py/tests/unit/tools/test_docs_tooling.py
files:
  - docs/usage/storage.md
  - docs/usage/storage-auth-and-signing.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
tests:
  - src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 180
plan_revision: 3
plan_commit: e8901de4
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-08T02:25:35Z"
commit: null
sources:
  - id: docs-storage
    resource: docs/usage/storage.md
    title: User-facing object storage guide
  - id: docs-storage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: User-facing GCP auth and presigned URL guide
  - id: okf-object-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: OKF v0.2 domain knowledge chapter for object storage
tags:
  - storage
  - docs
  - okf
  - signing
---

# Task t03: Synchronize Storage Usage Guides and OKF `object-storage` Knowledge Chapter

## Objective & Scope Boundary

Bring user-facing storage documentation (`docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`) and the OKF v0.2 storage domain chapter (`.agents/bundles/knowledge/domains/storage/object-storage.md`) into 100% present-tense parity with Chapters 01–04 [^docs-storage] [^docs-storage-auth] [^okf-object-storage].

## Bounded Read Manifest

- `docs/usage/storage.md` — Core `SyncObjectStore` / `AsyncObjectStore`, streaming, Litestar, and framework adapter examples [^docs-storage].
- `docs/usage/storage-auth-and-signing.md` — GCP token caches and multi-cloud presigned URL examples [^docs-storage-auth].
- `docs/usage/storage-parquet-and-orm.md` — Cross-links with Parquet and ORM docs.
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — OKF truth block, architecture, `sources:`, and gotchas [^okf-object-storage].
- `src/py/vertebra/_storage.pyi` — Authoritative public PyO3 symbols on `vertebra._storage`.
- `src/py/tests/unit/tools/test_docs_tooling.py` — Docs build and link validation test gate.

## Symbol & Signature Contracts

- `docs/usage/storage.md`:
  - Document fail-closed `SyncObjectStore` / `AsyncObjectStore` construction (explicit `LocalConfig`, `S3Config`, `GCSConfig`, `R2Config`, `OCIConfig`, `AzureConfig`, or test-only `MemoryConfig`), `get_stream`, `put_stream`, conditional `put(if_not_exists=..., if_match=...)`, `list_page`, `list_with_delimiter`, `rename`, `StreamingStorageResponse` (`206` / `416`), `ObjectStoreLitestarStore`, `ObjectStoreFileSystemAdapter`, `@deconstructible` `DjangoStorage`, Advanced Alchemy `register_storage_backend`, and batch-streaming `write_parquet` / `iter_parquet_batches`.
- `docs/usage/storage-auth-and-signing.md`:
  - Document `from vertebra.storage import SyncGcpTokenCache, AsyncGcpTokenCache, GcpTokenSnapshot` (backed by `vertebra._vertebra`), removing any `vertebra._storage.SyncGcpTokenCache` import examples, and document GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR signing.
- `.agents/bundles/knowledge/domains/storage/object-storage.md`:
  - Preserve `ch01` `RustTaskAwaitable`, `src/rs/vertebra-core/src/notify.rs` + `src/rs/vertebra-core/src/gcp_auth.rs` provenance, unified `vertebra._vertebra` GCP token caches, and fail-closed construction invariants.
  - Extend Section 4 (`Range Reads & Ecosystem Adapters`) and `sources:` with `ch02`–`ch04` `@deconstructible` `DjangoStorage`, `StorageRegistry` Advanced Alchemy registration, Litestar `ObjectStoreLitestarStore` / `ObjectStoreFileSystemAdapter` / RFC 9110 `206` & `416` streaming, and batch-streaming Arrow/Parquet helpers, updating `verified` to the latest chapter commit.

## Edge Cases & Error Semantics

- Zero standalone `---` horizontal rules in `.agents/bundles/knowledge/domains/storage/object-storage.md`, truth block `<= 40` lines immediately after `# H1`, and 1:1 `sources` to footnote parity.
- Every relative link across `docs/usage/storage*.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md` resolves cleanly under `make docs` and `make review-check`.
- Zero privacy or prose-hygiene violations (host filesystem user paths, OAuth bearer tokens, or numbered architectural tier clichés) anywhere in docs or OKF files.

## Public-Seam Test Blueprint

- **Canonical test / validation gates**: `src/py/tests/unit/tools/test_docs_tooling.py`, `make docs`, and `make review-check`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Stale symbol elimination | Search `docs/usage/` and `.agents/bundles/knowledge/domains/storage/object-storage.md` for `RustStorageAwaitable`, `_storage.SyncGcpTokenCache`, or `storages.get(` | `0` matches across all documentation and OKF files. |
| OKF bundle & privacy validation | `uv run --no-sync python -m tools.scripts.review_setup validate --rules bundles,privacy` | Exits `0` with `0` bundle or privacy issues; `object-storage.md` truth block `<= 40` lines and all `sources[].resource` paths exist. |
| Zensical site & docs tooling build | `make docs` and `uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q` | Both exit `0` with zero broken links or docstring warnings. |

## Verification

- Initial check: `uv run --no-sync python -m tools.scripts.review_setup validate --rules bundles,privacy`
- Final Green + docs + review-check gate: `make review-check && make docs && uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`

## Acceptance Criteria

- [ ] `docs/usage/storage.md` and `docs/usage/storage-auth-and-signing.md` accurately document all Chapter 01–04 APIs without deprecated symbols (R1–R8).
- [ ] `.agents/bundles/knowledge/domains/storage/object-storage.md` cites `RustTaskAwaitable`, `notify.rs`, and unified `vertebra._vertebra` token caches and passes `make review-check` + `make docs` (R1–R8).

## Notes & Discoveries

- [2026-11-07 04:15] Update `[3/3 files documented]` in `chapters/ch04-arrow-shm-and-docs/spec.md` and root `specs/storage-completeness/spec.md` when closing `ch04:t03`.

[^docs-storage]: `docs/usage/storage.md` — Storage usage documentation.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — Storage authentication and presigned URL guide.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — OKF domain chapter for object storage.
