---
type: Task
id: storage-completeness:ch05:t02
chapter_id: ch05-sqlspec-arrow-and-docs
title: Synchronize storage usage guides and OKF object-storage knowledge chapter
description: Update docs/usage/storage.md, docs/usage/storage-auth-and-signing.md,
  and .agents/bundles/knowledge/domains/storage/object-storage.md to match the shipped
  storage, auth, framework, and Arrow contracts.
status: stable
state: in_progress
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch05:t01
read_set:
- docs/usage/storage.md
- docs/usage/storage-auth-and-signing.md
- .agents/bundles/knowledge/domains/storage/object-storage.md
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_streaming.py
- src/py/vertebra/extensions/litestar/store.py
files:
- docs/usage/storage.md
- docs/usage/storage-auth-and-signing.md
- .agents/bundles/knowledge/domains/storage/object-storage.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 180
plan_revision: 10
plan_commit: '78293774'
state_revision: 1
claimed_by: agent:final-storage-docs-executor
claimed_at: '2026-11-08T18:17:27.526464Z'
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-08T18:17:27.526464Z'
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
- id: facade-contract
  resource: src/py/vertebra/storage/_store.py::SyncObjectStore
  title: Public Python storage and signing contracts
- id: http-contract
  resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
  title: Async HTTP Range response contract
- id: litestar-contract
  resource: src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore
  title: Litestar Store and filesystem contracts
tags:
- storage
- docs
- okf
- signing
---
# Task t02: Synchronize Storage Usage Guides and OKF `object-storage` Knowledge Chapter

## Objective & Scope Boundary

Bring user-facing storage documentation (`docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`) and the OKF v0.2 storage domain chapter (`.agents/bundles/knowledge/domains/storage/object-storage.md`) into 100% present-tense parity with Chapters 01–05 [^docs-storage] [^docs-storage-auth] [^okf-object-storage].

## Bounded Read Manifest

- `docs/usage/storage.md` — owned documentation or implemented public contract.
- `docs/usage/storage-auth-and-signing.md` — owned documentation or implemented public contract.
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — owned documentation or implemented public contract.
- `src/py/vertebra/storage/_store.py` — owned documentation or implemented public contract.
- `src/py/vertebra/storage/_streaming.py` — owned documentation or implemented public contract.
- `src/py/vertebra/extensions/litestar/store.py` — owned documentation or implemented public contract.

## Symbol & Signature Contracts

- `docs/usage/storage.md`:
  - Document fail-closed `SyncObjectStore` / `AsyncObjectStore` construction (explicit `LocalConfig`, `S3Config`, `GCSConfig`, `R2Config`, `OCIConfig`, `AzureConfig`, or test-only `MemoryConfig`), `get_stream`, `put_stream`, conditional `put(if_not_exists=..., if_match=...)`, `list_page`, `list_with_delimiter`, `rename`, `StreamingStorageResponse` (`206` / `416`), `ObjectStoreLitestarStore`, `ObjectStoreFileSystemAdapter`, `@deconstructible` `DjangoStorage`, Advanced Alchemy `register_storage_backend`, and batch-streaming `write_parquet` / `iter_parquet_batches`.
- `docs/usage/storage-auth-and-signing.md`:
  - Document `from vertebra.storage import SyncGcpTokenCache, AsyncGcpTokenCache, GcpTokenSnapshot` (backed by `vertebra._vertebra`), removing any `vertebra._storage.SyncGcpTokenCache` import examples, and document GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR signing.
- `.agents/bundles/knowledge/domains/storage/object-storage.md`:
  - Preserve `ch01` `RustTaskAwaitable`, `src/rs/vertebra-core/src/notify.rs` + `src/rs/vertebra-core/src/gcp_auth.rs` provenance, unified `vertebra._vertebra` GCP token caches, and fail-closed construction invariants.
  - Extend Section 4 (`Range Reads & Ecosystem Adapters`) and `sources:` with `ch02`–`ch04` `@deconstructible` `DjangoStorage`, `StorageRegistry` Advanced Alchemy registration, Litestar `ObjectStoreLitestarStore` / `ObjectStoreFileSystemAdapter` / RFC 9110 `206` & `416` streaming, and batch-streaming Arrow/Parquet helpers, updating `verified` to the latest chapter commit.

- Document bounded Parquet batch prefetch by default (`pre_buffer=False`), permitted fixed footer read-ahead, and the SQLSpec explicit `pre_buffer=True` compatibility option that opts out of the bounded-prefetch guarantee. Full-Table reads intentionally materialize their Table return and preserve caller options.

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

- [ ] `docs/usage/storage.md` and `docs/usage/storage-auth-and-signing.md` accurately document all Chapter 01–05 APIs without deprecated symbols (R1–R8).
- [ ] `.agents/bundles/knowledge/domains/storage/object-storage.md` cites `RustTaskAwaitable`, `notify.rs`, and unified `vertebra._vertebra` token caches and passes `make review-check` + `make docs` (R1–R8).

## Notes & Discoveries

- [2026-11-08 00:00] When closing ch05:t02, retain the completed ch04 `[1/1 files documented in ch04]` manifest and set the ch05 three-file manifest to `[3/3 files documented]`; set the root five-file manifest to `[5/5 files documented]`, preserving its already documented product/tech-stack.md and knowledge/domains/extensions/litestar.md targets.

[^docs-storage]: `docs/usage/storage.md` — Storage usage documentation.
[^docs-storage-auth]: `docs/usage/storage-auth-and-signing.md` — Storage authentication and presigned URL guide.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — OKF domain chapter for object storage.

The implemented facade, response and Litestar signatures anchor runnable examples [^facade-contract] [^http-contract] [^litestar-contract].

[^facade-contract]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Public Python storage and signing contracts.
[^http-contract]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Async HTTP Range response contract.
[^litestar-contract]: `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` — Litestar Store and filesystem contracts.
