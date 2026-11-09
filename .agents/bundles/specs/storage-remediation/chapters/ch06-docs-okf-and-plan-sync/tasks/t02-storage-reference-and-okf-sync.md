---
type: Task
id: "storage-remediation:ch06:t02"
chapter_id: ch06-docs-okf-and-plan-sync
title: Expand docs/reference/storage.md API coverage and synchronize product.md and object-storage.md OKF chapters
description: Expand mkdocstrings member lists in docs/reference/storage.md to cover the full public vertebra.storage surface, flatten the truth block and fix vertebra._storage vs vertebra._vertebra attribution in .agents/bundles/product/product.md, and resynthesize .agents/bundles/knowledge/domains/storage/object-storage.md within <= 200 lines.
status: stable
state: in_progress
priority: P1
verification_strategy: documentation_validation
depends_on:
  - "storage-remediation:ch06:t01"
read_set:
  - docs/reference/storage.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - src/py/vertebra/storage/__init__.py
  - src/py/vertebra/storage/_store.py
files:
  - docs/reference/storage.md
  - .agents/bundles/product/product.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
tests: []
estimated_loc: 220
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
claimed_by: executor
claimed_at: "2026-11-09T01:27:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T01:27:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: doc-ref-storage
    resource: docs/reference/storage.md
    title: Zensical vertebra.storage API reference
  - id: okf-product
    resource: .agents/bundles/product/product.md
    title: Vertebra Product Definition OKF chapter
  - id: okf-object-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: Object Storage, Streaming, and Presigning OKF domain chapter
tags:
  - storage
  - docs
  - okf
  - reference
---

# Task t02: Storage API Reference and OKF Knowledge Synchronization

## Objective & Scope Boundary

Update `docs/reference/storage.md` [^doc-ref-storage], `.agents/bundles/product/product.md` [^okf-product], and `.agents/bundles/knowledge/domains/storage/object-storage.md` [^okf-object-storage] (`3` files):
1. In `docs/reference/storage.md`: expand `SyncObjectStore` and `AsyncObjectStore` `members:` lists to cover factory constructors, CRUD, `exists`, conditional writes, `get_view`, `get_stream`, `put_stream`, `put_file`, `get_file`, `download_many`, `upload_many`, `staged_download`, `get_range`, `open_reader`, `open_writer`, `list_page`, `list_with_delimiter`, `glob`, `copy`, `rename`, `compose`, `sign`, `presign_get`, `presign_put`, `verify_presigned_url`, `iter_arrow_batches`, `iter_parquet_batches`, `write_arrow`, `write_parquet`, and `to_async` / `to_sync`; add directives covering `PutResult`, `SyncWritableFile`, `AsyncWritableFile`, `PreconditionError`, `RangeNotSatisfiableError`, `StreamingStorageResponse`, `ByteRange`, `parse_byte_range`, `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot`.
2. In `.agents/bundles/product/product.md`: convert `<!-- truth: start -->` .. `<!-- truth: end -->` into `<= 40` flat bullet lines (moving `## Project Purpose` and `## Core Product Capabilities` outside or below the truth block so zero `##` headings appear inside the truth block), update the `Object Storage` bullet and Architectural Overview table so `vertebra._storage` lists its actual PyO3 exports (`SyncObjectStore`, `AsyncObjectStore`, `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, `AsyncMultipartUpload`, `ObjectMeta`, `PutResult`, `StorageBuffer`, `RustTaskAwaitable`) while `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` are listed under `vertebra._vertebra`, and update `verified:` to the latest ancestor commit.
3. In `.agents/bundles/knowledge/domains/storage/object-storage.md`: resynthesize in present tense within `<= 200` total lines and `<= 40` truth-block lines covering `GcpTokenCacheCapsule`, GCS conditional `version` / `skip_signature` static-bearer / `allow_ambient_adc=false` / `x-goog-user-project` / HTTP & IAM connection pooling / custom `endpoint` presigning, zero-copy `PyBackedBytes` vs synchronous mutable buffer copy, checked `get_range`, `StorePath::parse` raw keys, `PreconditionError`, streaming `OciObjectStore`, bulk file transfers (`get_file`, `download_many`, `upload_many`, `staged_download`), `copy(..., overwrite=False)`, `glob`, server-side GCS `compose`, read-ahead `open_reader`, streaming `open_writer`, Parquet `pre_buffer=True` + encoding options, zero-copy `read_arrow_shm`, and clean-break framework storage adapters.

## Bounded Read Manifest

- `docs/reference/storage.md` — Zensical storage reference page [^doc-ref-storage].
- `.agents/bundles/product/product.md` — Product definition OKF chapter [^okf-product].
- `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage domain OKF chapter [^okf-object-storage].
- `src/py/vertebra/storage/__init__.py` — Public `vertebra.storage` exports.
- `src/py/vertebra/storage/_store.py` — Public store, reader, writer, config, and metadata classes.

## Symbol & Signature Contracts

- Every `mkdocstrings` target in `docs/reference/storage.md` matches an exported symbol in `vertebra.storage` / `vertebra.typing`.
- `len(truth_lines) <= 40` in both `product.md` and `object-storage.md`; total line count of `object-storage.md` is `<= 200`.

## Edge Cases & Error Semantics

- `PreconditionError` is defined in `vertebra._typing`, re-exported via `vertebra.typing` and `vertebra.storage`; reference it via `vertebra.typing.PreconditionError` (or `vertebra._typing.PreconditionError` if Griffe requires concrete definition path — verify with `make docs`).
- Zero standalone `---` horizontal rules in OKF Markdown bodies.

## Public-Seam Test Blueprint

- **Verification strategy**: `documentation_validation` (`make docs && make docs-clean && make review-check`).
- **Worked Check Matrix**:

| Check | Target | Expected Result |
| --- | --- | --- |
| Zensical reference build & docstring scan | `make docs && make docs-clean` | Exits `0` with all `members:` rendered cleanly. |
| OKF bundle, link, truth-budget, & privacy gate | `make review-check` and `wc -l .agents/bundles/knowledge/domains/storage/object-storage.md` | `make review-check` exits `0` and `object-storage.md` is `<= 200` lines. |

## Verification

```bash
make docs && make docs-clean
make review-check
```

## Acceptance Criteria

- [ ] `docs/reference/storage.md` renders the complete `vertebra.storage` public API cleanly under `make docs` (R6.2).
- [ ] `.agents/bundles/product/product.md` and `.agents/bundles/knowledge/domains/storage/object-storage.md` (`<= 200` lines) pass `make review-check` with `<= 40`-line flat-bullet truth blocks and accurate symbol attribution (R6.2).

## Notes & Discoveries

- [2026-11-08 20:55] Stamp `verified.commit` with an ancestor commit SHA of `HEAD` so `ReviewSetupValidator._check_bundles` passes.

[^doc-ref-storage]: `docs/reference/storage.md` — Zensical `vertebra.storage` API reference.
[^okf-product]: `.agents/bundles/product/product.md` — Vertebra Product Definition OKF chapter.
[^okf-object-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object Storage, Streaming, and Presigning OKF domain chapter.
