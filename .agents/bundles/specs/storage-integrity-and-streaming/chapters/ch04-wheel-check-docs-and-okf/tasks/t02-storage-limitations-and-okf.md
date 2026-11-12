---
type: Task
id: "storage-integrity-and-streaming:ch04:t02"
chapter_id: ch04-wheel-check-docs-and-okf
title: Complete storage limitations, OKF chapter, and usage docs
description: Record every storage caveat in the limitations chapter, update the object-storage OKF chapter for create-only copy, receipts, verified reads, and direct Arrow writes, and document the new options in the storage usage guide.
status: draft
state: pending
priority: P2
verification_strategy: docs
depends_on:
  - "storage-integrity-and-streaming:ch04:t01"
read_set:
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - docs/usage/storage.md
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/django/storage.py
files:
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - docs/usage/storage.md
tests: []
estimated_loc: 120
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
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: Limitations chapter
  - id: presign-verify
    resource: src/py/vertebra/storage/_store.py::_BaseObjectStore
    title: verify_presigned_url raises NotImplementedError for gcs, azure, oci
  - id: django-times
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: get_created_time and get_accessed_time raise NotImplementedError
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: rename is copy then delete on object stores
tags:
  - docs
  - okf
  - storage
---

# Task t02: Complete Storage Limitations, OKF Chapter, and Usage Docs

## Objective & Scope Boundary

1. `object-storage-limitations.md` truth block [^limitations]:
   - Replace the create-only copy/rename bullet with: create-only copy and rename are supported on all backends; S3/B2 rely on conditional `CompleteMultipartUpload`, R2 on `cf-copy-destination-if-none-match`; S3-compatible servers that ignore those conditions return their own error.
   - Add: `verify_presigned_url` raises `NotImplementedError` for `gcs`, `azure`, and `oci` signing providers because offline verification needs provider keys [^presign-verify].
   - Add: `DjangoStorage.get_created_time` / `get_accessed_time` raise `NotImplementedError` because object metadata exposes only modification time [^django-times].
   - Add: `rename` on cloud object stores is copy then delete and not atomic; a crash between steps leaves both objects [^store-rs].
   - Add: stream verification (`open_reader` / `get_stream` with `expected_sha256`) raises on the final read; earlier chunks were already yielded.
   - Add: SQLSpec 0.71.1 pipelines still encode whole Arrow payloads until the upstream direct-write hook lands (see `research/upstream-sqlspec-direct-arrow-write.md` in this flow).
   - Update Caller Guidance and Planned Fixes to match; keep the truth block within 40 lines; add `[^]` footnotes and `sources` entries; refresh `verified`.
2. `object-storage.md`: drop the s3/r2 exception from the create-only bullet; add receipt (`PutResult.sha256`, `checksum`, `expected_sha256`, `IntegrityError`) and verified-read bullets; note `ObjectStoreBackend.supports_direct_arrow_write`.
3. `docs/usage/storage.md`: a "Content integrity" section with a short `put(..., checksum=True)` and `get(..., expected_sha256=...)` example and the `IntegrityError` contract; a note that `copy(..., overwrite=False)` is create-only on every backend.

## Bounded Read Manifest

- The three owned files plus `_store.py::_BaseObjectStore.verify_presigned_url` and `django/storage.py::DjangoStorage`.

## Symbol & Signature Contracts

- Documentation names: `PutResult.sha256`, `vertebra.typing.IntegrityError`, `checksum`, `expected_sha256`, `supports_direct_arrow_write`.

## Edge Cases & Error Semantics

- OKF truth block must stay ≤ 40 lines; move detail into body sections if needed.
- No internal finding identifiers in prose.

## Public-Seam Test Blueprint

- **Canonical gate**: `make docs` and `make review-check`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Docs build | `make docs` | exit `0` |
| Caveats present | `grep -c "NotImplementedError" object-storage-limitations.md` | `≥ 2` |
| Rename caveat | `grep -q "not atomic" object-storage-limitations.md` | match |
| No stale s3 exception | `grep -q "create-only .* not supported" object-storage.md` | no match |

## Verification

- `make docs`
- `make review-check`
- `grep -c NotImplementedError .agents/bundles/knowledge/domains/storage/object-storage-limitations.md`

## Acceptance Criteria

- [ ] Limitations chapter lists every caveat with source footnotes (R8).
- [ ] OKF chapter and usage docs describe create-only copy, receipts, verified reads, and direct Arrow writes (R8).

## Notes & Discoveries

- [2026-11-12 01:30] The limitations chapter at HEAD already records the s3/r2 create-only gap and names this flow under Planned Fixes; this task rewrites those entries to the delivered state.

[^limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Limitations.
[^presign-verify]: `src/py/vertebra/storage/_store.py::_BaseObjectStore` — `verify_presigned_url`.
[^django-times]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Time accessors.
[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `rename`.
