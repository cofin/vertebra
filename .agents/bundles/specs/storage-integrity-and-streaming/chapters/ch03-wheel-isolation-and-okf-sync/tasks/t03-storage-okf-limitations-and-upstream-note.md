---
type: Task
id: "storage-integrity-and-streaming:ch03:t03"
flow_id: storage-integrity-and-streaming
chapter_id: ch03-wheel-isolation-and-okf-sync
title: "Synchronize Object Storage OKF Chapters Within 200 Lines and Draft Upstream SQLSpec Hook"
description: "Update object-storage.md (<= 200L) and object-storage-limitations.md (<= 200L) with receipts, verified reads, and caveats, and author research/upstream-sqlspec-direct-arrow-write.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch03:t02"
files:
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - .agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md
tests: []
read_set:
  - .agents/bundles/knowledge/domains/storage/object-storage.md
  - .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/django/storage.py
  - src/py/vertebra/extensions/sqlspec/storage.py
estimated_loc: 160
tags:
  - okf
  - storage
  - limitations
  - sqlspec
sources:
  - id: okf-storage
    resource: .agents/bundles/knowledge/domains/storage/object-storage.md
    title: "Object storage OKF chapter"
  - id: okf-limitations
    resource: .agents/bundles/knowledge/domains/storage/object-storage-limitations.md
    title: "Object storage limitations OKF chapter"
  - id: presign-verify
    resource: src/py/vertebra/storage/_store.py::_BaseObjectStore
    title: "verify_presigned_url raising NotImplementedError for gcs, azure, and oci"
  - id: django-times
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: "get_created_time and get_accessed_time raising NotImplementedError"
---

# Task `t03`: Synchronize Object Storage OKF Chapters Within 200 Lines and Draft Upstream SQLSpec Hook

## Objective & Scope Boundary

Synchronize `.agents/bundles/knowledge/domains/storage/object-storage.md` (`<= 200L`) and `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` (`<= 200L`) in present tense, and author `.agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md` (flagged for promotion to `.agents/bundles/research/upstream-sqlspec-direct-arrow-write.md` during `/flow:archive`) per [`../spec.md` §2](../spec.md) [^okf-storage] [^okf-limitations].

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/storage/object-storage.md` — architecture and adapter surface (`168L`) [^okf-storage].
- `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — verified caveats (`74L`) [^okf-limitations].
- `src/py/vertebra/storage/_store.py` — `_BaseObjectStore.verify_presigned_url` and `ObjectMeta`/`PutResult` [^presign-verify].
- `src/py/vertebra/extensions/django/storage.py` — `get_created_time` / `get_accessed_time` [^django-times].
- `src/py/vertebra/extensions/sqlspec/storage.py` — `ObjectStoreBackend.supports_direct_arrow_write`.

## Contracts & Edge Cases

- `object-storage.md` (`<= 200` lines, `<= 40`-line truth block): record CRC32C/MD5/SHA-256 receipts (`ObjectMeta`/`PutResult`), verified streaming reads, `vertebra.typing.IntegrityError`, `ObjectStoreBackend.supports_direct_arrow_write`, Advanced Alchemy `save_arrow*`, and `verify_storage_wheel.py` isolation gates.
- `object-storage-limitations.md` (`<= 200` lines, `<= 40`-line truth block): record `verify_presigned_url` `NotImplementedError` on `gcs`/`azure`/`oci` [^presign-verify], `DjangoStorage.{get_created_time,get_accessed_time}` `NotImplementedError` [^django-times], non-atomic copy-then-delete cloud `rename`, final-poll `VerifyingStream` error semantics, and SQLSpec `<= 0.71.1` pipeline Arrow payload buffering until upstream hook adoption.
- `research/upstream-sqlspec-direct-arrow-write.md`: valid OKF v0.2 `type: Research` (`status: stable`, expanded YAML `sources:` citing only repo-tracked files, zero `.venv` paths or standalone `---` body rules) containing the upstream `SyncStoragePipeline.write_arrow` / `AsyncStoragePipeline.write_arrow` `supports_direct_arrow_write` hook specification and promotion note for `.agents/bundles/research/` before `/flow:archive`.

## Test Matrix

- Assert `object-storage.md` and `object-storage-limitations.md` are both `<= 200` lines; assert `NotImplementedError` caveats appear in `object-storage-limitations.md`; run `make docs && make review-check`.

## Verification

```bash
python3 -c 'from pathlib import Path; [(_ for _ in ()).throw(AssertionError(f"{p}: {len(Path(p).read_text().splitlines())}L")) for p in (".agents/bundles/knowledge/domains/storage/object-storage.md", ".agents/bundles/knowledge/domains/storage/object-storage-limitations.md") if len(Path(p).read_text().splitlines()) > 200]' && make docs && make review-check
```

## Acceptance Criteria

- [ ] `object-storage.md` (`<= 200L`), `object-storage-limitations.md` (`<= 200L`), and `research/upstream-sqlspec-direct-arrow-write.md` pass all OKF v0.2 and privacy checks (`R6`).
- [ ] Zero regressions in `make docs && make review-check`.

[^okf-storage]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Object storage OKF chapter.
[^okf-limitations]: `.agents/bundles/knowledge/domains/storage/object-storage-limitations.md` — Object storage limitations OKF chapter.
[^presign-verify]: `src/py/vertebra/storage/_store.py::_BaseObjectStore` — `verify_presigned_url` raising `NotImplementedError` for `gcs`, `azure`, and `oci`.
[^django-times]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — `get_created_time` and `get_accessed_time` raising `NotImplementedError`.
