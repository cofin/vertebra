---
type: Task
id: "storage-integrity-and-streaming:ch03:t02"
flow_id: storage-integrity-and-streaming
chapter_id: ch03-wheel-isolation-and-okf-sync
title: "Update Object Storage User Guides and Changelog"
description: "Document CRC32C/MD5/SHA-256 receipts, verified reads, IntegrityError, create-only cloud copy, and direct Arrow writes in docs/usage/storage*.md and docs/changelog/index.md."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: documentation_validation
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch03:t01"
files:
  - docs/usage/storage.md
  - docs/usage/storage-parquet-and-orm.md
  - docs/changelog/index.md
tests: []
read_set:
  - docs/usage/storage.md
  - docs/usage/storage-parquet-and-orm.md
  - docs/changelog/index.md
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
estimated_loc: 150
tags:
  - docs
  - storage
  - arrow
  - changelog
sources:
  - id: docs-storage
    resource: docs/usage/storage.md
    title: "Object storage usage guide"
  - id: docs-parquet
    resource: docs/usage/storage-parquet-and-orm.md
    title: "Parquet, Arrow, and ORM storage guide"
  - id: docs-changelog
    resource: docs/changelog/index.md
    title: "Release changelog"
---

# Task `t02`: Update Object Storage User Guides and Changelog

## Objective & Scope Boundary

Update `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, and `docs/changelog/index.md` to cover all shipped integrity, create-only copy, streaming Arrow, and companion wheel isolation features per [`../spec.md` §2](../spec.md) [^docs-storage] [^docs-parquet] [^docs-changelog]. Out of scope: OKF chapters and the upstream SQLSpec research note (`t03`).

## Bounded Read Manifest

- `docs/usage/storage.md` — write/read, conditional copy, and integrity sections [^docs-storage].
- `docs/usage/storage-parquet-and-orm.md` — Arrow/Parquet, SQLSpec, Advanced Alchemy, and Django storage sections [^docs-parquet].
- `docs/changelog/index.md` — `Added:` / `Changed:` / `Fixed:` format conventions [^docs-changelog].
- `src/py/vertebra/storage/_store.py` and `src/py/vertebra/extensions/sqlspec/storage.py` — public symbols.

## Contracts & Edge Cases

- `docs/usage/storage.md`: add a concise "Content Integrity & Verified Reads" section demonstrating `put(..., checksum=True)` (`PutResult.{sha256,crc32c,md5}`), `put_stream(..., expected_sha256=...)`, `get(..., expected_sha256=...)`, and `vertebra.typing.IntegrityError` (noting distinction from `vertebra.db.IntegrityError`), plus create-only `copy(..., overwrite=False)` across S3/B2/R2.
- `docs/usage/storage-parquet-and-orm.md`: document `ObjectStoreBackend.supports_direct_arrow_write`, Advanced Alchemy `save_arrow` / `save_arrow_async`, `DjangoStorage` `content.sha256` opt-in, and the SQLSpec `<= 0.71.1` pipeline buffering caveat.
- `docs/changelog/index.md`: record exact class/method names under `Added:` / `Changed:` / `Fixed:` without redundant version-bump entries or external library comparisons.

## Test Matrix

- Verify `make docs` exits `0` (`scan_public_docstrings`, `zensical build --clean`, `postbuild`, `verify_docs_artifacts`) and `IntegrityError` + `supports_direct_arrow_write` appear in the updated guides.

## Verification

```bash
make docs
```

## Acceptance Criteria

- [ ] `docs/usage/storage.md`, `docs/usage/storage-parquet-and-orm.md`, and `docs/changelog/index.md` accurately document receipts, verified reads, direct Arrow streaming, and changelog entries (`R6`).
- [ ] `make docs` exits `0`.

[^docs-storage]: `docs/usage/storage.md` — Object storage usage guide.
[^docs-parquet]: `docs/usage/storage-parquet-and-orm.md` — Parquet, Arrow, and ORM storage guide.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
