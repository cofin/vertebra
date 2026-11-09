---
type: Task
id: "storage-remediation:ch06:t01"
chapter_id: ch06-docs-okf-and-plan-sync
title: Update Zensical storage usage guides (storage.md, storage-auth-and-signing.md, storage-parquet-and-orm.md)
description: Document PreconditionError, bulk transfers (get_file, download_many, upload_many, staged_download), read-ahead open_reader, streaming open_writer, conditional copy, glob, server-side compose, Parquet dictionary/byte-stream-split options, explicit memory:// namespaces, shared token_cache=, and multiprocessing spawn guidance across docs/usage/storage*.md.
status: stable
state: in_progress
priority: P1
verification_strategy: documentation_validation
depends_on: []
read_set:
  - docs/usage/storage.md
  - docs/usage/storage-auth-and-signing.md
  - docs/usage/storage-parquet-and-orm.md
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/_arrow.py
files:
  - docs/usage/storage.md
  - docs/usage/storage-auth-and-signing.md
  - docs/usage/storage-parquet-and-orm.md
tests: []
estimated_loc: 180
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
  - id: doc-usage-storage
    resource: docs/usage/storage.md
    title: Zensical Object Storage usage guide
  - id: doc-usage-auth
    resource: docs/usage/storage-auth-and-signing.md
    title: Zensical Storage Authentication and Signed URLs guide
  - id: doc-usage-parquet
    resource: docs/usage/storage-parquet-and-orm.md
    title: Zensical Range Reads, Parquet, and ORM Attachments guide
tags:
  - storage
  - docs
  - zensical
---

# Task t01: Zensical Storage Usage Guides Update

## Objective & Scope Boundary

Update `docs/usage/storage.md` [^doc-usage-storage], `docs/usage/storage-auth-and-signing.md` [^doc-usage-auth], and `docs/usage/storage-parquet-and-orm.md` [^doc-usage-parquet] (`3` files):
1. In `docs/usage/storage.md`: document `PreconditionError` on failed `if_not_exists` / `if_match` / `if_none_match` / `copy(..., overwrite=False)` / `rename(..., overwrite=False)` preconditions, raw object key preservation, streaming `put_file` / `get_file`, bounded-concurrency `download_many` / `upload_many` / `staged_download`, `glob`, server-side GCS `compose` (with multipart fallback on other backends), `open_reader(..., read_ahead=4_194_304)`, `open_writer` (`SyncWritableFile` / `AsyncWritableFile`), `S3Config(virtual_hosted_style=...)`, `R2Config(custom_domain=...)`, `HTTPConfig(headers=...)`, Parquet `pre_buffer=True` default + `use_dictionary` / `use_byte_stream_split` / `options`, and `multiprocessing.get_context("spawn")` guidance when combining multi-process Python workers with native Tokio-backed object stores.
2. In `docs/usage/storage-auth-and-signing.md`: document C-ABI `PyCapsule` cross-extension `token_cache=` sharing across GCS data-plane operations, server-side `compose`, and IAM `signBlob`, plus custom `endpoint` support on GCS V4 and S3 `virtual_hosted_style` presigning.
3. In `docs/usage/storage-parquet-and-orm.md`: replace `SyncObjectStore.from_memory(...)` and `SyncObjectStore("memory://")` with `SyncObjectStore("memory://analytics")` and `SyncObjectStore("memory://documents")`, document `read_ahead` on `open_reader` and `write_parquet(..., use_dictionary=..., use_byte_stream_split=...)`, and show `SigningConfig` / `token_cache` forwarding on Advanced Alchemy and SQLSpec `ObjectStoreBackend`.

Out of scope: `docs/reference/storage.md` and OKF bundle files (`t02`).

## Bounded Read Manifest

- `docs/usage/storage.md` — Object storage usage guide [^doc-usage-storage].
- `docs/usage/storage-auth-and-signing.md` — Storage authentication and signed URLs guide [^doc-usage-auth].
- `docs/usage/storage-parquet-and-orm.md` — Parquet, range reads, and ORM attachments guide [^doc-usage-parquet].
- `src/py/vertebra/storage/_store.py` — Authoritative Python store and config signatures.
- `src/py/vertebra/storage/_arrow.py` — Authoritative Arrow/Parquet reader and writer signatures.

## Symbol & Signature Contracts

- Every code block uses public imports from `vertebra.storage`, `vertebra.typing`, `vertebra.extensions.alchemy.storage`, `vertebra.extensions.sqlspec.storage`, `vertebra.extensions.litestar`, or `vertebra.extensions.django`.
- Any package installation command uses synchronized `=== "uv"`, `=== "pip"`, `=== "pdm"`, `=== "poetry"` tabs.
- Zero contributor `make *` commands appear in `docs/usage/*.md`.

## Edge Cases & Error Semantics

- Bare `"memory://"` without a namespace raises `ImproperConfigurationError`; all examples pass an explicit namespace (`"memory://documents"` or `MemoryConfig(namespace="...")`).
- Forking a process after initializing Tokio runtimes (`os.fork` / `fork` start method) is unsafe across native threads; examples show `multiprocessing.get_context("spawn")` and constructing `SyncObjectStore` / `AsyncObjectStore` inside worker processes.

## Public-Seam Test Blueprint

- **Verification strategy**: `documentation_validation` (`make docs && make docs-clean` plus privacy validation).
- **Worked Check Matrix**:

| Check | Target | Expected Result |
| --- | --- | --- |
| Zensical site build & link check | `make docs && make docs-clean` | Exits `0` with zero broken links or warnings. |
| Privacy & contributor command check | `uv run --no-sync python -m tools.scripts.review_setup validate --rules privacy` | Exits `0` with zero violations. |

## Verification

```bash
make docs && make docs-clean
uv run --no-sync python -m tools.scripts.review_setup validate --rules privacy
```

## Acceptance Criteria

- [ ] `docs/usage/storage.md`, `docs/usage/storage-auth-and-signing.md`, and `docs/usage/storage-parquet-and-orm.md` accurately document all remediated and newly added storage APIs with zero stale `memory://` or `pre_buffer=False` claims (R6.1).
- [ ] `make docs && make docs-clean` and privacy validation exit `0` (R6.1).

## Notes & Discoveries

- [2026-11-08 20:55] Keep code examples concise and omit internal `frozen=True, gc=False` boilerplate in user-facing snippets.

[^doc-usage-storage]: `docs/usage/storage.md` — Zensical Object Storage usage guide.
[^doc-usage-auth]: `docs/usage/storage-auth-and-signing.md` — Zensical Storage Authentication and Signed URLs guide.
[^doc-usage-parquet]: `docs/usage/storage-parquet-and-orm.md` — Zensical Range Reads, Parquet, and ORM Attachments guide.
