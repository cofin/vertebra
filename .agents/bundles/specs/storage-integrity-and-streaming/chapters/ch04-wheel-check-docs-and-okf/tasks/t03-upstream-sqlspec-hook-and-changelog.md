---
type: Task
id: "storage-integrity-and-streaming:ch04:t03"
chapter_id: ch04-wheel-check-docs-and-okf
title: Draft the upstream SQLSpec hook and update changelog and Arrow docs
description: Write the upstream SQLSpec issue and PR text for a direct Arrow write hook in SyncStoragePipeline/AsyncStoragePipeline.write_arrow, add changelog entries for every user-visible change in this flow, and document direct Arrow writes in the Parquet and ORM storage guide.
status: draft
state: pending
priority: P2
verification_strategy: docs
depends_on:
  - "storage-integrity-and-streaming:ch04:t02"
read_set:
  - .venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py
  - docs/changelog/index.md
  - docs/usage/storage-parquet-and-orm.md
  - src/py/vertebra/extensions/sqlspec/storage.py
files:
  - .agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md
  - docs/changelog/index.md
  - docs/usage/storage-parquet-and-orm.md
tests: []
estimated_loc: 140
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
  - id: upstream-pipeline
    resource: .venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py
    title: SyncStoragePipeline.write_arrow, write_arrow_batches, _write_bytes; AsyncStoragePipeline.write_arrow (sqlspec 0.71.1)
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: supports_direct_arrow_write and write_arrow_sync/async
  - id: issue-100
    resource: https://github.com/cofin/vertebra/issues/100
    title: Streamed Arrow writes from SQLSpec pipelines
tags:
  - docs
  - sqlspec
  - upstream
---

# Task t03: Draft the Upstream SQLSpec Hook and Update Changelog and Arrow Docs

## Objective & Scope Boundary

1. Create `research/upstream-sqlspec-direct-arrow-write.md` (OKF frontmatter: `type: Research`, `status: draft`, `sources`) containing:
   - Issue title `feat(storage): let backends receive Arrow tables directly in storage pipelines` and body: current behavior (`SyncStoragePipeline.write_arrow` and `AsyncStoragePipeline.write_arrow` call `_encode_arrow_payload` then `_write_bytes`, buffering the full file) [^upstream-pipeline]; proposal (when the resolved backend has `supports_direct_arrow_write = True`, call `backend.write_arrow_sync(path, table, format_hint=..., compression=..., write_options=...)` / `await backend.write_arrow_async(...)` and build telemetry rows/bytes from the returned metadata); compatibility (additive, defaults to existing path); acceptance (a table larger than 10× part size keeps peak buffered memory bounded; telemetry matches; injected writer failure leaves the prior object).
   - PR description draft and a test outline for `tests/unit/storage/test_pipeline.py` upstream.
   - Tracking: comment text for [#100](https://github.com/cofin/vertebra/issues/100) retitling it to `feat(sqlspec): add direct Arrow write hook to SQLSpec storage pipelines` and noting backend streaming landed in [#101](https://github.com/cofin/vertebra/pull/101).
2. `docs/changelog/index.md`: under the unreleased section, `Added:` SHA-256 write receipts (`PutResult.sha256`, `checksum`, `expected_sha256`), verified reads, `vertebra.typing.IntegrityError`, `ObjectStoreBackend.supports_direct_arrow_write`, `make storage-wheel-check`; `Fixed:` create-only `copy`/`rename` on S3/B2 and R2; `Changed:` `put_multipart` streams parts.
3. `docs/usage/storage-parquet-and-orm.md`: a short section on direct Arrow writes, what SQLSpec versions use them, and that older pipelines still buffer.

Out of scope: filing the upstream issue (delivery is user-driven).

## Bounded Read Manifest

- Upstream `pipeline.py` write paths [^upstream-pipeline]; `docs/changelog/index.md` structure; `docs/usage/storage-parquet-and-orm.md`; `ObjectStoreBackend` [^sqlspec-backend].

## Symbol & Signature Contracts

- Upstream symbols cited exactly: `SyncStoragePipeline.write_arrow`, `AsyncStoragePipeline.write_arrow`, `_encode_arrow_payload`, `_write_bytes`.

## Edge Cases & Error Semantics

- The draft must not reference internal tooling or this repository's planning files.
- Changelog omits version bump lines.

## Public-Seam Test Blueprint

- **Canonical gate**: `make docs`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Docs build | `make docs` | exit `0` |
| Draft present | `test -f .agents/bundles/specs/storage-integrity-and-streaming/research/upstream-sqlspec-direct-arrow-write.md` | exit `0` |
| Changelog | `grep -q "IntegrityError" docs/changelog/index.md` | match |

## Verification

- `make docs`
- `make review-check`

## Acceptance Criteria

- [ ] Upstream SQLSpec hook is drafted as an explicit external dependency (R9).
- [ ] Changelog and Arrow docs describe the delivered behavior (R8).

## Notes & Discoveries

- [2026-11-12 01:30] SQLSpec 0.71.1 is the installed version at HEAD; the hook does not exist upstream.

[^upstream-pipeline]: `.venv/lib/python3.12/site-packages/sqlspec/storage/pipeline.py` — Upstream pipeline.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Backend.
[^issue-100]: [#100](https://github.com/cofin/vertebra/issues/100) — Streamed Arrow writes.
