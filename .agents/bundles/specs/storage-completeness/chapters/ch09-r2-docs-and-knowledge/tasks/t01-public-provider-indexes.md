---
type: Task
id: storage-completeness:ch09:t01
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 claims from public navigation and reference
description: Remove R2 claims from public navigation and reference
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch07:t03
- storage-completeness:ch08:t03
read_set:
- docs/index.md
- docs/usage/index.md
- docs/reference/storage.md
- src/py/tests/unit/tools/test_docs_tooling.py
- docs/usage/storage.md
files:
- docs/index.md
- docs/usage/index.md
- docs/reference/storage.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 60
plan_revision: 5
plan_commit: bdc7799ab18b9a961dacf99f5eee9af46a557aa3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: docs/index.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 claims from public navigation and reference

## Objective & Scope Boundary

Remove R2 claims from public navigation and reference. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `docs/index.md` — owned implementation or canonical regression surface.
- `docs/usage/index.md` — owned implementation or canonical regression surface.
- `docs/reference/storage.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.
- `docs/usage/storage.md` — collaborating contract.

## Symbol & Signature Contracts

Remove first-class R2 entries from object-storage provider tables and reference configuration list. Preserve S3 custom endpoint wording and every remaining provider. The user guide's detailed behavior is owned by ch04:t03, not duplicated here. Keep API reference symbols matching installed exported classes.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Provider surface | Build docs and inspect storage references | No R2Config/r2:// supported-provider entry; S3/GCS/Azure/OCI/local/memory/http remain |

## Verification

Validate the owned documentation against the exported API and completed native evidence. Record the documentation build and focused validation results; documentation-only edits do not require an artificial failing public-call regression. Use the verified native build checkpoint from the completed R2 removal tasks without rebuilding unchanged extensions.

- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/tests/unit/tools/test_docs_tooling.py`
- `flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`
- `make review-check`
- `make docs`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] These three public documents are the owned documentation manifest items.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `docs/index.md` — Current repository source for this task.
