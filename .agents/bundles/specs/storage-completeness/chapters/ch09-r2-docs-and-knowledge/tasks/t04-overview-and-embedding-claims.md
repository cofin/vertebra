---
type: Task
id: storage-completeness:ch09:t04
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 from repository and embedding overviews
description: Remove R2 from repository and embedding overviews
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch09:t01
read_set:
- README.md
- docs/reference/index.md
- docs/usage/embeddings-and-vectors.md
- src/py/tests/unit/tools/test_docs_tooling.py
- docs/usage/storage.md
files:
- README.md
- docs/reference/index.md
- docs/usage/embeddings-and-vectors.md
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
  resource: README.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from repository and embedding overviews

## Objective & Scope Boundary

Remove R2 from repository and embedding overviews. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `README.md` — owned implementation or canonical regression surface.
- `docs/reference/index.md` — owned implementation or canonical regression surface.
- `docs/usage/embeddings-and-vectors.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.
- `docs/usage/storage.md` — collaborating contract.

## Symbol & Signature Contracts

Remove first-class R2 from the README storage capability list, reference navigation configuration list, and model-bundle staging provider list. Preserve all remaining providers and examples. Keep changelog history intact; the detailed storage guide belongs to ch04:t03.

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
- [ ] The README, reference index and embedding guide are the three owned documentation manifest items.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `README.md` — Current repository source for this task.
