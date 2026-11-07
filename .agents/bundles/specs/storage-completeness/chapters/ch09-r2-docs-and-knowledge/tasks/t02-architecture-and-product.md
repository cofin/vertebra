---
type: Task
id: storage-completeness:ch09:t02
chapter_id: ch09-r2-docs-and-knowledge
title: Remove R2 from architecture and product capability claims
description: Remove R2 from architecture and product capability claims
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch07:t03
- storage-completeness:ch08:t03
read_set:
- docs/architecture/pyo3-and-workers.md
- .agents/bundles/product/product.md
- .agents/bundles/knowledge/domains/index.md
- src/py/tests/unit/tools/test_docs_tooling.py
files:
- docs/architecture/pyo3-and-workers.md
- .agents/bundles/product/product.md
- .agents/bundles/knowledge/domains/index.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 65
plan_revision: 4
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
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
commit: null
sources:
- id: implementation
  resource: docs/architecture/pyo3-and-workers.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from architecture and product capability claims

## Objective & Scope Boundary

Remove R2 from architecture and product capability claims. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `docs/architecture/pyo3-and-workers.md` — owned implementation or canonical regression surface.
- `.agents/bundles/product/product.md` — owned implementation or canonical regression surface.
- `.agents/bundles/knowledge/domains/index.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

Remove first-class R2 from native extension capability summaries, product supported-backend declarations and domain knowledge catalog. Keep HMAC, S3, Azure, GCS and OCI signing claims intact where implemented. Do not rewrite unrelated changelog history, email model labels or unrelated r2 variable substrings.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Capabilities reflect APIs | Search owned object-storage provider claims and build docs | No first-class R2 advertised; other capability descriptions retained |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/tests/unit/tools/test_docs_tooling.py`
- `uv run --no-sync pytest src/py/tests/unit/tools/test_docs_tooling.py -q`
- `make review-check`
- `make docs`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] These three architecture/product/catalog files are the owned documentation manifest items.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `docs/architecture/pyo3-and-workers.md` — Current repository source for this task.
