---
type: Task
id: storage-completeness:ch09:t03
chapter_id: ch09-r2-docs-and-knowledge
title: Record provider atomicity, key identity, cleanup and R2 withdrawal
description: Record provider atomicity, key identity, cleanup and R2 withdrawal
status: draft
state: pending
priority: P1
verification_strategy: documentation_validation
depends_on:
- storage-completeness:ch16:t01
read_set:
- .agents/bundles/knowledge/domains/storage/object-storage.md
- src/py/tests/unit/tools/test_docs_tooling.py
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/oci.rs
- src/py/vertebra/storage/_store.py
files:
- .agents/bundles/knowledge/domains/storage/object-storage.md
tests:
- src/py/tests/unit/tools/test_docs_tooling.py
estimated_loc: 180
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
  resource: .agents/bundles/knowledge/domains/storage/object-storage.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Record provider atomicity, key identity, cleanup and R2 withdrawal

## Objective & Scope Boundary

Record provider atomicity, key identity, cleanup and R2 withdrawal. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `.agents/bundles/knowledge/domains/storage/object-storage.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/tools/test_docs_tooling.py` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

## Symbol & Signature Contracts

Rewrite storage knowledge source provenance and truth blocks around actual supported backends and OCI-owned uniform chunking. Remove r2.rs source/footnote/table entries, R2 signing and configuration claims. Document raw path identity, typed PreconditionError, SDK transport/refresh reuse, provider-specific conditional publication, local cooperating Vertebra process guarantee with uncooperative direct OS writers excluded, cancellation cleanup ownership, and evidence-backed supported platform behavior. No 5 TiB guarantee or universal local metadata guarantee is invented. Evidence is native/public tests, not plan assertions.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Bounded truth/provenance | Validate object-storage.md links and source footnotes | All source symbols exist; truth block <=40 lines and reflects tests |
| Platform guarantee | Document local subprocess races and OS behavior actually tested | No unsupported cross-process/platform claim |

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
- [ ] This knowledge file is the owned documentation manifest item; user guides stay with ch04:t03.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `.agents/bundles/knowledge/domains/storage/object-storage.md` — Current repository source for this task.
