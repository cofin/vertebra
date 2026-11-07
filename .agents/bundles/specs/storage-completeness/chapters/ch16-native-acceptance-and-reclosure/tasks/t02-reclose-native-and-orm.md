---
type: Task
id: storage-completeness:ch16:t02
chapter_id: ch16-native-acceptance-and-reclosure
title: Reconcile original native and ORM task acceptance after repairs
description: Reconcile original native and ORM task acceptance after repairs
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
- storage-completeness:ch16:t01
- storage-completeness:ch06:t01
- storage-completeness:ch06:t02
- storage-completeness:ch05:t02
- storage-completeness:ch07:t03
read_set:
- .agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/spec.md
- .agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/tasks/t01-rust-streaming-and-ops.md
- .agents/bundles/specs/storage-completeness/chapters/ch02-orm-and-sql-storage-contracts/spec.md
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/tests/unit/extensions/test_django_plugin.py
- src/rs/vertebra-storage/src/store.rs
files:
- .agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/spec.md
- .agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/tasks/t01-rust-streaming-and-ops.md
- .agents/bundles/specs/storage-completeness/chapters/ch02-orm-and-sql-storage-contracts/spec.md
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/tests/unit/extensions/test_django_plugin.py
estimated_loc: 60
plan_revision: 6
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
  resource: .agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/spec.md
  title: Owned implementation seam
tags:
- storage
- repair
---

# Reconcile original native and ORM task acceptance after repairs

## Objective & Scope Boundary

Reconcile original native and ORM task acceptance after repairs. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `.agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/spec.md` — owned implementation or canonical regression surface.
- `.agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/tasks/t01-rust-streaming-and-ops.md` — owned implementation or canonical regression surface.
- `.agents/bundles/specs/storage-completeness/chapters/ch02-orm-and-sql-storage-contracts/spec.md` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_django_plugin.py` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.

## Symbol & Signature Contracts

After ch16:t01 and ch06, run correctness review and then a fresh quality review of native repairs and original native chapter. Reconcile ch01:t01 to completed with repair acceptance commit/evidence, retain original baseline commit as provenance, and stamp ch01 last_reviewed_commit only at reviewed checkpoint. Then run complete canonical ORM/SQLSpec/Django suite and independent correctness plus fresh quality review before ch02 closes. Native-dependent original ch03/ch04 worksheets unblock only after these reviews; never erase preexisting implementation commit evidence or declare HTTP/Arrow complete merely because native tests pass. Root roadmap/catalog/state/checklist synchronization occurs atomically with review stamps. This coordination task does not depend on ch01 completed, avoiding a cycle.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Review gate | Focused native tests green but correctness/quality not both completed | Original ch01 remains active; no review stamp invented |
| Reclosure | Both reviews pass and adapter regressions green | ch01/ch02 review stamps identify actual reviewed commits; original task IDs/commits preserved |
| Downstream unblock | ch01/ch02 closed with evidence | ch03 and ch04:t02 pending with blocked reasons cleared; no implementation claimed |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/extensions/test_django_plugin.py`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py src/py/tests/unit/extensions/test_django_plugin.py -q`
- `make review-check`
- `make docs`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] Own native/ORM review evidence and state reconciliation; user and knowledge documentation stay assigned to ch04/ch09.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `.agents/bundles/specs/storage-completeness/chapters/ch01-native-engine-and-auth/spec.md` — Current repository source for this task.
