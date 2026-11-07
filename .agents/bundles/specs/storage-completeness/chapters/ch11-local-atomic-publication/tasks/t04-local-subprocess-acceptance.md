---
type: Task
id: storage-completeness:ch11:t04
chapter_id: ch11-local-atomic-publication
title: Prove local CAS across processes, roots and cleanup boundaries
description: Prove local CAS across processes, roots and cleanup boundaries
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
- storage-completeness:ch11:t03
read_set:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/local_publication.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 120
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
  resource: src/rs/vertebra-storage/src/local_publication.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Prove local CAS across processes, roots and cleanup boundaries

## Objective & Scope Boundary

Prove local CAS across processes, roots and cleanup boundaries. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/local_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/conditional.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Add canonical native subprocess regressions with independently launched child processes, distinct LocalConfig roots and different TMPDIR environments. Use barriers for stage readiness, competing write and final publication, not sleeps. `/root` key a/b and `/root/a` key b share the physical parent lock. Case/Unicode leaf aliases on supported case-insensitive filesystems share that same parent lock without a custom casefold. Opposing two-directory copy/rename uses physical-directory-ID dedupe/order and cannot deadlock. During upload, list/list_page/list_with_delimiter through narrow and broader overlapping roots must not expose `.vtb-cas-lock#0` or `.vertebra-stage-<uuid>#<counter>`; legal names such as keep#suffix remain visible and readable, while name#123 retains existing SDK unsupported semantics. Cancellation/abort removes the owned stage while the persistent lock remains. Assert actual postpublication metadata is collected under guard, permission errors remain real, and no world-writable temp root/registry or extra infrastructure is required. Record Linux/macOS/Windows evidence separately; direct OS writers remain excluded.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Physical identity | Parent /root a/b; child /root/a b; two concurrent Create uploads | One success and one FileExistsError; shared exact winner bytes |
| Changed ETag | Child ordinary put lands after staged update starts | Stale update rejects; child bytes retained |
| Artifact namespace | Legal keep#suffix exists while an SDK-illegal numeric-# stage is cancelled | Legal key listed/read; actual stage absent from list and removed after abort |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/local_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
