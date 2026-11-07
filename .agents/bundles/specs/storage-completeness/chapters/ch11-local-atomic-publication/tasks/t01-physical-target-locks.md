---
type: Task
id: storage-completeness:ch11:t01
chapter_id: ch11-local-atomic-publication
title: Add persistent OS locks per physical parent directory
description: Add persistent OS locks per physical parent directory
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch10:t01
- storage-completeness:ch10:t03
- storage-completeness:ch10:t04
read_set:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/store.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 200
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

# Add persistent OS locks per physical parent directory

## Objective & Scope Boundary

Add persistent OS locks per physical parent directory. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/local_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/lib.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Private `local_publication.rs` introduces LocalPublication and RAII DirectoryGuard. Every physical destination parent directory uses the persistent SDK-illegal filename `.vtb-cas-lock#0` inside that directory. Use native OS cross-process File::lock/unlock semantics, not a Python/process mutex or OS-temp hash/registry. A single parent-directory lock serializes all leaf spellings including case/Unicode aliases without reimplementing filesystem name equivalence. Canonicalize/open the physical parent; two-directory mutations deduplicate and sort stable physical directory identities (Unix device/inode; Windows volume/file ID) before acquisition, with deterministic ordering on macOS/Linux/Windows. Overlapping LocalConfig roots and symlink aliases therefore converge on the same physical lockfile. Acquire lockfiles using SDK-normal create/umask access modes and existing-file read-lock compatibility; do not require extra user-managed infrastructure or chmod policy. Real permission/unsupported-filesystem errors propagate clearly. Never unlink/recreate persistent lockfiles: that introduces lock ABA. Stage payload outside the lock; acquire only for destination predicate, atomic publication and actual resulting metadata. t03 routes every cooperating Local mutation through this seam; direct uncooperative OS writers remain outside the guarantee.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Overlapping roots | Independent processes use LocalConfig(/data) key a/b and LocalConfig(/data/a) key b | Same physical parent .vtb-cas-lock#0 serializes them |
| Two-key ordering | Concurrent copy/rename a→b and b→a with subprocess barrier | Physical-directory-ID order/dedupe prevents deadlock; exact data/conflicts asserted |
| Native platform behavior | Run lock contention on supported Unix/macOS/Windows CI | Blocking/exclusion verified; no process mutex substituted |

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
