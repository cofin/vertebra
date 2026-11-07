---
type: Task
id: storage-completeness:ch11:t03
chapter_id: ch11-local-atomic-publication
title: Guard every local mutation and publish with native atomic operations
description: Guard every local mutation and publish with native atomic operations
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch11:t02
- storage-completeness:ch10:t03
read_set:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/local_publication.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 240
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

# Guard every local mutation and publish with native atomic operations

## Objective & Scope Boundary

Guard every local mutation and publish with native atomic operations. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/local_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Route ordinary put/put_opts/put_file/delete/copy/rename and multipart final publication through per-physical-parent DirectoryGuards. Stage payload outside locks. Acquire source/destination physical parent directory guards in t01's deduplicated deterministic order for two-directory mutations; same-directory operations acquire only once. Under the held guard evaluate Create/Match/NotMatch against actual current target metadata and perform native atomic publication. Create uses native no-replace hard-link/create semantics; Match/NotMatch/Overwrite use same-filesystem atomic rename under the guard. Obtain actual resulting metadata/ETag before releasing the guard so returned results cannot describe a competing later object. Delete/rename/copy publication also participates; no ordinary write bypasses the guard. Never invoke unsupported LocalFileSystem PutMode::Update then fall back to unconditional overwrite. Preserve original target until successful publication and retain abort/stage ownership on error. Local SDK's actual content_type/attribute capability is documented honestly rather than fabricating persistence. Direct noncooperating OS writers are outside the guarantee. Lock acquisition executes off async runtime worker threads when it can block; no Python/global process lock or process mutex fallback.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Local create race | Two subprocesses stream same absent destination and synchronize before publish | Exactly one Create succeeds; loser FileExistsError; complete winning payload |
| Stale update | Start Match(etag0), independent process puts v1 before completion | Precondition conflict; v1 exact bytes remain |
| Every mutation cooperates | Pause conditional publish guard and race ordinary put/delete/copy/rename from another process | Second operation cannot bypass guard; no torn publication |

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
