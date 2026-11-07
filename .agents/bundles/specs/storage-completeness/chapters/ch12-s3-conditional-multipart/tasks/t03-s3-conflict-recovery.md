---
type: Task
id: storage-completeness:ch12:t03
chapter_id: ch12-s3-conditional-multipart
title: Surface S3 completion conflicts without hidden replay
description: Surface S3 completion conflicts without hidden replay
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch12:t02
read_set:
- src/rs/vertebra-storage/src/s3_publication.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/s3_publication.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 170
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
  resource: src/rs/vertebra-storage/src/s3_publication.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Surface S3 completion conflicts without hidden replay

## Objective & Scope Boundary

Surface S3 completion conflicts without hidden replay. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/s3_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/cloud_request.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`S3PublicationUpload::complete` performs direct conditional CompleteMultipartUpload. A known HTTP409 ConditionalRequestConflict is a typed PreconditionError/retryable CAS conflict with clear provider context; restore and retain the abortable upload handle, perform idempotent abort cleanup and return the conflict. Do not automatically re-initiate/replay a consumed unrewindable producer, stage/re-copy all S3 uploads, retain payloads, or broaden permissions. A caller may explicitly start a fresh operation with rewindable input. A412 fails without overwriting the competitor. An ambiguous transport outcome or HTTP200 response is never blindly retried: parse a complete 200 body for success/Error XML; retain cleanup ownership while reporting an honest indeterminate/provider error if its outcome cannot be established. Cleanup must never delete a competing committed target. The root adjudicates this direct-complete behavior; R1 makes no hidden automatic replay promise.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| 409 conflict | Complete returns 409; emulator requires new upload id for retry | Typed CAS conflict plus idempotent abort cleanup; no automatic replay or unconditional overwrite |
| 412 conflict | Complete returns 412 after competing write | No retry overwrites competitor |
| Cancellation in recovery | Cancel during failure cleanup after 409 | Owned upload abort cleanup retained; competing target intact |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/s3_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
