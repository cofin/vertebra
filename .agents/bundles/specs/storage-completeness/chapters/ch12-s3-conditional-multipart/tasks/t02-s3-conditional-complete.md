---
type: Task
id: storage-completeness:ch12:t02
chapter_id: ch12-s3-conditional-multipart
title: Publish S3 multipart with conditional completion and actual result
description: Publish S3 multipart with conditional completion and actual result
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch12:t01
read_set:
- src/rs/vertebra-storage/src/s3_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/s3_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 220
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

# Publish S3 multipart with conditional completion and actual result

## Objective & Scope Boundary

Publish S3 multipart with conditional completion and actual result. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/s3_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/cloud_request.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`S3PublicationUpload::complete` sends CompleteMultipartUpload ordered PartNumber/ETag XML using exact SDK PartId content_id values and If-None-Match:* for Create or If-Match:<expected> for Match. S3 accepts only `If-None-Match: *`, never an arbitrary forbidden tag. For NotMatch(tag), HEAD current state, reject equality, then fence completion with If-Match(current ETag), or Create when absent. A safe 412 retry re-evaluates the logical predicate and reuses the MPU only where provider behavior proves that upload ID remains valid; bound to eight attempts and return a typed CAS conflict on exhaustion, without claiming the forbidden tag matched. Known409 remains the no-replay failure contract of t03. Never use HEAD→unconditional overwrite or blindly retry ambiguous transport/200 outcomes. Parse response body even on 200 and reject embedded Error XML. Parse actual returned ETag/version into PutResult; do not synthesize final etag via a later racy HEAD. Preserve init metadata and caller content_type. 412 maps typed Precondition (Create conflicts AlreadyExists where required). Never publish through generic copy-if-not-exists because it loses update semantics/results/metadata.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Create race | Competing object appears after multipart init, before completion | Conditional complete fails; competing bytes/metadata intact |
| Stale Match | ETag changes before complete | If-Match sent; PreconditionError mapping; no overwrite |
| HTTP200 error | Complete returns 200 with Error XML | Failure recognized and abort ownership retained; no false success |
| Actual metadata/result | Successful conditional complete returns ETag and version | Metadata retained and returned result equals response, not racy HEAD |

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
