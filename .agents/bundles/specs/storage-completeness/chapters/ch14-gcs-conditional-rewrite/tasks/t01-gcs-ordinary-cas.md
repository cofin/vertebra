---
type: Task
id: storage-completeness:ch14:t01
chapter_id: ch14-gcs-conditional-rewrite
title: Resolve GCS ETag predicates into generation CAS for ordinary puts
description: Resolve GCS ETag predicates into generation CAS for ordinary puts
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch10:t01
- storage-completeness:ch13:t02
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 140
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
  resource: src/rs/vertebra-storage/src/store.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Resolve GCS ETag predicates into generation CAS for ordinary puts

## Objective & Scope Boundary

Resolve GCS ETag predicates into generation CAS for ordinary puts. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

For GCS `put_opts(if_match=etag)`, HEAD once to obtain both opaque ETag and generation, verify expected ETag, then PutMode::Update(UpdateVersion { e_tag: Some(actual_etag), version: Some(generation) }). Never pass ETag in place of generation and never use HEAD→Overwrite. For arbitrary negative if_none_match: HEAD current object; if absent use Create; if present with matching forbidden ETag fail; otherwise Update with actual generation. Retry only CAS races, repeating predicate evaluation with new HEAD; bound retries explicitly (8 attempts, then precondition/retryable conflict) and never switch to unconditional mode. `if_none_match="*"` uses Create directly. Preserve content_type/attributes and actual PutResult from successful atomic put.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| GCS ETag/generation distinct | HEAD ETag abc, generation 1729; if_match abc | UpdateVersion uses generation 1729; caller ETag checked exactly |
| Negative race | Forbidden ETag old; HEAD sees allowed new1; competitor publishes forbidden old before PUT | CAS fails, re-evaluation rejects; forbidden value never overwritten |
| Absent negative | HEAD missing then competitor creates before Create | Create conflict triggers safe predicate re-evaluation or explicit failure, no overwrite |

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

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
