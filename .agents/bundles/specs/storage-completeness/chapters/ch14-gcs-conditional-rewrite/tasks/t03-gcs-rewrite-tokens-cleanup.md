---
type: Task
id: storage-completeness:ch14:t03
chapter_id: ch14-gcs-conditional-rewrite
title: Complete GCS rewrite tokens and return actual published generation
description: Complete GCS rewrite tokens and return actual published generation
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch14:t02
read_set:
- src/rs/vertebra-storage/src/gcs_publication.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/gcs_publication.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 180
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
  resource: src/rs/vertebra-storage/src/gcs_publication.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Complete GCS rewrite tokens and return actual published generation

## Objective & Scope Boundary

Complete GCS rewrite tokens and return actual published generation. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcs_publication.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/cloud_request.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/conditional.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Loop JSON rewrite until done=true, passing prior rewriteToken and identical pinned source/destination conditions each request. Bounded in-flight state contains token, generations, identifiers and metadata only, never payload. Parse final resource.etag and resource.generation into PutResult and returned ObjectMeta; avoid post-publication HEAD races. If token expires/fails, preserve atomicity and explicit error; source generation cleanup is conditional on owned generation so another object is never deleted. Cleanup stage on success, failed precondition, provider error and cancellation, retaining ownership until completion. Provider-side eventual cleanup limits are documented honestly.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Multi-request rewrite | Responses done=false token=t1, done=false token=t2, done=true resource(etag=final,generation=900) | All requests retain exact source/destination predicates; result final/900 |
| Competing stage object | Another object appears at same stage name after upload generation captured | Conditional cleanup deletes only owned generation or leaves explicit cleanup failure |
| Cancelled token loop | Cancel after first token | Stage cleanup owned and target is either old or fully committed; no false metadata result |

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

[^implementation]: `src/rs/vertebra-storage/src/gcs_publication.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
