---
type: Task
id: storage-completeness:ch14:t01
chapter_id: ch14-gcs-conditional-rewrite
title: Resolve GCS ETag predicates into generation CAS for ordinary puts
description: Resolve GCS ETag predicates into generation CAS for ordinary puts
status: stable
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch20:t01
- storage-completeness:ch10:t01
- storage-completeness:ch13:t02
- storage-completeness:ch21:t02
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/src/gcs_request.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 210
plan_revision: 8
plan_commit: c737ffeb
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

- `src/rs/vertebra-storage/src/store.rs`
- `src/rs/vertebra-storage/src/conditional.rs`
- `src/rs/vertebra-storage/src/lib.rs`
- `src/rs/vertebra-storage/src/gcs_request.rs`
- `src/rs/vertebra-storage/src/cloud_request.rs`
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs`

## Symbol & Signature Contracts

Activate `mod gcs_request;` by removing its exact test-only gate. The completed private request primitive is immutable in this task; own only store.rs, conditional.rs and lib.rs activation/routing. Construct and retain one shared GcsRequestClient per concrete GCS backend from the same SDK/config pair; do not rebuild credentials or an HTTP client per chunk/request. Remove exact future-config dead-code expectations when these fields gain their first real production consumer.

Implement the shared ConditionalWrite trait for GcsRequestClient with Snapshot=GcsObjectSnapshot, using its real snapshot/put_snapshot methods. Ordinary positive Match and negative NotMatch route through the SAME bounded logical-predicate CAS loop introduced by ch20; no GCS-only retry loop. Snapshot uses canonical quoted XML ETag plus generation/metageneration. Present writes fence both versions. Missing Match fails before PUT; missing NotMatch uses Create generation-match0. On a 412, capture a new tuple and re-evaluate the original ETag predicate, bounded to eight attempts; a metadata change that leaves the ETag unchanged may safely retry. An exhausted budget has truthful typed conflict context. Preserve metadata and actual mutation PutResult.

The generic SDK UpdateVersion path is forbidden for GCS ETag predicates because it ignores e_tag and cannot fence metadata-only changes. Overwrite and simple Create retain their supported native behavior. Public Python signatures and version/ETag namespaces remain unchanged. Rewrite and final receipt tests belong to t04 after the stage exists.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Positive metadata race | HEAD ETag A/G1729/M4; metadata changes to F/M5 at unchanged G | PUT carries paired G/M conditions, no unsafe success; predicate is re-evaluated within the shared budget. |
| Negative metadata race | if_none_match F; HEAD allowed A/G1729/M4, competitor installs F/M5 | Paired condition fails, fresh snapshot rejects F and competitor bytes/metadata survive. |
| Missing predicates | Missing Match; missing NotMatch with and without competing creation | Match rejects without PUT; NotMatch uses generation-match0 and never falls back to Overwrite. |
| Stable logical predicate | Metadata version changes but ETag remains the allowed value | Shared bounded CAS may retry the new paired snapshot; successful receipt is the actual PUT result. |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib`

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
