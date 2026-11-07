---
type: Task
id: storage-completeness:ch16:t01
chapter_id: ch16-native-acceptance-and-reclosure
title: Validate provider races, raw identity, metadata and cleanup at native seams
description: Validate provider races, raw identity, metadata and cleanup at native seams
status: draft
state: pending
priority: P1
verification_strategy: integration_acceptance
depends_on:
- storage-completeness:ch05:t03
- storage-completeness:ch08:t03
- storage-completeness:ch11:t04
- storage-completeness:ch12:t03
- storage-completeness:ch13:t02
- storage-completeness:ch14:t03
- storage-completeness:ch15:t02
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_storage_store.py
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/oci.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 160
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

# Validate provider races, raw identity, metadata and cleanup at native seams

## Objective & Scope Boundary

Validate provider races, raw identity, metadata and cleanup at native seams. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — collaborating contract.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Run the complete native storage acceptance matrix after provider repair tasks. Add only missing public-seam regressions in canonical files; a source-text check or mocked ordinary put cannot substitute for multipart publication races. Real Memory and Local store behavior must run; S3/Azure/GCS/OCI HTTP capture fixtures must enforce actual completion predicates, metadata, multipart IDs, token/error responses and abort calls. Use deterministic barriers for races, independent processes for Local, cancellation barriers for cleanup, and return exact typed conflicts across PyO3. Rebuild native extension and prove stub parity, lazy imports and no first-class R2 APIs. Do not mark original ch01:t01 complete until this evidence and both independent reviews exist.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Cross-provider predicate matrix | Create, matching/stale Match, negative ETag, absent object for memory/local/S3/Azure/GCS/OCI | Atomic winner preservation; typed conflict and actual result metadata |
| Ownership matrix | Producer error, part failure, complete error, cancellation at publish | No unowned abort handle/stage and no partial target |
| Raw metadata identity | Unicode/%/%2F/%252F foreign SDK/local keys plus listing prefixes | Exact list→get bytes and prefix identity |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/tests/unit/storage/test_storage_store.py`
- `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q`

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
