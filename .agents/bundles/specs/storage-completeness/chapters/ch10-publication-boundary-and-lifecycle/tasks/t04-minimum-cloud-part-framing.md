---
type: Task
id: storage-completeness:ch10:t04
chapter_id: ch10-publication-boundary-and-lifecycle
title: Frame arbitrary S3 and GCS producer chunks into valid bounded parts
description: Frame arbitrary S3 and GCS producer chunks into valid bounded parts
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch10:t03
read_set:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/oci.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/src/conditional.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 150
plan_revision: 5
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
  title: Primary provider minimum-part limits and SDK forwarding evidence
tags:
- storage
- repair
---

# Frame arbitrary S3 and GCS producer chunks into valid bounded parts

## Objective & Scope Boundary

Frame arbitrary S3 and GCS producer chunks into valid bounded parts. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — collaborating contract.

## Symbol & Signature Contracts

Pinned SDK S3/GCS MultipartUpload::put_part maps each caller PutPayload directly to one service part; both require at least 5 MiB for non-final parts. `MultipartUploadSession::write_chunk` must frame arbitrary producer chunks for S3/GCS using a private `PutPayloadMut` current-part accumulator with 8 MiB target. Slice incoming Bytes and retain segments without copying the full stream; fill/await exactly one upload.put_part before accepting more producer data. Retain only one partial part plus the current caller-owned input; no Vec/list of all parts or unbounded concurrent tasks. Existing OCI-owned 8 MiB UniformPartAccumulator remains the OCI policy; Azure/local/memory preserve native semantics unless their provider boundary explicitly opts into framing. `complete` flushes short final tail (and correct empty-object behavior), preserving abort ownership on failure; part_sizes_flushed reports actual service parts. Enforce provider's 10000-part limit before sending the disallowed part, abort safely and describe the effective fixed-part capacity honestly; no invented 5 TiB promise, 1 GiB buffers or single-PUT fallback. Do not adopt consuming WriteMultipart::finish unless handle ownership can survive its failure; the explicit rolling path is selected.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Tiny chunks | S3 and GCS emulators reject nonfinal parts <5 MiB; producer yields 64 KiB chunks totaling2*8MiB+3 | Service receives 8 MiB,8MiB,3byte parts, exact reconstructed payload and original metadata |
| Large/irregular chunks | Caller yields3MiB,13MiB,17bytes and pauses uploads | Bounded framing emits8MiB parts incrementally; producer backpressure holds; no whole-stream accumulation |
| Tail/failure/limit | Empty stream, short final tail, part error and injected10001st part | Correct empty/tail semantics; abort remains callable; explicit limit error without publication |

## Verification

Record failing service-sized-part regressions before the fix, then run:

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact part sizes, backpressure/cancellation barriers and exit statuses. No large payload allocation is needed for injected count-limit tests.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The executable transport staging and provider acceptance contracts await independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK source and primary provider limits.
