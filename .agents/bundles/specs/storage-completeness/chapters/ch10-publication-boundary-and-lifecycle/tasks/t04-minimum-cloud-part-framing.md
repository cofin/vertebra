---
type: Task
id: storage-completeness:ch10:t04
chapter_id: ch10-publication-boundary-and-lifecycle
title: Frame S3 producer chunks and preserve bounded backend policies
description: Frame S3 producer chunks and preserve bounded backend policies
status: stable
state: completed
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
- src/rs/vertebra-storage/src/store.rs
estimated_loc: 150
plan_revision: 7
plan_commit: 5f64d1a8
state_revision: 0
claimed_by: agent:minimum_part_framing
claimed_at: '2026-11-08T00:29:50Z'
blocked_reason: null
unblock_condition: null
next_step: Coordinator signs the verified source and binds its receipt.
last_verified_at: '2026-11-08T00:36:39Z'
last_verified_commit: fe992ae3
verification_evidence: |
  Public S3 responder regressions: before framing 3 failed and 1 passed (exit 101): EntityTooSmall on tiny chunks, early tail transmission, and sub-frame producer blocked on premature upload. The private real-session count regression also failed without count guards (exit 101).
  Green: canonical storage_engine_tests 39 passed (exit 0); full native --lib 10 passed (exit 0); exact --lib store::tests 1 passed (exit 0). Real HTTP captures show 8388608/8388608/3 and 8388608/8388608/17-byte framing, upload backpressure, preserved content type, empty/tail semantics, failed and cancelled part abort ownership, and zero disallowed frame/tail requests at injected count 10000.
  cargo fmt --all --check and cargo clippy -p vertebra-storage --all-targets -- -D warnings exit 0. Cargo cache lock is outer; SHM test lock is inner. No Python installed-native acceptance is claimed by this private native slice.
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-08T00:34:55Z'
commit: 85ed9f36
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

# Frame S3 producer chunks and preserve bounded backend policies

## Objective & Scope Boundary

Frame S3 producer chunks and preserve bounded backend policies. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/conditional.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/oci.rs` — collaborating contract.
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — collaborating contract.

## Symbol & Signature Contracts

Pinned S3 MultipartUpload::put_part forwards each caller PutPayload as one service part, requiring at least 5 MiB for each non-final part. MultipartUploadSession frames arbitrary S3 producer chunks with a private PutPayloadMut accumulator targeting 8 MiB. Slice incoming Bytes, retain only the current partial part and caller-owned input, and await each flush before consuming more producer data. Complete flushes a short final tail and handles an empty stream while preserving abort ownership. Enforce S3's 10000-part limit before sending a disallowed part; preserve OCI's separate owned 8 MiB/count policy. Azure/Local/Memory retain their native semantics. GCS conditional publication delegates framing to ch14's bounded 8 MiB JSON resumable stage with 256 KiB-aligned non-final requests and exact-frame EOF finalization; no XML multipart 10000-part cap applies to that session. This task does not require future GCS resumable public acceptance before its implementation. No whole-object buffer, unbounded in-flight tasks, consuming finish that loses abort ownership, maximum-object-size promise or single-PUT fallback is introduced.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the canonical integration target and actual public ObjectStoreEngine/MultipartUploadSession calls with a real S3 HTTP responder. The count-limit fixture is an inline store.rs test that constructs the actual private session counter at 10000 and sends the next part through the same live session path; its real upload/HTTP collaborator proves no disallowed request or publication. No public counter hook, 80 GiB fixture, mocked session or source-text assertion is introduced.

| Case | Input | Observable result |
| --- | --- | --- |
| Tiny chunks | S3 responder rejects nonfinal parts <5 MiB; producer yields 64 KiB chunks totaling2*8MiB+3 | Service receives 8 MiB,8MiB,3byte parts, exact reconstructed payload and original metadata |
| Large/irregular chunks | Caller yields3MiB,13MiB,17bytes and pauses uploads | Bounded framing emits8MiB parts incrementally; producer backpressure holds; no whole-stream accumulation |
| Tail/failure/limit | Empty stream, short final tail, part error and injected10001st part | Correct empty/tail semantics; abort remains callable; explicit limit error without publication |

## Verification

Record failing service-sized-part regressions before the fix, then run:

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Also run the private count-limit regression with `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-storage-cargo.lock flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib store::tests`.

Record exact part sizes, backpressure/cancellation barriers and exit statuses. No large payload allocation is needed for injected count-limit tests.

## Acceptance Criteria

- [x] Every matrix row passes at its public seam with existing behavior preserved.
- [x] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [x] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [x] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

S3 framing uses segmented PutPayloadMut views and awaits each 8 MiB flush before slicing additional caller input. The existing OCI accumulator and other backend policies remain unchanged. The count fixture constructs the actual S3 session, seeds only its private completed-part count, then exercises real frame/tail calls and HTTP abort; no public counter hook or large stream is required. Count limits prevent provider requests before publication. Error and cancellation tests retain the same abortable upload handle.

The executable transport staging and provider acceptance contracts are independently approved at 5f64d1a8. The bounded inline count-fixture clarification retains that identity.

[^implementation]: `src/rs/vertebra-storage/src/store.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK source and primary provider limits.
