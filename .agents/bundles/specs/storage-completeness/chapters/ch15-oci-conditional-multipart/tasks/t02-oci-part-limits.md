---
type: Task
id: storage-completeness:ch15:t02
chapter_id: ch15-oci-conditional-multipart
title: Enforce OCI uniform part sizing and explicit part-count failure
description: Enforce OCI uniform part sizing and explicit part-count failure
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch15:t01
read_set:
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/src/store.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/src/store.rs
tests:
- src/rs/vertebra-storage/src/oci.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 100
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
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Enforce OCI uniform part sizing and explicit part-count failure

## Objective & Scope Boundary

Enforce OCI uniform part sizing and explicit part-count failure. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/oci.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/src/store.rs` — owned implementation or canonical regression surface.
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — owned implementation or canonical regression surface.

- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Keep OCI_UNIFORM_PART_SIZE exactly 8 MiB for all non-trailing parts. Enforce OCI maximum 10000 parts before sending a part that exceeds the provider limit; count zero-byte/short tail according to actual upload sequence. Return explicit InvalidInput/storage limit error and abort owned multipart upload without publishing. Do not claim a 5 TiB maximum that 8 MiB*10000 cannot reach, allocate 1 GiB buffers, resize uniform parts after parts are sent, or fall back to whole-object single PUT. Test the private next-part boundary inside inline `oci::tests`, where the actual private OciMultipartUpload state is accessible, rather than adding a public test hook or allocating 80 GiB. Construct the actual upload with a real OciObjectStore transport, an owned upload ID tracked by a local HTTP responder, `next_part` set to 10000/10001 and minimal metadata; call its real MultipartUpload trait methods. For 10001, assert the limit error before any UploadPart request, then exercise the retained real abort handle against that owned ID and verify the committed target bytes stay unchanged. The high-level automatic abort-on-error contract remains verified by ch10 cleanup tests. Do not fabricate an error, mock a client/abort method, or attempt private-field mutation from the external integration-test crate. Existing public engine framing/payload assertions remain in storage_engine_tests.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Uniform parts | Irregular producer chunks total 2*8 MiB+17 | Part sizes 8 MiB,8 MiB,17 and exact reconstructed payload |
| Private part limit | Inline oci::tests constructs actual upload at next_part=10001 with a tiny payload and actual HTTP-owned session ID | Error before UploadPart HTTP, retained handle aborts its owned upload through real transport, committed target remains exact bytes. |
| Private legal boundary | Inline actual upload next_part=10000 sends a tiny final part and completes against the real local responder | Last legal index is accepted once; commit contains exact recorded part ID/ETag and returns the actual provider result. |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib oci::tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/rs/vertebra-storage/src/oci.rs` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
