---
type: Task
id: storage-completeness:ch14:t02
chapter_id: ch14-gcs-conditional-rewrite
title: Create generation-fenced GCS resumable stages
description: Create generation-fenced GCS resumable stages
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch10:t02
- storage-completeness:ch10:t03
- storage-completeness:ch14:t01
- storage-completeness:ch21:t01
read_set:
- src/rs/vertebra-storage/src/gcs_stage.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/gcs_stage.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/src/gcs_stage.rs
estimated_loc: 230
plan_revision: 7
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
  resource: src/rs/vertebra-storage/src/gcs_stage.rs
  title: Owned native implementation
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: SDK and primary provider protocol evidence
tags:
- storage
- repair
---

# Create generation-fenced GCS resumable stages

## Objective & Scope Boundary

Implement this bounded native contract after its listed predecessors. Own only the declared production and canonical test files. Preserve public signatures, caller ownership, lazy dependencies and provider error identity. Create a signed conventional source commit after verification.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcs_stage.rs`
- `src/rs/vertebra-storage/src/lib.rs`
- `src/rs/vertebra-storage/src/cloud_request.rs`
- `src/rs/vertebra-storage/src/conditional.rs`
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md`

## Symbol & Signature Contracts

Add the real private `gcs_stage.rs` under exactly `#[cfg(test)] mod gcs_stage;` until t04 activates its first production caller. No public test API, broad dead-code allowance or fake stage implementation is introduced. Inline `gcs_stage::tests` exercise actual CloudRequestClient/SDK HTTP and credential-provider collaborators against a local responder.

`GcsResumableStage` retains the concrete `Arc<GoogleCloudStorage>`, exact resolved GCS configuration, `Arc<CloudRequestClient>` and shared native async session state. `GcsStageConfig` carries `base: Url`, `bucket: String`, `client_options: ClientOptions`, and `skip_signature: bool` captured by ch21:t01. `StageReceipt { key: StorePath, generation: String, size: u64 }` identifies only the stage object. Stage methods `begin`, `write_chunk(Bytes)`, `finish() -> Result<StageReceipt, StorageError>` and `abort() -> Result<(), StorageError>` retain Active/Finalized ownership; t03 completes recovery/cleanup before runtime activation. Store pending data and the request offset in retained async state BEFORE sending, so dropping a future cannot lose them.

Initiate `POST {base}/upload/storage/v1/b/{bucket}/o?uploadType=resumable&name={stage}&ifGenerationMatch=0` with JSON contentType/metadata and exact body length, omitting X-Upload-Content-Length when total size is unknown. Use an unpredictable UUID key beside the destination's parent without reserving a legal user prefix. Encode opaque bucket/key segments exactly once. The zero-generation predicate is part of session initiation, never an unsupported XML MPU completion option. Retry a definitive initiation collision with a fresh UUID only before producer consumption. A late 412 fails safely without changing names or replaying consumed input.

Obtain the initiating bearer from the concrete SDK provider on each authenticated request; skip lookup entirely when skip_signature is true and omit Authorization for an empty bearer, matching the SDK. Use the configured SDK transport. Retain Location as an opaque authentication capability, resolve relative Location against the configured endpoint, and reject unexpected origin or an insecure downgrade before sending configured headers. Session requests need no OAuth bearer; never log the capability URI.

Frame bounded 8 MiB requests with non-final byte counts divisible by 256 KiB. Segmented PutPayload/Bytes may retain at most the current bounded frame plus a partial tail. Send Content-Range `bytes s-e/*` before EOF; final partial uses `bytes s-e/N`. For exact-frame EOF send an empty PUT with `bytes */N`; empty input uses `bytes */0`. Successful 200/201 responses yield validated bucket/name/generation/size. JSON ETag stays internal and never becomes the public predicate token. Provider maximum-size enforcement is preserved; no XML 10,000-part cap is imposed on resumable sessions.

## Edge Cases & Error Semantics

Retain session, pending frame and exact generation ownership across errors and cancellation. Preserve the original failure if cleanup also fails. Never replay consumed producer input, buffer the complete object, delete an unfenced object, or expose a session URI, bearer token, raw response body or credential-bearing error source chain.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Atomic stage creation | Existing stage name plus concurrent creator | Initiation/finalization predicate preserves that object; late conflict never restarts consumed input. |
| Unknown input length | Empty stream, partial tail, exact 8 MiB boundary, multiple frames | Actual responder sees exact Content-Length/Content-Range and full ordered bytes; bounded retained data. |
| Configuration | Custom HTTP endpoint/headers, raw percent and Unicode key, skip-signature and empty bearer | Actual request preserves configured transport and exact key identity; forbidden credential lookup is not performed. |
| Metadata | content_type and user attributes | Initiation body and final stage receipt match exact uploaded metadata/size/generation. |

## Verification

Record the failing behavioral regression and passing native result at this task's actual boundary. Test-only private primitives use real SDK transport and local HTTP responders; they make no premature public-engine publication claim.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcs_stage::tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

Record exact exit statuses and native checkpoint. Use the shared SHM lock for canonical engine tests. The publication activation task additionally runs normal non-test `cargo check`.

## Acceptance Criteria

- [ ] Every matrix row passes through its declared real boundary.
- [ ] Ownership stays within read_set <= 6, production files <= 3, and implementation LOC <= 250.
- [ ] No public API, auth fork, full-input replay or whole-object buffer is introduced.
- [ ] Signed source commit and verification evidence are recorded; documentation uses ch09:t03 and ch04:t03 after native acceptance.

## Notes & Discoveries

The executable frontier awaits independent review. A contradictory SDK or provider contract routes through `/flow:revise storage-completeness` before implementation.

[^implementation]: `src/rs/vertebra-storage/src/gcs_stage.rs` — Owned native implementation.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK and provider protocol evidence.
