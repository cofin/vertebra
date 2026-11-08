---
type: Task
id: storage-completeness:ch14:t03
chapter_id: ch14-gcs-conditional-rewrite
title: Recover and clean up GCS resumable stage sessions
description: Recover and clean up GCS resumable stage sessions
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch14:t02
read_set:
- src/rs/vertebra-storage/src/gcs_stage.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/src/conditional.rs
- .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
files:
- src/rs/vertebra-storage/src/gcs_stage.rs
tests:
- src/rs/vertebra-storage/src/gcs_stage.rs
estimated_loc: 235
plan_revision: 8
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

# Recover and clean up GCS resumable stage sessions

## Objective & Scope Boundary

Implement this bounded native contract after its listed predecessors. Own only the declared production and canonical test files. Preserve public signatures, caller ownership, lazy dependencies and provider error identity. Create a signed conventional source commit after verification.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcs_stage.rs`
- `src/rs/vertebra-storage/src/cloud_request.rs`
- `src/rs/vertebra-storage/src/conditional.rs`
- `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md`

## Symbol & Signature Contracts

Keep gcs_stage test-gated until t04. Complete the real private stage's `probe` and `abort` lifecycle, bounded acknowledgement recovery, and cancellation-safe retained frame state. After interruption or 5xx, query the session using empty PUT with Content-Range `bytes */*`, or `bytes */N` after EOF. A 308 with no Range acknowledges zero bytes; `Range: bytes=0-k` acknowledges through k. Validate monotonic offsets against acknowledged bytes and the retained frame. Resend only its missing bytes; consume no further producer input before acknowledgement. Bound recovery to eight attempts, then return an explicit transport/indeterminate error with ownership retained. A 200/201 status response captures the validated final StageReceipt. 404/410, malformed/regressing/out-of-bounds Range and late 412 fail explicitly without a hidden producer rewind or new session.

Active-session cancellation sends DELETE with zero Content-Length; JSON cancellation success is HTTP 499. Finalized-stage cleanup sends generation-specific JSON DELETE with BOTH `generation=G` and `ifGenerationMatch=G`, never delete-by-name. If finalization was uncertain, probe before deciding which cleanup applies. If the session is expired/unreachable and no generation was captured, retain/report indeterminate cleanup ownership; a random name is insufficient proof to delete a later object. Repeated abort is idempotent only after ownership is actually resolved. Preserve the original operation error when cleanup also fails. Async state and guards retain pending frame/session/generation across a dropped send or finish future; cleanup retry uses that exact state. All diagnostics are capability-, body-, credential- and raw-source-chain-free.

## Edge Cases & Error Semantics

Retain session, pending frame and exact generation ownership across errors and cancellation. Preserve the original failure if cleanup also fails. Never replay consumed producer input, buffer the complete object, delete an unfenced object, or expose a session URI, bearer token, raw response body or credential-bearing error source chain.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Uncertain chunk | Connection breaks after partial persistence, probe returns 308 Range | Only retained unacknowledged bytes are resent; producer is not advanced or replayed. |
| Bad acknowledgement | Missing, regressing or beyond-frame Range; exhausted eight recoveries | Defined safe offset handling or explicit failure; no corrupted continuation. |
| Cancellation | Barrier cancels during send or finalization | Re-entered abort owns the exact session/frame; 499 cancellation or captured-generation deletion resolves it. |
| Stage replacement | Another object occupies stage name after final receipt | Cleanup targets only the owned generation and preserves replacement bytes. |
| Unknowable finalization | Expired/unreachable session without a captured generation | Indeterminate cleanup remains explicit; no guessed name-only deletion. |
| Privacy | Failure contains token-bearing Location/body | Public error omits URI, token, headers and untrusted body while retaining safe operation/status context. |

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
