---
type: Task
id: storage-completeness:ch14:t04
chapter_id: ch14-gcs-conditional-rewrite
title: Publish staged GCS generations and return canonical XML receipts
description: Publish staged GCS generations and return canonical XML receipts
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch14:t03
read_set:
- src/rs/vertebra-storage/src/gcs_stage.rs
- src/rs/vertebra-storage/src/gcs_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
- src/rs/vertebra-storage/src/cloud_request.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
- src/rs/vertebra-storage/src/gcs_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
estimated_loc: 240
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
  resource: src/rs/vertebra-storage/src/gcs_publication.rs
  title: Owned native implementation
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: SDK and primary provider protocol evidence
tags:
- storage
- repair
---

# Publish staged GCS generations and return canonical XML receipts

## Objective & Scope Boundary

Implement this bounded native contract after its listed predecessors. Own only the declared production and canonical test files. Preserve public signatures, caller ownership, lazy dependencies and provider error identity. Create a signed conventional source commit after verification.

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/gcs_stage.rs`
- `src/rs/vertebra-storage/src/gcs_publication.rs`
- `src/rs/vertebra-storage/src/conditional.rs`
- `src/rs/vertebra-storage/src/lib.rs`
- `src/rs/vertebra-storage/src/cloud_request.rs`
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs`

## Symbol & Signature Contracts

Add private `GcsPublicationUpload` implementing the existing native MultipartUpload boundary using the completed GcsResumableStage. Activate `mod gcs_stage;` by removing its exact test-only gate and register `mod gcs_publication;` in lib.rs. Wire the existing GCS PublicationBackend into this wrapper, retaining the same SDK store, credential provider and resolved transport/configuration. Remove every exact field-local `#[cfg_attr(not(test), expect(dead_code, reason = "consumed by ch14:t04 GCS publication"))]` introduced by ch21:t01 in conditional.rs as the real production wrapper begins reading GcsStageConfig and its GCS variant field. Normal non-test cargo check and all-target Clippy must pass in this activation commit; no private helper can remain unused behind test compilation.

Finish the stage and retain its exact source generation. Execute authenticated JSON objects.rewrite with sourceGeneration and source-generation precondition pinned, raw destination encoded once, and destination ifGenerationMatch=0 for Create or the same verified SDK HEAD generation for Match/NotMatch. Evaluate caller ETags against XML SDK metadata. A conflicting destination generation rejects safely; no HEAD-to-unconditional-overwrite or implicit full-upload retry is allowed. Include caller metadata/content_type in the rewrite body. Overwrite uses the provider's normal atomic rewrite behavior.

Loop until done=true, passing the prior rewriteToken and IDENTICAL source/destination predicates on every continuation. State contains only token/generations/identifiers/metadata, never object payload. Parse the final destination generation, then retrieve its canonical XML receipt using SDK `get_opts(path, GetOptions { head: true, version: Some(destination_generation), ..Default::default() })`. Return that exact XML ETag/version; JSON resource.etag is never returned as the public ETag. A competitor after rewrite cannot change this result. If that generation is unavailable, return an explicit provider/indeterminate receipt failure; never substitute an unfenced HEAD or successor metadata. Cleanup still targets only the owned stage generation. Preserve cleanup ownership across provider errors and cancellation, and preserve the original failure if cleanup also fails. Raw listings may observe temporary cloud stages; no listing-invisibility promise or reserved cloud keyspace is introduced.

## Edge Cases & Error Semantics

Retain session, pending frame and exact generation ownership across errors and cancellation. Preserve the original failure if cleanup also fails. Never replay consumed producer input, buffer the complete object, delete an unfenced object, or expose a session URI, bearer token, raw response body or credential-bearing error source chain.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Read/publication configuration identity | Public SDK read followed by native multipart publication through separately configured explicit endpoint, singleton path fallback and singleton inline-key fallback fixtures; default headers/HTTP policy and rotating SDK credential supplier | Actual read, resumable initiation and rewrite requests reach the same configured origin with identical required transport headers; the same SDK credential provider refreshes, skip-signature performs no lookup, and an empty bearer is omitted. Dual-source rejection remains constructor acceptance in ch21:t01. |
| Multi-request publication | done=false/t1, done=false/t2, done=true generation=900 | Every request keeps exact source/destination predicates; no source bytes are recopied in process. |
| XML versus JSON receipt | Distinct JSON/XML ETags and a competitor just after rewrite | Public receipt is generation-900 XML ETag, usable for subsequent native if_match; successor is never reported as our result. |
| Missing published generation | Generation-pinned XML receipt lookup fails | Explicit provider/indeterminate receipt error; no fabricated success or successor token. |
| Destination race | Competitor wins after expected ETag/generation resolution | Provider precondition rejects and competitor's exact bytes survive. |
| Metadata and cleanup | Typed metadata with source replacement or cancelled token loop | Destination metadata persists, cleanup deletes only owned stage generation, and cancellation retains abort ownership. |

## Verification

Record the failing regression and passing public native multipart result at this task's actual boundary. This task owns the full actual SDK-read-versus-publication HTTP comparison; ch21:t01 owns only constructor/read/captured-config acceptance.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --locked`

Record exact exit statuses and native checkpoint. Use the shared SHM lock for canonical engine tests. The publication activation task additionally runs normal non-test `cargo check`.

## Acceptance Criteria

- [ ] Every matrix row passes through its declared real boundary.
- [ ] Ownership stays within read_set <= 6, production files <= 3, and implementation LOC <= 250.
- [ ] No public API, auth fork, full-input replay or whole-object buffer is introduced.
- [ ] Signed source commit and verification evidence are recorded; documentation uses ch09:t03 and ch04:t03 after native acceptance.

## Notes & Discoveries

The executable frontier awaits independent review. A contradictory SDK or provider contract routes through `/flow:revise storage-completeness` before implementation.

[^implementation]: `src/rs/vertebra-storage/src/gcs_publication.rs` — Owned native implementation.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — SDK and provider protocol evidence.
