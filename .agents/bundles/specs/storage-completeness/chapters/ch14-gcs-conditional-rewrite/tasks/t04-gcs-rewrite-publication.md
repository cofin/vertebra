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
- src/rs/vertebra-storage/src/gcs_request.rs
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
files:
- src/rs/vertebra-storage/src/gcs_publication.rs
- src/rs/vertebra-storage/src/conditional.rs
- src/rs/vertebra-storage/src/lib.rs
tests:
- src/rs/vertebra-storage/tests/storage_engine_tests.rs
- src/rs/vertebra-storage/src/gcs_publication.rs
estimated_loc: 240
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
- `src/rs/vertebra-storage/src/gcs_request.rs`
- `src/rs/vertebra-storage/tests/storage_engine_tests.rs`

## Symbol & Signature Contracts

Add private `GcsPublicationUpload` implementing the existing native MultipartUpload boundary using the completed GcsResumableStage. Activate `mod gcs_stage;` by removing its exact test-only gate and register `mod gcs_publication;` in lib.rs. Wire the existing GCS PublicationBackend into this wrapper, retaining the same SDK store, credential provider and resolved transport/configuration. Remove every exact field-local `#[cfg_attr(not(test), expect(dead_code, reason = "consumed by ch14:t04 GCS publication"))]` introduced by ch21:t01 in conditional.rs as the real production wrapper begins reading GcsStageConfig and its GCS variant field. Normal non-test cargo check and all-target Clippy must pass in this activation commit; no private helper can remain unused behind test compilation.

Finish the stage and retain its exact source generation. Resolve destination predicates through the real GcsRequestClient snapshot tuple (quoted XML ETag, generation, metageneration). Match compares that ETag exactly; absent Match fails before rewrite. Present Match/NotMatch use BOTH JSON ifGenerationMatch=G and ifMetagenerationMatch=M; absent NotMatch/Create use ifGenerationMatch=0 with no metageneration. A race may fail with a truthful typed conflict; never use unconditional rewrite or replay consumed input. Metadata/content_type remain in the rewrite body. Source generation and source precondition stay pinned.

Loop rewrite until done=true, carrying identical source/destination conditions and prior rewriteToken. Capture final resource.generation AND resource.metageneration; never return resource.etag as the public ETag. Construct GcsObjectVersion from the returned G/M and call GcsRequestClient::head_snapshot(path, Some(&version)) for XML HEAD with generation=G query and both G/M match headers. Metadata-only replacement within the same generation must produce an explicit receipt failure, not a successor token; disappearance or unavailable generation also fails honestly. A successful receipt is the canonical quoted XML ETag for that exact snapshot. Owned stage cleanup still pins its data generation, not an arbitrary current object. No full-input retry, payload accumulation or false publication success is permitted.

Declare inline gcs_publication::tests in this owned module. Its real private wrapper constructor takes the shared Arc<GcsRequestClient> plus destination/options. For custom default headers and rotating external credential suppliers, build the actual SDK/config pair through pub(crate) store::build_gcs_store with a real GoogleCloudStorageBuilder, obtain the actual request client, execute SDK reads and drive the real wrapper MultipartUpload methods against a local responder. Only the external credential/service collaborator is substituted. Public engine integration separately covers exposed BackendConfig options; with_store Custom is not an injection substitute for GCS publication.

## Edge Cases & Error Semantics

Retain session, pending frame and exact generation ownership across errors and cancellation. Preserve the original failure if cleanup also fails. Never replay consumed producer input, buffer the complete object, delete an unfenced object, or expose a session URI, bearer token, raw response body or credential-bearing error source chain.

## Public-Seam Test Blueprint

| Case | Input | Observable result |
| --- | --- | --- |
| Read/publication configuration identity | Public SDK read followed by native multipart publication through separately configured explicit endpoint, singleton path fallback and singleton inline-key fallback fixtures; default headers/HTTP policy and rotating SDK credential supplier | Actual read, resumable initiation and rewrite requests reach the same configured origin with identical required transport headers; the same SDK credential provider refreshes, skip-signature performs no lookup, and an empty bearer is omitted. Dual-source rejection remains constructor acceptance in ch21:t01. |
| Multi-request publication | done=false/t1, done=false/t2, done=true generation=900 | Every request keeps exact source/destination predicates; no source bytes are recopied in process. |
| XML versus JSON receipt | Distinct JSON/XML ETags, successful G900/M1 receipt and separate metadata-only replacement to M2 before lookup | Success returns the quoted XML token for G900/M1; changed metadata produces explicit receipt failure, never a successor token. |
| Missing published generation | Generation-pinned XML receipt lookup fails | Explicit provider/indeterminate receipt error; no fabricated success or successor token. |
| Destination race | Competitor changes metadata/ETag at the same generation after snapshot | Rewrite carries both ifGenerationMatch and ifMetagenerationMatch; typed rejection preserves competitor data/metadata. |
| Absent negative predicate | Missing NotMatch succeeds, or competitor creates before rewrite; missing Match separately | NotMatch uses generation-match0, race rejects without Overwrite; absent Match makes no rewrite request. |
| Metadata and cleanup | Typed metadata with source replacement or cancelled token loop | Destination metadata persists, cleanup deletes only owned stage generation, and cancellation retains abort ownership. |

## Verification

Record the failing regression and passing public native multipart result at this task's actual boundary. This task owns the full actual SDK-read-versus-publication HTTP comparison; ch21:t01 owns only constructor/read/captured-config acceptance.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo fmt --manifest-path src/rs/Cargo.toml --all --check`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --test storage_engine_tests`
- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings`

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" cargo check --manifest-path src/rs/Cargo.toml -p vertebra-storage --locked`

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" flock /tmp/vertebra-flow-shm-tests.lock uv run --no-sync cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage --lib gcs_publication::tests`

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
