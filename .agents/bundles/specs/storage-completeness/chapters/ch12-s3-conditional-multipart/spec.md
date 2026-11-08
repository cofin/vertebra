---
type: Spec
parent_flow: storage-completeness
chapter_id: ch12-s3-conditional-multipart
title: S3 Metadata Multipart and Conditional Completion
description: S3 Metadata Multipart and Conditional Completion
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: storage-completeness:ch12:t01
plan_revision: 6
plan_commit: e3e344b5
state_revision: 0
last_verified_checkpoint: 5fe477fa
last_reviewed_commit: null
created_at: '2026-11-07T12:00:00Z'
updated_at: '2026-11-07T12:00:00Z'
generated:
  by: flow/0.28.0
  at: '2026-11-07T12:00:00Z'
verified: []
sources:
- id: source
  resource: src/rs/vertebra-storage/src/s3_publication.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# S3 Metadata Multipart and Conditional Completion

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

S3 Metadata Multipart and Conditional Completion addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Initiate S3 multipart with caller metadata and SDK part transport

Add private `S3PublicationUpload` holding Arc<AmazonS3>, resolved bucket/endpoint/region/client options, upload id, ordered Vec<PartId> (identifiers only), condition and attributes. Initiate multipart with the SDK transport boundary and fresh AwsAuthorizer credentials, carrying content_type and user attributes/tags in CreateMultipartUpload request. SDK public `MultipartStore::put_part(path,&id,part_idx,PutPayload)` uploads parts and `abort_multipart` cleans them; avoid hidden/private SDK methods. Metadata-bearing initialization returns real upload id; do not use default trait create_multipart when it loses metadata. Preserve normal/custom S3 endpoints and existing signing/HTTP options. This task prepares and tests initialization/part/abort independently before conditional completion.

This task activates the staged transport module: remove only the ch10:t02 `#[cfg(test)]` gate on `mod cloud_request;` in owned `lib.rs` as the first S3 initiation caller is wired. Keep all transport symbols crate-private and run normal non-test `cargo check` plus all-target Clippy; no test-only import, feature assumption, unconsumed helper or broad dead-code allowance may survive activation.

The wrapper's actual constructor accepts a retained concrete SDK store: `async S3PublicationUpload::begin(store: Arc<AmazonS3>, config: S3PublicationConfig, path: StorePath, options: PutMultipartOptions, condition: PublishCondition) -> Result<Self, ObjectStoreError>`, with private `S3PublicationConfig { bucket: String, region: String, endpoint: String, client_options: ClientOptions }`. Production dispatch and inline native tests use this same constructor. Inline tests build a real `AmazonS3Builder` with the configured local endpoint and `.with_credentials(rotating_provider)`; use `Arc<dyn CredentialProvider<Credential = AwsCredential>>` only as the external credential supplier, never replace the wrapper, HttpClient or SDK authorizer. Canonical engine tests exercise ordinary supported configuration. The actual initiation, SDK part uploads and abort must reach the same local responder; each request obtains the SDK's current credential, and the responder captures distinct access-key/token identities after rotation. A configured header/proxy exclusion and allow_http setting must reach initiation through the newly activated transport, not merely an unrelated ordinary SDK read. This assigns the end-to-end transport/refresh proof after the provider caller exists.

### R2: Publish S3 multipart with conditional completion and actual result

`S3PublicationUpload::complete` sends CompleteMultipartUpload ordered PartNumber/ETag XML using exact SDK PartId content_id values and If-None-Match:* for Create or If-Match:<expected> for Match. S3 accepts only `If-None-Match: *`, never an arbitrary forbidden tag. For NotMatch(tag), HEAD current state, reject equality, then fence completion with If-Match(current ETag), or Create when absent. A safe 412 retry re-evaluates the logical predicate and reuses the MPU only where provider behavior proves that upload ID remains valid; bound to eight attempts and return a typed CAS conflict on exhaustion, without claiming the forbidden tag matched. Known409 remains the no-replay failure contract of t03. Never use HEAD→unconditional overwrite or blindly retry ambiguous transport/200 outcomes. Parse response body even on 200 and reject embedded Error XML. Parse actual returned ETag/version into PutResult; do not synthesize final etag via a later racy HEAD. Preserve init metadata and caller content_type. 412 maps typed Precondition (Create conflicts AlreadyExists where required). Never publish through generic copy-if-not-exists because it loses update semantics/results/metadata.


### R3: Surface S3 completion conflicts without hidden replay

`S3PublicationUpload::complete` performs direct conditional CompleteMultipartUpload. A known HTTP409 ConditionalRequestConflict is a typed PreconditionError/retryable CAS conflict with clear provider context; restore and retain the abortable upload handle, perform idempotent abort cleanup and return the conflict. Do not automatically re-initiate/replay a consumed unrewindable producer, stage/re-copy all S3 uploads, retain payloads, or broaden permissions. A caller may explicitly start a fresh operation with rewindable input. A412 fails without overwriting the competitor. An ambiguous transport outcome or HTTP200 response is never blindly retried: parse a complete200 body for success/Error XML; retain cleanup ownership while reporting an honest indeterminate/provider error if its outcome cannot be established. Cleanup must never delete a competing committed target. The root adjudicates this direct-complete behavior; R1 makes no hidden automatic replay promise.

### R4: Declare native protocol and filesystem dependencies

Declare direct quick-xml0.39.4 with serialize support in vertebra-storage/Cargo.toml, using the already locked0.39.4 package from object_store's existing dependency graph. Update only vertebra-storage's dependency edge in Cargo.lock through Cargo's normal resolver; do not upgrade unrelated packages, rewrite PostgreSQL package entries or add an SDK fork. S3 CreateMultipartUpload/CompleteMultipartUpload and Azure block-list serialization/parsing reuse this maintained XML codec; HTTP200 Error bodies and XML entities/namespaces must be parsed structurally, not substring/string-split approximations. This task precedes S3 provider source imports. Preserve concurrent manifest/lock edits by other agents.

Caller → seam → collaborator: public storage/framework calls → owned deep module boundary → native SDK, transport or upstream framework. Public signatures remain stable except the deliberate removal of first-class R2 and addition of typed PreconditionError.

| Failure or edge | Required invariant |
| --- | --- |
| Competing writers | The winning object remains intact; stale CAS fails with typed PreconditionError or create-only FileExistsError. |
| Provider/transport error | Preserve the provider failure and cleanup ownership; no silent fallback or full-object buffering. |
| Optional dependency absent | Imports remain lazy and configured construction fails explicitly. |

| Alternative | Decision |
| --- | --- |
| Deep owned boundary with exact provider semantics | Chosen: hides credentials, staging and concurrency while preserving configured SDK behavior. |
| Generic head-then-write, Python lock, copy-create publication or blanket exception conversion | Rejected: loses atomicity, update results, metadata or error identity. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch12:t01](tasks/t01-s3-metadata-init.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch12:t02](tasks/t02-s3-conditional-complete.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3 | [ch12:t03](tasks/t03-s3-conflict-recovery.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

| R4 | [ch12:t04](tasks/t04-xml-dependency.md) | static_validation | `src/rs/vertebra-storage/Cargo.toml` and locked Cargo check |

## 5. Implementation Plan

Dependency order is t04 XML dependency, then t01 initiation, t02 completion and t03 conflict cleanup.

- [ ] storage-completeness:ch12:t01 — Initiate S3 multipart with caller metadata and SDK part transport (tasks/t01-s3-metadata-init.md)
- [ ] storage-completeness:ch12:t02 — Publish S3 multipart with conditional completion and actual result (tasks/t02-s3-conditional-complete.md)
- [ ] storage-completeness:ch12:t03 — Surface S3 completion conflicts without hidden replay (tasks/t03-s3-conflict-recovery.md)

- [x] [35a4f784] storage-completeness:ch12:t04 — Declare native protocol and filesystem dependencies (tasks/t04-xml-dependency.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch12-s3-conditional-multipart` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- The stable revision6 provider frontier is independently approved at `e3e344b5`. Shared ordinary negative-CAS and Local acceptance remain explicit prerequisites for provider activation.
- Native protocol/filesystem dependency edges are completed at `35a4f784`; all 572 locked package identities, versions, sources and checksums are preserved.
- Initiation, conditional completion and conflict cleanup remain pending in t01–t03. No S3 publication acceptance or chapter review stamp is claimed.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/s3_publication.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
