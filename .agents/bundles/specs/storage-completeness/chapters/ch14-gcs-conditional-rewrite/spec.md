---
type: Spec
parent_flow: storage-completeness
chapter_id: ch14-gcs-conditional-rewrite
title: GCS Generation CAS and Staged Rewrite Publication
description: GCS Generation CAS and Staged Rewrite Publication
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 4
plan_commit: null
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
  resource: src/rs/vertebra-storage/src/store.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# GCS Generation CAS and Staged Rewrite Publication

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

GCS Generation CAS and Staged Rewrite Publication addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Resolve GCS ETag predicates into generation CAS for ordinary puts

For GCS `put_opts(if_match=etag)`, HEAD once to obtain both opaque ETag and generation, verify expected ETag, then PutMode::Update(UpdateVersion { e_tag: Some(actual_etag), version: Some(generation) }). Never pass ETag in place of generation and never use HEAD→Overwrite. For arbitrary negative if_none_match: HEAD current object; if absent use Create; if present with matching forbidden ETag fail; otherwise Update with actual generation. Retry only CAS races, repeating predicate evaluation with new HEAD; bound retries explicitly (8 attempts, then precondition/retryable conflict) and never switch to unconditional mode. `if_none_match="*"` uses Create directly. Preserve content_type/attributes and actual PutResult from successful atomic put.

### R2: Stage GCS multipart and publish through pinned JSON rewrite

Add GcsPublicationUpload retaining Arc<GoogleCloudStorage>, unique private MPU stage key, condition, caller attributes and resolved GCS JSON endpoint/transport. SDK multipart writes stage; complete obtains immutable source generation and ETag before publication. Execute JSON `objects.rewrite` with sourceGeneration pinned, destination ifGenerationMatch=0 (Create) or verified current generation (Match/negative predicate), and source precondition. Include metadata/body so attributes/content_type persist; GET/HEAD→overwrite or generic copy-create is forbidden. Use SDK credentials().get_credential() bearer and configured endpoint/transport, including emulator skip_signature behavior. Stage uses a unique unpredictable key with Create semantics and collision retry; never overwrite an existing user object. Track the exact owned source generation for rewrite and cleanup. No new reserved cloud keyspace or cross-process listing-invisibility promise is introduced; raw provider listings may observe temporary stages and docs describe that lifecycle honestly.

### R3: Complete GCS rewrite tokens and return actual published generation

Loop JSON rewrite until done=true, passing prior rewriteToken and identical pinned source/destination conditions each request. Bounded in-flight state contains token, generations, identifiers and metadata only, never payload. Parse final resource.etag and resource.generation into PutResult and returned ObjectMeta; avoid post-publication HEAD races. If token expires/fails, preserve atomicity and explicit error; source generation cleanup is conditional on owned generation so another object is never deleted. Cleanup stage on success, failed precondition, provider error and cancellation, retaining ownership until completion. Provider-side eventual cleanup limits are documented honestly.

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

Canonical public GCS ETags remain the existing SDK XML API ETag at head/list/put/multipart receipt boundaries. JSON resource ETags are a separate namespace and must never be compared with a caller XML ETag. Evaluate caller predicates against SDK XML HEAD metadata and use that same snapshot generation for destination fencing. After successful JSON rewrite, parse the returned destination generation, then obtain its exact XML receipt through SDK `get_opts(path, GetOptions { head: true, version: Some(destination_generation), ..Default::default() })`; the SDK emits the generation query. Return that generation-pinned metadata ETag/version, never an unfenced current HEAD or the JSON resource ETag. If that generation cannot be retrieved, report an explicit provider/indeterminate receipt failure and retain cleanup ownership; never substitute a successor token. A competing writer immediately after rewrite must not alter the returned successful publication identity.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented]`. Chapter behavior is recorded in `.agents/bundles/knowledge/domains/storage/object-storage.md` by ch09:t03 and user guides by ch04:t03; per-task tests are executable documentation. No separate untracked documentation obligation remains.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Strategy | Verification |
| --- | --- | --- | --- |
| R1 | [ch14:t01](tasks/t01-gcs-ordinary-cas.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch14:t02](tasks/t02-gcs-stage-rewrite.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3 | [ch14:t03](tasks/t03-gcs-rewrite-tokens-cleanup.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [ ] storage-completeness:ch14:t01 — Resolve GCS ETag predicates into generation CAS for ordinary puts (tasks/t01-gcs-ordinary-cas.md)
- [ ] storage-completeness:ch14:t02 — Stage GCS multipart and publish through pinned JSON rewrite (tasks/t02-gcs-stage-rewrite.md)
- [ ] storage-completeness:ch14:t03 — Complete GCS rewrite tokens and return actual published generation (tasks/t03-gcs-rewrite-tokens-cleanup.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch14-gcs-conditional-rewrite` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 3 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
