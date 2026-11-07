---
type: Spec
parent_flow: storage-completeness
chapter_id: ch15-oci-conditional-multipart
title: OCI Conditional Commit and Multipart Limits
description: OCI Conditional Commit and Multipart Limits
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
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# OCI Conditional Commit and Multipart Limits

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

OCI Conditional Commit and Multipart Limits addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Carry publication conditions into OCI multipart commit

Extend existing owned OciMultipartUpload to retain Create/Match/NotMatch condition and caller metadata from initiation through CommitMultipartUpload. Add native If-None-Match:* or If-Match headers to commit using existing OCI request signing/transport; OCI only accepts If-None-Match:*: arbitrary negative ETag resolves HEAD, rejects a forbidden match, then commits with If-Match(actual current ETag), or uses If-None-Match:* on absence. On CAS conflict re-evaluate the predicate at most8times, then return typed precondition conflict; never unchecked HEAD→overwrite. Parse provider commit ETag/version into PutResult. Preserve OCI Cavage signing, namespace/bucket/region/endpoint, part identifiers and caller attributes/content type. 409/412 map create versus typed update preconditions accurately. Keep abort handle on completion error and cancellation; no generic copy-create publication.

### R2: Enforce OCI uniform part sizing and explicit part-count failure

Keep OCI_UNIFORM_PART_SIZE exactly 8 MiB for all non-trailing parts. Enforce OCI maximum 10000 parts before sending a part that exceeds the provider limit; count zero-byte/short tail according to actual upload sequence. Return explicit InvalidInput/storage limit error and abort owned multipart upload without publishing. Do not claim a 5 TiB maximum that 8 MiB*10000 cannot reach, allocate 1 GiB buffers, resize uniform parts after parts are sent, or fall back to whole-object single PUT. Test the limit using injected counter/metadata fixtures rather than allocating 80 GiB.

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
| R1 | [ch15:t01](tasks/t01-oci-native-conditional-commit.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch15:t02](tasks/t02-oci-part-limits.md) | regression_tdd | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |

## 5. Implementation Plan

- [ ] storage-completeness:ch15:t01 — Carry publication conditions into OCI multipart commit (tasks/t01-oci-native-conditional-commit.md)
- [ ] storage-completeness:ch15:t02 — Enforce OCI uniform part sizing and explicit part-count failure (tasks/t02-oci-part-limits.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch15-oci-conditional-multipart` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 2 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/oci.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
