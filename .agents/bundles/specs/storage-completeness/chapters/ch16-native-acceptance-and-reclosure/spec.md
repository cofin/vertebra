---
type: Spec
parent_flow: storage-completeness
chapter_id: ch16-native-acceptance-and-reclosure
title: Native Acceptance and Original Chapter Reclosure
description: Native Acceptance and Original Chapter Reclosure
status: draft
state: planned
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 6
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

# Native Acceptance and Original Chapter Reclosure

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Native Acceptance and Original Chapter Reclosure addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Validate provider races, raw identity, metadata and cleanup at native seams

Run the complete native storage acceptance matrix after provider repair tasks and ch20:t01. Ordinary negative-predicate byte puts and multipart publication are separate mandatory lanes, including the real S3/Azure forbidden-version barriers and actual OCI conditional headers. Add only missing public-seam regressions in canonical files; a source-text check or mocked ordinary put cannot substitute for multipart publication races. Real Memory and Local store behavior must run; S3/Azure/GCS/OCI HTTP capture fixtures must enforce actual completion predicates, metadata, multipart IDs, token/error responses and abort calls. Use deterministic barriers for races, independent processes for Local, cancellation barriers for cleanup, and return exact typed conflicts across PyO3. Rebuild native extension and prove stub parity, lazy imports and no first-class R2 APIs. Do not mark original ch01:t01 complete until this evidence and both independent reviews exist.

### R2: Reconcile original native and ORM task acceptance after repairs

After ch16:t01, ch20:t01 and ch06, run correctness review and then a fresh quality review of native repairs and original native chapter. Reconcile ch01:t01 to completed with repair acceptance commit/evidence, retain original baseline commit as provenance, and stamp ch01 last_reviewed_commit only at reviewed checkpoint. Then run complete canonical ORM/SQLSpec/Django suite and independent correctness plus fresh quality review before ch02 closes. Native-dependent original ch03/ch04 worksheets unblock only after these reviews; never erase preexisting implementation commit evidence or declare HTTP/Arrow complete merely because native tests pass. Root roadmap/catalog/state/checklist synchronization occurs atomically with review stamps. This coordination task does not depend on ch01 completed, avoiding a cycle.

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
| R1 | [ch16:t01](tasks/t01-native-contract-acceptance.md) | integration_acceptance | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R2 | [ch16:t02](tasks/t02-reclose-native-and-orm.md) | integration_acceptance | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch16:t01 — Validate provider races, raw identity, metadata and cleanup at native seams (tasks/t01-native-contract-acceptance.md)
- [ ] storage-completeness:ch16:t02 — Reconcile original native and ORM task acceptance after repairs (tasks/t02-reclose-native-and-orm.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch16-native-acceptance-and-reclosure` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 2 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/rs/vertebra-storage/src/store.rs` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/conditional-publication_20261107.md` — Source-grounded architecture and probe evidence.
