---
type: Spec
parent_flow: storage-completeness
chapter_id: ch19-sqlspec-arrow-streaming
title: SQLSpec Lazy Arrow and Parquet Integration
description: SQLSpec Lazy Arrow and Parquet Integration
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
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# SQLSpec Lazy Arrow and Parquet Integration

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

SQLSpec Lazy Arrow and Parquet Integration addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Replace SQLSpec eager async Arrow batch materialization

`ObjectStoreBackend._iter_arrow_async` resolves matching keys incrementally by existing metadata/list APIs and delegates each object to async_store.iter_parquet_batches or iter_arrow_batches with caller file_format/batch_size/columns. Remove asyncio.to_thread(lambda:list(self.stream_arrow_sync(...))) and never materialize all input batches or call sync_store on event loop. Preserve stream_arrow_async protocol and exact ArrowRecordBatch shape expected by installed SQLSpec; close delegated iterator in finally on consumer early termination/cancellation. Keep existing projection/pattern semantics and no-match empty iterator. read_arrow_async may materialize requested output Table as its protocol requires, but not encoded whole-file bytes.

### R2: Route SQLSpec table/cursor writes through bounded native streams

`ObjectStoreBackend.write_arrow_sync/write_arrow_async` and write_arrow_cursor_sync/write_arrow_cursor_async use sync/async store write_arrow/write_parquet, selected by existing file_format inference. Preserve installed SQLSpec call signatures, kwargs/projection behavior and existing explicit ArrowCursor buffer_view fast path when it already owns valid IPC bytes; do not first serialize a whole new Parquet file via BufferOutputStream or list batches. Existing caller Table/cursor may own output data; newly encoded bytes stream via native MPU with backpressure. Compression/schema/row_group_size kwargs pass through or raise clear validation errors, never ignored. Async methods use only async store helpers and await cleanup. Final original ch04:t02 integrates/accepts these APIs after ch04:t01 strict Arrow IPC prerequisite.

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
| R1 | [ch19:t01](tasks/t01-lazy-arrow-reader.md) | behavior_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R2 | [ch19:t02](tasks/t02-streaming-arrow-writers.md) | behavior_tdd | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch19:t01 — Replace SQLSpec eager async Arrow batch materialization (tasks/t01-lazy-arrow-reader.md)
- [ ] storage-completeness:ch19:t02 — Route SQLSpec table/cursor writes through bounded native streams (tasks/t02-streaming-arrow-writers.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch19-sqlspec-arrow-streaming` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 2 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/extensions/sqlspec/storage.py` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
