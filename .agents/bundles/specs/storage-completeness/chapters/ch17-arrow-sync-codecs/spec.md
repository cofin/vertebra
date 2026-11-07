---
type: Spec
parent_flow: storage-completeness
chapter_id: ch17-arrow-sync-codecs
title: Bounded Arrow and Parquet Encoding and Sync Storage
description: Bounded Arrow and Parquet Encoding and Sync Storage
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
  resource: src/py/vertebra/storage/_arrow.py
  title: Grounded implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Bounded Arrow and Parquet Encoding and Sync Storage

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Bounded Arrow and Parquet Encoding and Sync Storage addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Implement bounded synchronous Arrow/Parquet codec helpers

Add private `storage/_arrow.py` as the deep optional-PyArrow boundary. `iter_input_batches(data, *, batch_size: int, schema: Any = None) -> Iterator[Any]` accepts Table, RecordBatchReader, iterable of RecordBatch or ArrowCursor.to_arrow_table(); slices every Table/oversized RecordBatch into at most batch_size rows, preserves schema and consumes iterables lazily without list/to_pylist. A caller-owned Table may already materialize rows; no whole encoded input file is materialized. Private draining sink has write/tell/flush/close with a deque of bounded encoded chunks and `drain() -> Iterator[memoryview]`; sinks hand each batch's emitted bytes to native multipart before accepting the next batch. PyArrow IPC stream writer preserves schema/dictionaries/replacement across batches; ParquetWriter keeps footer state and writes rowgroups with caller compression/schema. Validate positive batch/rowgroup size before upload. Missing PyArrow raises existing MissingDependencyError. No hand-written Arrow FlatBuffer or Parquet metadata parser.

### R2: Expose synchronous Arrow/Parquet read and write helpers

Expose exact SyncObjectStore APIs: write_arrow(key,data,*,batch_size=65536)->ObjectMeta; iter_arrow_batches(key)->Iterator[RecordBatch]; read_arrow(key)->Table; write_parquet(key,batches,*,schema=None,compression="zstd",row_group_size=65536)->ObjectMeta; iter_parquet_batches(key,*,batch_size=65536,columns:Sequence[str]|None=None)->Iterator[RecordBatch]; read_parquet(key,*,columns=None)->Table. Delegate optional dependency/codec detail to _arrow.py. Native multipart upload receives encoder chunks incrementally and aborts on producer/encode/publish failure; native returned metadata remains authoritative. Sync reads wrap owned SyncReadableFile in pyarrow.ipc.open_stream or pq.ParquetFile(pre_buffer=False).iter_batches, closing helper-owned reader on close/error; caller store stays open. Table-returning convenience APIs may materialize output batches but never pre-read entire encoded file. Signatures and reader lifetime are documented in Google docstrings; public helper implementation stays thin.

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
| R1 | [ch17:t01](tasks/t01-sync-arrow-codecs.md) | behavior_tdd | `src/py/tests/unit/storage/test_storage_store.py` |
| R2 | [ch17:t02](tasks/t02-sync-public-helpers.md) | behavior_tdd | `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch17:t01 — Implement bounded synchronous Arrow/Parquet codec helpers (tasks/t01-sync-arrow-codecs.md)
- [ ] storage-completeness:ch17:t02 — Expose synchronous Arrow/Parquet read and write helpers (tasks/t02-sync-public-helpers.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch17-arrow-sync-codecs` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 2 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/storage/_arrow.py` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
