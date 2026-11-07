---
type: Spec
parent_flow: storage-completeness
chapter_id: ch18-arrow-async-bridge
title: Bounded PyArrow Worker and Native Async I/O Bridge
description: Bounded PyArrow Worker and Native Async I/O Bridge
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

# Bounded PyArrow Worker and Native Async I/O Bridge

<!-- truth: start -->
This chapter restores an observable storage contract at the existing `5fe477fa` functional baseline. [^source]
Production work stays within each task's explicit ownership. Raw keys, conditional publication, metadata and cleanup are correctness contracts.
A completed implementation still requires correctness review followed by fresh quality review.
<!-- truth: end -->

## 1. Specification, Context & Provenance

Bounded PyArrow Worker and Native Async I/O Bridge addresses confirmed storage contract gaps. The concrete source and canonical tests are bounded in each worksheet. The baseline is available even while ch01:t01 is reopened; tasks depend only on explicitly listed predecessor artifacts. Use project-local pyo3-bridge/runtime-ipc/python-framework skills according to owned language and the storage domain knowledge chapter.

## 2. Requirements, Concrete Contracts & Architecture

Grounded contract evidence: [^research].

### R1: Bridge synchronous PyArrow reads to owning-loop native async ranges

Add private `AsyncRangeBridge` created on an explicit owning asyncio loop with AsyncObjectStore, key, object_size and max_cache_bytes=8 MiB. Its worker-only seekable `read(n)`, readinto, seek and tell callbacks schedule await store.get_range on that loop via asyncio.run_coroutine_threadsafe, never SyncObjectStore I/O. A bounded LRU stores at most 8 MiB of fetched range bytes; a required PyArrow consumer read may return its requested range with an explicit bytes copy, but never a whole-object prefetch/list/mmap spool. Use pq.ParquetFile(metadata=optional parsed footer,pre_buffer=False). Set Parquet iter_batches/read_row_group use_threads=False so each per-reader job owns sequential callbacks rather than sharing mutable seek state across Arrow worker threads. IPC reader uses the same bounded callback bridge and PyArrow handles dictionaries natively.

Private `AsyncReaderJob` owns one pyarrow reader/generator and one in-flight per-batch job. `async next_batch() -> RecordBatch | None` starts one asyncio.to_thread job inside _arrow.py only; worker executes next(reader) and returns one batch, so a paused async consumer reserves no blocked worker. No worker waits for a producer queue between jobs. All asyncio operations remain on the captured owning loop; a call from another loop raises RuntimeError before I/O. `async aclose()` flips a local closing flag, prevents new requests, cancels/tracks in-flight native futures, awaits worker completion and closes reader. Local state/locks are per bridge; no Python/global process lock. A caller-owned AsyncObjectStore is never closed.

### R2: Expose incremental async IPC and Parquet batch readers

Expose AsyncObjectStore.iter_arrow_batches(key)->AsyncIterator[RecordBatch], read_arrow(key)->Table, iter_parquet_batches(key,*,batch_size=65536,columns=None)->AsyncIterator[RecordBatch], and read_parquet(key,*,columns=None)->Table. Thin _store.py methods delegate to _arrow.py AsyncReaderJob; _store.py contains no asyncio.to_thread and performs no synchronous PyArrow/cloud reader operations on the event loop. Metadata HEAD and get_range run natively async. Each anext awaits exactly one worker batch job; explicit aclose(), contextlib.aclosing exit, decode error and cancellation await bridge cleanup. A bare async-for break does not automatically close a Python async generator; document explicit closing for early termination and do not promise cleanup before aclose/finalization. Parquet projection is passed into PyArrow and sparse reads fetch only footer/needed column data according to actual PyArrow requests; no eager list(reader), whole input bytes or Table conversion in iterator. Table conveniences materialize only requested output. IPC dictionaries/replacements and zero-batch streams remain native PyArrow responsibilities.

### R3: Encode async batches with bounded multipart backpressure and cleanup

Expose AsyncObjectStore.write_arrow and write_parquet with sync helper signatures and awaitable ObjectMeta results; accept sync or async batch iterables. Private AsyncWriterJob performs only one schema-init/write_batch/close job at a time in _arrow.py worker threads. Its synchronous sink schedules await native multipart.write_part for bounded encoded chunks on captured owner loop; each callback waits for actual upload completion, enforcing producer backpressure without retaining whole encoded file. Schema/dictionary/ParquetWriter state stays owned across jobs but no worker remains reserved while waiting for the next consumer-provided batch. Table input slices into bounded batches, never to_pylist. Capture loop explicitly and reject foreign-loop use. On cancellation, stop new batches and I/O, signal callback closure, await in-flight worker/native futures and idempotent upload.abort before propagating cancellation; no dangling MPU or helper-owned reader. Close/write failure preserves original error and caller-owned store remains open. _store.py has zero asyncio.to_thread and no PyArrow network callback on event-loop thread.

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
| R1 | [ch18:t01](tasks/t01-async-range-bridge.md) | behavior_tdd | `src/py/tests/unit/storage/test_storage_store.py` |
| R2 | [ch18:t02](tasks/t02-async-read-helpers.md) | behavior_tdd | `src/py/tests/unit/storage/test_storage_store.py` |
| R3 | [ch18:t03](tasks/t03-async-write-helpers.md) | behavior_tdd | `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [ ] storage-completeness:ch18:t01 — Bridge synchronous PyArrow reads to owning-loop native async ranges (tasks/t01-async-range-bridge.md)
- [ ] storage-completeness:ch18:t02 — Expose incremental async IPC and Parquet batch readers (tasks/t02-async-read-helpers.md)
- [ ] storage-completeness:ch18:t03 — Encode async batches with bounded multipart backpressure and cleanup (tasks/t03-async-write-helpers.md)

**Chapter Recovery Checkpoint:** run every task's focused verification, then correctness review and a fresh quality review before closing this chapter. `/flow:implement storage-completeness ch18-arrow-async-bridge` executes its ready worksheet.

## 6. Current State & Remaining Plan Snapshot

- Baseline: `5fe477fa`; implementation is pending and cold plan review is required.
- Remaining: all 3 tasks above; dependent tasks remain ordered by depends_on.
- Documentation: `[0/1 files documented]`, owned by the explicit consolidation tasks.
- Reclosure of ch01 and downstream acceptance waits for native repair gates; this chapter never depends on that reclosure.

[^source]: `src/py/vertebra/storage/_arrow.py` — Verified repository implementation seam.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
