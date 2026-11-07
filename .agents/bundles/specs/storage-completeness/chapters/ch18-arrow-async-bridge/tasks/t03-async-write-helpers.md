---
type: Task
id: storage-completeness:ch18:t03
chapter_id: ch18-arrow-async-bridge
title: Encode async batches with bounded multipart backpressure and cleanup
description: Encode async batches with bounded multipart backpressure and cleanup
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch18:t01
- storage-completeness:ch17:t02
- storage-completeness:ch10:t03
read_set:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/vertebra/db/arrow.py
- .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
files:
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 240
plan_revision: 4
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
  resource: src/py/vertebra/storage/_store.py
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Encode async batches with bounded multipart backpressure and cleanup

## Objective & Scope Boundary

Encode async batches with bounded multipart backpressure and cleanup. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_arrow.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/db/arrow.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Expose AsyncObjectStore.write_arrow and write_parquet with sync helper signatures and awaitable ObjectMeta results; accept sync or async batch iterables. Private AsyncWriterJob performs only one schema-init/write_batch/close job at a time in _arrow.py worker threads. Its synchronous sink schedules await native multipart.write_part for bounded encoded chunks on captured owner loop; each callback waits for actual upload completion, enforcing producer backpressure without retaining whole encoded file. Schema/dictionary/ParquetWriter state stays owned across jobs but no worker remains reserved while waiting for the next consumer-provided batch. Table input slices into bounded batches, never to_pylist. Capture loop explicitly and reject foreign-loop use. On cancellation, stop new batches and I/O, signal callback closure, await in-flight worker/native futures and idempotent upload.abort before propagating cancellation; no dangling MPU or helper-owned reader. Close/write failure preserves original error and caller-owned store remains open. _store.py has zero asyncio.to_thread and no PyArrow network callback on event-loop thread.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Backpressure | Async producer logs each batch; gate native write_part | Producer cannot advance unboundedly; each current batch drains before next is read |
| Producer/cancellation failure | Raise at batch 2 or cancel while sink waits native upload | Abort awaited; no visible partial target/dangling task; original error/cancellation propagates |
| Dictionary/zero rows | Write dictionary replacement IPC and empty explicit-schema Parquet | Valid decoded results and actual native metadata |
| Store ownership | Writer finishes/fails then reuse supplied AsyncObjectStore | Store remains operational; only helper upload/reader closed |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/storage/_arrow.py src/py/tests/unit/storage/test_storage_store.py`
- `uv run --no-sync mypy src/py/vertebra/storage/_store.py src/py/vertebra/storage/_arrow.py`
- `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/storage/_store.py` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
