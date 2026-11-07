---
type: Task
id: storage-completeness:ch18:t02
chapter_id: ch18-arrow-async-bridge
title: Expose incremental async IPC and Parquet batch readers
description: Expose incremental async IPC and Parquet batch readers
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch18:t01
- storage-completeness:ch17:t02
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
estimated_loc: 190
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

# Expose incremental async IPC and Parquet batch readers

## Objective & Scope Boundary

Expose incremental async IPC and Parquet batch readers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_arrow.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/db/arrow.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Expose AsyncObjectStore.iter_arrow_batches(key)->AsyncIterator[RecordBatch], read_arrow(key)->Table, iter_parquet_batches(key,*,batch_size=65536,columns=None)->AsyncIterator[RecordBatch], and read_parquet(key,*,columns=None)->Table. Thin _store.py methods delegate to _arrow.py AsyncReaderJob; _store.py contains no asyncio.to_thread and performs no synchronous PyArrow/cloud reader operations on the event loop. Metadata HEAD and get_range run natively async. Each anext awaits exactly one worker batch job; explicit aclose(), contextlib.aclosing exit, decode error and cancellation await bridge cleanup. A bare async-for break does not automatically close a Python async generator; document explicit closing for early termination and do not promise cleanup before aclose/finalization. Parquet projection is passed into PyArrow and sparse reads fetch only footer/needed column data according to actual PyArrow requests; no eager list(reader), whole input bytes or Table conversion in iterator. Table conveniences materialize only requested output. IPC dictionaries/replacements and zero-batch streams remain native PyArrow responsibilities.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Async Parquet projection | 250 rows, groups 100/100/50, columns=["id"], instrument native ranges | Exact three batches, no payload column, no whole-file/pre_buffer request |
| Async IPC dictionaries | Two dictionary batches plus replacement and empty stream | Decoded values/schema exact; empty iterator terminates cleanly |
| Negative read | Corrupt footer/IPC, missing PyArrow or object deleted during range | Original decode/dependency/notfound error; bridge closed; store remains usable |

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
