---
type: Task
id: storage-completeness:ch18:t01
chapter_id: ch18-arrow-async-bridge
title: Bridge synchronous PyArrow reads to owning-loop native async ranges
description: Bridge synchronous PyArrow reads to owning-loop native async ranges
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch17:t01
read_set:
- src/py/vertebra/storage/_arrow.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/vertebra/storage/_store.py
- .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
files:
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
  resource: src/py/vertebra/storage/_arrow.py
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Bridge synchronous PyArrow reads to owning-loop native async ranges

## Objective & Scope Boundary

Bridge synchronous PyArrow reads to owning-loop native async ranges. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Add private `AsyncRangeBridge` created on an explicit owning asyncio loop with AsyncObjectStore, key, object_size and max_cache_bytes=8 MiB. Its worker-only seekable `read(n)`, readinto, seek and tell callbacks schedule await store.get_range on that loop via asyncio.run_coroutine_threadsafe, never SyncObjectStore I/O. A bounded LRU stores at most 8 MiB of fetched range bytes; a required PyArrow consumer read may return its requested range with an explicit bytes copy, but never a whole-object prefetch/list/mmap spool. Use pq.ParquetFile(metadata=optional parsed footer,pre_buffer=False). Set Parquet iter_batches/read_row_group use_threads=False so each per-reader job owns sequential callbacks rather than sharing mutable seek state across Arrow worker threads. IPC reader uses the same bounded callback bridge and PyArrow handles dictionaries natively.

Private `AsyncReaderJob` owns one pyarrow reader/generator and one in-flight per-batch job. `async next_batch() -> RecordBatch | None` starts one asyncio.to_thread job inside _arrow.py only; worker executes next(reader) and returns one batch, so a paused async consumer reserves no blocked worker. No worker waits for a producer queue between jobs. All asyncio operations remain on the captured owning loop; a call from another loop raises RuntimeError before I/O. `async aclose()` flips a local closing flag, prevents new requests, cancels/tracks in-flight native futures, awaits worker completion and closes reader. Local state/locks are per bridge; no Python/global process lock. A caller-owned AsyncObjectStore is never closed.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Eventloop remains live | Worker read requests remote range while independent owning-loop task advances barrier | Native async range runs on owner loop; event loop remains responsive; no sync store call |
| Paused consumer | Read one batch, pause before requesting next | No active blocked worker; no next batch/range requested |
| Cache/cancellation | Read distant rowgroups; cancel while range is in flight | Cache <=8 MiB, new I/O stops, reader/job/futures cleaned and store remains usable |
| Loop mismatch | Use bridge from a different asyncio loop | Explicit RuntimeError before I/O |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/storage/_arrow.py src/py/tests/unit/storage/test_storage_store.py`
- `uv run --no-sync mypy src/py/vertebra/storage/_arrow.py`
- `uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/storage/_arrow.py` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
