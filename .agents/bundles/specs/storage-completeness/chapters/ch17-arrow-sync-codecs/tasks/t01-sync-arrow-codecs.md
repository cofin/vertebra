---
type: Task
id: storage-completeness:ch17:t01
chapter_id: ch17-arrow-sync-codecs
title: Implement bounded synchronous Arrow/Parquet codec helpers
description: Implement bounded synchronous Arrow/Parquet codec helpers
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
- src/py/vertebra/storage/_arrow.py
- src/py/tests/unit/storage/test_storage_store.py
- src/py/vertebra/db/arrow.py
- src/py/vertebra/storage/_store.py
- .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
files:
- src/py/vertebra/storage/_arrow.py
tests:
- src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 220
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

# Implement bounded synchronous Arrow/Parquet codec helpers

## Objective & Scope Boundary

Implement bounded synchronous Arrow/Parquet codec helpers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_arrow.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/db/arrow.py` — collaborating contract.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Add private `storage/_arrow.py` as the deep optional-PyArrow boundary. `iter_input_batches(data, *, batch_size: int, schema: Any = None) -> Iterator[Any]` accepts Table, RecordBatchReader, iterable of RecordBatch or ArrowCursor.to_arrow_table(); slices every Table/oversized RecordBatch into at most batch_size rows, preserves schema and consumes iterables lazily without list/to_pylist. A caller-owned Table may already materialize rows; no whole encoded input file is materialized. Private draining sink has write/tell/flush/close with a deque of bounded encoded chunks and `drain() -> Iterator[memoryview]`; sinks hand each batch's emitted bytes to native multipart before accepting the next batch. PyArrow IPC stream writer preserves schema/dictionaries/replacement across batches; ParquetWriter keeps footer state and writes rowgroups with caller compression/schema. Validate positive batch/rowgroup size before upload. Missing PyArrow raises existing MissingDependencyError. No hand-written Arrow FlatBuffer or Parquet metadata parser.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Lazy producer | Generator yields batches0,1,2 and records advancement; second batch raises | First batch upload occurs before second read; abort on error and no target published |
| Schema/dictionaries | Dictionary-encoded IPC batches change/reuse dictionary; explicit empty schema | Valid IPC roundtrip, replacement semantics preserved, empty stream decodes |
| Parquet row groups | 250 rows, row_group_size100, compression zstd | Three groups 100/100/50 with exact schema/values; no full-file encoded buffer |

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
