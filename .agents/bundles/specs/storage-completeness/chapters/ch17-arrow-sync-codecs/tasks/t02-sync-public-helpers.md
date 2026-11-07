---
type: Task
id: storage-completeness:ch17:t02
chapter_id: ch17-arrow-sync-codecs
title: Expose synchronous Arrow/Parquet read and write helpers
description: Expose synchronous Arrow/Parquet read and write helpers
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch17:t01
- storage-completeness:ch16:t02
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
estimated_loc: 180
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

# Expose synchronous Arrow/Parquet read and write helpers

## Objective & Scope Boundary

Expose synchronous Arrow/Parquet read and write helpers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_arrow.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/storage/test_storage_store.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/db/arrow.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

Expose exact SyncObjectStore APIs: write_arrow(key,data,*,batch_size=65536)->ObjectMeta; iter_arrow_batches(key)->Iterator[RecordBatch]; read_arrow(key)->Table; write_parquet(key,batches,*,schema=None,compression="zstd",row_group_size=65536)->ObjectMeta; iter_parquet_batches(key,*,batch_size=65536,columns:Sequence[str]|None=None)->Iterator[RecordBatch]; read_parquet(key,*,columns=None)->Table. Delegate optional dependency/codec detail to _arrow.py. Native multipart upload receives encoder chunks incrementally and aborts on producer/encode/publish failure; native returned metadata remains authoritative. Sync reads wrap owned SyncReadableFile in pyarrow.ipc.open_stream or pq.ParquetFile(pre_buffer=False).iter_batches, closing helper-owned reader on close/error; caller store stays open. Table-returning convenience APIs may materialize output batches but never pre-read entire encoded file. Signatures and reader lifetime are documented in Google docstrings; public helper implementation stays thin.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Sync projection | 250 rows id/payload, three rowgroups; iter_parquet_batches(batch_size100,columns=["id"]) | Batch rows100/100/50, only id, exact IDs 0..249 |
| IPC/Parquet roundtrip | Real Memory and Local stores; empty and dictionary batches | Valid decoded schema/values and native ObjectMeta |
| Early generator close | Read first batch then close iterator | Owned reader closes; passed store still usable; no fullfile cache |

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
