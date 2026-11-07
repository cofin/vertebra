---
type: Task
id: storage-completeness:ch19:t01
chapter_id: ch19-sqlspec-arrow-streaming
title: Replace SQLSpec eager async Arrow batch materialization
description: Replace SQLSpec eager async Arrow batch materialization
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch18:t02
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
- .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
files:
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 130
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
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: Owned implementation seam
- id: research
  resource: .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
  title: Grounded storage publication or Arrow API evidence
tags:
- storage
- repair
---

# Replace SQLSpec eager async Arrow batch materialization

## Objective & Scope Boundary

Replace SQLSpec eager async Arrow batch materialization. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.
- `src/py/vertebra/storage/_arrow.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`ObjectStoreBackend._iter_arrow_async` resolves matching keys incrementally by existing metadata/list APIs and delegates each object to async_store.iter_parquet_batches or iter_arrow_batches with caller file_format/batch_size/columns. Remove asyncio.to_thread(lambda:list(self.stream_arrow_sync(...))) and never materialize all input batches or call sync_store on event loop. Preserve stream_arrow_async protocol and exact ArrowRecordBatch shape expected by installed SQLSpec; close delegated iterator in finally on consumer early termination/cancellation. Keep existing projection/pattern semantics and no-match empty iterator. read_arrow_async may materialize requested output Table as its protocol requires, but not encoded whole-file bytes.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Laziness | Multiple matching objects and gated batch iterator; consume only first batch | No second object or remaining batch read; first values exact |
| Projection | 250 rows id/payload over3groups, columns=["id"] | RecordBatch rows100/100/50, schema id, exact values |
| No-match/error/close | No matches; corrupt object; cancel after first batch | Empty iterator or original error; owned bridge closes; store remains open |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/extensions/sqlspec/storage.py` — Current repository source for this task.

[^research]: `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Source-grounded architecture and probe evidence.
