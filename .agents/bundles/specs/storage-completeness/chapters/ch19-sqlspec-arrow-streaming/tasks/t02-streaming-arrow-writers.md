---
type: Task
id: storage-completeness:ch19:t02
chapter_id: ch19-sqlspec-arrow-streaming
title: Route SQLSpec table/cursor writes through bounded native streams
description: Route SQLSpec table/cursor writes through bounded native streams
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:ch19:t01
- storage-completeness:ch18:t03
- storage-completeness:ch04:t01
read_set:
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
- src/py/vertebra/storage/_arrow.py
- src/py/vertebra/db/arrow.py
- .agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md
files:
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 140
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

# Route SQLSpec table/cursor writes through bounded native streams

## Objective & Scope Boundary

Route SQLSpec table/cursor writes through bounded native streams. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.
- `src/py/vertebra/storage/_arrow.py` — collaborating contract.
- `src/py/vertebra/db/arrow.py` — collaborating contract.

- `.agents/bundles/specs/storage-completeness/research/arrow-async-bridge_20261107.md` — Grounded SDK/protocol and ownership evidence.

## Symbol & Signature Contracts

Grounded contract evidence: [^research].

`ObjectStoreBackend.write_arrow_sync/write_arrow_async` and write_arrow_cursor_sync/write_arrow_cursor_async use sync/async store write_arrow/write_parquet, selected by existing file_format inference. Preserve installed SQLSpec call signatures, kwargs/projection behavior and existing explicit ArrowCursor buffer_view fast path when it already owns valid IPC bytes; do not first serialize a whole new Parquet file via BufferOutputStream or list batches. Existing caller Table/cursor may own output data; newly encoded bytes stream via native MPU with backpressure. Compression/schema/row_group_size kwargs pass through or raise clear validation errors, never ignored. Async methods use only async store helpers and await cleanup. Final original ch04:t02 integrates/accepts these APIs after ch04:t01 strict Arrow IPC prerequisite.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Cursor Parquet stream | ArrowCursor250rows, row_group_size100 | Three correct groups, no whole encoded-file BufferOutputStream/list |
| Async producer error | Batch source raises after first rowgroup | Upload abort awaited; no target; original error |
| Protocol preservation | Installed SQLSpec public write/read/stream calls sync+async | Existing signatures, output shapes, compression and values retained |

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
