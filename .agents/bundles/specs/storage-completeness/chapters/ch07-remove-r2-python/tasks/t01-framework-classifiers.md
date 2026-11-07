---
type: Task
id: storage-completeness:ch07:t01
chapter_id: ch07-remove-r2-python
title: Remove R2 from framework backend classifiers
description: Remove R2 from framework backend classifiers
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on: []
read_set:
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/extensions/starlette/storage.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
- src/py/vertebra/storage/_store.py
files:
- src/py/vertebra/extensions/alchemy/storage.py
- src/py/vertebra/extensions/sqlspec/storage.py
- src/py/vertebra/extensions/starlette/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 55
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
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Remove R2 from framework backend classifiers

## Objective & Scope Boundary

Remove R2 from framework backend classifiers. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/extensions/alchemy/storage.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/sqlspec/storage.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/extensions/starlette/storage.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.
- `src/py/vertebra/storage/_store.py` — collaborating contract.

## Symbol & Signature Contracts

Remove R2Config imports and entries in `_STORAGE_CONFIGS` from Advanced Alchemy, SQLSpec and Starlette. Remove r2:// advertised URI support in their docstrings. Keep S3Config with arbitrary explicit custom endpoints accepted; no provider hostname blacklist, alias or Cloudflare compatibility shim. This task precedes core Python API withdrawal so its imports stay valid at every commit.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Supported custom endpoint | Construct each adapter using S3Config(endpoint="http://127.0.0.1:9000", allow_http=True) | Normal S3 classification retained |
| Removed first-class classifier | Read adapter public configurations and constructor behavior after core withdrawal | No R2Config import remains; r2:// ultimately rejected by store constructor |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/starlette/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py src/py/vertebra/extensions/sqlspec/storage.py src/py/vertebra/extensions/starlette/storage.py`
- `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/extensions/alchemy/storage.py` — Current repository source for this task.
