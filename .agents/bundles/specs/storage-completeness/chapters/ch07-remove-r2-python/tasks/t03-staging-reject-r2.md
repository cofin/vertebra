---
type: Task
id: storage-completeness:ch07:t03
chapter_id: ch07-remove-r2-python
title: Reject removed R2 URIs before staging local-path resolution
description: Reject removed R2 URIs before staging local-path resolution
status: draft
state: pending
priority: P1
verification_strategy: regression_tdd
depends_on:
- storage-completeness:ch07:t02
read_set:
- src/py/vertebra/db/_staging.py
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
- src/py/vertebra/db/_staging.py
tests:
- src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py
- src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 90
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
  resource: src/py/vertebra/db/_staging.py
  title: Owned implementation seam
tags:
- storage
- repair
---

# Reject removed R2 URIs before staging local-path resolution

## Objective & Scope Boundary

Reject removed R2 URIs before staging local-path resolution. Execute one bounded worksheet and create one signed local conventional commit after verification. Read the existing functional baseline at `5fe477fa`; a reopened ch01 is not an execution prerequisite. Own only the listed production and canonical test files. [^implementation]

## Bounded Read Manifest

- `src/py/vertebra/db/_staging.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — owned implementation or canonical regression surface.
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — owned implementation or canonical regression surface.

## Symbol & Signature Contracts

`ObjectStoreStaging.validate_staging_target`, `_parse_implicit_uri`, `_resolve_backend_path` and `_resolve_store_key` reject URL scheme r2 with a clear ValueError identifying unsupported storage scheme. Remove r2 from `_REMOTE_SCHEMES` only together with explicit unknown/removed-URI rejection so `r2://bucket/key` never becomes a local filename or directory. Preserve plain paths, file:// and supported remote schemes, and explicit passed-store compatibility checks. No R2 alias or special endpoint normalization.

Caller → seam → collaborator: public sync/async object-store or adapter operation → owned symbols above → existing native engine/provider or upstream framework contract. Preserve public APIs outside this task.

## Edge Cases & Error Semantics

The matrix below specifies observable failure behavior. Preserve the original error when cleanup also fails; retain ownership until abort or cleanup completes. No whole-object Vec/list/spool, size-cap fallback, Python lock, SDK fork, auth reimplementation or silent alias is permitted.

## Public-Seam Test Blueprint

Use the listed canonical test files and actual public store/framework calls. Do not assert source text or merely duplicate a helper implementation.

| Case | Input | Observable result |
| --- | --- | --- |
| Implicit rejected URI | resolve_sync_store/resolve_async_store(None,"r2://bucket/data.parquet") | ValueError; no r2: local path created |
| Explicit store rejected URI | Memory store plus r2://bucket/key passed through staging public export/read path | ValueError before any write; store unchanged |
| Supported staging | file path and s3://bucket/key with configured store | Existing path resolution unchanged |

## Verification

Record the failing public regression before implementation and the passing result after. Execute each command below in this worktree; the storage target directory is reserved and shared SHM tests use the host lock.

- `CARGO_TARGET_DIR="$PWD/../vertebra/src/rs/target" PYO3_PYTHON="$PWD/.venv/bin/python3.12" make dev`
- `uv run --no-sync python -m mypy.stubtest vertebra._storage`
- `uv run --no-sync ruff check src/py/vertebra/db/_staging.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_data_storage_integrations.py`
- `uv run --no-sync mypy src/py/vertebra/db/_staging.py`
- `uv run --no-sync pytest src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

Record exact exit statuses, native build checkpoint and any platform limitation in verification_evidence. Review closure uses these results, never planning-only structural validation.

## Acceptance Criteria

- [ ] Every matrix row passes at its public seam with existing behavior preserved.
- [ ] Only owned files change; bounds stay at read_set <= 6, production files <= 3 and estimated implementation LOC <= 250.
- [ ] User-facing usage and native guarantees are documented by ch04:t03 and ch09; this task records its observable contract in the canonical regression tests.
- [ ] A signed local conventional commit and verification evidence are recorded; no hosted action is performed.

## Notes & Discoveries

The plan awaits independent cold review. Any implementation contract contradiction routes through `/flow:revise storage-completeness` before production edits.

[^implementation]: `src/py/vertebra/db/_staging.py` — Current repository source for this task.
