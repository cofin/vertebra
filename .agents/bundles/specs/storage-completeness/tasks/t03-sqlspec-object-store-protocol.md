---
type: Task
id: storage-completeness:t03
title: Complete SQLSpec ObjectStoreProtocol conformance over SyncObjectStore and AsyncObjectStore
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t02
files:
- src/py/vertebra/extensions/sqlspec/storage.py
tests:
- src/py/tests/unit/extensions/test_data_storage_integrations.py
plan_revision: 1
plan_commit: "7657e8b7c2c96bdda6821aec70a2b603f438a127"
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: '2026-11-06T06:38:00Z'
updated_at: '2026-11-06T06:38:00Z'
commit: null
tags:
- storage
- sqlspec
requirements:
- R3
- R4
---

# Complete SQLSpec ObjectStoreProtocol conformance over SyncObjectStore and AsyncObjectStore

## Objective

Complete `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` so it satisfies the full SQLSpec `ObjectStoreProtocol` contract (sync and async reads, writes, streaming bytes/Arrow/Parquet, listing, metadata, copy/move, signing, and fail-closed URI/store requirement without `memory://` defaults).

## Context

Read `../spec.md`, `t02-unified-gcp-auth-signing-and-fail-closed.md`, and `.agents/skills/sqlspec/SKILL.md`. Keep `build_duckdb_gcs_secret_sql` intact until `native-duckdb-driver` relocates it.

## Steps

1. Add failing tests in `test_data_storage_integrations.py` verifying `isinstance(backend, ObjectStoreProtocol)` (or exact method/signature parity), fail-closed error on `ObjectStoreBackend()` with no store/URI, and streaming bytes/Arrow/Parquet round-trips across sync and async APIs.
2. Update `src/py/vertebra/extensions/sqlspec/storage.py` to require an explicit `SyncObjectStore`, `AsyncObjectStore`, `StorageConfig`, or URI and delegate every `ObjectStoreProtocol` method cleanly to native `get_stream`, `put_stream`, `list_page`, `rename`, and `sign_url`.
3. Run verification and commit `feat(sqlspec): complete ObjectStoreProtocol conformance and fail-closed construction`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/storage.py
uv run --no-sync mypy src/py/vertebra/extensions/sqlspec/storage.py
```

## Acceptance Criteria

- [ ] R3, R4: `extensions/sqlspec/storage.py::ObjectStoreBackend` satisfies SQLSpec `ObjectStoreProtocol` with streaming Arrow/Parquet I/O and rejects empty/implicit `memory://` construction.

## Notes & Discoveries

None.
