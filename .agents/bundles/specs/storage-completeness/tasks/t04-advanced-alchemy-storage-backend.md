---
type: Task
id: storage-completeness:t04
title: Register Advanced Alchemy ObjectStoreBackend without monkeypatching storages.get
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
- storage-completeness:t03
files:
- src/py/vertebra/extensions/alchemy/storage.py
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
- advanced-alchemy
requirements:
- R3
- R5
---

# Register Advanced Alchemy ObjectStoreBackend without monkeypatching storages.get

## Objective

Complete `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` against Advanced Alchemy's `StorageBackend` / `FileObject` contract, register backends exclusively via `StorageRegistry.register_backend`, remove the module-level `setattr(storages, "get", storages.get_backend)` monkeypatch, and fail closed when `fs`/`store` is omitted.

## Context

Read `../spec.md`, `t03-sqlspec-object-store-protocol.md`, and `.agents/skills/advanced-alchemy/SKILL.md`.

## Steps

1. Add failing tests in `test_data_storage_integrations.py` asserting that `advanced_alchemy.types.file_object.storages` is not monkeypatched with synthetic attributes, `ObjectStoreBackend(key="test")` without `fs`/`store` raises `ImproperConfigurationError`, and `FileObject` sync/async save, get_bytes, sign, and delete operations pass end-to-end.
2. Remove the `setattr(storages, ...)` monkeypatch and implicit `memory://` default from `src/py/vertebra/extensions/alchemy/storage.py` and complete all `StorageBackend` sync and async methods over `SyncObjectStore` and `AsyncObjectStore`.
3. Run verification and commit `fix(alchemy): remove storages.get monkeypatch and complete StorageBackend contract`.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q
uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py
uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py
```

## Acceptance Criteria

- [ ] R3, R5: `extensions/alchemy/storage.py` registers cleanly through `StorageRegistry.register_backend` with zero monkeypatches or implicit `memory://` fallbacks.

## Notes & Discoveries

None.
