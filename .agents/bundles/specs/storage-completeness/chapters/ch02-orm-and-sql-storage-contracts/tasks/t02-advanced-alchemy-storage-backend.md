---
type: Task
id: "storage-completeness:ch02:t02"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Remove storages.get monkeypatch and harden Advanced Alchemy StorageBackend
description: Delete import-time storages.get monkeypatching and memory:// fallbacks in vertebra.extensions.alchemy.storage and register StorageBackend strictly via Advanced Alchemy StorageRegistry.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/extensions/alchemy/storage.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
  - .agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/extensions/alchemy/storage.py
tests:
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
estimated_loc: 160
plan_revision: 2
plan_commit: cdb978d3
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T04:15:00Z"
commit: null
sources:
  - id: alchemy-storage-mod
    resource: src/py/vertebra/extensions/alchemy/storage.py::StorageBackend
    title: Advanced Alchemy StorageBackend and register_storage_backend
  - id: store-zero-thread
    resource: src/py/vertebra/storage/_store.py::AsyncObjectStore
    title: Non-blocking AsyncObjectStore backing Advanced Alchemy async file operations
tags:
  - storage
  - advanced-alchemy
  - extensions
  - python
---

# Task t02: Remove `storages.get` Monkeypatch and Harden Advanced Alchemy `StorageBackend`

## Objective & Scope Boundary

Delete the `setattr(storages, _storage_get_attr, storages.get_backend)` monkeypatch and `"memory://"` fallbacks from `src/py/vertebra/extensions/alchemy/storage.py`, enforce fail-closed `fs` configuration on `StorageBackend` and `register_storage_backend`, and verify full `FileObject` lifecycle conformance through Advanced Alchemy's public `StorageRegistry` API (`storages.register_backend` / `storages.get_backend`) with zero `asyncio.to_thread` calls [^alchemy-storage-mod] [^store-zero-thread].

## Bounded Read Manifest

- `src/py/vertebra/extensions/alchemy/storage.py` — `StorageBackend`, `ObjectStoreBackend` alias (if any — remove aliases if unrequired, or verify canonical class name in tests), and `register_storage_backend` [^alchemy-storage-mod].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, and `StorageConfig` [^store-zero-thread].
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Existing Advanced Alchemy `FileObject`, `StorageRegistry`, and `test_storage_and_alchemy_zero_to_thread` tests.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — Advanced Alchemy extension conventions.
- `.agents/skills/python-framework/SKILL.md` — Typing, docstring, and zero-inline-comment rules.

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/alchemy/storage.py`:
  - Remove lines mutating `advanced_alchemy.types.file_object.storages` (`_storage_get_attr = "get"; setattr(storages, ...)`).
  - `class StorageBackend(AAStorageBackend)`:
    - `__init__(self, key: str, fs: SyncObjectStore | AsyncObjectStore | StorageConfig | str, *, prefix: str = "", **kwargs: Any) -> None`: raise `ImproperConfigurationError` if `fs` is `None` or an empty string.
    - `save_object` / `save_object_async` (supporting `bytes`, file-like streams, and async iterables via `put` / `put_stream` without `asyncio.to_thread`)
    - `get_object` / `get_object_async`
    - `delete_object` / `delete_object_async`
    - `sign` / `sign_async` (honoring `expires_in` and `for_upload`)
  - `register_storage_backend(key: str, fs: SyncObjectStore | AsyncObjectStore | StorageConfig | str, *, registry: StorageRegistry | None = None, prefix: str = "") -> StorageBackend`:
    - Registers on `registry or storages` via `register_backend(backend)` and returns `backend`; raises `ImproperConfigurationError` when `fs` is missing/empty.

## Edge Cases & Error Semantics

- `StorageBackend(key="media", fs=None)` (via `Any`), `StorageBackend(key="media", fs="")`, and `register_storage_backend("media", "")` raise `ImproperConfigurationError`.
- Importing `vertebra.extensions.alchemy.storage` does NOT add a `.get` attribute onto `advanced_alchemy.types.file_object.storages` unless upstream `StorageRegistry` natively defines it; all Vertebra callers and tests resolve backends via `storages.get_backend(key)`.
- `save_object_async` with large chunked data streams via `AsyncObjectStore.put_stream` without `asyncio.to_thread`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_data_storage_integrations.py` (real `StorageRegistry` + `FileObject` + `LocalConfig(tmp_path)`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Zero monkeypatch check | Import `vertebra.extensions.alchemy.storage` and inspect `StorageRegistry` | Module AST has zero `setattr(storages, ...)` calls; `registry = StorageRegistry(); register_storage_backend("avatars", LocalConfig(tmp_path), registry=registry)` resolves via `registry.get_backend("avatars")`. |
| Fail-closed missing `fs` | `StorageBackend(key="avatars", fs="")` and `register_storage_backend("avatars", None)` | Both raise `ImproperConfigurationError`. |
| `FileObject` sync & async lifecycle | Save `FileObject(backend="avatars", filename="u1.png", content=b"\x89PNG...")` via `save_object_async`, read `get_object_async`, sign, and `delete_object_async` | Round-trips exact bytes `b"\x89PNG..."`, deletes cleanly, and passes `test_storage_and_alchemy_zero_to_thread`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k alchemy -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`

## Acceptance Criteria

- [ ] `storages.get` monkeypatch and `"memory://"` fallback are completely removed from `src/py/vertebra/extensions/alchemy/storage.py` (R3, R5).
- [ ] `StorageBackend` and `register_storage_backend` integrate cleanly with `StorageRegistry.register_backend` / `get_backend` and `FileObject` with zero `asyncio.to_thread` calls (R5).

## Notes & Discoveries

- [2026-11-07 04:15] Update any test in `test_data_storage_integrations.py` that called the monkeypatched `storages.get(...)` to call `storages.get_backend(...)`.

[^alchemy-storage-mod]: `src/py/vertebra/extensions/alchemy/storage.py::StorageBackend` — Advanced Alchemy storage backend module.
[^store-zero-thread]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Native Rust-backed async object store.
