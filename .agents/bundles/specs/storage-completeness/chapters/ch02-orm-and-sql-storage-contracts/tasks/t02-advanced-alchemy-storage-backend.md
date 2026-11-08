---
type: Task
id: "storage-completeness:ch02:t02"
chapter_id: ch02-orm-and-sql-storage-contracts
title: Remove storages.get monkeypatch and harden Advanced Alchemy ObjectStoreBackend
description: Delete import-time storages.get monkeypatching and memory:// fallbacks in vertebra.extensions.alchemy.storage and register ObjectStoreBackend strictly via Advanced Alchemy StorageRegistry.
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
plan_revision: 3
plan_commit: e8901de4
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
updated_at: "2026-11-08T02:09:14Z"
commit: null
sources:
  - id: alchemy-storage-mod
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
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

# Task t02: Register Fail-Closed Advanced Alchemy `ObjectStoreBackend` Without Monkeypatching

## Objective & Scope Boundary

Remove both `storages.get` monkeypatches and implicit memory targets from the existing public concrete `ObjectStoreBackend`. Register it through Advanced Alchemy's real registry API, preserve existing explicit registration forms and `FileObject` behavior, and stream chunked/file inputs through native multipart operations without `asyncio.to_thread` or whole-payload collection.[^alchemy-storage-mod][^store-zero-thread]

## Bounded Read Manifest

- `src/py/vertebra/extensions/alchemy/storage.py` — Concrete `ObjectStoreBackend` and `register_storage_backend`.[^alchemy-storage-mod]
- `src/py/vertebra/storage/_store.py` — Shared native stores, metadata, and stream upload.[^store-zero-thread]
- `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Advanced Alchemy `FileObject`, registry, signing, and transaction lifecycle tests.
- `.agents/bundles/knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md` — Extension conventions.
- `.agents/skills/python-framework/SKILL.md` — Python typing, docstrings, and optional imports.

**Installed upstream contract evidence:** Advanced Alchemy `1.12.0`: `.venv/lib/python3.12/site-packages/advanced_alchemy/types/file_object/base.py::StorageBackend`, `registry.py::StorageRegistry`, and `file.py::FileObject` in the same package. The abstract API reads bytes with `get_content` / `get_content_async`; it has no `get_object` methods. `StorageRegistry` is a singleton; tests unregister only their own backend keys.

## Symbol & Signature Contracts

- Retain `class ObjectStoreBackend(AAStorageBackend)` as the concrete public class; the imported upstream abstract base may be named `AAStorageBackend`. No new concrete class named `StorageBackend` and no compatibility aliases.
- Preserve `ObjectStoreBackend.__init__(self, key: str = "default", fs: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, *, store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, prefix: str = "", **kwargs: Any) -> None`. Explicit `store` takes precedence over `fs`; absent/blank selected target raises `ImproperConfigurationError`. Preserve explicit `MemoryConfig()` and shared `.to_sync()` / `.to_async()` engine handles.
- Remove module-import and per-registration mutation of `storages.get`. Preserve upstream `StorageRegistry.register_backend(backend)`, `get_backend(key)`, and `set_default_backend` behavior without replacing registry methods or classes.
- Preserve `register_storage_backend(store_or_key: SyncObjectStore | AsyncObjectStore | str | StorageConfig, *, store: SyncObjectStore | AsyncObjectStore | str | StorageConfig | None = None, key: str = "default", prefix: str = "", set_default: bool = False, registry: StorageRegistry | None = None) -> ObjectStoreBackend`. Existing `register_storage_backend(store, key="media")` and `register_storage_backend("media", store=store)` forms work; a bare key with no target raises `ImproperConfigurationError`. Construct/validate the backend before mutating registry/default state.
- Preserve exact upstream method signatures: `get_content(path, *, options=None) -> bytes`, `get_content_async`; `save_object(file_object: FileObject, data: DataLike, *, use_multipart: bool | None = None, chunk_size: int = 5 * 1024 * 1024, max_concurrency: int = 12) -> FileObject`, `save_object_async(..., data: AsyncDataLike, ...)`; delete scalar/sequence methods and scalar/sequence GET/PUT `sign` / `sign_async` overloads.
- Buffer inputs use native `put` unless multipart is requested. Iterable/async-iterable inputs use native `put_stream` incrementally. File-like inputs read bounded `chunk_size` chunks; `Path` inputs retain native `put_file`. For legacy no-argument async-readable objects already exercised in canonical tests, preserve their one-read supported contract. Metadata populates the supplied `FileObject` with actual native size/etag. `chunk_size < 1` raises `ValueError` before consuming a stream.
- Caller wiring: `FileObject.save` / `save_async` → registered `ObjectStoreBackend.save_object` / `save_object_async` → paired native put/stream; `FileObject.get_content` / `get_content_async` → paired native get. Existing range/list/copy/delete/signing methods remain public.

## Edge Cases & Error Semantics

- `ObjectStoreBackend()`, `ObjectStoreBackend(fs=None)`, `ObjectStoreBackend(fs="  ")`, `register_storage_backend("media")`, and `register_storage_backend("media", store="")` raise `ImproperConfigurationError` without registry mutations.
- A failed upload propagates its original exception; native `put_stream` aborts incomplete multipart uploads. Async cancellation is preserved.
- `prefix` namespaces every key once and rejects `..` path segments rather than permitting traversal into sibling keys.
- Unsupported signing retains `NotImplementedError`; HMAC signing honors expiry, upload/download method, and scalar/list shape.

## Public-Seam Test Blueprint

**Canonical module:** `src/py/tests/unit/extensions/test_data_storage_integrations.py`; real native local stores, Advanced Alchemy registry, `FileObject`, and `StoredObject`. Observe intended Red before production edits.

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| No imported mutation | In a fresh subprocess inspect the real registry's `.get` availability before and after importing the Vertebra module | Import leaves the registry API unchanged. |
| Fail-closed target | Omitted fs, None/blank fs, bare registration key; valid `LocalConfig(root=tmp_path)` in explicit fs/store forms | Invalid inputs raise `ImproperConfigurationError`; registered backend resolves by exact key via `get_backend`. |
| Sync and async `FileObject` lifecycle | `obj = FileObject(backend=backend, filename="u1.png", content_type="image/png")`; `obj.save(b"png")`; `await obj.save_async(b"png2")` | `get_content` / `get_content_async` return exact bytes, metadata sizes are 3/4, signed URLs verify, and deletion removes keys. |
| Incremental stream | Iterator and async iterator yield `b"chunk1-"`, `b"chunk2"`; bounded-reading file-like payload; `use_multipart=True, chunk_size=4` | Actual stored bytes equal `b"chunk1-chunk2"`; generator consumption stays incremental and FileObject size is 13. |
| Existing public lifecycle | Canonical range/list/copy/signing, Path upload, no-argument async-readable, session tracker tests | Existing assertions stay green without changing upstream APIs. |

## Verification

- Initial Red: `uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -k alchemy -q` after adding the focused regression.
- Final: `uv run --no-sync ruff check src/py/vertebra/extensions/alchemy/storage.py src/py/tests/unit/extensions/test_data_storage_integrations.py && uv run --no-sync mypy src/py/vertebra/extensions/alchemy/storage.py && uv run --no-sync pytest src/py/tests/unit/extensions/test_data_storage_integrations.py -q`.

## Acceptance Criteria

- [ ] Both registry monkeypatches and implicit `memory://` fallbacks are removed; public concrete `ObjectStoreBackend` and explicit registration forms remain usable (R3, R5).
- [ ] Real `FileObject` / `StorageRegistry` lifecycle, metadata, streaming, and signing pass through paired native stores without `asyncio.to_thread` (R1, R5).
- [ ] Invalid configuration and traversal are rejected before native or registry effects; touched Python passes lint/type checks (R3, R5).

## Notes & Discoveries

- Installed contract baseline: Advanced Alchemy `1.12.0`; concrete Vertebra class is `ObjectStoreBackend`, abstract upstream base is `StorageBackend`, and read operations are `get_content[_async]`. Canonical tests contain no `test_storage_and_alchemy_zero_to_thread` test.

[^alchemy-storage-mod]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Concrete backend and explicit registration forms.
[^store-zero-thread]: `src/py/vertebra/storage/_store.py::AsyncObjectStore` — Native asynchronous streaming and shared-engine conversion.
