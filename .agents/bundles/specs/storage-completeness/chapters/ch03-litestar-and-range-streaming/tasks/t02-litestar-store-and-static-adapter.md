---
type: Task
id: "storage-completeness:ch03:t02"
chapter_id: ch03-litestar-and-range-streaming
title: Implement ObjectStoreLitestarStore and ObjectStoreFileSystemAdapter in extensions.litestar
description: Add an AsyncObjectStore-backed Litestar Store (ObjectStoreLitestarStore) with TTL metadata and an async static-files adapter (ObjectStoreFileSystemAdapter) with Range support in vertebra.extensions.litestar.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-completeness:ch03:t01"
read_set:
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/litestar/__init__.py
  - src/py/vertebra/storage/_streaming.py
  - src/py/tests/unit/extensions/test_litestar_plugin.py
  - .agents/bundles/knowledge/domains/extensions/litestar.md
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/extensions/litestar/store.py
  - src/py/vertebra/extensions/litestar/__init__.py
tests:
  - src/py/tests/unit/extensions/test_litestar_plugin.py
estimated_loc: 190
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
updated_at: "2026-11-08T02:25:35Z"
commit: null
sources:
  - id: litestar-store-mod
    resource: src/py/vertebra/extensions/litestar/store.py::ValkeyStore
    title: Litestar Store implementations in vertebra.extensions.litestar.store
  - id: litestar-pkg-init
    resource: src/py/vertebra/extensions/litestar/__init__.py
    title: Litestar extension package exports
  - id: range-streaming
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: Non-blocking HTTP Range streaming response
tags:
  - storage
  - litestar
  - extensions
  - http
---

# Task t02: Implement `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter` in `extensions.litestar`

## Objective & Scope Boundary

Add `ObjectStoreLitestarStore` (implementing `litestar.stores.base.Store` over `AsyncObjectStore` with persistent TTL attribute metadata and namespace isolation) and `ObjectStoreFileSystemAdapter` (implementing Litestar's async `FileSystemProtocol` `info` and `open` methods for `create_static_files_router`) in `src/py/vertebra/extensions/litestar/store.py` and export both in `src/py/vertebra/extensions/litestar/__init__.py` [^litestar-store-mod] [^litestar-pkg-init] [^range-streaming].

## Bounded Read Manifest

- `src/py/vertebra/extensions/litestar/store.py` — `_StoreBase`, `MemoryStore`, and `ValkeyStore` patterns [^litestar-store-mod].
- `src/py/vertebra/extensions/litestar/__init__.py` — Lazy/direct exports for `vertebra.extensions.litestar` [^litestar-pkg-init].
- `src/py/vertebra/storage/_streaming.py` — `StreamingStorageResponse` from `ch03:t01` [^range-streaming].
- `src/py/tests/unit/extensions/test_litestar_plugin.py` — Litestar plugin, store, and static-route unit tests.
- `.agents/bundles/knowledge/domains/extensions/litestar.md` — Litestar integration patterns.
- `.agents/skills/python-framework/SKILL.md` — Typing, docstring, and zero-inline-comment rules.

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/litestar/store.py`:
  - `class ObjectStoreLitestarStore(_StoreBase)`:
    - `__init__(self, store: AsyncObjectStore | StorageConfig | str, *, namespace: str | None = None) -> None`: fail closed with `ImproperConfigurationError` if `store` is `None` or `""`.
    - `with_namespace(self, namespace: str) -> ObjectStoreLitestarStore`
    - `async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None` (stores payload under `<namespace>/<key>` with `attributes={"vertebra-expires-at-ms": ...}` when `expires_in` is set)
    - `async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None` (checks `vertebra-expires-at-ms`; deletes and returns `None` if expired; updates TTL when `renew_for` is provided)
    - `async def delete(self, key: str) -> None` (idempotent on missing keys)
    - `async def delete_all(self) -> None` (paginates via `list_page` under the store's namespace prefix and deletes entries without affecting sibling namespaces)
    - `async def exists(self, key: str) -> bool` (respects TTL expiration)
    - `async def expires_in(self, key: str) -> int | None` (returns remaining whole seconds `>= 0` when TTL is set, `None` when key has no TTL or does not exist)
  - `class ObjectStoreFileSystemAdapter`:
    - `__init__(self, store: AsyncObjectStore | StorageConfig | str, *, base_path: str = "") -> None`: fail closed with `ImproperConfigurationError` on empty/missing `store`.
    - `async def info(self, path: str | os.PathLike[str], **kwargs: Any) -> dict[str, Any]` (returns Litestar `FileInfo`-compatible mapping `{"name": ..., "size": meta.size, "type": "file" | "directory", "mtime": epoch_float}`, raising `FileNotFoundError` when neither file nor prefix exists, and rejecting `..` traversal).
    - `async def open(self, file: str | os.PathLike[str], mode: str = "rb", buffering: int = -1, **kwargs: Any) -> AsyncReadableFile` (returns `AsyncReadableFile` supporting async `read`, `seek`, `tell`, and context management).
- `src/py/vertebra/extensions/litestar/__init__.py`: export `ObjectStoreLitestarStore` and `ObjectStoreFileSystemAdapter` in `__all__`.

## Edge Cases & Error Semantics

- `ObjectStoreLitestarStore(None)` (via `Any`) or `ObjectStoreFileSystemAdapter("")` raises `ImproperConfigurationError`.
- `delete_all()` on a namespaced store (`store.with_namespace("sessions")`) deletes only keys under `"sessions/"` and leaves `"cache/"` keys untouched.
- Requesting `GET /static/../secret` through Litestar `create_static_files_router(..., fs=ObjectStoreFileSystemAdapter(...))` returns `404 Not Found` (path traversal rejected).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/extensions/test_litestar_plugin.py` (real `Litestar` `AsyncTestClient`, `create_static_files_router`, and `LocalConfig(tmp_path)`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Fail-closed init | `ObjectStoreLitestarStore("")` and `ObjectStoreFileSystemAdapter(None)` | Both raise `ImproperConfigurationError`. |
| `Store` CRUD, TTL expiry, `renew_for`, & namespace `delete_all` | `s1 = store.with_namespace("s1")`, `s2 = store.with_namespace("s2")`; `await s1.set("k", b"v", expires_in=1)`, inspect `expires_in("k")`, advance clock / expire, `s2.set("keep", b"1")`, `await s1.delete_all()` | Active key returns `b"v"` and `expires_in >= 0`; expired key returns `None` and `exists == False`; `s1.delete_all()` preserves `await s2.get("keep") == b"1"`. |
| Litestar `create_static_files_router` + `Range` route | Mount `create_static_files_router(path="/assets", directories=["."], fs=ObjectStoreFileSystemAdapter(LocalConfig(tmp_path)))` and a `StreamingStorageResponse` handler; request `/assets/app.js` and `Range: bytes=0-7` / `Range: bytes=9999-` | `/assets/app.js` returns `200` with exact JS bytes; `Range: bytes=0-7` returns `206` with 8 bytes; `Range: bytes=9999-` returns `416` with `Content-Range: bytes */<size>`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -k "object_store or static" -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/extensions/litestar src/py/tests/unit/extensions/test_litestar_plugin.py && uv run --no-sync mypy src/py/vertebra/extensions/litestar && uv run --no-sync pytest src/py/tests/unit/extensions/test_litestar_plugin.py -q`

## Acceptance Criteria

- [ ] `ObjectStoreLitestarStore` satisfies Litestar `Store` (`set`, `get`, `delete`, `delete_all`, `exists`, `expires_in`, `with_namespace`) over `AsyncObjectStore` (R3, R7).
- [ ] `ObjectStoreFileSystemAdapter` serves static assets cleanly via Litestar `create_static_files_router`, and Litestar handlers serve `StreamingStorageResponse` `206` / `416` byte ranges end-to-end (R7).

## Notes & Discoveries

- [2026-11-07 04:15] Check `litestar.file_system` (`BaseLocalFileSystem` / `FileSystemProtocol`) method signatures in the installed `litestar` version when implementing `ObjectStoreFileSystemAdapter`.

[^litestar-store-mod]: `src/py/vertebra/extensions/litestar/store.py::ValkeyStore` — Litestar key-value stores.
[^litestar-pkg-init]: `src/py/vertebra/extensions/litestar/__init__.py` — Litestar extension exports.
[^range-streaming]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Range-aware streaming response.
