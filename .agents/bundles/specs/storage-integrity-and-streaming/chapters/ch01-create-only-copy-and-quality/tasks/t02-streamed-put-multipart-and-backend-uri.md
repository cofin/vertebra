---
type: Task
id: "storage-integrity-and-streaming:ch01:t02"
chapter_id: ch01-create-only-copy-and-quality
title: Stream put_multipart and remove the backend URI sentinel
description: Reimplement SyncObjectStore.put_multipart and AsyncObjectStore.put_multipart on put_stream so parts are not materialized into a list, and drop the literal memory:// sentinel from ObjectStoreBackend.__init__.
status: draft
state: pending
priority: P2
verification_strategy: tdd
depends_on: []
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/tests/unit/storage/test_storage_store.py
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
files:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/extensions/sqlspec/storage.py
  - src/py/tests/unit/storage/test_storage_store.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py::test_put_multipart_streams_parts
  - src/py/tests/unit/storage/test_storage_store.py::test_sqlspec_backend_store_uri_resolution
estimated_loc: 90
plan_revision: 1
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
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
commit: null
sources:
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: put_multipart, put_stream, AsyncObjectStore.put_multipart
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: __init__ store_uri resolution
tags:
  - storage
  - sqlspec
---

# Task t02: Stream `put_multipart` and Remove the Backend URI Sentinel

## Objective & Scope Boundary

1. `_store.py::SyncObjectStore.put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult` returns `self.put_stream(path, parts)`; docstring: "Upload `parts` in order as one object through the incremental multipart path." Same for `AsyncObjectStore.put_multipart` with `await self.put_stream(path, parts)` [^py-store]. The native `put_multipart` binding stays for Rust callers; Python no longer calls it.
2. `extensions/sqlspec/storage.py::ObjectStoreBackend.__init__`: replace `if store is not None and isinstance(uri, str) and uri.strip() and uri != "memory://":` with `if store is not None and isinstance(uri, str) and uri.strip():` [^sqlspec-backend]. `uri` already defaults to `None`.

Out of scope: native `put_multipart` removal (Rust ABI), checksum options (ch02).

## Bounded Read Manifest

- `_store.py` — `SyncObjectStore.put_multipart`, `put_stream`, `AsyncObjectStore.put_multipart`, `AsyncObjectStore.put_stream` [^py-store].
- `extensions/sqlspec/storage.py` — `ObjectStoreBackend.__init__` [^sqlspec-backend].
- Both test files for existing fixtures and call sites.

## Symbol & Signature Contracts

- `def put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult` (sync) and `async def put_multipart(self, path: str, parts: Iterable[Buffer]) -> PutResult` (async accepts sync iterables, matching `AsyncObjectStore.put_stream`'s accepted inputs).
- `ObjectStoreBackend.store_uri`: explicit non-empty `uri` string when `store` is supplied; otherwise `sync_store.url`.

## Edge Cases & Error Semantics

- Generator input: consumed lazily; an exception raised by the generator aborts the upload (existing `put_stream` behavior) and no object is created.
- Empty `parts`: zero-byte object (parity with `put_stream`).
- `store=SyncObjectStore("memory://")`, `uri=None`: `store_uri == "memory://"` from `sync_store.url`.
- `store=...`, `uri="s3://bucket/x"`: `store_uri == "s3://bucket/x"`.

## Public-Seam Test Blueprint

- **Canonical gate**: `uv run pytest src/py/tests/unit/storage/test_storage_store.py -k "put_multipart or store_uri" -v`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Generator parts | `store.put_multipart("k", (bytes([i]) * 4 for i in range(3)))` on `memory://` | `get("k") == b"\x00"*4 + b"\x01"*4 + b"\x02"*4` |
| Generator raises mid-stream | generator yields `b"a"` then raises `RuntimeError("boom")` | `RuntimeError("boom")`; `exists("k2") is False` |
| Async parity | `await AsyncObjectStore("memory://").put_multipart("k", [b"x", b"y"])` | `get == b"xy"` |
| Backend explicit memory uri | `ObjectStoreBackend("memory://", store=SyncObjectStore("memory://"))` | `store_uri == "memory://"` |
| Backend no uri | `ObjectStoreBackend(store=SyncObjectStore("memory://"))` | `store_uri == "memory://"`, `protocol == "memory"` |

## Verification

- `uv run pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`
- `uv run ruff check src/py/vertebra/storage/_store.py src/py/vertebra/extensions/sqlspec/storage.py && uv run mypy src/py/vertebra/storage/_store.py src/py/vertebra/extensions/sqlspec/storage.py`

## Acceptance Criteria

- [ ] `put_multipart` streams without building a list of every part (R2).
- [ ] No literal `"memory://"` comparison remains in `ObjectStoreBackend` (R2).

## Notes & Discoveries

- [2026-11-12 01:30] Existing integration tests construct backends with `"memory://"` URIs; they keep passing because explicit URIs now always win.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Facade multipart and stream methods.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Constructor.
