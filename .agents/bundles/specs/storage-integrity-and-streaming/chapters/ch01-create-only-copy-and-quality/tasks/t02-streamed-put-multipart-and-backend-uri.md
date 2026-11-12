---
type: Task
id: "storage-integrity-and-streaming:ch01:t02"
chapter_id: ch01-create-only-copy-and-quality
title: Stream put_multipart and remove the backend URI sentinel
description: Reimplement SyncObjectStore.put_multipart and AsyncObjectStore.put_multipart on put_stream so parts are not materialized into a list, and drop the literal memory:// sentinel from ObjectStoreBackend.__init__.
status: stable
state: in_progress
priority: P2
verification_strategy: regression_tdd
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
  - src/py/tests/unit/extensions/test_data_storage_integrations.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py::test_put_multipart_streams_parts
  - src/py/tests/unit/storage/test_storage_store.py::test_put_multipart_empty_and_failed_iterables
  - src/py/tests/unit/extensions/test_data_storage_integrations.py::test_sqlspec_backend_store_uri_resolution
estimated_loc: 90
plan_revision: 2
plan_commit: c368cb00
state_revision: 0
claimed_by: agent:multipart-executor
claimed_at: "2026-11-12T02:24:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-12T02:58:52Z"
last_verified_commit: null
verification_evidence: "Red: reused mutable chunks fail sync/async and explicit memory URI fails (3 failures). Green: canonical storage/integration modules 434 passed, 1 existing skip; post-restore focused regressions 8 passed; Ruff check/format and focused Mypy exit 0."
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T02:58:52Z"
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
| Reused mutable parts | one `bytearray` overwritten with `b"aaaa"`, `b"bbbb"`, `b"cccc"` between yields | `get("parts") == b"aaaabbbbcccc"` |
| Generator raises mid-stream | generator yields `b"a"` then raises `RuntimeError("boom")` | `RuntimeError("boom")`; `exists("k2") is False` |
| Async parity | `await AsyncObjectStore("memory://").put_multipart("k", [b"x", b"y"])` | `get == b"xy"` |
| Backend explicit memory uri | `ObjectStoreBackend("memory://", store=SyncObjectStore.from_local(tmp_path))` | `store_uri == "memory://"` |
| Backend no uri | `ObjectStoreBackend(store=SyncObjectStore("memory://"))` | `store_uri == "memory://"`, `protocol == "memory"` |

## Verification

- `uv run pytest src/py/tests/unit/storage/test_storage_store.py src/py/tests/unit/extensions/test_data_storage_integrations.py -q`
- `uv run ruff check src/py/vertebra/storage/_store.py src/py/vertebra/extensions/sqlspec/storage.py && uv run mypy src/py/vertebra/storage/_store.py src/py/vertebra/extensions/sqlspec/storage.py`

## Acceptance Criteria

- [x] `put_multipart` streams without building a list of every part (R2).
- [x] No literal `"memory://"` comparison remains in `ObjectStoreBackend` (R2).

## Notes & Discoveries

- [2026-11-12 02:58] Real native storage tests prove the producer may reuse one mutable buffer between yields. Both sync and async preserve each yielded value; iterable failure preserves an existing object and leaves absent keys unpublished. Storage extension rebuilding resolves the stale constructor signature before behavioral validation.

- [2026-11-12 01:30] Existing integration tests construct backends with `"memory://"` URIs; they keep passing because explicit URIs now always win.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Facade multipart and stream methods.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — Constructor.
