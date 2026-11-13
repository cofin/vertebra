---
type: Task
id: "storage-integrity-and-streaming:ch01:t03"
flow_id: storage-integrity-and-streaming
chapter_id: ch01-integrity-receipts-and-verified-reads
title: "Re-Export IntegrityError in vertebra.typing and Wire Python Storage Facade Receipts"
description: "Explicitly re-export IntegrityError in src/py/vertebra/typing.py (__all__) and vertebra.storage (__all__), and wire sha256/crc32c/md5 receipts and verified reads across SyncObjectStore and AsyncObjectStore."
status: stable
state: completed
commit: 85849b32
last_verified_commit: 85849b32
last_verified_at: "2026-11-13T02:21:00Z"
verification_evidence: "cargo test -p vertebra-storage (47 passed) + clippy + stubtest + mypy + pytest (485 passed, 1 skipped) + review-check (exit 0)"
claimed_by: null
claimed_at: null
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch01:t02"
files:
  - src/py/vertebra/typing.py
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/framework/test_typing.py
  - src/py/tests/unit/storage/test_storage_store.py
read_set:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 235
tags:
  - storage
  - typing
  - integrity
  - python
sources:
  - id: py-public-typing
    resource: src/py/vertebra/typing.py
    title: "Explicit public typing facade and __all__ tuple"
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::PutResult
    title: "ObjectMeta, PutResult, SyncObjectStore, AsyncObjectStore, open_writer, and open_reader"
  - id: storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: "Public vertebra.storage exports"
---

# Task `t03`: Re-Export `IntegrityError` in `vertebra.typing` and Wire Python Storage Facade Receipts

## Objective & Scope Boundary

Explicitly import and re-export `IntegrityError` in `src/py/vertebra/typing.py` (`__all__`) and `src/py/vertebra/storage/{_store,__init__}.py` (`__all__`), add `sha256`, `crc32c`, and `md5` fields to `ObjectMeta` / `PutResult`, and forward `checksum` and `expected_*` options across `SyncObjectStore` and `AsyncObjectStore` per [`../spec.md` §2](../spec.md) [^py-public-typing] [^py-store] [^storage-init]. Out of scope: SQLSpec, Alchemy, and Django storage integrations (`ch02`).

## Bounded Read Manifest

- `src/py/vertebra/_typing.py` and `src/py/vertebra/typing.py` — `IntegrityError` export list and `__all__` [^py-public-typing].
- `src/py/vertebra/_storage.pyi` — native keyword signatures from `t02`.
- `src/py/vertebra/storage/_store.py` — `ObjectMeta`, `PutResult`, `_native_to_object_meta`, `_native_to_put_result`, `SyncObjectStore`, `AsyncObjectStore`, `SyncWritableFile`/`AsyncWritableFile`, and `SyncReadableFile`/`AsyncReadableFile` [^py-store].
- `src/py/vertebra/storage/__init__.py` — `__all__` re-exports [^storage-init].
- `src/py/tests/unit/storage/test_storage_store.py` — parametrized sync/async store tests.

## Contracts & Edge Cases

- `src/py/vertebra/typing.py`: explicitly import `IntegrityError` from `vertebra._typing` and list `"IntegrityError"` in `__all__` so `from vertebra.typing import IntegrityError` and `lib.rs::make_integrity_err` resolve `IntegrityError(VertebraError, OSError)` (distinct from `vertebra.db.IntegrityError(DatabaseError)`). Also re-export `IntegrityError` in `vertebra.storage._store` (`__all__`) and `vertebra.storage.__init__` (`__all__`).
- `ObjectMeta` & `PutResult`: add `sha256: str | None = None`, `crc32c: str | None = None`, and `md5: str | None = None` mapped from native metadata.
- `SyncObjectStore` & `AsyncObjectStore`: add `checksum: bool = False, expected_sha256: str | None = None, expected_crc32c: str | None = None, expected_md5: str | None = None` to `put`, `put_stream`, `put_multipart`, `put_file`, and `open_writer` (raising `IntegrityError` on `close()`/`aclose()` mismatch after aborting), and `expected_sha256: str | None = None` to `get`, `get_file`, and `open_reader` (raising `IntegrityError` on EOF read when mismatched; early close raises no error).

## Test Matrix

- Parametrized tests in `test_typing.py` and `test_storage_store.py` across `("sync", "async")` and `("put", "put_stream", "put_multipart", "put_file", "open_writer")`: `vertebra.typing.IntegrityError is vertebra.storage.IntegrityError` and `not issubclass(vertebra.typing.IntegrityError, vertebra.db.DatabaseError)`; `checksum=True` matches `hashlib.sha256`, `hashlib.md5`, and Castagnoli CRC32C; mismatched `expected_sha256`/`expected_crc32c`/`expected_md5` raises `IntegrityError` (never falling back to bare `OSError`) and preserves prior object bytes; verified `get`, `get_file` (leaving destination absent on failure), and `open_reader` pass/fail/early-stop cases; malformed hex `ValueError`; default `checksum=False` returns `None` receipts.

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/typing.py src/py/vertebra/storage src/py/tests/unit/storage/test_storage_store.py && uv run --no-sync mypy src/py/vertebra/typing.py src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/storage/test_storage_store.py -k "IntegrityError or sha256 or crc32c or md5 or verified" -q
```

## Acceptance Criteria

- [x] `from vertebra.typing import IntegrityError` and `from vertebra.storage import IntegrityError` resolve cleanly, and sync/async facades enforce receipts and verified reads across all write/read paths (`R1`–`R3`).
- [x] Zero regressions in the chapter's fast verification gate.

[^py-public-typing]: `src/py/vertebra/typing.py` — Explicit public typing facade and `__all__` tuple.
[^py-store]: `src/py/vertebra/storage/_store.py::PutResult` — `ObjectMeta`, `PutResult`, `SyncObjectStore`, `AsyncObjectStore`, `open_writer`, and `open_reader`.
[^storage-init]: `src/py/vertebra/storage/__init__.py` — Public `vertebra.storage` exports.
