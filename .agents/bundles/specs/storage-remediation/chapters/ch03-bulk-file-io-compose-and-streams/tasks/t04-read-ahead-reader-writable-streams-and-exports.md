---
type: Task
id: "storage-remediation:ch03:t04"
chapter_id: ch03-bulk-file-io-compose-and-streams
title: Pass buffers zero-copy without memoryview wrapping, add 4 MiB read-ahead open_reader, and add streaming open_writer
description: Pass payloads directly into native put and put_multipart in _store.py so PyBytes stays zero-copy, add read_ahead=4_194_304 block caching to SyncReadableFile and AsyncReadableFile, implement SyncWritableFile and AsyncWritableFile (open_writer), and export PreconditionError, SyncWritableFile, and AsyncWritableFile in vertebra.storage.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch03:t03"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
  - src/py/tests/unit/storage/test_storage_store.py
files:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 230
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T00:23:20Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:32:30Z"
last_verified_commit: 19dac0d8
verification_evidence: "pytest src/py/tests/unit/storage (193 passed); ruff check/format and mypy on src/py/vertebra/storage clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:32:30Z"
commit: 19dac0d8
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T00:32:30Z"
    commit: 19dac0d8
sources:
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncReadableFile
    title: SyncReadableFile, AsyncReadableFile, and SyncObjectStore/AsyncObjectStore stream helpers
  - id: py-storage-init
    resource: src/py/vertebra/storage/__init__.py
    title: Public vertebra.storage exports
tags:
  - storage
  - streaming
  - ffi
  - tdd
---

# Task t04: Zero-Copy `put` Passthrough, `4 MiB` Read-Ahead `open_reader`, and Streaming `open_writer`

## Objective & Scope Boundary

Update `src/py/vertebra/storage/_store.py` [^py-store] and `src/py/vertebra/storage/__init__.py` [^py-storage-init] (`2` files):
1. Remove `memoryview(data).cast("B")` in `SyncObjectStore.put`, `SyncObjectStore.put_multipart`, `AsyncObjectStore.put`, and `AsyncObjectStore.put_multipart` so `bytes`, `bytearray`, `memoryview`, `StorageBuffer`, and NumPy buffers pass directly to `extract_buffer_bytes` (preserving `PyBackedBytes` zero-copy for `bytes` and computing `fallback_etag` from `native_meta.size` when backend omits an ETag).
2. Add `read_ahead: int = 4_194_304` (`>= 0`) to `SyncReadableFile`, `AsyncReadableFile`, `SyncObjectStore.open_reader`, and `AsyncObjectStore.open_reader` (`_buf: bytes`, `_buf_start: int` in `__slots__`), coalescing sub-`read_ahead` reads into a single `get_range` call per `4 MiB` window while bypassing the cache when `n >= read_ahead` or `read_ahead == 0`.
3. Implement `SyncWritableFile(io.RawIOBase)` and `AsyncWritableFile` plus `open_writer(self, path: str, *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, part_size: int = 8_388_608, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None)` on `SyncObjectStore` and `AsyncObjectStore` (rolling `bytearray` buffer flushing `part_size` slices to `create_multipart` `write_part`, completing on `close()` / `aclose()`, exposing `.result: PutResult | None`, and aborting on `.abort()` or unhandled `with` / `async with` exception).
4. Re-export `PreconditionError`, `SyncWritableFile`, and `AsyncWritableFile` from `vertebra.storage._store` and `vertebra.storage.__init__`.

Out of scope: `_arrow.py` Parquet options (`ch04:t02`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_store.py` — `SyncReadableFile`, `AsyncReadableFile`, `SyncObjectStore`, `AsyncObjectStore` [^py-store].
- `src/py/vertebra/storage/__init__.py` — `__all__` exports [^py-storage-init].
- `src/py/tests/unit/storage/test_storage_store.py` — Reader, writer, and buffer unit tests.

## Symbol & Signature Contracts

- `SyncReadableFile(store: SyncObjectStore, path: str, size: int, chunk_size: int = 65536, read_ahead: int = 4_194_304)`
- `AsyncReadableFile(store: AsyncObjectStore, path: str, size: int, chunk_size: int = 65536, read_ahead: int = 4_194_304)`
- `class SyncWritableFile(io.RawIOBase)` and `class AsyncWritableFile`
- `vertebra.storage.__all__` includes `"AsyncWritableFile"`, `"PreconditionError"`, `"SyncWritableFile"`.

## Edge Cases & Error Semantics

- `read_ahead < 0` or `part_size <= 0` raises `ValueError`.
- Writing `0` bytes and closing `open_writer` completes cleanly and creates a `0`-byte object (`PutResult`).
- Writing from a `bytearray` into `SyncWritableFile.write(buf)` / `await AsyncWritableFile.write(buf)` and immediately calling `buf.clear()` succeeds with zero `BufferError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `open_reader` `4 MiB` read-ahead coalescing | Spy/loopback range counter on `1 MiB` object; `16` sequential `4 KiB` reads + backward seek within window | Issues `1` `get_range` call total (`read_ahead=0` issues `16` calls). |
| `SyncWritableFile` & `AsyncWritableFile` multi-part flush + `io.BufferedWriter` / `gzip` | Write `20 MiB` across `part_size=8_388_608` using reusable `bytearray` | Completes upload with `result.etag`, `store.get` matches exact `20 MiB`, and `bytearray.clear()` never fails. |
| `open_writer` exception abort | Raise `ValueError("boom")` inside `with store.open_writer("aborted.bin"):` | Exception propagates and `store.exists("aborted.bin")` is `False`. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_store.py src/py/vertebra/storage/__init__.py src/py/tests/unit/storage/test_storage_store.py
uv run --no-sync mypy src/py/vertebra/storage/_store.py src/py/vertebra/storage/__init__.py
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q
```

## Acceptance Criteria

- [x] [19dac0d8] `put` and `put_multipart` forward buffers directly without `memoryview` wrapping (R3.4).
- [x] [19dac0d8] `open_reader` caches `read_ahead` range windows, `open_writer` streams multipart writes with automatic abort-on-error, and `PreconditionError`, `SyncWritableFile`, and `AsyncWritableFile` are exported in `vertebra.storage` (R3.4).

## Notes & Discoveries

- [2026-11-09 00:23] Executes in parallel with `t01` ahead of `t02`/`t03` because it touches only Python reader/writer/export seams with no dependency on the new native bulk-I/O bindings.
- [2026-11-09 00:32] Native `extract_buffer_bytes` accepts only byte-format (`B`/`b`/`c`) buffers, so `_BaseObjectStore._native_payload` passes `bytes`, `bytearray`, and `B`-format views unchanged and casts typed buffers (`pyarrow.Buffer`, `array.array`, NumPy) to a flat `B` view. `_arrow.py` Parquet batch readers pass `read_ahead=0` because pyarrow issues exact range reads and row-group demand paging must stay bounded.
- [2026-11-08 20:55] `SyncWritableFile` inherits `io.RawIOBase` (`writable() -> True`) so standard library `gzip.GzipFile(fileobj=wf, mode="wb")` and `io.TextIOWrapper(io.BufferedWriter(wf))` work out of the box.

[^py-store]: `src/py/vertebra/storage/_store.py::SyncReadableFile` — `SyncReadableFile`, `AsyncReadableFile`, and `SyncObjectStore`/`AsyncObjectStore` stream helpers.
[^py-storage-init]: `src/py/vertebra/storage/__init__.py` — Public `vertebra.storage` exports.
