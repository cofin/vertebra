---
type: Task
id: "storage-completeness:ch03:t01"
chapter_id: ch03-litestar-and-range-streaming
title: Implement RFC 9110 Range/206/416 StreamingStorageResponse and async chunk/fastpath delivery
description: Add RFC 9110 single-range parsing (200, 206 Partial Content, 416 Range Not Satisfiable) and non-blocking AsyncObjectStore streaming/fastpath delivery to vertebra.storage._streaming.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/storage/_streaming.py
  - src/py/vertebra/storage/__init__.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/server/test_zero_copy_buffer.py
  - .agents/skills/python-framework/SKILL.md
files:
  - src/py/vertebra/storage/_streaming.py
  - src/py/vertebra/storage/__init__.py
tests:
  - src/py/tests/unit/server/test_zero_copy_buffer.py
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 180
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
  - id: streaming-resp
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: StreamingStorageResponse zero-copy HTTP response wrapper
  - id: storage-pkg-init
    resource: src/py/vertebra/storage/__init__.py
    title: Public exports of vertebra.storage
tags:
  - storage
  - http
  - streaming
  - zero-copy
---

# Task t01: Implement RFC 9110 `Range`/`206`/`416` `StreamingStorageResponse` and Async Chunk/Fastpath Delivery

## Objective & Scope Boundary

Upgrade `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` (and public exports in `src/py/vertebra/storage/__init__.py`) to parse RFC 9110 `Range: bytes=...` headers, emit `200 OK`, `206 Partial Content` (`Content-Range: bytes <start>-<end>/<total>`), or `416 Range Not Satisfiable` (`Content-Range: bytes */<total>` with empty body), and stream/resolve fastpath tuples asynchronously over `AsyncObjectStore.get_stream` and `get_range` without blocking the event loop [^streaming-resp] [^storage-pkg-init].

## Bounded Read Manifest

- `src/py/vertebra/storage/_streaming.py` — Current `StreamingStorageResponse`, `aiter_chunks`, `iter_chunks`, and `to_fastpath_tuple` [^streaming-resp].
- `src/py/vertebra/storage/__init__.py` — Public symbols exported from `vertebra.storage` [^storage-pkg-init].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, `ObjectMeta`, and `StorageBuffer`.
- `src/py/tests/unit/server/test_zero_copy_buffer.py` — Zero-copy `StorageBuffer` and fastpath response tests.
- `.agents/skills/python-framework/SKILL.md` — `msgspec.Struct`, typing, and docstring conventions.

## Symbol & Signature Contracts

- `src/py/vertebra/storage/_streaming.py`:
  - `class ByteRange(msgspec.Struct, frozen=True, gc=False)`: `start: int`, `end_exclusive: int`, `total_size: int` (`content_length` and `content_range_header` properties).
  - `parse_byte_range(range_header: str | None, total_size: int) -> ByteRange | None` (raises `RangeNotSatisfiableError(total_size=total_size)` on malformed `bytes=` syntax, multi-range `,`, `start >= total_size`, `start > end`, `-0`, or `total_size == 0` with a range header).
  - `StreamingStorageResponse`:
    - `classmethod async def from_async_store(cls, store: AsyncObjectStore, key: str, *, range_header: str | None = None, content_type: str | None = None, headers: Mapping[str, str] | None = None, chunk_size: int = 65536) -> StreamingStorageResponse` (calls `await store.head(key)` once, resolves `200` / `206` / `416`, populates `Accept-Ranges: bytes`, `ETag`, `Content-Length`, and `Content-Range`).
    - `classmethod def from_sync_store(cls, store: SyncObjectStore, key: str, *, range_header: str | None = None, content_type: str | None = None, headers: Mapping[str, str] | None = None, chunk_size: int = 65536) -> StreamingStorageResponse`.
    - `async def aiter_chunks(self) -> AsyncIterator[StorageBuffer | bytes]`: yields `b""` (zero chunks) on `416`, single/chunked `await store.get_range(...)` on `206`, and `store.get_stream(key)` on `200` over `AsyncObjectStore` without blocking.
    - `async def to_async_fastpath_tuple(self) -> tuple[int, list[tuple[str, str]], StorageBuffer | bytes]` alongside sync `to_fastpath_tuple(self)` (which raises `TypeError` if called synchronously on an `AsyncObjectStore` without pre-fetched buffer rather than blocking).

## Edge Cases & Error Semantics

- `Range: bytes=0-63` on a `100`-byte object -> status `206`, `Content-Range: bytes 0-63/100`, `Content-Length: 64`, body `data[0:64]`.
- `Range: bytes=60-999` on a `100`-byte object -> clamps `end` per RFC 9110 §14.1.2 to `99`: status `206`, `Content-Range: bytes 60-99/100`, `Content-Length: 40`, body `data[60:100]`.
- `Range: bytes=-16` on a `100`-byte object -> status `206`, `Content-Range: bytes 84-99/100`, `Content-Length: 16`, body `data[84:100]`.
- `Range: bytes=100-120`, `Range: bytes=50-20`, `Range: bytes=-0`, or `Range: bytes=0-1, 4-5` on a `100`-byte object -> status `416`, `Content-Range: bytes */100`, `Content-Length: 0`, empty body `b""`.
- Non-`bytes` unit (`Range: items=0-5`) is ignored per RFC 9110 §14.2 and served as `200 OK` with `Accept-Ranges: bytes`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/server/test_zero_copy_buffer.py` and `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Full `200 OK` async stream | 100-byte object `"video.mp4"`, `range_header=None` via `from_async_store` | Status `200`, `Accept-Ranges: bytes`, `Content-Length: 100`, `aiter_chunks` yields `StorageBuffer` chunks totaling 100 bytes. |
| Bounded, open-ended, & suffix `206` | `bytes=0-15`, `bytes=80-`, `bytes=-12` on 100-byte object | Status `206`, `Content-Range` = `bytes 0-15/100`, `bytes 80-99/100`, `bytes 88-99/100` with matching slice payloads. |
| Out-of-bounds / inverted / multi-range `416` | `bytes=100-110`, `bytes=40-10`, `bytes=0-2,4-6` on 100-byte object | Status `416`, `Content-Range: bytes */100`, `Content-Length: 0`, `b"".join([bytes(c) async for c in resp.aiter_chunks()]) == b""`. |
| Non-blocking `to_async_fastpath_tuple` | `await resp.to_async_fastpath_tuple()` on `206` range `0-31` | Returns `(206, headers, StorageBuffer)` with `len(buf) == 32` and zero `SyncObjectStore` calls. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/server/test_zero_copy_buffer.py -q`
- Final Green + lint + typecheck gate: `uv run --no-sync ruff check src/py/vertebra/storage src/py/tests/unit/server/test_zero_copy_buffer.py && uv run --no-sync mypy src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/server/test_zero_copy_buffer.py src/py/tests/unit/storage/test_storage_store.py -q`

## Acceptance Criteria

- [ ] `StreamingStorageResponse` supports RFC 9110 single-range `200`, `206`, and `416` responses with exact `Content-Range`, `Accept-Ranges`, and `Content-Length` headers (R7).
- [ ] Async streaming and fastpath resolution execute entirely on `AsyncObjectStore` without blocking event-loop threads (R7).

## Notes & Discoveries

- [2026-11-07 04:15] Export `ByteRange` and `RangeNotSatisfiableError` (if public) in `vertebra/storage/__init__.py` `__all__` sorted alphabetically.

[^streaming-resp]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — Storage HTTP streaming response.
[^storage-pkg-init]: `src/py/vertebra/storage/__init__.py` — Public storage package exports.
