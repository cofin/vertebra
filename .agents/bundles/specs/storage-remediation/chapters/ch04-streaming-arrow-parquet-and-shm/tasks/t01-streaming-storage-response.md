---
type: Task
id: "storage-remediation:ch04:t01"
chapter_id: ch04-streaming-arrow-parquet-and-shm
title: Stream StreamingStorageResponse.iter_chunks() and range-slice to_fastpath_tuple() without full-blob materialization
description: Stream HTTP 200 responses via get_stream and HTTP 206 responses via bounded get_range chunks in StreamingStorageResponse.iter_chunks(), and slice HTTP 206 bodies via get_range in to_fastpath_tuple().
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/storage/_streaming.py
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_storage_store.py
files:
  - src/py/vertebra/storage/_streaming.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 80
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
claimed_by: executor
claimed_at: "2026-11-09T00:46:30Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:46:30Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: py-streaming
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: StreamingStorageResponse chunk and fastpath methods
tags:
  - storage
  - streaming
  - http
  - tdd
---

# Task t01: True Streaming `StreamingStorageResponse`

## Objective & Scope Boundary

Update `src/py/vertebra/storage/_streaming.py` [^py-streaming] (`1` file) so `StreamingStorageResponse.iter_chunks(self, chunk_size: int | None = None) -> Iterator[memoryview]` defaults `step = self.chunk_size if chunk_size is None else chunk_size`, streams `200` responses via `self.store.get_stream(self.key, chunk_size=step)` and `206` responses via bounded `self.store.get_range` windows (`<= step`) yielding zero-copy `memoryview` slices, and `to_fastpath_tuple()` uses `self.store.get_range(self.key, start, end - start)` on `206` responses instead of loading the entire blob with `get_view()` while preserving `list[tuple[bytes, bytes]]` headers.

Out of scope: `_arrow.py` and `db/arrow.py` (`t02`, `t03`).

## Bounded Read Manifest

- `src/py/vertebra/storage/_streaming.py` — `StreamingStorageResponse` [^py-streaming].
- `src/py/vertebra/storage/_store.py` — `SyncObjectStore.get_stream` and `get_range` contracts.
- `src/py/tests/unit/storage/test_storage_store.py` — `StreamingStorageResponse` unit tests.

## Symbol & Signature Contracts

- `StreamingStorageResponse.iter_chunks(self, chunk_size: int | None = None) -> Iterator[memoryview]`
- `StreamingStorageResponse.to_fastpath_tuple(self) -> tuple[int, list[tuple[bytes, bytes]], bytes | memoryview]`

## Edge Cases & Error Semantics

- `chunk_size is not None and chunk_size <= 0` raises `ValueError("chunk_size must be positive")`.
- `status_code == 416` or (`total_size == 0` and `data is None`) yields zero chunks and returns `b""` from `to_fastpath_tuple()` without calling `get_view`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| `200` streaming chunks | `256 KiB` object, `resp.iter_chunks(chunk_size=65536)` with `get_view` forbidden/asserted uncalled | Yields `4` `memoryview` chunks reconstructing exact `256 KiB` payload without calling `get_view`. |
| `206` range chunks & `to_fastpath_tuple()` | `Range: bytes=10-25` (`16` bytes), `iter_chunks(chunk_size=6)` and `to_fastpath_tuple()` | Yields `[6B, 6B, 4B]` and `to_fastpath_tuple()` returns `(206, bytes_headers, 16B)` via `get_range` only. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/storage/_streaming.py src/py/tests/unit/storage/test_storage_store.py
uv run --no-sync mypy src/py/vertebra/storage/_streaming.py
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -k streaming -q
```

## Acceptance Criteria

- [ ] `StreamingStorageResponse.iter_chunks()` and `to_fastpath_tuple()` never materialize full blobs for `206` range requests or streamed `200` responses (R4.1).

## Notes & Discoveries

- [2026-11-08 20:55] Yield `memoryview(chunk)` directly over `StorageBuffer` or range buffers so `Iterator[memoryview]` stays zero-copy and compatible with `aiter_chunks()`.

[^py-streaming]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — `StreamingStorageResponse` chunk and fastpath methods.
