---
type: Task
id: "storage-integrity-and-streaming:ch02:t04"
chapter_id: ch02-write-receipts-and-verified-reads
title: Python facade receipts and verified reads
description: Add PutResult.sha256 and checksum/expected_sha256 keyword arguments to every SyncObjectStore and AsyncObjectStore write path and expected_sha256 to get, get_file, and open_reader, with parametrized receipt and mismatch tests.
status: draft
state: pending
priority: P1
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch02:t03"
read_set:
  - src/py/vertebra/storage/_store.py
  - src/py/vertebra/_storage.pyi
  - src/py/tests/unit/storage/test_storage_store.py
  - .agents/bundles/specs/storage-integrity-and-streaming/chapters/ch02-write-receipts-and-verified-reads/spec.md
files:
  - src/py/vertebra/storage/_store.py
  - src/py/tests/unit/storage/test_storage_store.py
tests:
  - src/py/tests/unit/storage/test_storage_store.py::test_sha256_receipts
  - src/py/tests/unit/storage/test_storage_store.py::test_sha256_mismatch_preserves_prior_object
  - src/py/tests/unit/storage/test_storage_store.py::test_verified_reads
estimated_loc: 230
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
    resource: src/py/vertebra/storage/_store.py::PutResult
    title: PutResult, _native_to_put_result, SyncObjectStore, AsyncObjectStore, open_writer, open_reader
tags:
  - storage
  - integrity
---

# Task t04: Python Facade Receipts and Verified Reads

## Objective & Scope Boundary

In `_store.py` [^py-store]:

1. `PutResult` gains `sha256: str | None = None` with docstring "Lowercase hex SHA-256 of the bytes sent, present only when `checksum=True` or `expected_sha256` is set."; `_native_to_put_result` copies `native_meta.sha256`.
2. `SyncObjectStore` and `AsyncObjectStore`: `put`, `put_stream`, `put_multipart`, `put_file`, `open_writer` accept `checksum: bool = False, expected_sha256: str | None = None` and forward them to the native call (`open_writer` forwards into the lazily created multipart upload). `get`, `get_file`, `open_reader` accept `expected_sha256: str | None = None`; `open_reader` passes it to `get_stream` and raises `IntegrityError` on the read that reaches end of object.
3. Docstrings list `IntegrityError` under `Raises:`.

Out of scope: integration backends (ch03/t01).

## Bounded Read Manifest

- `_store.py` — `PutResult`, `_native_to_put_result`, write and read methods on both facades, `SyncWritableFile`, `SyncReadableFile`.
- `_storage.pyi` — binding signatures from `t03`.
- `test_storage_store.py` — memory store fixtures.

## Symbol & Signature Contracts

```python
def put_stream(self, path: str, chunks: Iterable[Buffer], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None, checksum: bool = False, expected_sha256: str | None = None) -> PutResult: ...
def get_file(self, path: str, file_path: str | Path, *, expected_sha256: str | None = None) -> ObjectMeta: ...
```

## Edge Cases & Error Semantics

- `open_writer(..., expected_sha256=d)` closed after writing mismatching bytes: `close()` raises `IntegrityError`; upload aborted.
- `open_reader` with early stop: no error.
- Default arguments: no behavior change, `sha256 is None`.

## Public-Seam Test Blueprint

- **Canonical gate**: `uv run pytest src/py/tests/unit/storage/test_storage_store.py -k "sha256 or verified" -v`, parametrized over `mode in ("sync", "async")` and `method in ("put", "put_stream", "put_multipart", "put_file", "open_writer")`.

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Receipt | payload `b"vertebra" * 1000`, `checksum=True` | `result.sha256 == hashlib.sha256(payload).hexdigest()` |
| Mismatch keeps prior | `put("k", b"old")`; write `b"new"` with `expected_sha256=hashlib.sha256(b"x").hexdigest()` | `IntegrityError`; `get("k") == b"old"` |
| Verified get ok | `get("k", expected_sha256=hashlib.sha256(b"old").hexdigest())` | `b"old"` |
| Verified get bad | `get("k", expected_sha256="0" * 64)` | `IntegrityError` |
| get_file bad | `get_file("k", tmp_path / "o", expected_sha256="0" * 64)` | `IntegrityError`; `not (tmp_path / "o").exists()` |
| Reader bad | `open_reader("k", expected_sha256="0" * 64).read()` | `IntegrityError` |
| Malformed | `expected_sha256="ABC"` | `ValueError` |
| Default | `put("k", b"a")` | `sha256 is None` |

## Verification

- `make dev && uv run pytest src/py/tests/unit/storage/test_storage_store.py -k "sha256 or verified" -v`
- `uv run mypy src/py/vertebra/storage/_store.py && uv run ruff check src/py/vertebra/storage/_store.py`

## Acceptance Criteria

- [ ] Receipts match `hashlib.sha256` for single, multipart, stream, file, and writer paths (R5).
- [ ] Mismatched digests leave prior objects intact; corrupted reads raise before returning bytes (R5).

## Notes & Discoveries

- [2026-11-12 01:30] `put_multipart` delegates to `put_stream` after ch01/t02, so it inherits the options.

[^py-store]: `src/py/vertebra/storage/_store.py::PutResult` — Facade.
