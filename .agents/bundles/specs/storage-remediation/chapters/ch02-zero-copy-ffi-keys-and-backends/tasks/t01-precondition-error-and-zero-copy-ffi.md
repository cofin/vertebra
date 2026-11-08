---
type: Task
id: "storage-remediation:ch02:t01"
chapter_id: ch02-zero-copy-ffi-keys-and-backends
title: Define PreconditionError in vertebra.typing, map 412 preconditions in to_py_err, and use zero-copy PyBackedBytes for immutable PyBytes
description: Add PreconditionError(VertebraError, OSError) to vertebra._typing and vertebra.typing, map StorageError::Precondition and object_store::Error::Precondition to PreconditionError in vertebra-storage, and wrap immutable PyBytes zero-copy via PyBackedBytes while copying mutable buffers synchronously.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/tests/unit/storage/test_storage_store.py
files:
  - src/py/vertebra/_typing.py
  - src/py/vertebra/typing.py
  - src/rs/vertebra-storage/src/lib.rs
tests:
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 110
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: agent:executor
claimed_at: "2026-11-08T23:21:40Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-08T23:24:49Z"
last_verified_commit: d889b958
verification_evidence: "maturin develop + ruff check + mypy + pytest test_storage_store.py (124 passed) + cargo test test_pyo3_sync_and_async_streaming_multipart_conditional_pagination_and_rename"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-08T23:25:15Z"
commit: d889b958
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: py-typing
    resource: src/py/vertebra/_typing.py::VertebraError
    title: Base exception definitions in vertebra._typing
  - id: py-typing-facade
    resource: src/py/vertebra/typing.py::VertebraError
    title: Public vertebra.typing facade
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes
    title: PyO3 buffer extraction and error mapping in vertebra-storage
tags:
  - storage
  - ffi
  - preconditions
  - tdd
---

# Task t01: Define `PreconditionError` and Zero-Copy `PyBackedBytes` FFI Extraction

## Objective & Scope Boundary

Define `PreconditionError(VertebraError, OSError)` in `src/py/vertebra/_typing.py` [^py-typing] and re-export it in `src/py/vertebra/typing.py` [^py-typing-facade]; update `to_py_err` and `extract_buffer_bytes` in `src/rs/vertebra-storage/src/lib.rs` [^storage-lib-rs] so conditional `412` mismatches raise `vertebra.typing.PreconditionError` and immutable `PyBytes` buffers cross FFI zero-copy via `Bytes::from_owner(pyo3::backed::PyBackedBytes::from(...))` while mutable `PyBuffer<u8>` inputs copy synchronously before dropping the buffer view.

Out of scope: `_store.py` re-export of `PreconditionError` and removal of `memoryview(data).cast("B")` in `_store.py` (`ch03:t04`).

## Bounded Read Manifest

- `src/py/vertebra/_typing.py` — `VertebraError` and exception exports [^py-typing].
- `src/py/vertebra/typing.py` — public `vertebra.typing` re-exports [^py-typing-facade].
- `src/rs/vertebra-storage/src/lib.rs` — `to_py_err` and `extract_buffer_bytes` [^storage-lib-rs].
- `src/py/tests/unit/storage/test_storage_store.py` — conditional put and buffer protocol unit tests.

## Symbol & Signature Contracts

- `src/py/vertebra/_typing.py` & `src/py/vertebra/typing.py`:
  - `class PreconditionError(VertebraError, OSError): """Raised when a conditional storage or resource precondition fails."""` in `__all__`.
- `src/rs/vertebra-storage/src/lib.rs`:
  - `extract_buffer_bytes(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Bytes>`:
    1. `data.cast::<StorageBuffer>()` -> `Ok(storage_buf.borrow().data.clone())`.
    2. `data.cast::<pyo3::types::PyBytes>()` -> `Ok(Bytes::from_owner(pyo3::backed::PyBackedBytes::from(py_bytes.clone())))`.
    3. `PyBuffer::<u8>::get(data)` -> verify `buf.is_c_contiguous()` (else `PyBufferError`), copy via `Bytes::from(buf.to_vec(py)?)`, drop `buf`, return `Ok(bytes)`.
  - `to_py_err(err: StorageError) -> PyErr`: map `StorageError::Precondition { path, message }` and `StorageError::ObjectStore(object_store::Error::Precondition { .. })` to `vertebra.typing.PreconditionError` via `Python::attach(|py| ...)`.

## Edge Cases & Error Semantics

- Mutating or calling `.clear()` on a `bytearray` immediately after `native_store.put("k", buf)` or `upload.write_part(buf)` must succeed without `BufferError` and leave stored bytes intact.
- Non-contiguous `memoryview(b"abcdef")[::2]` raises `BufferError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Conditional `if_match` mismatch raises `PreconditionError` | `store.put("k", b"v1")`; `store.put("k", b"v2", if_match="wrong-etag")` | Raises `PreconditionError` (`isinstance(exc, (VertebraError, OSError))` and `not isinstance(exc, PermissionError)`). |
| Mutable `bytearray` cleared after native `put` & `write_part` | `buf = bytearray(b"hello"); native.put("k", buf); buf.clear()` | `buf == bytearray()` and `native.get("k") == b"hello"`. |
| Strided `memoryview` rejected | `native.put("k", memoryview(b"abcdef")[::2])` | Raises `BufferError`. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target uv run --offline --no-sync maturin develop --uv --offline --manifest-path src/rs/vertebra-storage/Cargo.toml
uv run --no-sync ruff check src/py/vertebra/_typing.py src/py/vertebra/typing.py src/py/tests/unit/storage/test_storage_store.py
uv run --no-sync mypy src/py/vertebra/_typing.py src/py/vertebra/typing.py
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q
```

## Acceptance Criteria

- [x] `PreconditionError` is exported from `vertebra.typing` and raised on native `412` precondition failures (R2.1).
- [x] `extract_buffer_bytes` wraps `PyBytes` in `PyBackedBytes` zero-copy and copies mutable `PyBuffer<u8>` inputs synchronously before dropping the buffer view (R2.1).

## Notes & Discoveries

- [2026-11-08 20:55] Existing tests in `test_storage_store.py` expecting `PermissionError` on `if_match` / `if_none_match` mismatches are updated to assert `PreconditionError`.

[^py-typing]: `src/py/vertebra/_typing.py::VertebraError` — Base exception definitions in `vertebra._typing`.
[^py-typing-facade]: `src/py/vertebra/typing.py::VertebraError` — Public `vertebra.typing` facade.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::extract_buffer_bytes` — PyO3 buffer extraction and error mapping in `vertebra-storage`.
