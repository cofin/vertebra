---
type: Task
id: "storage-integrity-and-streaming:ch02:t03"
chapter_id: ch02-write-receipts-and-verified-reads
title: PyO3 bindings and IntegrityError for receipts and verified reads
description: Add vertebra.typing.IntegrityError, map StorageError::Integrity to it, expose PyObjectMeta.sha256, and forward checksum and expected_sha256 through the sync and async vertebra._storage write and read bindings and stub.
status: draft
state: pending
priority: P1
verification_strategy: tdd
depends_on:
  - "storage-integrity-and-streaming:ch02:t02"
read_set:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/_typing.py
  - .agents/bundles/specs/storage-integrity-and-streaming/chapters/ch02-write-receipts-and-verified-reads/spec.md
files:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/vertebra/_typing.py
tests: []
estimated_loc: 160
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
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: Error mapping, make_precondition_err, PyObjectMeta, put/create_multipart/put_file/get/get_file/get_stream bindings
  - id: py-typing
    resource: src/py/vertebra/_typing.py::PreconditionError
    title: Storage error classes re-exported through vertebra.typing
tags:
  - storage
  - pyo3
  - integrity
---

# Task t03: PyO3 Bindings and `IntegrityError` for Receipts and Verified Reads

## Objective & Scope Boundary

1. `_typing.py`: add `class IntegrityError(VertebraError, OSError)` next to `PreconditionError`, docstring "Raised when a SHA-256 receipt or verified read does not match the expected digest.", and export it wherever `PreconditionError` is exported [^py-typing].
2. `lib.rs`: add `make_integrity_err(message: String) -> PyErr` mirroring `make_precondition_err`; `to_py_err` maps `StorageError::Integrity { path, expected, actual }` to it. `PyObjectMeta` gains `#[pyo3(get)] sha256: Option<String>` copied from `StoredObjectMeta`. Sync and async `put`, `create_multipart`, `put_file` add keyword args `checksum=false, expected_sha256=None` forwarded into `PutConditionalOptions`. `get`, `get_file`, `get_stream` add `expected_sha256=None`; when set they call `get_verified`, `get_file_verified`, `get_stream_verified` [^lib-rs].
3. `_storage.pyi`: matching keyword arguments and `sha256: str | None` on the meta type.

Out of scope: Python facade (`t04`).

## Bounded Read Manifest

- `lib.rs` — `to_py_err`, `make_precondition_err`, `PyObjectMeta`, `PySyncObjectStore` and `PyAsyncObjectStore` write/read methods [^lib-rs].
- `_storage.pyi` — current binding signatures.
- `_typing.py` — `VertebraError`, `PreconditionError` [^py-typing].

## Symbol & Signature Contracts

```python
class IntegrityError(VertebraError, OSError): ...
```

```rust
#[pyo3(signature = (path, data, *, content_type=None, attributes=None, if_not_exists=false, if_match=None, if_none_match=None, checksum=false, expected_sha256=None))]
#[pyo3(signature = (path, *, expected_sha256=None))]
```

## Edge Cases & Error Semantics

- Integrity errors are raised as `IntegrityError`, never `RuntimeError` or `PreconditionError`.
- Hashing and verification run inside `py.detach` / the async runtime; no Python objects are held.
- Malformed digest maps through `InvalidInput` to `ValueError`.

## Public-Seam Test Blueprint

- **Canonical gate**: `uv run python -m mypy.stubtest vertebra._storage` (behavioral tests land with the facade in `t04`).

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Stub parity | `mypy.stubtest vertebra._storage` | exit `0` |
| Error class | `issubclass(vertebra.typing.IntegrityError, OSError)` | `True` |
| Native mismatch | `vertebra._storage` memory store `put("k", b"a", expected_sha256="0"*64)` | `IntegrityError` |

## Verification

- `make dev && uv run python -m mypy.stubtest vertebra._storage`
- `uv run python -c "from vertebra.typing import IntegrityError; assert issubclass(IntegrityError, OSError)"`
- `cd src/rs && cargo clippy -p vertebra-storage --all-targets -- -D warnings`

## Acceptance Criteria

- [ ] Bindings forward checksum options and map integrity failures to `IntegrityError` (R5).

## Notes & Discoveries

- [2026-11-12 01:30] `PreconditionError` is defined in `src/py/vertebra/_typing.py` and raised from Rust through `make_precondition_err`.

[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — Bindings.
[^py-typing]: `src/py/vertebra/_typing.py::PreconditionError` — Error classes.
