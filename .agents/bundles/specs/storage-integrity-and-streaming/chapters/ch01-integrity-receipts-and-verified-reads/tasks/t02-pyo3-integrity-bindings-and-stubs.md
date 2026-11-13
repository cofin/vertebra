---
type: Task
id: "storage-integrity-and-streaming:ch01:t02"
flow_id: storage-integrity-and-streaming
chapter_id: ch01-integrity-receipts-and-verified-reads
title: "Add PyO3 Integrity Bindings, _typing.IntegrityError, and _storage.pyi Stubs"
description: "Define IntegrityError in _typing.py, map StorageError::Integrity and checksum/receipt attributes in vertebra-storage/src/lib.rs, and update _storage.pyi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "storage-integrity-and-streaming:ch01:t01"
files:
  - src/py/vertebra/_typing.py
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
tests:
  - src/py/tests/unit/framework/test_typing.py
read_set:
  - src/py/vertebra/_typing.py
  - src/rs/vertebra-storage/src/store.rs
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/tests/unit/framework/test_typing.py
estimated_loc: 180
tags:
  - storage
  - pyo3
  - stubs
  - integrity
sources:
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: "make_precondition_err, to_py_err, PyObjectMeta, and sync/async store bindings"
  - id: py-private-typing
    resource: src/py/vertebra/_typing.py::PreconditionError
    title: "Storage exception hierarchy in vertebra._typing"
---

# Task `t02`: Add PyO3 Integrity Bindings, `_typing.IntegrityError`, and `_storage.pyi` Stubs

## Objective & Scope Boundary

Define `IntegrityError(VertebraError, OSError)` in `src/py/vertebra/_typing.py`, wire `make_integrity_err`, `PyObjectMeta.{sha256,crc32c,md5}`, and `checksum` / `expected_{sha256,crc32c,md5}` keyword arguments across `PySyncObjectStore` and `PyAsyncObjectStore` in `src/rs/vertebra-storage/src/lib.rs`, and update `src/py/vertebra/_storage.pyi` per [`../spec.md` §2](../spec.md) [^lib-rs] [^py-private-typing]. Out of scope: `src/py/vertebra/typing.py` and `vertebra.storage` Python facades (`t03`).

## Bounded Read Manifest

- `src/py/vertebra/_typing.py` — `VertebraError` and `PreconditionError` [^py-private-typing].
- `src/rs/vertebra-storage/src/store.rs` — `PutConditionalOptions`, `StoredObjectMeta`, and `StorageError::Integrity` from `t01`.
- `src/rs/vertebra-storage/src/lib.rs` — `make_precondition_err`, `to_py_err`, `PyObjectMeta`, and `put`/`create_multipart`/`put_file`/`get`/`get_file`/`get_stream` bindings [^lib-rs].
- `src/py/vertebra/_storage.pyi` — native stub declarations.
- `src/py/tests/unit/framework/test_typing.py` — typing exception hierarchy tests.

## Contracts & Edge Cases

- `src/py/vertebra/_typing.py`: add `class IntegrityError(VertebraError, OSError)` alongside `PreconditionError` and include `"IntegrityError"` in `_typing.__all__`.
- `src/rs/vertebra-storage/src/lib.rs`: add `make_integrity_err` importing `vertebra.typing.IntegrityError` (mirroring `make_precondition_err`); map `StorageError::Integrity` in `to_py_err`; expose `#[pyo3(get)]` `sha256`, `crc32c`, and `md5` on `PyObjectMeta`; add `checksum=false, expected_sha256=None, expected_crc32c=None, expected_md5=None` keyword args on sync & async `put`, `create_multipart`, and `put_file`, and `expected_sha256=None` on `get`, `get_file`, and `get_stream` (detaching the GIL for all hashing/I/O).
- `src/py/vertebra/_storage.pyi`: match every PyO3 signature and `ObjectMeta` property so `mypy.stubtest vertebra._storage` exits `0`.

## Test Matrix

- Assert in `src/py/tests/unit/framework/test_typing.py` that `_typing.IntegrityError` subclasses both `VertebraError` and `OSError`; verify `mypy.stubtest vertebra._storage` passes against the compiled `_storage` extension.

## Verification

```bash
uv run --no-sync maturin develop --manifest-path src/rs/vertebra-storage/Cargo.toml --uv && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py -q
```

## Acceptance Criteria

- [ ] `_storage` bindings forward checksum/expected-digest kwargs, expose `sha256`/`crc32c`/`md5` on `PyObjectMeta`, map `StorageError::Integrity` to `IntegrityError`, and pass `mypy.stubtest vertebra._storage` (`R3`).
- [ ] Zero regressions in `test_typing.py` and Clippy.

[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — `make_precondition_err`, `to_py_err`, `PyObjectMeta`, and sync/async store bindings.
[^py-private-typing]: `src/py/vertebra/_typing.py::PreconditionError` — Storage exception hierarchy in `vertebra._typing`.
