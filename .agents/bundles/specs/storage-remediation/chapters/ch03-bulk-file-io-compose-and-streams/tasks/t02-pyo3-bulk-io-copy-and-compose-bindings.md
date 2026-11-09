---
type: Task
id: "storage-remediation:ch03:t02"
chapter_id: ch03-bulk-file-io-compose-and-streams
title: Expose PyO3 bindings and _storage.pyi stubs for get_file, get_files, put_files, conditional copy, and compose
description: Add get_file, get_files, put_files, copy(..., *, if_not_exists=False), and compose to PySyncObjectStore and PyAsyncObjectStore in lib.rs and _storage.pyi.
status: stable
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "storage-remediation:ch03:t01"
read_set:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
  - src/py/tests/unit/storage/test_storage_store.py
files:
  - src/rs/vertebra-storage/src/lib.rs
  - src/py/vertebra/_storage.pyi
tests:
  - src/py/tests/unit/storage/test_storage_store.py
estimated_loc: 150
plan_revision: 1
plan_commit: fd571e3a
state_revision: 1
claimed_by: executor
claimed_at: "2026-11-09T00:33:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:33:00Z"
commit: null
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified: []
sources:
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore
    title: PyO3 bindings on PySyncObjectStore and PyAsyncObjectStore
  - id: storage-pyi
    resource: src/py/vertebra/_storage.pyi::SyncObjectStore
    title: Type stubs for vertebra._storage
tags:
  - storage
  - ffi
  - pyo3
  - tdd
---

# Task t02: PyO3 Bindings & `_storage.pyi` Stubs for Bulk I/O, Conditional `copy`, and `compose`

## Objective & Scope Boundary

Expose `get_file`, `get_files`, `put_files`, `copy(..., *, if_not_exists: bool = False)`, and `compose` on `PySyncObjectStore` (using `py.detach` + `block_on_storage`) and `PyAsyncObjectStore` (using `spawn_rust_task` -> `RustTaskAwaitable`) in `src/rs/vertebra-storage/src/lib.rs` [^storage-lib-rs] and declare exact matching stubs in `src/py/vertebra/_storage.pyi` [^storage-pyi].

Out of scope: High-level Python `SyncObjectStore` / `AsyncObjectStore` methods (`t03`).

## Bounded Read Manifest

- `src/rs/vertebra-storage/src/lib.rs` — `PySyncObjectStore`, `PyAsyncObjectStore`, `spawn_rust_task` [^storage-lib-rs].
- `src/py/vertebra/_storage.pyi` — `SyncObjectStore`, `AsyncObjectStore` [^storage-pyi].
- `src/py/tests/unit/storage/test_storage_store.py` — Native store binding tests.

## Symbol & Signature Contracts

- `SyncObjectStore` (`_storage.pyi` / `PySyncObjectStore`):
  - `def get_file(self, path: str, file_path: str) -> ObjectMeta: ...`
  - `def get_files(self, pairs: list[tuple[str, str]], *, concurrency: int = 16) -> list[ObjectMeta]: ...`
  - `def put_files(self, pairs: list[tuple[str, str]], *, concurrency: int = 16) -> list[ObjectMeta]: ...`
  - `def copy(self, src: str, dst: str, *, if_not_exists: bool = False) -> None: ...`
  - `def compose(self, sources: list[str], dst: str, *, content_type: str | None = None) -> ObjectMeta: ...`
- `AsyncObjectStore` (`_storage.pyi` / `PyAsyncObjectStore`): identical parameters returning `RustTaskAwaitable`.

## Edge Cases & Error Semantics

- `concurrency <= 0` raises `ValueError` before entering Tokio.
- Sync calls detach the GIL (`py.detach`) for the full duration of file/network I/O (`FFI-01`).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/storage/test_storage_store.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Native `get_file`, `get_files`, `put_files`, `copy(if_not_exists=True)`, `compose` | `vertebra._storage.SyncObjectStore` and `AsyncObjectStore` over `file://` and `memory://` | Sync and async bindings round-trip files, enforce `FileExistsError` on `if_not_exists=True`, and compose multi-part objects; `mypy.stubtest vertebra._storage` passes. |

## Verification

```bash
PATH="$PWD/.venv/bin:$PATH" PYO3_PYTHON="$PWD/.venv/bin/python" CARGO_TARGET_DIR=/tmp/vtb-cargo-target uv run --offline --no-sync maturin develop --uv --offline --manifest-path src/rs/vertebra-storage/Cargo.toml
uv run --no-sync python -m mypy.stubtest vertebra._storage --allowlist tools/stubtest/allowlist.txt
uv run --no-sync pytest src/py/tests/unit/storage/test_storage_store.py -q
```

## Acceptance Criteria

- [ ] `PySyncObjectStore` and `PyAsyncObjectStore` expose `get_file`, `get_files`, `put_files`, `copy(..., if_not_exists=...)`, and `compose`, and `mypy.stubtest vertebra._storage` exits `0` (R3.2).

## Notes & Discoveries

- [2026-11-08 20:55] Convert `StoredObjectMeta` vectors to `Py<PyObjectMeta>` inside the `spawn_rust_task` Python completion closure without calling `Python::attach` on Tokio worker threads.

[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` — PyO3 bindings on `PySyncObjectStore` and `PyAsyncObjectStore`.
[^storage-pyi]: `src/py/vertebra/_storage.pyi::SyncObjectStore` — Type stubs for `vertebra._storage`.
