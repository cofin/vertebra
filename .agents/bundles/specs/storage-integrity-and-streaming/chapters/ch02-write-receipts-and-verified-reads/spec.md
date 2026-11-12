---
type: Spec
parent_flow: storage-integrity-and-streaming
chapter_id: ch02-write-receipts-and-verified-reads
title: "Chapter 2: Write Receipts and Verified Reads"
description: Engine SHA-256 receipts with expected-digest enforcement before publish, verified reads, PyO3 bindings with IntegrityError, and the Python facade surface.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-create-only-copy-and-quality
replan_trigger: null
current_task: "storage-integrity-and-streaming:ch02:t01"
plan_revision: 1
plan_commit: null
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-12T01:30:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-spec
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: put_opts, create_multipart, get, get_file, get_stream
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: write_chunk, complete_with_meta, abort
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: Error mapping and PySyncObjectStore/PyAsyncObjectStore bindings
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::PutResult
    title: PutResult and facade write/read methods
  - id: sqlspec-backend
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: write_arrow_sync/async through write_batches_sync/async
  - id: issue-99
    resource: https://github.com/cofin/vertebra/issues/99
    title: SHA-256 write receipts and verified reads
  - id: issue-100
    resource: https://github.com/cofin/vertebra/issues/100
    title: Streamed Arrow writes from SQLSpec pipelines
tags:
  - storage
  - integrity
  - arrow
---

# Chapter 2: Write Receipts and Verified Reads

<!-- truth: start -->
- `PutConditionalOptions` gains `checksum: bool` and `expected_sha256: Option<String>`; `StoredObjectMeta` gains `sha256: Option<String>`; `StorageError` gains `Integrity { path, expected, actual }` mapped to `vertebra.typing.IntegrityError(OSError)` [^store-rs] [^lib-rs].
- Single puts hash the payload before sending; multipart sessions hash in `write_chunk` and compare in `complete_with_meta`, aborting on mismatch [^session-rs].
- `ObjectStoreEngine::get_verified` and `get_file_verified` hash before returning or renaming; `VerifyingStream` raises on the final poll [^store-rs].
- `PutResult.sha256` is populated only when hashing was requested [^py-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

No content digest exists over uploaded or downloaded bytes; SHA-256 appears only in OCI signing and presign helpers [^issue-99].

### 1.2 Non-Goals

- Provider-native checksum headers (`x-amz-checksum-sha256`) — receipts are computed locally over sent bytes.
- Streamed Arrow writes and the checksum benchmark (ch03); documentation (ch04).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined | Public Seam |
| --- | --- | --- |
| Engine | `store.rs::ObjectStoreEngine::put_opts`, `::create_multipart`, `::put_multipart`, `::get`, `::get_file`, `::get_stream`, `::MultipartUploadSession::write_chunk`, `::complete_with_meta`, `::abort`, `::StoredObjectMeta`, `::PutConditionalOptions`, `::StorageError` | Rust engine |
| Bindings | `lib.rs::to_py_err`, `PySyncObjectStore::put`, `::create_multipart`, `::get`, `::get_file`, `PyObjectMeta` | `vertebra._storage` |
| Facade | `_store.py::SyncObjectStore.put`, `::put_stream`, `::put_file`, `::get`, `::get_file`, `::open_writer`, `::PutResult`; `vertebra/typing.py` errors | `vertebra.storage` |
| SQLSpec | `extensions/sqlspec/storage.py::ObjectStoreBackend.write_arrow_sync`, `::write_arrow_async`, `::write_bytes_sync`; `_arrow.py::write_batches_sync` | SQLSpec protocol |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Object Storage](../../../../knowledge/domains/storage/object-storage.md); skills `runtime-ipc`, `pyo3-bridge`, `sqlspec`, `performance`.

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R3** (`t01`), **R4** read half (`t02`), **R5** (`t03` bindings, `t04` facade).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```rust
pub struct PutConditionalOptions { /* existing */ pub checksum: bool, pub expected_sha256: Option<String> }
pub struct StoredObjectMeta { /* existing */ pub sha256: Option<String> }
pub enum StorageError { /* existing */ #[error("integrity check failed for '{path}': expected sha256 {expected}, got {actual}")] Integrity { path: String, expected: String, actual: String } }
impl ObjectStoreEngine {
    pub async fn get_verified(&self, path: &str, expected_sha256: &str) -> Result<Bytes, StorageError>;
    pub async fn get_file_verified(&self, path: &str, dest: &str, expected_sha256: &str) -> Result<StoredObjectMeta, StorageError>;
    pub async fn get_stream_verified(&self, path: &str, expected_sha256: &str) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError>;
}
```

```python
def put(self, path: str, data: Buffer, *, ..., checksum: bool = False, expected_sha256: str | None = None) -> PutResult: ...
def get(self, path: str, *, expected_sha256: str | None = None) -> bytes: ...
class IntegrityError(OSError): ...
```

### 2.3 Caller -> Seam -> Collaborator Integration Map

- Facade kwargs → PyO3 kwargs → `PutConditionalOptions` → engine hashing via `sha2::Sha256` → `hex::encode`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario | Trigger | Expected | Invariant |
| --- | --- | --- | --- |
| Malformed expected digest | not 64 lowercase hex | `StorageError::InvalidInput` → `ValueError` | Validate before I/O |
| Mismatch single put | wrong digest | `Integrity` before request | Prior object intact |
| Mismatch multipart | wrong digest | `abort()` then `Integrity` | Prior object intact |
| Mismatch read | wrong digest | `Integrity`; no bytes, no file | No unverified data |
| Default | no options | `sha256 is None`, no hasher | Zero cost |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `checksum=True`, when any write path completes, then `PutResult.sha256 == hashlib.sha256(payload).hexdigest()`.

### 2.6 Alternatives Considered

| Decision | Chosen | Rejected and why |
| --- | --- | --- |
| Stream verification | Error on final poll | Buffering whole object breaks streaming. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]`; documented in ch04/t02 and ch04/t03.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R3 | [t01](tasks/t01-engine-sha256-write-receipts.md) | `tdd` | `cargo test -p vertebra-storage sha256` |
| R4 | [t02](tasks/t02-engine-verified-reads.md) | `tdd` | `cargo test -p vertebra-storage verified` (benchmark half of R4 in ch03/t02) |
| R5 | [t03](tasks/t03-pyo3-integrity-bindings.md) | `tdd` | `uv run python -m mypy.stubtest vertebra._storage` |
| R5 | [t04](tasks/t04-python-facade-receipts.md) | `tdd` | `pytest src/py/tests/unit/storage/test_storage_store.py -k "sha256 or verified"` |

## 5. Implementation Plan

- [ ] storage-integrity-and-streaming:ch02:t01 — Engine SHA-256 write receipts and expected-digest enforcement (tasks/t01-engine-sha256-write-receipts.md)
- [ ] storage-integrity-and-streaming:ch02:t02 — Engine verified reads (tasks/t02-engine-verified-reads.md)
- [ ] storage-integrity-and-streaming:ch02:t03 — PyO3 bindings and IntegrityError (tasks/t03-pyo3-integrity-bindings.md)
- [ ] storage-integrity-and-streaming:ch02:t04 — Python facade receipts and verified reads (tasks/t04-python-facade-receipts.md)

**Chapter Recovery Checkpoint:** `cd src/rs && cargo test -p vertebra-storage && cd ../.. && make dev && uv run python -m mypy.stubtest vertebra._storage && uv run pytest src/py/tests/unit/storage -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 1`, HEAD `c8722c76`; depends on ch01.
- **Remaining Tasks in Chapter**: `t01`–`t04` (`0/4`).

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Engine.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — Multipart session.
[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — Bindings.
[^py-store]: `src/py/vertebra/storage/_store.py::PutResult` — Facade.
[^sqlspec-backend]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec backend.
[^issue-99]: [#99](https://github.com/cofin/vertebra/issues/99) — Receipts and verified reads.
[^issue-100]: [#100](https://github.com/cofin/vertebra/issues/100) — Streamed Arrow writes.
