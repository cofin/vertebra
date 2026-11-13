---
type: Spec
flow_id: storage-integrity-and-streaming
chapter_id: ch01-integrity-receipts-and-verified-reads
title: "Chapter 01: Content-Integrity Receipts, Verified Reads, and Storage IntegrityError"
description: "Implements CRC32C, MD5, and SHA-256 content-integrity receipts on ObjectMeta and PutResult, abort-before-publish digest validation, verified streaming reads, and vertebra.typing.IntegrityError."
status: stable
state: active
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - storage
  - integrity
  - pyo3
  - checksums
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: "ObjectStoreEngine put_opts, create_multipart, put_file, get, get_file, and get_stream"
  - id: session-rs
    resource: src/rs/vertebra-storage/src/store.rs::MultipartUploadSession
    title: "MultipartUploadSession write_chunk, complete_with_meta, and abort"
  - id: lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::to_py_err
    title: "PyO3 storage error mapping and PyObjectMeta bindings"
  - id: py-private-typing
    resource: src/py/vertebra/_typing.py::PreconditionError
    title: "Internal exception definitions re-exported through vertebra.typing"
  - id: py-public-typing
    resource: src/py/vertebra/typing.py
    title: "Explicit public typing export list"
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::PutResult
    title: "Python ObjectMeta, PutResult, SyncObjectStore, and AsyncObjectStore"
---

# Chapter 01: Content-Integrity Receipts, Verified Reads, and Storage IntegrityError

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `vertebra-storage` computes no content digests over uploaded or downloaded payloads ([#99](https://github.com/cofin/vertebra/issues/99)) [^store-rs], and `src/py/vertebra/typing.py` explicitly lists every export from `vertebra._typing` without a storage `IntegrityError` [^py-public-typing].
- **Target Architecture**: `ObjectStoreEngine` computes CRC32C, MD5, and SHA-256 receipts when `checksum=True` or `expected_{sha256,crc32c,md5}` is set, aborts multipart uploads before publish on mismatch, and verifies `get`, `get_file`, and `get_stream` (`VerifyingStream`) [^session-rs]. `IntegrityError(VertebraError, OSError)` is defined in `_typing.py` and explicitly re-exported in `src/py/vertebra/typing.py` (`__all__`) and `vertebra.storage.__all__` (cleanly separated from `vertebra.db.IntegrityError(DatabaseError)`) [^lib-rs] [^py-private-typing] [^py-store].
- **Scope & Boundaries**: Covers `R1`, `R2` (read half), and `R3` across `vertebra-storage`, `_typing.py`, `typing.py`, `_storage.pyi`, and `vertebra/storage/` (`3` compact tasks).
<!-- truth: end -->

## 1. Specification, Context & Provenance

Callers uploading and reading objects across memory, local, S3/B2, R2, GCS, Azure, and OCI backends require opt-in end-to-end content digests (`sha256`, `crc32c`, `md5`) without paying hashing cost on default writes ([#99](https://github.com/cofin/vertebra/issues/99)) [^store-rs]. Because `src/py/vertebra/typing.py` explicitly enumerates every re-exported symbol rather than wildcard-importing `_typing`, `IntegrityError(VertebraError, OSError)` must be registered in `_typing.py`, `typing.py` (`__all__`), and `vertebra.storage` (`_store.py` and `__init__.py` `__all__`) so `lib.rs::make_integrity_err` resolves `vertebra.typing.IntegrityError` without colliding with `vertebra.db.IntegrityError(DatabaseError)` [^lib-rs] [^py-public-typing]. Guidance: [object-storage.md](../../../../knowledge/domains/storage/object-storage.md).

## 2. Requirements, Concrete Contracts & Architecture

```rust
// src/rs/vertebra-storage/src/store.rs
pub struct PutConditionalOptions {
    pub if_not_exists: bool,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub content_type: Option<String>,
    pub attributes: HashMap<String, String>,
    pub checksum: bool,
    pub expected_sha256: Option<String>,
    pub expected_crc32c: Option<String>,
    pub expected_md5: Option<String>,
}
pub struct StoredObjectMeta {
    // existing fields retained
    pub sha256: Option<String>,
    pub crc32c: Option<String>,
    pub md5: Option<String>,
}
pub enum StorageError {
    // existing variants retained
    #[error("integrity check failed for '{path}' ({algorithm}): expected {expected}, got {actual}")]
    Integrity { path: String, algorithm: &'static str, expected: String, actual: String },
}
impl ObjectStoreEngine {
    pub async fn get_verified(&self, path: &str, expected_sha256: &str) -> Result<Bytes, StorageError>;
    pub async fn get_file_verified(&self, path: &str, dest: &str, expected_sha256: &str) -> Result<StoredObjectMeta, StorageError>;
    pub async fn get_stream_verified(&self, path: &str, expected_sha256: &str) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError>;
}
```

```python
# src/py/vertebra/_typing.py, src/py/vertebra/typing.py (__all__), & vertebra.storage (__all__)
class IntegrityError(VertebraError, OSError):
    """Raised when an object storage content receipt or verified read mismatches an expected digest."""

# src/py/vertebra/storage/_store.py
class ObjectMeta(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    # existing fields retained
    sha256: str | None = None
    crc32c: str | None = None
    md5: str | None = None

class PutResult(ObjectMeta, frozen=True, gc=False, kw_only=True):
    """Result metadata returned from a write operation, carrying sha256/crc32c/md5 receipts when requested."""
```

- **Validation & Failure Contracts**:
  - Format checks run before I/O (`StorageError::InvalidInput` -> `ValueError`): `expected_sha256` = `64` lowercase hex chars, `expected_crc32c` = `8` lowercase hex chars (Castagnoli `0x82F63B78` polynomial), `expected_md5` = `32` lowercase hex chars (`md-5 = "0.10"`, already in `src/rs/Cargo.lock`).
  - When `checksum=False` and all `expected_*` are `None`, zero hashers are allocated and `sha256`/`crc32c`/`md5` are `None` (unless provider metadata attributes supply them).
  - Single-shot `put_opts` hashes before building `PutPayload`; `MultipartUploadSession` updates active hashers in `write_chunk` and verifies in `complete_with_meta` before `complete()`, calling `self.abort().await` on mismatch so prior objects remain intact; `get_file_verified` unlinks `.part` before rename on mismatch; `VerifyingStream` yields `Err(StorageError::Integrity)` on the final stream poll without buffering the full object.

## 3. Documentation & OKF Knowledge Plan

User documentation and OKF updates for integrity receipts, verified reads, and `IntegrityError` land in `ch03` (`t02`–`t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`, `R2` (read half) (Rust CRC32C/MD5/SHA-256 receipts, abort-before-publish, verified reads) | `t01` | `src/rs/vertebra-storage/{Cargo.toml,src/store.rs}` | `cargo test -p vertebra-storage` |
| `R3` (`_typing.py` `IntegrityError`, `lib.rs` bindings & `make_integrity_err`, `_storage.pyi`) | `t02` | `src/py/vertebra/_typing.py`, `src/rs/vertebra-storage/src/lib.rs`, `src/py/vertebra/_storage.pyi` | `mypy.stubtest vertebra._storage` |
| `R3` (`typing.py` `__all__`, `storage` `__all__`, `ObjectMeta`/`PutResult` & sync/async facades) | `t03` | `src/py/vertebra/typing.py`, `src/py/vertebra/storage/{_store,__init__}.py` | `pytest test_typing.py test_storage_store.py` |

## 5. Implementation Plan

- [x] [85849b32] [t01-rust-write-receipts-and-verified-reads.md](tasks/t01-rust-write-receipts-and-verified-reads.md) — Add CRC32C/MD5/SHA-256 receipts, expected-digest abort-before-publish, and `get_verified`/`get_file_verified`/`get_stream_verified` in `vertebra-storage`.
- [x] [85849b32] [t02-pyo3-integrity-bindings-and-stubs.md](tasks/t02-pyo3-integrity-bindings-and-stubs.md) — Define `IntegrityError` in `_typing.py`, map `StorageError::Integrity` and receipt/checksum kwargs in `lib.rs`, and update `_storage.pyi`.
- [x] [85849b32] [t03-typing-and-storage-facade-receipts.md](tasks/t03-typing-and-storage-facade-receipts.md) — Re-export storage `IntegrityError` in `src/py/vertebra/typing.py` and `vertebra.storage.__all__`, and wire receipts and verified reads across `SyncObjectStore` and `AsyncObjectStore`.

## 6. Current State & Remaining Plan Snapshot

- **Implemented (`3/3` tasks at `85849b32`, `47` Rust + `485` Python tests passed)**, awaiting Zero-Context Chapter Review closeout stamp.
- **Fast Chapter Gate (`< 35s`)**:
  `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-storage --all-targets -- -D warnings && UV_CACHE_DIR=/tmp/uv-cache uv run --no-sync maturin develop --manifest-path src/rs/vertebra-storage/Cargo.toml --uv && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync ruff check src/py/vertebra/_typing.py src/py/vertebra/typing.py src/py/vertebra/storage && uv run --no-sync mypy src/py/vertebra/typing.py src/py/vertebra/storage && uv run --no-sync pytest src/py/tests/unit/framework/test_typing.py src/py/tests/unit/storage/test_storage_store.py -q`

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — `ObjectStoreEngine` `put_opts`, `create_multipart`, `put_file`, `get`, `get_file`, and `get_stream`.
[^session-rs]: `src/rs/vertebra-storage/src/store.rs::MultipartUploadSession` — `MultipartUploadSession` `write_chunk`, `complete_with_meta`, and `abort`.
[^lib-rs]: `src/rs/vertebra-storage/src/lib.rs::to_py_err` — PyO3 storage error mapping and `PyObjectMeta` bindings.
[^py-private-typing]: `src/py/vertebra/_typing.py::PreconditionError` — Internal exception definitions re-exported through `vertebra.typing`.
[^py-public-typing]: `src/py/vertebra/typing.py` — Explicit public typing export list.
[^py-store]: `src/py/vertebra/storage/_store.py::PutResult` — Python `ObjectMeta`, `PutResult`, `SyncObjectStore`, and `AsyncObjectStore`.
