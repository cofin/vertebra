---
type: Spec
parent_flow: storage-completeness
chapter_id: ch01-native-engine-and-auth
title: "Chapter 01: Native Rust Storage Engine, GCP Auth Unification, and Fail-Closed Facade"
description: Add zero-copy streaming reads, incremental multipart uploads, conditional puts, paginated listing, rename, OCI PAR presigning, unified GCP token caches, and fail-closed construction across vertebra-storage and vertebra.storage.
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "storage-completeness:ch01:t04"
plan_revision: 2
plan_commit: cdb978d3
state_revision: 3
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T04:15:00Z"
updated_at: "2026-11-07T17:32:04Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Native Rust ObjectStoreEngine methods and StoredObjectMeta
  - id: r2-rs
    resource: src/rs/vertebra-storage/src/r2.rs::build_r2_store
    title: Cloudflare R2 and S3-compatible multipart part-size handling
  - id: gcp-auth-rs
    resource: src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider
    title: GCS credential provider delegating to vertebra_core::gcp_auth::GcpTokenCache
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: Python SyncObjectStore, AsyncObjectStore, and GCSConfig facade
tags:
  - storage
  - rust
  - pyo3
  - gcp
  - signing
---

# Chapter 01: Native Rust Storage Engine, GCP Auth Unification, and Fail-Closed Facade

<!-- truth: start -->
- **Zero-Copy Streaming & Incremental Multipart**: `ObjectStoreEngine` exposes `get_stream`, `put_opts`, stateful `MultipartUploadSession` (with uniform 8 MiB R2/OCI chunk buffering rather than whole-payload `Vec<Bytes>` / `Vec<u8>` concatenation), `list_page`, `list_with_delimiter`, and `rename` [^store-rs] [^r2-rs].
- **Async Completion Discipline**: Every async PyO3 method on `AsyncObjectStore`, `AsyncStreamReader`, and `AsyncMultipartUpload` schedules onto Tokio and delivers through `RustTaskAwaitable` (`AsyncCompletionQueue` + `Notifier`) without calling `Python::attach` on worker threads or `asyncio.to_thread` in Python [^py-store].
- **Single GCP Token Authority**: `vertebra._storage` removes duplicate `GcpTokenSnapshot`, `SyncGcpTokenCache`, and `AsyncGcpTokenCache` PyO3 classes; `vertebra.storage` re-exports `vertebra._vertebra.{GcpTokenSnapshot, SyncGcpTokenCache, AsyncGcpTokenCache}` and deletes pure-Python `urllib` OAuth fetchers [^gcp-auth-rs] [^py-store].
- **Fail-Closed Stores**: `SyncObjectStore` and `AsyncObjectStore` reject missing `vertebra._storage` (`MissingDependencyError`) and omitted target URIs/configs (`ImproperConfigurationError`), deleting `self._memory` dicts while keeping explicit `MemoryConfig()` and `"memory://..."` URIs valid [^py-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`src/rs/vertebra-storage` currently buffers multipart uploads in `Vec<Bytes>` (re-concatenating Cloudflare R2 parts into `Vec<u8>`), lacks streaming chunk iterators, conditional `put`, cursor pagination, server-side `rename`, and end-to-end OCI PAR presigning, and exports duplicate GCP token-cache PyO3 classes alongside pure-Python OAuth token fetchers and `self._memory` dict fallbacks in `src/py/vertebra/storage/_store.py` [^store-rs] [^r2-rs] [^gcp-auth-rs] [^py-store]. Chapter 01 completes the Rust/PyO3 storage engine, unifies GCP auth on `vertebra._vertebra`, and makes `SyncObjectStore` and `AsyncObjectStore` fail closed.

### 1.2 Non-Goals

- Framework storage adapters in `vertebra.extensions.{sqlspec,alchemy,django,litestar}` (owned by Chapters 02 and 03).
- HTTP `Range` response parsing and Arrow/Parquet batch helpers (owned by Chapters 03 and 04).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [object-storage.md](../../../../knowledge/domains/storage/object-storage.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.1 (`ch01:t01`)**: Extend `StoredObjectMeta` with `content_type: Option<String>` and `attributes: HashMap<String, String>`; implement `PutConditionalOptions`, `put_opts`, `StreamReader` (`next_chunk` yielding `Option<Bytes>` -> `StorageBuffer`), incremental `MultipartUploadSession` (`write_part`, `complete`, `abort` with uniform 8 MiB buffered chunking for `r2` and `oci`), `list_page(prefix, offset, limit)`, `list_with_delimiter(prefix)`, and `rename(src, dst, if_not_exists)` in `vertebra-storage`.
- **R1.2 (`ch01:t02`)**: Expose `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, `AsyncMultipartUpload`, and updated `ObjectMeta`, `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, and `AsyncReadableFile` signatures in `src/py/vertebra/_storage.pyi` and `src/py/vertebra/storage/_store.py` (`get_stream`, `put(..., if_not_exists=..., if_match=..., if_none_match=..., content_type=..., attributes=...)`, `put_stream`, `list_page`, `list_with_delimiter`, `rename`).
- **R2.1 (`ch01:t03`)**: Trim `src/rs/vertebra-storage/src/gcp_auth.rs` to `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider` over `vertebra_core::gcp_auth::GcpTokenCache`, and complete GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR presigning (`sign_oci_par_url`) in `signing.rs` and `oci.rs`.
- **R2.2 & R3 (`ch01:t04`)**: Remove `PyGcpTokenSnapshot`, `PySyncGcpTokenCache`, and `PyAsyncGcpTokenCache` from `vertebra._storage` (`lib.rs` and `_storage.pyi`), re-export `vertebra._vertebra.{GcpTokenSnapshot, SyncGcpTokenCache, AsyncGcpTokenCache}` from `vertebra.storage`, delete pure-Python OAuth fetchers and `self._memory` fallbacks from `storage/_store.py`, and enforce `MissingDependencyError` / `ImproperConfigurationError`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-storage/src/store.rs`):
  - `pub struct PutConditionalOptions { pub if_not_exists: bool, pub if_match: Option<String>, pub if_none_match: Option<String>, pub content_type: Option<String>, pub attributes: HashMap<String, String> }`
  - `pub async fn put_opts(&self, path: &str, data: Bytes, opts: PutConditionalOptions) -> Result<StoredObjectMeta, ObjectStoreError>`
  - `pub async fn get_stream(&self, path: &str) -> Result<BoxStream<'static, object_store::Result<Bytes>>, ObjectStoreError>`
  - `pub async fn create_multipart(&self, path: &str, opts: PutConditionalOptions) -> Result<MultipartUploadSession, ObjectStoreError>`
  - `pub async fn list_page(&self, prefix: Option<&str>, offset: Option<&str>, limit: usize) -> Result<(Vec<StoredObjectMeta>, Option<String>), ObjectStoreError>`
  - `pub async fn list_with_delimiter(&self, prefix: Option<&str>) -> Result<(Vec<StoredObjectMeta>, Vec<String>), ObjectStoreError>`
  - `pub async fn rename(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), ObjectStoreError>`
- Python (`src/py/vertebra/storage/_store.py` & `src/py/vertebra/_storage.pyi`):
  - `SyncObjectStore.get_stream(self, key: str, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]`
  - `AsyncObjectStore.get_stream(self, key: str, *, chunk_size: int | None = None) -> AsyncIterator[StorageBuffer]`
  - `SyncObjectStore.put_stream(self, key: str, chunks: Iterable[bytes | memoryview], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None) -> ObjectMeta`
  - `AsyncObjectStore.put_stream(self, key: str, chunks: AsyncIterable[bytes | memoryview] | Iterable[bytes | memoryview], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None) -> ObjectMeta`

### 2.3 Edge-Case & Alternatives Summary

- **Edge cases**: `if_not_exists=True` on existing key maps `object_store::Error::AlreadyExists` -> `FileExistsError`; `put_stream` aborts multipart session when producer iterator raises; `SyncObjectStore(None)` or `SyncObjectStore("")` raises `ImproperConfigurationError`.
- **Alternatives**: Delegating R2/OCI part normalization inside `MultipartUploadSession` avoids buffering full multi-GB uploads while satisfying R2's uniform non-trailing part-size rule.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch01]` (consolidated in `ch04:t03` once all storage and framework APIs land; `src/py/vertebra/_storage.pyi` docstrings and type stubs stay synchronized in `ch01:t02` and `ch01:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1.1 | [t01](tasks/t01-rust-streaming-and-ops.md) | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R1.2 | [t02](tasks/t02-python-streaming-facade-and-stubs.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_multicloud_storage.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R2.1 | [t03](tasks/t03-rust-gcp-auth-and-oci-signing.md) | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_signing.py` |
| R2.2, R3 | [t04](tasks/t04-unified-token-caches-and-fail-closed.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py`, `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [x] [beb6a972] storage-completeness:ch01:t01 — Implement streaming reads, incremental multipart sessions, conditional puts, pagination, and rename in vertebra-storage (tasks/t01-rust-streaming-and-ops.md)
- [x] [cb6b0647] storage-completeness:ch01:t02 — Wire Python streaming/conditional/paginated storage facade and _storage.pyi stubs (tasks/t02-python-streaming-facade-and-stubs.md)
- [x] [494ad92b] storage-completeness:ch01:t03 — Deduplicate Rust GCS auth wrapper and complete multi-cloud presigning including OCI PAR (tasks/t03-rust-gcp-auth-and-oci-signing.md)
- [ ] storage-completeness:ch01:t04 — Unify Python GCP token caches on vertebra._vertebra and enforce fail-closed stores (tasks/t04-unified-token-caches-and-fail-closed.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && make dev && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync pytest src/py/tests/unit/storage -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 494ad92b`; chapter `state: active`, `planning_status: complete`; `t01` completed (`beb6a972`), `t02` completed (`cb6b0647`), `t03` completed (`494ad92b`).
- **Remaining Tasks in Chapter**: `t04` (`1/4` pending).
- **Downstream Dependents**: Unblocks `ch02-orm-and-sql-storage-contracts`, `ch03-litestar-and-range-streaming`, and `ch04-arrow-shm-and-docs`.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust object-store wrapper and metadata model.
[^r2-rs]: `src/rs/vertebra-storage/src/r2.rs::build_r2_store` — R2 builder and uniform multipart chunking constraints.
[^gcp-auth-rs]: `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` — GCS credential provider over `vertebra_core::gcp_auth`.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python object storage facade and credential configuration.
