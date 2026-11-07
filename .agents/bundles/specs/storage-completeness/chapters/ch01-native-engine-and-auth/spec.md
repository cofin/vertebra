---
type: Spec
parent_flow: storage-completeness
chapter_id: ch01-native-engine-and-auth
title: 'Chapter 01: Native Rust Storage Engine, GCP Auth Unification, and Fail-Closed Facade'
description: Add zero-copy streaming reads, incremental multipart uploads, conditional puts, paginated listing, rename, OCI
  PAR presigning, unified GCP token caches, and fail-closed construction across vertebra-storage and vertebra.storage.
status: draft
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 4
plan_commit: null
state_revision: 6
last_verified_checkpoint: dc2c2053
last_reviewed_commit: null
created_at: '2026-11-07T04:15:00Z'
updated_at: '2026-11-07T19:06:00Z'
generated:
  by: flow-setup
  at: '2026-11-07T04:15:00Z'
verified:
- by: agent:code-reviewer
  at: '2026-11-07T19:05:09Z'
  commit: dc2c2053
- by: agent:flow-setup
  at: '2026-11-07T04:15:00Z'
  commit: cdb978d3
sources:
- id: store-rs
  resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
  title: Native Rust ObjectStoreEngine methods and StoredObjectMeta
- id: oci-multipart
  resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
  title: OCI-owned incremental multipart part-size handling
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
- **Zero-Copy Streaming & Incremental Multipart**: `ObjectStoreEngine` exposes `get_stream`, `put_opts`, stateful `MultipartUploadSession` (with uniform 8 MiB OCI chunk buffering rather than whole-payload `Vec<Bytes>` / `Vec<u8>` concatenation), `list_page`, `list_with_delimiter`, and `rename` [^store-rs] [^oci-multipart].
- **Async Completion Discipline**: Every async PyO3 method on `AsyncObjectStore`, `AsyncStreamReader`, and `AsyncMultipartUpload` schedules onto Tokio and delivers through `RustTaskAwaitable` (`AsyncCompletionQueue` + `Notifier`) without calling `Python::attach` on worker threads or `asyncio.to_thread` in Python [^py-store].
- **Single GCP Token Authority**: `vertebra._storage` removes duplicate `GcpTokenSnapshot`, `SyncGcpTokenCache`, and `AsyncGcpTokenCache` PyO3 classes; `vertebra.storage` re-exports `vertebra._vertebra.{GcpTokenSnapshot, SyncGcpTokenCache, AsyncGcpTokenCache}` and deletes pure-Python `urllib` OAuth fetchers [^gcp-auth-rs] [^py-store].
- **Fail-Closed Stores**: `SyncObjectStore` and `AsyncObjectStore` reject missing `vertebra._storage` (`MissingDependencyError`) and omitted target URIs/configs (`ImproperConfigurationError`), deleting `self._memory` dicts while keeping explicit `MemoryConfig()` and `"memory://..."` URIs valid [^py-store].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapter 01 delivers the native Rust/PyO3 storage engine, unified GCP OAuth2 token-cache authority, and fail-closed Python facade in `src/rs/vertebra-storage` and `src/py/vertebra/storage/_store.py` [^store-rs] [^oci-multipart] [^gcp-auth-rs] [^py-store]: incremental `MultipartUploadSession` (with uniform `8 MiB` OCI part chunking), `StreamReader` (`get_stream`), conditional `put_opts`, cursor `list_page` / `list_with_delimiter`, `rename`, end-to-end OCI PAR presigning (`sign_oci_par_url`), unified `vertebra._vertebra` token caches, and fail-closed `SyncObjectStore` / `AsyncObjectStore` construction.

### 1.2 Non-Goals

- Framework storage adapters in `vertebra.extensions.{sqlspec,alchemy,django,litestar}` (owned by Chapters 02 and 03).
- HTTP `Range` response parsing and Arrow/Parquet batch helpers (owned by Chapters 03 and 04).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [python-framework](../../../../../skills/python-framework/SKILL.md). Knowledge: [object-storage.md](../../../../knowledge/domains/storage/object-storage.md) and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.1 (`ch01:t01`)**: Extend `StoredObjectMeta` with `content_type: Option<String>` and `attributes: HashMap<String, String>`; implement `PutConditionalOptions`, `put_opts`, `StreamReader` (`next_chunk` yielding `Option<Bytes>` -> `StorageBuffer`), incremental `MultipartUploadSession` (`write_part`, `complete`, `abort` with uniform 8 MiB buffered chunking for `oci`), `list_page(prefix, offset, limit)`, `list_with_delimiter(prefix)`, and `rename(src, dst, if_not_exists)` in `vertebra-storage`.
- **R1.2 (`ch01:t02`)**: Expose `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, `AsyncMultipartUpload`, and updated `ObjectMeta`, `SyncObjectStore`, `AsyncObjectStore`, `SyncReadableFile`, and `AsyncReadableFile` signatures in `src/py/vertebra/_storage.pyi` and `src/py/vertebra/storage/_store.py` (`get_stream`, `put(..., if_not_exists=..., if_match=..., if_none_match=..., content_type=..., attributes=...)`, `put_stream`, `list_page`, `list_with_delimiter`, `rename`).
- **R2.1 (`ch01:t03`)**: Trim `src/rs/vertebra-storage/src/gcp_auth.rs` to `GCS_OAUTH_SCOPES` and `RefreshingGcpCredentialProvider` over `vertebra_core::gcp_auth::GcpTokenCache`, and complete GCS V4 (`GOOG4-RSA-SHA256` + IAM `signBlob`), S3 SigV4, Azure SAS, and OCI PAR presigning (`sign_oci_par_url`) in `signing.rs` and `oci.rs`.
- **R2.2 & R3 (`ch01:t04`)**: Remove `PyGcpTokenSnapshot`, `PySyncGcpTokenCache`, and `PyAsyncGcpTokenCache` from `vertebra._storage` (`lib.rs` and `_storage.pyi`), re-export `vertebra._vertebra.{GcpTokenSnapshot, SyncGcpTokenCache, AsyncGcpTokenCache}` from `vertebra.storage`, delete pure-Python OAuth fetchers and `self._memory` fallbacks from `storage/_store.py`, and enforce `MissingDependencyError` / `ImproperConfigurationError`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-storage/src/store.rs`):
  - `pub struct PutConditionalOptions { pub if_not_exists: bool, pub if_match: Option<String>, pub if_none_match: Option<String>, pub content_type: Option<String>, pub attributes: HashMap<String, String> }`
  - `pub async fn put_opts(&self, path: &str, data: Bytes, opts: PutConditionalOptions) -> Result<StoredObjectMeta, StorageError>`
  - `pub async fn get_stream(&self, path: &str) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError>`
  - `pub async fn create_multipart(&self, path: &str, opts: PutConditionalOptions) -> Result<MultipartUploadSession, StorageError>`
  - `pub async fn list_page(&self, prefix: Option<&str>, offset: Option<&str>, limit: usize) -> Result<(Vec<StoredObjectMeta>, Option<String>), StorageError>`
  - `pub async fn list_with_delimiter(&self, prefix: Option<&str>) -> Result<(Vec<StoredObjectMeta>, Vec<String>), StorageError>`
  - `pub async fn rename(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), StorageError>`
- Python (`src/py/vertebra/storage/_store.py` & `src/py/vertebra/_storage.pyi`):
  - `SyncObjectStore.get_stream(self, path: str, *, chunk_size: int | None = None) -> Iterator[StorageBuffer]`
  - `AsyncObjectStore.get_stream(self, path: str, *, chunk_size: int | None = None) -> AsyncIterator[StorageBuffer]`
  - `SyncObjectStore.put_stream(self, path: str, chunks: Iterable[Buffer], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None) -> PutResult`
  - `AsyncObjectStore.put_stream(self, path: str, chunks: AsyncIterable[Buffer] | Iterable[Buffer], *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None) -> PutResult`

### 2.3 Edge-Case & Alternatives Summary

- **Edge cases**: `if_not_exists=True` on existing key maps `object_store::Error::AlreadyExists` -> `FileExistsError`; `put_stream` aborts multipart session when producer iterator raises; `SyncObjectStore(None)` or `SyncObjectStore("")` raises `ImproperConfigurationError`.
- **Alternatives**: Delegating OCI part normalization inside `MultipartUploadSession` avoids buffering full multi-GB uploads while satisfying OCI's uniform non-trailing part-size contract.

## 3. Documentation & OKF Knowledge Plan

Progress: `[1/1 files documented in ch01]`

* `.agents/bundles/knowledge/domains/storage/object-storage.md` — Documents `ch01` `RustTaskAwaitable` + `notify.rs` provenance, streaming/conditional/multipart engine contracts, unified `vertebra._vertebra` token caches, and fail-closed `SyncObjectStore` / `AsyncObjectStore` invariants (`docs/usage/storage*.md` and `ch02`–`ch04` adapter/Arrow extensions are owned by `ch04:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R1.1 | [t01](tasks/t01-rust-streaming-and-ops.md) | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R1.2 | [t02](tasks/t02-python-streaming-facade-and-stubs.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_multicloud_storage.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R2.1 | [t03](tasks/t03-rust-gcp-auth-and-oci-signing.md) | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_signing.py` |
| R2.2, R3 | [t04](tasks/t04-unified-token-caches-and-fail-closed.md) | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py`, `src/py/tests/unit/storage/test_storage_store.py` |

## 5. Implementation Plan

- [!] storage-completeness:ch01:t01 — Implement streaming reads, incremental multipart sessions, conditional puts, pagination, and rename in vertebra-storage (tasks/t01-rust-streaming-and-ops.md)
- [x] [cb6b0647] storage-completeness:ch01:t02 — Wire Python streaming/conditional/paginated storage facade and _storage.pyi stubs (tasks/t02-python-streaming-facade-and-stubs.md)
- [x] [494ad92b] storage-completeness:ch01:t03 — Deduplicate Rust GCS auth wrapper and complete multi-cloud presigning including OCI PAR (tasks/t03-rust-gcp-auth-and-oci-signing.md)
- [x] [dc2c2053] storage-completeness:ch01:t04 — Unify Python GCP token caches on vertebra._vertebra and enforce fail-closed stores (tasks/t04-unified-token-caches-and-fail-closed.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-storage && make dev && uv run --no-sync python -m mypy.stubtest vertebra._storage && uv run --no-sync pytest src/py/tests/unit/storage -q` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- Baseline `5fe477fa` provides the committed native and facade APIs.
- ch01:t01 is blocked on raw-key and provider atomic-publication repairs in ch05, ch08 and ch10–ch15; other original task commits remain recorded.
- last_reviewed_commit is null. ch16 supplies fresh native acceptance, correctness review and fresh quality review before this chapter recloses.
- Repair chapters use the baseline artifacts directly and never require this chapter to be completed first.

[^store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust object-store wrapper and metadata model.
[^oci-multipart]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — OCI-owned accumulator and multipart chunking constraints.
[^gcp-auth-rs]: `src/rs/vertebra-storage/src/gcp_auth.rs::RefreshingGcpCredentialProvider` — GCS credential provider over `vertebra_core::gcp_auth`.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python object storage facade and credential configuration.
