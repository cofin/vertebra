---
type: Spec
flow_id: storage-remediation
title: Native GCS Data Plane, Bulk Transfers, and Zero-Copy Storage Hardening
description: Remediate native GCS data-plane authentication, conditional generation updates, emulator static-bearer bypass, connection pooling, custom-endpoint V4 presigning, zero-copy PyBackedBytes FFI, checked byte-range arithmetic, raw object key preservation, streaming OCI operations, streaming bulk file transfers, server-side GCS compose, glob prefix pushdown, read-ahead/writable file streams, bounded Arrow/Parquet/SHM codecs, and clean-break framework storage adapters.
status: stable
state: active
has_chapters: true
chapters:
  - ch01-native-gcs-plane-and-signing
  - ch02-zero-copy-ffi-keys-and-backends
  - ch03-bulk-file-io-compose-and-streams
  - ch04-streaming-arrow-parquet-and-shm
  - ch05-framework-adapters-and-test-hygiene
  - ch06-docs-okf-and-plan-sync
current_chapter: ch05-framework-adapters-and-test-hygiene
current_task: "storage-remediation:ch05:t03"
parent_prd: native-data-platform
research: []
plan_revision: 1
plan_commit: fd571e3a
state_revision: 14
last_verified_checkpoint: 85bff05a
last_reviewed_chapter: ch04-streaming-arrow-parquet-and-shm
last_reviewed_commit: 85bff05a
created_at: "2026-11-08T20:30:00Z"
updated_at: "2026-11-09T01:19:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: agent:code-reviewer
    at: "2026-11-08T20:30:00Z"
    commit: fd571e3a
sources:
  - id: core-gcp-auth
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache
    title: Core GCP OAuth2 token cache shared across PyO3 extensions
  - id: py-gcp-auth
    resource: src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache
    title: PyO3 SyncGcpTokenCache and AsyncGcpTokenCache bindings on vertebra._vertebra
  - id: storage-store-rs
    resource: src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine
    title: Rust multicloud ObjectStoreEngine over object_store and OCI
  - id: storage-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore
    title: PyO3 bindings for vertebra._storage
  - id: storage-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked
    title: Native multicloud URL presigning and IAM signBlob client
  - id: storage-oci-rs
    resource: src/rs/vertebra-storage/src/oci.rs::OciObjectStore
    title: Native OCI Object Storage REST implementation
  - id: py-store
    resource: src/py/vertebra/storage/_store.py::SyncObjectStore
    title: Python SyncObjectStore, AsyncObjectStore, and StorageConfig classes
  - id: py-streaming
    resource: src/py/vertebra/storage/_streaming.py::StreamingStorageResponse
    title: HTTP 200/206/416 streaming storage response
  - id: py-storage-arrow
    resource: src/py/vertebra/storage/_arrow.py::iter_batches_sync
    title: Bounded Arrow IPC and Parquet storage readers and writers
  - id: py-db-arrow
    resource: src/py/vertebra/db/arrow.py::read_arrow_shm
    title: Shared-memory VTBARROW cursor hydration and columnar helpers
  - id: py-sqlspec-storage
    resource: src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend
    title: SQLSpec ObjectStoreBackend and DuckDB GCS secret helper
  - id: py-alchemy-storage
    resource: src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend
    title: Advanced Alchemy ObjectStoreBackend adapter
  - id: py-django-storage
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: Django Storage backend over paired native object stores
  - id: py-litestar-store
    resource: src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore
    title: Litestar key-value store and filesystem adapter over ObjectStore
tags:
  - storage
  - gcs
  - performance
  - ffi
  - arrow
  - remediation
---

# Flow: Native GCS Data Plane, Bulk Transfers, and Zero-Copy Storage Hardening

<!-- truth: start -->
- **C-ABI Cross-Extension Token Sharing**: `vertebra._vertebra` (`SyncGcpTokenCache` / `AsyncGcpTokenCache`) exports a `#[repr(C)]` `PyCapsule` (`c"vertebra.GcpTokenCacheCapsule"`) consumed by `vertebra._storage` via `SharedGcpTokenCacheHandle` so GCS data-plane I/O, server-side `compose`, and IAM `signBlob` share one `GcpTokenCache` instance (`generation`, `invalidate()`, single-flight async refresh) without casting `#[repr(Rust)]` struct layouts across `cdylib` boundaries [^core-gcp-auth] [^py-gcp-auth].
- **Native GCS Plane & Connection Pooling**: `gs://` conditional updates (`if_match`) map numeric generations directly to `UpdateVersion.version` (`x-goog-if-generation-match`) and verify non-numeric ETags via `head` before passing `existing.version`; `skip_signature=true` / `STORAGE_EMULATOR_HOST` supplies a non-blocking `StaticCredentialProvider` so writes/deletes never dial `169.254.169.254`; `allow_ambient_adc=false` isolates builder construction from process env; `quota_project_id` forwards `x-goog-user-project`; and pooled HTTP `ClientOptions` + pooled `LazyLock<reqwest::Client>` clients eliminate per-call TLS handshakes and global `os.environ["NO_PROXY"]` mutations [^storage-store-rs] [^storage-signing-rs] [^py-store].
- **Zero-Copy Immutable FFI, Mutable Buffer Safety, Raw Keys & Preconditions**: `extract_buffer_bytes` wraps immutable `PyBytes` zero-copy via `PyBackedBytes` (`Bytes::from_owner`) while copying mutable buffers (`bytearray`, `memoryview`) synchronously before dropping `PyBuffer` so `bytearray.clear()` never hits `BufferError`; `get_range` validates `offset.checked_add(length)` and `length == 0`; `StorePath::parse` preserves raw `~` and `%` keys; and conditional `412` mismatches raise `PreconditionError(VertebraError, OSError)` [^storage-lib-rs] [^storage-store-rs] [^py-litestar-store].
- **Bulk Transfers, Read-Ahead/Writable Streams, `glob` & Server-Side `compose`**: `ObjectStoreEngine` streams `put_file` (`> 8 MiB` multipart) and atomic `get_file`, drives bounded-concurrency `get_files` / `put_files` (`download_many`, `upload_many`, `/dev/shm`-default `staged_download`), supports `copy(..., overwrite=False)` (`copy_if_not_exists`), `glob()` prefix pushdown (`head` fast path when wildcard-free), server-side GCS `compose()` (`<= 32`-way tree fold with temp cleanup + streaming multipart fallback elsewhere), `4 MiB` read-ahead `open_reader`, and streaming multipart `open_writer` (`SyncWritableFile` / `AsyncWritableFile`) [^storage-store-rs] [^py-store].
- **Streaming HTTP, Parquet Encoding Options, Zero-Copy SHM & Clean-Break Adapters**: `OciObjectStore` streams `get_opts`, honors conditional/copy options, and paginates `list`; `StreamingStorageResponse` streams `iter_chunks()` and range-slices `to_fastpath_tuple()`; `_arrow.py` enforces top-to-bottom layout, `__slots__`, zero-copy `readinto`, demand-paged `pre_buffer=False` streaming vs `pre_buffer=True` full-table reads, `Buffer` writer support, and `use_dictionary` / `use_byte_stream_split` / `options` passthrough; `read_arrow_shm` retains `memoryview` zero-copy across `VTBARROW` and raw IPC buffers with columnar `_table_to_rows_and_cols`; and framework adapters drop deprecated shims/re-exports [^storage-oci-rs] [^py-streaming] [^py-storage-arrow] [^py-db-arrow] [^py-sqlspec-storage] [^py-alchemy-storage] [^py-django-storage].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Zero-context code and quality reviews of `feat/storage-updates` (`077a3fe9..fd571e3a`), combined with a full audit of the native GCS data plane, bulk transfer ergonomics, and hot-path throughput across `vertebra-storage`, `vertebra-core`, `vertebra.storage`, `vertebra.db.arrow`, and framework storage extensions, identified concrete functional and performance gaps to resolve before opening the storage pull request:

1. **Broken cross-extension `token_cache=` sharing and blocking Python `GCSConfig` setup**: `SyncObjectStore` and `AsyncObjectStore` retain `token_cache=` only on Python `self._gcp_token_cache` and never pass it into `vertebra._storage`, causing `ObjectStoreEngine::from_url` to build an isolated `GcpTokenCache` (or fail when credentials live solely on `token_cache`) [^py-store] [^storage-store-rs]. Meanwhile, `GCSConfig.to_url_and_options()` blocks synchronously on `self.resolve_adc_token()` for `target_service_account` and mutates process-wide `os.environ["NO_PROXY"]` [^py-store].
2. **Native GCS data-plane & V4 presigning defects**: `object_store`'s GCS client requires `UpdateVersion.version` (`x-goog-if-generation-match`) for `PutMode::Update` and rejects `e_tag`-only updates; `skip_signature=true` / `STORAGE_EMULATOR_HOST` hangs or fails on `put`/`multipart`/`delete` when no `StaticCredentialProvider` is attached; `GoogleCloudStorageBuilder::from_env()` leaks ambient env variables even when `allow_ambient_adc=false`; `quota_project_id` is omitted from `x-goog-user-project`; IAM `signBlob` builds a fresh `reqwest::Client` per call; and GCS V4 canonical request / URL assembly hardcodes `storage.googleapis.com` instead of honoring custom `endpoint` configurations [^storage-store-rs] [^storage-signing-rs].
3. **FFI copy bug, unchecked range math, key percent-encoding, and cloud/OCI backend gaps**: `_store.py` wraps `data` in `memoryview(data).cast("B")` before `native.put` / `native.put_multipart` (masking `PyBytes`) while `extract_buffer_bytes` copies every buffer via `to_vec()`; `get_range` performs unchecked `offset + length` addition and emits inverted `Range: bytes=offset-(offset-1)` on `length == 0`; `StorePath::from` percent-encodes `~` and `%` (forcing `unquote` workarounds in `litestar/store.py`); conditional `412` mismatches raise `PermissionError` instead of `PreconditionError`; `S3Config.virtual_hosted_style`, `R2Config.custom_domain`, `HTTPConfig.headers`, and `file://` conditional put metadata are unwired; and `OciObjectStore` buffers entire GET bodies in RAM while ignoring `GetOptions`, `PutOptions`, `CopyOptions`, and `nextStartWith` pagination [^storage-lib-rs] [^storage-store-rs] [^storage-oci-rs] [^py-litestar-store].
4. **Bulk file transfers, streaming writable handles, read-ahead range caching, `glob`, conditional `copy`, and server-side `compose`**: `ObjectStoreEngine::put_file` reads entire files into RAM via `tokio::fs::read` instead of streaming chunks for files `> 8 MiB`, and there is no streaming `get_file`, bounded-concurrency Rust batch transfer (`download_many` / `upload_many`), `/dev/shm` `staged_download` context manager, `open_writer` (`SyncWritableFile` / `AsyncWritableFile`), read-ahead cache on `SyncReadableFile` / `AsyncReadableFile` (`read_ahead=4_194_304`), `copy(..., overwrite=False)`, prefix-pushed `glob()`, or server-side GCS `compose()` (`<= 32`-way tree fold) [^storage-store-rs] [^py-store].
5. **Streaming HTTP, Arrow/Parquet codec options, SHM Arrow, adapter, test, and doc/OKF alignment**: `StreamingStorageResponse.iter_chunks()` and `to_fastpath_tuple()` materialize full blobs via `get_view()`; `storage/_arrow.py` lacks `__all__`, public-first ordering, `__slots__`, zero-copy `readinto`, demand-paged vs full-table `pre_buffer` defaults, and `Buffer` writer support, while `write_parquet` / `iter_parquet_batches` / `write_arrow` / `iter_arrow_batches` lack `use_dictionary`, `use_byte_stream_split`, and `options` passthrough; `read_arrow_shm` copies `bytes(raw_view)` and eagerly materializes `to_pylist()` tuples while `_table_to_rows_and_cols` allocates `N` intermediate dicts; framework storage adapters and tests retain shims, nested imports, and `for` subcase loops; and Zensical docs + OKF chapters need synchronization [^py-streaming] [^py-storage-arrow] [^py-db-arrow] [^py-sqlspec-storage] [^py-alchemy-storage] [^py-django-storage].

**Non-Goals**:
- Database driver fail-loud guards and `memory://` removal (owned by stacked child flow `driver-fail-loud`).
- Standalone PostgreSQL wire driver implementation (owned by stacked child flow `native-postgres-driver`).

### 1.2 Upstream Execution Baseline

- **Branch**: `feat/storage-updates` at `fd571e3a` (`main` merge-base `077a3fe9`).
- **Parent PRD**: [`native-data-platform`](../native-data-platform/spec.md) (sequenced immediately after archived `storage-completeness` [`a2af5066`] and ahead of `driver-fail-loud` and `native-postgres-driver`).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Cross-Extension GCP Auth | `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` [^core-gcp-auth], `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` [^py-gcp-auth] | C-ABI `PyCapsule` boundary (`FFI-04`) sharing one refcounted `GcpTokenCache` across `_vertebra` and `_storage` `cdylib` binaries. |
| Native GCS, Bulk I/O & Compose | `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` [^storage-store-rs], `src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked` [^storage-signing-rs] | Remote-owned HTTP/TLS storage, GCS JSON `compose`, and IAM `signBlob` boundaries tested against deterministic loopback HTTP servers (`127.0.0.1:0`). |
| PyO3 FFI & OCI Store | `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` [^storage-lib-rs], `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` [^storage-oci-rs] | Zero-copy immutable `PyBackedBytes` / `StorageBuffer` FFI seam (`FFI-01`..`FFI-06`) and streaming `object_store::ObjectStore` implementation for OCI REST. |
| Python Facade, Streams & Codecs | `src/py/vertebra/storage/_store.py::SyncObjectStore` [^py-store], `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` [^py-streaming], `src/py/vertebra/storage/_arrow.py::iter_batches_sync` [^py-storage-arrow], `src/py/vertebra/db/arrow.py::read_arrow_shm` [^py-db-arrow] | Local-substitutable / in-process streaming readers, writers, batch transfers, `glob`, `compose`, Parquet options, and zero-copy `VTBARROW` IPC codecs. |
| Framework Storage Adapters | `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` [^py-sqlspec-storage], `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` [^py-alchemy-storage], `src/py/vertebra/extensions/django/storage.py::DjangoStorage` [^py-django-storage], `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` [^py-litestar-store] | Public framework adapter seams for SQLSpec, Advanced Alchemy, Django, and Litestar. |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- **OKF Knowledge**: [`object-storage.md`](../../knowledge/domains/storage/object-storage.md), [`litestar.md`](../../knowledge/domains/extensions/litestar.md), [`ffi-and-gil-discipline.md`](../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [`module-and-class-layout.md`](../../knowledge/patterns/python/module-and-class-layout.md), [`adr-004-optional-driver-wheels-and-shm-ownership.md`](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [`adr-005-persistent-object-store-ttl-envelopes.md`](../../knowledge/decisions/adr-005-persistent-object-store-ttl-envelopes.md).
- **Skills**: [`pyo3-bridge`](../../../skills/pyo3-bridge/SKILL.md), [`runtime-ipc`](../../../skills/runtime-ipc/SKILL.md), [`python-framework`](../../../skills/python-framework/SKILL.md), [`review-docs-okf`](../../../skills/review-docs-okf/SKILL.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R1 (Cross-Extension `GcpTokenCacheCapsule`)**: Define `GCP_TOKEN_CACHE_CAPSULE_NAME` (`c"vertebra.GcpTokenCacheCapsule"`), `#[repr(C)] pub struct GcpTokenCacheCapsule`, and safe `SharedGcpTokenCacheHandle` in `vertebra-core::gcp_auth`, expose `_capsule(self) -> object` on `SyncGcpTokenCache` and `AsyncGcpTokenCache` in `vertebra._vertebra`, and wire `token_cache=` through `SyncObjectStore` / `AsyncObjectStore` into `vertebra._storage` so GCS data-plane operations, server-side `compose`, and IAM `signBlob` share one token cache.
- **R2 (Native GCS Data-Plane Correctness & Pooling)**: Support GCS `PutMode::Update` (`if_match`) for both numeric object generations (`1` RTT) and ETag strings (`head` check + generation `version`), attach a non-blocking `StaticCredentialProvider` when `skip_signature=true` without credentials so emulator writes/deletes never contact GCE metadata, honor `allow_ambient_adc=false` via `GoogleCloudStorageBuilder::new()`, forward `quota_project_id` as `x-goog-user-project`, and apply shared pooled `ClientOptions` (with loopback proxy exclusions and zero `os.environ["NO_PROXY"]` mutation) across `Gcs`, `S3`, `R2`, `Azure`, and `Http`.
- **R3 (Pooled IAM `signBlob`, Custom GCS Presigning Endpoint, & Non-Blocking `GCSConfig`)**: Reuse `LazyLock<reqwest::Client>` pools in `signing.rs` (forwarding `x-goog-user-project` on IAM `signBlob`), add `endpoint: Option<String>` to `GcsSignConfig` plus `endpoint` and `token_cache_capsule` to `py_sign_gcs_v4_iam_url` / `py_sign_gcs_v4_iam_url_async` (and `endpoint` to `build_gcs_v4_canonical_request`) for custom GCS hosts/emulators and shared token caches, and remove blocking `resolve_adc_token()` and `eager_impersonation` from `GCSConfig.to_url_and_options()`.
- **R4 (Zero-Copy Immutable FFI, Mutable Buffer Copy Safety, Checked `get_range`, Raw Keys, & `PreconditionError`)**: Pass buffers directly into native `put` / `put_multipart` without wrapping `PyBytes` in `memoryview`; extract `PyBytes` zero-copy via `PyBackedBytes` (`Bytes::from_owner`) while copying mutable `PyBuffer<u8>` (`bytearray`, `memoryview`) synchronously before returning so caller `bytearray.clear()` never raises `BufferError`; validate `offset.checked_add(length)` and `length == 0` in `get_range`; preserve raw object keys via `StorePath::parse` (and preserve `content_type` / `attributes` in `file://` conditional puts); and map conditional failures to `PreconditionError(VertebraError, OSError)` exported from `vertebra.typing` and `vertebra.storage`.
- **R5 (Cloud Backend Options, Litestar Raw Keys, & Streaming/Paginated OCI)**: Wire `S3Config.virtual_hosted_style` across `AmazonS3Builder` and `S3SignConfig`, wire `R2Config.custom_domain` and `HTTPConfig.headers`, remove `urllib.parse.unquote` from `litestar/store.py`, and upgrade `OciObjectStore` to stream `get_opts` bodies, honor `GetOptions` / `PutOptions` / `CopyOptions`, and paginate `list` / `list_with_delimiter`.
- **R6 (Streaming File I/O, Bounded-Concurrency Batch Transfers, Conditional `copy`, `glob`, Server-Side `compose`, Read-Ahead `open_reader`, & Streaming `open_writer`)**:
  - Stream `ObjectStoreEngine::put_file` (`<= 8 MiB` single `put`, `> 8 MiB` `8 MiB` multipart chunks with `abort()` on error) and `get_file` (stream `get_opts` payload to sibling `.part` temp file, flush, atomic `rename`, unlink on error).
  - Implement bounded-concurrency Rust `get_files` and `put_files` (`buffer_unordered(concurrency)`) exposed on Python `SyncObjectStore` and `AsyncObjectStore` as `get_file`, `download_many`, `upload_many`, and `/dev/shm`-default `staged_download`.
  - Add `copy(src, dst, *, overwrite: bool = True)` (`copy_if_not_exists`), `glob(pattern: str) -> list[ObjectMeta]` (`head` fast-path when wildcard-free, prefix pushdown + `fnmatchcase` otherwise), and `compose(sources, dst, *, content_type=None) -> PutResult` (native GCS JSON `/compose` `<= 32`-way tree fold with `finally` temp-object cleanup on `gs://`, streaming multipart concatenation on all other backends).
  - Add `read_ahead: int = 4_194_304` block caching to `open_reader` (`SyncReadableFile` / `AsyncReadableFile`) and streaming multipart `open_writer` (`SyncWritableFile` / `AsyncWritableFile`, `part_size: int = 8_388_608`).
- **R7 (True Streaming HTTP Responses, Arrow/Parquet Options & Layout, & Zero-Copy SHM Arrow)**: Stream `StreamingStorageResponse.iter_chunks()` (`200` via `get_stream`, `206` via bounded `get_range` windows) and slice `206` `to_fastpath_tuple()` ranges via `get_range`; organize `storage/_arrow.py` with `__all__`, public-before-private layout, `__slots__`, zero-copy `readinto`, demand-paged `pre_buffer=False` streaming vs `pre_buffer=True` full-table Parquet defaults, and `Buffer` writer support; expose `use_dictionary: bool | Sequence[str] = True`, `use_byte_stream_split: bool | Sequence[str] = False`, and `options: Mapping[str, Any] | None = None` on `write_parquet` (plus `options` on `iter_parquet_batches`, `write_arrow`, `iter_arrow_batches`); and keep `read_arrow_shm` zero-copy over `memoryview` across `VTBARROW` and raw IPC buffers with lazy `ArrowCursor.__len__` and columnar `_table_to_rows_and_cols`.
- **R8 (Clean-Break Framework Adapters & Pytest Hygiene)**: Remove `_IPCBuffer` and group sync/async methods cleanly in `sqlspec/storage.py` (raising `ImproperConfigurationError` when `build_duckdb_gcs_secret_sql` lacks a token and checking `supports_signing`); forward `signing=` and `token_cache=` for `StorageConfig` in `alchemy/storage.py`; remove `vertebra.storage` re-exports from `django/{storage.py,__init__.py}`, avoid redundant `bytes(...)` copies in `DjangoStorage.save`, and delete `open_readable_file` and `_get_native_module` in `_store.py`; hoist nested test imports and convert test loops / `asyncio.run` to `@pytest.mark.parametrize` and `@pytest.mark.anyio`.
- **R9 (Zensical Docs & OKF Bundle Synchronization)**: Update `docs/usage/storage*.md` and `docs/reference/storage.md` (`SyncObjectStore("memory://documents")`, `PreconditionError`, `get_file`, `download_many`, `upload_many`, `staged_download`, `open_reader(..., read_ahead=...)`, `open_writer`, `copy(..., overwrite=False)`, `glob`, `compose`, Parquet `use_dictionary` / `use_byte_stream_split`, `multiprocessing` `spawn` guidance for native Tokio workers, and complete `mkdocstrings` `members:` reference coverage), and synchronize `.agents/bundles/product/product.md` (`<= 40`-line flat-bullet truth block + exact `vertebra._storage` exports) and `.agents/bundles/knowledge/domains/storage/object-storage.md` (`<= 200` lines).

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

- **Rust (`vertebra-core::gcp_auth` & `vertebra-storage`)**:
  - `pub const GCP_TOKEN_CACHE_CAPSULE_NAME: &CStr = c"vertebra.GcpTokenCacheCapsule";`
  - `#[repr(C)] pub struct GcpTokenCacheCapsule { ... }` + `#[derive(Clone)] pub struct SharedGcpTokenCacheHandle`
  - `ObjectStoreEngine::get_file(&self, path: &str, file_path: &str) -> Result<StoredObjectMeta, StorageError>`
  - `ObjectStoreEngine::get_files(&self, pairs: Vec<(String, String)>, concurrency: usize) -> Result<Vec<StoredObjectMeta>, StorageError>`
  - `ObjectStoreEngine::put_files(&self, pairs: Vec<(String, String)>, concurrency: usize) -> Result<Vec<StoredObjectMeta>, StorageError>`
  - `ObjectStoreEngine::copy(&self, src: &str, dst: &str, if_not_exists: bool) -> Result<(), StorageError>`
  - `ObjectStoreEngine::compose(&self, sources: Vec<String>, dst: &str, content_type: Option<String>) -> Result<StoredObjectMeta, StorageError>`
- **PyO3 (`vertebra._vertebra` & `vertebra._storage`)**:
  - `SyncGcpTokenCache._capsule(self) -> object` and `AsyncGcpTokenCache._capsule(self) -> object`
  - `SyncObjectStore.__new__(cls, url: str, options: dict[str, str] | None = None, token_cache_capsule: object | None = None) -> SyncObjectStore` (and `AsyncObjectStore.__new__`)
  - `get_file(self, path: str, file_path: str)`, `get_files(self, pairs: list[tuple[str, str]], *, concurrency: int = 16)`, `put_files(self, pairs: list[tuple[str, str]], *, concurrency: int = 16)`, `copy(self, src: str, dst: str, *, if_not_exists: bool = False)`, `compose(self, sources: list[str], dst: str, *, content_type: str | None = None)` on `SyncObjectStore` and `AsyncObjectStore` (`RustTaskAwaitable` on `AsyncObjectStore`)
  - `py_sign_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> str` and `py_verify_s3_v4_url(..., endpoint: str | None = None, virtual_hosted_style: bool = False) -> bool`
  - `py_sign_gcs_v4_iam_url(..., private_key_pem: str | None = None, endpoint: str | None = None, token_cache_capsule: object | None = None) -> str` and `py_sign_gcs_v4_iam_url_async(..., private_key_pem: str | None = None, endpoint: str | None = None, token_cache_capsule: object | None = None) -> RustTaskAwaitable`
- **Python (`vertebra.typing` & `vertebra.storage`)**:
  - `class PreconditionError(VertebraError, OSError): ...`
  - `class SyncWritableFile(io.RawIOBase)` and `class AsyncWritableFile` exported in `vertebra.storage.__all__`
  - `get_file(path: str, file_path: str | Path) -> ObjectMeta`
  - `download_many(keys: Sequence[str], destination: str | Path, *, concurrency: int = 16) -> list[Path]`
  - `upload_many(paths: Sequence[str | Path], destination_prefix: str = "", *, concurrency: int = 16) -> list[PutResult]`
  - `staged_download(keys: Sequence[str], destination: str | Path | None = None, *, concurrency: int = 16)` (`Iterator[list[Path]]` / `AsyncIterator[list[Path]]`)
  - `open_reader(path: str, *, size: int | None = None, chunk_size: int = 65536, read_ahead: int = 4_194_304) -> SyncReadableFile | AsyncReadableFile`
  - `open_writer(path: str, *, content_type: str | None = None, attributes: Mapping[str, str] | None = None, part_size: int = 8_388_608, if_not_exists: bool = False, if_match: str | None = None, if_none_match: str | None = None) -> SyncWritableFile | AsyncWritableFile`
  - `copy(src: str, dst: str, *, overwrite: bool = True) -> None`
  - `glob(pattern: str) -> list[ObjectMeta]`
  - `compose(sources: Sequence[str], dst: str, *, content_type: str | None = None) -> PutResult`
  - `write_parquet(key: str, batches: Any, *, schema: Any = None, compression: str = "zstd", row_group_size: int = 65536, use_dictionary: bool | Sequence[str] = True, use_byte_stream_split: bool | Sequence[str] = False, options: Mapping[str, Any] | None = None) -> PutResult`
  - `iter_parquet_batches(key: str, *, batch_size: int = 65536, columns: Sequence[str] | None = None, options: Mapping[str, Any] | None = None)`
  - `write_arrow(key: str, data: Any, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None) -> PutResult`
  - `iter_arrow_batches(key: str, *, batch_size: int = 65536, options: Mapping[str, Any] | None = None)`

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `SyncObjectStore(target, token_cache=cache)` / `AsyncObjectStore(target, token_cache=cache)` -> `resolved_cache._capsule()` (`vertebra._vertebra`) -> `PySyncObjectStore::new` / `PyAsyncObjectStore::new` (`vertebra._storage`) -> `SharedGcpTokenCacheHandle::from_capsule_ptr` -> `ObjectStoreEngine::from_url_with_cache` -> `RefreshingGcpCredentialProvider` & GCS `compose`.
- `SyncObjectStore.put(key, payload)` -> passes `payload` directly without `memoryview` wrapping -> `extract_buffer_bytes`: `PyBytes` wrapped zero-copy in `PyBackedBytes`, `bytearray` copied synchronously -> `ObjectStoreEngine::put_opts` -> HTTP `412` mapped to `vertebra.typing.PreconditionError`.
- `SyncObjectStore.download_many` / `staged_download` -> `PySyncObjectStore::get_files` (`py.detach`) -> `ObjectStoreEngine::get_files` (`buffer_unordered(concurrency)`) -> atomic `.part` write + `rename`.
- `SyncObjectStore.compose(sources, dst)` -> `ObjectStoreEngine::compose`: on `gs://` executes `<= 32`-way tree-folded GCS JSON `/compose` with `finally` temp-object cleanup; on other backends streams `get_stream` chunks into `create_multipart`.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Cross-`.so` token invalidation | `401` on IAM `signBlob` or `cache.invalidate(gen)` | Invalidates the single shared `GcpTokenCache`; next data-plane or `compose` request refreshes once | Zero split-brain token caches or undefined cross-`cdylib` Rust struct casts (`FFI-04`). |
| Mutable `bytearray` reuse after `put` / `open_writer.write` | `buf = bytearray(b"abc"); store.put("k", buf); buf.clear()` | `buf.clear()` succeeds immediately without `BufferError: Existing exports of data` while immutable `bytes` stay zero-copy via `PyBackedBytes` | Safe caller buffer reuse + zero-copy immutable `bytes` (`FFI-05`). |
| GCS `compose` with `> 32` sources or mid-flight failure | `compose([k0..k69], "final.parquet")` or missing `k40` | Folds `32`-source batches into `{dst}.compose-{n:04d}` and deletes every temporary object even when a fold step fails | Zero leaked intermediate `.compose-*` objects on GCS. |
| `download_many` / `staged_download` collision or partial failure | Duplicate basenames `["a/x.parquet", "b/x.parquet"]` or missing key | Duplicate basenames rejected up front with `ValueError`; partial batch failure unlinks files created in that call; `staged_download` unlinks staged files in `finally` | Zero silent file clobbering or `/dev/shm` leaks. |
| `open_reader` small sequential/footer reads | Parquet footer + metadata reads (`< 4 MiB` each) | `SyncReadableFile` / `AsyncReadableFile` caches one `4 MiB` `get_range` block and serves subsequent adjacent reads from RAM (`read_ahead=0` disables caching) | Eliminates dozens of tiny HTTP `Range` RTTs per Parquet file. |
| `get_range` overflow or zero length | `offset=1, length=u64::MAX` or `offset=5, length=0` | Overflow raises `ValueError`; `length == 0` validates existence (`FileNotFoundError` if missing) and returns `b""` without sending `Range: bytes=5-4` | Zero FFI panics/aborts (`FFI-06`) and RFC 9110 range validity. |

### 2.5 Acceptance Scenarios (Given / When / Then)

1. **Given** a loopback OAuth2 + GCS + IAM `signBlob` server and an `AsyncGcpTokenCache` passed to `AsyncObjectStore("gs://b", ..., token_cache=cache)`, **When** `await store.put(...)`, `await store.compose(...)`, `await store.get(...)`, and `await store.sign(...)` run, **Then** the OAuth2 token endpoint is called once across data-plane, server-side compose, and IAM signing, and `x-goog-user-project` is sent when `quota_project_id` is configured.
2. **Given** `70` shard objects in a loopback GCS server, **When** `store.compose(shards, "merged.bin", content_type="application/octet-stream")` runs, **Then** `ObjectStoreEngine` performs a `32`-way tree fold via `/storage/v1/b/<bucket>/o/<dst>/compose`, produces the exact concatenated bytes, and deletes all `.compose-*` temporary objects.
3. **Given** multiple `12 MiB` objects and a reusable `bytearray` scratch buffer, **When** `open_writer`, `put_file`, `download_many`, `staged_download`, and `open_reader` (`read_ahead=4_194_304`) execute, **Then** `bytearray.clear()` never raises `BufferError`, transfers stream with bounded memory, `staged_download` cleans up `/dev/shm` on exit or exception, and sub-`4 MiB` `open_reader` reads coalesce into a single `get_range` call per `4 MiB` window.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam Assessment | Decision |
| --- | --- | --- |
| **Chosen**: `#[repr(C)]` `GcpTokenCacheCapsule` vtable in `vertebra-core::gcp_auth` + `SharedGcpTokenCacheHandle` | Deep module hiding cross-`cdylib` refcounting, fast-path read lock, and `refresh_async_fn` single-flight async refresh behind a safe Rust handle (matching `ShmCapsule` in `shm.rs`). | **Selected** |
| Retain `PyBackedBytes` / `PyBuffer` across `bytearray` writes | Exports a live `Py_buffer` view that causes `BufferError` if the caller mutates or clears a pooled `bytearray` while Tokio or multipart accumulators still hold `Bytes`. | **Rejected** (`PyBackedBytes` restricted strictly to immutable `PyBytes`). |
| Client-side download + re-upload for `gs://` `compose()` | Wastes `2x` network bandwidth and worker memory for multi-gigabyte shard merges when GCS natively supports server-side `/compose` up to `32` objects per call. | **Rejected** (use native GCS `/compose` tree fold on `gs://` and streaming multipart fallback elsewhere). |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/6 files documented]`
- `[ ]` `docs/usage/storage.md` — Document `PreconditionError`, raw key semantics, `get_file`, `download_many`, `upload_many`, `staged_download`, `open_reader(..., read_ahead=...)`, `open_writer`, `copy(..., overwrite=False)`, `glob`, `compose`, backend config options, and `multiprocessing` `spawn` start-method guidance.
- `[ ]` `docs/usage/storage-auth-and-signing.md` — Document cross-extension `token_cache=` sharing across GCS data plane, server-side `compose`, and IAM `signBlob`, plus custom `endpoint` presigning.
- `[ ]` `docs/usage/storage-parquet-and-orm.md` — Replace `SyncObjectStore("memory://")` with explicit `SyncObjectStore("memory://documents")`, document `write_parquet(..., use_dictionary=..., use_byte_stream_split=...)`, and use clean-break `vertebra.storage` imports.
- `[ ]` `docs/reference/storage.md` — Expand `mkdocstrings` `members:` allowlist to cover the complete `SyncObjectStore` / `AsyncObjectStore` surface (`get_file`, `download_many`, `upload_many`, `staged_download`, `open_writer`, `glob`, `compose`, etc.), `SyncWritableFile`, `AsyncWritableFile`, `PreconditionError`, `PutResult`, `StreamingStorageResponse`, `ByteRange`, and `parse_byte_range`.
- `[ ]` `.agents/bundles/product/product.md` — Keep `<!-- truth: start -->` `<= 40` flat bullet lines (zero `##` headings) and align `vertebra._storage` vs `vertebra._vertebra` PyO3 class inventory.
- `[ ]` `.agents/bundles/knowledge/domains/storage/object-storage.md` — Synthesize `GcpTokenCacheCapsule`, GCS conditional `version` / emulator static-bearer / server-side `compose` invariants, zero-copy immutable `PyBackedBytes`, `PreconditionError`, bulk file I/O, read-ahead/writable handles, streaming OCI, and zero-copy `read_arrow_shm` within `<= 200` lines.

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 (`GcpTokenCacheCapsule` & `_vertebra` bindings) | `ch01 / t01` | `behavior_tdd` | `src/rs/vertebra-core/src/gcp_auth.rs`, `src/py/tests/unit/storage/test_gcs_token_refresh.py` |
| R1, R2 (Native GCS plane, `if_match`, `skip_signature`, pooling) | `ch01 / t02` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R3 (Pooled IAM `signBlob`, custom GCS `endpoint`, PyO3 stubs) | `ch01 / t03` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_signing.py` |
| R1, R3 (Python `GCSConfig` & `token_cache._capsule()` wiring) | `ch01 / t04` | `behavior_tdd` | `src/py/tests/unit/storage/test_gcs_token_refresh.py`, `src/py/tests/unit/storage/test_signing.py` |
| R4 (`PreconditionError` & zero-copy `PyBackedBytes` vs mutable buffer copy) | `ch02 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R4, R5 (Checked `get_range`, `StorePath::parse`, `file://` attrs, S3/R2/HTTP builders) | `ch02 / t02` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs`, `src/py/tests/unit/storage/test_signing.py` |
| R5 (S3 `virtual_hosted_style` PyO3/stubs & Litestar raw keys) | `ch02 / t03` | `behavior_tdd` | `src/py/tests/unit/storage/test_signing.py`, `src/py/tests/unit/extensions/test_litestar_plugin.py` |
| R5 (Streaming, conditional, paginated `OciObjectStore`) | `ch02 / t04` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R6 (Rust streaming `put_file`/`get_file`, `get_files`/`put_files`, conditional `copy`, GCS `compose`) | `ch03 / t01` | `behavior_tdd` | `src/rs/vertebra-storage/tests/storage_engine_tests.rs` |
| R6 (PyO3 bindings & `_storage.pyi` for bulk file I/O, conditional `copy`, `compose`) | `ch03 / t02` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R6 (Python `get_file`, `download_many`, `upload_many`, `staged_download`, `copy`, `glob`, `compose`) | `ch03 / t03` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R4, R6 (Direct non-`memoryview` `put`/`put_multipart`, read-ahead `open_reader`, streaming `open_writer`) | `ch03 / t04` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R7 (Streaming `StreamingStorageResponse`) | `ch04 / t01` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R7 (`_arrow.py` layout/`__slots__`/`readinto`/`pre_buffer`/`Buffer` + Parquet/Arrow option passthrough) | `ch04 / t02` | `behavior_tdd` | `src/py/tests/unit/storage/test_storage_store.py` |
| R7 (Zero-copy `read_arrow_shm` & columnar `_table_to_rows_and_cols`) | `ch04 / t03` | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| R8 (`sqlspec/storage.py` & `alchemy/storage.py` cleanup) | `ch05 / t01` | `behavior_tdd` | `src/py/tests/unit/extensions/test_data_storage_integrations.py` |
| R8 (`django` clean-break exports & `_store.py` alias removal) | `ch05 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_django_plugin.py`, `src/py/tests/unit/storage/test_storage_store.py` |
| R8 (Pytest top-level imports, `parametrize`, `anyio`) | `ch05 / t03` | `static_validation` | `src/py/tests/unit/storage/test_storage_store.py`, `src/py/tests/unit/extensions/test_litestar_plugin.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| R9 (`docs/usage/storage*.md`) | `ch06 / t01` | `documentation_validation` | `make docs && make docs-clean` |
| R9 (`docs/reference/storage.md`, `product.md`, & `object-storage.md` OKF synthesis) | `ch06 / t02` | `documentation_validation` | `make docs && make docs-clean && make review-check` |

## 5. Implementation Plan

- [x] [4b81d314] ch01-native-gcs-plane-and-signing — Cross-extension `GcpTokenCacheCapsule`, native GCS conditional/emulator/pooling fixes, pooled IAM `signBlob`, custom GCS endpoint presigning, and non-blocking `GCSConfig` (chapters/ch01-native-gcs-plane-and-signing/spec.md)
- [x] [09970202] ch02-zero-copy-ffi-keys-and-backends — Zero-copy `PyBackedBytes` FFI, checked `get_range`, raw `StorePath::parse` keys, `PreconditionError`, S3/HTTP backend options, Litestar raw keys, and streaming/conditional/paginated `OciObjectStore` (chapters/ch02-zero-copy-ffi-keys-and-backends/spec.md)
- [x] [46d39d95] ch03-bulk-file-io-compose-and-streams — Streaming `put_file`/`get_file`, bounded-concurrency `download_many`/`upload_many`/`staged_download`, conditional `copy`, `glob`, server-side GCS `compose`, read-ahead `open_reader`, and streaming `open_writer` (chapters/ch03-bulk-file-io-compose-and-streams/spec.md)
- [x] [85bff05a] ch04-streaming-arrow-parquet-and-shm — True streaming `StreamingStorageResponse`, bounded `_arrow.py` layout/readinto/pre-buffer + Parquet dictionary/encoding options, and zero-copy `read_arrow_shm` + columnar Arrow hydration (chapters/ch04-streaming-arrow-parquet-and-shm/spec.md)
- [~] ch05-framework-adapters-and-test-hygiene — SQLSpec, Advanced Alchemy, and Django storage adapter cleanups, `_store.py` alias removal, and pytest top-level import / parameterization hygiene (chapters/ch05-framework-adapters-and-test-hygiene/spec.md)
- [ ] ch06-docs-okf-and-plan-sync — Zensical storage usage/reference docs and OKF `product.md` + `object-storage.md` synchronization (chapters/ch06-docs-okf-and-plan-sync/spec.md)
- **Chapter / Phase Recovery Checkpoint**: `make lint-all && make rs-test && uv run --no-sync pytest src/py/tests/unit/storage src/py/tests/unit/extensions src/py/tests/unit/db -q`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `85bff05a` — `4/6` chapters completed and reviewed (`ch01` `1ff643ec`, `ch02` `09970202`, `ch03` `46d39d95`, `ch04` `85bff05a`).
- **Active Chapter**: `ch05-framework-adapters-and-test-hygiene` (`1/3` tasks completed: `t01` `3b24b0af`).
- **Remaining Plan**: `ch05` (`t02`, `t03` + review) -> `ch06` (`t01`–`t02` + review) -> dual review, archive, push, and stacked PRs (`feat/storage-updates`, `feat/driver-fail-loud`, `feat/native-postgres-driver`).
- **Documentation & OKF Sync**: `[0/6 files documented]`.
- **Active Invariants & Triggers**: Zero `planning_status: empirical_dependent` chapters; every chapter is decision-complete (`planning_status: complete`).

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCache` — Core GCP OAuth2 token cache shared across PyO3 extensions.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs::PySyncGcpTokenCache` — PyO3 `SyncGcpTokenCache` and `AsyncGcpTokenCache` bindings on `vertebra._vertebra`.
[^storage-store-rs]: `src/rs/vertebra-storage/src/store.rs::ObjectStoreEngine` — Rust multicloud `ObjectStoreEngine` over `object_store` and OCI.
[^storage-lib-rs]: `src/rs/vertebra-storage/src/lib.rs::PySyncObjectStore` — PyO3 bindings for `vertebra._storage`.
[^storage-signing-rs]: `src/rs/vertebra-storage/src/signing.rs::sign_gcs_v4_with_options_and_cache_checked` — Native multicloud URL presigning and IAM `signBlob` client.
[^storage-oci-rs]: `src/rs/vertebra-storage/src/oci.rs::OciObjectStore` — Native OCI Object Storage REST implementation.
[^py-store]: `src/py/vertebra/storage/_store.py::SyncObjectStore` — Python `SyncObjectStore`, `AsyncObjectStore`, and `StorageConfig` classes.
[^py-streaming]: `src/py/vertebra/storage/_streaming.py::StreamingStorageResponse` — HTTP `200`/`206`/`416` streaming storage response.
[^py-storage-arrow]: `src/py/vertebra/storage/_arrow.py::iter_batches_sync` — Bounded Arrow IPC and Parquet storage readers and writers.
[^py-db-arrow]: `src/py/vertebra/db/arrow.py::read_arrow_shm` — Shared-memory `VTBARROW` cursor hydration and columnar helpers.
[^py-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py::ObjectStoreBackend` — SQLSpec `ObjectStoreBackend` and DuckDB GCS secret helper.
[^py-alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py::ObjectStoreBackend` — Advanced Alchemy `ObjectStoreBackend` adapter.
[^py-django-storage]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django `Storage` backend over paired native object stores.
[^py-litestar-store]: `src/py/vertebra/extensions/litestar/store.py::ObjectStoreLitestarStore` — Litestar key-value store and filesystem adapter over `ObjectStore`.
