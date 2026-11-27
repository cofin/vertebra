---
type: Guide
title: Object Storage, Streaming & Presigning
description: Native multicloud object I/O, shared GCP token authority, fail-closed credential checks, conditional writes, bulk file transfers, file-like readers and writers, presigning, Arrow/Parquet/CSV codecs, and framework storage adapters.
status: stable
tags:
- domains
- storage
- object-store
- signing
generated:
  by: flow/0.30.1
  at: "2026-11-12T23:30:00Z"
verified:
- by: agent:flow-setup
  at: "2026-11-27T22:10:00Z"
  commit: f3b624a2
- by: agent:flow-executor
  at: "2026-11-19T00:25:00Z"
  commit: 6f76f8a7
- by: agent:flow-executor
  at: "2026-11-17T08:15:00Z"
  commit: 5a833832
- by: agent:flow-archive
  at: "2026-11-14T18:30:30Z"
  commit: 1b191388
- by: agent:flow-learn
  at: "2026-11-14T17:15:00Z"
  commit: 1b191388
- by: agent:flow-setup
  at: "2026-11-12T23:30:00Z"
  commit: 18baadcd
sources:
- id: core-gcp-auth
  resource: src/rs/vertebra-core/src/gcp_auth.rs
  title: Shared Rust GCP OAuth2 token cache and capsule name
- id: core-gcp-identity
  resource: src/rs/vertebra-core/src/gcp_identity.rs
  title: Audience-scoped GCP OIDC ID token cache
- id: py-gcp-auth
  resource: src/rs/vertebra-py/src/gcp_auth.rs
  title: PyO3 GCP token cache bindings in vertebra._vertebra
- id: py-gcp-identity
  resource: src/rs/vertebra-py/src/gcp_identity.rs
  title: PyO3 GCP OIDC ID token cache bindings in vertebra._vertebra
- id: storage-lib
  resource: src/rs/vertebra-storage/src/lib.rs
  title: PyO3 module bindings, buffer extraction, and error mapping for vertebra._storage
- id: storage-store
  resource: src/rs/vertebra-storage/src/store.rs
  title: ObjectStoreEngine, GCS compose, file transfers, and HTTP clients
- id: storage-gcp-auth
  resource: src/rs/vertebra-storage/src/gcp_auth.rs
  title: RefreshingGcpCredentialProvider over vertebra_core::gcp_auth
- id: storage-oci
  resource: src/rs/vertebra-storage/src/oci.rs
  title: Streaming OCI REST v2 ObjectStore and PAR support
- id: storage-r2
  resource: src/rs/vertebra-storage/src/r2.rs
  title: Cloudflare R2 uniform multipart chunking
- id: storage-signing
  resource: src/rs/vertebra-storage/src/signing.rs
  title: Multicloud URL and header signing with pooled IAM clients
- id: storage-pyi
  resource: src/py/vertebra/_storage.pyi
  title: Type stubs for vertebra._storage
- id: py-store
  resource: src/py/vertebra/storage/_store.py
  title: SyncObjectStore, AsyncObjectStore, and SigningConfig
- id: py-config
  resource: src/py/vertebra/storage/_config.py
  title: Provider storage config structs, ObjectMeta, PutResult, and ArtifactReference
- id: py-files
  resource: src/py/vertebra/storage/_files.py
  title: SyncReadableFile, AsyncReadableFile, SyncWritableFile, and AsyncWritableFile
- id: py-secrets
  resource: src/py/vertebra/storage/_secrets.py
  title: SyncGcsBearerSecretProvider and AsyncGcsBearerSecretProvider for DuckDB GCS rotation
- id: py-streaming
  resource: src/py/vertebra/storage/_streaming.py
  title: StreamingStorageResponse and byte-range parsing
- id: py-arrow
  resource: src/py/vertebra/storage/_arrow.py
  title: Demand-driven Arrow and Parquet codec bridge
- id: py-typing
  resource: src/py/vertebra/_typing.py
  title: PreconditionError and IntegrityError definitions
- id: db-arrow
  resource: src/py/vertebra/db/arrow.py
  title: ArrowCursor and zero-copy read_arrow_shm
- id: core-notify
  resource: src/rs/vertebra-core/src/notify.rs
  title: Native Notifier event-loop completion wakeups
- id: ext-litestar-store
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Persistent Litestar Store and confined async filesystem
- id: ext-alchemy-storage
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Advanced Alchemy StorageBackend adapter
- id: ext-sqlspec-storage
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: SQLSpec ObjectStoreProtocol adapter
- id: ext-django-storage
  resource: src/py/vertebra/extensions/django/storage.py
  title: Deconstructible Django Storage with native readers
- id: tests-storage-engine
  resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
  title: Rust integration tests for vertebra-storage
- id: tests-storage-core
  resource: src/py/tests/unit/storage/test_storage_core_and_ranges.py
  title: Python core CRUD, range, and conditional check tests
- id: tests-storage-arrow
  resource: src/py/tests/unit/storage/test_storage_arrow_and_streaming.py
  title: Python Arrow, Parquet, IPC streaming, and buffered reader/writer tests
- id: tests-storage-bulk
  resource: src/py/tests/unit/storage/test_storage_bulk_checksums_and_backends.py
  title: Python bulk transfer, multipart, checksum, and backend wiring tests
- id: tests-storage-integration
  resource: src/py/tests/integration/storage/conftest.py
  title: Emulator integration fixtures
- id: verify-storage-wheel
  resource: tools/scripts/verify_storage_wheel.py::main
  title: Companion wheel and sdist isolation verifier for storage, bigquery, spanner, and bundler
---

# Object Storage, Streaming & Presigning

<!-- truth: start -->
- **Rust Extension (`vertebra._storage`)**: Free-threaded (`gil_used = false`, `_storage.pyi`) bindings over `object_store 0.13`, streaming OCI REST, and multicloud signing. Sync calls detach the GIL; async calls return `TaskAwaitable` drained via per-loop `AsyncCompletionQueue` and `Notifier` (`loop.add_reader`) [^storage-lib] [^storage-pyi] [^core-notify].
- **Shared GCP Token Authority**: `SyncGcpTokenCache` / `AsyncGcpTokenCache` (`gcp_auth.rs`) and audience-scoped `SyncGcpIdTokenCache` / `AsyncGcpIdTokenCache` (`gcp_identity.rs`) live in `vertebra._vertebra` (re-exported by `vertebra.storage`). GCS stores pass `token_cache._capsule()` (`vertebra.GcpTokenCacheCapsule`) into `_storage`, sharing one cache across data I/O, server-side `compose`, and keyless IAM `signBlob` (invalidating and retrying once on `401`) [^core-gcp-auth] [^core-gcp-identity] [^py-gcp-auth] [^py-gcp-identity] [^storage-gcp-auth].
- **Fail-Closed Credentials & HTTP Pooling**: Blank/partial Azure Entra triples, malformed Azure SAS tokens, partial OCI Cavage credentials, and invalid explicit GCS credential options fail with `ImproperConfigurationError`. HTTP clients pool 64 idle connections/host (`object_store`, 10 s connect / 300 s total) or 32 (`reqwest` compose 10 s / 300 s; IAM `signBlob` 10 s / 30 s) [^storage-store] [^storage-signing] [^py-store] [^py-config].
- **Paths, Buffers & Preconditions**: `extract_buffer_bytes` keeps `StorageBuffer`/`bytes` zero-copy and copies writable buffers before GIL release; keys collapse empty `/` and `.` segments and reject `..` (`ValueError`). `AlreadyExists` maps to `FileExistsError`, `Precondition` to `PreconditionError`, and checksum mismatch (`expected_sha256`) to `IntegrityError` [^py-typing] [^storage-lib] [^storage-store].
- **Files, Renames, Bulk & Readers/Writers**: `get_file` writes `.{name}.{pid}.{seq}.part`, calls `sync_all` on file and parent dir (Unix), and renames atomically; `supports_atomic_rename` is `True` only on `file`/`local`, `memory`, and `oci` (`atomic_only=True` raises `ValueError` elsewhere). `download_many`, `upload_many`, and `staged_download` run bounded `buffer_unordered`; `open_reader` caches a 4 MiB read-ahead window and `open_writer` buffers 8 MiB multipart chunks [^storage-store] [^py-store] [^py-files].
- **Arrow/Parquet/CSV & Framework Adapters**: `vertebra.storage._arrow` streams `parquet`, `arrow-ipc`, and `csv`; `read_arrow_shm` wraps `VTBARROW` SHM in `ArrowCursor`. Advanced Alchemy, SQLSpec (`DirectStreamWriteProtocol`), Litestar, and Django adapters require an explicit store, config, or URI [^py-arrow] [^db-arrow] [^ext-alchemy-storage] [^ext-sqlspec-storage] [^ext-django-storage] [^verify-storage-wheel].
<!-- truth: end -->

Known caveats (R2 custom domains, non-atomic cloud rename, unsupported conditions, `fsync`, timeouts) live in [Object Storage Implementation Limitations](object-storage-limitations.md).

## Rust Engine & Signing (`vertebra-storage`)

- **`ObjectStoreEngine` (`store.rs`)**: Wraps `object_store` across `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, and `http`/`https`, adding conditional `put_opts`, multipart uploads (including create-only `LocalCreateOnlyUpload` and `MemoryCreateOnlyUpload` for `if_not_exists`/`if_none_match="*"`), durable `get_file`/`put_file` (>8 MiB part streaming), `rename_with_options`, fast synchronous `memory://` polling (`try_{put,get,get_range}_memory_sync`) without Tokio runtime entry, and 32-way folded GCS `compose` (cleaning temporary objects on success, failure, or cancellation) [^storage-store]. `.to_async()` and `.to_sync()` share the underlying `Arc` (including memory data and HTTP pools) [^storage-lib].
- **GCS, OCI & R2 Behavior**: `allow_ambient_adc=false` uses `GoogleCloudStorageBuilder::new()`; `quota_project_id` attaches `x-goog-user-project`; GCS `if_match` resolves ETag to generation when non-numeric [^storage-store]. [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^storage-oci] streams GET bodies via `bytes_stream`. [`MultipartUploadSession`](../../../../../src/rs/vertebra-storage/src/r2.rs)[^storage-r2] buffers S3/B2/R2/GCS/OCI parts to 8 MiB so non-final parts meet the 5 MiB minimum.
- **Checksums & Buffers**: Uploads return `PutResult` (`sha256`, `crc32c`, `md5` computed over verified bytes; never populated from user metadata); `checksum=True` hashes locally, and reads verify `expected_sha256` (`VerifyingStream`) raising `IntegrityError` on mismatch. `get_range` rejects `offset + length` overflow and answers `length == 0` with `head` + empty bytes.
- **Presigning & Verification (`signing.rs`)**: Implements Vertebra HMAC URLs, AWS/R2 SigV4 (1–604,800 s, `virtual_hosted_style`), GCS V4 `GOOG4-RSA-SHA256` (local PKCS#8 or IAM `signBlob`, honoring `GCSConfig.endpoint`), Azure service SAS, and OCI PAR/Cavage signatures, plus matching `verify_presigned_url` verifiers [^storage-signing] [^storage-oci].

## Python Facade (`vertebra.storage`)

- **Configs & Token Auto-Wiring (`_config.py`, `_store.py`, `_secrets.py`)**: Provider configs (`LocalConfig`, `S3Config`, `GCSConfig`, `AzureConfig`, `OCIConfig`, `R2Config`, `HTTPConfig`, `MemoryConfig`) and metadata structs (`ObjectMeta`, `PutResult`, `ArtifactReference`) live in [`_config.py`](../../../../../src/py/vertebra/storage/_config.py)[^py-config] as frozen `msgspec.Struct`s re-exported by [`_store.py`](../../../../../src/py/vertebra/storage/_store.py)[^py-store], requiring an explicit target (`MissingDependencyError` for `vertebra-core[storage]` if `_storage` is absent). `AzureConfig` and `OCIConfig` preserve explicit empty strings so Rust fails closed. Configs with credentials auto-derive `SigningConfig`. GCS stores auto-create a token cache when `access_token` and `target_service_account` are omitted; [`_secrets.py`](../../../../../src/py/vertebra/storage/_secrets.py)[^py-secrets] exposes GCS bearer rotation providers for DuckDB.
- **Bulk Transfers & `glob`**: `download_many`, `upload_many`, and `staged_download` (unique dir under writable `/dev/shm`, else temp dir) reject duplicate keys/basenames before starting. `glob` matches full keys (`*` and `?` match `/`).
- **Multipart & File-Like I/O (`_files.py`, `_store.py`)**: `put_multipart(path, parts)` streams each part before advancing the iterable and aborts on producer failure. [`SyncWritableFile` / `AsyncWritableFile`](../../../../../src/py/vertebra/storage/_files.py)[^py-files] lazily open multipart uploads on the first full part or close (`aclose()` returns `PutResult | None`; unclosed async writers emit `ResourceWarning`). `SyncReadableFile` / `AsyncReadableFile` serve short reads from a 4 MiB cached range window (`read_at` preserves cursor offset) [^py-files] [^py-store].

## Arrow, Parquet & CSV Codecs (`_arrow.py`)

- **Formats & Streaming**: Supports `parquet`, `arrow-ipc` (`.arrow`, `.ipc`, `.feather`), and `csv` (`text/csv`, `pyarrow.csv.open_csv` / `CSVWriter`), draining each encoded chunk into `put_stream` before advancing input. Async PyArrow work runs in per-demand pool steps while I/O stays on the caller loop, cleaning up before returning on cancellation [^py-arrow].
- **Options & Projection**: Writers accept Tables, RecordBatches, `ArrowCursor`, `__arrow_c_stream__`, or batch iterables (forwarding `if_not_exists`, rejecting schema-less empty iterables). Readers support `columns` projection; Parquet batch readers use `read_ahead=0, pre_buffer=False`, whole-table reads default to `pre_buffer=True`, and `write_parquet` exposes `use_dictionary`, `use_byte_stream_split`, and `options` overrides [^py-arrow] [^py-store].

## HTTP Range & Framework Adapters

- **HTTP Range (`_streaming.py`)**: `parse_byte_range` parses closed, open-ended, and suffix ranges (`RangeNotSatisfiableError` on invalid input); `StreamingStorageResponse.from_sync_store` / `from_async_store` issue one `head` and return `200`, `206`, or `416` (`to_async_fastpath_tuple()` for async stores) [^py-streaming].
- **Litestar (`extensions/litestar/store.py`)**: `ObjectStoreLitestarStore` wraps payloads in a versioned `VTBSTORE\x01` MessagePack envelope with expiry deadline; `ObjectStoreFileSystemAdapter` confines static GET/HEAD to a base path [^ext-litestar-store].
- **Advanced Alchemy (`extensions/alchemy/storage.py`)**: `ObjectStoreBackend` and `register_storage_backend` forward `signing`/`token_cache` only for config/URI construction, forward integrity kwargs on `save_object*`, and add `save_arrow` / `save_arrow_async` [^ext-alchemy-storage].
- **SQLSpec (`extensions/sqlspec/storage.py`)**: `ObjectStoreBackend` subclasses `ObjectStoreBase` (`ObjectStoreProtocol` and `DirectStreamWriteProtocol`: `supports_direct_arrow_write`, `open_write_stream_*`), adds `write_arrow_*` / `write_arrow_cursor_*` (`overwrite=False` -> `if_not_exists=True`), delegates `read_arrow_*` to whole-table reads and `stream_arrow_*` to batch readers, rejects `signing`/`token_cache` on prebuilt stores, and pairs with `ObjectStoreRegistry` (`get_object_store_registry()`) for native `memory://`/`oci://`/`r2://` and missing-backend resolution [^ext-sqlspec-storage].
- **Django (`extensions/django/storage.py`)**: `DjangoStorage` is migration-deconstructible, forwards `.sha256`/`.crc32c`/`.md5` digests, and opens `File` handles over `SyncReadableFile` [^ext-django-storage].

## Tests

Rust engine, streaming, and signing tests live in [`storage_engine_tests.rs`](../../../../../src/rs/vertebra-storage/tests/storage_engine_tests.rs)[^tests-storage-engine]; Python unit tests live in [`test_storage_core_and_ranges.py`](../../../../../src/py/tests/unit/storage/test_storage_core_and_ranges.py)[^tests-storage-core], [`test_storage_arrow_and_streaming.py`](../../../../../src/py/tests/unit/storage/test_storage_arrow_and_streaming.py)[^tests-storage-arrow], and [`test_storage_bulk_checksums_and_backends.py`](../../../../../src/py/tests/unit/storage/test_storage_bulk_checksums_and_backends.py)[^tests-storage-bulk]; emulator fixtures live in [`conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py)[^tests-storage-integration]; wheel isolation is verified by [`verify_storage_wheel.py`](../../../../../tools/scripts/verify_storage_wheel.py)[^verify-storage-wheel] and `make {storage,bigquery,spanner,bundler}-wheel-check`.

[^core-gcp-auth]: `src/rs/vertebra-core/src/gcp_auth.rs` — Shared Rust GCP OAuth2 token cache and capsule name.
[^core-gcp-identity]: `src/rs/vertebra-core/src/gcp_identity.rs` — Audience-scoped GCP OIDC ID token cache.
[^py-gcp-auth]: `src/rs/vertebra-py/src/gcp_auth.rs` — PyO3 GCP token cache bindings in `vertebra._vertebra`.
[^py-gcp-identity]: `src/rs/vertebra-py/src/gcp_identity.rs` — PyO3 GCP OIDC ID token cache bindings in `vertebra._vertebra`.
[^storage-lib]: `src/rs/vertebra-storage/src/lib.rs` — PyO3 module bindings, buffer extraction, and error mapping.
[^storage-store]: `src/rs/vertebra-storage/src/store.rs` — `ObjectStoreEngine`, GCS compose, file transfers, and HTTP clients.
[^storage-gcp-auth]: `src/rs/vertebra-storage/src/gcp_auth.rs` — `RefreshingGcpCredentialProvider`.
[^storage-oci]: `src/rs/vertebra-storage/src/oci.rs` — Streaming OCI REST v2 `ObjectStore` and PAR support.
[^storage-r2]: `src/rs/vertebra-storage/src/r2.rs` — Cloudflare R2 uniform multipart chunking.
[^storage-signing]: `src/rs/vertebra-storage/src/signing.rs` — Multicloud URL and header signing with pooled IAM clients.
[^storage-pyi]: `src/py/vertebra/_storage.pyi` — Type stubs for `vertebra._storage`.
[^py-store]: `src/py/vertebra/storage/_store.py` — `SyncObjectStore`, `AsyncObjectStore`, and `SigningConfig`.
[^py-config]: `src/py/vertebra/storage/_config.py` — Provider storage config structs, `ObjectMeta`, `PutResult`, and `ArtifactReference`.
[^py-files]: `src/py/vertebra/storage/_files.py` — `SyncReadableFile`, `AsyncReadableFile`, `SyncWritableFile`, and `AsyncWritableFile`.
[^py-secrets]: `src/py/vertebra/storage/_secrets.py` — `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` for DuckDB GCS rotation.
[^py-streaming]: `src/py/vertebra/storage/_streaming.py` — `StreamingStorageResponse` and byte-range parsing.
[^py-arrow]: `src/py/vertebra/storage/_arrow.py` — Demand-driven Arrow and Parquet codec bridge.
[^py-typing]: `src/py/vertebra/_typing.py` — `PreconditionError` and `IntegrityError` definitions.
[^db-arrow]: `src/py/vertebra/db/arrow.py` — `ArrowCursor` and zero-copy `read_arrow_shm`.
[^core-notify]: `src/rs/vertebra-core/src/notify.rs` — Native `Notifier` event-loop completion wakeups.
[^ext-litestar-store]: `src/py/vertebra/extensions/litestar/store.py` — Persistent Litestar Store and confined async filesystem.
[^ext-alchemy-storage]: `src/py/vertebra/extensions/alchemy/storage.py` — Advanced Alchemy StorageBackend adapter.
[^ext-sqlspec-storage]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreProtocol adapter.
[^ext-django-storage]: `src/py/vertebra/extensions/django/storage.py` — Deconstructible Django Storage with native readers.
[^tests-storage-engine]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests for `vertebra-storage`.
[^tests-storage-core]: `src/py/tests/unit/storage/test_storage_core_and_ranges.py` — Python core CRUD, range, and conditional check tests.
[^tests-storage-arrow]: `src/py/tests/unit/storage/test_storage_arrow_and_streaming.py` — Python Arrow, Parquet, IPC streaming, and buffered reader/writer tests.
[^tests-storage-bulk]: `src/py/tests/unit/storage/test_storage_bulk_checksums_and_backends.py` — Python bulk transfer, multipart, checksum, and backend wiring tests.
[^tests-storage-integration]: `src/py/tests/integration/storage/conftest.py` — Emulator integration fixtures.
[^verify-storage-wheel]: `tools/scripts/verify_storage_wheel.py::main` — Companion wheel and sdist isolation verifier for `storage`, `bigquery`, `spanner`, and `bundler`.
