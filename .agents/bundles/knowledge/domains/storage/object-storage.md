---
type: Guide
title: "Object Storage, Streaming & Presigning"
description: "Native multicloud streaming, conditional writes, signing, and shared sync/async storage contracts for SQLSpec, Advanced Alchemy, and Django."
status: stable
tags:
  - domains
  - storage
  - object-store
  - signing
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:docs-okf-specialist
    at: "2026-11-08T16:09:37Z"
    commit: 99317bed
sources:
  - id: src-vertebra-core-src-gcp-auth-rs
    resource: src/rs/vertebra-core/src/gcp_auth.rs
    title: "Shared Rust GCP OAuth2 token cache (src/rs/vertebra-core/src/gcp_auth.rs)"
  - id: src-vertebra-core-src-notify-rs
    resource: src/rs/vertebra-core/src/notify.rs
    title: "Cross-platform eventfd/pipe Notifier (src/rs/vertebra-core/src/notify.rs)"
  - id: src-vertebra-py-src-gcp-auth-rs
    resource: src/rs/vertebra-py/src/gcp_auth.rs
    title: "PyO3 GCP token cache bindings in vertebra._vertebra"
  - id: src-vertebra-storage-cargo-toml
    resource: src/rs/vertebra-storage/Cargo.toml
    title: "Rust crate manifest (src/rs/vertebra-storage/Cargo.toml)"
  - id: src-vertebra-storage-src-lib-rs
    resource: src/rs/vertebra-storage/src/lib.rs
    title: "PyO3 module bindings for vertebra._storage (src/rs/vertebra-storage/src/lib.rs)"
  - id: src-storage-pyi
    resource: src/py/vertebra/_storage.pyi
    title: "Python type stub for vertebra._storage"
  - id: src-vertebra-storage-src-store-rs
    resource: src/rs/vertebra-storage/src/store.rs
    title: "ObjectStoreEngine and MultipartUploadSession (src/rs/vertebra-storage/src/store.rs)"
  - id: src-vertebra-storage-src-gcp-auth-rs
    resource: src/rs/vertebra-storage/src/gcp_auth.rs
    title: "RefreshingGcpCredentialProvider over vertebra_core::gcp_auth (src/rs/vertebra-storage/src/gcp_auth.rs)"
  - id: src-vertebra-storage-src-oci-rs
    resource: src/rs/vertebra-storage/src/oci.rs
    title: "OCI REST v2 ObjectStore and PAR support (src/rs/vertebra-storage/src/oci.rs)"
  - id: src-vertebra-storage-src-r2-rs
    resource: src/rs/vertebra-storage/src/r2.rs
    title: "Cloudflare R2 uniform multipart chunking (src/rs/vertebra-storage/src/r2.rs)"
  - id: src-vertebra-storage-src-signing-rs
    resource: src/rs/vertebra-storage/src/signing.rs
    title: "Multicloud URL and header signing (src/rs/vertebra-storage/src/signing.rs)"
  - id: src-vertebra-storage-tests-storage-engine-tests-rs
    resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
    title: "Rust integration tests for vertebra-storage (src/rs/vertebra-storage/tests/storage_engine_tests.rs)"
  - id: src-storage-init-py
    resource: src/py/vertebra/storage/__init__.py
    title: "Public vertebra.storage exports (src/py/vertebra/storage/__init__.py)"
  - id: src-storage-store-py
    resource: src/py/vertebra/storage/_store.py
    title: "Fail-closed SyncObjectStore and AsyncObjectStore facade (src/py/vertebra/storage/_store.py)"
  - {id: src-storage-streaming-py, resource: src/py/vertebra/storage/_streaming.py, title: "StreamingStorageResponse FastPath descriptor (src/py/vertebra/storage/_streaming.py)"}
  - {id: src-extensions-litestar-store-py, resource: src/py/vertebra/extensions/litestar/store.py, title: "Persistent Litestar Store and confined async filesystem"}
  - {id: src-tests-storage-store-py, resource: src/py/tests/unit/storage/test_storage_store.py, title: "Range parsing, async ownership, and storage response contracts"}
  - {id: src-tests-zero-copy-buffer-py, resource: src/py/tests/unit/server/test_zero_copy_buffer.py, title: "Native async storage handler buffer submission"}
  - {id: src-tests-litestar-plugin-py, resource: src/py/tests/unit/extensions/test_litestar_plugin.py, title: "Persistent TTL, namespaces, static GET/HEAD, and explicit Range handler"}
  - id: src-extensions-alchemy-storage-py
    resource: src/py/vertebra/extensions/alchemy/storage.py
    title: "Advanced Alchemy StorageBackend adapter (src/py/vertebra/extensions/alchemy/storage.py)"
  - id: src-extensions-sqlspec-storage-py
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: "SQLSpec ObjectStoreProtocol adapter (src/py/vertebra/extensions/sqlspec/storage.py)"
  - id: src-extensions-django-storage-py
    resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
    title: "Deconstructible Django Storage with native readers and paired stores"
  - id: src-tests-data-storage-integrations-py
    resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
    title: "SQLSpec protocol, Advanced Alchemy FileObject, registry, and streaming tests"
  - id: src-tests-django-plugin-py
    resource: src/py/tests/unit/extensions/test_django_plugin.py
    title: "Django File, migration reconstruction, naming, timestamp, and async tests"
  - id: src-tests-integration-storage-conftest-py
    resource: src/py/tests/integration/storage/conftest.py
    title: "Floci emulator integration fixtures (src/py/tests/integration/storage/conftest.py)"
---

# Object Storage, Streaming & Presigning

<!-- truth: start -->
- **Rust Extension, Streaming & GIL-Free Async Execution (`vertebra-storage`, `vertebra.storage`)**: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs] (typed in [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)[^src-storage-pyi]) builds `vertebra._storage` (`#[pymodule(gil_used = false)]`) over `object_store 0.13`. `SyncObjectStore`, `SyncStreamReader`, and `SyncMultipartUpload` release the GIL via `py.detach()`, while `AsyncObjectStore`, `AsyncStreamReader`, `AsyncMultipartUpload`, and `py_sign_gcs_v4_iam_url_async` return a `RustTaskAwaitable` backed by a per-loop `AsyncCompletionQueue` and [`vertebra_core::notify::Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs)[^src-vertebra-core-src-notify-rs] (`eventfd` on Linux, pipe elsewhere) registered via `loop.add_reader`, completing on `STORAGE_RUNTIME` with zero Tokio `Python::attach` calls.
- **Unified GCP Token Authority, Multi-Cloud Signing & Fail-Closed Facade**: [`vertebra.storage`](../../../../../src/py/vertebra/storage/_store.py)[^src-storage-store-py] re-exports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` directly from [`vertebra._vertebra`](../../../../../src/rs/vertebra-py/src/gcp_auth.rs)[^src-vertebra-py-src-gcp-auth-rs] over [`vertebra_core::gcp_auth::GcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs)[^src-vertebra-core-src-gcp-auth-rs] (with `.to_async()` / `.to_sync()` sharing the underlying `Arc<GcpTokenCache>` and `allow_ambient_adc`), while [`vertebra-storage/src/gcp_auth.rs`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs)[^src-vertebra-storage-src-gcp-auth-rs] exposes only `RefreshingGcpCredentialProvider` and `GCS_OAUTH_SCOPES`. `SyncObjectStore` and `AsyncObjectStore` fail closed with `MissingDependencyError` (`vertebra-core[storage]`) when `vertebra._storage` is absent and `ImproperConfigurationError` on missing or invalid target URIs/configs, with zero pure-Python `_memory` dict fallbacks or `urllib` OAuth fetchers.
<!-- truth: end -->

Object storage comprises two layers: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-cargo-toml] (`vertebra._storage`, `#[pymodule(gil_used = false)]`) over `object_store 0.13`, and [`vertebra.storage`](../../../../../src/py/vertebra/storage/__init__.py)[^src-storage-init-py], which resolves `vertebra._storage` lazily at store construction time via `import_optional("vertebra._storage")` and fails closed when the optional wheel is uninstalled.

## 1. Rust Engine (`store.rs`, `oci.rs`, `r2.rs`, `lib.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BackendConfig` / `ObjectStoreEngine`](../../../../../src/rs/vertebra-storage/src/store.rs)[^src-vertebra-storage-src-store-rs] | `store.rs` | Wraps `Arc<dyn object_store::ObjectStore>` across `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, and `http`/`https`. Exposes `put_opts` (`PutConditionalOptions`: `if_not_exists`, `if_match`, `if_none_match`, `content_type`, `attributes`), `get_stream`, `create_multipart`, `list_page`, `list_with_delimiter`, `rename`, `get_range`, `head`, `exists`, `delete`, `copy`, and `list`. |
| `PySyncStreamReader` / `PyAsyncStreamReader` (`SyncStreamReader` / `AsyncStreamReader`) | `lib.rs` | Pulls streaming `Bytes` chunks from `ObjectStoreEngine::get_stream` and slices them to optional `chunk_size` bounds as zero-copy `StorageBuffer` views without whole-object buffering. |
| `MultipartUploadSession` (`PySyncMultipartUpload` / `PyAsyncMultipartUpload`) | `store.rs`, `lib.rs` | Stateful `Box<dyn MultipartUpload>` session (`write_part`, `complete`, `abort`). For `r2` and `oci`, buffers writes into uniform `8 MiB` parts ([`R2_UNIFORM_PART_SIZE`](../../../../../src/rs/vertebra-storage/src/r2.rs)[^src-vertebra-storage-src-r2-rs]) incrementally without concatenating full payloads into `Vec<u8>`. |
| `RefreshingGcpCredentialProvider` | `gcp_auth.rs` | Bridges `Arc<vertebra_core::gcp_auth::GcpTokenCache>` (`GCS_OAUTH_SCOPES`) into `GoogleCloudStorageBuilder::with_credentials(...)` when no static `service_account_path` or `service_account_key` is configured. |
| [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^src-vertebra-storage-src-oci-rs] | `oci.rs` | Native `ObjectStore` over OCI Object Storage REST v2 (`/n/{ns}/b/{bucket}/o/{name}`, pre-authenticated `par_url`, RSA-SHA256 Cavage request signing, and `OciMultipartUpload`). |
| `PySyncObjectStore` / `PyAsyncObjectStore` / `RustTaskAwaitable` | `lib.rs` | Frozen PyO3 classes (`SyncObjectStore` / `AsyncObjectStore`) whose `.to_async()` and `.to_sync()` methods clone the shared `ObjectStoreEngine` (`Arc`), keeping `InMemory` namespaces and HTTP pools unified across sync and async callers. |

Module signing functions exported by `_storage`: `py_sign_hmac_url`, `py_verify_hmac_url`, `py_sign_s3_v4_url`, `py_verify_s3_v4_url`, `py_sign_aws_sigv4_headers`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`, `build_gcs_v4_canonical_request`, `assemble_gcs_iam_v4_url`, `py_sign_oci_cavage_headers`, `sign_oci_par_url`, `py_sign_azure_sas_url`, `py_chunk_r2_uniform_parts`.

## 2. Signing (`signing.rs`, `oci.rs`)

| Symbol | Scheme |
|---|---|
| `HmacSigner` (`sign_url`, `verify_url`) | Vertebra HMAC-SHA256 URLs with `X-Vtb-Expires`, `X-Vtb-Method`, `X-Vtb-Signature` (`constant_time_eq` verification). |
| `S3SignConfig`, `sign_s3_v4_url`, `verify_s3_v4_url` | AWS SigV4 query presigning (`UNSIGNED-PAYLOAD`, RFC 3986 path encoding, 1–604,800s expiry bounds)[^src-vertebra-storage-src-signing-rs]. |
| `AwsSigV4HeaderConfig`, `sign_aws_sigv4_headers` | AWS SigV4 request header signing (`Authorization`, `x-amz-date`, `x-amz-content-sha256`, optional `x-amz-security-token`). |
| `GcsSignConfig`, `sign_gcs_v4_with_iam_endpoint` | GCS V4 (`GOOG4-RSA-SHA256`) local PKCS#8 signing or sync/async (`RustTaskAwaitable`) IAM Credentials `signBlob` signing (1–604,800s expiry). |
| `AzureSasConfig`, `sign_azure_sas_url` | Azure Blob service SAS (`sv=2020-12-06`, `sr=b`, `sp=r`/`sp=cw`). |
| `sign_oci_par_url` | OCI Pre-Authenticated Request URL construction (`/p/{token}/n/{ns}/b/{bucket}/o/{encoded_key}`) with positive expiry validation. |

## 3. Python Facade (`_store.py`)

Provider configs are `msgspec.Struct(frozen=True, gc=False, kw_only=True)` implementing `to_url_and_options()`: `LocalConfig`, `S3Config`, `GCSConfig`, `AzureConfig`, `OCIConfig`, `R2Config`, `HTTPConfig`, `MemoryConfig` (`StorageConfig` union), paired with `SigningConfig`.

- `SyncObjectStore(target, *, options, signing, token_cache)` and `AsyncObjectStore(target, *, options, signing, token_cache)` validate schemes upfront and delegate every CRUD, streaming (`get_stream`, `put_stream`), conditional `put`, `list_page`, `list_with_delimiter`, `rename`, and `get_view` (`memoryview` over `StorageBuffer`) call to `vertebra._storage`.
- `GCSConfig.sync_token_cache` / `async_token_cache` construct `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`. Calling `store.to_async()` or `store.to_sync()` converts any attached token cache via `.to_async()` / `.to_sync()`, preserving a single shared Rust `Arc<GcpTokenCache>` generation counter.
- `store.sign(key, *, expires_in=3600, method="GET")`, `presign_get`, and `presign_put` support `gs` (local RSA and keyless IAM `signBlob` with automatic `401` token-generation invalidation and retry), `s3`/`r2`, `azure`, `oci` (`par_token` or `par_url_prefix`), and `hmac`.

## 4. Range Reads & Ecosystem Adapters

| Symbol | Source | Responsibility |
|---|---|---|
| [`SyncReadableFile` / `AsyncReadableFile`](../../../../../src/py/vertebra/storage/_store.py) | `_store.py` | Seekable read-only streams (`io.RawIOBase` on sync) streaming from offset `0` via `get_stream` in `__iter__`/`__aiter__` and serving random-access `read` / cursor-preserving `read_at` via `get_range` for PyArrow and Polars Parquet footers. |
| [`StreamingStorageResponse`](../../../../../src/py/vertebra/storage/_streaming.py)[^src-storage-streaming-py] | `_streaming.py` | Frozen `msgspec.Struct` descriptor with metadata-resolved HTTP `200`/`206`/`416`, sync/async chunks and FastPath tuples, and `307` presigned URL handoffs. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] | `extensions/alchemy/storage.py` | Concrete Advanced Alchemy `StorageBackend` with real registry registration, native `FileObject` operations, and bounded streaming uploads through paired stores. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] | `extensions/sqlspec/storage.py` | Exact SQLSpec `ObjectStoreProtocol` `*_sync` / `*_async` methods plus native buffer-stream, paginated-listing, and seekable-reader extensions over paired stores. |
| [`DjangoStorage`](../../../../../src/py/vertebra/extensions/django/storage.py)[^src-extensions-django-storage-py] | `extensions/django/storage.py` | Deconstructible Django `Storage` subclass returning real seekable `File` handles, with native sync/async persistence, naming, URL, directory, local-path, and modification-time contracts. |

### HTTP Range and Native Response Consumers

`ByteRange(start, end_exclusive, total_size)` and `parse_byte_range` resolve one closed, open-ended, or suffix byte range, clamp its end, and ignore unknown units. Malformed, multiple, zero-suffix, or unsatisfiable byte ranges raise `RangeNotSatisfiableError`. `from_sync_store` / awaited `from_async_store` perform one native HEAD and produce `200`, `206`, or empty `416` (`Content-Range: bytes */<size>`), with authoritative length, ETag, content type, and `Accept-Ranges: bytes`.[^src-storage-streaming-py][^src-tests-storage-store-py]

For async stores, explicitly await `to_async_fastpath_tuple()` in the handler: it returns `(status, list[tuple[bytes, bytes]], memoryview | bytes)` through native async `get_view` or `get_range`, with no synchronous store calls. Full reads retain the `StorageBuffer` owner through a memoryview; ranges return bytes. Pass the resolved body and byte header pairs to native `Response.from_buffer`; decode header pairs with Latin-1 at the `submit_raw_response` helper's string-header boundary. Async stores reject synchronous `to_fastpath_tuple()`. `aiter_chunks()` uses native full-object streams or bounded range reads.[^src-storage-streaming-py][^src-tests-zero-copy-buffer-py]

### Litestar Store and Static Files

`ObjectStoreLitestarStore` accepts an explicit async store, config, or URI. Each object contains a versioned `VTBSTORE\x01` MessagePack envelope holding the wall-clock millisecond deadline and bytes, so TTL survives store reconstruction even when backend attributes disappear. Expired entries are deleted on read; renewal applies only to expiring entries. Logical keys are UTF-8 hex encoded; `with_namespace` composes validated child prefixes, and paginated `delete_all` removes that namespace and descendants while preserving siblings.[^src-extensions-litestar-store-py][^src-tests-litestar-plugin-py]

`ObjectStoreFileSystemAdapter(store, base_path="public")` exposes a confined virtual `/`, async metadata, and native `AsyncReadableFile` handles with async reads and synchronous seek/tell. Configure `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)` for static GET/HEAD. Explicit Range handling uses a separate route: build `StreamingStorageResponse.from_async_store(..., range_header=request.headers.get("range"))`, then return Litestar `Stream` over byte-converted `aiter_chunks()`, forwarding status, headers, content range, and media type.[^src-extensions-litestar-store-py][^src-tests-litestar-plugin-py]

### SQLSpec Protocol and Native Extensions

`ObjectStoreBackend` implements SQLSpec's byte/text, CRUD, listing, metadata, and signing methods with their exact `*_sync` / `*_async` names. Native extensions expose `get_stream_sync` / `get_stream_async` as `StorageBuffer` iterators, `put_stream_*` as incremental uploads returning `PutResult`, `list_page_*` as relative-key pages with opaque native cursors, and `open_sync` / `open_async` as seekable-reader factories. SQLSpec `stream_read_sync` yields `bytes`; callers await `stream_read_async` before iterating its byte chunks. Conditional and metadata write options reach native `put` / `put_stream`, and moves use native `rename`.[^src-extensions-sqlspec-storage-py]

Both reader factories are synchronous. `open_sync(size=None)` resolves metadata immediately; supplying `size` avoids that lookup. `open_async` defers metadata until reading. Keys and configured namespaces reject parent traversal. Arrow/Parquet helpers and `build_duckdb_gcs_secret_sql` retain their existing contracts.[^src-extensions-sqlspec-storage-py]

### Advanced Alchemy Registry and File Objects

`register_storage_backend(store, key="media")` and `register_storage_backend("media", store=store)` register the concrete `ObjectStoreBackend` through `StorageRegistry.register_backend`; consumers resolve it through `get_backend`. Construction validates the target before registry/default mutation, and imports preserve the upstream registry API. `FileObject.save` / `save_async` use native `put`, `put_file`, or incremental `put_stream`; file readers receive bounded `chunk_size` reads, while legacy no-argument async readers retain their one-read behavior. Saved objects receive native size/etag, and `get_content` / `get_content_async` return stored bytes. Producer exceptions and async cancellation propagate through multipart cleanup.[^src-extensions-alchemy-storage-py]

### Django Storage and File Handles

`DjangoStorage` subclasses Django's real `Storage` and uses its deconstructible constructor for migration reconstruction. Django's public `MigrationWriter` serializer registry handles only Vertebra's eight `StorageConfig` types, preserving every field through public configuration constructors and recursive field serialization. `_open` / inherited `open` return a `File` subclass over `SyncReadableFile`; reopening resets or recreates the reader through the same native store and qualified key, including `FieldFile` consumers. `aopen` awaits native metadata and supplies its size before constructing the synchronous Django handle. Subsequent reads and reopening follow Django's synchronous interface. Runtime return-annotation introspection resolves to Django's concrete `File`, while static typing uses `File[bytes]`.[^src-extensions-django-storage-py]

`save` follows Django validation and available-name allocation, and `asave` applies equivalent collision/max-length rules with native async existence checks. `ContentFile` chunks and async iterables upload incrementally; text chunks encode as UTF-8. Names reject traversal before `location` qualification. `listdir` returns sorted direct children; `path` resolves under an explicit local root and raises `NotImplementedError` on remote or memory stores. `url` / `aurl` sign qualified keys or quote them under `base_url`. `get_modified_time` returns an aware UTC datetime from `ObjectMeta.last_modified`; creation/access hooks raise `NotImplementedError` because native metadata supplies neither timestamp.[^src-extensions-django-storage-py]

## 5. Emulator Fixtures & Test Suites

[`src/py/tests/integration/storage/conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py)[^src-tests-integration-storage-conftest-py] defines Floci emulator fixtures (`floci_s3_config`, `floci_azure_config`, `floci_gcs_config`, `floci_r2_config`). Rust engine and PyO3 streaming/signing tests run in [`storage_engine_tests.rs`](../../../../../src/rs/vertebra-storage/tests/storage_engine_tests.rs)[^src-vertebra-storage-tests-storage-engine-tests-rs], and Python unit suites live in `src/py/tests/unit/storage/`. Adapter consumer coverage exercises SQLSpec protocols/pipelines and Advanced Alchemy registry/`FileObject` behavior in [`test_data_storage_integrations.py`](../../../../../src/py/tests/unit/extensions/test_data_storage_integrations.py)[^src-tests-data-storage-integrations-py], and Django migration reconstruction, seekable files, naming, async persistence, timestamps, and signing in [`test_django_plugin.py`](../../../../../src/py/tests/unit/extensions/test_django_plugin.py)[^src-tests-django-plugin-py].

## 6. Operational Invariants & Gotchas

- **Fail-closed construction**: Omitting or passing an empty/schemeless target raises `ImproperConfigurationError`; missing `vertebra._storage` raises `MissingDependencyError` (`vertebra-core[storage]`). Ephemeral test stores must pass `MemoryConfig()` or `"memory://<namespace>"` explicitly and pair sync/async handles via `.to_async()` / `.to_sync()` so they share the same Rust `InMemory` instance.
- **Adapter targets**: SQLSpec and Advanced Alchemy `ObjectStoreBackend`, `register_storage_backend`, and `DjangoStorage` require an explicit store, config, or URI. Paired handles share the engine; explicit `MemoryConfig()` remains available for tests. Advanced Alchemy and Django adapters use native async operations without `asyncio.to_thread`.[^src-extensions-sqlspec-storage-py][^src-extensions-alchemy-storage-py][^src-extensions-django-storage-py]
- **Multipart stream cleanup**: `put_stream` aborts the active `MultipartUploadSession` on producer exceptions so uncommitted parts are cleaned up.
- **Offline verification scope**: `verify_presigned_url` supports `hmac` and `s3` only; `gcs`, `azure`, and `oci` raise `NotImplementedError`.

[^src-vertebra-core-src-gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs` — Shared Rust GCP OAuth2 token cache (src/rs/vertebra-core/src/gcp_auth.rs).
[^src-vertebra-core-src-notify-rs]: `src/rs/vertebra-core/src/notify.rs` — Cross-platform eventfd/pipe Notifier (src/rs/vertebra-core/src/notify.rs).
[^src-vertebra-py-src-gcp-auth-rs]: `src/rs/vertebra-py/src/gcp_auth.rs` — PyO3 GCP token cache bindings in vertebra._vertebra.
[^src-vertebra-storage-cargo-toml]: `src/rs/vertebra-storage/Cargo.toml` — Rust crate manifest (src/rs/vertebra-storage/Cargo.toml).
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — PyO3 module bindings for vertebra._storage (src/rs/vertebra-storage/src/lib.rs).
[^src-storage-pyi]: `src/py/vertebra/_storage.pyi` — Python type stub for vertebra._storage.
[^src-vertebra-storage-src-store-rs]: `src/rs/vertebra-storage/src/store.rs` — ObjectStoreEngine and MultipartUploadSession (src/rs/vertebra-storage/src/store.rs).
[^src-vertebra-storage-src-gcp-auth-rs]: `src/rs/vertebra-storage/src/gcp_auth.rs` — RefreshingGcpCredentialProvider over vertebra_core::gcp_auth (src/rs/vertebra-storage/src/gcp_auth.rs).
[^src-vertebra-storage-src-oci-rs]: `src/rs/vertebra-storage/src/oci.rs` — OCI REST v2 ObjectStore and PAR support (src/rs/vertebra-storage/src/oci.rs).
[^src-vertebra-storage-src-r2-rs]: `src/rs/vertebra-storage/src/r2.rs` — Cloudflare R2 uniform multipart chunking (src/rs/vertebra-storage/src/r2.rs).
[^src-vertebra-storage-src-signing-rs]: `src/rs/vertebra-storage/src/signing.rs` — Multicloud URL and header signing (src/rs/vertebra-storage/src/signing.rs).
[^src-vertebra-storage-tests-storage-engine-tests-rs]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests for vertebra-storage (src/rs/vertebra-storage/tests/storage_engine_tests.rs).
[^src-storage-init-py]: `src/py/vertebra/storage/__init__.py` — Public vertebra.storage exports (src/py/vertebra/storage/__init__.py).
[^src-storage-store-py]: `src/py/vertebra/storage/_store.py` — Fail-closed SyncObjectStore and AsyncObjectStore facade (src/py/vertebra/storage/_store.py).
[^src-storage-streaming-py]: `src/py/vertebra/storage/_streaming.py` — StreamingStorageResponse FastPath descriptor (src/py/vertebra/storage/_streaming.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Advanced Alchemy StorageBackend adapter (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreProtocol adapter (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-extensions-django-storage-py]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Django Storage and native file contracts.
[^src-tests-data-storage-integrations-py]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` — SQLSpec and Advanced Alchemy native consumer behavior.
[^src-tests-django-plugin-py]: `src/py/tests/unit/extensions/test_django_plugin.py` — Django Storage consumer behavior and annotation introspection.
[^src-tests-integration-storage-conftest-py]: `src/py/tests/integration/storage/conftest.py` — Floci emulator integration fixtures (src/py/tests/integration/storage/conftest.py).
[^src-extensions-litestar-store-py]: `src/py/vertebra/extensions/litestar/store.py` — Persistent Litestar Store and confined async filesystem.
[^src-tests-storage-store-py]: `src/py/tests/unit/storage/test_storage_store.py` — Range parsing, async ownership, and storage response contracts.
[^src-tests-zero-copy-buffer-py]: `src/py/tests/unit/server/test_zero_copy_buffer.py` — Native async storage handler buffer submission.
[^src-tests-litestar-plugin-py]: `src/py/tests/unit/extensions/test_litestar_plugin.py` — Persistent TTL, namespaces, static GET/HEAD, and explicit Range handler.
