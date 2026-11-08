---
type: Guide
title: Object Storage, Streaming & Presigning
description: Native multicloud streaming, signing, framework storage contracts, and
  bounded Arrow/Parquet codecs with strict named SHM allocation.
status: stable
tags:
- domains
- storage
- object-store
- signing
verified:
- by: agent:docs-okf-specialist
  at: '2026-11-08T18:20:18.754346Z'
  commit: bf5a4cf3
sources:
- id: src-vertebra-core-src-gcp-auth-rs
  resource: src/rs/vertebra-core/src/gcp_auth.rs
  title: Shared Rust GCP OAuth2 token cache
- id: src-vertebra-py-src-gcp-auth-rs
  resource: src/rs/vertebra-py/src/gcp_auth.rs
  title: PyO3 GCP token cache bindings in vertebra._vertebra
- id: src-vertebra-storage-src-lib-rs
  resource: src/rs/vertebra-storage/src/lib.rs
  title: PyO3 module bindings for vertebra._storage
- id: src-vertebra-storage-src-store-rs
  resource: src/rs/vertebra-storage/src/store.rs
  title: ObjectStoreEngine and MultipartUploadSession
- id: src-vertebra-storage-src-gcp-auth-rs
  resource: src/rs/vertebra-storage/src/gcp_auth.rs
  title: RefreshingGcpCredentialProvider over vertebra_core::gcp_auth
- id: src-vertebra-storage-src-oci-rs
  resource: src/rs/vertebra-storage/src/oci.rs
  title: OCI REST v2 ObjectStore and PAR support
- id: src-vertebra-storage-src-r2-rs
  resource: src/rs/vertebra-storage/src/r2.rs
  title: Cloudflare R2 uniform multipart chunking
- id: src-vertebra-storage-src-signing-rs
  resource: src/rs/vertebra-storage/src/signing.rs
  title: Multicloud URL and header signing
- id: src-vertebra-storage-tests-storage-engine-tests-rs
  resource: src/rs/vertebra-storage/tests/storage_engine_tests.rs
  title: Rust integration tests for vertebra-storage
- id: src-storage-store-py
  resource: src/py/vertebra/storage/_store.py
  title: Fail-closed SyncObjectStore and AsyncObjectStore facade
- id: src-storage-streaming-py
  resource: src/py/vertebra/storage/_streaming.py
  title: StreamingStorageResponse FastPath descriptor
- id: src-extensions-litestar-store-py
  resource: src/py/vertebra/extensions/litestar/store.py
  title: Persistent Litestar Store and confined async filesystem
- id: src-tests-storage-store-py
  resource: src/py/tests/unit/storage/test_storage_store.py
  title: Range parsing, async ownership, and storage response contracts
- id: src-tests-zero-copy-buffer-py
  resource: src/py/tests/unit/server/test_zero_copy_buffer.py
  title: Native async storage handler buffer submission
- id: src-tests-litestar-plugin-py
  resource: src/py/tests/unit/extensions/test_litestar_plugin.py
  title: Persistent TTL, namespaces, static GET/HEAD, and explicit Range handler
- id: src-extensions-alchemy-storage-py
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Advanced Alchemy StorageBackend adapter
- id: src-extensions-sqlspec-storage-py
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: SQLSpec ObjectStoreProtocol adapter
- id: src-extensions-django-storage-py
  resource: src/py/vertebra/extensions/django/storage.py::DjangoStorage
  title: Deconstructible Django Storage with native readers and paired stores
- id: src-tests-data-storage-integrations-py
  resource: src/py/tests/unit/extensions/test_data_storage_integrations.py
  title: SQLSpec protocol, Advanced Alchemy FileObject, registry, and streaming tests
- id: src-tests-django-plugin-py
  resource: src/py/tests/unit/extensions/test_django_plugin.py
  title: Django File, migration reconstruction, naming, timestamp, and async tests
- id: src-tests-integration-storage-conftest-py
  resource: src/py/tests/integration/storage/conftest.py
  title: Floci emulator integration fixtures
- id: src-storage-arrow-py
  resource: src/py/vertebra/storage/_arrow.py
  title: Demand-driven Arrow and Parquet codec bridge
- id: src-db-arrow-py
  resource: src/py/vertebra/db/arrow.py::ArrowCursor
  title: Real IPC cursor with threshold-selected native SHM
- id: src-db-arrow-shm-rs
  resource: src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region
  title: Strict Unix named shared-memory allocation
- id: src-core-notify-rs
  resource: src/rs/vertebra-core/src/notify.rs
  title: Native Notifier event-loop completion wakeups
---

# Object Storage, Streaming & Presigning

<!-- truth: start -->
- **Rust Extension, Streaming & GIL-Free Async Execution (`vertebra-storage`, `vertebra.storage`)**: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs] (typed in [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)) builds `vertebra._storage` (`#[pymodule(gil_used = false)]`) over `object_store 0.13`. `SyncObjectStore`, `SyncStreamReader`, and `SyncMultipartUpload` release the GIL via `py.detach()`, while `AsyncObjectStore`, `AsyncStreamReader`, `AsyncMultipartUpload`, and `py_sign_gcs_v4_iam_url_async` return a `RustTaskAwaitable` backed by a per-loop `AsyncCompletionQueue` and [`vertebra_core::notify::Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs) (`eventfd` on Linux, pipe elsewhere) registered via `loop.add_reader`, completing on `STORAGE_RUNTIME` without Tokio `Python::attach` on the notifier-backed path. When the notifier or `add_reader` is unavailable, `RustCompletionRing::signal_fallback` attaches to Python and calls `loop.call_soon_threadsafe`.[^src-core-notify-rs]
- **Unified GCP Token Authority, Multi-Cloud Signing & Fail-Closed Facade**: [`vertebra.storage`](../../../../../src/py/vertebra/storage/_store.py)[^src-storage-store-py] re-exports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` directly from [`vertebra._vertebra`](../../../../../src/rs/vertebra-py/src/gcp_auth.rs)[^src-vertebra-py-src-gcp-auth-rs] over [`vertebra_core::gcp_auth::GcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs)[^src-vertebra-core-src-gcp-auth-rs] (with `.to_async()` / `.to_sync()` sharing the underlying `Arc<GcpTokenCache>` and `allow_ambient_adc`), while [`vertebra-storage/src/gcp_auth.rs`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs)[^src-vertebra-storage-src-gcp-auth-rs] exposes only `RefreshingGcpCredentialProvider` and `GCS_OAUTH_SCOPES`. `SyncObjectStore` and `AsyncObjectStore` fail closed with `MissingDependencyError` (`vertebra-core[storage]`) when `vertebra._storage` is absent and `ImproperConfigurationError` for explicit invalid targets; an omitted constructor argument raises `TypeError`. The facade has no pure-Python `_memory` fallback or `urllib` OAuth fetcher.
- **Arrow data plane**: Storage batch codecs use native streams/ranges and multipart uploads; async PyArrow operations run in per-demand SyncPool steps while I/O stays on the caller loop. Required Unix named SHM allocation errors propagate, and `ArrowCursor.from_rows` emits real IPC only.[^src-storage-arrow-py][^src-db-arrow-py][^src-db-arrow-shm-rs]
- Native S3 and B2-normalized uploads share bounded 8 MiB part accumulation with R2/OCI; every non-final S3 part meets the 5 MiB minimum, while the final part may be shorter. [^src-vertebra-storage-src-store-rs]
<!-- truth: end -->

## 1. Rust Engine (`store.rs`, `oci.rs`, `r2.rs`, `lib.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BackendConfig` / `ObjectStoreEngine`](../../../../../src/rs/vertebra-storage/src/store.rs)[^src-vertebra-storage-src-store-rs] | `store.rs` | Wraps `Arc<dyn object_store::ObjectStore>` across `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, and `http`/`https`. Exposes `put_opts` (`PutConditionalOptions`: `if_not_exists`, `if_match`, `if_none_match`, `content_type`, `attributes`), `get_stream`, `create_multipart`, `list_page`, `list_with_delimiter`, `rename`, `get_range`, `head`, `exists`, `delete`, `copy`, and `list`. |
| `PySyncStreamReader` / `PyAsyncStreamReader` (`SyncStreamReader` / `AsyncStreamReader`) | `lib.rs` | Pulls streaming `Bytes` chunks from `ObjectStoreEngine::get_stream` and slices them to optional `chunk_size` bounds as zero-copy `StorageBuffer` views without whole-object buffering. |
| `MultipartUploadSession` (`PySyncMultipartUpload` / `PyAsyncMultipartUpload`) | `store.rs`, `lib.rs` | Stateful `Box<dyn MultipartUpload>` session (`write_part`, `complete`, `abort`). For normalized `s3`/`b2`, `r2`, and `oci`, buffers writes into `8 MiB` parts ([`R2_UNIFORM_PART_SIZE`](../../../../../src/rs/vertebra-storage/src/r2.rs)[^src-vertebra-storage-src-r2-rs]) incrementally without concatenating full payloads into `Vec<u8>`. |
| `RefreshingGcpCredentialProvider` | `gcp_auth.rs` | Bridges `Arc<vertebra_core::gcp_auth::GcpTokenCache>` (`GCS_OAUTH_SCOPES`) into `GoogleCloudStorageBuilder::with_credentials(...)` when no static `service_account_path` or `service_account_key` is configured. |
| [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^src-vertebra-storage-src-oci-rs] | `oci.rs` | Native `ObjectStore` over OCI Object Storage REST v2 (`/n/{ns}/b/{bucket}/o/{name}`, pre-authenticated `par_url`, RSA-SHA256 Cavage request signing, and `OciMultipartUpload`). |
| `PySyncObjectStore` / `PyAsyncObjectStore` / `RustTaskAwaitable` | `lib.rs` | Frozen PyO3 classes (`SyncObjectStore` / `AsyncObjectStore`) whose `.to_async()` and `.to_sync()` methods clone the shared `ObjectStoreEngine` (`Arc`), keeping `InMemory` namespaces and HTTP pools unified across sync and async callers. |

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

`SyncReadableFile` and `AsyncReadableFile` are seekable read-only streams. Iteration from offset zero uses `get_stream`; random-access `read` and cursor-preserving `read_at` use native `get_range` for codec footers.[^src-storage-store-py]

### Arrow IPC, Parquet, and Shared Memory

Both stores expose `iter_arrow_batches(key)`, `iter_parquet_batches(key, *, batch_size=65536, columns=None)`, `write_arrow(key, data, *, batch_size=65536)`, and `write_parquet(key, batches, *, schema=None, compression="zstd", row_group_size=65536)`. Readers yield real RecordBatch objects; writes return native `PutResult`. Table, RecordBatch, ArrowCursor, and synchronous batch iterables retain schema/dictionary state; schema-bearing empty inputs produce valid files, while a schema-less empty iterable raises `ValueError`. Encoders drain each operation into native `put_stream` before advancing input, including final IPC EOS or Parquet footer.[^src-storage-arrow-py][^src-storage-store-py]

The private codec bridge uses one SyncPool operation per initialization, batch, or close. Native async stream/range I/O runs on the caller loop; repeated cancellation waits for actual loop-side I/O and codec-worker exit before closing sources or sinks. Explicit `close`/`aclose` stops reader demand. Writer failure/cancellation closes its owned producer and aborts multipart upload. Parquet batch readers default to `pre_buffer=False`; private codec options preserve an explicit `True` as an opt-out from bounded prefetch. Full-Table helpers intentionally materialize the decoded Table, preserve empty schemas, and support IPC-file fallback through seekable ranges. SQLSpec delegates its Arrow methods to these helpers, retaining raw IPC buffer inputs, caller options, and PyArrow compression defaults.[^src-storage-arrow-py]

`ArrowCursor.from_rows(rows, columns=None, *, threshold_rows=256)` requires PyArrow and preserves tuple, mapping, inferred-column, and empty-row semantics. Threshold-selected SHM requires the native `write_ipc_to_shm`; missing native support and allocation failures propagate. Unix named allocation errors include the normalized name and OS error. The 16-byte `VTBARROW` frame header and retained-view ownership remain unchanged.[^src-db-arrow-py][^src-db-arrow-shm-rs]

### HTTP Range and Native Response Consumers

`ByteRange(start, end_exclusive, total_size)` and `parse_byte_range` resolve one closed, open-ended, or suffix byte range, clamp its end, and ignore unknown units. Malformed, multiple, zero-suffix, or unsatisfiable byte ranges raise `RangeNotSatisfiableError`. `from_sync_store` / awaited `from_async_store` perform one native HEAD and produce `200`, `206`, or empty `416` (`Content-Range: bytes */<size>`), with authoritative length, ETag, content type, and `Accept-Ranges: bytes`.[^src-storage-streaming-py][^src-tests-storage-store-py]

For async stores, explicitly await `to_async_fastpath_tuple()` in the handler: it returns `(status, list[tuple[bytes, bytes]], memoryview | bytes)` through native async `get_view` or `get_range`, with no synchronous store calls. Full reads retain the `StorageBuffer` owner through a memoryview; ranges return bytes. Pass the resolved body and byte header pairs to native `Response.from_buffer`; decode header pairs with Latin-1 at the `submit_raw_response` helper's string-header boundary. Async stores reject synchronous `to_fastpath_tuple()`. `aiter_chunks()` uses native full-object streams or bounded range reads.[^src-storage-streaming-py][^src-tests-zero-copy-buffer-py]

### Litestar Store and Static Files

`ObjectStoreLitestarStore` accepts an explicit async store, config, or URI. Each object contains a versioned `VTBSTORE\x01` MessagePack envelope holding the wall-clock millisecond deadline and bytes, so TTL survives store reconstruction even when backend attributes disappear. Expired entries are deleted on read; renewal applies only to expiring entries. Logical keys are UTF-8 hex encoded; `with_namespace` composes validated child prefixes, and paginated `delete_all` removes that namespace and descendants while preserving siblings.[^src-extensions-litestar-store-py][^src-tests-litestar-plugin-py]

`ObjectStoreFileSystemAdapter(store, base_path="public")` exposes a confined virtual `/`, async metadata, and native `AsyncReadableFile` handles with async reads and synchronous seek/tell. Configure `create_static_files_router(path="/assets", directories=["/"], file_system=adapter, resolve_symlinks=False)` for static GET/HEAD. Explicit Range handling uses a separate route: await `StreamingStorageResponse.from_async_store(..., range_header=request.headers.get("range"))`, then return Litestar `Stream` over byte-converted `aiter_chunks()`, forwarding status, headers, content range, and media type.[^src-extensions-litestar-store-py][^src-tests-litestar-plugin-py]

### SQLSpec Protocol and Native Extensions

`ObjectStoreBackend` implements SQLSpec's byte/text, CRUD, listing, metadata, and signing methods with their exact `*_sync` / `*_async` names. Native extensions expose `get_stream_sync` / `get_stream_async` as `StorageBuffer` iterators, `put_stream_*` as incremental uploads returning `PutResult`, `list_page_*` as relative-key pages with opaque native cursors, and `open_sync` / `open_async` as seekable-reader factories. SQLSpec `stream_read_sync` yields `bytes`; callers await `stream_read_async` before iterating its byte chunks. Conditional and metadata write options reach native `put` / `put_stream`, and moves use native `rename`.[^src-extensions-sqlspec-storage-py]

Both reader factories are synchronous. `open_sync(size=None)` resolves metadata immediately; supplying `size` avoids that lookup. `open_async` defers metadata until reading. Keys and configured namespaces reject parent traversal. SQLSpec `read_arrow_*` returns schema-preserving Tables, while `stream_arrow_async` is a normal function returning an async iterator over native async matches and batch reads. Closing or cancelling the outer stream closes its active inner iterator. `write_arrow_*` and `write_arrow_cursor_*` retain `None` returns, format options, raw IPC buffer inputs, and existing Parquet defaults. Explicit `pre_buffer=True` is forwarded as an opt-out from bounded batch prefetch; `build_duckdb_gcs_secret_sql` retains its contract.[^src-extensions-sqlspec-storage-py]

### Advanced Alchemy Registry and File Objects

`register_storage_backend(store, key="media")` and `register_storage_backend("media", store=store)` register the concrete `ObjectStoreBackend` through `StorageRegistry.register_backend`; consumers resolve it through `get_backend`. Construction validates the target before registry/default mutation, and imports preserve the upstream registry API. `FileObject.save` / `save_async` use native `put`, `put_file`, or incremental `put_stream`; file readers receive bounded `chunk_size` reads, while legacy no-argument async readers retain their one-read behavior. Saved objects receive native size/etag, and `get_content` / `get_content_async` return stored bytes. Producer exceptions and async cancellation propagate through multipart cleanup.[^src-extensions-alchemy-storage-py]

### Django Storage and File Handles

`DjangoStorage` subclasses Django's real `Storage` and uses its deconstructible constructor for migration reconstruction. Django's public `MigrationWriter` serializer registry handles only Vertebra's eight `StorageConfig` types, preserving every field through public configuration constructors and recursive field serialization. `_open` / inherited `open` return a `File` subclass over `SyncReadableFile`; reopening resets or recreates the reader through the same native store and qualified key, including `FieldFile` consumers. `aopen` awaits native metadata and supplies its size before constructing the synchronous Django handle. Subsequent reads and reopening follow Django's synchronous interface. Runtime return-annotation introspection resolves to Django's concrete `File`, while static typing uses `File[bytes]`.[^src-extensions-django-storage-py]

`save` follows Django validation and available-name allocation, and `asave` applies equivalent collision/max-length rules with native async existence checks. `ContentFile` chunks and async iterables upload incrementally; text chunks encode as UTF-8. Names reject traversal before `location` qualification. `listdir` returns sorted direct children; `path` resolves under an explicit local root and raises `NotImplementedError` on remote or memory stores. `url` / `aurl` sign qualified keys or quote them under `base_url`. `get_modified_time` returns an aware UTC datetime from `ObjectMeta.last_modified`; creation/access hooks raise `NotImplementedError` because native metadata supplies neither timestamp.[^src-extensions-django-storage-py]

## 5. Emulator Fixtures & Test Suites

[`src/py/tests/integration/storage/conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py)[^src-tests-integration-storage-conftest-py] defines Floci emulator fixtures (`floci_s3_config`, `floci_azure_config`, `floci_gcs_config`, `floci_r2_config`). Rust engine and PyO3 streaming/signing tests run in [`storage_engine_tests.rs`](../../../../../src/rs/vertebra-storage/tests/storage_engine_tests.rs)[^src-vertebra-storage-tests-storage-engine-tests-rs], and Python unit suites live in `src/py/tests/unit/storage/`. Adapter consumer coverage exercises SQLSpec protocols/pipelines and Advanced Alchemy registry/`FileObject` behavior in [`test_data_storage_integrations.py`](../../../../../src/py/tests/unit/extensions/test_data_storage_integrations.py)[^src-tests-data-storage-integrations-py], and Django migration reconstruction, seekable files, naming, async persistence, timestamps, and signing in [`test_django_plugin.py`](../../../../../src/py/tests/unit/extensions/test_django_plugin.py)[^src-tests-django-plugin-py].

## 6. Operational Invariants & Gotchas

- **Fail-closed construction**: The positional target is required (`TypeError` when omitted); explicit `None`, empty, or schemeless targets raise `ImproperConfigurationError`; missing `vertebra._storage` raises `MissingDependencyError` (`vertebra-core[storage]`). Ephemeral test stores must pass `MemoryConfig()` or `"memory://<namespace>"` explicitly and pair sync/async handles via `.to_async()` / `.to_sync()` so they share the same Rust `InMemory` instance.
- **Multipart stream cleanup**: `put_stream` aborts the active `MultipartUploadSession` on producer exceptions so uncommitted parts are cleaned up.
- **Offline verification scope**: `verify_presigned_url` supports `hmac` and `s3` only; `gcs`, `azure`, and `oci` raise `NotImplementedError`.

[^src-vertebra-core-src-gcp-auth-rs]: `src/rs/vertebra-core/src/gcp_auth.rs` — Shared Rust GCP OAuth2 token cache (src/rs/vertebra-core/src/gcp_auth.rs).
[^src-vertebra-py-src-gcp-auth-rs]: `src/rs/vertebra-py/src/gcp_auth.rs` — PyO3 GCP token cache bindings in vertebra._vertebra.
[^src-vertebra-storage-src-lib-rs]: `src/rs/vertebra-storage/src/lib.rs` — PyO3 module bindings for vertebra._storage (src/rs/vertebra-storage/src/lib.rs).
[^src-vertebra-storage-src-store-rs]: `src/rs/vertebra-storage/src/store.rs` — ObjectStoreEngine and MultipartUploadSession (src/rs/vertebra-storage/src/store.rs).
[^src-vertebra-storage-src-gcp-auth-rs]: `src/rs/vertebra-storage/src/gcp_auth.rs` — RefreshingGcpCredentialProvider over vertebra_core::gcp_auth (src/rs/vertebra-storage/src/gcp_auth.rs).
[^src-vertebra-storage-src-oci-rs]: `src/rs/vertebra-storage/src/oci.rs` — OCI REST v2 ObjectStore and PAR support (src/rs/vertebra-storage/src/oci.rs).
[^src-vertebra-storage-src-r2-rs]: `src/rs/vertebra-storage/src/r2.rs` — Cloudflare R2 uniform multipart chunking (src/rs/vertebra-storage/src/r2.rs).
[^src-vertebra-storage-src-signing-rs]: `src/rs/vertebra-storage/src/signing.rs` — Multicloud URL and header signing (src/rs/vertebra-storage/src/signing.rs).
[^src-vertebra-storage-tests-storage-engine-tests-rs]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests for vertebra-storage (src/rs/vertebra-storage/tests/storage_engine_tests.rs).
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
[^src-storage-arrow-py]: `src/py/vertebra/storage/_arrow.py` — Bounded native-I/O codecs and cancellation ownership.
[^src-db-arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Real IPC cursor and native SHM selection.
[^src-db-arrow-shm-rs]: `src/rs/vertebra-db/src/arrow_shm.rs::allocate_shm_region` — Unix named allocation error propagation.

[^src-core-notify-rs]: `src/rs/vertebra-core/src/notify.rs` — Native event-loop completion wakeups.
