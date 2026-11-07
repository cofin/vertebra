---
type: Guide
title: "Object Storage, Streaming & Presigning"
description: "The vertebra-storage Rust crate (vertebra._storage) and vertebra.storage Python facade for multicloud streaming I/O, conditional puts, presigned URLs, unified GCP token caches, and range reads."
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
  - by: agent:code-reviewer
    at: "2026-11-07T19:05:09Z"
    commit: dc2c2053
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
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
    title: "OCI REST v2 ObjectStore, uniform multipart chunking and PAR support (src/rs/vertebra-storage/src/oci.rs)"
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
  - id: src-storage-streaming-py
    resource: src/py/vertebra/storage/_streaming.py
    title: "StreamingStorageResponse FastPath descriptor (src/py/vertebra/storage/_streaming.py)"
  - id: src-extensions-alchemy-storage-py
    resource: src/py/vertebra/extensions/alchemy/storage.py
    title: "Advanced Alchemy StorageBackend adapter (src/py/vertebra/extensions/alchemy/storage.py)"
  - id: src-extensions-sqlspec-storage-py
    resource: src/py/vertebra/extensions/sqlspec/storage.py
    title: "SQLSpec ObjectStoreProtocol adapter (src/py/vertebra/extensions/sqlspec/storage.py)"
  - {id: src-extensions-django-storage-py, resource: "src/py/vertebra/extensions/django/storage.py::DjangoStorage", title: "Deconstructible Django Storage and File contract"}
  - {id: src-tests-django-plugin-py, resource: "src/py/tests/unit/extensions/test_django_plugin.py", title: "Django consumed-source collision recovery tests"}
  - {id: src-tests-data-storage-integrations-py, resource: "src/py/tests/unit/extensions/test_data_storage_integrations.py", title: "SQLSpec configured signing and adapter acceptance tests"}
  - {id: src-tests-integration-storage-conftest-py, resource: "src/py/tests/integration/storage/conftest.py", title: "Floci emulator integration fixtures (src/py/tests/integration/storage/conftest.py)"}
  - {id: src-db-staging-py, resource: "src/py/vertebra/db/_staging.py::ObjectStoreStaging", title: "Database staging qualification and storage resolution"}
  - {id: src-tests-db-staging-py, resource: "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py", title: "Staging rejection and native Memory/local compatibility tests"}
---

# Object Storage, Streaming & Presigning

<!-- truth: start -->
- **Rust Extension, Streaming & GIL-Free Async Execution (`vertebra-storage`, `vertebra.storage`)**: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-src-lib-rs] (typed in [`_storage.pyi`](../../../../../src/py/vertebra/_storage.pyi)[^src-storage-pyi]) builds `vertebra._storage` (`#[pymodule(gil_used = false)]`) over `object_store 0.13`. `SyncObjectStore`, `SyncStreamReader`, and `SyncMultipartUpload` release the GIL via `py.detach()`, while `AsyncObjectStore`, `AsyncStreamReader`, `AsyncMultipartUpload`, and `py_sign_gcs_v4_iam_url_async` return a `RustTaskAwaitable` backed by a per-loop `AsyncCompletionQueue` and [`vertebra_core::notify::Notifier`](../../../../../src/rs/vertebra-core/src/notify.rs)[^src-vertebra-core-src-notify-rs] (`eventfd` on Linux, pipe elsewhere) registered via `loop.add_reader`, completing on `STORAGE_RUNTIME` with zero Tokio `Python::attach` calls.
- **Unified GCP Token Authority, Multi-Cloud Signing & Fail-Closed Facade**: [`vertebra.storage`](../../../../../src/py/vertebra/storage/_store.py)[^src-storage-store-py] re-exports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` directly from [`vertebra._vertebra`](../../../../../src/rs/vertebra-py/src/gcp_auth.rs)[^src-vertebra-py-src-gcp-auth-rs] over [`vertebra_core::gcp_auth::GcpTokenCache`](../../../../../src/rs/vertebra-core/src/gcp_auth.rs)[^src-vertebra-core-src-gcp-auth-rs] (with `.to_async()` / `.to_sync()` sharing the underlying `Arc<GcpTokenCache>` and `allow_ambient_adc`), while [`vertebra-storage/src/gcp_auth.rs`](../../../../../src/rs/vertebra-storage/src/gcp_auth.rs)[^src-vertebra-storage-src-gcp-auth-rs] exposes only `RefreshingGcpCredentialProvider` and `GCS_OAUTH_SCOPES`. `SyncObjectStore` and `AsyncObjectStore` fail closed with `MissingDependencyError` (`vertebra-core[storage]`) when `vertebra._storage` is absent and `ImproperConfigurationError` on missing or invalid target URIs/configs, with zero pure-Python `_memory` dict fallbacks or `urllib` OAuth fetchers.
<!-- truth: end -->

Object storage comprises two layers: [`vertebra-storage`](../../../../../src/rs/vertebra-storage/src/lib.rs)[^src-vertebra-storage-cargo-toml] (`vertebra._storage`, `#[pymodule(gil_used = false)]`) over `object_store 0.13`, and [`vertebra.storage`](../../../../../src/py/vertebra/storage/__init__.py)[^src-storage-init-py], which resolves `vertebra._storage` lazily at store construction time via `import_optional("vertebra._storage")` and fails closed when the optional wheel is uninstalled.

## 1. Rust Engine (`store.rs`, `oci.rs`, `lib.rs`)

| Symbol | Source | Responsibility |
|---|---|---|
| [`BackendConfig` / `ObjectStoreEngine`](../../../../../src/rs/vertebra-storage/src/store.rs)[^src-vertebra-storage-src-store-rs] | `store.rs` | Wraps `Arc<dyn object_store::ObjectStore>` across `memory`, `file`, `s3`/`b2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, and `http`/`https`. Exposes `put_opts` (`PutConditionalOptions`: `if_not_exists`, `if_match`, `if_none_match`, `content_type`, `attributes`), `get_stream`, `create_multipart`, `list_page`, `list_with_delimiter`, `rename`, `get_range`, `head`, `exists`, `delete`, `copy`, and `list`. |
| `PySyncStreamReader` / `PyAsyncStreamReader` (`SyncStreamReader` / `AsyncStreamReader`) | `lib.rs` | Pulls streaming `Bytes` chunks from `ObjectStoreEngine::get_stream` and slices them to optional `chunk_size` bounds as zero-copy `StorageBuffer` views without whole-object buffering. |
| `MultipartUploadSession` (`PySyncMultipartUpload` / `PyAsyncMultipartUpload`) | `store.rs`, `lib.rs`, [`oci.rs`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^src-vertebra-storage-src-oci-rs] | Stateful `Box<dyn MultipartUpload>` session (`write_part`, `complete`, `abort`). OCI buffers writes incrementally through `oci.rs::UniformPartAccumulator` and `OCI_UNIFORM_PART_SIZE` (`8 MiB`) without concatenating full payloads into `Vec<u8>`. |
| `RefreshingGcpCredentialProvider` | `gcp_auth.rs` | Bridges `Arc<vertebra_core::gcp_auth::GcpTokenCache>` (`GCS_OAUTH_SCOPES`) into `GoogleCloudStorageBuilder::with_credentials(...)` when no static `service_account_path` or `service_account_key` is configured. |
| [`OciObjectStore`](../../../../../src/rs/vertebra-storage/src/oci.rs)[^src-vertebra-storage-src-oci-rs] | `oci.rs` | Native `ObjectStore` over OCI Object Storage REST v2 (`/n/{ns}/b/{bucket}/o/{name}`, pre-authenticated `par_url`, RSA-SHA256 Cavage request signing, and `OciMultipartUpload`). |
| `PySyncObjectStore` / `PyAsyncObjectStore` / `RustTaskAwaitable` | `lib.rs` | Frozen PyO3 classes (`SyncObjectStore` / `AsyncObjectStore`) whose `.to_async()` and `.to_sync()` methods clone the shared `ObjectStoreEngine` (`Arc`), keeping `InMemory` namespaces and HTTP pools unified across sync and async callers. |

Module signing functions exported by `_storage`: `py_sign_hmac_url`, `py_verify_hmac_url`, `py_sign_s3_v4_url`, `py_verify_s3_v4_url`, `py_sign_aws_sigv4_headers`, `py_sign_gcs_v4_iam_url`, `py_sign_gcs_v4_iam_url_async`, `build_gcs_v4_canonical_request`, `assemble_gcs_iam_v4_url`, `py_sign_oci_cavage_headers`, `sign_oci_par_url`, `py_sign_azure_sas_url`.

Native Rust and Python store constructors reject `r2://`; the native R2 module, `py_chunk_r2_uniform_parts` and its stub export are absent. Generic S3 endpoints remain supported, and uniform multipart accumulation belongs to OCI. [^src-vertebra-storage-src-store-rs] [^src-vertebra-storage-src-lib-rs] [^src-storage-pyi] [^src-vertebra-storage-src-oci-rs]

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

Provider configs are `msgspec.Struct(frozen=True, gc=False, kw_only=True)` implementing `to_url_and_options()`: `LocalConfig`, `S3Config`, `GCSConfig`, `AzureConfig`, `OCIConfig`, `HTTPConfig`, `MemoryConfig` (`StorageConfig` union), paired with `SigningConfig`. `R2Config` and `from_r2` are absent from the facade and package exports. Both store classes reject `r2://` with `ImproperConfigurationError` (a `ValueError`) before native construction. Advanced Alchemy, SQLSpec and Starlette classify the remaining configs. SQLSpec also rejects an R2 logical URI paired with a supported explicit native store; its exact diagnostic is `Unsupported storage URI scheme 'r2'`, without URI credentials or query text. Supported rooted S3/GCS logical overrides retain exact metadata, raw keys and sync/async operations. [^src-storage-store-py] [^src-storage-init-py] [^src-extensions-sqlspec-storage-py] [^src-tests-data-storage-integrations-py]

`S3Config(endpoint=...)` and `from_s3(endpoint=...)` accept explicit generic S3 custom endpoints and retain native SigV4 signing. Endpoint hostnames have no provider blacklist, R2 alias or compatibility normalization. [^src-storage-store-py] [^src-tests-data-storage-integrations-py]

- `SyncObjectStore(target, *, options, signing, token_cache)` and `AsyncObjectStore(target, *, options, signing, token_cache)` validate schemes upfront and delegate every CRUD, streaming (`get_stream`, `put_stream`), conditional `put`, `list_page`, `list_with_delimiter`, `rename`, and `get_view` (`memoryview` over `StorageBuffer`) call to `vertebra._storage`.
- `GCSConfig.sync_token_cache` / `async_token_cache` construct `vertebra._vertebra.{SyncGcpTokenCache, AsyncGcpTokenCache}`. Calling `store.to_async()` or `store.to_sync()` converts any attached token cache via `.to_async()` / `.to_sync()`, preserving a single shared Rust `Arc<GcpTokenCache>` generation counter.
- `store.sign(key, *, expires_in=3600, method="GET")`, `presign_get`, and `presign_put` support `gs` (local RSA and keyless IAM `signBlob` with automatic `401` token-generation invalidation and retry), `s3`, `azure`, `oci` (`par_token` or `par_url_prefix`), and `hmac`.

### Database Staging Qualification

`ObjectStoreStaging` rejects `r2://` destinations and resolves the declared URI from a string store, backend `store_uri`, native `url` or config `to_url_and_options()` before accepting a relative destination. A removed declaration raises the exact scheme-only diagnostic `Unsupported storage URI scheme 'r2'` before path resolution, producer consumption, object effects or real sync/async BigQuery query connections. Implicit unsupported schemes fail explicitly. Configured remote targets must match protocol and authority/bucket; comparison preserves `gcs`→`gs` and `azure`→`az`. Matching SQLSpec URIs remain fully qualified for root containment and raw-key validation. Reason: stripping qualification must never redirect a foreign target into the configured store or local disk. [^src-db-staging-py] [^src-tests-data-storage-integrations-py]

Explicit native Memory stores retain logical namespace keys, and native local stores retain their existing file-authority path mapping. SQLSpec's declared Memory authority still requires a match. Relative paths with an explicit store, matching local/file targets, supported remote targets and literal percent keys preserve their existing behavior. [^src-db-staging-py] [^src-tests-db-staging-py] [^src-tests-data-storage-integrations-py]

### Raw Object Keys and Conditional Errors

Object keys are nonempty relative raw text. Native `validate_object_path` and facade `_normalize_key` reject outer whitespace, boundary or repeated `/`, backslashes, literal `.` segments, and the strict parent-segment forms `..`, `%2e%2e`, `.%2e` and `%2e.` (case insensitive). Unicode and literal `%`, `%2F` and `%252F` retain distinct identities. Native SDK paths use `Path::parse`; returned metadata paths remain raw and are passed directly to read, copy, rename and delete operations without URL decoding. Both copy/rename operands are validated before mutation. [^src-vertebra-storage-src-store-rs] [^src-storage-store-py]

Listing prefixes use a separate boundary rule: native `None`, empty text and `/` mean the root; a relative `logs/` prefix is valid. The facade exposes empty text and `/` as root prefixes. SQLSpec first verifies matching URI scheme/authority or containment in the configured local root, derives the relative key, then applies the raw-key grammar. Matching `file://` paths and absolute `Path` values inside that root remain accepted. Alchemy validates relative keys before adding its configured namespace. [^src-storage-store-py] [^src-extensions-sqlspec-storage-py] [^src-extensions-alchemy-storage-py]

`vertebra.storage.PreconditionError` is the exact native `vertebra._storage.PreconditionError` class and subclasses `PermissionError`. Both native precondition error forms map to it; actual IO/SDK permission failures remain built-in `PermissionError`, and create-only conflicts remain `FileExistsError`. The public exception resolves lazily: importing `vertebra.storage` succeeds without the storage wheel, while requesting this exception then raises `MissingDependencyError`. [^src-vertebra-storage-src-lib-rs] [^src-storage-init-py] [^src-storage-pyi]

The error type identifies a reported condition conflict; provider-specific atomic publication is a separate contract. Rust source at `d50cf6a2` uses native Memory Create/Update at multipart completion and rejects unsupported negative multipart predicates or other providers' conditional multipart requests before upload. Its 28 Rust engine cases pass; the installed Python extension still requires a rebuild and acceptance for that checkpoint. Local/cloud publication repairs remain open. Error exports and adapter collision naming alone do not establish cross-worker multipart atomicity. [^src-vertebra-storage-src-store-rs] [^src-vertebra-storage-tests-storage-engine-tests-rs]

## 4. Range Reads & Ecosystem Adapters

| Symbol | Source | Responsibility |
|---|---|---|
| [`SyncReadableFile` / `AsyncReadableFile`](../../../../../src/py/vertebra/storage/_store.py) | `_store.py` | Seekable read-only streams (`io.RawIOBase` on sync) streaming from offset `0` via `get_stream` in `__iter__`/`__aiter__` and serving random-access `read` / cursor-preserving `read_at` via `get_range` for PyArrow and Polars Parquet footers. |
| [`StreamingStorageResponse`](../../../../../src/py/vertebra/storage/_streaming.py)[^src-storage-streaming-py] | `_streaming.py` | Frozen `msgspec.Struct` descriptor for streaming objects, HTTP `206 Partial Content` byte ranges, and `307 Temporary Redirect` presigned URL handoffs through `FastPath`. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] | `extensions/alchemy/storage.py` | Explicit-target `ObjectStoreBackend(StorageBackend)` shares a native engine across sync and async handles. `register_storage_backend` uses `StorageRegistry.register_backend` / `get_backend` without mutating `storages.get`; `FileObject` consumers use `save_object`, `get_content`, `delete_object`, and `sign` with async companions. |
| [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] | `extensions/sqlspec/storage.py` | Explicit-target `ObjectStoreProtocol` adapter preserves canonical `*_sync` / `*_async` operations and unsupported-signing `NotImplementedError`. Paired `get_stream`, `put_stream`, `list_page`, and `open` extensions use the same suffixes and native shared-engine handles. |
| [`DjangoStorage`](../../../../../src/py/vertebra/extensions/django/storage.py)[^src-extensions-django-storage-py] | `extensions/django/storage.py` | Deconstructible Django `Storage` with explicit configuration, Django `File` handles, namespace-aware native operations, collision-safe names, URLs, local paths, and UTC timestamps. |

### Adapter File and Registry Contracts

SQLSpec constructs one primary native store and creates its shared-engine companion lazily. Conditional write options reach the native store; moves use native rename. Relative and URI-qualified keys preserve raw identities and reject ambiguous or mismatched qualification before effects. `build_duckdb_gcs_secret_sql` remains in this module; Arrow/Parquet streaming completion belongs to `ch04` [^src-extensions-sqlspec-storage-py].

SQLSpec signing qualifies each key, then checks the corresponding store's public `supports_signing` capability. Unavailable signing raises `NotImplementedError` with backend context. Configured `presign_get` / `presign_put` calls preserve validation and provider errors without conversion or message classification. Native HMAC expiry errors remain exact `ValueError`; local IAM HTTP 503 failures remain exact `RuntimeError` with the native status message. Sync/async scalar/list shapes and GET/PUT selection are verified through the real signer. [^src-extensions-sqlspec-storage-py] [^src-tests-data-storage-integrations-py]

Advanced Alchemy accepts a URI/store/config first or a backend key with explicit `store=` through `register_storage_backend`. Bounded sync/async reader chunks and iterables feed native multipart uploads; producer failures abort unfinished uploads. `get_content` / `get_content_async` return full bytes, and saved `FileObject` values receive size, etag, and a UTC save timestamp [^src-extensions-alchemy-storage-py].

Django `_open` / inherited `open` return `File` over `SyncReadableFile`; `aopen` awaits the complete native payload before wrapping `BytesIO` because Django `File` reads synchronously. Both support read-only binary and text modes. `save` / `asave` stream Django chunks, apply filename limits, and retry native create-only collisions unless `file_overwrite=True`. `listdir` returns immediate names; `url` / `aurl` use signing or `base_url`; `path` supports local stores only. Creation/access timestamps reuse the object's UTC modification timestamp because object metadata does not provide separate values [^src-extensions-django-storage-py].

Django `_save` performs one native upload attempt; public `save` retries through Django's `Storage.save` with the original per-call `max_length`. `asave` retains that limit during alternative-name selection. Both rewind seekable content to zero after `FileExistsError`; a failed rewind preserves the original collision, and an impossible filename limit raises `SuspiciousFileOperation`. Overwrite mode propagates reported collisions without retry. Same-instance sync/async calls keep independent limits. The tests consume input, publish a native create-only racer, then raise a second native create-only conflict inside the producer. They verify recovery and full retry payloads; atomic multipart-completion acceptance remains open in the native repair chapters. [^src-extensions-django-storage-py] [^src-tests-django-plugin-py]

## 5. Emulator Fixtures & Test Suites

[`src/py/tests/integration/storage/conftest.py`](../../../../../src/py/tests/integration/storage/conftest.py)[^src-tests-integration-storage-conftest-py] defines Floci emulator fixtures (`floci_s3_config`, `floci_azure_config`, `floci_gcs_config`). Rust engine and PyO3 streaming/signing tests run in [`storage_engine_tests.rs`](../../../../../src/rs/vertebra-storage/tests/storage_engine_tests.rs)[^src-vertebra-storage-tests-storage-engine-tests-rs], and Python unit suites live in `src/py/tests/unit/storage/`.

## 6. Operational Invariants & Gotchas

- **Fail-closed construction**: Omitting or passing an empty/schemeless target raises `ImproperConfigurationError`; missing `vertebra._storage` raises `MissingDependencyError` (`vertebra-core[storage]`). Ephemeral test stores must pass `MemoryConfig()` or `"memory://<namespace>"` explicitly and pair sync/async handles via `.to_async()` / `.to_sync()` so they share the same Rust `InMemory` instance.
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
[^src-vertebra-storage-src-oci-rs]: `src/rs/vertebra-storage/src/oci.rs` — OCI REST v2 ObjectStore, uniform multipart chunking and PAR support (src/rs/vertebra-storage/src/oci.rs).
[^src-vertebra-storage-src-signing-rs]: `src/rs/vertebra-storage/src/signing.rs` — Multicloud URL and header signing (src/rs/vertebra-storage/src/signing.rs).
[^src-vertebra-storage-tests-storage-engine-tests-rs]: `src/rs/vertebra-storage/tests/storage_engine_tests.rs` — Rust integration tests for vertebra-storage (src/rs/vertebra-storage/tests/storage_engine_tests.rs).
[^src-storage-init-py]: `src/py/vertebra/storage/__init__.py` — Public vertebra.storage exports (src/py/vertebra/storage/__init__.py).
[^src-storage-store-py]: `src/py/vertebra/storage/_store.py` — Fail-closed SyncObjectStore and AsyncObjectStore facade (src/py/vertebra/storage/_store.py).
[^src-storage-streaming-py]: `src/py/vertebra/storage/_streaming.py` — StreamingStorageResponse FastPath descriptor (src/py/vertebra/storage/_streaming.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Advanced Alchemy StorageBackend adapter (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — SQLSpec ObjectStoreProtocol adapter (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-tests-integration-storage-conftest-py]: `src/py/tests/integration/storage/conftest.py` — Floci emulator integration fixtures (src/py/tests/integration/storage/conftest.py).

[^src-extensions-django-storage-py]: `src/py/vertebra/extensions/django/storage.py::DjangoStorage` — Deconstructible Django Storage and File contract.
[^src-tests-django-plugin-py]: `src/py/tests/unit/extensions/test_django_plugin.py` — Consumed-source producer collision recovery and independent limits.
[^src-tests-data-storage-integrations-py]: `src/py/tests/unit/extensions/test_data_storage_integrations.py` — Native signing validation, provider failures and result shapes.
[^src-db-staging-py]: `src/py/vertebra/db/_staging.py::ObjectStoreStaging` — Scheme rejection, configured qualification and path resolution.
[^src-tests-db-staging-py]: `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` — Staging rejection and native Memory/local compatibility.
