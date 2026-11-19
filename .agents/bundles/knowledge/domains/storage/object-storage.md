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
verified:
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
  title: SyncObjectStore, AsyncObjectStore, configs, readers, and writers
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
- `vertebra._storage` (`#[pymodule(gil_used = false)]`, typed in `_storage.pyi`) exports `SyncObjectStore`, `AsyncObjectStore`, `SyncStreamReader`, `AsyncStreamReader`, `SyncMultipartUpload`, `AsyncMultipartUpload`, `ObjectMeta`, `StorageBuffer`, `RustTaskAwaitable`, and signing functions over `object_store 0.13` [^storage-lib] [^storage-pyi].
- Sync calls release the GIL with `py.detach()`; async calls return `RustTaskAwaitable` completed through a per-loop `AsyncCompletionQueue` and `Notifier` (`eventfd` on Linux, pipe elsewhere) registered with `loop.add_reader` [^storage-lib] [^core-notify].
- `SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenSnapshot` (`gcp_auth.rs`) and audience-bound `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot` (`gcp_identity.rs`) live in `vertebra._vertebra`; `vertebra.storage` re-exports all six types. Canonical ADC discovery evaluates `explicit options -> ambient ADC file (GOOGLE_APPLICATION_CREDENTIALS / gcloud ADC, including service_account, authorized_user, and external_account WIF) -> ambient env tokens (GOOGLE_OAUTH_ACCESS_TOKEN / CLOUDSDK_AUTH_ACCESS_TOKEN) -> GCE metadata server`, and GCS stores pass `token_cache._capsule()` to `vertebra._storage` (`PyCapsule` `vertebra.GcpTokenCacheCapsule`), while `gcp_identity.rs` supports `authorized_user` (`client_id` audience), service-account, GCE metadata, and `target_service_account` ID token minting [^core-gcp-auth] [^core-gcp-identity] [^py-gcp-auth] [^py-gcp-identity] [^storage-lib].
- One shared cache backs GCS data requests (`RefreshingGcpCredentialProvider`), server-side `compose`, and keyless IAM `signBlob`; `compose` and `signBlob` invalidate the observed token generation and retry once on `401` [^storage-gcp-auth] [^storage-store] [^storage-signing].
- GCS `allow_ambient_adc=false` builds the client with `GoogleCloudStorageBuilder::new()` instead of `from_env()`; `access_token` installs a static bearer provider, `skip_signature` installs an empty bearer and skips signing, and `quota_project_id` becomes `x-goog-user-project` on storage, compose, IAM, and token requests [^storage-store] [^storage-signing] [^core-gcp-auth].
- Credential validation fails closed (`ImproperConfigurationError`) on incomplete or blank Azure Entra triples (`tenant_id`, `client_id`, `client_secret`), malformed Azure `sas_token` strings, and partial or blank OCI Cavage credentials [^storage-store] [^py-store]. Explicit GCS credential options (`service_account_path`, `service_account_key`, `application_credentials_path`, `application_credentials`) also fail closed during auto token-cache construction [^py-store].
- HTTP clients are pooled: `object_store` clients keep 64 idle connections per host, the GCS compose and IAM `reqwest` clients keep 32, all with 10 s connect and 300 s total timeouts [^storage-store] [^storage-signing].
- GCS presigning forwards `GCSConfig.endpoint` as the URL origin, and S3 presigning honors `virtual_hosted_style` [^py-store] [^storage-signing].
- `extract_buffer_bytes` keeps `StorageBuffer` and `bytes` zero-copy (`PyBackedBytes`) and copies any other C-contiguous `u8` buffer synchronously before releasing the GIL, so callers may mutate `bytearray` sources afterwards [^storage-lib].
- Object keys collapse empty `/` and `.` segments in Python before passing `StorePath::parse`, so spaces, `%`, `~`, and non-ASCII characters are stored verbatim while `..` segments raise `ValueError` [^storage-store] [^py-store].
- `get_range` rejects `offset + length` overflow with `ValueError` and answers `length == 0` with a `head` check and empty bytes [^storage-store].
- `StorageError::Precondition` maps to `vertebra.typing.PreconditionError` (an `OSError`); `AlreadyExists` maps to `FileExistsError`, so `if_not_exists=True`, `copy(..., overwrite=False)`, and `rename(..., overwrite=False)` raise `FileExistsError` while `if_match` / `if_none_match` failures raise `PreconditionError`; S3/B2 use conditional multipart completion and R2 uses `cf-copy-destination-if-none-match: *`, while unsupported provider conditions fail with the backend error [^storage-lib] [^py-typing] [^storage-store].
- GCS `if_match` accepts a numeric generation directly or resolves a matching ETag to the object generation before a conditional `PutMode::Update` [^storage-store].
- `OciObjectStore` streams GET bodies (`bytes_stream`) instead of buffering whole objects [^storage-oci].
- `put_file` / `get_file` stream files over 8 MiB in parts; `get_file` writes a sibling `.{name}.{pid}.{seq}.part` file, flushes and calls `sync_all` on the `.part` file before renaming into place, and calls `sync_all` on the destination parent directory on Unix; `get_files` / `put_files` run with bounded `buffer_unordered(concurrency)` and keep results in input order [^storage-store].
- `supports_atomic_rename` is `True` on `file`/`local`, `memory`, and `oci` and `False` on `s3`, `r2`, `gcs`, `azure`, and `http`; `rename(..., atomic_only=True)` raises `ValueError` (`StorageError::InvalidInput`) when the backend lacks native atomic rename instead of falling back to `copy` + `delete` [^storage-store] [^storage-lib] [^py-store].
- Python adds `download_many`, `upload_many`, and `staged_download` (unique directory under writable `/dev/shm`, else the temp dir; caller-owned `destination` preserves pre-existing files), rejecting duplicate basenames or keys before any transfer [^py-store].
- `glob` matches full keys, so `*` and `?` also match `/`; `compose` uses the GCS compose API with 32-source folds whose temporaries are deleted on success, failure, or cancellation, and multipart concatenation elsewhere [^py-store] [^storage-store].
- `open_reader(..., read_ahead=4_194_304)` serves short reads from one cached range window; `open_writer(..., part_size=8_388_608)` returns `SyncWritableFile` (`io.RawIOBase`) or `AsyncWritableFile`, whose `aclose()` returns `PutResult | None` [^py-store].
- `vertebra.storage._arrow` streams `parquet`, `arrow-ipc` (`arrow`, `ipc`, `feather`), and `csv` (`text/csv`, `pyarrow.csv.open_csv` / `CSVWriter`, column projection, and `if_not_exists` forwarding); Parquet batch readers use `read_ahead=0` and `pre_buffer=False`, whole-table Parquet reads default to `pre_buffer=True`, and `write_parquet` exposes `use_dictionary`, `use_byte_stream_split`, and `options` [^py-arrow] [^py-store].
- `read_arrow_shm` wraps a shared-memory Arrow IPC or `VTBARROW` region in a zero-copy `ArrowCursor` that anchors the region views [^db-arrow].
- Framework adapters accept only an explicit store, `StorageConfig`, or URI; `vertebra.extensions.django` exports Django integration types only, so stores and configs import from `vertebra.storage` [^ext-alchemy-storage] [^ext-sqlspec-storage] [^ext-django-storage].
- Uploads (`put*`, `multipart`, `AsyncWritableFile.aclose()`) populate `PutResult` (`ObjectMeta` includes optional `sha256`, `crc32c`, `md5` computed over verified bytes; user metadata never populates receipt fields), `checksum=True` hashes payloads locally, and reads (`get`, `get_file`, `get_stream`, `open_reader`) enforce `expected_sha256` (`VerifyingStream` on streams), raising `vertebra.typing.IntegrityError` (`OSError`) on mismatch [^storage-lib] [^py-typing] [^py-store] [^storage-store].
- Advanced Alchemy adds `save_arrow` / `save_arrow_async` and forwards integrity options on `save_object*`, SQLSpec implements `DirectStreamWriteProtocol` (`supports_direct_arrow_write(file_format, *, overwrite=True)`, `open_write_stream_*`) plus `write_arrow_*` / `write_arrow_cursor_*` with `overwrite=False` (`if_not_exists=True`), and `DjangoStorage` forwards `.sha256` / `.crc32c` / `.md5` digests [^ext-alchemy-storage] [^ext-sqlspec-storage] [^ext-django-storage].
- `verify_storage_wheel.py` and `make {storage,bigquery,spanner,bundler}-wheel-check` verify wheel and sdist isolation boundaries across companion packages [^verify-storage-wheel].
<!-- truth: end -->

Known caveats (R2 custom domains, non-atomic cloud rename, unsupported provider conditions, `fsync`, cancellation windows, request timeouts) live in [Object Storage Implementation Limitations](object-storage-limitations.md).

## Rust Engine

`ObjectStoreEngine` wraps `Arc<dyn object_store::ObjectStore>` across `memory`, `file`, `s3`/`b2`, `r2`, `oci`, `gs`/`gcs`, `az`/`azure`/`abfs`, and `http`/`https`, and adds conditional `put_opts`, multipart sessions, file transfers (with `sync_all` durability on the `.part` file and parent directory in `get_file`), paging, `rename_with_options` (`supports_atomic_rename` / `atomic_only`), and GCS `compose` [^storage-store]. Azure and OCI construction validates credential options fail-closed: incomplete or empty Azure Entra triples (`tenant_id`, `client_id`, `client_secret`), empty or malformed Azure `sas_token` `key=value` pairs, and partial or empty OCI Cavage sets (`tenancy_ocid`, `user_ocid`, `fingerprint`, `private_key_pem`) return `StorageError::InvalidInput` [^storage-store]. `MultipartUploadSession` buffers normalized S3/B2, R2, GCS, and OCI writes into 8 MiB parts so every non-final part meets the 5 MiB minimum [^storage-r2]. `SyncStreamReader` / `AsyncStreamReader` slice streamed `Bytes` to an optional `chunk_size` as zero-copy `StorageBuffer` views [^storage-lib]. `.to_async()` and `.to_sync()` clone the shared engine `Arc`, so in-memory data and HTTP pools are shared across handles [^storage-lib].

## Signing

`signing.rs` implements Vertebra HMAC URLs (`X-Vtb-Expires`, `X-Vtb-Method`, `X-Vtb-Signature`, constant-time verification), AWS SigV4 query and header signing (`UNSIGNED-PAYLOAD`, RFC 3986 path encoding, 1–604800 s expiry), GCS V4 `GOOG4-RSA-SHA256` with local PKCS#8 keys or sync/async IAM `signBlob`, and Azure service SAS; `oci.rs` builds PAR URLs and Cavage request signatures [^storage-signing] [^storage-oci]. `verify_presigned_url` verifies HMAC-SHA256 (`py_verify_hmac_url`), AWS/R2 SigV4 (`py_verify_s3_v4_url`), GCS V4 (`py_verify_gcs_v4_url` using `verify_key_pem`, PEM private key, or HMAC credentials), Azure service SAS (`py_verify_azure_sas_url`), and OCI PAR URLs (`py_verify_oci_par_url`, validating `object_path` / `expires_at` / `now_ts`) [^py-store] [^storage-signing].

## Python Facade

Provider configs are frozen `msgspec.Struct` types implementing `to_url_and_options()`; constructors require an explicit target, raise `ImproperConfigurationError` for `None`, empty, or unsupported targets, and raise `MissingDependencyError` (`vertebra-core[storage]`) when `vertebra._storage` is absent [^py-store]. `AzureConfig` and `OCIConfig` preserve explicit empty strings (`is not None`) so Rust rejects blank credential fields at construction [^py-store] [^storage-store]. Configs with credentials derive `SigningConfig` automatically: S3 and R2 access keys, GCS service-account identities, Azure shared keys, and OCI PAR tokens or prefixes [^py-store]. GCS stores without `access_token` or `target_service_account` build a token cache when none is supplied, raising `ImproperConfigurationError` if explicit GCS credential options (`service_account_path`, `service_account_key`, `application_credentials_path`, `application_credentials`) are blank or invalid; `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` expose GCS OAuth2 bearer tokens to DuckDB secret rotation via `vertebra.storage` [^py-store] [^py-secrets].

`put_multipart(path, parts)` accepts a synchronous buffer iterable on both store facades and delegates to the incremental `put_stream` path. Each part reaches the native upload before the next part is requested, and producer failure aborts the upload before publishing.

`SyncWritableFile` and `AsyncWritableFile` copy writes into a rolling buffer, create the multipart upload lazily on the first full part or on close, and abort on `abort()`, an escaping exception, or (sync only) collection; an unclosed async writer emits `ResourceWarning` [^py-store]. `SyncReadableFile` and `AsyncReadableFile` iterate from offset zero with `get_stream`; `read` serves requests shorter than `read_ahead` from one cached `get_range` window and issues larger reads directly, while `read_at` reads without moving the cursor [^py-store].

## Arrow, Parquet, and CSV

Batch codecs (`vertebra.storage._arrow`) support `parquet`, `arrow-ipc` (`.arrow`, `.ipc`, `.feather`), and `csv` (`text/csv` via `pyarrow.csv.open_csv` / `read_csv` / `CSVWriter`, folding loose read/parse/convert/write option keys) and drain each encoded chunk into native `put_stream` before advancing input [^py-arrow]. Async PyArrow work runs in per-demand pool steps while native I/O stays on the caller loop, and cancellation waits for codec and I/O cleanup before releasing sources or sinks [^py-arrow]. Writers accept a Table, RecordBatch, `ArrowCursor`, `__arrow_c_stream__` producer, or synchronous batch iterable, forward `if_not_exists` to `put_stream`, and raise `ValueError` on schema-less empty iterables [^py-arrow]. Readers support `columns` projection across Parquet, IPC, and CSV; a single column name passed as `str` to `use_dictionary` or `use_byte_stream_split` selects that column, and caller `options` override keyword arguments [^py-arrow] [^py-store].

## HTTP Range and Framework Adapters

`parse_byte_range` resolves one closed, open-ended, or suffix range and raises `RangeNotSatisfiableError` for malformed or unsatisfiable input; `StreamingStorageResponse.from_sync_store` / `from_async_store` perform one `head` and produce `200`, `206`, or an empty `416` with `Content-Range: bytes */<size>` [^py-streaming]. Async stores must await `to_async_fastpath_tuple()`; `to_fastpath_tuple()` rejects async stores [^py-streaming].

`ObjectStoreLitestarStore` stores a versioned `VTBSTORE\x01` MessagePack envelope with the expiry deadline so TTL survives reconstruction, and `ObjectStoreFileSystemAdapter` confines static GET/HEAD to a base path [^ext-litestar-store]. Advanced Alchemy `ObjectStoreBackend` and `register_storage_backend` forward `signing` and `token_cache` only when building from a config or URI, reject them with a prebuilt store, forward integrity kwargs on `save_object*`, and add `save_arrow` / `save_arrow_async` [^ext-alchemy-storage]. SQLSpec `ObjectStoreBackend` honors an explicit nonempty URI (including `"memory://"`) with a prebuilt store, while an omitted or blank URI uses that store's URL [^ext-sqlspec-storage]. It implements `DirectStreamWriteProtocol` (`supports_direct_arrow_write(file_format, *, overwrite=True)` and `open_write_stream_sync` / `open_write_stream_async`) alongside `write_arrow_*` and `write_arrow_cursor_*` across `parquet`, `arrow-ipc`, and `csv`, mapping `overwrite=False` onto `if_not_exists=True` [^ext-sqlspec-storage]. It delegates `read_arrow_*` to whole-table reads and `stream_arrow_*` to batch readers, rejects `signing` and `token_cache` alongside a prebuilt store with `ImproperConfigurationError`, and propagates native signing errors unchanged [^ext-sqlspec-storage]. `DjangoStorage` subclasses Django `Storage`, deconstructs for migrations, forwards `.sha256` / `.crc32c` / `.md5` digests, and returns `File` handles over `SyncReadableFile` [^ext-django-storage].

## Tests

Rust engine, streaming, and signing tests run in `storage_engine_tests.rs`; Python contracts live in `test_storage_core_and_ranges.py`, `test_storage_arrow_and_streaming.py`, and `test_storage_bulk_checksums_and_backends.py` under `src/py/tests/unit/storage/`; emulator fixtures for S3, Azure, GCS, and R2 live in the integration `conftest.py`; companion wheel and sdist isolation is checked by `verify_storage_wheel.py` and `make {storage,bigquery,spanner,bundler}-wheel-check` [^tests-storage-engine] [^tests-storage-core] [^tests-storage-arrow] [^tests-storage-bulk] [^tests-storage-integration] [^verify-storage-wheel].

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
[^py-store]: `src/py/vertebra/storage/_store.py` — Stores, configs, readers, and writers.
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
