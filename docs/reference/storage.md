# Object Storage

`vertebra.storage` provides backend configs, sync and async object stores, file-like readers and writers, presigned URLs, and HTTP Range helpers backed by `vertebra-storage`. See the [object storage guide](../usage/storage.md) for backend selection and examples.

## Object stores

Construct a store from a URI or configuration with `SyncObjectStore(target, *, options=None, signing=None, token_cache=None)`. Factory methods (`from_config`, `from_local`, `from_s3`, `from_gcs`, `from_azure`, `from_oci`, `from_r2`, `from_http`, and `from_memory`) build the same store from fields. Each store exposes read-only properties (`url`, `scheme`, `options`, `token_cache`, `supports_signing`), batch transfers (`upload_many`, `download_many`, `staged_download`), paginated listing (`list_page`, `list_with_delimiter`), URL signing (`sign`, `presign_get`, `presign_put`, `verify_presigned_url`), `put_multipart`, `to_async()`, `close()`, and context-manager support.

`AsyncObjectStore` mirrors `SyncObjectStore` with matching constructors, properties, and signatures. I/O methods are awaited, except `get_stream`, `iter_arrow_batches`, `iter_parquet_batches`, `open_reader`, and `open_writer`, which return async iterators or file handles directly. `staged_download` is an async context manager, `verify_presigned_url` stays synchronous, and `to_sync()` returns a `SyncObjectStore` sharing the same Rust handle.

::: vertebra.storage._store.SyncObjectStore
    options:
      heading_level: 3
      show_bases: false
      show_source: false
      members:
        - put
        - put_stream
        - get
        - get_view
        - get_stream
        - head
        - exists
        - delete
        - list
        - open_reader
        - open_writer
        - write_arrow
        - write_parquet
        - iter_arrow_batches
        - iter_parquet_batches
        - sign
        - verify_presigned_url

::: vertebra.storage._store.AsyncObjectStore
    options:
      heading_level: 3
      members: false

## Backend configurations

All configurations are frozen `msgspec.Struct` types. `GCSConfig.sync_token_cache()` and `GCSConfig.async_token_cache()` build a shared token cache for the configured identity.

::: vertebra.storage._store.S3Config
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.GCSConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.AzureConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.R2Config
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.OCIConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.LocalConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.MemoryConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.HTTPConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

## Readers and writers

Readers support `read`, `read_at`, `seek`, `tell`, and `close`; the async reader awaits `read` and `read_at`. Writers support `write`, `tell`, `close` (`aclose` on the async writer), `abort`, and the `result` property.

::: vertebra.storage._store.SyncReadableFile
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.AsyncReadableFile
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.SyncWritableFile
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.AsyncWritableFile
    options:
      heading_level: 4
      members: false

## Metadata and signing

::: vertebra.storage._store.ObjectMeta
    options:
      heading_level: 4
      show_bases: false
      members: false

::: vertebra.storage._store.PutResult
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.SigningConfig
    options:
      heading_level: 4
      show_bases: false
      members: false

## GCS token caches

`vertebra.storage` re-exports OAuth bearer token caches (`SyncGcpTokenCache`, `AsyncGcpTokenCache`, `GcpTokenSnapshot`) and audience-bound OIDC ID token caches (`SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, `GcpIdTokenSnapshot`) from `vertebra._vertebra`. It also provides `SyncGcsBearerSecretProvider` and `AsyncGcsBearerSecretProvider` for DuckDB GCS bearer-token rotation. Passing one cache via `token_cache=` or `id_token_cache=` shares a single token and refresh schedule across multiple stores, secret providers, or embedders. Call `get_token()` to fetch a snapshot, or `to_async()` and `to_sync()` to switch between handles sharing the same Rust cache. See the [authentication guide](../usage/storage-auth-and-signing.md#google-cloud-credentials-and-token-caches) for construction, ADC discovery order, and sharing.

::: vertebra.storage._secrets.SyncGcsBearerSecretProvider
    options:
      heading_level: 4
      show_bases: false
      show_source: false
      members: false

::: vertebra.storage._secrets.AsyncGcsBearerSecretProvider
    options:
      heading_level: 4
      show_bases: false
      show_source: false
      members: false

## HTTP Range responses

`parse_byte_range(range_header, total_size)` resolves one `Range` header into a `ByteRange(start, end_exclusive, total_size)`, whose `content_length` and `content_range_header` properties feed the response headers. It returns `None` when the header is missing or uses a unit other than `bytes`, and raises `RangeNotSatisfiableError` (a `ValueError` carrying `total_size`) for malformed or unsatisfiable ranges.

::: vertebra.storage._streaming.StreamingStorageResponse
    options:
      heading_level: 4
      show_bases: false
      show_source: false
      members: false

## Errors

::: vertebra.typing.IntegrityError
    options:
      heading_level: 4
      members: false

::: vertebra.typing.PreconditionError
    options:
      heading_level: 4
      members: false
