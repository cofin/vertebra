# `vertebra.storage`

Backend configurations, GIL-free `SyncObjectStore` and `AsyncObjectStore`, file-like readers and writers, presigned URLs, and HTTP Range helpers backed by the `vertebra-storage` native extension. See the [object storage guide](../usage/storage.md) for backend selection and examples.

## Object stores

Construct a store from a URI or configuration: `SyncObjectStore(target, *, options=None, signing=None, token_cache=None)`. The class methods `from_config`, `from_local`, `from_s3`, `from_gcs`, `from_azure`, `from_oci`, `from_r2`, `from_http`, and `from_memory` build the same store from configuration fields. Every store exposes the read-only properties `url`, `scheme`, `options`, `token_cache`, and `supports_signing`, plus `close()` and the context-manager protocol. `put_multipart(key, parts)` uploads a sequence of buffers as one object.

`AsyncObjectStore` mirrors `SyncObjectStore` with the same constructor, class methods, properties, and method signatures. Its I/O methods are awaited, except `get_stream`, `iter_arrow_batches`, `iter_parquet_batches`, `open_reader`, and `open_writer`, which return async iterators or file objects directly. `staged_download` is an async context manager, `verify_presigned_url` stays synchronous, and `to_sync()` returns a `SyncObjectStore` sharing the same native engine.

::: vertebra.storage._store.SyncObjectStore
    options:
      heading_level: 3
      inherited_members: true
      members:
        - to_async
        - put
        - put_stream
        - put_file
        - upload_many
        - get
        - get_view
        - get_stream
        - get_range
        - get_file
        - download_many
        - staged_download
        - head
        - exists
        - delete
        - copy
        - rename
        - compose
        - list
        - list_page
        - list_with_delimiter
        - glob
        - open_reader
        - open_writer
        - write_arrow
        - write_parquet
        - iter_arrow_batches
        - iter_parquet_batches
        - sign
        - presign_get
        - presign_put
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

`vertebra.storage` re-exports `SyncGcpTokenCache`, `AsyncGcpTokenCache`, and `GcpTokenSnapshot` (OAuth bearer tokens) alongside `SyncGcpIdTokenCache`, `AsyncGcpIdTokenCache`, and `GcpIdTokenSnapshot` (audience-bound OIDC ID tokens) from the core `vertebra._vertebra` extension. One cache passed as `token_cache=` or `id_token_cache=` to several stores or embedders shares a single token and refresh schedule. `SyncGcpTokenCache.get_token(*, force_refresh=False)` returns a `GcpTokenSnapshot`, and `SyncGcpIdTokenCache.get_token(audience, *, force_refresh=False)` returns a `GcpIdTokenSnapshot`; the async variants await the same calls, and `to_async()` / `to_sync()` convert between handles over one native cache. The [authentication guide](../usage/storage-auth-and-signing.md#google-cloud-credentials-and-token-caches) covers construction, ADC discovery order, and sharing.

## HTTP Range responses

`parse_byte_range(range_header, total_size)` resolves one `Range` header into a `ByteRange(start, end_exclusive, total_size)`, whose `content_length` and `content_range_header` properties feed the response headers. It returns `None` when the header is missing or uses a unit other than `bytes`, and raises `RangeNotSatisfiableError` (a `ValueError` carrying `total_size`) for malformed or unsatisfiable ranges.

::: vertebra.storage._streaming.StreamingStorageResponse
    options:
      heading_level: 4
      show_bases: false
      members: false

## Errors

::: vertebra.typing.PreconditionError
    options:
      heading_level: 4
