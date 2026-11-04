# `vertebra.storage` — Multi-Cloud & Local Object Storage

Typed object storage configurations, GIL-free `SyncObjectStore` and `AsyncObjectStore` operations, HMAC and cloud-native URL presigning (`SigningConfig`), and random-access range readers (`SyncReadableFile` and `AsyncReadableFile`) backed by `vertebra-storage`. See the [Object Storage Usage Guide](../usage/storage.md) for backend selection and examples.

| Component | Classes | Purpose |
| --- | --- | --- |
| **Store** | `SyncObjectStore`, `AsyncObjectStore` | Synchronous and asynchronous object I/O (`put`, `get`, `head`, `delete`, `list`, `copy`) |
| **Backend Configs** | `S3Config`, `GCSConfig`, `AzureConfig`, `R2Config`, `OCIConfig`, `LocalConfig`, `MemoryConfig`, `HTTPConfig` | Frozen `msgspec.Struct` backend configurations for AWS S3, GCS (with ADC), Azure Blob, Cloudflare R2, native OCI REST v2, local disk, in-memory, and HTTP/WebDAV |
| **Signing & Range Reads** | `ObjectMeta`, `SigningConfig`, `SyncReadableFile`, `AsyncReadableFile` | Presigned URLs (HMAC, SigV4, GCS V4, Azure SAS, OCI Cavage) and PyArrow seekable range readers |

## Object Store

`SyncObjectStore` and `AsyncObjectStore` wrap the Rust `object_store` runtime with zero-copy `Buffer` support.

::: vertebra.storage._store.SyncObjectStore
    options:
      heading_level: 3
      members:
        - from_url
        - put
        - get
        - head
        - delete
        - list
        - copy
        - presign_get
        - presign_put
        - open_reader

::: vertebra.storage._store.AsyncObjectStore
    options:
      heading_level: 3
      members:
        - from_url
        - put
        - get
        - head
        - delete
        - list
        - copy
        - presign_get
        - presign_put
        - open_reader

## Cloud & Local Backend Configurations

::: vertebra.storage._store.S3Config
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.GCSConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.AzureConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.R2Config
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.OCIConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.LocalConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.MemoryConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.HTTPConfig
    options:
      heading_level: 4
      members: false

## Metadata, Presigning & Random-Access Readers

::: vertebra.storage._store.ObjectMeta
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.SigningConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.SyncReadableFile
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.AsyncReadableFile
    options:
      heading_level: 4
      members: false
