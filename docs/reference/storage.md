# `vertebra.storage` — Multi-Cloud & Local Object Storage

Typed object storage configurations, GIL-free `ObjectStore` operations, HMAC and cloud-native URL presigning (`SigningConfig`), random-access range readers (`ReadableFile`), and transactional upload staging (`UploadTracker`) backed by `vertebra-storage`. See the [Object Storage Usage Guide](../usage/storage.md) for backend selection and examples.

| Component | Classes | Purpose |
| --- | --- | --- |
| **Store & Registry** | `ObjectStore`, `StorageRegistry` | Synchronous and asynchronous object I/O (`put`, `get`, `head`, `delete`, `list`, `copy`) and named bucket registration |
| **Backend Configs** | `S3Config`, `GcsConfig`, `AzureConfig`, `R2Config`, `OciConfig`, `LocalConfig`, `MemoryConfig`, `HttpConfig` | Frozen `msgspec.Struct` backend configurations for AWS S3, GCS, Azure Blob, Cloudflare R2, native OCI REST v2, local disk, in-memory, and HTTP/WebDAV |
| **Signing, Range Reads & Staging** | `ObjectMeta`, `SigningConfig`, `ReadableFile`, `UploadTracker` | Presigned URLs (HMAC, SigV4, GCS V4, Azure SAS, OCI Cavage), PyArrow seekable range readers, and DB-transactional upload commit/rollback hooks |

## Object Store & Registry

`ObjectStore` wraps the Rust `object_store` runtime with zero-copy `Buffer` support, and `StorageRegistry` indexes named stores across your application.

::: vertebra.storage._store.ObjectStore
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
        - presign
        - open_readable

::: vertebra.storage.registry.StorageRegistry
    options:
      heading_level: 3
      members:
        - register
        - get
        - default

## Cloud & Local Backend Configurations

::: vertebra.storage._store.S3Config
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.GcsConfig
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

::: vertebra.storage._store.OciConfig
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

::: vertebra.storage._store.HttpConfig
    options:
      heading_level: 4
      members: false

## Metadata, Presigning, Random-Access Readers & Upload Tracking

::: vertebra.storage._store.ObjectMeta
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.SigningConfig
    options:
      heading_level: 4
      members: false

::: vertebra.storage._store.ReadableFile
    options:
      heading_level: 4
      members: false

::: vertebra.storage.pipeline.UploadTracker
    options:
      heading_level: 3
