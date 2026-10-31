# `vertebra.storage` — Multi-Cloud & Local Object Storage

Typed object storage configurations, `ObjectStore` operations, HMAC/cloud URL presigning, and staged upload validation pipelines backed by `vertebra-storage`.

## Object Store & Registry

::: vertebra.storage._store.ObjectStore
    options:
      heading_level: 3

::: vertebra.storage.registry.StorageRegistry
    options:
      heading_level: 3

## Cloud & Local Backend Configurations

::: vertebra.storage._store.S3Config
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.GcsConfig
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.AzureConfig
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.R2Config
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.OciConfig
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.LocalConfig
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.MemoryConfig
    options:
      heading_level: 3
      members: false

## Metadata, Presigning & Upload Tracking

::: vertebra.storage._store.ObjectMeta
    options:
      heading_level: 3
      members: false

::: vertebra.storage._store.SigningConfig
    options:
      heading_level: 3
      members: false

::: vertebra.storage.pipeline.UploadTracker
    options:
      heading_level: 3
