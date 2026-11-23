"""Sync and async object storage integration for Starlette and FastAPI applications."""

from __future__ import annotations

from vertebra.extensions._web.asgi_starlette import attach_state_storage as attach_storage
from vertebra.framework.storage import (
    AsyncObjectStore,
    AsyncStorageRegistry,
    AzureConfig,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    OCIConfig,
    R2Config,
    S3Config,
    StorageConfig,
    SyncObjectStore,
    SyncStorageRegistry,
)

__all__ = (
    "AsyncObjectStore",
    "AsyncStorageRegistry",
    "AzureConfig",
    "GCSConfig",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "R2Config",
    "S3Config",
    "StorageConfig",
    "SyncObjectStore",
    "SyncStorageRegistry",
    "attach_storage",
)
