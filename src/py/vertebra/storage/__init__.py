"""Unified storage module for local, S3, and GCS backends.

This module provides filesystem clients, object stores, and cloud-specific clients:

Filesystem clients (IPC-backed):
    :class:`SyncFileSystemClient` (sync) and :class:`AsyncFileSystemClient` (async)
    are the canonical interfaces for filesystem operations. They support multiple
    backends (local, S3, GCS) via the ``backend`` parameter.

Object stores (high-level API):
    :class:`LocalStore`, :class:`S3Store`, :class:`GCSStore` and their async
    variants provide a high-level object store interface with operations like
    ``get``, ``put``, ``list``, ``head``, and streaming.

Cloud clients (simple wrappers):
    :class:`S3Client`, :class:`GCSClient` and their async variants provide
    simple upload/download APIs for cloud storage.

Example usage::

    # Local filesystem
    fs = SyncFileSystemClient(backend="local")
    data = fs.read_bytes("/path/to/file")

    # S3 via object store
    store = S3Store(bucket="mybucket")
    data = store.get("key/to/object")

    # GCS via simple client
    client = GCSClient(bucket="mybucket")
    data = client.download_bytes("key/to/object")
"""

from typing import TYPE_CHECKING

from vertebra.core.exceptions import StorageError
from vertebra.storage.client import AsyncFileSystemClient, Backend, FileEntry, SyncFileSystemClient
from vertebra.storage.fsspec import AsyncVertebraFileSystem, SyncVertebraFileSystem
from vertebra.storage.gcs import AsyncGCSClient, GCSClient
from vertebra.storage.s3 import AsyncS3Client, S3Client
from vertebra.storage.store import (
    AsyncGCSStore,
    AsyncLocalStore,
    AsyncObjectStore,
    AsyncS3Store,
    ByteRange,
    GCSStore,
    ListResult,
    LocalStore,
    ObjectMeta,
    PutOptions,
    PutResult,
    S3Store,
    StoreRegistry,
    SyncObjectStore,
    async_store_from_url,
    store_from_url,
    store_registry,
)

__all__ = (
    # Filesystem clients
    "AsyncFileSystemClient",
    # Cloud clients
    "AsyncGCSClient",
    # Object stores
    "AsyncGCSStore",
    "AsyncLocalStore",
    "AsyncObjectStore",
    "AsyncS3Client",
    "AsyncS3Store",
    # fsspec integration
    "AsyncVertebraFileSystem",
    "Backend",
    "ByteRange",
    "FileEntry",
    "GCSClient",
    "GCSStore",
    "ListResult",
    "LocalStore",
    "ObjectMeta",
    "PutOptions",
    "PutResult",
    "S3Client",
    "S3Store",
    "StorageError",
    "StoreRegistry",
    "SyncFileSystemClient",
    "SyncObjectStore",
    "SyncVertebraFileSystem",
    "async_store_from_url",
    "store_from_url",
    "store_registry",
)
