"""Native object storage, Arrow file bridge, and V4/OCI/Azure/GCS signing package for Vertebra."""

from typing import TYPE_CHECKING

from vertebra.storage._store import (
    AsyncGcpTokenCache,
    AsyncObjectStore,
    AsyncReadableFile,
    AzureConfig,
    GcpTokenSnapshot,
    GCSConfig,
    HTTPConfig,
    LocalConfig,
    MemoryConfig,
    ObjectMeta,
    OCIConfig,
    PutResult,
    R2Config,
    S3Config,
    SigningConfig,
    StorageConfig,
    SyncGcpTokenCache,
    SyncObjectStore,
    SyncReadableFile,
)
from vertebra.storage._streaming import StreamingStorageResponse
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from vertebra._storage import PreconditionError as PreconditionError

__all__ = (
    "AsyncGcpTokenCache",
    "AsyncObjectStore",
    "AsyncReadableFile",
    "AzureConfig",
    "GCSConfig",
    "GcpTokenSnapshot",
    "HTTPConfig",
    "LocalConfig",
    "MemoryConfig",
    "OCIConfig",
    "ObjectMeta",
    "PreconditionError",
    "PutResult",
    "R2Config",
    "S3Config",
    "SigningConfig",
    "StorageConfig",
    "StreamingStorageResponse",
    "SyncGcpTokenCache",
    "SyncObjectStore",
    "SyncReadableFile",
)


def __getattr__(name: str) -> type[PermissionError]:
    """Resolve the native conflict type only when a caller requests it."""
    if name != "PreconditionError":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    native = import_optional("vertebra._storage")
    if native is None:
        raise MissingDependencyError("vertebra._storage", install_package="vertebra-core[storage]", extra="storage")
    exception: type[PermissionError] = native.PreconditionError
    return exception
