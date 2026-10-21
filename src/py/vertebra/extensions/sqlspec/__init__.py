"""SQLSpec extension and zero-copy object storage integration for Vertebra."""

from vertebra.extensions.sqlspec.plugin import (
    SQLSPEC_PLUGIN_PRIORITY,
    SQLSPEC_STATE_KEY,
    SQLSpecState,
    VertebraSQLSpecPlugin,
)
from vertebra.extensions.sqlspec.storage import (
    StreamingStorageResponse,
    VertebraObjectStore,
    VertebraSQLSpecStorageAdapter,
)

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "SQLSpecState",
    "StreamingStorageResponse",
    "VertebraObjectStore",
    "VertebraSQLSpecPlugin",
    "VertebraSQLSpecStorageAdapter",
)
