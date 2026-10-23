"""SQLSpec extension, PEP 695 generic configurations, and zero-copy object storage integration."""

from vertebra.extensions.sqlspec.plugin import (
    SQLSPEC_PLUGIN_PRIORITY,
    SQLSPEC_STATE_KEY,
    AsyncDatabaseConfig,
    AsyncDriverAdapterBase,
    BrokerPlugin,
    ChannelMessage,
    DatabaseConfigProtocol,
    ProxyAsyncConfig,
    ProxySyncConfig,
    SQLSpecAsyncConfig,
    SQLSpecConfig,
    SQLSpecPlugin,
    SQLSpecState,
    SQLSpecSyncConfig,
    SyncDatabaseConfig,
    SyncDriverAdapterBase,
)
from vertebra.extensions.sqlspec.storage import (
    ArrowFlightSerializer,
    SqlSpecStorageBridge,
    SQLSpecStore,
    StreamingStorageResponse,
)

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "ArrowFlightSerializer",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "BrokerPlugin",
    "ChannelMessage",
    "DatabaseConfigProtocol",
    "ProxyAsyncConfig",
    "ProxySyncConfig",
    "SQLSpecAsyncConfig",
    "SQLSpecConfig",
    "SQLSpecPlugin",
    "SQLSpecState",
    "SQLSpecStore",
    "SQLSpecSyncConfig",
    "SqlSpecStorageBridge",
    "StreamingStorageResponse",
    "SyncDatabaseConfig",
    "SyncDriverAdapterBase",
)
