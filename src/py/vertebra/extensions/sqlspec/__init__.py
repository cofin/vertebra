"""SQLSpec extension, PEP 695 generic configurations, and zero-copy object storage integration."""

from vertebra.extensions.sqlspec.oracle import OracleHybridSearch
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
    SQLSpecConfig,
    SQLSpecPlugin,
    SQLSpecState,
    SyncDatabaseConfig,
    SyncDriverAdapterBase,
)
from vertebra.extensions.sqlspec.storage import (
    SQLSpecStore,
    StreamingStorageResponse,
)

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "BrokerPlugin",
    "ChannelMessage",
    "DatabaseConfigProtocol",
    "OracleHybridSearch",
    "ProxyAsyncConfig",
    "ProxySyncConfig",
    "SQLSpecConfig",
    "SQLSpecPlugin",
    "SQLSpecState",
    "SQLSpecStore",
    "StreamingStorageResponse",
    "SyncDatabaseConfig",
    "SyncDriverAdapterBase",
)
