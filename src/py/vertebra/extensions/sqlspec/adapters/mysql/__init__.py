"""Vertebra MySQL/MariaDB SQLSpec adapter package with PEP 562 lazy attribute loading."""

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.mysql.adk import (
        MySQLADKConfig,
        MySQLAsyncADKArtifactStore,
        MySQLAsyncADKMemoryStore,
        MySQLAsyncADKStore,
        MySQLSyncADKArtifactStore,
        MySQLSyncADKMemoryStore,
        MySQLSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.mysql.config import MySQLAsyncConfig, MySQLSyncConfig
    from vertebra.extensions.sqlspec.adapters.mysql.core import (
        build_connection_config,
        build_statement_config,
        default_statement_config,
        driver_profile,
        map_mysql_exception,
    )
    from vertebra.extensions.sqlspec.adapters.mysql.data_dictionary import (
        MySQLAsyncDataDictionary,
        MySQLSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.mysql.driver import MySQLAsyncDriver, MySQLSyncDriver
    from vertebra.extensions.sqlspec.adapters.mysql.events import MySQLEventQueueStore, MySQLEventsConfig
    from vertebra.extensions.sqlspec.adapters.mysql.litestar import MySQLLitestarConfig, MySQLLitestarStore

__all__ = (
    "MySQLADKConfig",
    "MySQLAsyncADKArtifactStore",
    "MySQLAsyncADKMemoryStore",
    "MySQLAsyncADKStore",
    "MySQLAsyncConfig",
    "MySQLAsyncDataDictionary",
    "MySQLAsyncDriver",
    "MySQLEventQueueStore",
    "MySQLEventsConfig",
    "MySQLLitestarConfig",
    "MySQLLitestarStore",
    "MySQLSyncADKArtifactStore",
    "MySQLSyncADKMemoryStore",
    "MySQLSyncADKStore",
    "MySQLSyncConfig",
    "MySQLSyncDataDictionary",
    "MySQLSyncDriver",
    "build_connection_config",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
    "map_mysql_exception",
)

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "MySQLADKConfig": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLADKConfig"),
    "MySQLAsyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLAsyncADKArtifactStore"),
    "MySQLAsyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLAsyncADKMemoryStore"),
    "MySQLAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLAsyncADKStore"),
    "MySQLAsyncConfig": ("vertebra.extensions.sqlspec.adapters.mysql.config", "MySQLAsyncConfig"),
    "MySQLAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.mysql.data_dictionary",
        "MySQLAsyncDataDictionary",
    ),
    "MySQLAsyncDriver": ("vertebra.extensions.sqlspec.adapters.mysql.driver", "MySQLAsyncDriver"),
    "MySQLEventQueueStore": ("vertebra.extensions.sqlspec.adapters.mysql.events", "MySQLEventQueueStore"),
    "MySQLEventsConfig": ("vertebra.extensions.sqlspec.adapters.mysql.events", "MySQLEventsConfig"),
    "MySQLLitestarConfig": ("vertebra.extensions.sqlspec.adapters.mysql.litestar", "MySQLLitestarConfig"),
    "MySQLLitestarStore": ("vertebra.extensions.sqlspec.adapters.mysql.litestar", "MySQLLitestarStore"),
    "MySQLSyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLSyncADKArtifactStore"),
    "MySQLSyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLSyncADKMemoryStore"),
    "MySQLSyncADKStore": ("vertebra.extensions.sqlspec.adapters.mysql.adk", "MySQLSyncADKStore"),
    "MySQLSyncConfig": ("vertebra.extensions.sqlspec.adapters.mysql.config", "MySQLSyncConfig"),
    "MySQLSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.mysql.data_dictionary",
        "MySQLSyncDataDictionary",
    ),
    "MySQLSyncDriver": ("vertebra.extensions.sqlspec.adapters.mysql.driver", "MySQLSyncDriver"),
    "build_connection_config": ("vertebra.extensions.sqlspec.adapters.mysql.core", "build_connection_config"),
    "build_statement_config": ("vertebra.extensions.sqlspec.adapters.mysql.core", "build_statement_config"),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.mysql.core", "default_statement_config"),
    "driver_profile": ("vertebra.extensions.sqlspec.adapters.mysql.core", "driver_profile"),
    "map_mysql_exception": ("vertebra.extensions.sqlspec.adapters.mysql.core", "map_mysql_exception"),
}


def __getattr__(name: str) -> Any:
    """Resolve exported adapter symbols lazily via PEP 562 module lookup."""
    target = _LAZY_ATTRS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_path, attr_name = target
    value = getattr(ensure_module(module_path), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """Return sorted exported module attributes."""
    return sorted(__all__)
