"""Vertebra SQL Server (MSSQL) SQLSpec adapter package with PEP 562 lazy attribute loading."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.mssql._typing import (
        MSSQLAsyncConnectionContext,
        MSSQLAsyncSessionContext,
        MSSQLConnectionParams,
        MSSQLDriverFeatures,
        MSSQLPoolParams,
        MSSQLSyncConnectionContext,
        MSSQLSyncSessionContext,
    )
    from vertebra.extensions.sqlspec.adapters.mssql.adk import (
        MSSQLADKConfig,
        MSSQLAsyncADKArtifactStore,
        MSSQLAsyncADKMemoryStore,
        MSSQLAsyncADKStore,
        MSSQLSyncADKArtifactStore,
        MSSQLSyncADKMemoryStore,
        MSSQLSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.mssql.config import MSSQLAsyncConfig, MSSQLSyncConfig
    from vertebra.extensions.sqlspec.adapters.mssql.core import (
        build_connection_config,
        build_statement_config,
        default_statement_config,
        driver_profile,
        map_mssql_exception,
        rewrite_mssql_placeholders,
    )
    from vertebra.extensions.sqlspec.adapters.mssql.data_dictionary import (
        MSSQLAsyncDataDictionary,
        MSSQLSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.mssql.driver import MSSQLAsyncDriver, MSSQLSyncDriver
    from vertebra.extensions.sqlspec.adapters.mssql.events import MSSQLEventQueueStore
    from vertebra.extensions.sqlspec.adapters.mssql.litestar import MSSQLLitestarStore

__all__ = (
    "MSSQLADKConfig",
    "MSSQLAsyncADKArtifactStore",
    "MSSQLAsyncADKMemoryStore",
    "MSSQLAsyncADKStore",
    "MSSQLAsyncConfig",
    "MSSQLAsyncConnectionContext",
    "MSSQLAsyncDataDictionary",
    "MSSQLAsyncDriver",
    "MSSQLAsyncSessionContext",
    "MSSQLConnectionParams",
    "MSSQLDriverFeatures",
    "MSSQLEventQueueStore",
    "MSSQLLitestarStore",
    "MSSQLPoolParams",
    "MSSQLSyncADKArtifactStore",
    "MSSQLSyncADKMemoryStore",
    "MSSQLSyncADKStore",
    "MSSQLSyncConfig",
    "MSSQLSyncConnectionContext",
    "MSSQLSyncDataDictionary",
    "MSSQLSyncDriver",
    "MSSQLSyncSessionContext",
    "build_connection_config",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
    "map_mssql_exception",
    "rewrite_mssql_placeholders",
)

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "MSSQLADKConfig": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLADKConfig"),
    "MSSQLAsyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLAsyncADKArtifactStore"),
    "MSSQLAsyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLAsyncADKMemoryStore"),
    "MSSQLAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLAsyncADKStore"),
    "MSSQLAsyncConfig": ("vertebra.extensions.sqlspec.adapters.mssql.config", "MSSQLAsyncConfig"),
    "MSSQLAsyncConnectionContext": (
        "vertebra.extensions.sqlspec.adapters.mssql._typing",
        "MSSQLAsyncConnectionContext",
    ),
    "MSSQLAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.mssql.data_dictionary",
        "MSSQLAsyncDataDictionary",
    ),
    "MSSQLAsyncDriver": ("vertebra.extensions.sqlspec.adapters.mssql.driver", "MSSQLAsyncDriver"),
    "MSSQLAsyncSessionContext": (
        "vertebra.extensions.sqlspec.adapters.mssql._typing",
        "MSSQLAsyncSessionContext",
    ),
    "MSSQLConnectionParams": ("vertebra.extensions.sqlspec.adapters.mssql._typing", "MSSQLConnectionParams"),
    "MSSQLDriverFeatures": ("vertebra.extensions.sqlspec.adapters.mssql._typing", "MSSQLDriverFeatures"),
    "MSSQLEventQueueStore": ("vertebra.extensions.sqlspec.adapters.mssql.events", "MSSQLEventQueueStore"),
    "MSSQLLitestarStore": ("vertebra.extensions.sqlspec.adapters.mssql.litestar", "MSSQLLitestarStore"),
    "MSSQLPoolParams": ("vertebra.extensions.sqlspec.adapters.mssql._typing", "MSSQLPoolParams"),
    "MSSQLSyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLSyncADKArtifactStore"),
    "MSSQLSyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLSyncADKMemoryStore"),
    "MSSQLSyncADKStore": ("vertebra.extensions.sqlspec.adapters.mssql.adk", "MSSQLSyncADKStore"),
    "MSSQLSyncConfig": ("vertebra.extensions.sqlspec.adapters.mssql.config", "MSSQLSyncConfig"),
    "MSSQLSyncConnectionContext": (
        "vertebra.extensions.sqlspec.adapters.mssql._typing",
        "MSSQLSyncConnectionContext",
    ),
    "MSSQLSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.mssql.data_dictionary",
        "MSSQLSyncDataDictionary",
    ),
    "MSSQLSyncDriver": ("vertebra.extensions.sqlspec.adapters.mssql.driver", "MSSQLSyncDriver"),
    "MSSQLSyncSessionContext": (
        "vertebra.extensions.sqlspec.adapters.mssql._typing",
        "MSSQLSyncSessionContext",
    ),
    "build_connection_config": ("vertebra.extensions.sqlspec.adapters.mssql.core", "build_connection_config"),
    "build_statement_config": ("vertebra.extensions.sqlspec.adapters.mssql.core", "build_statement_config"),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.mssql.core", "default_statement_config"),
    "driver_profile": ("vertebra.extensions.sqlspec.adapters.mssql.core", "driver_profile"),
    "map_mssql_exception": ("vertebra.extensions.sqlspec.adapters.mssql.core", "map_mssql_exception"),
    "rewrite_mssql_placeholders": ("vertebra.extensions.sqlspec.adapters.mssql.core", "rewrite_mssql_placeholders"),
}


def __getattr__(name: str) -> Any:
    """Resolve exported SQL Server SQLSpec symbols lazily via PEP 562."""
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
