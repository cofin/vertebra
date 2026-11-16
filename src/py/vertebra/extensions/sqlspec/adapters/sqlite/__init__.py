"""Vertebra SQLite SQLSpec adapter package with PEP 562 lazy attribute loading."""

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.sqlite.adk import (
        SQLiteADKConfig,
        SQLiteAsyncADKArtifactStore,
        SQLiteAsyncADKMemoryStore,
        SQLiteAsyncADKStore,
        SQLiteSyncADKArtifactStore,
        SQLiteSyncADKMemoryStore,
        SQLiteSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig, SQLiteSyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.core import (
        build_connection_config,
        datetime_to_julian,
        default_statement_config,
        driver_profile,
        julian_to_datetime,
    )
    from vertebra.extensions.sqlspec.adapters.sqlite.data_dictionary import (
        SQLiteAsyncDataDictionary,
        SQLiteSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver, SQLiteSyncDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore, SQLiteEventsConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.litestar import SQLiteLitestarConfig, SQLiteLitestarStore

__all__ = (
    "SQLiteADKConfig",
    "SQLiteAsyncADKArtifactStore",
    "SQLiteAsyncADKMemoryStore",
    "SQLiteAsyncADKStore",
    "SQLiteAsyncConfig",
    "SQLiteAsyncDataDictionary",
    "SQLiteAsyncDriver",
    "SQLiteEventQueueStore",
    "SQLiteEventsConfig",
    "SQLiteLitestarConfig",
    "SQLiteLitestarStore",
    "SQLiteSyncADKArtifactStore",
    "SQLiteSyncADKMemoryStore",
    "SQLiteSyncADKStore",
    "SQLiteSyncConfig",
    "SQLiteSyncDataDictionary",
    "SQLiteSyncDriver",
    "build_connection_config",
    "datetime_to_julian",
    "default_statement_config",
    "driver_profile",
    "julian_to_datetime",
)

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "SQLiteADKConfig": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteADKConfig"),
    "SQLiteAsyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteAsyncADKArtifactStore"),
    "SQLiteAsyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteAsyncADKMemoryStore"),
    "SQLiteAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteAsyncADKStore"),
    "SQLiteAsyncConfig": ("vertebra.extensions.sqlspec.adapters.sqlite.config", "SQLiteAsyncConfig"),
    "SQLiteAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.sqlite.data_dictionary",
        "SQLiteAsyncDataDictionary",
    ),
    "SQLiteAsyncDriver": ("vertebra.extensions.sqlspec.adapters.sqlite.driver", "SQLiteAsyncDriver"),
    "SQLiteEventQueueStore": ("vertebra.extensions.sqlspec.adapters.sqlite.events", "SQLiteEventQueueStore"),
    "SQLiteEventsConfig": ("vertebra.extensions.sqlspec.adapters.sqlite.events", "SQLiteEventsConfig"),
    "SQLiteLitestarConfig": ("vertebra.extensions.sqlspec.adapters.sqlite.litestar", "SQLiteLitestarConfig"),
    "SQLiteLitestarStore": ("vertebra.extensions.sqlspec.adapters.sqlite.litestar", "SQLiteLitestarStore"),
    "SQLiteSyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteSyncADKArtifactStore"),
    "SQLiteSyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteSyncADKMemoryStore"),
    "SQLiteSyncADKStore": ("vertebra.extensions.sqlspec.adapters.sqlite.adk", "SQLiteSyncADKStore"),
    "SQLiteSyncConfig": ("vertebra.extensions.sqlspec.adapters.sqlite.config", "SQLiteSyncConfig"),
    "SQLiteSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.sqlite.data_dictionary",
        "SQLiteSyncDataDictionary",
    ),
    "SQLiteSyncDriver": ("vertebra.extensions.sqlspec.adapters.sqlite.driver", "SQLiteSyncDriver"),
    "build_connection_config": ("vertebra.extensions.sqlspec.adapters.sqlite.core", "build_connection_config"),
    "datetime_to_julian": ("vertebra.extensions.sqlspec.adapters.sqlite.core", "datetime_to_julian"),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.sqlite.core", "default_statement_config"),
    "driver_profile": ("vertebra.extensions.sqlspec.adapters.sqlite.core", "driver_profile"),
    "julian_to_datetime": ("vertebra.extensions.sqlspec.adapters.sqlite.core", "julian_to_datetime"),
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
    """Return sorted public attribute names exposed by this package."""
    return sorted(__all__)
