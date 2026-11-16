"""DuckDB SQLSpec adapter package exports with PEP 562 lazy loading for optional stores."""

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.duckdb.adk import (
        DuckDBAsyncADKArtifactStore,
        DuckDBAsyncADKMemoryStore,
        DuckDBAsyncADKStore,
        DuckDBSyncADKArtifactStore,
        DuckDBSyncADKMemoryStore,
        DuckDBSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.duckdb.config import DuckDBAsyncConfig, DuckDBSyncConfig
    from vertebra.extensions.sqlspec.adapters.duckdb.core import (
        ARROW_MIN_ROWS,
        DUCKDB_COPY_FILENAME_FOLLOWS_QUERY,
        build_connection_config,
        build_duckdb_copy_sql,
        build_profile,
        build_statement_config,
        collect_rows,
        default_statement_config,
        description_has_arrow_divergent_types,
        driver_profile,
        duckdb_copy_filename_follows_query,
        map_duckdb_exception,
        order_duckdb_copy_parameters,
    )
    from vertebra.extensions.sqlspec.adapters.duckdb.data_dictionary import (
        DuckDBAsyncDataDictionary,
        DuckDBSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver, DuckDBSyncDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.events import DuckDBEventQueueStore
    from vertebra.extensions.sqlspec.adapters.duckdb.litestar import DuckDBLitestarStore

__all__ = (
    "ARROW_MIN_ROWS",
    "DUCKDB_COPY_FILENAME_FOLLOWS_QUERY",
    "DuckDBAsyncADKArtifactStore",
    "DuckDBAsyncADKMemoryStore",
    "DuckDBAsyncADKStore",
    "DuckDBAsyncConfig",
    "DuckDBAsyncDataDictionary",
    "DuckDBAsyncDriver",
    "DuckDBEventQueueStore",
    "DuckDBLitestarStore",
    "DuckDBSyncADKArtifactStore",
    "DuckDBSyncADKMemoryStore",
    "DuckDBSyncADKStore",
    "DuckDBSyncConfig",
    "DuckDBSyncDataDictionary",
    "DuckDBSyncDriver",
    "build_connection_config",
    "build_duckdb_copy_sql",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "default_statement_config",
    "description_has_arrow_divergent_types",
    "driver_profile",
    "duckdb_copy_filename_follows_query",
    "map_duckdb_exception",
    "order_duckdb_copy_parameters",
)

_LAZY_ATTRS: dict[str, tuple[str, str]] = {
    "ARROW_MIN_ROWS": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "ARROW_MIN_ROWS"),
    "DUCKDB_COPY_FILENAME_FOLLOWS_QUERY": (
        "vertebra.extensions.sqlspec.adapters.duckdb.core",
        "DUCKDB_COPY_FILENAME_FOLLOWS_QUERY",
    ),
    "DuckDBAsyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBAsyncADKArtifactStore",
    ),
    "DuckDBAsyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBAsyncADKMemoryStore",
    ),
    "DuckDBAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.duckdb.adk", "DuckDBAsyncADKStore"),
    "DuckDBAsyncConfig": ("vertebra.extensions.sqlspec.adapters.duckdb.config", "DuckDBAsyncConfig"),
    "DuckDBAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.duckdb.data_dictionary",
        "DuckDBAsyncDataDictionary",
    ),
    "DuckDBAsyncDriver": ("vertebra.extensions.sqlspec.adapters.duckdb.driver", "DuckDBAsyncDriver"),
    "DuckDBEventQueueStore": ("vertebra.extensions.sqlspec.adapters.duckdb.events", "DuckDBEventQueueStore"),
    "DuckDBLitestarStore": ("vertebra.extensions.sqlspec.adapters.duckdb.litestar", "DuckDBLitestarStore"),
    "DuckDBSyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBSyncADKArtifactStore",
    ),
    "DuckDBSyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.duckdb.adk",
        "DuckDBSyncADKMemoryStore",
    ),
    "DuckDBSyncADKStore": ("vertebra.extensions.sqlspec.adapters.duckdb.adk", "DuckDBSyncADKStore"),
    "DuckDBSyncConfig": ("vertebra.extensions.sqlspec.adapters.duckdb.config", "DuckDBSyncConfig"),
    "DuckDBSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.duckdb.data_dictionary",
        "DuckDBSyncDataDictionary",
    ),
    "DuckDBSyncDriver": ("vertebra.extensions.sqlspec.adapters.duckdb.driver", "DuckDBSyncDriver"),
    "build_connection_config": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "build_connection_config"),
    "build_duckdb_copy_sql": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "build_duckdb_copy_sql"),
    "build_profile": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "build_profile"),
    "build_statement_config": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "build_statement_config"),
    "collect_rows": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "collect_rows"),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "default_statement_config"),
    "description_has_arrow_divergent_types": (
        "vertebra.extensions.sqlspec.adapters.duckdb.core",
        "description_has_arrow_divergent_types",
    ),
    "driver_profile": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "driver_profile"),
    "duckdb_copy_filename_follows_query": (
        "vertebra.extensions.sqlspec.adapters.duckdb.core",
        "duckdb_copy_filename_follows_query",
    ),
    "map_duckdb_exception": ("vertebra.extensions.sqlspec.adapters.duckdb.core", "map_duckdb_exception"),
    "order_duckdb_copy_parameters": (
        "vertebra.extensions.sqlspec.adapters.duckdb.core",
        "order_duckdb_copy_parameters",
    ),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_ATTRS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = target
    value = getattr(ensure_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
