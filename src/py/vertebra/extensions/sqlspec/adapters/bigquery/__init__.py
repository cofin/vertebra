"""Lazy PEP 562 public exports for the Vertebra BigQuery SQLSpec adapter."""

from typing import TYPE_CHECKING, Any, Final

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.bigquery.adk import (
        BigQueryADKConfig,
        BigQueryAsyncADKArtifactStore,
        BigQueryAsyncADKMemoryStore,
        BigQueryAsyncADKStore,
        BigQuerySyncADKArtifactStore,
        BigQuerySyncADKMemoryStore,
        BigQuerySyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.config import (
        BigQueryAsyncConfig,
        BigQuerySyncConfig,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.core import (
        BIGQUERY_STATEMENT_CONFIG,
        BigQueryConnectionQueryJob,
        BigQueryConnectionShim,
        build_profile,
        build_statement_config,
        default_statement_config,
        driver_profile,
        map_bigquery_exception,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary import (
        BigQueryAsyncDataDictionary,
        BigQueryMetadataScope,
        BigQuerySyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.driver import (
        BigQueryAsyncDriver,
        BigQuerySyncDriver,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.events import (
        BigQueryEventQueueStore,
        BigQueryEventsConfig,
    )
    from vertebra.extensions.sqlspec.adapters.bigquery.litestar import (
        BigQueryLitestarConfig,
        BigQueryLitestarStore,
    )

__all__ = (
    "BIGQUERY_STATEMENT_CONFIG",
    "BigQueryADKConfig",
    "BigQueryAsyncADKArtifactStore",
    "BigQueryAsyncADKMemoryStore",
    "BigQueryAsyncADKStore",
    "BigQueryAsyncConfig",
    "BigQueryAsyncDataDictionary",
    "BigQueryAsyncDriver",
    "BigQueryConnectionQueryJob",
    "BigQueryConnectionShim",
    "BigQueryEventQueueStore",
    "BigQueryEventsConfig",
    "BigQueryLitestarConfig",
    "BigQueryLitestarStore",
    "BigQueryMetadataScope",
    "BigQuerySyncADKArtifactStore",
    "BigQuerySyncADKMemoryStore",
    "BigQuerySyncADKStore",
    "BigQuerySyncConfig",
    "BigQuerySyncDataDictionary",
    "BigQuerySyncDriver",
    "build_profile",
    "build_statement_config",
    "default_statement_config",
    "driver_profile",
    "map_bigquery_exception",
)

_EXPORT_MAP: Final[dict[str, tuple[str, str]]] = {
    "BIGQUERY_STATEMENT_CONFIG": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "BIGQUERY_STATEMENT_CONFIG"),
    "BigQueryADKConfig": ("vertebra.extensions.sqlspec.adapters.bigquery.adk", "BigQueryADKConfig"),
    "BigQueryAsyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQueryAsyncADKArtifactStore",
    ),
    "BigQueryAsyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQueryAsyncADKMemoryStore",
    ),
    "BigQueryAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.bigquery.adk", "BigQueryAsyncADKStore"),
    "BigQueryAsyncConfig": ("vertebra.extensions.sqlspec.adapters.bigquery.config", "BigQueryAsyncConfig"),
    "BigQueryAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary",
        "BigQueryAsyncDataDictionary",
    ),
    "BigQueryAsyncDriver": ("vertebra.extensions.sqlspec.adapters.bigquery.driver", "BigQueryAsyncDriver"),
    "BigQueryConnectionQueryJob": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "BigQueryConnectionQueryJob"),
    "BigQueryConnectionShim": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "BigQueryConnectionShim"),
    "map_bigquery_exception": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "map_bigquery_exception"),
    "BigQueryEventQueueStore": ("vertebra.extensions.sqlspec.adapters.bigquery.events", "BigQueryEventQueueStore"),
    "BigQueryEventsConfig": ("vertebra.extensions.sqlspec.adapters.bigquery.events", "BigQueryEventsConfig"),
    "BigQueryLitestarConfig": ("vertebra.extensions.sqlspec.adapters.bigquery.litestar", "BigQueryLitestarConfig"),
    "BigQueryLitestarStore": ("vertebra.extensions.sqlspec.adapters.bigquery.litestar", "BigQueryLitestarStore"),
    "BigQueryMetadataScope": (
        "vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary",
        "BigQueryMetadataScope",
    ),
    "BigQuerySyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQuerySyncADKArtifactStore",
    ),
    "BigQuerySyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.bigquery.adk",
        "BigQuerySyncADKMemoryStore",
    ),
    "BigQuerySyncADKStore": ("vertebra.extensions.sqlspec.adapters.bigquery.adk", "BigQuerySyncADKStore"),
    "BigQuerySyncConfig": ("vertebra.extensions.sqlspec.adapters.bigquery.config", "BigQuerySyncConfig"),
    "BigQuerySyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary",
        "BigQuerySyncDataDictionary",
    ),
    "BigQuerySyncDriver": ("vertebra.extensions.sqlspec.adapters.bigquery.driver", "BigQuerySyncDriver"),
    "build_profile": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "build_profile"),
    "build_statement_config": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "build_statement_config"),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "default_statement_config"),
    "driver_profile": ("vertebra.extensions.sqlspec.adapters.bigquery.core", "driver_profile"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr_name = target
    value = getattr(ensure_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
