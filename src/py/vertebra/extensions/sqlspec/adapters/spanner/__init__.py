"""Lazy PEP 562 public exports for the Vertebra Cloud Spanner SQLSpec adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.spanner._typing import (
        SpannerAsyncConnectionContext,
        SpannerAsyncSessionContext,
        SpannerConnectionParams,
        SpannerDriverFeatures,
        SpannerPoolParams,
        SpannerSyncConnectionContext,
        SpannerSyncSessionContext,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.adk import (
        SpannerADKConfig,
        SpannerAsyncADKArtifactStore,
        SpannerAsyncADKMemoryStore,
        SpannerAsyncADKStore,
        SpannerSyncADKArtifactStore,
        SpannerSyncADKMemoryStore,
        SpannerSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.config import (
        SpangresAsyncConfig,
        SpangresSyncConfig,
        SpannerAsyncConfig,
        SpannerSyncConfig,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.core import (
        ADAPTER_NAME,
        DEFAULT_SPANNER_SCOPES,
        SpannerConnectionBatch,
        SpannerConnectionShim,
        default_statement_config,
        encode_spanner_batch_parameters,
        encode_spanner_parameters,
        encode_typed_spanner_value,
        farm_fingerprint_shard,
        is_spanner_table_missing,
        map_spanner_exception,
        normalize_spanner_dialect,
        object_to_artifact_reference,
        parse_utc_timestamp,
        positive_int_or_none,
        resolve_spanner_config,
        resolve_storage_backend,
        sqlspec_dialect_name,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.data_dictionary import (
        SpannerAsyncDataDictionary,
        SpannerSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.driver import (
        SpannerAsyncDriver,
        SpannerSyncDriver,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.events import (
        SpannerEventQueueStore,
        SpannerEventsConfig,
    )
    from vertebra.extensions.sqlspec.adapters.spanner.litestar import (
        SpannerLitestarConfig,
        SpannerLitestarStore,
    )

__all__ = (
    "ADAPTER_NAME",
    "DEFAULT_SPANNER_SCOPES",
    "SpangresAsyncConfig",
    "SpangresSyncConfig",
    "SpannerADKConfig",
    "SpannerAsyncADKArtifactStore",
    "SpannerAsyncADKMemoryStore",
    "SpannerAsyncADKStore",
    "SpannerAsyncConfig",
    "SpannerAsyncConnectionContext",
    "SpannerAsyncDataDictionary",
    "SpannerAsyncDriver",
    "SpannerAsyncSessionContext",
    "SpannerConnectionBatch",
    "SpannerConnectionParams",
    "SpannerConnectionShim",
    "SpannerDriverFeatures",
    "SpannerEventQueueStore",
    "SpannerEventsConfig",
    "SpannerLitestarConfig",
    "SpannerLitestarStore",
    "SpannerPoolParams",
    "SpannerSyncADKArtifactStore",
    "SpannerSyncADKMemoryStore",
    "SpannerSyncADKStore",
    "SpannerSyncConfig",
    "SpannerSyncConnectionContext",
    "SpannerSyncDataDictionary",
    "SpannerSyncDriver",
    "SpannerSyncSessionContext",
    "default_statement_config",
    "encode_spanner_batch_parameters",
    "encode_spanner_parameters",
    "encode_typed_spanner_value",
    "farm_fingerprint_shard",
    "is_spanner_table_missing",
    "map_spanner_exception",
    "normalize_spanner_dialect",
    "object_to_artifact_reference",
    "parse_utc_timestamp",
    "positive_int_or_none",
    "resolve_spanner_config",
    "resolve_storage_backend",
    "sqlspec_dialect_name",
)

_EXPORT_MAP: Final[dict[str, tuple[str, str]]] = {
    "ADAPTER_NAME": ("vertebra.extensions.sqlspec.adapters.spanner.core", "ADAPTER_NAME"),
    "DEFAULT_SPANNER_SCOPES": ("vertebra.extensions.sqlspec.adapters.spanner.core", "DEFAULT_SPANNER_SCOPES"),
    "SpangresAsyncConfig": ("vertebra.extensions.sqlspec.adapters.spanner.config", "SpangresAsyncConfig"),
    "SpangresSyncConfig": ("vertebra.extensions.sqlspec.adapters.spanner.config", "SpangresSyncConfig"),
    "SpannerADKConfig": ("vertebra.extensions.sqlspec.adapters.spanner.adk", "SpannerADKConfig"),
    "SpannerAsyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerAsyncADKArtifactStore",
    ),
    "SpannerAsyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerAsyncADKMemoryStore",
    ),
    "SpannerAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.spanner.adk", "SpannerAsyncADKStore"),
    "SpannerAsyncConfig": ("vertebra.extensions.sqlspec.adapters.spanner.config", "SpannerAsyncConfig"),
    "SpannerAsyncConnectionContext": (
        "vertebra.extensions.sqlspec.adapters.spanner._typing",
        "SpannerAsyncConnectionContext",
    ),
    "SpannerAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.spanner.data_dictionary",
        "SpannerAsyncDataDictionary",
    ),
    "SpannerAsyncDriver": ("vertebra.extensions.sqlspec.adapters.spanner.driver", "SpannerAsyncDriver"),
    "SpannerAsyncSessionContext": (
        "vertebra.extensions.sqlspec.adapters.spanner._typing",
        "SpannerAsyncSessionContext",
    ),
    "SpannerConnectionBatch": ("vertebra.extensions.sqlspec.adapters.spanner.core", "SpannerConnectionBatch"),
    "SpannerConnectionParams": ("vertebra.extensions.sqlspec.adapters.spanner._typing", "SpannerConnectionParams"),
    "SpannerConnectionShim": ("vertebra.extensions.sqlspec.adapters.spanner.core", "SpannerConnectionShim"),
    "SpannerDriverFeatures": ("vertebra.extensions.sqlspec.adapters.spanner._typing", "SpannerDriverFeatures"),
    "SpannerEventQueueStore": ("vertebra.extensions.sqlspec.adapters.spanner.events", "SpannerEventQueueStore"),
    "SpannerEventsConfig": ("vertebra.extensions.sqlspec.adapters.spanner.events", "SpannerEventsConfig"),
    "SpannerLitestarConfig": ("vertebra.extensions.sqlspec.adapters.spanner.litestar", "SpannerLitestarConfig"),
    "SpannerLitestarStore": ("vertebra.extensions.sqlspec.adapters.spanner.litestar", "SpannerLitestarStore"),
    "SpannerPoolParams": ("vertebra.extensions.sqlspec.adapters.spanner._typing", "SpannerPoolParams"),
    "SpannerSyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerSyncADKArtifactStore",
    ),
    "SpannerSyncADKMemoryStore": (
        "vertebra.extensions.sqlspec.adapters.spanner.adk",
        "SpannerSyncADKMemoryStore",
    ),
    "SpannerSyncADKStore": ("vertebra.extensions.sqlspec.adapters.spanner.adk", "SpannerSyncADKStore"),
    "SpannerSyncConfig": ("vertebra.extensions.sqlspec.adapters.spanner.config", "SpannerSyncConfig"),
    "SpannerSyncConnectionContext": (
        "vertebra.extensions.sqlspec.adapters.spanner._typing",
        "SpannerSyncConnectionContext",
    ),
    "SpannerSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.spanner.data_dictionary",
        "SpannerSyncDataDictionary",
    ),
    "SpannerSyncDriver": ("vertebra.extensions.sqlspec.adapters.spanner.driver", "SpannerSyncDriver"),
    "SpannerSyncSessionContext": (
        "vertebra.extensions.sqlspec.adapters.spanner._typing",
        "SpannerSyncSessionContext",
    ),
    "default_statement_config": ("vertebra.extensions.sqlspec.adapters.spanner.core", "default_statement_config"),
    "encode_spanner_batch_parameters": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "encode_spanner_batch_parameters",
    ),
    "encode_spanner_parameters": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "encode_spanner_parameters",
    ),
    "encode_typed_spanner_value": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "encode_typed_spanner_value",
    ),
    "farm_fingerprint_shard": ("vertebra.extensions.sqlspec.adapters.spanner.core", "farm_fingerprint_shard"),
    "is_spanner_table_missing": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "is_spanner_table_missing",
    ),
    "map_spanner_exception": ("vertebra.extensions.sqlspec.adapters.spanner.core", "map_spanner_exception"),
    "normalize_spanner_dialect": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "normalize_spanner_dialect",
    ),
    "object_to_artifact_reference": (
        "vertebra.extensions.sqlspec.adapters.spanner.core",
        "object_to_artifact_reference",
    ),
    "parse_utc_timestamp": ("vertebra.extensions.sqlspec.adapters.spanner.core", "parse_utc_timestamp"),
    "positive_int_or_none": ("vertebra.extensions.sqlspec.adapters.spanner.core", "positive_int_or_none"),
    "resolve_spanner_config": ("vertebra.extensions.sqlspec.adapters.spanner.core", "resolve_spanner_config"),
    "resolve_storage_backend": ("vertebra.extensions.sqlspec.adapters.spanner.core", "resolve_storage_backend"),
    "sqlspec_dialect_name": ("vertebra.extensions.sqlspec.adapters.spanner.core", "sqlspec_dialect_name"),
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
