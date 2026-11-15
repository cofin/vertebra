"""Pure-Rust Google Cloud BigQuery sync and async drivers, zero-copy Arrow Storage APIs, and Object Tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.adapters.bigquery.config import (
        BIGQUERY_DIALECT_TAG,
        DEFAULT_BIGQUERY_REST_ENDPOINT,
        DEFAULT_BIGQUERY_STORAGE_ENDPOINT,
        BigQueryConfig,
        BigQueryDryRunResult,
    )
    from vertebra.db.adapters.bigquery.core import BigQueryCore
    from vertebra.db.adapters.bigquery.driver import (
        AsyncBigQueryDriver,
        SyncBigQueryDriver,
    )
    from vertebra.db.adapters.bigquery.embeddings import BigQueryEmbeddingPushdown
    from vertebra.db.adapters.bigquery.object_tables import (
        ObjectRef,
        ObjectTableConfig,
    )
    from vertebra.db.adapters.bigquery.tools import BigQueryToolProvider

__all__ = (
    "BigQueryToolProvider",
    "BigQueryEmbeddingPushdown",
    "BIGQUERY_DIALECT_TAG",
    "DEFAULT_BIGQUERY_REST_ENDPOINT",
    "DEFAULT_BIGQUERY_STORAGE_ENDPOINT",
    "AsyncBigQueryDriver",
    "BigQueryConfig",
    "BigQueryCore",
    "BigQueryDryRunResult",
    "ObjectRef",
    "ObjectTableConfig",
    "SyncBigQueryDriver",
)

__lazy_modules__ = [
    "vertebra.db.adapters.bigquery.tools",
    "vertebra.db.adapters.bigquery.embeddings",
    "vertebra.db.adapters.bigquery.config",
    "vertebra.db.adapters.bigquery.core",
    "vertebra.db.adapters.bigquery.object_tables",
    "vertebra.db.adapters.bigquery.driver",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "BigQueryToolProvider": ("vertebra.db.adapters.bigquery.tools", "BigQueryToolProvider"),
    "BigQueryEmbeddingPushdown": ("vertebra.db.adapters.bigquery.embeddings", "BigQueryEmbeddingPushdown"),
    "BIGQUERY_DIALECT_TAG": ("vertebra.db.adapters.bigquery.config", "BIGQUERY_DIALECT_TAG"),
    "DEFAULT_BIGQUERY_REST_ENDPOINT": ("vertebra.db.adapters.bigquery.config", "DEFAULT_BIGQUERY_REST_ENDPOINT"),
    "DEFAULT_BIGQUERY_STORAGE_ENDPOINT": (
        "vertebra.db.adapters.bigquery.config",
        "DEFAULT_BIGQUERY_STORAGE_ENDPOINT",
    ),
    "AsyncBigQueryDriver": ("vertebra.db.adapters.bigquery.driver", "AsyncBigQueryDriver"),
    "BigQueryConfig": ("vertebra.db.adapters.bigquery.config", "BigQueryConfig"),
    "BigQueryCore": ("vertebra.db.adapters.bigquery.core", "BigQueryCore"),
    "BigQueryDryRunResult": ("vertebra.db.adapters.bigquery.config", "BigQueryDryRunResult"),
    "ObjectRef": ("vertebra.db.adapters.bigquery.object_tables", "ObjectRef"),
    "ObjectTableConfig": ("vertebra.db.adapters.bigquery.object_tables", "ObjectTableConfig"),
    "SyncBigQueryDriver": ("vertebra.db.adapters.bigquery.driver", "SyncBigQueryDriver"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
