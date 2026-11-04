"""Pure-Rust Google Cloud BigQuery sync and async drivers, zero-copy Arrow Storage APIs, and Object Tables."""

from __future__ import annotations

from vertebra.db.adapters.bigquery.config import (
    BIGQUERY_DIALECT_TAG,
    DEFAULT_BIGQUERY_REST_ENDPOINT,
    DEFAULT_BIGQUERY_STORAGE_ENDPOINT,
    BigQueryConfig,
    BigQueryDryRunResult,
    ObjectRef,
    ObjectTableConfig,
)
from vertebra.db.adapters.bigquery.core import BigQueryCore
from vertebra.db.adapters.bigquery.driver import (
    AsyncBigQueryDriver,
    SyncBigQueryDriver,
)

__all__ = (
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
