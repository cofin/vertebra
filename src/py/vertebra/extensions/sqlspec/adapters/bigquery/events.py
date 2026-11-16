"""BigQuery partitioned and clustered SQLSpec event queue store."""

from typing import Any, TypedDict

from sqlspec.extensions.events import BaseEventQueueStore

from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    positive_int_or_none,
    qualify_store_table,
    quote_bigquery_table,
)

__all__ = (
    "BigQueryEventQueueStore",
    "BigQueryEventsConfig",
)


class BigQueryEventsConfig(TypedDict, total=False):
    """Typed extension configuration for BigQuery partitioned event queues."""

    queue_table: str
    """Qualified BigQuery table name for the persisted event queue."""
    lease_seconds: int
    """Default visibility timeout in seconds for leased event rows."""
    retention_seconds: int
    """Retention window in seconds before acknowledged events expire."""
    poll_interval: float
    """Polling interval in seconds when waiting for queued events."""
    event_poll_interval: float
    """Interval in seconds between change-stream or queue poll iterations."""
    with_partitioning: bool
    """Enable BigQuery DATE partitioning on ``available_at``."""
    partition_expiration_days: int
    """Partition retention window in days when partitioning is enabled."""
    require_partition_filter: bool
    """Require partition filter predicates in generated event queue DDL."""


class BigQueryEventQueueStore(BaseEventQueueStore[Any]):
    """GoogleSQL event queue DDL store using ``DATE(available_at)`` partitioning and clustering."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"
    extension_config_options = BaseEventQueueStore.extension_config_options | frozenset(
        {
            "partition_expiration_days",
            "require_partition_filter",
            "with_partitioning",
        }
    )

    def _events_extension_config(self) -> dict[str, Any]:
        ext = getattr(self._config, "extension_config", {})
        if isinstance(ext, dict):
            events_cfg = ext.get("events", {})
            if isinstance(events_cfg, dict):
                return dict(events_cfg)
        return {}

    def _qualified_table(self) -> str:
        return qualify_store_table(self._config, self.table_name)

    def _column_types(self) -> tuple[str, str, str]:
        """Return ``(payload_type, metadata_type, timestamp_type)`` for GoogleSQL event tables."""
        return "JSON", "JSON", "TIMESTAMP"

    def _string_type(self, length: int) -> str:
        """Return the GoogleSQL variable-length ``STRING`` type."""
        _ = length
        return "STRING"

    def _integer_type(self) -> str:
        """Return the GoogleSQL 64-bit integer type."""
        return "INT64"

    def _timestamp_default(self) -> str:
        """Return the GoogleSQL UTC current timestamp expression."""
        return "CURRENT_TIMESTAMP()"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Attach BigQuery partitioning and clustering clauses to table DDL."""
        _ = object_type
        return statement

    def create_statements(self) -> list[str]:
        """Return idempotent BigQuery partitioned and clustered ``CREATE TABLE`` DDL statements."""
        cfg = self._events_extension_config()
        with_partitioning = bool(cfg.get("with_partitioning", True))
        expiration_days = positive_int_or_none(cfg.get("partition_expiration_days"), "partition_expiration_days")
        require_filter = bool(cfg.get("require_partition_filter", False))
        partition_suffix, options_suffix = bigquery_partition_clauses(
            "available_at",
            enabled=with_partitioning,
            expiration_days=expiration_days,
            require_filter=require_filter,
            cluster_by=("channel", "status", "available_at"),
        )
        table_ref = self._qualified_table()
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "event_id STRING NOT NULL, "
            "channel STRING NOT NULL, "
            "payload_json JSON NOT NULL, "
            "metadata_json JSON, "
            "status STRING NOT NULL DEFAULT 'pending', "
            "available_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
            "lease_expires_at TIMESTAMP, "
            "attempts INT64 NOT NULL DEFAULT 0, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
            f"){partition_suffix}{options_suffix}"
        )
        return [ddl]

    def drop_statements(self) -> list[str]:
        """Return idempotent BigQuery ``DROP TABLE IF EXISTS`` DDL statements."""
        table_ref = (
            self._qualified_table()
            if getattr(self._config, "project_id", None) or getattr(self._config, "dataset_id", None)
            else quote_bigquery_table(self.table_name)
        )
        return [f"DROP TABLE IF EXISTS {table_ref}"]
