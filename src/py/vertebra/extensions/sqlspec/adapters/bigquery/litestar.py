"""BigQuery Litestar session store backed by GoogleSQL ``MERGE`` and ``PARTITION BY DATE(expires_at)``."""

from __future__ import annotations

import base64
from datetime import datetime
from typing import Any, ClassVar, NotRequired

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import SchemaTarget

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    is_bigquery_table_missing,
    parse_utc_timestamp,
    positive_int_or_none,
    qualify_store_table,
    quote_bigquery_table,
)

__all__ = (
    "BigQueryLitestarConfig",
    "BigQueryLitestarStore",
)


class BigQueryLitestarConfig(LitestarConfig):
    """Typed extension configuration for BigQuery Litestar session stores."""

    with_partitioning: NotRequired[bool]
    """Enable BigQuery time-based partitioning on the session table."""
    partition_expiration_days: NotRequired[int]
    """Partition retention window in days when partitioning is enabled."""
    require_partition_filter: NotRequired[bool]
    """Require partition filter predicates in generated session table DDL."""


class BigQueryLitestarStore(AdapterLitestarStore):
    """BigQuery Litestar session store supporting both sync and async Vertebra configs."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"
    _dialect: ClassVar[str] = "bigquery"
    _commit_after_write: ClassVar[bool] = False
    _create_uses_script: ClassVar[bool] = False
    _check_create_schema_enabled: ClassVar[bool] = False
    _check_expiry_in_python: ClassVar[bool] = True
    _log_writes: ClassVar[bool] = False
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"partition_expiration_days", "require_partition_filter", "with_partitioning"}
    )

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        quote_bigquery_table(table_name)

    def _qualified_table(self) -> str:
        return qualify_store_table(self._config, self._table_name)

    def _litestar_extension_config(self) -> dict[str, Any]:
        ext = getattr(self._config, "extension_config", {})
        if isinstance(ext, dict):
            litestar_cfg = ext.get("litestar", {})
            if isinstance(litestar_cfg, dict):
                return dict(litestar_cfg)
        return {}

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return dataset-qualified ``SchemaTarget`` definitions for the BigQuery session table."""
        return [SchemaTarget.from_ddl(self._qualified_table(), self._table_ddl(), dialect=self._dialect)]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return ``[]`` because BigQuery clusters sessions inline instead of using secondary B-tree indexes."""
        return []

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """No-op companion reconciliation because BigQuery declares clustering in table DDL."""
        _ = driver

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """No-op async companion reconciliation because BigQuery declares clustering in table DDL."""
        _ = driver

    def _table_ddl(self) -> str:
        cfg = self._litestar_extension_config()
        partition_suffix, options_suffix = bigquery_partition_clauses(
            "expires_at",
            enabled=bool(cfg.get("with_partitioning", True)),
            expiration_days=positive_int_or_none(cfg.get("partition_expiration_days"), "partition_expiration_days"),
            require_filter=bool(cfg.get("require_partition_filter", False)),
            cluster_by=("session_id",),
        )
        return (
            f"CREATE TABLE IF NOT EXISTS {self._qualified_table()} ("
            "session_id STRING NOT NULL, "
            "data BYTES NOT NULL, "
            "expires_at TIMESTAMP, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
            "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
            f"){partition_suffix}{options_suffix}"
        )

    def _drop_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._qualified_table()}"]

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_bigquery_table_missing(exc)

    def _coerce_bytes(self, value: Any) -> bytes:
        if isinstance(value, str):
            try:
                return base64.b64decode(value, validate=True)
            except Exception:
                return value.encode("utf-8")
        return super()._coerce_bytes(value)

    def _encode_expires_at(self, expires_at: datetime | None) -> str | None:
        return None if expires_at is None else expires_at.isoformat()

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else parse_utc_timestamp(raw)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return (
            f"SELECT data, expires_at FROM {self._qualified_table()} WHERE session_id = @session_id LIMIT 1",
            ({"session_id": key},),
        )

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"UPDATE {self._qualified_table()} "
            "SET expires_at = @expires_at, updated_at = CURRENT_TIMESTAMP() "
            "WHERE session_id = @session_id"
        )
        return sql, ({"session_id": key, "expires_at": encoded_expires_at},)

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        table_ref = self._qualified_table()
        sql = (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @session_id AS session_id, @data AS data, @expires_at AS expires_at) AS source "
            "ON target.session_id = source.session_id "
            "WHEN MATCHED THEN UPDATE SET "
            "data = source.data, expires_at = source.expires_at, updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at, created_at, updated_at) "
            "VALUES (source.session_id, source.data, source.expires_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
        )
        return sql, ({"session_id": key, "data": data, "expires_at": encoded_expires_at},)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._qualified_table()} WHERE session_id = @session_id", ({"session_id": key},)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._qualified_table()} WHERE TRUE", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return (
            f"SELECT expires_at FROM {self._qualified_table()} WHERE session_id = @session_id LIMIT 1",
            ({"session_id": key},),
        )

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return (
            f"SELECT expires_at FROM {self._qualified_table()} WHERE session_id = @session_id LIMIT 1",
            ({"session_id": key},),
        )

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        return (
            f"DELETE FROM {self._qualified_table()} WHERE expires_at IS NOT NULL AND expires_at <= @now",
            ({"now": now.isoformat()},),
        )
