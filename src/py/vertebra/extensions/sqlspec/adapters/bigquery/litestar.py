"""BigQuery Litestar session store backed by GoogleSQL ``MERGE`` and ``PARTITION BY DATE(expires_at)``."""

import base64
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

from vertebra.extensions.sqlspec.adapters.bigquery.core import (
    bigquery_partition_clauses,
    is_bigquery_table_missing,
    parse_utc_timestamp,
    positive_int_or_none,
    qualify_store_table,
)

__all__ = (
    "BigQueryLitestarConfig",
    "BigQueryLitestarStore",
)


class BigQueryLitestarConfig(TypedDict, total=False):
    """Typed extension configuration for BigQuery Litestar session stores."""

    session_table: str
    """Qualified BigQuery table name storing Litestar HTTP sessions."""
    with_partitioning: bool
    """Enable BigQuery time-based partitioning on the session table."""
    partition_expiration_days: int
    """Partition retention window in days when partitioning is enabled."""
    require_partition_filter: bool
    """Require partition filter predicates in generated session table DDL."""


def _coerce_session_bytes(value: Any) -> bytes:
    """Normalize GoogleSQL ``BYTES`` or base64-encoded string payloads into raw ``bytes``."""
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        try:
            return base64.b64decode(value, validate=True)
        except Exception:
            return value.encode("utf-8")
    return bytes(value)


class BigQueryLitestarStore(BaseSQLSpecStore[Any]):
    """BigQuery Litestar session store supporting both sync and async Vertebra configs."""

    __slots__ = ()

    adapter_name = "vertebra_bigquery"
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {
            "partition_expiration_days",
            "require_partition_filter",
            "with_partitioning",
        }
    )

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _qualified_table(self) -> str:
        return qualify_store_table(self._config, self._table_name)

    def _litestar_extension_config(self) -> dict[str, Any]:
        ext = getattr(self._config, "extension_config", {})
        if isinstance(ext, dict):
            litestar_cfg = ext.get("litestar", {})
            if isinstance(litestar_cfg, dict):
                return dict(litestar_cfg)
        return {}

    def _table_ddl(self) -> str:
        """Build the BigQuery ``CREATE TABLE IF NOT EXISTS`` DDL with optional date partitioning."""
        cfg = self._litestar_extension_config()
        with_partitioning = bool(cfg.get("with_partitioning", True))
        expiration_days = positive_int_or_none(cfg.get("partition_expiration_days"), "partition_expiration_days")
        require_filter = bool(cfg.get("require_partition_filter", False))
        partition_suffix, options_suffix = bigquery_partition_clauses(
            "expires_at",
            enabled=with_partitioning,
            expiration_days=expiration_days,
            require_filter=require_filter,
            cluster_by=("session_id",),
        )
        table_ref = self._qualified_table()
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} ("
            "session_id STRING NOT NULL, "
            "data BYTES NOT NULL, "
            "expires_at TIMESTAMP, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(), "
            "updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()"
            f"){partition_suffix}{options_suffix}"
        )

    def _drop_table_sql(self) -> list[str]:
        """Return idempotent BigQuery ``DROP TABLE IF EXISTS`` DDL statements."""
        return [f"DROP TABLE IF EXISTS {self._qualified_table()}"]

    def _get_sql(self) -> str:
        table_ref = self._qualified_table()
        return f"SELECT data, expires_at FROM {table_ref} WHERE session_id = @session_id LIMIT 1"

    def _merge_sql(self) -> str:
        table_ref = self._qualified_table()
        return (
            f"MERGE INTO {table_ref} AS target "
            "USING (SELECT @session_id AS session_id, @data AS data, @expires_at AS expires_at) AS source "
            "ON target.session_id = source.session_id "
            "WHEN MATCHED THEN UPDATE SET "
            "data = source.data, expires_at = source.expires_at, updated_at = CURRENT_TIMESTAMP() "
            "WHEN NOT MATCHED THEN INSERT (session_id, data, expires_at, created_at, updated_at) "
            "VALUES (source.session_id, source.data, source.expires_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
        )

    async def create_table(self) -> None:
        """Create the partitioned BigQuery Litestar session table if missing."""
        sql = self._table_ddl()
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await driver.execute(sql)
            return

        def _sync_create() -> None:
            with self._config.provide_session() as driver:
                driver.execute(sql)

        await async_(_sync_create)()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session bytes for ``key`` and optionally extend expiration."""
        now = datetime.now(UTC)
        new_expiry = self._calculate_expires_at(renew_for) if renew_for is not None else None
        select_sql = self._get_sql()
        update_sql = (
            f"UPDATE {self._qualified_table()} "
            "SET expires_at = @expires_at, updated_at = CURRENT_TIMESTAMP() "
            "WHERE session_id = @session_id"
        )
        delete_sql = f"DELETE FROM {self._qualified_table()} WHERE session_id = @session_id"

        if self._is_async_config():
            try:
                async with self._config.provide_session() as driver:
                    row = await driver.select_one_or_none(select_sql, {"session_id": key})
                    if row is None:
                        return None
                    expires_at = row.get("expires_at")
                    if expires_at is not None and parse_utc_timestamp(expires_at) <= now:
                        await driver.execute(delete_sql, {"session_id": key})
                        return None
                    if new_expiry is not None:
                        await driver.execute(
                            update_sql,
                            {"session_id": key, "expires_at": new_expiry.isoformat()},
                        )
                    return _coerce_session_bytes(row.get("data"))
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return None
                raise

        def _sync_get() -> bytes | None:
            try:
                with self._config.provide_session() as driver:
                    row = driver.select_one_or_none(select_sql, {"session_id": key})
                    if row is None:
                        return None
                    expires_at = row.get("expires_at")
                    if expires_at is not None and parse_utc_timestamp(expires_at) <= now:
                        driver.execute(delete_sql, {"session_id": key})
                        return None
                    if new_expiry is not None:
                        driver.execute(
                            update_sql,
                            {"session_id": key, "expires_at": new_expiry.isoformat()},
                        )
                    return _coerce_session_bytes(row.get("data"))
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return None
                raise

        return await async_(_sync_get)()

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Upsert a session entry using GoogleSQL ``MERGE``."""
        payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_at = self._calculate_expires_at(expires_in) if expires_in is not None else None
        params = {
            "session_id": key,
            "data": payload,
            "expires_at": expires_at.isoformat() if expires_at is not None else None,
        }
        sql = self._merge_sql()

        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await driver.execute(sql, params)
            return

        def _sync_set() -> None:
            with self._config.provide_session() as driver:
                driver.execute(sql, params)

        await async_(_sync_set)()

    async def delete(self, key: str) -> None:
        """Remove a session record by ``key``."""
        sql = f"DELETE FROM {self._qualified_table()} WHERE session_id = @session_id"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await driver.execute(sql, {"session_id": key})
            return

        def _sync_delete() -> None:
            with self._config.provide_session() as driver:
                driver.execute(sql, {"session_id": key})

        await async_(_sync_delete)()

    async def delete_all(self) -> None:
        """Delete all records from the BigQuery Litestar session table."""
        sql = f"DELETE FROM {self._qualified_table()} WHERE TRUE"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await driver.execute(sql)
            return

        def _sync_delete_all() -> None:
            with self._config.provide_session() as driver:
                driver.execute(sql)

        await async_(_sync_delete_all)()

    async def exists(self, key: str) -> bool:
        """Return True when ``key`` exists and has not expired."""
        sql = self._get_sql()
        now = datetime.now(UTC)

        if self._is_async_config():
            try:
                async with self._config.provide_session() as driver:
                    row = await driver.select_one_or_none(sql, {"session_id": key})
                    if row is None:
                        return False
                    expires_at = row.get("expires_at")
                    return expires_at is None or parse_utc_timestamp(expires_at) > now
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return False
                raise

        def _sync_exists() -> bool:
            try:
                with self._config.provide_session() as driver:
                    row = driver.select_one_or_none(sql, {"session_id": key})
                    if row is None:
                        return False
                    expires_at = row.get("expires_at")
                    return expires_at is None or parse_utc_timestamp(expires_at) > now
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return False
                raise

        return await async_(_sync_exists)()

    async def expires_in(self, key: str) -> int | None:
        """Return remaining lifetime in seconds for ``key``, or None if missing or permanent."""
        sql = f"SELECT expires_at FROM {self._qualified_table()} WHERE session_id = @session_id LIMIT 1"
        now = datetime.now(UTC)

        if self._is_async_config():
            try:
                async with self._config.provide_session() as driver:
                    row = await driver.select_one_or_none(sql, {"session_id": key})
                    if row is None or row.get("expires_at") is None:
                        return None
                    delta = parse_utc_timestamp(row["expires_at"]) - now
                    return max(0, int(delta.total_seconds()))
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return None
                raise

        def _sync_expires_in() -> int | None:
            try:
                with self._config.provide_session() as driver:
                    row = driver.select_one_or_none(sql, {"session_id": key})
                    if row is None or row.get("expires_at") is None:
                        return None
                    delta = parse_utc_timestamp(row["expires_at"]) - now
                    return max(0, int(delta.total_seconds()))
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return None
                raise

        return await async_(_sync_expires_in)()

    async def delete_expired(self) -> int:
        """Purge expired sessions and return deleted row count."""
        sql = f"DELETE FROM {self._qualified_table()} WHERE expires_at IS NOT NULL AND expires_at <= @now"
        now_iso = datetime.now(UTC).isoformat()

        if self._is_async_config():
            try:
                async with self._config.provide_session() as driver:
                    result = await driver.execute(sql, {"now": now_iso})
                    return int(getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0)
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return 0
                raise

        def _sync_delete_expired() -> int:
            try:
                with self._config.provide_session() as driver:
                    result = driver.execute(sql, {"now": now_iso})
                    return int(getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0)
            except Exception as exc:
                if is_bigquery_table_missing(exc):
                    return 0
                raise

        return await async_(_sync_delete_expired)()
