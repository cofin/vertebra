"""Cloud Spanner hash-sharded Litestar session store supporting GoogleSQL and Spangres TTL."""

from datetime import UTC, datetime, timedelta
from typing import Any, Literal, TypedDict

from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    farm_fingerprint_shard,
    is_spanner_table_missing,
    normalize_spanner_dialect,
    parse_utc_timestamp,
    validate_spanner_identifier,
)

__all__ = (
    "SpannerLitestarConfig",
    "SpannerLitestarStore",
)


class SpannerLitestarConfig(TypedDict, total=False):
    """Typed extension configuration keys supported by ``SpannerLitestarStore``."""

    commit_mode: str
    """Transaction commit mode used for Litestar session writes."""
    connection_key: str
    """Dependency injection key for database connections."""
    create_schema: bool
    """Create the Litestar session table when it does not already exist."""
    manage_schema: bool
    """Create or migrate the session table automatically on startup."""
    migrations_path: str
    """Filesystem path to custom session table migrations."""
    pool_key: str
    """Dependency injection key for database connection pools."""
    session_key: str
    """Dependency injection key for database session drivers."""
    session_table: str
    """Cloud Spanner table name storing Litestar HTTP sessions."""


def _coerce_session_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


class SpannerLitestarStore(BaseSQLSpecStore[Any]):
    """Cloud Spanner Litestar session store with ``FARM_FINGERPRINT`` sharding and ``expires_at`` TTL."""

    __slots__ = ("_shard_count",)

    adapter_name = "vertebra_spanner"

    def __init__(self, config: Any) -> None:
        super().__init__(config)
        self._shard_count = 16

    def _dialect(self) -> Literal["googlesql", "spangres"]:
        return normalize_spanner_dialect(
            getattr(self._config, "spanner_dialect", None) or getattr(self._config, "dialect", "googlesql")
        )

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _validated_table(self) -> str:
        return validate_spanner_identifier(self.table_name, "session_table")

    def compute_shard_id(self, key: str) -> int:
        """Compute the deterministic hash shard prefix for session ``key``."""
        return farm_fingerprint_shard(key, self._shard_count)

    def _table_ddl(self) -> str:
        table = self._validated_table()
        if self._dialect() == "spangres":
            return (
                f"CREATE TABLE IF NOT EXISTS {table} ("
                "shard_id BIGINT NOT NULL, "
                "session_id VARCHAR(256) NOT NULL, "
                "data BYTEA NOT NULL, "
                "expires_at TIMESTAMPTZ, "
                "PRIMARY KEY (shard_id, session_id)"
                ") TTL INTERVAL '0 days' ON expires_at"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"shard_id INT64 NOT NULL DEFAULT (MOD(ABS(FARM_FINGERPRINT(session_id)), {self._shard_count})), "
            "session_id STRING(256) NOT NULL, "
            "data BYTES(MAX) NOT NULL, "
            "expires_at TIMESTAMP"
            ") PRIMARY KEY (shard_id, session_id), "
            "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))"
        )

    def _drop_table_sql(self) -> list[str]:
        return [f"DROP TABLE IF EXISTS {self._validated_table()}"]

    def _merge_sql(self) -> str:
        table = self._validated_table()
        if self._dialect() == "spangres":
            return (
                f"INSERT INTO {table} (shard_id, session_id, data, expires_at) "
                "VALUES (@shard_id, @session_id, @data, @expires_at) "
                "ON CONFLICT (shard_id, session_id) DO UPDATE "
                "SET data = EXCLUDED.data, expires_at = EXCLUDED.expires_at"
            )
        return (
            f"MERGE INTO {table} AS target "
            "USING (SELECT @shard_id AS shard_id, @session_id AS session_id, "
            "@data AS data, @expires_at AS expires_at) AS source "
            "ON target.shard_id = source.shard_id AND target.session_id = source.session_id "
            "WHEN MATCHED THEN UPDATE SET data = source.data, expires_at = source.expires_at "
            "WHEN NOT MATCHED THEN INSERT (shard_id, session_id, data, expires_at) "
            "VALUES (source.shard_id, source.session_id, source.data, source.expires_at)"
        )

    async def create_table(self) -> None:
        """Create the Litestar session table in Cloud Spanner."""
        ddl = self._table_ddl()
        if self._is_async_config():
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(ddl)
            return

        def _sync_create() -> None:
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(ddl)

        await async_(_sync_create)()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session bytes for ``key`` via a lock-free snapshot unless expired."""
        table = self._validated_table()
        shard_id = self.compute_shard_id(key)
        now = datetime.now(UTC)
        new_expiry = self._calculate_expires_at(renew_for) if renew_for is not None else None
        select_sql = (
            f"SELECT data, expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1"
        )
        point_params = {"shard_id": shard_id, "session_id": key}

        if self._is_async_config():
            try:
                async with self._config.provide_session(transaction=False) as driver:
                    row = await driver.select_one_or_none(select_sql, point_params)
                if row is None:
                    return None
                expires_at = row.get("expires_at")
                if expires_at is not None and parse_utc_timestamp(expires_at) <= now:
                    async with self._config.provide_session(transaction=True) as w_driver:
                        await w_driver.execute(
                            f"DELETE FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id",
                            point_params,
                        )
                    return None
                if new_expiry is not None:
                    async with self._config.provide_session(transaction=True) as w_driver:
                        await w_driver.execute(
                            f"UPDATE {table} SET expires_at = @expires_at "
                            "WHERE shard_id = @shard_id AND session_id = @session_id",
                            {
                                "shard_id": shard_id,
                                "session_id": key,
                                "expires_at": new_expiry.isoformat(),
                            },
                        )
                return _coerce_session_bytes(row.get("data"))
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return None
                raise

        def _sync_get() -> bytes | None:
            try:
                with self._config.provide_session(transaction=False) as driver:
                    row = driver.select_one_or_none(select_sql, point_params)
                if row is None:
                    return None
                expires_at = row.get("expires_at")
                if expires_at is not None and parse_utc_timestamp(expires_at) <= now:
                    with self._config.provide_session(transaction=True) as w_driver:
                        w_driver.execute(
                            f"DELETE FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id",
                            point_params,
                        )
                    return None
                if new_expiry is not None:
                    with self._config.provide_session(transaction=True) as w_driver:
                        w_driver.execute(
                            f"UPDATE {table} SET expires_at = @expires_at "
                            "WHERE shard_id = @shard_id AND session_id = @session_id",
                            {
                                "shard_id": shard_id,
                                "session_id": key,
                                "expires_at": new_expiry.isoformat(),
                            },
                        )
                return _coerce_session_bytes(row.get("data"))
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return None
                raise

        return await async_(_sync_get)()

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Persist session bytes for ``key`` inside a ``ReadWrite`` transaction."""
        payload = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_at = self._calculate_expires_at(expires_in) if expires_in is not None else None
        params = {
            "shard_id": self.compute_shard_id(key),
            "session_id": key,
            "data": payload,
            "expires_at": expires_at.isoformat() if expires_at is not None else None,
        }
        sql = self._merge_sql()

        if self._is_async_config():
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(sql, params)
            return

        def _sync_set() -> None:
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(sql, params)

        await async_(_sync_set)()

    async def delete(self, key: str) -> None:
        """Delete the session row matching ``(shard_id, key)``."""
        table = self._validated_table()
        sql = f"DELETE FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id"
        params = {"shard_id": self.compute_shard_id(key), "session_id": key}

        if self._is_async_config():
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(sql, params)
            return

        def _sync_delete() -> None:
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(sql, params)

        await async_(_sync_delete)()

    async def delete_all(self) -> None:
        """Delete all rows from the Spanner Litestar session table."""
        table = self._validated_table()
        sql = f"DELETE FROM {table} WHERE TRUE"
        if self._is_async_config():
            async with self._config.provide_session(transaction=True) as driver:
                await driver.execute(sql)
            return

        def _sync_delete_all() -> None:
            with self._config.provide_session(transaction=True) as driver:
                driver.execute(sql)

        await async_(_sync_delete_all)()

    async def exists(self, key: str) -> bool:
        """Return ``True`` if ``key`` exists and has not expired via a lock-free snapshot."""
        table = self._validated_table()
        sql = f"SELECT expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1"
        params = {"shard_id": self.compute_shard_id(key), "session_id": key}
        now = datetime.now(UTC)

        if self._is_async_config():
            try:
                async with self._config.provide_session(transaction=False) as driver:
                    row = await driver.select_one_or_none(sql, params)
                if row is None:
                    return False
                expires_at = row.get("expires_at")
                return expires_at is None or parse_utc_timestamp(expires_at) > now
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return False
                raise

        def _sync_exists() -> bool:
            try:
                with self._config.provide_session(transaction=False) as driver:
                    row = driver.select_one_or_none(sql, params)
                if row is None:
                    return False
                expires_at = row.get("expires_at")
                return expires_at is None or parse_utc_timestamp(expires_at) > now
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return False
                raise

        return await async_(_sync_exists)()

    async def expires_in(self, key: str) -> int | None:
        """Return remaining lifetime in seconds for ``key`` via a lock-free snapshot."""
        table = self._validated_table()
        sql = f"SELECT expires_at FROM {table} WHERE shard_id = @shard_id AND session_id = @session_id LIMIT 1"
        params = {"shard_id": self.compute_shard_id(key), "session_id": key}
        now = datetime.now(UTC)

        if self._is_async_config():
            try:
                async with self._config.provide_session(transaction=False) as driver:
                    row = await driver.select_one_or_none(sql, params)
                if row is None or row.get("expires_at") is None:
                    return None
                remaining = int((parse_utc_timestamp(row["expires_at"]) - now).total_seconds())
                return max(0, remaining)
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return None
                raise

        def _sync_expires_in() -> int | None:
            try:
                with self._config.provide_session(transaction=False) as driver:
                    row = driver.select_one_or_none(sql, params)
                if row is None or row.get("expires_at") is None:
                    return None
                remaining = int((parse_utc_timestamp(row["expires_at"]) - now).total_seconds())
                return max(0, remaining)
            except Exception as exc:
                if is_spanner_table_missing(exc):
                    return None
                raise

        return await async_(_sync_expires_in)()

    async def delete_expired(self) -> int:
        """Delete expired session rows and return the affected count."""
        table = self._validated_table()
        sql = f"DELETE FROM {table} WHERE expires_at IS NOT NULL AND expires_at <= @now"
        params = {"now": datetime.now(UTC).isoformat()}

        if self._is_async_config():
            async with self._config.provide_session(transaction=True) as driver:
                result = await driver.execute(sql, params)
            return int(getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0)

        def _sync_delete_expired() -> int:
            with self._config.provide_session(transaction=True) as driver:
                result = driver.execute(sql, params)
            return int(getattr(result, "rows_affected", 0) or getattr(result, "num_rows", 0) or 0)

        return await async_(_sync_delete_expired)()

    def create_statements(self) -> list[str]:
        """Return idempotent ``CREATE TABLE IF NOT EXISTS`` DDL statements."""
        return [self._table_ddl()]

    def drop_statements(self) -> list[str]:
        """Return idempotent ``DROP TABLE IF EXISTS`` DDL statements."""
        return self._drop_table_sql()
