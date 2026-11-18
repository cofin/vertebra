"""Cloud Spanner hash-sharded Litestar session store supporting GoogleSQL and Spangres TTL."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore, CompanionIndex
from sqlspec.migrations.schema import DropStatement, SchemaTarget, metadata_has_index
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


class SpannerLitestarConfig(LitestarConfig):
    """Typed extension configuration keys supported by ``SpannerLitestarStore``."""


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

    def _spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        return normalize_spanner_dialect(
            getattr(self._config, "spanner_dialect", None) or getattr(self._config, "dialect", "googlesql")
        )

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _validated_table(self) -> str:
        return validate_spanner_identifier(self.table_name, "session_table")

    def _split_table_name(self) -> tuple[str | None, str]:
        table = self._validated_table()
        schema_name, dot, bare_table = table.partition(".")
        if dot:
            return schema_name, bare_table
        return None, table

    def _companion_index(self) -> CompanionIndex:
        schema_name, bare_table = self._split_table_name()
        table = self._validated_table()
        idx_name = validate_spanner_identifier(f"idx_{table.replace('.', '_')}_expires_at", "session_index")
        if self._spanner_dialect() == "spangres":
            ddl = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}(expires_at) WHERE expires_at IS NOT NULL"
        else:
            ddl = f"CREATE NULL_FILTERED INDEX IF NOT EXISTS {idx_name} ON {table}(expires_at)"
        return CompanionIndex(name=idx_name, table=bare_table, schema=schema_name, ddl=ddl)

    def compute_shard_id(self, key: str) -> int:
        """Compute the deterministic hash shard prefix for session ``key``."""
        return farm_fingerprint_shard(key, self._shard_count)

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered ``SchemaTarget`` definitions for the Cloud Spanner session store."""
        schema_name, bare_table = self._split_table_name()
        parse_dialect = "postgres" if self._spanner_dialect() == "spangres" else "spanner"
        return [SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect=parse_dialect)]

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return the Spanner session expiration companion index definition."""
        return [self._companion_index()]

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create missing companion indexes on a synchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                driver.execute_script(index.ddl)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create missing companion indexes on an asynchronous driver without committing."""
        for index in self.get_companion_indexes():
            rows = await driver.data_dictionary.get_indexes(driver, table=index.table, schema=index.schema)
            if not metadata_has_index(rows, index.table, index.name, schema_name=index.schema):
                await driver.execute_script(index.ddl)

    def _table_ddl(self) -> str:
        table = self._validated_table()
        if self._spanner_dialect() == "spangres":
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
        table = self._validated_table()
        idx = self._companion_index()
        qualified_index = f"{idx.schema}.{idx.name}" if idx.schema else idx.name
        return [
            f"DROP INDEX IF EXISTS {qualified_index}",
            f"DROP TABLE IF EXISTS {table}",
        ]

    def downgrade_drops(self) -> list[DropStatement]:
        """Return index-before-table downgrade drop statements for the Spanner session store."""
        return [DropStatement(statement) for statement in self._drop_table_sql()]

    def _merge_sql(self) -> str:
        table = self._validated_table()
        if self._spanner_dialect() == "spangres":
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
        """Create the Litestar session table and companion index in Cloud Spanner."""
        statements = self.create_statements()
        if self._is_async_config():
            async with self._config.provide_session(transaction=False) as driver:
                for stmt in statements:
                    await driver.execute(stmt)
            return

        def _sync_create() -> None:
            with self._config.provide_session(transaction=False) as driver:
                for stmt in statements:
                    driver.execute(stmt)

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
                async with self._config.provide_session(transaction=False, read_only=True) as driver:
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
                with self._config.provide_session(transaction=False, read_only=True) as driver:
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
                async with self._config.provide_session(transaction=False, read_only=True) as driver:
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
                with self._config.provide_session(transaction=False, read_only=True) as driver:
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
                async with self._config.provide_session(transaction=False, read_only=True) as driver:
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
                with self._config.provide_session(transaction=False, read_only=True) as driver:
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
        """Return idempotent ``CREATE TABLE IF NOT EXISTS`` and companion index DDL statements."""
        return [self._table_ddl(), self._companion_index().ddl]

    def drop_statements(self) -> list[str]:
        """Return idempotent ``DROP INDEX IF EXISTS`` and ``DROP TABLE IF EXISTS`` DDL statements."""
        return self._drop_table_sql()
