"""DuckDB Litestar session store supporting both sync and async Vertebra DuckDB configs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from sqlspec.extensions.litestar.store import (
    MAX_TABLE_NAME_LENGTH,
    VALID_TABLE_NAME_PATTERN,
    BaseSQLSpecStore,
    CompanionIndex,
)
from sqlspec.migrations.schema import SchemaTarget
from sqlspec.utils.sync_tools import async_
from sqlspec.utils.text import split_qualified_identifier

from vertebra.extensions.sqlspec.adapters.duckdb.core import format_duckdb_identifier, split_schema_and_table

if TYPE_CHECKING:
    from sqlspec.migrations.schema import DropStatement

__all__ = ("DuckDBLitestarStore",)

_MAX_TABLE_NAME_PARTS: Final[int] = 2


def _timestamp_to_datetime(ts: str | datetime | None) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)
    dt = datetime.fromisoformat(str(ts))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


class DuckDBLitestarStore(BaseSQLSpecStore[Any]):
    """DuckDB Litestar session store using `ON CONFLICT DO UPDATE` and `provide_session()`."""

    __slots__ = ()

    adapter_name = "vertebra_duckdb"
    _dialect: ClassVar[str] = "duckdb"

    def __init__(self, config: Any) -> None:
        """Initialize the DuckDB Litestar session store for `DuckDBSyncConfig` or `DuckDBAsyncConfig`."""
        super().__init__(config)

    @staticmethod
    def _ensure_table_name(table_name: str) -> None:
        """Validate a 1- or 2-part DuckDB session table identifier."""
        cleaned = table_name.strip()
        if not cleaned or cleaned != table_name:
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        parts = split_qualified_identifier(cleaned, quote_chars='"')
        if not parts or len(parts) > _MAX_TABLE_NAME_PARTS:
            msg = f"Invalid table name: {table_name!r}"
            raise ValueError(msg)
        for segment in parts:
            if len(segment) > MAX_TABLE_NAME_LENGTH or not VALID_TABLE_NAME_PATTERN.fullmatch(segment):
                msg = f"Invalid table name: {table_name!r}"
                raise ValueError(msg)

    def _table_ref(self) -> str:
        return format_duckdb_identifier(self._table_name)

    def _table_ddl(self) -> str:
        table_ref = self._table_ref()
        idx_name = format_duckdb_identifier(f"idx_{self._table_name.replace('.', '_')}_expires_at")
        return (
            f"CREATE TABLE IF NOT EXISTS {table_ref} (\n"
            "    session_id VARCHAR PRIMARY KEY,\n"
            "    data BLOB NOT NULL,\n"
            "    expires_at TIMESTAMPTZ,\n"
            "    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,\n"
            "    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP\n"
            ");\n"
            f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_ref}(expires_at);"
        )

    def _drop_table_sql(self) -> list[str]:
        idx_name = format_duckdb_identifier(f"idx_{self._table_name.replace('.', '_')}_expires_at")
        return [f"DROP INDEX IF EXISTS {idx_name}", f"DROP TABLE IF EXISTS {self._table_ref()}"]

    def _schema_target(self) -> SchemaTarget:
        """Build the DuckDB Litestar session `SchemaTarget` with optional schema scope."""
        if "." in self._table_name:
            schema_name, bare_table = split_schema_and_table(self._table_name)
            return SchemaTarget.from_ddl(bare_table, self._table_ddl(), schema=schema_name, dialect="duckdb")
        bare_table = self._table_name.strip('"')
        return SchemaTarget.from_ddl(bare_table, self._table_ddl(), dialect="duckdb")

    def get_schema_targets(self) -> list[SchemaTarget]:
        """Return ordered `SchemaTarget` definitions for the DuckDB session table."""
        return super().get_schema_targets()

    def get_companion_indexes(self) -> list[CompanionIndex]:
        """Return DuckDB companion expiration index definitions parsed from store DDL."""
        return super().get_companion_indexes()

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Create any missing DuckDB companion expiration indexes synchronously."""
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Create any missing DuckDB companion expiration indexes asynchronously."""
        await super().reconcile_companion_objects_async(driver)

    def downgrade_drops(self) -> list[DropStatement]:
        """Return ordered downgrade drop statements for the DuckDB session index and table."""
        return super().downgrade_drops()

    def _datetime_to_param(self, dt: datetime | None) -> str | None:
        if dt is None:
            return None
        return dt.astimezone(UTC).isoformat()

    async def create_table(self) -> None:
        """Create the Litestar session table if absent."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                await driver.execute_script(self._table_ddl())
                await driver.commit()
            self._log_table_created()
        else:
            await async_(self._create_table_sync)()
        await self.reconcile_schema(assume_existing=True)

    def _create_table_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute_script(self._table_ddl())
            driver.commit()
        self._log_table_created()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session payload bytes by `key`, optionally renewing its TTL."""
        if self._config.is_async:
            return await self._get_async(key, renew_for)
        return await async_(self._get_sync)(key, renew_for)

    def _get_sql(self) -> str:
        return (
            f"SELECT data, expires_at FROM {self._table_ref()} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )

    def _renew_sql(self) -> str:
        return f"UPDATE {self._table_ref()} SET expires_at = ?, updated_at = NOW() WHERE session_id = ?"

    def _get_sync(self, key: str, renew_for: int | timedelta | None) -> bytes | None:
        with self._config.provide_session() as driver:
            row = driver.select_one_or_none(self._get_sql(), key)
            if row is None:
                return None
            expires_at_val = row["expires_at"]
            if renew_for is not None and expires_at_val is not None:
                new_exp = self._calculate_expires_at(renew_for)
                if new_exp is not None:
                    driver.execute(self._renew_sql(), self._datetime_to_param(new_exp), key)
                    driver.commit()
            data = row["data"]
            return bytes(data) if not isinstance(data, bytes) else data

    async def _get_async(self, key: str, renew_for: int | timedelta | None) -> bytes | None:
        async with self._config.provide_session() as driver:
            row = await driver.select_one_or_none(self._get_sql(), key)
            if row is None:
                return None
            expires_at_val = row["expires_at"]
            if renew_for is not None and expires_at_val is not None:
                new_exp = self._calculate_expires_at(renew_for)
                if new_exp is not None:
                    await driver.execute(self._renew_sql(), self._datetime_to_param(new_exp), key)
                    await driver.commit()
            data = row["data"]
            return bytes(data) if not isinstance(data, bytes) else data

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Insert or update a session record via `ON CONFLICT (session_id) DO UPDATE`."""
        data = self._value_to_bytes(value)
        expires_at_str = self._datetime_to_param(self._calculate_expires_at(expires_in))
        sql = (
            f"INSERT INTO {self._table_ref()} (session_id, data, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT (session_id) DO UPDATE SET "
            "data = EXCLUDED.data, expires_at = EXCLUDED.expires_at, updated_at = NOW()"
        )
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                await driver.execute(sql, key, data, expires_at_str)
                await driver.commit()
            return
        await async_(self._exec_sync)(sql, (key, data, expires_at_str))

    def _exec_sync(self, sql: str, params: tuple[Any, ...]) -> None:
        with self._config.provide_session() as driver:
            driver.execute(sql, *params)
            driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a single session row by key."""
        sql = f"DELETE FROM {self._table_ref()} WHERE session_id = ?"
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                await driver.execute(sql, key)
                await driver.commit()
            return
        await async_(self._exec_sync)(sql, (key,))

    async def delete_all(self) -> None:
        """Delete all stored sessions."""
        sql = f"DELETE FROM {self._table_ref()}"
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                await driver.execute(sql)
                await driver.commit()
            self._log_delete_all()
            return
        await async_(self._delete_all_sync)()

    def _delete_all_sync(self) -> None:
        with self._config.provide_session() as driver:
            driver.execute(f"DELETE FROM {self._table_ref()}")
            driver.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Return True when `key` exists and has not expired."""
        sql = (
            f"SELECT 1 AS present FROM {self._table_ref()} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)"
        )
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                return await driver.select_value_or_none(sql, key) is not None
        return await async_(self._exists_sync)(key, sql)

    def _exists_sync(self, key: str, sql: str) -> bool:
        with self._config.provide_session() as driver:
            return driver.select_value_or_none(sql, key) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining lifetime in seconds for `key`, or None when absent/no expiry."""
        sql = f"SELECT expires_at FROM {self._table_ref()} WHERE session_id = ?"
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                raw_exp = await driver.select_value_or_none(sql, key)
        else:
            raw_exp = await async_(self._select_val_sync)(sql, (key,))
        expires_at = _timestamp_to_datetime(cast("str | datetime | None", raw_exp))
        if expires_at is None:
            return None
        now = datetime.now(UTC)
        if expires_at <= now:
            return 0
        return int((expires_at - now).total_seconds())

    def _select_val_sync(self, sql: str, params: tuple[Any, ...]) -> Any:
        with self._config.provide_session() as driver:
            return driver.select_value_or_none(sql, *params)

    async def delete_expired(self) -> int:
        """Delete all expired sessions and return deleted row count."""
        sql = f"DELETE FROM {self._table_ref()} WHERE expires_at <= CURRENT_TIMESTAMP RETURNING 1 AS deleted"
        if self._config.is_async:
            async with self._config.provide_session() as driver:
                rows = await driver.select(sql)
                await driver.commit()
            count = len(rows)
        else:
            count = await async_(self._delete_expired_sync)(sql)
        if count > 0:
            self._log_delete_expired(count)
        return count

    def _delete_expired_sync(self, sql: str) -> int:
        with self._config.provide_session() as driver:
            rows = driver.select(sql)
            driver.commit()
            return len(rows)
