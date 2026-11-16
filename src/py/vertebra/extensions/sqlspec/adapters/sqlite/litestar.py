"""SQLite Litestar session store backed by Vertebra native drivers."""

from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore

from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    apply_pragmas_async,
    apply_pragmas_sync,
    datetime_to_julian,
    extension_pragma_statements,
    julian_to_datetime,
)

__all__ = ("SQLiteLitestarConfig", "SQLiteLitestarStore")


class SQLiteLitestarConfig(LitestarConfig):
    """SQLite-specific Litestar configuration adding PRAGMA tuning controls."""

    pragma_profile: NotRequired[bool]
    """Apply the built-in SQLite extension PRAGMA profile when initializing connections."""

    pragma_overrides: NotRequired[dict[str, str | int | bool]]
    """Validated SQLite PRAGMA overrides applied during schema preparation."""


class SQLiteLitestarStore(BaseSQLSpecStore[Any]):
    """Litestar server-side session store supporting both sync and async SQLite configs."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {
            "pragma_overrides",
            "pragma_profile",
        }
    )

    def __init__(self, config: Any) -> None:
        """Initialize the SQLite Litestar store from `SQLiteSyncConfig` or `SQLiteAsyncConfig`."""
        super().__init__(config)
        self._pragma_statements = extension_pragma_statements(config, "litestar")
        self._configured_driver_ids: set[int] = set()

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _ensure_pragmas_sync(self, driver: Any) -> None:
        if self._pragma_statements and id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def _ensure_pragmas_async(self, driver: Any) -> None:
        if self._pragma_statements and id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def _table_ddl(self) -> str:
        """Return SQLite CREATE TABLE and expiration index DDL for Litestar sessions."""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            "session_id TEXT PRIMARY KEY, "
            "data BLOB NOT NULL, "
            "expires_at REAL"
            ");\n"
            f"CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at "
            f"ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;"
        )

    def _drop_table_sql(self) -> list[str]:
        """Return SQL statements dropping the session index and table."""
        return [
            f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    def prepare_schema_sync(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs before migration DDL generation."""
        self._ensure_pragmas_sync(driver)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs asynchronously before migration DDL generation."""
        await self._ensure_pragmas_async(driver)

    async def create_table(self) -> None:
        """Create the Litestar session table if enabled by `create_schema_enabled`."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        ddl = self._table_ddl()
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                await driver.execute_script(ddl)
                await driver.commit()
        else:
            with self._config.provide_session() as driver:
                self._ensure_pragmas_sync(driver)
                driver.execute_script(ddl)
                driver.commit()
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve a non-expired session payload by key, optionally extending its TTL."""
        select_sql = (
            f"SELECT data, expires_at FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > julianday('now'))"
        )
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                row = await driver.select_one_or_none(select_sql, key)
                if row is None:
                    return None
                if renew_for is not None and row.get("expires_at") is not None:
                    new_expires = datetime_to_julian(self._calculate_expires_at(renew_for))
                    if new_expires is not None:
                        await driver.execute(
                            f"UPDATE {self._table_name} SET expires_at = ? WHERE session_id = ?",
                            new_expires,
                            key,
                        )
                        await driver.commit()
                return bytes(row["data"])

        with self._config.provide_session() as driver:
            self._ensure_pragmas_sync(driver)
            row = driver.select_one_or_none(select_sql, key)
            if row is None:
                return None
            if renew_for is not None and row.get("expires_at") is not None:
                new_expires = datetime_to_julian(self._calculate_expires_at(renew_for))
                if new_expires is not None:
                    driver.execute(
                        f"UPDATE {self._table_name} SET expires_at = ? WHERE session_id = ?",
                        new_expires,
                        key,
                    )
                    driver.commit()
            return bytes(row["data"])

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Insert or replace a Litestar session record with `ON CONFLICT(session_id) DO UPDATE`."""
        data = self._value_to_bytes(value)
        expires_julian = datetime_to_julian(self._calculate_expires_at(expires_in))
        sql = (
            f"INSERT INTO {self._table_name} (session_id, data, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(session_id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at"
        )
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                await driver.execute(sql, key, data, expires_julian)
                await driver.commit()
            return
        with self._config.provide_session() as driver:
            self._ensure_pragmas_sync(driver)
            driver.execute(sql, key, data, expires_julian)
            driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a single session row by `session_id`."""
        sql = f"DELETE FROM {self._table_name} WHERE session_id = ?"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                await driver.execute(sql, key)
                await driver.commit()
            return
        with self._config.provide_session() as driver:
            self._ensure_pragmas_sync(driver)
            driver.execute(sql, key)
            driver.commit()

    async def delete_all(self) -> None:
        """Delete every stored session row from the session table."""
        sql = f"DELETE FROM {self._table_name}"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                await driver.execute(sql)
                await driver.commit()
        else:
            with self._config.provide_session() as driver:
                self._ensure_pragmas_sync(driver)
                driver.execute(sql)
                driver.commit()
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        """Return True when `key` exists and has not expired."""
        sql = (
            f"SELECT 1 FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > julianday('now'))"
        )
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                return await driver.select_value_or_none(sql, key) is not None
        with self._config.provide_session() as driver:
            self._ensure_pragmas_sync(driver)
            return driver.select_value_or_none(sql, key) is not None

    async def expires_in(self, key: str) -> int | None:
        """Return remaining lifetime in seconds for `key`, or `None` when absent/unexpiring."""
        sql = f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                raw = await driver.select_value_or_none(sql, key)
        else:
            with self._config.provide_session() as driver:
                self._ensure_pragmas_sync(driver)
                raw = driver.select_value_or_none(sql, key)
        if raw is None:
            return None
        expires_dt = julian_to_datetime(raw)
        if expires_dt is None:
            return None
        remaining = int((expires_dt - datetime.now(UTC)).total_seconds())
        return max(0, remaining)

    async def delete_expired(self) -> int:
        """Delete all expired session rows and return the affected row count."""
        sql = f"DELETE FROM {self._table_name} WHERE expires_at IS NOT NULL AND expires_at <= julianday('now')"
        if self._is_async_config():
            async with self._config.provide_session() as driver:
                await self._ensure_pragmas_async(driver)
                res = await driver.execute(sql)
                await driver.commit()
                deleted = res.rows_affected
        else:
            with self._config.provide_session() as driver:
                self._ensure_pragmas_sync(driver)
                res = driver.execute(sql)
                driver.commit()
                deleted = res.rows_affected
        count = int(deleted) if deleted and deleted > 0 else 0
        if count > 0:
            self._log_delete_expired(count)
        return count
