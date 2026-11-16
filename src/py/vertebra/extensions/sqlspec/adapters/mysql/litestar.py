"""MySQL and MariaDB Litestar session store backed by Vertebra native drivers."""

from datetime import UTC, datetime, timedelta
from typing import Any, NotRequired

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore
from sqlspec.utils.sync_tools import async_

from vertebra.extensions.sqlspec.adapters.mysql.core import (
    is_mysql_table_missing,
    mysql_index_options,
    mysql_table_options,
)

__all__ = ("MySQLLitestarConfig", "MySQLLitestarStore")


class MySQLLitestarConfig(LitestarConfig):
    """MySQL-specific Litestar configuration adding DDL `table_options` and `index_options`."""

    table_options: NotRequired[str]
    """Raw MySQL table DDL options."""

    index_options: NotRequired[str]
    """Raw MySQL index DDL options."""


class MySQLLitestarStore(BaseSQLSpecStore[Any]):
    """MariaDB-compatible MySQL Litestar session store using `ON DUPLICATE KEY UPDATE` and `?` binds."""

    __slots__ = ("_index_options", "_table_options")

    adapter_name = "vertebra_mysql"
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset({"index_options", "table_options"})

    def __init__(self, config: Any) -> None:
        """Initialize the MySQL/MariaDB Litestar session store."""
        super().__init__(config)
        ext = getattr(config, "extension_config", {})
        litestar_cfg = ext.get("litestar", {}) if isinstance(ext, dict) else {}
        if not isinstance(litestar_cfg, dict):
            litestar_cfg = {}
        self._index_options: str = mysql_index_options(litestar_cfg)
        self._table_options: str = mysql_table_options(litestar_cfg)

    def _is_async_config(self) -> bool:
        return bool(getattr(self._config, "is_async", False))

    def _table_ddl(self) -> str:
        """Build MySQL/MariaDB `CREATE TABLE IF NOT EXISTS` DDL for Litestar sessions."""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            "session_id VARCHAR(255) PRIMARY KEY, "
            "data LONGBLOB NOT NULL, "
            "expires_at DATETIME(6), "
            "created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6), "
            "updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), "
            f"INDEX idx_{self._table_name}_expires_at (expires_at){self._index_options}"
            f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci{self._table_options}"
        )

    def _drop_table_sql(self) -> list[str]:
        """Return DDL statements dropping the Litestar session table."""
        return [f"DROP TABLE IF EXISTS {self._table_name}"]

    def _create_table_sync(self) -> None:
        ddl = self._table_ddl()
        with self._config.provide_session() as driver:
            driver.execute_script(ddl)
            driver.commit()

    def _get_sync(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        select_sql = (
            f"SELECT data, expires_at FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(select_sql, key)
                if row is None:
                    return None
                if renew_for is not None and row["expires_at"] is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        naive_expires_at = new_expires_at.replace(tzinfo=None)
                        driver.execute(
                            f"UPDATE {self._table_name} SET expires_at = ?, updated_at = UTC_TIMESTAMP(6) WHERE session_id = ?",
                            naive_expires_at,
                            key,
                        )
                        driver.commit()
                return bytes(row["data"])
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise

    def _set_sync(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        naive_expires_at = expires_at.replace(tzinfo=None) if expires_at is not None else None
        upsert_sql = (
            f"INSERT INTO {self._table_name} (session_id, data, expires_at) "
            "VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE "
            "data = VALUES(data), "
            "expires_at = VALUES(expires_at), "
            "updated_at = UTC_TIMESTAMP(6)"
        )
        with self._config.provide_session() as driver:
            driver.execute(upsert_sql, key, data, naive_expires_at)
            driver.commit()

    def _delete_sync(self, key: str) -> None:
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {self._table_name} WHERE session_id = ?", key)
                driver.commit()
        except Exception as exc:
            if not is_mysql_table_missing(exc):
                raise

    def _delete_all_sync(self) -> None:
        try:
            with self._config.provide_session() as driver:
                driver.execute(f"DELETE FROM {self._table_name}")
                driver.commit()
        except Exception as exc:
            if not is_mysql_table_missing(exc):
                raise

    def _exists_sync(self, key: str) -> bool:
        sql = (
            f"SELECT 1 AS found FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        try:
            with self._config.provide_session() as driver:
                return driver.select_one_or_none(sql, key) is not None
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return False
            raise

    def _expires_in_sync(self, key: str) -> int | None:
        try:
            with self._config.provide_session() as driver:
                row = driver.select_one_or_none(
                    f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?",
                    key,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        if row is None or row["expires_at"] is None:
            return None
        expires_at = row["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        expires_utc = expires_at.replace(tzinfo=UTC) if expires_at.tzinfo is None else expires_at.astimezone(UTC)
        now = datetime.now(UTC)
        if expires_utc <= now:
            return 0
        return int((expires_utc - now).total_seconds())

    def _delete_expired_sync(self) -> int:
        try:
            with self._config.provide_session() as driver:
                res = driver.execute(f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)")
                driver.commit()
                return int(res.rows_affected or 0)
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise

    async def create_table(self) -> None:
        """Create the Litestar session table when schema creation is enabled."""
        if not self.create_schema_enabled:
            await self.reconcile_schema()
            return
        if self._is_async_config():
            ddl = self._table_ddl()
            async with self._config.provide_session() as driver:
                await driver.execute_script(ddl)
                await driver.commit()
        else:
            await async_(self._create_table_sync)()
        self._log_table_created()
        await self.reconcile_schema(assume_existing=True)

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        """Retrieve session payload bytes or `None` when missing or expired."""
        if not self._is_async_config():
            return await async_(self._get_sync)(key, renew_for)
        select_sql = (
            f"SELECT data, expires_at FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(select_sql, key)
                if row is None:
                    return None
                if renew_for is not None and row["expires_at"] is not None:
                    new_expires_at = self._calculate_expires_at(renew_for)
                    if new_expires_at is not None:
                        naive_expires_at = new_expires_at.replace(tzinfo=None)
                        await driver.execute(
                            f"UPDATE {self._table_name} SET expires_at = ?, updated_at = UTC_TIMESTAMP(6) WHERE session_id = ?",
                            naive_expires_at,
                            key,
                        )
                        await driver.commit()
                return bytes(row["data"])
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        """Upsert a Litestar session payload via MySQL/MariaDB `ON DUPLICATE KEY UPDATE`."""
        if not self._is_async_config():
            await async_(self._set_sync)(key, value, expires_in)
            return
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        naive_expires_at = expires_at.replace(tzinfo=None) if expires_at is not None else None
        upsert_sql = (
            f"INSERT INTO {self._table_name} (session_id, data, expires_at) "
            "VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE "
            "data = VALUES(data), "
            "expires_at = VALUES(expires_at), "
            "updated_at = UTC_TIMESTAMP(6)"
        )
        async with self._config.provide_session() as driver:
            await driver.execute(upsert_sql, key, data, naive_expires_at)
            await driver.commit()

    async def delete(self, key: str) -> None:
        """Delete a Litestar session record by key."""
        if not self._is_async_config():
            await async_(self._delete_sync)(key)
            return
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {self._table_name} WHERE session_id = ?", key)
                await driver.commit()
        except Exception as exc:
            if not is_mysql_table_missing(exc):
                raise

    async def delete_all(self) -> None:
        """Remove all rows from the Litestar session table."""
        if not self._is_async_config():
            await async_(self._delete_all_sync)()
            return
        try:
            async with self._config.provide_session() as driver:
                await driver.execute(f"DELETE FROM {self._table_name}")
                await driver.commit()
            self._log_delete_all()
        except Exception as exc:
            if not is_mysql_table_missing(exc):
                raise

    async def exists(self, key: str) -> bool:
        """Return whether a non-expired Litestar session exists for `key`."""
        if not self._is_async_config():
            return await async_(self._exists_sync)(key)
        sql = (
            f"SELECT 1 AS found FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        try:
            async with self._config.provide_session() as driver:
                return (await driver.select_one_or_none(sql, key)) is not None
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return False
            raise

    async def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for `key` or `None` when persistent or missing."""
        if not self._is_async_config():
            return await async_(self._expires_in_sync)(key)
        try:
            async with self._config.provide_session() as driver:
                row = await driver.select_one_or_none(
                    f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?",
                    key,
                )
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return None
            raise
        if row is None or row["expires_at"] is None:
            return None
        expires_at = row["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)
        expires_utc = expires_at.replace(tzinfo=UTC) if expires_at.tzinfo is None else expires_at.astimezone(UTC)
        now = datetime.now(UTC)
        if expires_utc <= now:
            return 0
        return int((expires_utc - now).total_seconds())

    async def delete_expired(self) -> int:
        """Delete all expired Litestar session rows and return the deleted count."""
        if not self._is_async_config():
            return await async_(self._delete_expired_sync)()
        try:
            async with self._config.provide_session() as driver:
                res = await driver.execute(f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)")
                await driver.commit()
                count = int(res.rows_affected or 0)
                if count > 0:
                    self._log_delete_expired(count)
                return count
        except Exception as exc:
            if is_mysql_table_missing(exc):
                return 0
            raise
