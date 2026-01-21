"""SQLite session store for Litestar integration in Vertebra."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from datetime import timedelta
    from vertebra.extensions.sqlspec.sqlite.config import SqliteConfig


SECONDS_PER_DAY = 86400.0
JULIAN_EPOCH = 2440587.5

__all__ = ("SqliteLitestarStore",)


class SqliteLitestarStore(BaseSQLSpecStore["SqliteConfig"]):
    """SQLite session store for Litestar in Vertebra.

    Mirrors sqlspec.adapters.aiosqlite.litestar.store.AiosqliteStore but adapted
    for Vertebra's broker-backed SQLite driver.
    """

    __slots__ = ()

    def _get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BLOB NOT NULL,
            expires_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;
        """

    def _get_drop_table_sql(self) -> list[str]:
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    def _datetime_to_julian(self, dt: datetime | None) -> float | None:
        if dt is None:
            return None

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta_days = (dt - epoch).total_seconds() / SECONDS_PER_DAY
        return JULIAN_EPOCH + delta_days

    def _julian_to_datetime(self, julian: float | None) -> datetime | None:
        if julian is None:
            return None

        days_since_epoch = julian - JULIAN_EPOCH
        timestamp = days_since_epoch * SECONDS_PER_DAY
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    async def create_table(self) -> None:
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, [key])

            if row is None:
                return None

            data, expires_at_julian = row

            if renew_for is not None and expires_at_julian is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                new_expires_at_julian = self._datetime_to_julian(new_expires_at)
                if new_expires_at_julian is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = ?
                    WHERE session_id = ?
                    """
                    await conn.execute(update_sql, [new_expires_at_julian, key])

            return bytes(data)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        expires_at_julian = self._datetime_to_julian(expires_at)

        sql = f"""
        INSERT OR REPLACE INTO {self._table_name} (session_id, data, expires_at)
        VALUES (?, ?, ?)
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, [key, data, expires_at_julian])

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = ?"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, [key])

    async def delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        async with self._config.provide_connection() as conn:
            await conn.execute(sql)
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = ?
        AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))
        """

        async with self._config.provide_connection() as conn:
            result = await conn.fetchval(sql, [key])
            return result is not None

    async def expires_in(self, key: str) -> int | None:
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = ?
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, [key])

            if row is None or row[0] is None:
                return None

            expires_at_julian = row[0]
            expires_at = self._julian_to_datetime(expires_at_julian)

            if expires_at is None:
                return None

            now = datetime.now(timezone.utc)

            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE julianday(expires_at) <= julianday('now')"

        async with self._config.provide_connection() as conn:
            rowcount = await conn.execute(sql)
            count = rowcount or 0
            if count > 0:
                self._log_delete_expired(count)
            return count
