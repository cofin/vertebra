# ruff: noqa: S608
"""MySQL session store for Litestar integration in Vertebra."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from datetime import timedelta


__all__ = ("MysqlLitestarStore",)


class MysqlLitestarStore(BaseSQLSpecStore["VertebraMysqlAsyncConfig"]):
    """MySQL session store for Litestar in Vertebra.

    Mirrors sqlspec.adapters.asyncmy.litestar.store.AsyncmyStore but adapted
    for Vertebra's broker-backed MySQL driver.
    """

    __slots__ = ()

    def _get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id VARCHAR(255) PRIMARY KEY,
            data LONGBLOB NOT NULL,
            expires_at DATETIME(6),
            created_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),
            updated_at DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
            INDEX idx_{self._table_name}_expires_at (expires_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """

    def _get_drop_table_sql(self) -> list[str]:
        return [
            f"DROP INDEX idx_{self._table_name}_expires_at ON {self._table_name}",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    async def create_table(self) -> None:
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        async with await self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, [key])

            if row is None:
                return None

            # Vertebra fetchrow returns tuple
            data_value = cast("bytes", row[0])
            expires_at = cast("datetime | None", row[1])

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    naive_expires_at = new_expires_at.replace(tzinfo=None)
                    update_sql = f"""
                        UPDATE {self._table_name}
                        SET expires_at = %s, updated_at = UTC_TIMESTAMP(6)
                        WHERE session_id = %s
                        """
                    await conn.execute(update_sql, [naive_expires_at, key])

            return bytes(data_value)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)
        naive_expires_at = expires_at.replace(tzinfo=None) if expires_at else None

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES (%s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            data = new.data,
            expires_at = new.expires_at,
            updated_at = UTC_TIMESTAMP(6)
        """

        async with await self._config.provide_connection() as conn:
            await conn.execute(sql, [key, data, naive_expires_at])

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = %s"

        async with await self._config.provide_connection() as conn:
            await conn.execute(sql, [key])

    async def delete_all(self) -> None:
        sql = f"DELETE FROM {self._table_name}"

        async with await self._config.provide_connection() as conn:
            await conn.execute(sql)
        self._log_delete_all()

    async def exists(self, key: str) -> bool:
        sql = f"""
        SELECT 1 FROM {self._table_name}
        WHERE session_id = %s
        AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))
        """

        async with await self._config.provide_connection() as conn:
            result = await conn.fetchval(sql, [key])
            return result is not None

    async def expires_in(self, key: str) -> int | None:
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = %s
        """

        async with await self._config.provide_connection() as conn:
            expires_at_naive = cast("datetime | None", await conn.fetchval(sql, [key]))

            if expires_at_naive is None:
                return None

            expires_at_utc = expires_at_naive.replace(tzinfo=UTC)
            now = datetime.now(UTC)

            if expires_at_utc <= now:
                return 0

            delta = expires_at_utc - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)"

        async with await self._config.provide_connection() as conn:
            rowcount = await conn.execute(sql)
            count = rowcount or 0
            if count > 0:
                self._log_delete_expired(count)
            return count
