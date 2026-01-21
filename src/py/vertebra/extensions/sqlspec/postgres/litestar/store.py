"""PostgreSQL session store for Litestar integration in Vertebra."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from sqlspec.extensions.litestar.store import BaseSQLSpecStore

if TYPE_CHECKING:
    from datetime import timedelta
    from vertebra.extensions.sqlspec.postgres.config import PostgresConfig


__all__ = ("PostgresLitestarStore",)


class PostgresLitestarStore(BaseSQLSpecStore["PostgresConfig"]):
    """PostgreSQL session store for Litestar in Vertebra.

    Mirrors sqlspec.adapters.asyncpg.litestar.store.AsyncpgStore but adapted
    for Vertebra's broker-backed PostgreSQL driver.
    """

    __slots__ = ()

    def _get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_name} (
            session_id TEXT PRIMARY KEY,
            data BYTEA NOT NULL,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) WITH (fillfactor = 80);

        CREATE INDEX IF NOT EXISTS idx_{self._table_name}_expires_at
        ON {self._table_name}(expires_at) WHERE expires_at IS NOT NULL;

        ALTER TABLE {self._table_name} SET (
            autovacuum_vacuum_scale_factor = 0.05,
            autovacuum_analyze_scale_factor = 0.02
        );
        """

    def _get_drop_table_sql(self) -> list[str]:
        return [f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at", f"DROP TABLE IF EXISTS {self._table_name}"]

    async def create_table(self) -> None:
        sql = self._get_create_table_sql()
        async with self._config.provide_session() as driver:
            await driver.execute_script(sql)
        self._log_table_created()

    async def get(self, key: str, renew_for: int | timedelta | None = None) -> bytes | None:
        sql = f"""
        SELECT data, expires_at FROM {self._table_name}
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            row = await conn.fetchrow(sql, [key])

            if row is None:
                return None

            # Vertebra fetchrow returns tuple, but sqlspec expects dict access
            # We'll map it manually here.
            # Row index 0: data, index 1: expires_at
            data = row[0]
            expires_at = row[1]

            if renew_for is not None and expires_at is not None:
                new_expires_at = self._calculate_expires_at(renew_for)
                if new_expires_at is not None:
                    update_sql = f"""
                    UPDATE {self._table_name}
                    SET expires_at = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE session_id = $2
                    """
                    await conn.execute(update_sql, [new_expires_at, key])

            return bytes(data)

    async def set(self, key: str, value: str | bytes, expires_in: int | timedelta | None = None) -> None:
        data = self._value_to_bytes(value)
        expires_at = self._calculate_expires_at(expires_in)

        sql = f"""
        INSERT INTO {self._table_name} (session_id, data, expires_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (session_id)
        DO UPDATE SET
            data = EXCLUDED.data,
            expires_at = EXCLUDED.expires_at,
            updated_at = CURRENT_TIMESTAMP
        """

        async with self._config.provide_connection() as conn:
            await conn.execute(sql, [key, data, expires_at])

    async def delete(self, key: str) -> None:
        sql = f"DELETE FROM {self._table_name} WHERE session_id = $1"

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
        WHERE session_id = $1
        AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """

        async with self._config.provide_connection() as conn:
            result = await conn.fetchval(sql, [key])
            return result is not None

    async def expires_in(self, key: str) -> int | None:
        sql = f"""
        SELECT expires_at FROM {self._table_name}
        WHERE session_id = $1
        """

        async with self._config.provide_connection() as conn:
            expires_at = await conn.fetchval(sql, [key])

            if expires_at is None:
                return None

            now = datetime.now(timezone.utc)
            if expires_at <= now:
                return 0

            delta = expires_at - now
            return int(delta.total_seconds())

    async def delete_expired(self) -> int:
        sql = f"DELETE FROM {self._table_name} WHERE expires_at <= CURRENT_TIMESTAMP"

        async with self._config.provide_connection() as conn:
            rowcount = await conn.execute(sql)
            count = rowcount or 0
            if count > 0:
                self._log_delete_expired(count)
            return count
