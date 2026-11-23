"""SQLite Litestar session store backed by Vertebra native drivers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, NotRequired

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
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


class SQLiteLitestarStore(AdapterLitestarStore):
    """Litestar server-side session store supporting both sync and async SQLite configs."""

    __slots__ = ("_configured_driver_ids", "_pragma_statements")

    adapter_name = "vertebra_sqlite"
    _dialect: ClassVar[str] = "sqlite"
    _use_threadpool_for_sync: ClassVar[bool] = False
    _reconcile_after_create_table: ClassVar[bool] = True
    _uses_select_value: ClassVar[bool] = True
    extension_config_options = BaseSQLSpecStore.extension_config_options | frozenset(
        {"pragma_overrides", "pragma_profile"}
    )

    def __init__(self, config: Any) -> None:
        """Initialize the SQLite Litestar store from `SQLiteSyncConfig` or `SQLiteAsyncConfig`."""
        super().__init__(config)
        self._pragma_statements = extension_pragma_statements(config, "litestar")
        self._configured_driver_ids: set[int] = set()

    def _prepare_driver_sync(self, driver: Any) -> None:
        if self._pragma_statements and id(driver) not in self._configured_driver_ids:
            apply_pragmas_sync(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    async def _prepare_driver_async(self, driver: Any) -> None:
        if self._pragma_statements and id(driver) not in self._configured_driver_ids:
            await apply_pragmas_async(driver, self._pragma_statements)
            self._configured_driver_ids.add(id(driver))

    def _table_ddl(self) -> str:
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
        return [
            f"DROP INDEX IF EXISTS idx_{self._table_name}_expires_at",
            f"DROP TABLE IF EXISTS {self._table_name}",
        ]

    def prepare_schema_sync(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs before migration DDL generation."""
        self._prepare_driver_sync(driver)

    async def prepare_schema_async(self, driver: Any) -> None:
        """Apply configured SQLite PRAGMAs asynchronously before migration DDL generation."""
        await self._prepare_driver_async(driver)

    def reconcile_companion_objects_sync(self, driver: Any) -> None:
        """Apply PRAGMAs and ensure the SQLite partial expiration index exists."""
        self._prepare_driver_sync(driver)
        super().reconcile_companion_objects_sync(driver)

    async def reconcile_companion_objects_async(self, driver: Any) -> None:
        """Apply PRAGMAs and ensure the SQLite partial expiration index exists asynchronously."""
        await self._prepare_driver_async(driver)
        await super().reconcile_companion_objects_async(driver)

    def _encode_expires_at(self, expires_at: datetime | None) -> Any:
        return datetime_to_julian(expires_at)

    def _decode_expires_at(self, raw: Any) -> datetime | None:
        return None if raw is None else julian_to_datetime(raw)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT data, expires_at FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > julianday('now'))"
        )
        return sql, (key,)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        return f"UPDATE {self._table_name} SET expires_at = ? WHERE session_id = ?", (encoded_expires_at, key)

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._table_name} (session_id, data, expires_at) VALUES (?, ?, ?) "
            "ON CONFLICT(session_id) DO UPDATE SET data = excluded.data, expires_at = excluded.expires_at"
        )
        return sql, (key, data, encoded_expires_at)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_name} WHERE session_id = ?", (key,)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_name}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT 1 FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > julianday('now'))"
        )
        return sql, (key,)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?", (key,)

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        return f"DELETE FROM {self._table_name} WHERE expires_at IS NOT NULL AND expires_at <= julianday('now')", ()
