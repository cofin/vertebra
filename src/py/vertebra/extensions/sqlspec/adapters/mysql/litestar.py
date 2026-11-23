"""MySQL and MariaDB Litestar session store backed by Vertebra native drivers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, NotRequired

from sqlspec.config import LitestarConfig
from sqlspec.extensions.litestar.store import BaseSQLSpecStore

from vertebra.extensions.sqlspec._litestar import AdapterLitestarStore
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


class MySQLLitestarStore(AdapterLitestarStore):
    """MariaDB-compatible MySQL Litestar session store using `ON DUPLICATE KEY UPDATE` and `?` binds."""

    __slots__ = ("_index_options", "_table_options")

    adapter_name = "vertebra_mysql"
    _dialect: ClassVar[str] = "mysql"
    _reconcile_after_create_table: ClassVar[bool] = True
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

    def _table_ddl(self) -> str:
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
        return [f"DROP TABLE IF EXISTS {self._table_name}"]

    def _is_table_missing(self, exc: Exception) -> bool:
        return is_mysql_table_missing(exc)

    def _encode_expires_at(self, expires_at: datetime | None) -> datetime | None:
        return None if expires_at is None else expires_at.replace(tzinfo=None)

    def _get_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT data, expires_at FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        return sql, (key,)

    def _renew_query(self, key: str, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        return (
            f"UPDATE {self._table_name} SET expires_at = ?, updated_at = UTC_TIMESTAMP(6) WHERE session_id = ?",
            (encoded_expires_at, key),
        )

    def _set_query(self, key: str, data: bytes, encoded_expires_at: Any) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"INSERT INTO {self._table_name} (session_id, data, expires_at) "
            "VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE "
            "data = VALUES(data), "
            "expires_at = VALUES(expires_at), "
            "updated_at = UTC_TIMESTAMP(6)"
        )
        return sql, (key, data, encoded_expires_at)

    def _delete_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_name} WHERE session_id = ?", (key,)

    def _delete_all_query(self) -> tuple[str, tuple[Any, ...]]:
        return f"DELETE FROM {self._table_name}", ()

    def _exists_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        sql = (
            f"SELECT 1 AS found FROM {self._table_name} "
            "WHERE session_id = ? AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP(6))"
        )
        return sql, (key,)

    def _expires_in_query(self, key: str) -> tuple[str, tuple[Any, ...]]:
        return f"SELECT expires_at FROM {self._table_name} WHERE session_id = ?", (key,)

    def _delete_expired_query(self, now: datetime) -> tuple[str, tuple[Any, ...]]:
        _ = now
        return f"DELETE FROM {self._table_name} WHERE expires_at <= UTC_TIMESTAMP(6)", ()
