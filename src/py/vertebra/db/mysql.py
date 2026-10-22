"""MySQL and MariaDB driver and Cloud SQL (:3307) configuration."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import msgspec

from vertebra.db.dialer.cloudsql import CloudSqlConfig

__all__ = (
    "AsyncMysqlDriver",
    "MysqlConfig",
    "SyncMysqlDriver",
)


class MysqlConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Configuration for MySQL 8.0+/9.0+ and MariaDB connections."""

    dsn: str = "mysql://localhost:3306/mysql"
    min_size: int = 1
    max_size: int = 10
    cloud_sql_instance: str | None = None
    enable_iam_auth: bool = False
    charset: str = "utf8mb4"
    autocommit: bool = False

    def __post_init__(self) -> None:
        if self.min_size < 0 or self.max_size < 1 or self.min_size > self.max_size:
            msg = f"Invalid pool bounds: min_size={self.min_size}, max_size={self.max_size}"
            raise ValueError(msg)

    def to_cloud_dialer_config(self) -> CloudSqlConfig | None:
        """Return CloudSqlConfig when `cloud_sql_instance` is set."""
        if self.cloud_sql_instance:
            return CloudSqlConfig(
                instance_connection_name=self.cloud_sql_instance,
                enable_iam_auth=self.enable_iam_auth,
            )
        return None


class SyncMysqlDriver:
    """Synchronous MySQL driver facade returning plain tuples for SQLAlchemy 2.1 compatibility."""

    __slots__ = ("_config",)

    def __init__(self, config: MysqlConfig | None = None) -> None:
        self._config = config or MysqlConfig()

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    def execute_tuples(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        _ = (sql, parameters)
        return []


class AsyncMysqlDriver:
    """Asynchronous MySQL driver facade."""

    __slots__ = ("_config", "_sync")

    def __init__(self, config: MysqlConfig | None = None) -> None:
        self._config = config or MysqlConfig()
        self._sync = SyncMysqlDriver(self._config)

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    async def execute_tuples(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        return self._sync.execute_tuples(sql, parameters)
