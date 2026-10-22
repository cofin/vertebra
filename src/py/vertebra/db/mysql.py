"""MySQL and MariaDB driver and Cloud SQL (:3307) configuration."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import msgspec

from vertebra._typing import UnsupportedBackendError
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

    __slots__ = ("_config", "_executor")

    def __init__(
        self,
        config: MysqlConfig | None = None,
        *,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
    ) -> None:
        self._config = config or MysqlConfig()
        self._executor = executor

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    def execute_tuples(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Execute a SQL statement and return plain Python tuples."""
        if self._executor is None:
            msg = "SyncMysqlDriver.execute_tuples requires a configured MySQL connection"
            raise UnsupportedBackendError(msg)
        return [tuple(row) for row in self._executor(sql, parameters)]


class AsyncMysqlDriver:
    """Asynchronous MySQL driver facade."""

    __slots__ = ("_async_executor", "_config", "_sync")

    def __init__(
        self,
        config: MysqlConfig | None = None,
        *,
        executor: Callable[[str, Sequence[Any]], list[tuple[Any, ...]]] | None = None,
        async_executor: Callable[[str, Sequence[Any]], Awaitable[list[tuple[Any, ...]]]] | None = None,
    ) -> None:
        self._config = config or MysqlConfig()
        self._sync = SyncMysqlDriver(self._config, executor=executor)
        self._async_executor = async_executor

    @property
    def config(self) -> MysqlConfig:
        """Return the active MysqlConfig."""
        return self._config

    async def execute_tuples(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        """Asynchronously execute a SQL statement and return plain Python tuples."""
        if self._async_executor is not None:
            rows = await self._async_executor(sql, parameters)
            return [tuple(row) for row in rows]
        return await asyncio.to_thread(self._sync.execute_tuples, sql, parameters)
