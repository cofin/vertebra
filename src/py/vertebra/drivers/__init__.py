"""Bun-backed driver interfaces."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from vertebra.drivers.base import (
    AsyncConnection,
    AsyncCursor,
    AsyncDriver,
    AsyncPool,
    SyncConnection,
    SyncCursor,
    SyncDriver,
    SyncPool,
)
from vertebra.drivers.exceptions import (
    ERROR_CODE_MAP,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    format_error_message,
    map_error_code,
)
from vertebra.drivers.exceptions import Warning as DriverWarning
from vertebra.drivers.mysql import (
    AsyncMySQLConnection,
    AsyncMySQLCursor,
    AsyncMySQLDriver,
    AsyncMySQLPool,
    AsyncMySQLTransaction,
    SyncMySQLConnection,
    SyncMySQLCursor,
    SyncMySQLDriver,
    SyncMySQLDriverConfig,
    SyncMySQLPool,
    SyncMySQLTransaction,
)
from vertebra.drivers.postgres import (
    AsyncPostgresConnection,
    AsyncPostgresCursor,
    AsyncPostgresDriver,
    AsyncPostgresPool,
    AsyncPostgresTransaction,
    SyncPostgresConnection,
    SyncPostgresCursor,
    SyncPostgresDriver,
    SyncPostgresDriverConfig,
    SyncPostgresPool,
    SyncPostgresTransaction,
)
from vertebra.drivers.redis import (
    AsyncRedisConnection,
    AsyncRedisCursor,
    AsyncRedisDriver,
    AsyncRedisPool,
    SyncRedisConnection,
    SyncRedisCursor,
    SyncRedisDriver,
    SyncRedisDriverConfig,
    SyncRedisPool,
)
from vertebra.drivers.sqlite import (
    AsyncSqliteConnection,
    AsyncSqliteCursor,
    AsyncSqliteDriver,
    AsyncSqlitePool,
    SyncSqliteConnection,
    SyncSqliteCursor,
    SyncSqliteDriver,
    SyncSqliteDriverConfig,
    SyncSqlitePool,
)
from vertebra.drivers.types import (
    decode_mysql,
    decode_postgres,
    decode_sqlite,
    encode_mysql,
    encode_postgres,
    encode_sqlite,
)

Warning = DriverWarning  # noqa: A001


@dataclass(frozen=True, slots=True)
class DriverConfig:
    """Configuration for Bun-backed drivers."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class DriverBackend(Protocol):
    """Driver backend contract (implemented in PRD-4)."""

    def connect(self) -> None:
        """Establish a connection to the backend."""

    def close(self) -> None:
        """Close the backend connection."""


def get_driver_backend(config: DriverConfig) -> DriverBackend:
    """Return the configured driver backend."""
    if config.name not in _DRIVER_BACKENDS:
        msg = f"Driver backend '{config.name}' not configured yet"
        raise RuntimeError(msg)
    return _DRIVER_BACKENDS[config.name](config)


DriverBackendFactory = Callable[[DriverConfig], DriverBackend]
_DRIVER_BACKENDS: dict[str, DriverBackendFactory] = {}


def register_driver_backend(name: str, factory: DriverBackendFactory) -> None:
    """Register a driver backend factory."""
    _DRIVER_BACKENDS[name] = factory


def available_driver_backends() -> list[str]:
    """Return the list of registered driver backends."""
    return sorted(_DRIVER_BACKENDS)


__all__ = [
    "ERROR_CODE_MAP",
    "AsyncConnection",
    "AsyncCursor",
    "AsyncDriver",
    "AsyncMySQLConnection",
    "AsyncMySQLCursor",
    "AsyncMySQLDriver",
    "AsyncMySQLPool",
    "AsyncMySQLTransaction",
    "AsyncPool",
    "AsyncPostgresConnection",
    "AsyncPostgresCursor",
    "AsyncPostgresDriver",
    "AsyncPostgresPool",
    "AsyncPostgresTransaction",
    "AsyncRedisConnection",
    "AsyncRedisCursor",
    "AsyncRedisDriver",
    "AsyncRedisPool",
    "AsyncSqliteConnection",
    "AsyncSqliteCursor",
    "AsyncSqliteDriver",
    "AsyncSqlitePool",
    "DataError",
    "DatabaseError",
    "DriverBackend",
    "DriverConfig",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "SyncConnection",
    "SyncCursor",
    "SyncDriver",
    "SyncMySQLConnection",
    "SyncMySQLCursor",
    "SyncMySQLDriver",
    "SyncMySQLDriverConfig",
    "SyncMySQLPool",
    "SyncMySQLTransaction",
    "SyncPool",
    "SyncPostgresConnection",
    "SyncPostgresCursor",
    "SyncPostgresDriver",
    "SyncPostgresDriverConfig",
    "SyncPostgresPool",
    "SyncPostgresTransaction",
    "SyncRedisConnection",
    "SyncRedisCursor",
    "SyncRedisDriver",
    "SyncRedisDriverConfig",
    "SyncRedisPool",
    "SyncSqliteConnection",
    "SyncSqliteCursor",
    "SyncSqliteDriver",
    "SyncSqliteDriverConfig",
    "SyncSqlitePool",
    "Warning",
    "available_driver_backends",
    "decode_mysql",
    "decode_postgres",
    "decode_sqlite",
    "encode_mysql",
    "encode_postgres",
    "encode_sqlite",
    "format_error_message",
    "get_driver_backend",
    "map_error_code",
    "register_driver_backend",
]
