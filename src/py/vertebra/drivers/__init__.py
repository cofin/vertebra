"""Broker-backed driver interfaces."""

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
from vertebra.drivers.mysql import AsyncMysqlDriver, AsyncMysqlDriverConfig, SyncMysqlDriver, SyncMysqlDriverConfig
from vertebra.drivers.postgres import (
    AsyncPostgresDriver,
    AsyncPostgresDriverConfig,
    SyncPostgresDriver,
    SyncPostgresDriverConfig,
)
from vertebra.drivers.redis import AsyncRedisDriver, AsyncRedisDriverConfig, SyncRedisDriver, SyncRedisDriverConfig
from vertebra.drivers.sqlite import AsyncSqliteDriver, AsyncSqliteDriverConfig, SyncSqliteDriver, SyncSqliteDriverConfig
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
    """Configuration for broker-backed drivers."""

    name: str
    options: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class DriverBackend(Protocol):
    """Driver backend contract."""

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


__all__ = (
    "ERROR_CODE_MAP",
    "AsyncConnection",
    "AsyncCursor",
    "AsyncDriver",
    "AsyncMysqlDriver",
    "AsyncMysqlDriverConfig",
    "AsyncPool",
    "AsyncPostgresDriver",
    "AsyncPostgresDriverConfig",
    "AsyncRedisDriver",
    "AsyncRedisDriverConfig",
    "AsyncSqliteDriver",
    "AsyncSqliteDriverConfig",
    "DataError",
    "DatabaseError",
    "DriverBackend",
    "DriverConfig",
    "DriverWarning",
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
    "SyncMysqlDriver",
    "SyncMysqlDriverConfig",
    "SyncPool",
    "SyncPostgresDriver",
    "SyncPostgresDriverConfig",
    "SyncRedisDriver",
    "SyncRedisDriverConfig",
    "SyncSqliteDriver",
    "SyncSqliteDriverConfig",
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
)
