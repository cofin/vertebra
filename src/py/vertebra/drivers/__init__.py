"""Bun-backed driver interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from vertebra.drivers.base import AsyncConnection, BaseDriver, Connection, Cursor, Pool
from vertebra.drivers.exceptions import (
    DataError,
    DatabaseError,
    Error,
    ERROR_CODE_MAP,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    format_error_message,
    map_error_code,
)
from vertebra.drivers.types import (
    decode_mysql,
    decode_postgres,
    decode_sqlite,
    encode_mysql,
    encode_postgres,
    encode_sqlite,
)
from vertebra.drivers.sqlite import (
    AsyncSqliteConnection,
    SqliteConnection,
    SqliteCursor,
    SqliteDriver,
    SqliteDriverConfig,
    SqlitePool,
)
from vertebra.drivers.postgres import (
    AsyncPostgresConnection,
    PostgresConnection,
    PostgresCursor,
    PostgresDriver,
    PostgresDriverConfig,
    PostgresPool,
)

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
    raise RuntimeError(f"Driver backend '{config.name}' not configured yet")


__all__ = [
    "AsyncConnection",
    "BaseDriver",
    "Connection",
    "Cursor",
    "Pool",
    "DriverBackend",
    "DriverConfig",
    "get_driver_backend",
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
    "ERROR_CODE_MAP",
    "map_error_code",
    "format_error_message",
    "encode_postgres",
    "decode_postgres",
    "encode_mysql",
    "decode_mysql",
    "encode_sqlite",
    "decode_sqlite",
    "AsyncSqliteConnection",
    "SqliteConnection",
    "SqliteCursor",
    "SqliteDriver",
    "SqliteDriverConfig",
    "SqlitePool",
    "AsyncPostgresConnection",
    "PostgresConnection",
    "PostgresCursor",
    "PostgresDriver",
    "PostgresDriverConfig",
    "PostgresPool",
]
