"""SQLite-specific DBAPI."""

# ruff: noqa: A004
from typing import Any

from vertebra.extensions.sqlalchemy._dbapi import (
    Connection,
    Cursor,
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
    apilevel,
    threadsafety,
)

__all__ = (
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "sqlite_version_info",
    "threadsafety",
)

paramstyle = "qmark"
sqlite_version_info = (3, 30, 0)


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to SQLite pool."""
    return Connection(dsn, paramstyle=paramstyle, **kwargs)
