"""Postgres-specific DBAPI."""

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
    "threadsafety",
)

paramstyle = "numeric"


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to Postgres pool."""
    return Connection(dsn, paramstyle=paramstyle, **kwargs)
