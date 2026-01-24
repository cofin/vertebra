"""MySQL-specific DBAPI."""

from typing import Any

from vertebra.extensions.sqlalchemy._dbapi import (
    Connection,
    Cursor,
    DatabaseError,
    DataError,
    DBAPIWarning,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    apilevel,
    threadsafety,
)

Warning = DBAPIWarning  # PEP 249 requires this name

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

paramstyle = "qmark"


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to MySQL pool."""
    return Connection(dsn, paramstyle=paramstyle, **kwargs)
