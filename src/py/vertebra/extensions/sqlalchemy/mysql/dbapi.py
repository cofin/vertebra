"""MySQL-specific DBAPI."""

from vertebra.extensions.sqlalchemy._dbapi import *  # noqa: F403

paramstyle = "qmark"


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to MySQL pool."""
    return Connection(dsn, paramstyle=paramstyle, **kwargs)
