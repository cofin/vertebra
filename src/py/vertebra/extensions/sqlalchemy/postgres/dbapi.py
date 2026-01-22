"""Postgres-specific DBAPI."""

from vertebra.extensions.sqlalchemy._dbapi import *  # noqa: F403

paramstyle = "numeric"


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to Postgres pool."""
    return Connection(dsn, paramstyle=paramstyle, **kwargs)
