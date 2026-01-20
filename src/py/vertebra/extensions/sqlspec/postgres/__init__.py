"""Broker-backed PostgreSQL SQLSpec adapters."""

from vertebra.extensions.sqlspec.postgres._typing import VertebraPostgresAsyncConnection, VertebraPostgresSyncConnection
from vertebra.extensions.sqlspec.postgres.config import (
    PostgresDriverFeatures,
    VertebraPostgresAsyncConfig,
    VertebraPostgresSyncConfig,
)
from vertebra.extensions.sqlspec.postgres.core import default_statement_config
from vertebra.extensions.sqlspec.postgres.driver import (
    VertebraPostgresAsyncCursor,
    VertebraPostgresAsyncDriver,
    VertebraPostgresAsyncExceptionHandler,
    VertebraPostgresSyncCursor,
    VertebraPostgresSyncDriver,
    VertebraPostgresSyncExceptionHandler,
)

__all__ = (
    "PostgresDriverFeatures",
    "VertebraPostgresAsyncConfig",
    "VertebraPostgresAsyncConnection",
    "VertebraPostgresAsyncCursor",
    "VertebraPostgresAsyncDriver",
    "VertebraPostgresAsyncExceptionHandler",
    "VertebraPostgresSyncConfig",
    "VertebraPostgresSyncConnection",
    "VertebraPostgresSyncCursor",
    "VertebraPostgresSyncDriver",
    "VertebraPostgresSyncExceptionHandler",
    "default_statement_config",
)
