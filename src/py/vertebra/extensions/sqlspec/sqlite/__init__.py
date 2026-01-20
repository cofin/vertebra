"""Broker-backed SQLite SQLSpec adapters."""

from vertebra.extensions.sqlspec.sqlite._typing import VertebraSqliteAsyncConnection, VertebraSqliteSyncConnection
from vertebra.extensions.sqlspec.sqlite.config import (
    SqliteDriverFeatures,
    VertebraSqliteAsyncConfig,
    VertebraSqliteSyncConfig,
)
from vertebra.extensions.sqlspec.sqlite.core import default_statement_config
from vertebra.extensions.sqlspec.sqlite.driver import (
    VertebraSqliteAsyncCursor,
    VertebraSqliteAsyncDriver,
    VertebraSqliteAsyncExceptionHandler,
    VertebraSqliteSyncCursor,
    VertebraSqliteSyncDriver,
    VertebraSqliteSyncExceptionHandler,
)

__all__ = (
    "SqliteDriverFeatures",
    "VertebraSqliteAsyncConfig",
    "VertebraSqliteAsyncConnection",
    "VertebraSqliteAsyncCursor",
    "VertebraSqliteAsyncDriver",
    "VertebraSqliteAsyncExceptionHandler",
    "VertebraSqliteSyncConfig",
    "VertebraSqliteSyncConnection",
    "VertebraSqliteSyncCursor",
    "VertebraSqliteSyncDriver",
    "VertebraSqliteSyncExceptionHandler",
    "default_statement_config",
)
