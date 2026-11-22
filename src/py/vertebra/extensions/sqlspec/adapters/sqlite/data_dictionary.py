"""SQLite synchronous and asynchronous SQLSpec data dictionaries."""

from __future__ import annotations

from typing import ClassVar

from sqlspec.data_dictionary.dialects.sqlite.dictionary import (
    SqliteAsyncDataDictionary as _UpstreamSqliteAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.sqlite.dictionary import (
    SqliteSyncDataDictionary as _UpstreamSqliteSyncDataDictionary,
)

__all__ = ("SQLiteAsyncDataDictionary", "SQLiteSyncDataDictionary")


class SQLiteSyncDataDictionary(_UpstreamSqliteSyncDataDictionary):
    """SQLite-specific synchronous data dictionary backed by Vertebra `SQLiteSyncDriver`."""

    __slots__ = ()

    dialect: ClassVar[str] = "sqlite"


class SQLiteAsyncDataDictionary(_UpstreamSqliteAsyncDataDictionary):
    """SQLite-specific asynchronous data dictionary backed by Vertebra `SQLiteAsyncDriver`."""

    __slots__ = ()

    dialect: ClassVar[str] = "sqlite"
