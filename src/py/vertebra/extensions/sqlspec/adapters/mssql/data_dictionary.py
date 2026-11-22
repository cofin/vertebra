"""SQL Server (MSSQL) synchronous and asynchronous data dictionaries."""

from __future__ import annotations

from typing import ClassVar

from sqlspec.data_dictionary.dialects.mssql import MssqlVersionInfo
from sqlspec.data_dictionary.dialects.mssql.dictionary import (
    MssqlAsyncDataDictionary as _UpstreamMssqlAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.mssql.dictionary import (
    MssqlSyncDataDictionary as _UpstreamMssqlSyncDataDictionary,
)

__all__ = (
    "MSSQLAsyncDataDictionary",
    "MSSQLSyncDataDictionary",
    "MssqlVersionInfo",
)


class MSSQLSyncDataDictionary(_UpstreamMssqlSyncDataDictionary):
    """Synchronous SQL Server data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "mssql"


class MSSQLAsyncDataDictionary(_UpstreamMssqlAsyncDataDictionary):
    """Asynchronous SQL Server data dictionary backed by SQLSpec's shared dialect base."""

    __slots__ = ()

    dialect: ClassVar[str] = "mssql"
