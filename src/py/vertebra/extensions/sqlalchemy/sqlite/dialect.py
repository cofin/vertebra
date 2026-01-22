"""Vertebra SQLite Dialect."""

from typing import Any

from sqlalchemy.dialects.sqlite.base import SQLiteDialect

from . import dbapi


class VertebraSqliteDialect(SQLiteDialect):
    """Vertebra SQLite Dialect (Sync)."""

    name = "vertebra_sqlite"
    driver = "vertebra"

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})


class VertebraSqliteAsyncDialect(SQLiteDialect):
    """Vertebra SQLite Dialect (Async)."""

    name = "vertebra_sqlite_async"
    driver = "vertebra"
    is_async = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})
