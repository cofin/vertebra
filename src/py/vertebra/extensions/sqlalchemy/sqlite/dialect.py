"""Vertebra SQLite Dialect."""

from typing import Any

from sqlalchemy.dialects.sqlite.aiosqlite import SQLiteDialect_aiosqlite
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import URL

from vertebra.extensions.sqlalchemy import _async_dbapi
from vertebra.extensions.sqlalchemy._base import VertebraDialectBase

from . import dbapi


class VertebraSQLiteDialect(VertebraDialectBase, SQLiteDialect):
    """Vertebra SQLite Dialect (Sync)."""

    name = "sqlite"
    driver = "vertebra"
    supports_statement_cache = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    @classmethod
    def import_dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: URL) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (url.host,), {}


class VertebraSQLiteAsyncDialect(VertebraDialectBase, SQLiteDialect_aiosqlite):
    """Vertebra SQLite Dialect (Async)."""

    name = "sqlite"
    driver = "vertebra"
    is_async = True
    supports_statement_cache = True

    @classmethod
    def dbapi(cls) -> Any:
        return _async_dbapi

    @classmethod
    def import_dbapi(cls) -> Any:
        return _async_dbapi

    def create_connect_args(self, url: URL) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (url.host,), {}
