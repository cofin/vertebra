"""Vertebra MySQL Dialect."""

from typing import Any

from sqlalchemy.dialects.mysql.aiomysql import MySQLDialect_aiomysql
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.engine import URL

from vertebra.extensions.sqlalchemy import _async_dbapi
from vertebra.extensions.sqlalchemy._base import VertebraDialectBase

from . import dbapi


class VertebraMySQLDialect(VertebraDialectBase, MySQLDialect):
    """Vertebra MySQL Dialect (Sync)."""

    name = "mysql"
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

    def _detect_charset(self, connection: Any) -> str:
        return "utf8mb4"


class VertebraMySQLAsyncDialect(VertebraDialectBase, MySQLDialect_aiomysql):
    """Vertebra MySQL Dialect (Async)."""

    name = "mysql"
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

    def _detect_charset(self, connection: Any) -> str:
        return "utf8mb4"
