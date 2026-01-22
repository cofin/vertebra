"""Vertebra MySQL Dialect."""

from typing import Any

from sqlalchemy.dialects.mysql.base import MySQLDialect

from . import dbapi


class VertebraMysqlDialect(MySQLDialect):
    """Vertebra MySQL Dialect (Sync)."""

    name = "vertebra_mysql"
    driver = "vertebra"

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})


class VertebraMysqlAsyncDialect(MySQLDialect):
    """Vertebra MySQL Dialect (Async)."""

    name = "vertebra_mysql_async"
    driver = "vertebra"
    is_async = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})
