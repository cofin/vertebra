"""Vertebra Postgres Dialect."""

from typing import Any

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import URL

from vertebra.extensions.sqlalchemy import _async_dbapi
from vertebra.extensions.sqlalchemy._base import VertebraDialectBase

from . import dbapi


class VertebraPostgresDialect(VertebraDialectBase, PGDialect):
    """Vertebra Postgres Dialect (Sync)."""

    name = "postgresql"
    driver = "vertebra"
    supports_statement_cache = True
    supports_transactional_ddl = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    @classmethod
    def import_dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: URL) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (url.host,), {}


class VertebraPostgresAsyncDialect(VertebraDialectBase, PGDialect_asyncpg):
    """Vertebra Postgres Dialect (Async)."""

    name = "postgresql"
    driver = "vertebra"
    is_async = True
    supports_statement_cache = True

    @classmethod
    def dbapi(cls) -> Any:
        # PGDialect_asyncpg expects an async-compatible DBAPI
        return _async_dbapi

    @classmethod
    def import_dbapi(cls) -> Any:
        return _async_dbapi

    def create_connect_args(self, url: URL) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (url.host,), {}
