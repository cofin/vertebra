"""Vertebra Postgres Dialect."""

from typing import Any

from sqlalchemy.dialects.postgresql.base import PGDialect

from . import dbapi


class VertebraPostgresDialect(PGDialect):
    """Vertebra Postgres Dialect (Sync)."""

    name = "vertebra_postgres"
    driver = "vertebra"
    supports_transactional_ddl = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})


class VertebraPostgresAsyncDialect(PGDialect):
    """Vertebra Postgres Dialect (Async)."""

    name = "vertebra_postgres_async"
    driver = "vertebra"
    is_async = True
    supports_transactional_ddl = True

    @classmethod
    def dbapi(cls) -> Any:
        return dbapi

    def create_connect_args(self, url: Any) -> tuple[list[Any], dict[Any, Any]]:
        return ([url.host], {})
