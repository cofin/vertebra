"""SQLAlchemy 2.1 synchronous and asynchronous dialects for MySQL (+vtb / +vertebra)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.mysql.base import MySQLDialect as BaseMySQLDialect

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin

__all__ = (
    "AsyncMySQLDialect",
    "SyncMySQLDialect",
)


class SyncMySQLDialect(_SyncDialectMixin, BaseMySQLDialect):
    """SQLAlchemy 2.1 synchronous dialect for MySQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using MySQL colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)


class AsyncMySQLDialect(_AsyncDialectMixin, BaseMySQLDialect):
    """SQLAlchemy 2.1 asynchronous dialect for MySQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"

    def type_descriptor(self, typeobj: Any) -> Any:
        """Adapt SQLAlchemy and custom column types using MySQL colspecs."""
        parent_descriptor: Any = super().type_descriptor
        return parent_descriptor(typeobj)
