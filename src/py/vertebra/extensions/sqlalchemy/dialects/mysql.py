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

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MySQL dialect."""
        if isinstance(col_type, type):
            name = getattr(col_type, "__name__", "")
            if name in ("Vector", "BinaryJSON", "MsgspecJSON"):
                return col_type()
        return col_type


class AsyncMySQLDialect(_AsyncDialectMixin, BaseMySQLDialect):
    """SQLAlchemy 2.1 asynchronous dialect for MySQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MySQL dialect."""
        if isinstance(col_type, type):
            name = getattr(col_type, "__name__", "")
            if name in ("Vector", "BinaryJSON", "MsgspecJSON"):
                return col_type()
        return col_type
