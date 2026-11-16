"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Microsoft SQL Server (+vtb / +vertebra)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.mssql.base import MSDialect

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin

__all__ = (
    "AsyncMSSQLDialect",
    "SyncMSSQLDialect",
)


class SyncMSSQLDialect(_SyncDialectMixin, MSDialect):
    """SQLAlchemy 2.1 synchronous dialect for Microsoft SQL Server (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MSSQL dialect."""
        if isinstance(col_type, type):
            name = getattr(col_type, "__name__", "")
            if name in ("Vector", "BinaryJSON", "MsgspecJSON"):
                return col_type()
        return col_type


class AsyncMSSQLDialect(_AsyncDialectMixin, MSDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Microsoft SQL Server (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"

    @classmethod
    def type_descriptor(cls, col_type: Any) -> Any:
        """Adapt Vector or BinaryJSON column type for the MSSQL dialect."""
        if isinstance(col_type, type):
            name = getattr(col_type, "__name__", "")
            if name in ("Vector", "BinaryJSON", "MsgspecJSON"):
                return col_type()
        return col_type
