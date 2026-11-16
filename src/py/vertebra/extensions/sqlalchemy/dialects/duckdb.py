"""SQLAlchemy 2.1 synchronous and asynchronous dialects for DuckDB (+vtb / +vertebra)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.engine.default import DefaultDialect

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin

try:
    from duckdb_engine import Dialect as _BaseDuckDBDialect
except ImportError:
    _BaseDuckDBDialect = DefaultDialect

if TYPE_CHECKING:
    pass

__all__ = (
    "AsyncDuckDBDialect",
    "SyncDuckDBDialect",
)


class SyncDuckDBDialect(_SyncDialectMixin, _BaseDuckDBDialect):
    """SQLAlchemy 2.1 synchronous dialect for DuckDB (+vtb / +vertebra)."""

    name = "duckdb"
    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"


class AsyncDuckDBDialect(_AsyncDialectMixin, _BaseDuckDBDialect):
    """SQLAlchemy 2.1 asynchronous dialect for DuckDB (+vtb_async / +vertebra_async)."""

    name = "duckdb"
    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
