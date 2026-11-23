"""SQLAlchemy 2.1 synchronous and asynchronous dialects for PostgreSQL (+vtb / +vertebra)."""

from __future__ import annotations

from typing import Any

from sqlalchemy import exc
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.postgresql.types import PGUuid
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.sql import sqltypes

from vertebra.extensions.sqlalchemy.dialects._base import (
    _AsyncDialectMixin,
    _SyncDialectMixin,
)
from vertebra.extensions.sqlalchemy.types import HalfVector, PgVector, SparseVector

__all__ = (
    "AsyncPostgreSQLDialect",
    "SyncPostgreSQLDialect",
)

_PG_ISOLATION_LEVELS: frozenset[str] = frozenset(
    {"SERIALIZABLE", "READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ"}
)
_PG_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"57P01", "57P02", "57P03"})
_PG_NON_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"23505", "40001", "40P01"})


class _PGArrayCast(ARRAY[Any]):
    """PostgreSQL ARRAY type rendering explicit bind casts."""

    render_bind_cast = True


class _PGJSONBCast(JSONB):
    """PostgreSQL JSONB type rendering explicit bind casts."""

    render_bind_cast = True


class _PGVectorCast(PgVector):
    """PostgreSQL pgvector type rendering explicit bind casts."""

    render_bind_cast = True


_PG_COLSPECS: dict[type[Any], type[Any]] = {
    **PGDialect.colspecs,
    sqltypes.ARRAY: _PGArrayCast,
    ARRAY: _PGArrayCast,
    JSONB: _PGJSONBCast,
    sqltypes.Uuid: PGUuid,
    UUID: PGUuid,
    PgVector: _PGVectorCast,
}


class _PostgreSQLDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for PostgreSQL (+vtb / +vtb_async)."""

    supports_statement_cache = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    use_insertmanyvalues = True
    supports_native_jsonb = True
    bind_typing = BindTyping.RENDER_CASTS

    ischema_names = dict(PGDialect.ischema_names)
    ischema_names.update(
        {
            "array": ARRAY,
            "halfvec": HalfVector,
            "jsonb": JSONB,
            "sparsevec": SparseVector,
            "uuid": UUID,
            "vector": PgVector,
        }
    )

    def get_isolation_level_values(self, dbapi_conn: Any) -> Any:
        """Return supported PostgreSQL isolation levels including AUTOCOMMIT."""
        _ = dbapi_conn
        return (
            "AUTOCOMMIT",
            "SERIALIZABLE",
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
        )

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """Apply PostgreSQL isolation level, toggling autocommit for AUTOCOMMIT."""
        normalized = level.upper()
        if normalized == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
            return
        if normalized not in _PG_ISOLATION_LEVELS:
            msg = f"Invalid PostgreSQL isolation level: {level!r}"
            raise exc.ArgumentError(msg)
        dbapi_connection.autocommit = False
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {normalized}")
            cursor.execute("COMMIT")
        finally:
            cursor.close()

    def detect_autocommit_setting(self, dbapi_conn: Any) -> bool:
        """Return the current DBAPI connection autocommit setting."""
        return bool(getattr(dbapi_conn, "autocommit", False))

    _non_disconnect_sqlstates = _PG_NON_DISCONNECT_SQLSTATES
    _disconnect_sqlstates = _PG_DISCONNECT_SQLSTATES


class SyncPostgreSQLDialect(_PostgreSQLDialectMixin, _SyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for PostgreSQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    colspecs = _PG_COLSPECS


class AsyncPostgreSQLDialect(_PostgreSQLDialectMixin, _AsyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for PostgreSQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    colspecs = _PG_COLSPECS
