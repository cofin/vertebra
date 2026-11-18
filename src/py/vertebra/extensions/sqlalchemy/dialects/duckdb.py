"""SQLAlchemy 2.1 synchronous and asynchronous dialects for DuckDB (+vtb / +vertebra)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy.dialects.postgresql.psycopg2 import PGExecutionContext_psycopg2
from sqlalchemy.engine.url import URL

from vertebra.extensions.sqlalchemy.dialects._base import (
    _NON_DISCONNECT_SQLSTATES,
    _AsyncDialectMixin,
    _extract_sqlstate,
    _is_disconnect_common,
    _SyncDialectMixin,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from duckdb_engine import Dialect as _BaseDuckDBDialect
else:
    _BaseDuckDBDialect = ensure_module(
        "duckdb_engine",
        package="vertebra-core[sqlalchemy-duckdb]",
        extra="sqlalchemy-duckdb",
        install_package="vertebra-core[sqlalchemy-duckdb]",
    ).Dialect

__all__ = (
    "AsyncDuckDBDialect",
    "DuckDBExecutionContext",
    "SyncDuckDBDialect",
)


class DuckDBExecutionContext(PGExecutionContext_psycopg2):
    """Execution context for DuckDB avoiding psycopg2 notice inspection on DBAPI cursors."""

    def _log_notices(self, cursor: Any) -> None:
        conn = getattr(cursor, "connection", None)
        if conn is None or not getattr(conn, "notices", None):
            return
        parent_log: Any = super()._log_notices
        parent_log(cursor)


class _DuckDBDialectMixin:
    """Shared DuckDB dialect behavior bridging duckdb_engine onto Vertebra DBAPI."""

    _json_serializer: Any
    _json_deserializer: Any

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the DuckDB dialect while guarding optional JSON serializer kwargs."""
        json_serializer = kwargs.get("json_serializer")
        json_deserializer = kwargs.get("json_deserializer")
        try:
            super().__init__(*args, **kwargs)
        except TypeError as exc:
            stripped = {k: v for k, v in kwargs.items() if k not in {"json_serializer", "json_deserializer"}}
            if len(stripped) == len(kwargs):
                raise exc
            super().__init__(*args, **stripped)
            if json_serializer is not None:
                self._json_serializer = json_serializer
            if json_deserializer is not None:
                self._json_deserializer = json_deserializer

    def create_connect_args(self, url: URL) -> tuple[Any, dict[str, Any]]:
        """Pass the rendered URL explicitly without percent-encoding DuckDB :memory: paths."""
        clean_url = url.difference_update_query(["shm_broker"]) if "shm_broker" in url.query else url
        rendered = clean_url.render_as_string(hide_password=False)
        if clean_url.database == ":memory:":
            rendered = rendered.replace("/%3Amemory%3A", "/:memory:", 1)
        return (), {"url": rendered}

    def connect(self, *cargs: Any, **cparams: Any) -> Any:
        """Route DBAPI connections through the established Vertebra DBAPI module."""
        loaded_dbapi: Any = getattr(self, "loaded_dbapi", None)
        return loaded_dbapi.connect(*cargs, **cparams)

    def do_begin(self, connection: Any) -> None:
        """Leave transaction start to DBAPI execution since async adapters omit .begin()."""
        _ = connection

    def do_rollback(self, connection: Any) -> None:
        """Roll back the active DBAPI transaction cleanly."""
        connection.rollback()

    def do_ping(self, dbapi_connection: Any) -> Literal[True]:
        """Verify liveness using SELECT 1 without requiring a .closed attribute."""
        select_one: str = getattr(self, "_dialect_specific_select_one", "SELECT 1")
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(select_one)
        finally:
            cursor.close()
        return True

    def _get_server_version_info(self, connection: Any) -> tuple[int, int]:
        """Return PostgreSQL 8.0 compatibility tuple expected by duckdb_engine."""
        _ = connection
        return (8, 0)

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True for SQLSTATE 08*, closed connections, or disconnect errors, never raising."""
        try:
            sqlstate = _extract_sqlstate(e)
            if sqlstate in _NON_DISCONNECT_SQLSTATES or type(e).__name__ == "SerializationFailure":
                return False
            if sqlstate is not None and sqlstate.startswith("08"):
                return True
            if getattr(connection, "closed", False) or getattr(
                getattr(connection, "_connection", None), "closed", False
            ):
                return True
            parent = super()
            raw_parent_fn = getattr(parent, "is_disconnect", None)
            parent_fn = raw_parent_fn if callable(raw_parent_fn) and getattr(self, "dbapi", None) is not None else None
            return _is_disconnect_common(e, connection, cursor, parent_fn)
        except Exception:
            return False


class SyncDuckDBDialect(_DuckDBDialectMixin, _SyncDialectMixin, _BaseDuckDBDialect):
    """SQLAlchemy 2.1 synchronous dialect for DuckDB (+vtb / +vertebra)."""

    name = "duckdb"
    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
    execution_ctx_cls = DuckDBExecutionContext


class AsyncDuckDBDialect(_DuckDBDialectMixin, _AsyncDialectMixin, _BaseDuckDBDialect):
    """SQLAlchemy 2.1 asynchronous dialect for DuckDB (+vtb_async / +vertebra_async)."""

    name = "duckdb"
    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
    execution_ctx_cls = DuckDBExecutionContext
