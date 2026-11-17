"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Cloud Spanner GoogleSQL and Spangres (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from sqlalchemy import exc
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine.interfaces import IsolationLevel
from sqlalchemy.pool import NullPool

from vertebra.db.dbapi import NotSupportedError as DbapiNotSupportedError
from vertebra.extensions.sqlalchemy.dialects._base import (
    _DISCONNECT_SQLSTATES,
    _NON_DISCONNECT_SQLSTATES,
    _AsyncDialectMixin,
    _extract_sqlstate,
    _extract_vendor_code,
    _is_disconnect_common,
    _SyncDialectMixin,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect as _BaseSpannerDialect
    from sqlalchemy.engine.url import URL
    from sqlalchemy.pool import Pool
else:
    _BaseSpannerDialect = ensure_module(
        "google.cloud.sqlalchemy_spanner",
        package="vertebra-core[sqlalchemy-gcp]",
        extra="sqlalchemy-gcp",
        install_package="vertebra-core[sqlalchemy-gcp]",
    ).SpannerDialect

__all__ = (
    "AsyncSpangresDialect",
    "AsyncSpannerDialect",
    "SyncSpangresDialect",
    "SyncSpannerDialect",
)

_GRPC_UNAVAILABLE_CODE = 14
_SPANNER_ISOLATION_LEVELS: frozenset[str] = frozenset({"SERIALIZABLE", "REPEATABLE READ"})
_SPANNER_DISCONNECT_PHRASES: tuple[str, ...] = (
    "unavailable",
    "channel closed",
    "closed channel",
    "channel is closed",
    "transport is closing",
    "goaway",
    "reset by peer",
)


def _is_grpc_unavailable(exc_obj: BaseException) -> bool:
    for candidate in (exc_obj, getattr(exc_obj, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "code", None)
        if getattr(code, "name", None) == "UNAVAILABLE":
            return True
    return _extract_vendor_code(exc_obj) == _GRPC_UNAVAILABLE_CODE


def _wants_broker_pool(url: URL) -> bool:
    query: dict[str, Any] = dict(url.query) if url.query else {}
    if query.get("shm_broker") in {"true", "1", "yes"}:
        return False
    return query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}


def _check_spanner_disconnect(
    dialect_obj: Any,
    e: Exception,
    connection: Any,
    cursor: Any,
    parent_fn: Any,
) -> bool:
    try:
        sqlstate = _extract_sqlstate(e)
        if sqlstate in _NON_DISCONNECT_SQLSTATES or type(e).__name__ == "SerializationFailure":
            return False
        if sqlstate is not None and (sqlstate.startswith("08") or sqlstate in _DISCONNECT_SQLSTATES):
            return True
        if getattr(connection, "closed", False) or getattr(getattr(connection, "_connection", None), "closed", False):
            return True
        if _is_grpc_unavailable(e):
            return True
        msg = str(e).lower()
        if any(phrase in msg for phrase in _SPANNER_DISCONNECT_PHRASES):
            return True
        guarded_parent = parent_fn if callable(parent_fn) and getattr(dialect_obj, "dbapi", None) is not None else None
        return _is_disconnect_common(e, connection, cursor, guarded_parent)
    except Exception:
        return False


class _SpannerDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for Cloud Spanner GoogleSQL (+vtb / +vtb_async)."""

    _json_serializer: Any
    _json_deserializer: Any

    def __init__(
        self,
        json_serializer: Any = None,
        json_deserializer: Any = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Cloud Spanner dialect while preserving optional JSON serializer hooks."""
        super().__init__(**kwargs)
        if json_serializer is not None:
            self._json_serializer = json_serializer
        if json_deserializer is not None:
            self._json_deserializer = json_deserializer

    def do_execute(self, cursor: Any, statement: str, parameters: Any, context: Any = None) -> None:
        """Execute a statement directly through the Vertebra DBAPI cursor."""
        _ = context
        cursor.execute(statement, parameters)

    def do_execute_no_params(self, cursor: Any, statement: str, context: Any = None) -> None:
        """Execute a parameterless statement directly through the Vertebra DBAPI cursor."""
        _ = context
        cursor.execute(statement)

    def do_executemany(self, cursor: Any, statement: str, parameters: Any, context: Any = None) -> None:
        """Execute a batch statement directly through the Vertebra DBAPI cursor."""
        _ = context
        cursor.executemany(statement, parameters)

    def do_commit(self, dbapi_connection: Any) -> None:
        """Commit via the Vertebra DBAPI connection without spanner_dbapi trace attributes."""
        dbapi_connection.commit()

    def do_rollback(self, dbapi_connection: Any) -> None:
        """Roll back via the Vertebra DBAPI connection without spanner_dbapi unwrapping."""
        dbapi_connection.rollback()

    def do_close(self, dbapi_connection: Any) -> None:
        """Close the Vertebra DBAPI connection cleanly."""
        dbapi_connection.close()

    def get_isolation_level_values(self, dbapi_conn: Any) -> Sequence[IsolationLevel]:
        """Return supported Cloud Spanner GoogleSQL isolation levels."""
        _ = dbapi_conn
        return ("AUTOCOMMIT", "SERIALIZABLE", "REPEATABLE READ")

    def detect_autocommit_setting(self, dbapi_conn: Any) -> bool:
        """Return the current DBAPI connection autocommit setting."""
        return bool(getattr(dbapi_conn, "autocommit", False))

    def get_isolation_level(self, dbapi_connection: Any) -> IsolationLevel:
        """Return the active Cloud Spanner isolation level without spanner_dbapi proxies."""
        if getattr(dbapi_connection, "autocommit", False):
            return "AUTOCOMMIT"
        level = getattr(dbapi_connection, "isolation_level", None)
        return cast("IsolationLevel", str(level).upper()) if level else "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """Apply Cloud Spanner isolation level or autocommit mode on the DBAPI connection."""
        normalized = level.upper()
        if normalized == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
            return
        if normalized not in _SPANNER_ISOLATION_LEVELS:
            msg = f"Invalid Cloud Spanner isolation level: {level!r}"
            raise exc.ArgumentError(msg)
        dbapi_connection.autocommit = False

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True for SQLSTATE 08*, gRPC UNAVAILABLE, or closed-channel errors, never raising."""
        return _check_spanner_disconnect(self, e, connection, cursor, getattr(super(), "is_disconnect", None))


class _SpangresDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for Cloud Spanner PostgreSQL interface (Spangres)."""

    supports_twophase = False

    def get_isolation_level_values(self, dbapi_conn: Any) -> Any:
        """Return supported Spangres isolation levels including AUTOCOMMIT."""
        _ = dbapi_conn
        return ("AUTOCOMMIT", "SERIALIZABLE", "REPEATABLE READ")

    def detect_autocommit_setting(self, dbapi_conn: Any) -> bool:
        """Return the current DBAPI connection autocommit setting."""
        return bool(getattr(dbapi_conn, "autocommit", False))

    def get_isolation_level(self, dbapi_connection: Any) -> IsolationLevel:
        """Query the active Spangres transaction isolation level."""
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SHOW TRANSACTION ISOLATION LEVEL")
            row = cursor.fetchone()
        finally:
            cursor.close()
        if row is not None and row[0] is not None:
            return cast("IsolationLevel", str(row[0]).upper())
        return "AUTOCOMMIT" if getattr(dbapi_connection, "autocommit", False) else "SERIALIZABLE"

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """Apply Spangres isolation level, toggling autocommit for AUTOCOMMIT."""
        normalized = level.upper()
        if normalized == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
            return
        if normalized not in _SPANNER_ISOLATION_LEVELS:
            msg = f"Invalid Spangres isolation level: {level!r}"
            raise exc.ArgumentError(msg)
        dbapi_connection.autocommit = False
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {normalized}")
            cursor.execute("COMMIT")
        finally:
            cursor.close()

    def _twophase_unsupported(self) -> NoReturn:
        msg = "Cloud Spanner Spangres does not support two-phase commit (XA / PREPARE TRANSACTION)"
        raise exc.NotSupportedError(None, None, DbapiNotSupportedError(msg))

    def do_begin_twophase(self, connection: Any, xid: Any) -> None:
        """Reject two-phase commit begin because Spangres does not support XA transactions."""
        _ = (connection, xid)
        self._twophase_unsupported()

    def do_prepare_twophase(self, connection: Any, xid: Any) -> None:
        """Reject two-phase commit prepare because Spangres does not support PREPARE TRANSACTION."""
        _ = (connection, xid)
        self._twophase_unsupported()

    def do_commit_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        """Reject two-phase commit commit because Spangres does not support COMMIT PREPARED."""
        _ = (connection, xid, is_prepared, recover)
        self._twophase_unsupported()

    def do_rollback_twophase(
        self,
        connection: Any,
        xid: Any,
        is_prepared: bool = True,
        recover: bool = False,
    ) -> None:
        """Reject two-phase commit rollback because Spangres does not support ROLLBACK PREPARED."""
        _ = (connection, xid, is_prepared, recover)
        self._twophase_unsupported()

    def do_recover_twophase(self, connection: Any) -> list[Any]:
        """Reject two-phase commit recovery because Spangres has no pg_prepared_xacts."""
        _ = connection
        self._twophase_unsupported()

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True for SQLSTATE 08*/57P01-57P03, gRPC UNAVAILABLE, or closed connections."""
        return _check_spanner_disconnect(self, e, connection, cursor, getattr(super(), "is_disconnect", None))


class SyncSpannerDialect(_SpannerDialectMixin, _SyncDialectMixin, _BaseSpannerDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner GoogleSQL (+vtb / +vertebra)."""

    name = "spanner"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return BrokerPool when ?broker=true is enabled, otherwise NullPool."""
        if _wants_broker_pool(url):
            return _SyncDialectMixin.get_pool_class(url)
        return NullPool


class AsyncSpannerDialect(_SpannerDialectMixin, _AsyncDialectMixin, _BaseSpannerDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Cloud Spanner GoogleSQL (+vtb_async / +vertebra_async)."""

    name = "spanner"
    driver = "vtb_async"
    is_async = True
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return AsyncBrokerPool when ?broker=true is enabled, otherwise NullPool."""
        if _wants_broker_pool(url):
            return _AsyncDialectMixin.get_pool_class(url)
        return NullPool


class SyncSpangresDialect(_SpangresDialectMixin, _SyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for Spanner Spangres (+vtb / +vertebra)."""

    name = "spangres"
    driver = "vtb"
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    positional = True
    supports_statement_cache = True
    supports_native_jsonb = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return BrokerPool when ?broker=true is enabled, otherwise NullPool."""
        if _wants_broker_pool(url):
            return _SyncDialectMixin.get_pool_class(url)
        return NullPool


class AsyncSpangresDialect(_SpangresDialectMixin, _AsyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Spanner Spangres (+vtb_async / +vertebra_async)."""

    name = "spangres"
    driver = "vtb_async"
    is_async = True
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    positional = True
    supports_statement_cache = True
    supports_native_jsonb = True

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return AsyncBrokerPool when ?broker=true is enabled, otherwise NullPool."""
        if _wants_broker_pool(url):
            return _AsyncDialectMixin.get_pool_class(url)
        return NullPool
