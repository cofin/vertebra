"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Cloud Spanner GoogleSQL and Spangres (+vtb / +vertebra)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, NoReturn, cast

from sqlalchemy import exc
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine.interfaces import IsolationLevel

from vertebra.db.dbapi import NotSupportedError as DbapiNotSupportedError
from vertebra.extensions.sqlalchemy.dialects._base import (
    _DISCONNECT_SQLSTATES,
    _GCP_DISCONNECT_PHRASES,
    _AsyncDialectMixin,
    _SyncDialectMixin,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect as _BaseSpannerDialect
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


class _SpannerCommonDisconnectMixin:
    """Declarative disconnect and pool-class defaults shared by Spanner and Spangres dialects."""

    _disconnect_sqlstates = _DISCONNECT_SQLSTATES
    _extra_disconnect_phrases = _GCP_DISCONNECT_PHRASES
    _check_connection_closed = True
    _check_grpc_unavailable = True
    _guard_parent_dbapi = True
    _use_null_pool_when_unbrokered = True


class _SpannerDialectMixin(_SpannerCommonDisconnectMixin):
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


class _SpangresDialectMixin(_SpannerCommonDisconnectMixin):
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


class SyncSpannerDialect(_SpannerDialectMixin, _SyncDialectMixin, _BaseSpannerDialect):
    """SQLAlchemy 2.1 synchronous dialect for Cloud Spanner GoogleSQL (+vtb / +vertebra)."""

    name = "spanner"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False


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


class SyncSpangresDialect(_SpangresDialectMixin, _SyncDialectMixin, PGDialect):
    """SQLAlchemy 2.1 synchronous dialect for Spanner Spangres (+vtb / +vertebra)."""

    name = "spangres"
    driver = "vtb"
    default_paramstyle = "numeric_dollar"
    paramstyle = "numeric_dollar"
    positional = True
    supports_statement_cache = True
    supports_native_jsonb = True


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
