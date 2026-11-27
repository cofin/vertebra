"""SQLAlchemy 2.1 synchronous and asynchronous dialects for SQLite (+vtb / +vertebra)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.sqlite.base import SQLiteDialect as BaseSQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, Pool, QueuePool, StaticPool

from vertebra.extensions.sqlalchemy.dialects._base import (
    _AsyncDialectMixin,
    _resolve_broker_pool_class,
    _SyncDialectMixin,
)

__all__ = (
    "AsyncSQLiteDialect",
    "SyncSQLiteDialect",
)

_SQLITE_DISCONNECT_PHRASES: tuple[str, ...] = (
    "cannot operate on a closed",
    "closed database",
    "closed connection",
    "driver is closed",
)
_SQLITE_NON_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"23505", "40001", "40P01"})


class _SQLiteDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for SQLite (+vtb / +vtb_async)."""

    supports_statement_cache = True

    @classmethod
    def _is_url_file_db(cls, url: URL) -> bool:
        """Return True when the SQLite URL targets a file-backed database."""
        return (
            bool(url.database and url.database not in {":memory:", "memory://"}) and url.query.get("mode") != "memory"
        )

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory URLs and bound SyncBrokerPool, NullPool, or QueuePool for files."""
        is_async = bool(getattr(cls, "is_async", False))
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if (
            query.get("shm_broker") in {"true", "1", "yes"}
            or query.get("broker") in {"true", "1", "yes"}
            or query.get("broker_pool") in {"true", "1", "yes"}
        ):
            return _resolve_broker_pool_class(url, is_async=is_async)
        if not cls._is_url_file_db(url):
            return StaticPool
        return AsyncAdaptedQueuePool if is_async else QueuePool

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return server version info without importing stdlib sqlite3."""
        _ = connection
        version = getattr(self, "server_version_info", None)
        return version if isinstance(version, tuple) else (3, 45, 0)

    def do_ping(self, dbapi_connection: Any) -> bool:
        """Execute SELECT 1 with autocommit enabled so no open transaction remains."""
        has_autocommit = hasattr(dbapi_connection, "autocommit")
        before_autocommit = bool(dbapi_connection.autocommit) if has_autocommit else False
        if has_autocommit and not before_autocommit:
            dbapi_connection.autocommit = True
        cursor = dbapi_connection.cursor()
        try:
            select_one = getattr(self, "_dialect_specific_select_one", "SELECT 1")
            cursor.execute(select_one)
        finally:
            cursor.close()
            if has_autocommit and not before_autocommit:
                dbapi_connection.autocommit = before_autocommit
                get_driver_fn = getattr(self, "get_driver_connection", None)
                driver_conn = get_driver_fn(dbapi_connection) if callable(get_driver_fn) else dbapi_connection
                driver = getattr(driver_conn, "driver", None) or getattr(driver_conn, "_driver", None)
                if not getattr(driver_conn, "closed", False) and (
                    getattr(driver_conn, "_in_transaction", False) or getattr(driver, "in_transaction", False)
                ):
                    dbapi_connection.rollback()
        return True

    _non_disconnect_sqlstates = _SQLITE_NON_DISCONNECT_SQLSTATES
    _extra_disconnect_phrases = _SQLITE_DISCONNECT_PHRASES


class SyncSQLiteDialect(_SQLiteDialectMixin, _SyncDialectMixin, BaseSQLiteDialect):
    """SQLAlchemy 2.1 synchronous dialect for SQLite (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    server_version_info: tuple[int, ...] = (3, 45, 0)


class AsyncSQLiteDialect(_SQLiteDialectMixin, _AsyncDialectMixin, BaseSQLiteDialect):
    """SQLAlchemy 2.1 asynchronous dialect for SQLite (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    server_version_info: tuple[int, ...] = (3, 45, 0)
