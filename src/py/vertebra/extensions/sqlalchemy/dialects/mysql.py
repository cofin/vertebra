"""SQLAlchemy 2.1 synchronous and asynchronous dialects for MySQL (+vtb / +vertebra)."""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.mysql.base import MySQLDialect as BaseMySQLDialect

from vertebra.extensions.sqlalchemy.dialects._base import (
    _AsyncDialectMixin,
    _extract_vendor_code,
    _SyncDialectMixin,
)

__all__ = (
    "AsyncMySQLDialect",
    "SyncMySQLDialect",
)

_MYSQL_DISCONNECT_CODES: frozenset[int] = frozenset({1927, 2006, 2013, 2014, 2045, 2055, 4031})


class _MySQLDialectMixin:
    """Shared MySQL dialect behavior for vendor error extraction, charset detection, and disconnects."""

    def _detect_charset(self, connection: Any) -> str:
        """Detect the active connection character set, defaulting to utf8mb4."""
        raw_conn = getattr(connection, "connection", None)
        for candidate in (
            raw_conn,
            getattr(raw_conn, "driver_connection", None),
            getattr(raw_conn, "dbapi_connection", None),
            getattr(raw_conn, "_connection", None),
            getattr(raw_conn, "driver", None),
        ):
            if candidate is None:
                continue
            cset_fn = getattr(candidate, "character_set_name", None)
            if callable(cset_fn):
                name = cset_fn()
                if isinstance(name, str) and name:
                    return name
            charset = getattr(candidate, "charset", None)
            if isinstance(charset, str) and charset:
                return charset
        try:
            row = connection.exec_driver_sql("SELECT @@character_set_client").first()
            if row and row[0]:
                val = row[0]
                return val.decode() if isinstance(val, bytes) else str(val)
        except Exception:
            pass
        return "utf8mb4"

    def _extract_error_code(self, exception: BaseException) -> int | None:
        """Extract an integer MySQL error code from vendor_code, __cause__, or args[0]."""
        seen: set[int] = set()
        current: BaseException | None = exception
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            vendor_code = _extract_vendor_code(current)
            if vendor_code is not None:
                return vendor_code
            args = getattr(current, "args", ())
            if args:
                first = args[0]
                if isinstance(first, int) and not isinstance(first, bool):
                    return first
                if isinstance(first, BaseException):
                    current = first
                    continue
            current = getattr(current, "__cause__", None)
        return None

    _disconnect_codes = _MYSQL_DISCONNECT_CODES
    _guard_parent_dbapi = True

    def _extract_error_code_for_disconnect(self, e: Exception) -> int | None:
        return self._extract_error_code(e)


class SyncMySQLDialect(_MySQLDialectMixin, _SyncDialectMixin, BaseMySQLDialect):
    """SQLAlchemy 2.1 synchronous dialect for MySQL (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"


class AsyncMySQLDialect(_MySQLDialectMixin, _AsyncDialectMixin, BaseMySQLDialect):
    """SQLAlchemy 2.1 asynchronous dialect for MySQL (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
