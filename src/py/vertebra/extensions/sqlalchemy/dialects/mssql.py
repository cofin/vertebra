"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Microsoft SQL Server (+vtb / +vertebra)."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import exc
from sqlalchemy.dialects.mssql.base import MSDialect, MSExecutionContext

from vertebra.extensions.sqlalchemy.dialects._base import (
    _NON_DISCONNECT_SQLSTATES,
    _AsyncDialectMixin,
    _extract_sqlstate,
    _extract_vendor_code,
    _SyncDialectMixin,
)

__all__ = (
    "AsyncMSSQLDialect",
    "MSExecutionContext_vtb",
    "SyncMSSQLDialect",
)

_MSSQL_DISCONNECT_SQLSTATES: frozenset[str] = frozenset(
    {
        "08S01",
        "08001",
        "08003",
        "08007",
        "08S02",
        "HYT00",
        "HY010",
    }
)
_MSSQL_VERSION_RE: re.Pattern[str] = re.compile(r"Microsoft .*? - (\d+)\.(\d+)\.(\d+)\.(\d+)")
_MAX_NEXTSET_ITERATIONS: int = 16


def _parse_dotted_version(raw: str) -> tuple[int, ...]:
    parts: list[int] = []
    for token in re.split(r"[.\-]", raw.strip()):
        if token.isdigit():
            parts.append(int(token))
        elif parts:
            break
    return tuple(parts)


def _inspect_raw_server_version_info(connection: Any) -> tuple[int, ...] | None:
    raw_conn = getattr(connection, "connection", None)
    driver = getattr(raw_conn, "driver", None)
    candidates = (
        raw_conn,
        getattr(raw_conn, "driver_connection", None),
        getattr(raw_conn, "dbapi_connection", None),
        getattr(raw_conn, "_connection", None),
        driver,
        getattr(driver, "client", None),
        getattr(driver, "_client", None),
    )
    for candidate in candidates:
        if candidate is None:
            continue
        info = getattr(candidate, "server_version_info", None)
        if isinstance(info, (tuple, list)) and info:
            try:
                return tuple(int(part) for part in info)
            except (TypeError, ValueError):
                continue
    return None


class MSExecutionContext_vtb(MSExecutionContext):
    """Execution context advancing result sets for T-SQL OUTPUT clauses."""

    def post_exec(self) -> None:
        """Advance past leading empty result sets before buffering OUTPUT rows."""
        if (
            self.compiled is not None
            and getattr(self.compiled, "effective_returning", None)
            and not self._embedded_scope_identity
            and not self._select_lastrowid
            and not self.cursor.description
        ):
            nextset_fn = getattr(self.cursor, "nextset", None)
            if callable(nextset_fn):
                for _ in range(_MAX_NEXTSET_ITERATIONS):
                    if self.cursor.description:
                        break
                    if not nextset_fn():
                        break
        parent_post_exec: Any = super().post_exec
        parent_post_exec()


class _MSSQLDialectMixin:
    """Shared SQL Server dialect behavior for version detection, OUTPUT, and disconnects."""

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Resolve SQL Server version tuple from driver metadata, ProductVersion, or @@VERSION."""
        raw_info = _inspect_raw_server_version_info(connection)
        if raw_info is not None:
            return raw_info

        raw_version: Any = None
        try:
            raw_version = connection.exec_driver_sql(
                "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR)"
            ).scalar()
        except exc.DBAPIError:
            raw_version = None

        if isinstance(raw_version, bytes):
            raw_version = raw_version.decode("utf-8", errors="ignore")
        if isinstance(raw_version, str) and raw_version.strip():
            parsed = _parse_dotted_version(raw_version)
            if parsed:
                return parsed

        try:
            banner = connection.exec_driver_sql("SELECT @@VERSION").scalar()
        except exc.DBAPIError:
            return ()

        if isinstance(banner, bytes):
            banner = banner.decode("utf-8", errors="ignore")
        if isinstance(banner, str):
            match = _MSSQL_VERSION_RE.search(banner)
            if match is not None:
                return tuple(int(group) for group in match.groups())
            fallback_match = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", banner)
            if fallback_match is not None:
                return tuple(int(group) for group in fallback_match.groups())
        return ()

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True if the exception represents an MSSQL disconnect, never raising."""
        try:
            sqlstate = _extract_sqlstate(e)
            if sqlstate in _NON_DISCONNECT_SQLSTATES or type(e).__name__ == "SerializationFailure":
                return False
            if sqlstate is not None and (sqlstate in _MSSQL_DISCONNECT_SQLSTATES or sqlstate.startswith("08")):
                return True
            vendor_code = _extract_vendor_code(e)
            if vendor_code == 10054:
                return True
        except Exception:
            return False
        parent = super()
        parent_fn = getattr(parent, "is_disconnect", None)
        if callable(parent_fn):
            try:
                return bool(parent_fn(e, connection, cursor))
            except Exception:
                return False
        return False


class SyncMSSQLDialect(_MSSQLDialectMixin, _SyncDialectMixin, MSDialect):
    """SQLAlchemy 2.1 synchronous dialect for Microsoft SQL Server (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
    execution_ctx_cls = MSExecutionContext_vtb


class AsyncMSSQLDialect(_MSSQLDialectMixin, _AsyncDialectMixin, MSDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Microsoft SQL Server (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    default_paramstyle = "qmark"
    paramstyle = "qmark"
    execution_ctx_cls = MSExecutionContext_vtb
