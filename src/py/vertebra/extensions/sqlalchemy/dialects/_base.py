"""Shared mixins and execution contexts for SQLAlchemy 2.1 Vertebra dialects."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, ClassVar

from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, Pool, QueuePool

from vertebra.db.dbapi import SyncDBAPIModule
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, SyncBrokerPool
from vertebra.typing import ImproperConfigurationError

_GRPC_UNAVAILABLE_CODE: int = 14
_NON_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"40001", "40P01"})
_DISCONNECT_SQLSTATES: frozenset[str] = frozenset({"57P01", "57P02", "57P03"})
_DISCONNECT_VENDOR_CODES: frozenset[int] = frozenset(
    {
        1012,
        1927,
        2006,
        2013,
        2014,
        2045,
        2055,
        3113,
        3114,
        3135,
        4031,
        10054,
        12537,
    }
)
_DISCONNECT_PHRASES: tuple[str, ...] = (
    "connection closed",
    "connection reset",
    "broken pipe",
    "server closed the connection",
    "terminating connection",
    "connection refused",
    "connection lost",
    "driver closed",
    "closed connection",
    "disconnect",
    "socket closed",
)
_GCP_DISCONNECT_PHRASES: tuple[str, ...] = (
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


def _is_connection_closed(connection: Any) -> bool:
    return bool(
        getattr(connection, "closed", False) or getattr(getattr(connection, "_connection", None), "closed", False)
    )


def _extract_sqlstate(exc: BaseException) -> str | None:
    for candidate in (exc, getattr(exc, "__cause__", None)):
        if candidate is None:
            continue
        raw = getattr(candidate, "sqlstate", None)
        if raw is not None:
            normalized = str(raw).strip().upper()
            if normalized:
                return normalized
    return None


def _extract_vendor_code(exc: BaseException) -> int | None:
    for candidate in (exc, getattr(exc, "__cause__", None)):
        if candidate is None:
            continue
        for attr in ("vendor_code", "errno", "code"):
            raw = getattr(candidate, attr, None)
            if isinstance(raw, int) and not isinstance(raw, bool):
                return raw
            if isinstance(raw, str) and raw.isdigit():
                return int(raw)
    return None


def _is_disconnect_common(
    e: Exception,
    connection: Any,
    cursor: Any,
    super_is_disconnect: Callable[..., object] | None,
) -> bool:
    try:
        sqlstate = _extract_sqlstate(e)
        if sqlstate in _NON_DISCONNECT_SQLSTATES or type(e).__name__ == "SerializationFailure":
            return False
        if sqlstate is not None and (sqlstate.startswith("08") or sqlstate in _DISCONNECT_SQLSTATES):
            return True

        vendor_code = _extract_vendor_code(e)
        if vendor_code in _DISCONNECT_VENDOR_CODES:
            return True

        msg = str(e).lower()
        if any(phrase in msg for phrase in _DISCONNECT_PHRASES):
            return True

        if callable(super_is_disconnect):
            return bool(super_is_disconnect(e, connection, cursor))
    except Exception:
        return False
    return False


def _resolve_broker_pool_class(
    url: URL,
    *,
    is_async: bool,
    default_null_pool: bool = False,
) -> type[Pool]:
    query: dict[str, Any] = dict(url.query) if url.query else {}
    if query.get("shm_broker") in {"true", "1", "yes"}:
        return NullPool
    if query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}:
        broker = DatabaseRegistry.default_broker()
        if broker is None:
            msg = "No default broker is registered in DatabaseRegistry; register a broker before enabling ?broker=true"
            raise ImproperConfigurationError(msg)
        pool_cls: type[SyncBrokerPool] = AsyncBrokerPool if is_async else SyncBrokerPool
        return pool_cls.bind_class(
            broker=broker,
            url=url.render_as_string(hide_password=False),
        )
    if default_null_pool:
        return NullPool
    return AsyncAdaptedQueuePool if is_async else QueuePool


class SyncExecutionContext(DefaultExecutionContext):
    """Execution context for Vertebra synchronous dialects."""


class AsyncExecutionContext(DefaultExecutionContext):
    """Execution context for Vertebra asynchronous dialects."""


class _CommonDialectMixin:
    """Shared connection, pool-class, terminate, and declarative disconnect checks."""

    supports_statement_cache: bool = True
    _non_disconnect_sqlstates: ClassVar[frozenset[str]] = frozenset()
    _disconnect_sqlstates: ClassVar[frozenset[str]] = frozenset()
    _disconnect_codes: ClassVar[frozenset[int]] = frozenset()
    _non_disconnect_codes: ClassVar[frozenset[int]] = frozenset()
    _extra_disconnect_phrases: ClassVar[tuple[str, ...]] = ()
    _non_disconnect_phrases: ClassVar[tuple[str, ...]] = ()
    _disconnect_regex: ClassVar[re.Pattern[str] | None] = None
    _check_connection_closed: ClassVar[bool] = False
    _check_grpc_unavailable: ClassVar[bool] = False
    _use_null_pool_when_unbrokered: ClassVar[bool] = False
    _guard_parent_dbapi: ClassVar[bool] = False

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Pass the rendered URL explicitly so non-SQLite connections do not fall back to SQLite."""
        clean_url = url.difference_update_query(["shm_broker"]) if "shm_broker" in url.query else url
        return [], {"url": clean_url.render_as_string(hide_password=False)}

    def do_terminate(self, dbapi_connection: Any) -> None:
        """Terminate the DBAPI connection."""
        terminate_fn = getattr(dbapi_connection, "terminate", None)
        if callable(terminate_fn):
            terminate_fn()
            return
        dbapi_connection.close()

    def _extract_error_code_for_disconnect(self, e: Exception) -> int | None:
        return _extract_vendor_code(e)

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True if the exception represents a database disconnect, never raising."""
        try:
            sqlstate = _extract_sqlstate(e)
            if (
                sqlstate in _NON_DISCONNECT_SQLSTATES
                or (sqlstate is not None and sqlstate in self._non_disconnect_sqlstates)
                or type(e).__name__ == "SerializationFailure"
            ):
                return False
            if sqlstate is not None and (sqlstate.startswith("08") or sqlstate in self._disconnect_sqlstates):
                return True
            if self._check_connection_closed and _is_connection_closed(connection):
                return True
            vendor_code = self._extract_error_code_for_disconnect(e)
            if vendor_code is not None:
                if vendor_code in self._non_disconnect_codes:
                    return False
                if vendor_code in self._disconnect_codes:
                    return True
            if self._check_grpc_unavailable and _is_grpc_unavailable(e):
                return True
            if self._non_disconnect_phrases or self._extra_disconnect_phrases or self._disconnect_regex is not None:
                for candidate in (e, getattr(e, "orig", None), getattr(e, "__cause__", None)):
                    if candidate is None:
                        continue
                    msg = str(candidate).lower()
                    if any(phrase in msg for phrase in self._non_disconnect_phrases):
                        return False
                    if any(phrase in msg for phrase in self._extra_disconnect_phrases):
                        return True
                    if self._disconnect_regex is not None and self._disconnect_regex.search(msg):
                        return True
            raw_parent = getattr(super(), "is_disconnect", None)
            if self._guard_parent_dbapi and getattr(self, "dbapi", None) is None:
                parent_fn = None
            else:
                parent_fn = raw_parent if callable(raw_parent) else None
            return _is_disconnect_common(e, connection, cursor, parent_fn)
        except Exception:
            return False


class _SyncDialectMixin(_CommonDialectMixin):
    """Shared synchronous dialect functionality for Vertebra drivers."""

    is_async: bool = False

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "import_dbapi" not in cls.__dict__:
            type.__setattr__(cls, "import_dbapi", _SyncDialectMixin.__dict__["import_dbapi"])

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.1."""
        paramstyle = getattr(cls, "default_paramstyle", None) or getattr(cls, "paramstyle", None) or "qmark"
        dialect = getattr(cls, "name", None)
        return SyncDBAPIModule(paramstyle=paramstyle, dialect=dialect)

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return bound SyncBrokerPool, NullPool, or QueuePool based on broker configuration."""
        return _resolve_broker_pool_class(url, is_async=False, default_null_pool=cls._use_null_pool_when_unbrokered)

    def get_driver_connection(self, connection: Any) -> Any:
        """Return the underlying synchronous DBAPI connection."""
        return connection


class _AsyncDialectMixin(_CommonDialectMixin):
    """Shared asynchronous dialect functionality for Vertebra drivers."""

    is_async: bool = True
    has_terminate: bool = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "import_dbapi" not in cls.__dict__:
            type.__setattr__(cls, "import_dbapi", _AsyncDialectMixin.__dict__["import_dbapi"])

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.1."""
        paramstyle = getattr(cls, "default_paramstyle", None) or getattr(cls, "paramstyle", None) or "qmark"
        dialect = getattr(cls, "name", None)
        return SQLAlchemyAsyncDBAPI(paramstyle=paramstyle, dialect=dialect)

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return bound AsyncBrokerPool, NullPool, or AsyncAdaptedQueuePool based on broker configuration."""
        return _resolve_broker_pool_class(url, is_async=True, default_null_pool=cls._use_null_pool_when_unbrokered)

    def get_driver_connection(self, connection: Any) -> Any:
        """Unwrap the async adapter connection to return the underlying AsyncDBAPIConnection."""
        return getattr(connection, "_connection", connection)
