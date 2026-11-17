"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Google BigQuery (+vtb / +vertebra)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.pool import NullPool

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
    from sqlalchemy.engine.default import DefaultDialect as _BaseBigQueryDialect
    from sqlalchemy.engine.url import URL
    from sqlalchemy.pool import Pool
else:
    _BaseBigQueryDialect = ensure_module(
        "sqlalchemy_bigquery",
        package="vertebra-core[sqlalchemy-gcp]",
        extra="sqlalchemy-gcp",
        install_package="vertebra-core[sqlalchemy-gcp]",
    ).BigQueryDialect

__all__ = (
    "AsyncBigQueryDialect",
    "SyncBigQueryDialect",
)

_GRPC_UNAVAILABLE_CODE = 14
_GCP_DISCONNECT_PHRASES: tuple[str, ...] = (
    "unavailable",
    "channel closed",
    "closed channel",
    "channel is closed",
    "transport is closing",
    "goaway",
    "reset by peer",
)


def _is_grpc_unavailable(exc: BaseException) -> bool:
    for candidate in (exc, getattr(exc, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "code", None)
        if getattr(code, "name", None) == "UNAVAILABLE":
            return True
    return _extract_vendor_code(exc) == _GRPC_UNAVAILABLE_CODE


def _wants_broker_pool(url: URL) -> bool:
    query: dict[str, Any] = dict(url.query) if url.query else {}
    if query.get("shm_broker") in {"true", "1", "yes"}:
        return False
    return query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}


class _BigQueryDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for Google BigQuery (+vtb / +vtb_async)."""

    _json_serializer: Any
    _json_deserializer: Any

    def __init__(
        self,
        json_serializer: Any = None,
        json_deserializer: Any = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the BigQuery dialect while preserving optional JSON serializer hooks."""
        super().__init__(**kwargs)
        if json_serializer is not None:
            self._json_serializer = json_serializer
        if json_deserializer is not None:
            self._json_deserializer = json_deserializer

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True for SQLSTATE 08*, gRPC UNAVAILABLE, or closed-channel errors, never raising."""
        try:
            sqlstate = _extract_sqlstate(e)
            if sqlstate in _NON_DISCONNECT_SQLSTATES or type(e).__name__ == "SerializationFailure":
                return False
            if sqlstate is not None and (sqlstate.startswith("08") or sqlstate in _DISCONNECT_SQLSTATES):
                return True
            if getattr(connection, "closed", False) or getattr(
                getattr(connection, "_connection", None), "closed", False
            ):
                return True
            if _is_grpc_unavailable(e):
                return True
            msg = str(e).lower()
            if any(phrase in msg for phrase in _GCP_DISCONNECT_PHRASES):
                return True
            parent = super()
            raw_parent_fn = getattr(parent, "is_disconnect", None)
            parent_fn = raw_parent_fn if callable(raw_parent_fn) and getattr(self, "dbapi", None) is not None else None
            return _is_disconnect_common(e, connection, cursor, parent_fn)
        except Exception:
            return False


class SyncBigQueryDialect(_BigQueryDialectMixin, _SyncDialectMixin, _BaseBigQueryDialect):
    """SQLAlchemy 2.1 synchronous dialect for Google BigQuery (+vtb / +vertebra)."""

    name = "bigquery"
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


class AsyncBigQueryDialect(_BigQueryDialectMixin, _AsyncDialectMixin, _BaseBigQueryDialect):
    """SQLAlchemy 2.1 asynchronous dialect for Google BigQuery (+vtb_async / +vertebra_async)."""

    name = "bigquery"
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
