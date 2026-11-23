"""SQLAlchemy 2.1 synchronous and asynchronous dialects for Google BigQuery (+vtb / +vertebra)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.extensions.sqlalchemy.dialects._base import (
    _DISCONNECT_SQLSTATES,
    _GCP_DISCONNECT_PHRASES,
    _AsyncDialectMixin,
    _SyncDialectMixin,
)
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlalchemy.engine.default import DefaultDialect as _BaseBigQueryDialect
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


class _BigQueryDialectMixin:
    """Shared SQLAlchemy 2.1 dialect behavior for Google BigQuery (+vtb / +vtb_async)."""

    _json_serializer: Any
    _json_deserializer: Any
    _disconnect_sqlstates = _DISCONNECT_SQLSTATES
    _extra_disconnect_phrases = _GCP_DISCONNECT_PHRASES
    _check_connection_closed = True
    _check_grpc_unavailable = True
    _guard_parent_dbapi = True
    _use_null_pool_when_unbrokered = True

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


class SyncBigQueryDialect(_BigQueryDialectMixin, _SyncDialectMixin, _BaseBigQueryDialect):
    """SQLAlchemy 2.1 synchronous dialect for Google BigQuery (+vtb / +vertebra)."""

    name = "bigquery"
    driver = "vtb"
    paramstyle = "named"
    default_paramstyle = "named"
    supports_statement_cache = True
    supports_native_jsonb = False
    positional = False


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
