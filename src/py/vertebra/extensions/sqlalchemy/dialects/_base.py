"""Shared mixins and execution contexts for SQLAlchemy 2.1 Vertebra dialects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, Pool, QueuePool

from vertebra.db.dbapi import SyncDBAPIModule
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool

if TYPE_CHECKING:
    pass


class SyncExecutionContext(DefaultExecutionContext):
    """Execution context for Vertebra synchronous dialects."""


class AsyncExecutionContext(DefaultExecutionContext):
    """Execution context for Vertebra asynchronous dialects."""


class _SyncDialectMixin:
    """Shared synchronous dialect functionality for Vertebra drivers."""

    supports_statement_cache: bool = True
    is_async: bool = False

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Pass the rendered URL explicitly so non-SQLite connections do not fall back to SQLite."""
        clean_url = url.difference_update_query(["shm_broker"]) if "shm_broker" in url.query else url
        return [], {"url": clean_url.render_as_string(hide_password=False)}

    def do_begin(self, dbapi_connection: Any) -> None:
        """Begin through the sync connection or SQLAlchemy's async adaptation."""
        if hasattr(dbapi_connection, "run_async"):
            dbapi_connection.run_async(lambda conn: conn.begin())
        else:
            dbapi_connection.begin()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "import_dbapi" not in cls.__dict__:
            type.__setattr__(cls, "import_dbapi", _SyncDialectMixin.__dict__["import_dbapi"])

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the PEP 249 DBAPI module for SQLAlchemy 2.0/2.1."""
        paramstyle = getattr(cls, "paramstyle", "qmark")
        return SyncDBAPIModule(paramstyle=paramstyle)

    dbapi: Any = classmethod(lambda cls: cls.import_dbapi())

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return BrokerPool, NullPool, or QueuePool based on broker configuration."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}:
            return BrokerPool
        return QueuePool

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True if the exception represents a database disconnect or transient failure."""
        if getattr(e, "is_transient", False):
            return True
        msg = str(e).lower()
        phrases = (
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
        if any(p in msg for p in phrases):
            return True
        parent = super()
        if hasattr(parent, "is_disconnect"):
            return bool(parent.is_disconnect(e, connection, cursor))
        return False


class _AsyncDialectMixin:
    """Shared asynchronous dialect functionality for Vertebra drivers."""

    supports_statement_cache: bool = True
    is_async: bool = True

    def create_connect_args(self, url: URL) -> tuple[list[Any], dict[str, Any]]:
        """Pass the rendered URL explicitly so non-SQLite connections do not fall back to SQLite."""
        clean_url = url.difference_update_query(["shm_broker"]) if "shm_broker" in url.query else url
        return [], {"url": clean_url.render_as_string(hide_password=False)}

    def do_begin(self, dbapi_connection: Any) -> None:
        """Begin through the sync connection or SQLAlchemy's async adaptation."""
        if hasattr(dbapi_connection, "run_async"):
            dbapi_connection.run_async(lambda conn: conn.begin())
        else:
            dbapi_connection.begin()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "import_dbapi" not in cls.__dict__:
            type.__setattr__(cls, "import_dbapi", _AsyncDialectMixin.__dict__["import_dbapi"])

    @classmethod
    def import_dbapi(cls) -> Any:
        """Return the asynchronous DBAPI module for SQLAlchemy 2.0/2.1."""
        paramstyle = getattr(cls, "paramstyle", "qmark")
        return SQLAlchemyAsyncDBAPI(paramstyle=paramstyle)

    dbapi: Any = classmethod(lambda cls: cls.import_dbapi())

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return AsyncBrokerPool, NullPool, or AsyncAdaptedQueuePool based on broker configuration."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}:
            return AsyncBrokerPool
        return AsyncAdaptedQueuePool

    def is_disconnect(self, e: Exception, connection: Any = None, cursor: Any = None) -> bool:
        """Return True if the exception represents a database disconnect or transient failure."""
        if getattr(e, "is_transient", False):
            return True
        msg = str(e).lower()
        phrases = (
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
        if any(p in msg for p in phrases):
            return True
        parent = super()
        if hasattr(parent, "is_disconnect"):
            return bool(parent.is_disconnect(e, connection, cursor))
        return False
