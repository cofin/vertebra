"""SQLAlchemy 2.1 synchronous and asynchronous dialects for SQLite (+vtb / +vertebra)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy.dialects.sqlite.base import SQLiteDialect as BaseSQLiteDialect
from sqlalchemy.engine.url import URL
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, Pool, QueuePool, StaticPool

from vertebra.extensions.sqlalchemy.dialects._base import _AsyncDialectMixin, _SyncDialectMixin
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool

if TYPE_CHECKING:
    pass

__all__ = (
    "AsyncSQLiteDialect",
    "SyncSQLiteDialect",
)


class SyncSQLiteDialect(_SyncDialectMixin, BaseSQLiteDialect):
    """SQLAlchemy 2.1 synchronous dialect for SQLite (+vtb / +vertebra)."""

    driver = "vtb"
    supports_statement_cache = True
    server_version_info: tuple[int, ...] = (3, 45, 0)

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}:
            return BrokerPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return QueuePool

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return server version info without importing stdlib sqlite3."""
        _ = connection
        return self.server_version_info


class AsyncSQLiteDialect(_AsyncDialectMixin, BaseSQLiteDialect):
    """SQLAlchemy 2.1 asynchronous dialect for SQLite (+vtb_async / +vertebra_async)."""

    driver = "vtb_async"
    is_async = True
    supports_statement_cache = True
    server_version_info: tuple[int, ...] = (3, 45, 0)

    @classmethod
    def get_pool_class(cls, url: URL) -> type[Pool]:
        """Return StaticPool for in-memory databases and NullPool when lease brokering is enabled."""
        query: dict[str, Any] = dict(url.query) if url.query else {}
        if query.get("shm_broker") in {"true", "1", "yes"}:
            return NullPool
        if query.get("broker") in {"true", "1", "yes"} or query.get("broker_pool") in {"true", "1", "yes"}:
            return AsyncBrokerPool
        if not url.database or url.database in {":memory:", "memory://", ""}:
            return StaticPool
        return AsyncAdaptedQueuePool

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...]:
        """Return server version info without importing stdlib sqlite3."""
        _ = connection
        return self.server_version_info
