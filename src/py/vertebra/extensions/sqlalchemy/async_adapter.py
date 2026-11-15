"""SQLAlchemy asyncio DBAPI adapter wrapping standalone ``vertebra.db.dbapi`` connections."""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection

from vertebra.db.dbapi import AsyncDBAPIConnection, AsyncDBAPIModule

__all__ = (
    "AsyncAdapt_dbapi_connection",
    "SQLAlchemyAsyncDBAPI",
)


class SQLAlchemyAsyncDBAPI(AsyncDBAPIModule):
    """SQLAlchemy asyncio DBAPI wrapper adapting ``AsyncDBAPIConnection`` via ``AsyncAdapt_dbapi_connection``."""

    def connect(self, *args: Any, **kwargs: Any) -> AsyncAdapt_dbapi_connection:
        """Establish an asynchronous connection wrapped in SQLAlchemy's ``AsyncAdapt_dbapi_connection``."""
        async_conn: AsyncDBAPIConnection = super().connect(*args, **kwargs)
        return AsyncAdapt_dbapi_connection(self, cast("Any", async_conn))
