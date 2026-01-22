"""Async PEP 249-like DBAPI implementation for Vertebra broker."""

import contextlib
from collections.abc import Sequence
from typing import Any

from vertebra.drivers.broker import VertebraAsyncConnection, VertebraAsyncCursor
from vertebra.resources.broker import AsyncBrokerClient

# DBAPI Globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"
sqlite_version_info = (3, 30, 0)
has_stop = False


class Connection:
    """Async DBAPI Connection."""

    def __init__(self, pool_name: str, client: AsyncBrokerClient | None = None, paramstyle: str = paramstyle) -> None:
        self.pool_name = pool_name
        self._client = client or AsyncBrokerClient()
        self.paramstyle = paramstyle
        self._token: str | None = None
        self._connection: VertebraAsyncConnection | None = None
        self._transaction_in_progress = False

    async def _get_connection(self) -> VertebraAsyncConnection:
        if self._connection is None:
            self._token = await self._client.acquire(self.pool_name)
            self._connection = VertebraAsyncConnection(self.pool_name, self._client, self._token)
        return self._connection

    def cursor(self) -> "Cursor":
        return Cursor(self)

    async def commit(self) -> None:
        if self._transaction_in_progress and self._connection:
            await self._connection.commit()
            self._transaction_in_progress = False

    async def rollback(self) -> None:
        if self._transaction_in_progress and self._connection:
            await self._connection.rollback()
            self._transaction_in_progress = False

    async def close(self) -> None:
        if self._connection:
            with contextlib.suppress(Exception):
                await self._connection.close()
            self._connection = None
            self._token = None


class Cursor:
    """Async DBAPI Cursor."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self._cursor: VertebraAsyncCursor | None = None
        self.description: Sequence[Any] | None = None
        self.rowcount = -1

    async def _get_inner_cursor(self) -> VertebraAsyncCursor:
        if self._cursor is None:
            conn = await self.connection._get_connection()
            self._cursor = conn.cursor()
        return self._cursor

    async def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        conn = await self.connection._get_connection()
        if not self.connection._transaction_in_progress:
            await conn.begin()
            self.connection._transaction_in_progress = True

        import re

        if self.connection.paramstyle == "numeric" and parameters:
            operation = re.sub(r":(\d+)", r"$\1", operation)

        inner = await self._get_inner_cursor()
        self.rowcount = await inner.execute(operation, parameters) or -1

        if inner._columns:
            self.description = [(col, None, None, None, None, None, None) for col in inner._columns]
        else:
            self.description = None
        return self

    async def fetchone(self) -> Any | None:
        inner = await self._get_inner_cursor()
        return await inner.fetchone()

    async def fetchall(self) -> Sequence[Any]:
        inner = await self._get_inner_cursor()
        return await inner.fetchall()

    async def close(self) -> None:
        if self._cursor:
            await self._cursor.close()
            self._cursor = None


async def connect(dsn: str, **kwargs: Any) -> Connection:
    style = kwargs.pop("paramstyle", paramstyle)
    return Connection(dsn, paramstyle=style, **kwargs)
