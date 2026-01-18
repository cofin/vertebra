"""Bun-backed MySQL driver over IPC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vertebra._internal.ipc import IpcClient, IpcConfig
from vertebra.drivers.base import (
    AsyncConnection,
    AsyncCursor,
    AsyncDriver,
    AsyncPool,
    SyncConnection,
    SyncCursor,
    SyncDriver,
    SyncPool,
)
from vertebra.drivers.exceptions import DatabaseError, format_error_message, map_error_code

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable, Sequence
    from types import TracebackType


def _run_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    msg = "Use AsyncMySQLConnection inside an event loop"
    raise RuntimeError(msg)


def _raise_for_error(response: dict[str, Any]) -> None:
    if response.get("ok"):
        return
    error = response.get("error") or {}
    code = error.get("code")
    message = error.get("message") or "Driver error"
    exc_type = map_error_code(str(code)) if code else DatabaseError
    raise exc_type(format_error_message(str(code) if code else None, str(message)))


@dataclass(slots=True)
class SyncMySQLDriverConfig:
    connection_string: str = "mysql://localhost/mysql"
    ipc: IpcConfig = field(
        default_factory=lambda: IpcConfig(transport="tcp", host="127.0.0.1", port=7006, enforce_bun=False)
    )
    pool_max: int | None = None


class SyncMySQLCursor(SyncCursor):
    def __init__(self, connection: "SyncMySQLConnection") -> None:
        self._connection = connection
        self._rows: list[tuple[Any, ...]] = []
        self._columns: list[str] = []
        self._index = 0

    def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        result = self._connection._execute(query, params or [])
        self._columns = list(result.get("columns", []))
        self._rows = [tuple(row) for row in result.get("rows", [])]
        self._index = 0
        return result.get("rowcount")

    def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        total = 0
        for param_set in params:
            result = self._connection._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    def fetchone(self) -> Any:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[Any]:
        count = size or 1
        rows = self._rows[self._index : self._index + count]
        self._index += len(rows)
        return rows

    def fetchall(self) -> list[Any]:
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    def close(self) -> None:
        self._rows = []
        self._columns = []
        self._index = 0


class AsyncMySQLCursor(AsyncCursor):
    def __init__(self, connection: "AsyncMySQLConnection") -> None:
        self._connection = connection
        self._rows: list[tuple[Any, ...]] = []
        self._columns: list[str] = []
        self._index = 0

    async def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        result = await self._connection._execute(query, params or [])
        self._columns = list(result.get("columns", []))
        self._rows = [tuple(row) for row in result.get("rows", [])]
        self._index = 0
        return result.get("rowcount")

    async def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        total = 0
        for param_set in params:
            result = await self._connection._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    async def fetchone(self) -> Any:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        count = size or 1
        rows = self._rows[self._index : self._index + count]
        self._index += len(rows)
        return rows

    async def fetchall(self) -> list[Any]:
        rows = self._rows[self._index :]
        self._index = len(self._rows)
        return rows

    async def close(self) -> None:
        self._rows = []
        self._columns = []
        self._index = 0


class SyncMySQLConnection(SyncConnection):
    def __init__(self, client: IpcClient, connection_id: str) -> None:
        self._client = client
        self._connection_id = connection_id

    def cursor(self) -> SyncCursor:
        return SyncMySQLCursor(self)

    def commit(self) -> None:
        return

    def rollback(self) -> None:
        return

    def close(self) -> None:
        _run_sync(
            self._client.request({
                "id": "close",
                "driver": "mysql",
                "method": "close",
                "params": {"connectionId": self._connection_id},
            })
        )
        _run_sync(self._client.close())

    def transaction(self, isolation: str | None = None) -> "SyncMySQLTransaction":
        return SyncMySQLTransaction(self, isolation=isolation)

    def _execute(self, query: str, params: Sequence[Any]) -> dict[str, Any]:
        response = _run_sync(
            self._client.request({
                "id": "execute",
                "driver": "mysql",
                "method": "execute",
                "params": {"connectionId": self._connection_id, "sql": query, "params": list(params)},
            })
        )
        _raise_for_error(response)
        return response.get("result", {})


class AsyncMySQLConnection(AsyncConnection):
    def __init__(
        self, client: IpcClient, connection_string: str, connection_id: str | None = None, pool_max: int | None = None
    ) -> None:
        self._client = client
        self._connection_string = connection_string
        self._connection_id = connection_id
        self._pool_max = pool_max

    def cursor(self) -> AsyncCursor:
        return AsyncMySQLCursor(self)

    async def commit(self) -> None:
        return

    async def rollback(self) -> None:
        return

    async def _ensure_connection(self) -> str:
        if self._connection_id:
            return self._connection_id
        params: dict[str, Any] = {"connectionString": self._connection_string}
        if self._pool_max:
            params["pool"] = {"max": self._pool_max}
        response = await self._client.request({
            "id": "connect",
            "driver": "mysql",
            "method": "connect",
            "params": params,
        })
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            msg = "Connection id missing from response"
            raise DatabaseError(msg)
        self._connection_id = connection_id
        return connection_id

    async def fetch(self, query: str, params: Sequence[Any] | None = None) -> list[Any]:
        result = await self._execute(query, params or [])
        return [tuple(row) for row in result.get("rows", [])]

    async def fetchrow(self, query: str, params: Sequence[Any] | None = None) -> Any:
        rows = await self.fetch(query, params)
        return rows[0] if rows else None

    async def fetchval(self, query: str, params: Sequence[Any] | None = None) -> Any:
        row = await self.fetchrow(query, params)
        if row:
            return row[0]
        return None

    async def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        result = await self._execute(query, params or [])
        return result.get("rowcount")

    async def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        total = 0
        for param_set in params:
            result = await self._execute(query, param_set)
            total += result.get("rowcount", 0)
        return total

    async def close(self) -> None:
        if not self._connection_id:
            return
        await self._client.request({
            "id": "close",
            "driver": "mysql",
            "method": "close",
            "params": {"connectionId": self._connection_id},
        })
        await self._client.close()

    def transaction(self, isolation: str | None = None) -> "AsyncMySQLTransaction":
        return AsyncMySQLTransaction(self, isolation=isolation)

    async def _execute(self, query: str, params: Sequence[Any]) -> dict[str, Any]:
        connection_id = await self._ensure_connection()
        response = await self._client.request({
            "id": "execute",
            "driver": "mysql",
            "method": "execute",
            "params": {"connectionId": connection_id, "sql": query, "params": list(params)},
        })
        _raise_for_error(response)
        return response.get("result", {})


class SyncMySQLPool(SyncPool):
    def __init__(self, driver: "SyncMySQLDriver", min_size: int = 1, max_size: int = 5) -> None:
        self._driver = driver
        self._min_size = min_size
        self._max_size = max_size

    def acquire(self) -> SyncConnection:
        return self._driver._connect(pooled=True, pool_max=self._max_size)

    def release(self, connection: SyncConnection) -> None:
        connection.close()

    def close(self) -> None:
        return

    def get_size(self) -> int:
        return self._max_size

    def get_idle_size(self) -> int:
        return self._min_size


class AsyncMySQLPool(AsyncPool):
    def __init__(self, driver: "AsyncMySQLDriver", min_size: int = 1, max_size: int = 5) -> None:
        self._driver = driver
        self._min_size = min_size
        self._max_size = max_size

    async def acquire(self) -> AsyncConnection:
        return await self._driver._connect(pool_max=self._max_size)

    async def release(self, connection: AsyncConnection) -> None:
        await connection.close()

    async def close(self) -> None:
        return

    def get_size(self) -> int:
        return self._max_size

    def get_idle_size(self) -> int:
        return self._min_size


class SyncMySQLDriver(SyncDriver):
    def __init__(self, config: SyncMySQLDriverConfig | None = None) -> None:
        self._config = config or SyncMySQLDriverConfig()

    def _connect(self, pooled: bool = False, pool_max: int | None = None) -> SyncConnection:
        client = IpcClient(self._config.ipc)
        params: dict[str, Any] = {"connectionString": self._config.connection_string}
        if pooled:
            params["pool"] = {"max": pool_max or self._config.pool_max or 10}
        response = _run_sync(
            client.request({"id": "connect", "driver": "mysql", "method": "connect", "params": params})
        )
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            msg = "Connection id missing from response"
            raise DatabaseError(msg)
        return SyncMySQLConnection(client, connection_id)

    def connect(self) -> SyncConnection:
        return self._connect(pooled=False)

    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncPool:
        return SyncMySQLPool(self, min_size=min_size, max_size=max_size)


class AsyncMySQLDriver(AsyncDriver):
    def __init__(self, config: SyncMySQLDriverConfig | None = None) -> None:
        self._config = config or SyncMySQLDriverConfig()

    async def _connect(self, pool_max: int | None = None) -> AsyncMySQLConnection:
        client = IpcClient(self._config.ipc)
        connection = AsyncMySQLConnection(
            client, self._config.connection_string, pool_max=pool_max or self._config.pool_max
        )
        await connection._ensure_connection()
        return connection

    async def connect(self) -> AsyncConnection:
        return await self._connect()

    async def pool(self, min_size: int = 1, max_size: int = 10) -> AsyncPool:
        return AsyncMySQLPool(self, min_size=min_size, max_size=max_size)


class SyncMySQLTransaction:
    def __init__(self, connection: SyncMySQLConnection, isolation: str | None = None) -> None:
        self._connection = connection
        self._isolation = isolation

    def __enter__(self) -> "SyncMySQLTransaction":
        statement = "BEGIN"
        if self._isolation:
            statement = f"BEGIN ISOLATION LEVEL {self._isolation}"
        self._connection._execute(statement, [])
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc_type:
            self._connection._execute("ROLLBACK", [])
        else:
            self._connection._execute("COMMIT", [])


class AsyncMySQLTransaction:
    def __init__(self, connection: AsyncMySQLConnection, isolation: str | None = None) -> None:
        self._connection = connection
        self._isolation = isolation

    async def __aenter__(self) -> "AsyncMySQLTransaction":
        statement = "BEGIN"
        if self._isolation:
            statement = f"BEGIN ISOLATION LEVEL {self._isolation}"
        await self._connection._execute(statement, [])
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc_type:
            await self._connection._execute("ROLLBACK", [])
        else:
            await self._connection._execute("COMMIT", [])


__all__ = (
    "AsyncMySQLConnection",
    "AsyncMySQLCursor",
    "AsyncMySQLDriver",
    "AsyncMySQLPool",
    "AsyncMySQLTransaction",
    "SyncMySQLConnection",
    "SyncMySQLCursor",
    "SyncMySQLDriver",
    "SyncMySQLDriverConfig",
    "SyncMySQLPool",
    "SyncMySQLTransaction",
)
