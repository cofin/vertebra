"""Bun-backed PostgreSQL driver over IPC."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Iterable, Sequence

from vertebra._internal.ipc import IpcClient, IpcConfig
from vertebra.drivers.base import AsyncConnection, BaseDriver, Connection, Cursor, Pool
from vertebra.drivers.exceptions import DatabaseError, format_error_message, map_error_code


def _run_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("Use AsyncPostgresConnection inside an event loop")


def _raise_for_error(response: dict[str, Any]) -> None:
    if response.get("ok"):
        return
    error = response.get("error") or {}
    code = error.get("code")
    message = error.get("message") or "Driver error"
    exc_type = map_error_code(str(code)) if code else DatabaseError
    raise exc_type(format_error_message(str(code) if code else None, str(message)))


@dataclass(slots=True)
class PostgresDriverConfig:
    connection_string: str = "postgres://localhost/postgres"
    ipc: IpcConfig = IpcConfig(transport="tcp", host="127.0.0.1", port=7003, enforce_bun=False)


class PostgresCursor(Cursor):
    def __init__(self, connection: "PostgresConnection") -> None:
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


class PostgresConnection(Connection):
    def __init__(self, client: IpcClient, connection_id: str) -> None:
        self._client = client
        self._connection_id = connection_id

    def cursor(self) -> Cursor:
        return PostgresCursor(self)

    def commit(self) -> None:
        return

    def rollback(self) -> None:
        return

    def close(self) -> None:
        _run_sync(
            self._client.request(
                {
                    "id": "close",
                    "driver": "postgres",
                    "method": "close",
                    "params": {"connectionId": self._connection_id},
                }
            )
        )
        _run_sync(self._client.close())

    def _execute(self, query: str, params: Sequence[Any]) -> dict[str, Any]:
        response = _run_sync(
            self._client.request(
                {
                    "id": "execute",
                    "driver": "postgres",
                    "method": "execute",
                    "params": {
                        "connectionId": self._connection_id,
                        "sql": query,
                        "params": list(params),
                    },
                }
            )
        )
        _raise_for_error(response)
        return response.get("result", {})


class AsyncPostgresConnection(AsyncConnection):
    def __init__(self, client: IpcClient, connection_string: str, connection_id: str | None = None) -> None:
        self._client = client
        self._connection_string = connection_string
        self._connection_id = connection_id

    async def _ensure_connection(self) -> str:
        if self._connection_id:
            return self._connection_id
        response = await self._client.request(
            {
                "id": "connect",
                "driver": "postgres",
                "method": "connect",
                "params": {"connectionString": self._connection_string},
            }
        )
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            raise DatabaseError("Connection id missing from response")
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
        await self._client.request(
            {
                "id": "close",
                "driver": "postgres",
                "method": "close",
                "params": {"connectionId": self._connection_id},
            }
        )
        await self._client.close()

    async def _execute(self, query: str, params: Sequence[Any]) -> dict[str, Any]:
        connection_id = await self._ensure_connection()
        response = await self._client.request(
            {
                "id": "execute",
                "driver": "postgres",
                "method": "execute",
                "params": {
                    "connectionId": connection_id,
                    "sql": query,
                    "params": list(params),
                },
            }
        )
        _raise_for_error(response)
        return response.get("result", {})


class PostgresPool(Pool):
    def __init__(self, driver: "PostgresDriver", min_size: int = 1, max_size: int = 5) -> None:
        self._driver = driver
        self._min_size = min_size
        self._max_size = max_size

    def acquire(self) -> Connection:
        return self._driver.connect()

    def release(self, connection: Connection) -> None:
        connection.close()

    def close(self) -> None:
        return

    def get_size(self) -> int:
        return self._max_size

    def get_idle_size(self) -> int:
        return self._min_size


class PostgresDriver(BaseDriver):
    def __init__(self, config: PostgresDriverConfig | None = None) -> None:
        self._config = config or PostgresDriverConfig()

    def connect(self) -> Connection:
        client = IpcClient(self._config.ipc)
        response = _run_sync(
            client.request(
                {
                    "id": "connect",
                    "driver": "postgres",
                    "method": "connect",
                    "params": {"connectionString": self._config.connection_string},
                }
            )
        )
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            raise DatabaseError("Connection id missing from response")
        return PostgresConnection(client, connection_id)

    def connect_async(self) -> AsyncConnection:
        client = IpcClient(self._config.ipc)
        return AsyncPostgresConnection(client, self._config.connection_string)

    def pool(self, min_size: int = 1, max_size: int = 10) -> Pool:
        return PostgresPool(self, min_size=min_size, max_size=max_size)


__all__ = [
    "AsyncPostgresConnection",
    "PostgresConnection",
    "PostgresCursor",
    "PostgresDriver",
    "PostgresDriverConfig",
    "PostgresPool",
]
