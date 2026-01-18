"""Bun-backed Redis driver over IPC."""

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
from vertebra.drivers.exceptions import DatabaseError, format_error_message

if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable, Sequence


def _run_sync(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    msg = "Use AsyncRedisConnection inside an event loop"
    raise RuntimeError(msg)


def _raise_for_error(response: dict[str, Any]) -> None:
    if response.get("ok"):
        return
    error = response.get("error") or {}
    code = error.get("code") or "redis"
    message = error.get("message") or "Redis driver error"
    raise DatabaseError(format_error_message(str(code), str(message)))


def _normalize_args(args: Sequence[Any]) -> list[str]:
    normalized: list[str] = []
    for arg in args:
        if isinstance(arg, (bytes, bytearray, memoryview)):
            normalized.append(bytes(arg).decode("utf-8", "replace"))
        else:
            normalized.append(str(arg))
    return normalized


@dataclass(slots=True)
class SyncRedisDriverConfig:
    url: str = "redis://localhost:6379"
    ipc: IpcConfig = field(
        default_factory=lambda: IpcConfig(transport="tcp", host="127.0.0.1", port=7004, enforce_bun=False)
    )


class SyncRedisConnection(SyncConnection):
    def __init__(self, client: IpcClient, connection_id: str) -> None:
        self._client = client
        self._connection_id = connection_id

    def cursor(self) -> SyncCursor:
        return SyncRedisCursor()

    def commit(self) -> None:
        return

    def rollback(self) -> None:
        return

    def command(self, command: str, *args: Any) -> Any:
        response = _run_sync(
            self._client.request({
                "id": "command",
                "driver": "redis",
                "method": "command",
                "params": {"connectionId": self._connection_id, "command": command, "args": _normalize_args(args)},
            })
        )
        _raise_for_error(response)
        return response.get("result")

    def get(self, key: str) -> Any:
        return self.command("GET", key)

    def set(self, key: str, value: Any) -> Any:
        return self.command("SET", key, value)

    def delete(self, key: str) -> Any:
        return self.command("DEL", key)

    def close(self) -> None:
        _run_sync(
            self._client.request({
                "id": "close",
                "driver": "redis",
                "method": "close",
                "params": {"connectionId": self._connection_id},
            })
        )
        _run_sync(self._client.close())


class AsyncRedisConnection(AsyncConnection):
    def __init__(self, client: IpcClient, url: str, connection_id: str | None = None) -> None:
        self._client = client
        self._url = url
        self._connection_id = connection_id

    def cursor(self) -> AsyncCursor:
        return AsyncRedisCursor()

    async def commit(self) -> None:
        return

    async def rollback(self) -> None:
        return

    async def _ensure_connection(self) -> str:
        if self._connection_id:
            return self._connection_id
        response = await self._client.request({
            "id": "connect",
            "driver": "redis",
            "method": "connect",
            "params": {"url": self._url},
        })
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            msg = "Connection id missing from response"
            raise DatabaseError(msg)
        self._connection_id = connection_id
        return connection_id

    async def command(self, command: str, *args: Any) -> Any:
        connection_id = await self._ensure_connection()
        response = await self._client.request({
            "id": "command",
            "driver": "redis",
            "method": "command",
            "params": {"connectionId": connection_id, "command": command, "args": _normalize_args(args)},
        })
        _raise_for_error(response)
        return response.get("result")

    async def get(self, key: str) -> Any:
        return await self.command("GET", key)

    async def set(self, key: str, value: Any) -> Any:
        return await self.command("SET", key, value)

    async def delete(self, key: str) -> Any:
        return await self.command("DEL", key)

    async def close(self) -> None:
        if not self._connection_id:
            return
        await self._client.request({
            "id": "close",
            "driver": "redis",
            "method": "close",
            "params": {"connectionId": self._connection_id},
        })
        await self._client.close()


class SyncRedisPool(SyncPool):
    def __init__(self, driver: "SyncRedisDriver", min_size: int = 1, max_size: int = 5) -> None:
        self._driver = driver
        self._min_size = min_size
        self._max_size = max_size

    def acquire(self) -> SyncConnection:
        return self._driver.connect()

    def release(self, connection: SyncConnection) -> None:
        connection.close()

    def close(self) -> None:
        return

    def get_size(self) -> int:
        return self._max_size

    def get_idle_size(self) -> int:
        return self._min_size


class AsyncRedisPool(AsyncPool):
    def __init__(self, driver: "AsyncRedisDriver", min_size: int = 1, max_size: int = 5) -> None:
        self._driver = driver
        self._min_size = min_size
        self._max_size = max_size

    async def acquire(self) -> AsyncConnection:
        return await self._driver.connect()

    async def release(self, connection: AsyncConnection) -> None:
        await connection.close()

    async def close(self) -> None:
        return

    def get_size(self) -> int:
        return self._max_size

    def get_idle_size(self) -> int:
        return self._min_size


class SyncRedisDriver(SyncDriver):
    def __init__(self, config: SyncRedisDriverConfig | None = None) -> None:
        self._config = config or SyncRedisDriverConfig()

    def _connect(self) -> SyncConnection:
        client = IpcClient(self._config.ipc)
        response = _run_sync(
            client.request({
                "id": "connect",
                "driver": "redis",
                "method": "connect",
                "params": {"url": self._config.url},
            })
        )
        _raise_for_error(response)
        connection_id = response.get("result", {}).get("connectionId")
        if not connection_id:
            msg = "Connection id missing from response"
            raise DatabaseError(msg)
        return SyncRedisConnection(client, connection_id)

    def connect(self) -> SyncConnection:
        return self._connect()

    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncPool:
        return SyncRedisPool(self, min_size=min_size, max_size=max_size)


class AsyncRedisDriver(AsyncDriver):
    def __init__(self, config: SyncRedisDriverConfig | None = None) -> None:
        self._config = config or SyncRedisDriverConfig()

    async def connect(self) -> AsyncConnection:
        client = IpcClient(self._config.ipc)
        connection = AsyncRedisConnection(client, self._config.url)
        await connection._ensure_connection()
        return connection

    async def pool(self, min_size: int = 1, max_size: int = 10) -> AsyncPool:
        return AsyncRedisPool(self, min_size=min_size, max_size=max_size)


class SyncRedisCursor(SyncCursor):
    def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    def fetchone(self) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    def fetchmany(self, size: int | None = None) -> list[Any]:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    def fetchall(self) -> list[Any]:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    def close(self) -> None:
        return


class AsyncRedisCursor(AsyncCursor):
    async def execute(self, query: str, params: Sequence[Any] | None = None) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    async def executemany(self, query: str, params: Iterable[Sequence[Any]]) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    async def fetchone(self) -> Any:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    async def fetchall(self) -> list[Any]:
        msg = "Redis does not support cursors"
        raise NotImplementedError(msg)

    async def close(self) -> None:
        return


__all__ = (
    "AsyncRedisConnection",
    "AsyncRedisCursor",
    "AsyncRedisDriver",
    "AsyncRedisPool",
    "SyncRedisConnection",
    "SyncRedisCursor",
    "SyncRedisDriver",
    "SyncRedisDriverConfig",
    "SyncRedisPool",
)
