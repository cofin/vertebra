"""Shared broker-backed driver implementations."""

import asyncio
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra.resources import BrokerClient, BrokerClientConfig, BrokerPool

if TYPE_CHECKING:
    from collections.abc import Awaitable


def _run_sync(coro: "Awaitable[Any]") -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    msg = "Use async broker connection inside an event loop"
    raise RuntimeError(msg)


class BrokerDriverConfig(msgspec.Struct, frozen=True):
    """Configuration for a broker-backed database driver."""

    pool: str
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class SyncBrokerCursor:
    def __init__(self, connection: "SyncBrokerConnection") -> None:
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


class SyncBrokerConnection:
    def __init__(self, pool: str, client: BrokerClient, token: str) -> None:
        self._pool = pool
        self._client = client
        self._token = token

    def cursor(self) -> SyncBrokerCursor:
        return SyncBrokerCursor(self)

    def begin(self) -> None:
        _run_sync(self._client.begin(self._pool, self._token))

    def commit(self) -> None:
        _run_sync(self._client.commit(self._pool, self._token))

    def rollback(self) -> None:
        _run_sync(self._client.rollback(self._pool, self._token))

    def close(self) -> None:
        _run_sync(self._client.release(self._pool, self._token))

    def _execute(
        self, query: str, params: Sequence[Any] | dict[str, Any], correlation_id: str | None = None
    ) -> dict[str, Any]:
        payload: list[Any] | dict[str, Any]
        payload = params if isinstance(params, dict) else list(params)
        return _run_sync(self._client.execute(self._pool, self._token, query, payload, correlation_id))

    def _execute_many(
        self, query: str, params: Iterable[Sequence[Any] | dict[str, Any]], correlation_id: str | None = None
    ) -> dict[str, Any]:
        payload: list[list[Any] | dict[str, Any]] = []
        for param_set in params:
            if isinstance(param_set, dict):
                payload.append(param_set)
            else:
                payload.append(list(param_set))
        return _run_sync(self._client.execute_many(self._pool, self._token, query, payload, correlation_id))


class SyncBrokerPool:
    def __init__(self, pool: str, config: BrokerClientConfig, max_clients: int = 5) -> None:
        self._pool = BrokerPool(pool, client_config=config, max_clients=max_clients)

    def acquire(self) -> SyncBrokerConnection:
        lease = _run_sync(self._pool.acquire())
        return SyncBrokerConnection(self._pool._pool_name, lease.client, lease.token)

    def release(self, connection: SyncBrokerConnection) -> None:
        _run_sync(self._pool.release(connection._client, connection._token))

    def close(self) -> None:
        _run_sync(self._pool.close())

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


class AsyncBrokerCursor:
    def __init__(self, connection: "AsyncBrokerConnection") -> None:
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


class AsyncBrokerConnection:
    def __init__(self, pool: str, client: BrokerClient, token: str) -> None:
        self._pool = pool
        self._client = client
        self._token = token

    def cursor(self) -> AsyncBrokerCursor:
        return AsyncBrokerCursor(self)

    async def begin(self) -> None:
        await self._client.begin(self._pool, self._token)

    async def commit(self) -> None:
        await self._client.commit(self._pool, self._token)

    async def rollback(self) -> None:
        await self._client.rollback(self._pool, self._token)

    def transaction(self) -> "AsyncBrokerTransaction":
        return AsyncBrokerTransaction(self)

    async def fetch(self, query: str, params: Sequence[Any] | None = None) -> list[Any]:
        result = await self._execute(query, params or [])
        return [tuple(row) for row in result.get("rows", [])]

    async def fetchrow(self, query: str, params: Sequence[Any] | None = None) -> Any:
        rows = await self.fetch(query, params)
        return rows[0] if rows else None

    async def fetchval(self, query: str, params: Sequence[Any] | None = None) -> Any:
        row = await self.fetchrow(query, params)
        if not row:
            return None
        return row[0]

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
        await self._client.release(self._pool, self._token)

    async def _execute(
        self, query: str, params: Sequence[Any] | dict[str, Any], correlation_id: str | None = None
    ) -> dict[str, Any]:
        payload: list[Any] | dict[str, Any]
        payload = params if isinstance(params, dict) else list(params)
        return await self._client.execute(self._pool, self._token, query, payload, correlation_id)

    async def _execute_many(
        self, query: str, params: Iterable[Sequence[Any] | dict[str, Any]], correlation_id: str | None = None
    ) -> dict[str, Any]:
        payload: list[list[Any] | dict[str, Any]] = []
        for param_set in params:
            if isinstance(param_set, dict):
                payload.append(param_set)
            else:
                payload.append(list(param_set))
        return await self._client.execute_many(self._pool, self._token, query, payload, correlation_id)


class AsyncBrokerTransaction:
    def __init__(self, connection: AsyncBrokerConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> "AsyncBrokerTransaction":
        await self._connection._client.begin(self._connection._pool, self._connection._token)
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        if exc_type is None:
            await self._connection._client.commit(self._connection._pool, self._connection._token)
        else:
            await self._connection._client.rollback(self._connection._pool, self._connection._token)


class AsyncBrokerPool:
    def __init__(self, pool: str, config: BrokerClientConfig, max_clients: int = 5) -> None:
        self._pool = BrokerPool(pool, client_config=config, max_clients=max_clients)

    async def acquire(self) -> AsyncBrokerConnection:
        lease = await self._pool.acquire()
        return AsyncBrokerConnection(self._pool._pool_name, lease.client, lease.token)

    async def release(self, connection: AsyncBrokerConnection) -> None:
        await self._pool.release(connection._client, connection._token)

    async def close(self) -> None:
        await self._pool.close()

    def get_size(self) -> int:
        return 0

    def get_idle_size(self) -> int:
        return 0


__all__ = (
    "AsyncBrokerConnection",
    "AsyncBrokerCursor",
    "AsyncBrokerPool",
    "BrokerDriverConfig",
    "SyncBrokerConnection",
    "SyncBrokerCursor",
    "SyncBrokerPool",
)
