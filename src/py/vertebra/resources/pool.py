"""Broker pool abstractions with context manager support."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from vertebra.resources.ipc import BrokerClient, BrokerClientConfig

if TYPE_CHECKING:
    from types import TracebackType


class BrokerPoolError(RuntimeError):
    """Raised when broker pool operations fail."""


@dataclass(slots=True)
class BrokerLease:
    pool: "BrokerPool"
    client: BrokerClient
    token: str

    async def __aenter__(self) -> str:
        return self.token

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.pool.release(self.client, self.token)


class BrokerClientPool:
    def __init__(self, config: BrokerClientConfig, max_size: int = 5) -> None:
        self._config = config
        self._max_size = max_size
        self._pool: asyncio.LifoQueue[BrokerClient] = asyncio.LifoQueue()
        self._total = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> BrokerClient:
        try:
            return self._pool.get_nowait()
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._total < self._max_size:
                    self._total += 1
                    return BrokerClient(self._config)
            return await self._pool.get()

    async def release(self, client: BrokerClient) -> None:
        await self._pool.put(client)

    async def close(self) -> None:
        while True:
            try:
                client = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            await client.close()


class BrokerPool:
    def __init__(self, pool: str, client_config: BrokerClientConfig | None = None, max_clients: int = 5) -> None:
        self._pool_name = pool
        self._client_pool = BrokerClientPool(client_config or BrokerClientConfig(), max_size=max_clients)

    async def acquire(self) -> BrokerLease:
        client = await self._client_pool.acquire()
        token = await client.acquire(self._pool_name)
        return BrokerLease(self, client, token)

    async def release(self, client: BrokerClient, token: str) -> None:
        try:
            await client.release(self._pool_name, token)
        finally:
            await self._client_pool.release(client)

    async def health(self) -> bool:
        client = await self._client_pool.acquire()
        try:
            return await client.health(self._pool_name)
        finally:
            await self._client_pool.release(client)

    async def close(self) -> None:
        await self._client_pool.close()


__all__ = ["BrokerClientPool", "BrokerLease", "BrokerPool", "BrokerPoolError"]
