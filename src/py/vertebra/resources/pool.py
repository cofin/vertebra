"""Broker pool abstractions with context manager support."""

import asyncio
import queue
import threading

import msgspec

from vertebra.common.exceptions import PoolError
from vertebra.messaging.broker import AsyncBrokerClient, BrokerClientConfig, SyncBrokerClient


class AsyncBrokerLease(msgspec.Struct, frozen=True):
    """A lease for a broker connection, usable as an async context manager."""

    pool: "AsyncBrokerPool"
    client: AsyncBrokerClient
    token: str

    async def __aenter__(self) -> str:
        return self.token

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        await self.pool.release(self.client, self.token)


class AsyncBrokerClientPool:
    def __init__(self, config: BrokerClientConfig, max_size: int = 5) -> None:
        self._config = config
        self._max_size = max_size
        self._pool: asyncio.LifoQueue[AsyncBrokerClient] = asyncio.LifoQueue()
        self._total = 0
        self._lock = asyncio.Lock()

    async def acquire(self) -> AsyncBrokerClient:
        try:
            return self._pool.get_nowait()
        except asyncio.QueueEmpty:
            async with self._lock:
                if self._total < self._max_size:
                    self._total += 1
                    return AsyncBrokerClient(self._config)
            return await self._pool.get()

    async def release(self, client: AsyncBrokerClient) -> None:
        await self._pool.put(client)

    async def close(self) -> None:
        while True:
            try:
                client = self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
            await client.close()


class AsyncBrokerPool:
    def __init__(self, pool: str, client_config: BrokerClientConfig | None = None, max_clients: int = 5) -> None:
        self._pool_name = pool
        self._client_pool = AsyncBrokerClientPool(client_config or BrokerClientConfig(), max_size=max_clients)

    async def acquire(self) -> AsyncBrokerLease:
        client = await self._client_pool.acquire()
        token = await client.acquire(self._pool_name)
        return AsyncBrokerLease(self, client, token)

    async def release(self, client: AsyncBrokerClient, token: str) -> None:
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


class SyncBrokerLease(msgspec.Struct, frozen=True):
    """A lease for a sync broker connection."""

    pool: "SyncBrokerPool"
    client: SyncBrokerClient
    token: str

    def __enter__(self) -> str:
        return self.token

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> None:
        self.pool.release(self.client, self.token)


class SyncBrokerClientPool:
    def __init__(self, config: BrokerClientConfig, max_size: int = 5) -> None:
        self._config = config
        self._max_size = max_size
        self._pool: queue.LifoQueue[SyncBrokerClient] = queue.LifoQueue()
        self._total = 0
        self._lock = threading.Lock()

    def acquire(self) -> SyncBrokerClient:
        try:
            return self._pool.get_nowait()
        except queue.Empty:
            with self._lock:
                if self._total < self._max_size:
                    self._total += 1
                    return SyncBrokerClient(self._config)
            return self._pool.get()

    def release(self, client: SyncBrokerClient) -> None:
        self._pool.put(client)

    def close(self) -> None:
        while True:
            try:
                client = self._pool.get_nowait()
            except queue.Empty:
                break
            client.close()


class SyncBrokerPool:
    def __init__(self, pool: str, client_config: BrokerClientConfig | None = None, max_clients: int = 5) -> None:
        self._pool_name = pool
        self._client_pool = SyncBrokerClientPool(client_config or BrokerClientConfig(), max_size=max_clients)

    def acquire(self) -> SyncBrokerLease:
        client = self._client_pool.acquire()
        token = client.acquire(self._pool_name)
        return SyncBrokerLease(self, client, token)

    def release(self, client: SyncBrokerClient, token: str) -> None:
        try:
            client.release(self._pool_name, token)
        finally:
            self._client_pool.release(client)

    def health(self) -> bool:
        client = self._client_pool.acquire()
        try:
            return client.health(self._pool_name)
        finally:
            self._client_pool.release(client)

    def close(self) -> None:
        self._client_pool.close()


__all__ = (
    "AsyncBrokerClientPool",
    "AsyncBrokerLease",
    "AsyncBrokerPool",
    "PoolError",
    "SyncBrokerClientPool",
    "SyncBrokerLease",
    "SyncBrokerPool",
)
