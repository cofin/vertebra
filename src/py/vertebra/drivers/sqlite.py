"""Shared-memory SQLite driver backed by the Bun broker."""

import msgspec

from vertebra.drivers.base import AsyncDriver, SyncDriver
from vertebra.drivers.broker import (
    VertebraAsyncConnection,
    VertebraAsyncPool,
    VertebraDriverConfig,
    VertebraSyncConnection,
    VertebraSyncPool,
)
from vertebra.resources import AsyncBrokerClient, BrokerClientConfig, SyncBrokerClient


class SyncSqliteDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for synchronous SQLite driver."""

    pool: str = "sqlite"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class AsyncSqliteDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for asynchronous SQLite driver."""

    pool: str = "sqlite"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class SyncSqliteDriver(SyncDriver):
    def __init__(self, config: SyncSqliteDriverConfig | None = None) -> None:
        self._config = config or SyncSqliteDriverConfig()

    def connect(self) -> VertebraSyncConnection:
        client = SyncBrokerClient(self._config.client)
        token = client.acquire(self._config.pool)
        return VertebraSyncConnection(self._config.pool, client, token)

    def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraSyncPool:
        return VertebraSyncPool(self._config.pool, self._config.client, max_clients=max_size)


class AsyncSqliteDriver(AsyncDriver):
    def __init__(self, config: AsyncSqliteDriverConfig | None = None) -> None:
        self._config = config or AsyncSqliteDriverConfig()

    async def connect(self) -> VertebraAsyncConnection:
        client = AsyncBrokerClient(self._config.client)
        token = await client.acquire(self._config.pool)
        return VertebraAsyncConnection(self._config.pool, client, token)

    async def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraAsyncPool:
        return VertebraAsyncPool(self._config.pool, self._config.client, max_clients=max_size)


__all__ = ("AsyncSqliteDriver", "AsyncSqliteDriverConfig", "SyncSqliteDriver", "SyncSqliteDriverConfig")
