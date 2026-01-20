"""Shared-memory MySQL driver backed by the Bun broker."""

from __future__ import annotations

import msgspec

from vertebra.drivers.base import AsyncDriver, SyncDriver
from vertebra.drivers.broker import (
    VertebraAsyncConnection,
    VertebraAsyncPool,
    VertebraDriverConfig,
    VertebraSyncConnection,
    VertebraSyncPool,
    _run_sync,
)
from vertebra.resources import AsyncBrokerClient, BrokerClientConfig


class SyncMysqlDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for synchronous MySQL driver."""

    pool: str = "mysql"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class AsyncMysqlDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for asynchronous MySQL driver."""

    pool: str = "mysql"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class SyncMysqlDriver(SyncDriver):
    def __init__(self, config: SyncMysqlDriverConfig | None = None) -> None:
        self._config = config or SyncMysqlDriverConfig()

    def connect(self) -> VertebraSyncConnection:
        client = AsyncBrokerClient(self._config.client)
        token = _run_sync(client.acquire(self._config.pool))
        return VertebraSyncConnection(self._config.pool, client, token)

    def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraSyncPool:
        return VertebraSyncPool(self._config.pool, self._config.client, max_clients=max_size)


class AsyncMysqlDriver(AsyncDriver):
    def __init__(self, config: AsyncMysqlDriverConfig | None = None) -> None:
        self._config = config or AsyncMysqlDriverConfig()

    async def connect(self) -> VertebraAsyncConnection:
        client = AsyncBrokerClient(self._config.client)
        token = await client.acquire(self._config.pool)
        return VertebraAsyncConnection(self._config.pool, client, token)

    async def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraAsyncPool:
        return VertebraAsyncPool(self._config.pool, self._config.client, max_clients=max_size)


__all__ = ("AsyncMysqlDriver", "AsyncMysqlDriverConfig", "SyncMysqlDriver", "SyncMysqlDriverConfig")
