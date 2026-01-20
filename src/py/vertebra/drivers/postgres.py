"""Shared-memory PostgreSQL driver backed by the Bun broker."""

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
from vertebra.resources import BrokerClient, BrokerClientConfig


class SyncPostgresDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for synchronous PostgreSQL driver."""

    pool: str = "postgres"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class AsyncPostgresDriverConfig(VertebraDriverConfig, frozen=True):
    """Configuration for asynchronous PostgreSQL driver."""

    pool: str = "postgres"
    client: BrokerClientConfig = msgspec.field(default_factory=BrokerClientConfig)


class SyncPostgresDriver(SyncDriver):
    def __init__(self, config: SyncPostgresDriverConfig | None = None) -> None:
        self._config = config or SyncPostgresDriverConfig()

    def connect(self) -> VertebraSyncConnection:
        client = BrokerClient(self._config.client)
        token = _run_sync(client.acquire(self._config.pool))
        return VertebraSyncConnection(self._config.pool, client, token)

    def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraSyncPool:
        return VertebraSyncPool(self._config.pool, self._config.client, max_clients=max_size)


class AsyncPostgresDriver(AsyncDriver):
    def __init__(self, config: AsyncPostgresDriverConfig | None = None) -> None:
        self._config = config or AsyncPostgresDriverConfig()

    async def connect(self) -> VertebraAsyncConnection:
        client = BrokerClient(self._config.client)
        token = await client.acquire(self._config.pool)
        return VertebraAsyncConnection(self._config.pool, client, token)

    async def pool(self, min_size: int = 1, max_size: int = 10) -> VertebraAsyncPool:
        return VertebraAsyncPool(self._config.pool, self._config.client, max_clients=max_size)


__all__ = ("AsyncPostgresDriver", "AsyncPostgresDriverConfig", "SyncPostgresDriver", "SyncPostgresDriverConfig")