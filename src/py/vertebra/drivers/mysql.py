"""Shared-memory MySQL driver backed by the Bun broker."""

from __future__ import annotations

from dataclasses import dataclass

from vertebra.drivers.base import AsyncDriver, SyncDriver
from vertebra.drivers.broker import (
    AsyncBrokerConnection,
    AsyncBrokerPool,
    BrokerDriverConfig,
    SyncBrokerConnection,
    SyncBrokerPool,
    _run_sync,
)
from vertebra.resources import BrokerClient, BrokerClientConfig


@dataclass(slots=True)
class SyncMysqlDriverConfig(BrokerDriverConfig):
    pool: str = "mysql"
    client: BrokerClientConfig = BrokerClientConfig()  # noqa: RUF009


@dataclass(slots=True)
class AsyncMysqlDriverConfig(BrokerDriverConfig):
    pool: str = "mysql"
    client: BrokerClientConfig = BrokerClientConfig()  # noqa: RUF009


class SyncMysqlDriver(SyncDriver):
    def __init__(self, config: SyncMysqlDriverConfig | None = None) -> None:
        self._config = config or SyncMysqlDriverConfig()

    def connect(self) -> SyncBrokerConnection:
        client = BrokerClient(self._config.client)
        token = _run_sync(client.acquire(self._config.pool))
        return SyncBrokerConnection(self._config.pool, client, token)

    def pool(self, min_size: int = 1, max_size: int = 10) -> SyncBrokerPool:
        return SyncBrokerPool(self._config.pool, self._config.client, max_clients=max_size)


class AsyncMysqlDriver(AsyncDriver):
    def __init__(self, config: AsyncMysqlDriverConfig | None = None) -> None:
        self._config = config or AsyncMysqlDriverConfig()

    async def connect(self) -> AsyncBrokerConnection:
        client = BrokerClient(self._config.client)
        token = await client.acquire(self._config.pool)
        return AsyncBrokerConnection(self._config.pool, client, token)

    async def pool(self, min_size: int = 1, max_size: int = 10) -> AsyncBrokerPool:
        return AsyncBrokerPool(self._config.pool, self._config.client, max_clients=max_size)


__all__ = (
    "AsyncMysqlDriver",
    "AsyncMysqlDriverConfig",
    "SyncMysqlDriver",
    "SyncMysqlDriverConfig",
)
