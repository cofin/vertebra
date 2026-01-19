"""Shared-memory provider host for external adapters."""

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

import msgspec

from vertebra._internal.shm import ShmRing, ShmRingConfig
from vertebra.resources.ipc import BrokerClient, BrokerClientConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable

ProviderHandler = Callable[[dict[str, Any]], "dict[str, Any] | Awaitable[dict[str, Any]]"]


@dataclass(slots=True)
class ProviderConfig:
    pool: str
    driver: str
    max_size: int = 10
    name: str = "vertebra-broker"
    slot_size: int = 65536
    slots: int = 1024
    arrow_threshold_rows: int = 1_000
    arrow_threshold_bytes: int = 1_048_576


class ProviderHost:
    def __init__(self, config: ProviderConfig, handler: ProviderHandler) -> None:
        self._config = config
        self._handler = handler
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder()
        self._request = ShmRing.create(
            ShmRingConfig(name=f"/{config.name}.provider.{config.pool}.req", slot_size=config.slot_size, slots=config.slots)
        )
        self._response = ShmRing.create(
            ShmRingConfig(
                name=f"/{config.name}.provider.{config.pool}.resp", slot_size=config.slot_size, slots=config.slots
            )
        )

    async def register(self, client_config: "BrokerClientConfig | None" = None) -> None:
        client = BrokerClient(client_config or BrokerClientConfig(name=self._config.name))
        await client.register_provider(
            self._config.pool,
            self._config.driver,
            self._config.max_size,
            req_ring=f"/{self._config.name}.provider.{self._config.pool}.req",
            resp_ring=f"/{self._config.name}.provider.{self._config.pool}.resp",
        )

    async def serve(self) -> None:
        while True:
            data, _flags = await asyncio.to_thread(self._request.recv, 5_000)
            request = self._decoder.decode(data)
            response = self._handler(request)
            if asyncio.iscoroutine(response):
                response = await response
            if "id" not in response and "id" in request:
                response["id"] = request["id"]
            encoded = self._encoder.encode(response)
            self._response.send(encoded, 0, 5_000)


__all__ = ("ProviderConfig", "ProviderHost", "ProviderHandler")
