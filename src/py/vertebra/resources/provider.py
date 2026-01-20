"""Shared-memory provider host for external adapters."""

import asyncio
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._internal.serialization import MessagePackCodec
from vertebra._vertebra import SpscRing
from vertebra.resources.broker import AsyncBrokerClient, BrokerClientConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable

ProviderHandler = Callable[[dict[str, Any]], "dict[str, Any] | Awaitable[dict[str, Any]]"]


class ProviderConfig(msgspec.Struct, frozen=True):
    """Configuration for a shared-memory provider host."""

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
        self._codec = MessagePackCodec()
        self._request = SpscRing.create_named(
            f"/{config.name}.provider.{config.pool}.req", config.slot_size, config.slots
        )
        self._response = SpscRing.create_named(
            f"/{config.name}.provider.{config.pool}.resp", config.slot_size, config.slots
        )

    async def register(self, client_config: "BrokerClientConfig | None" = None) -> None:
        client = AsyncBrokerClient(client_config or BrokerClientConfig(name=self._config.name))
        await client.register_provider(
            self._config.pool,
            self._config.driver,
            self._config.max_size,
            req_ring=f"/{self._config.name}.provider.{self._config.pool}.req",
            resp_ring=f"/{self._config.name}.provider.{self._config.pool}.resp",
        )

    async def serve(self) -> None:
        while True:
            data = await asyncio.to_thread(self._request.recv_timeout, self._config.slot_size, 5_000)
            if data is None:
                continue  # Timeout, retry
            decoded = self._codec.decode(data)
            request: dict[str, Any] = decoded if isinstance(decoded, dict) else {}
            result = self._handler(request)
            response: dict[str, Any]
            if inspect.iscoroutine(result):
                response = await result
            else:
                response = result  # type: ignore[assignment]
            if "id" not in response and "id" in request:
                response["id"] = request["id"]
            encoded = self._codec.encode(response)
            self._response.send_timeout(encoded, 0, 5_000)


__all__ = ("ProviderConfig", "ProviderHandler", "ProviderHost")
