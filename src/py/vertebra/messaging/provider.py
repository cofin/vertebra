"""Shared-memory provider host for external adapters."""

import asyncio
import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._vertebra import SpscRing
from vertebra.common.serializers import MessagePackCodec
from vertebra.common.serializers.arrow import ArrowRegionPool, encode_arrow, should_use_arrow
from vertebra.messaging.broker import AsyncBrokerClient, BrokerClientConfig

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
        self._arrow_pool = ArrowRegionPool(prefix=f"{config.name}.arrow")
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
        try:
            while True:
                data = await asyncio.to_thread(self._request.recv_timeout, self._config.slot_size, 5_000)
                if data is None:
                    continue  # Timeout, retry
                decoded = self._codec.decode(data)
                request: dict[str, Any] = decoded if isinstance(decoded, dict) else {}

                # Check for arrow_release
                if request.get("kind") == "arrow_release":
                    released = self._arrow_pool.release_region(str(request.get("name", "")))
                    response = {"ok": released, "id": request.get("id")}
                    encoded = self._codec.encode(response)
                    self._response.send_timeout(encoded, 0, 5_000)
                    continue

                result = self._handler(request)
                response: dict[str, Any]
                if inspect.iscoroutine(result):
                    response = await result
                else:
                    response = result  # type: ignore[assignment]

                if "id" not in response and "id" in request:
                    response["id"] = request["id"]

                # Apply Arrow thresholding
                if should_use_arrow(response, self._config.arrow_threshold_rows, self._config.arrow_threshold_bytes):
                    try:
                        arrow_bytes = encode_arrow(response)
                        region = self._arrow_pool.create_region(len(arrow_bytes))
                        region.region.write(arrow_bytes)

                        # Replace rows with arrow info
                        response = {
                            "ok": True,
                            "id": response.get("id"),
                            "arrow": {"name": region.name, "size": region.size},
                            "rowcount": response.get("rowcount", len(response.get("rows", []))),
                        }
                    except Exception:  # noqa: BLE001, S110
                        # Fallback to MessagePack if Arrow encoding fails
                        pass

                encoded = self._codec.encode(response)
                self._response.send_timeout(encoded, 0, 5_000)
        finally:
            self._arrow_pool.close()


__all__ = ("ProviderConfig", "ProviderHandler", "ProviderHost")
