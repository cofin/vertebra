"""AsyncAPI 3.0.0 specification generator for WebSocket, SSE, and pub/sub channels."""

from __future__ import annotations

from typing import Any, Literal

import msgspec

__all__ = (
    "AsyncAPIConfig",
    "AsyncAPISpecBuilder",
    "AsyncChannelDef",
)


class AsyncChannelDef(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """AsyncAPI 3.0.0 channel definition for WebSocket, SSE, or pub/sub topics."""

    address: str
    protocol: Literal["ws", "wss", "sse", "shm-pubsub", "valkey", "redis"] = "ws"
    description: str = ""
    message_schema: dict[str, Any] = msgspec.field(default_factory=dict)


class AsyncAPIConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """AsyncAPI 3.0.0 document configuration."""

    title: str = "Vertebra Realtime API"
    version: str = "0.1.0"
    asyncapi_version: str = "3.0.0"
    schema_path: str = "/asyncapi.json"


class AsyncAPISpecBuilder:
    """Constructs AsyncAPI 3.0.0 specifications from registered event channels."""

    __slots__ = ("_channels", "_config")

    def __init__(self, config: AsyncAPIConfig | None = None) -> None:
        self._config = config or AsyncAPIConfig()
        self._channels: dict[str, AsyncChannelDef] = {}

    @property
    def config(self) -> AsyncAPIConfig:
        """Return the ``AsyncAPIConfig``."""
        return self._config

    def add_channel(self, name: str, channel: AsyncChannelDef) -> None:
        """Register an ``AsyncChannelDef`` under ``name``."""
        self._channels[name] = channel

    def build(self) -> dict[str, Any]:
        """Return the complete AsyncAPI 3.0.0 specification dictionary."""
        channels_obj: dict[str, Any] = {}
        for name, ch in self._channels.items():
            channels_obj[name] = {
                "address": ch.address,
                "description": ch.description,
                "bindings": {"protocol": ch.protocol},
                "messages": {"default": {"payload": ch.message_schema or {"type": "object"}}},
            }
        return {
            "asyncapi": self._config.asyncapi_version,
            "info": {
                "title": self._config.title,
                "version": self._config.version,
            },
            "channels": channels_obj,
        }
