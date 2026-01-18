"""Python IPC client for the vertebra-broker resource manager."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Literal

import msgspec


class BrokerClientError(RuntimeError):
    """Raised for broker IPC errors."""


@dataclass(frozen=True, slots=True)
class BrokerClientConfig:
    transport: Literal["uds", "tcp"] = "tcp"
    path: str | None = None
    host: str = "127.0.0.1"
    port: int = 7500
    timeout: float = 5.0
    codec: Literal["msgpack", "bincode"] = "msgpack"


class BrokerClient:
    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        self._config = config or BrokerClientConfig()
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        if self._config.transport == "uds":
            if not self._config.path:
                msg = "UDS transport requires a socket path"
                raise BrokerClientError(msg)
            self._reader, self._writer = await asyncio.open_unix_connection(self._config.path)
        else:
            self._reader, self._writer = await asyncio.open_connection(
                self._config.host, self._config.port
            )

    async def close(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        try:
            await self._writer.wait_closed()
        finally:
            self._reader = None
            self._writer = None

    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._config.codec != "msgpack":
            msg = "Only msgpack codec is supported by the Python client"
            raise BrokerClientError(msg)
        if self._reader is None or self._writer is None:
            await self.connect()
        if self._reader is None or self._writer is None:
            msg = "Broker connection unavailable"
            raise BrokerClientError(msg)
        encoded = self._encoder.encode(payload)
        frame = len(encoded).to_bytes(4, "big") + encoded
        self._writer.write(frame)
        await self._writer.drain()
        header = await asyncio.wait_for(self._reader.readexactly(4), timeout=self._config.timeout)
        size = int.from_bytes(header, "big")
        data = await asyncio.wait_for(self._reader.readexactly(size), timeout=self._config.timeout)
        return self._decoder.decode(data)

    async def acquire(self, pool: str) -> str:
        response = await self.request({"Acquire": {"pool": pool}})
        return _unwrap_response(response, "Acquired").get("token")

    async def release(self, pool: str, token: str) -> bool:
        response = await self.request({"Release": {"pool": pool, "token": token}})
        return bool(_unwrap_response(response, "Released").get("ok"))

    async def health(self, pool: str) -> bool:
        response = await self.request({"Health": {"pool": pool}})
        return bool(_unwrap_response(response, "Health").get("ok"))


def _unwrap_response(response: dict[str, Any], expected: str) -> dict[str, Any]:
    if "Error" in response:
        message = response["Error"].get("message", "Broker error")
        raise BrokerClientError(message)
    if expected not in response:
        msg = "Unexpected broker response"
        raise BrokerClientError(msg)
    return response[expected]


__all__ = ["BrokerClient", "BrokerClientConfig", "BrokerClientError"]
