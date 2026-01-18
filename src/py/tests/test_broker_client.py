from __future__ import annotations

import asyncio
from typing import Any

import pytest
import msgspec

from vertebra.resources import BrokerClient, BrokerClientConfig


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    decoder = msgspec.msgpack.Decoder()
    encoder = msgspec.msgpack.Encoder()
    try:
        while True:
            header = await reader.readexactly(4)
            size = int.from_bytes(header, "big")
            payload = await reader.readexactly(size)
            request = decoder.decode(payload)
            if "Acquire" in request:
                response: dict[str, Any] = {"Acquired": {"token": "token-1"}}
            elif "Release" in request:
                response = {"Released": {"ok": True}}
            elif "Health" in request:
                response = {"Health": {"ok": True}}
            else:
                response = {"Error": {"message": "unknown"}}
            encoded = encoder.encode(response)
            writer.write(len(encoded).to_bytes(4, "big") + encoded)
            await writer.drain()
    except asyncio.IncompleteReadError:
        return
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.anyio
async def test_broker_client_roundtrip() -> None:
    server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = BrokerClient(BrokerClientConfig(host="127.0.0.1", port=port))
    token = await client.acquire("default")
    assert token == "token-1"
    ok = await client.release("default", token)
    assert ok is True
    healthy = await client.health("default")
    assert healthy is True
    await client.close()
    server.close()
    await server.wait_closed()
