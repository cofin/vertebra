from __future__ import annotations

import asyncio
from typing import Any

import msgspec
import pytest

from vertebra.resources import BrokerClientConfig, BrokerPool


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
                response: dict[str, Any] = {"Acquired": {"token": "lease-1"}}
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
async def test_broker_pool_context_manager() -> None:
    server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    pool = BrokerPool(pool="default", client_config=BrokerClientConfig(host="127.0.0.1", port=port), max_clients=2)

    async with await pool.acquire() as token:
        assert token == "lease-1"

    healthy = await pool.health()
    assert healthy is True

    await pool.close()
    server.close()
    await server.wait_closed()
