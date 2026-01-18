from __future__ import annotations

import asyncio

import pytest

from vertebra._internal.ipc import IpcClient, IpcConfig, IpcPool
from vertebra._internal.serialization import MessagePackCodec


async def _handle_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    codec = MessagePackCodec()
    header = await reader.readexactly(4)
    size = int.from_bytes(header, "big")
    payload = await reader.readexactly(size)
    message = codec.decode(payload)
    response = {"ok": True, "echo": message}
    encoded = codec.encode(response)
    writer.write(len(encoded).to_bytes(4, "big") + encoded)
    await writer.drain()
    writer.close()


@pytest.mark.anyio
async def test_ipc_client_request() -> None:
    server = await asyncio.start_server(_handle_echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    config = IpcConfig(transport="tcp", host="127.0.0.1", port=port, enforce_bun=False)
    client = IpcClient(config)
    response = await client.request({"ping": "pong"})
    assert response["echo"] == {"ping": "pong"}
    await client.close()
    server.close()
    await server.wait_closed()


@pytest.mark.anyio
async def test_ipc_pool_request() -> None:
    server = await asyncio.start_server(_handle_echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    config = IpcConfig(transport="tcp", host="127.0.0.1", port=port, enforce_bun=False)
    pool = IpcPool(config, max_size=2)
    response = await pool.request({"hello": "world"})
    assert response["echo"] == {"hello": "world"}
    server.close()
    await server.wait_closed()
