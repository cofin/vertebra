from __future__ import annotations

import asyncio
from typing import Any

import pytest

from vertebra._internal.ipc import IpcConfig
from vertebra._internal.serialization import MessagePackCodec
from vertebra.drivers.sqlite import AsyncSqliteDriver, SyncSqliteDriverConfig


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    codec = MessagePackCodec()
    connection_id = "conn-1"
    try:
        while True:
            header = await reader.readexactly(4)
            size = int.from_bytes(header, "big")
            payload = await reader.readexactly(size)
            request = codec.decode(payload)
            method = request.get("method")
            if method == "connect":
                response: dict[str, Any] = {
                    "id": request.get("id"),
                    "ok": True,
                    "result": {"connectionId": connection_id},
                }
            elif method == "execute":
                response = {
                    "id": request.get("id"),
                    "ok": True,
                    "result": {"columns": ["value"], "rows": [[1], [2]], "rowcount": 2},
                }
            elif method == "close":
                response = {"id": request.get("id"), "ok": True, "result": {"closed": True}}
            else:
                response = {"id": request.get("id"), "ok": False, "error": {"code": "sqlite", "message": "unknown"}}
            encoded = codec.encode(response)
            writer.write(len(encoded).to_bytes(4, "big") + encoded)
            await writer.drain()
    except asyncio.IncompleteReadError:
        return
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.anyio
async def test_sqlite_driver_async_fetch() -> None:
    server = await asyncio.start_server(_handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    config = SyncSqliteDriverConfig(
        path=":memory:", ipc=IpcConfig(transport="tcp", host="127.0.0.1", port=port, enforce_bun=False)
    )
    driver = AsyncSqliteDriver(config)
    conn = await driver.connect()
    rows = await conn.fetch("select 1")
    assert rows == [(1,), (2,)]
    await conn.close()
    server.close()
    await server.wait_closed()
