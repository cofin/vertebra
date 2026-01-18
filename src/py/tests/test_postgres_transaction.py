from __future__ import annotations

import asyncio
from typing import Any

import pytest

from vertebra._internal.ipc import IpcConfig
from vertebra._internal.serialization import MessagePackCodec
from vertebra.drivers.postgres import AsyncPostgresDriver, SyncPostgresDriverConfig


async def _handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, queries: list[str]) -> None:
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
                params = request.get("params") or {}
                sql = params.get("sql")
                if isinstance(sql, str):
                    queries.append(sql)
                response = {"id": request.get("id"), "ok": True, "result": {"columns": [], "rows": [], "rowcount": 0}}
            elif method == "close":
                response = {"id": request.get("id"), "ok": True, "result": {"closed": True}}
            else:
                response = {"id": request.get("id"), "ok": False, "error": {"code": "postgres", "message": "unknown"}}
            encoded = codec.encode(response)
            writer.write(len(encoded).to_bytes(4, "big") + encoded)
            await writer.drain()
    except asyncio.IncompleteReadError:
        return
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.anyio
async def test_postgres_transaction_context() -> None:
    queries: list[str] = []
    server = await asyncio.start_server(lambda r, w: _handle_client(r, w, queries), "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    config = SyncPostgresDriverConfig(
        connection_string="postgres://localhost/test",
        ipc=IpcConfig(transport="tcp", host="127.0.0.1", port=port, enforce_bun=False),
    )
    driver = AsyncPostgresDriver(config)
    conn = await driver.connect()
    async with conn.transaction():
        await conn.execute("SELECT 1")
    await conn.close()
    server.close()
    await server.wait_closed()

    assert queries[0].startswith("BEGIN")
    assert "SELECT 1" in queries
    assert queries[-1] == "COMMIT"
