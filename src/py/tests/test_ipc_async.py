from __future__ import annotations

import asyncio

import pytest

from vertebra.ipc.channel import Channel


@pytest.mark.anyio
async def test_async_channel_roundtrip() -> None:
    client, server = Channel.create_pair(slot_size=1024, slots=4)

    async def server_task() -> None:
        proc_id, params = await server.receive()
        await server.respond(proc_id, {"ok": params["value"]})

    task = asyncio.create_task(server_task())
    result = await client.send("ping", {"value": 7})
    await task
    assert result["ok"] == 7
