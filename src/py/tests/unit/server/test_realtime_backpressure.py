"""Unit tests for bounded ASGI streaming queues and watermark backpressure."""

import asyncio
from typing import Any

import pytest

from vertebra.server._asgi._queues import (
    DEFAULT_STREAM_QUEUE_MAXSIZE,
    AsgiStreamQueuePair,
)
from vertebra.server._asgi._runner import AsgiRunner


def test_default_stream_queue_maxsize_is_256() -> None:
    """Verify streaming ASGI queues default to bounded maxsize=256."""
    pair = AsgiStreamQueuePair()
    assert DEFAULT_STREAM_QUEUE_MAXSIZE == 256
    assert pair.receive_queue.maxsize == 256
    assert pair.send_queue.maxsize == 256


@pytest.mark.anyio
async def test_stream_queue_pair_pauses_at_high_watermark_and_resumes_at_low() -> None:
    """Verify high/low watermark pause and resume notification on AsgiStreamQueuePair."""
    pair = AsgiStreamQueuePair(maxsize=8, high_watermark=4, low_watermark=2)

    for i in range(4):
        await pair.send({"type": "http.response.body", "body": f"c{i}".encode(), "more_body": True})

    assert pair.is_send_paused is True

    drained_first = await pair.pop_outbound()
    assert drained_first["body"] == b"c0"
    assert pair.is_send_paused is True

    drained_second = await pair.pop_outbound()
    assert drained_second["body"] == b"c1"
    assert pair.is_send_paused is False


@pytest.mark.anyio
async def test_asgi_runner_executes_streaming_session_with_bounded_queues() -> None:
    """Verify AsgiRunner.run_stream_session streams chunks through bounded queues."""

    async def streaming_app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        del scope
        req = await receive()
        assert req["type"] == "http.request"
        await send({"type": "http.response.start", "status": 200, "headers": []})
        for idx in range(3):
            await send(
                {
                    "type": "http.response.body",
                    "body": f"chunk-{idx}".encode(),
                    "more_body": idx < 2,
                }
            )

    runner = AsgiRunner(streaming_app, loop=asyncio.get_running_loop())
    pair = runner.create_stream_queues(maxsize=16, high_watermark=12, low_watermark=4)
    await pair.push_inbound({"type": "http.request", "body": b"", "more_body": False})

    scope: dict[str, Any] = {"type": "http", "method": "GET", "path": "/stream", "headers": []}
    await runner.run_stream_session(scope, pair)

    messages: list[dict[str, Any]] = []
    while not pair.send_queue.empty():
        messages.append(await pair.pop_outbound())

    assert messages[0]["type"] == "http.response.start"
    assert [m["body"] for m in messages[1:]] == [b"chunk-0", b"chunk-1", b"chunk-2"]
