"""Unit tests for bounded ASGI streaming queues and watermark backpressure."""

import asyncio
from typing import Any

import pytest

from vertebra.db.driver import NotificationEvent
from vertebra.extensions.django.channels import ChannelLayer
from vertebra.framework.channels import create_channels_backend
from vertebra.server._asgi._queues import (
    DEFAULT_STREAM_QUEUE_MAXSIZE,
    AsgiStreamQueuePair,
)
from vertebra.server._asgi._runner import AsgiRunner
from vertebra.server._channels import ChannelBus


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


class _ConformingAsyncNotificationSource:
    """Async notification source conforming to AsyncNotificationSource protocol."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.subscribed: set[str] = set()
        self._queue: asyncio.Queue[NotificationEvent] = asyncio.Queue()

    async def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))

    async def subscribe(self, channels: Any) -> None:
        self.subscribed.update(channels)

    async def unsubscribe(self, channels: Any) -> None:
        for c in channels:
            self.subscribed.discard(c)

    async def listen(self) -> Any:
        while True:
            evt = await self._queue.get()
            yield evt

    def inject_event(self, channel: str, payload: bytes, sender_id: str | None = None) -> None:
        self._queue.put_nowait(NotificationEvent(channel=channel, payload=payload, sender_id=sender_id))


class _ConformingSyncNotificationSource:
    """Sync notification source conforming to SyncNotificationSource protocol."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self.subscribed: set[str] = set()
        self._events: list[Any] = []

    def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))

    def subscribe(self, channels: Any) -> None:
        self.subscribed.update(channels)

    def unsubscribe(self, channels: Any) -> None:
        for c in channels:
            self.subscribed.discard(c)

    def listen(self) -> Any:
        yield from self._events

    def inject_event(self, channel: str, payload: bytes, sender_id: str | None = None) -> None:
        self._events.append(NotificationEvent(channel=channel, payload=payload, sender_id=sender_id))


@pytest.mark.anyio
async def test_channel_bus_bounded_backpressure_drops_oldest() -> None:
    """Verify ChannelBus stream queue does not block or grow unboundedly when consumer stalls."""
    bus = ChannelBus(maxsize=3)
    await bus.on_startup()
    await bus.subscribe(["updates"])

    stream = bus.stream_events()
    fetch_first = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    queue = next(iter(bus._queues))

    for idx in range(10):
        await bus.publish(f"msg-{idx}", ["updates"])

    first_item = await fetch_first
    assert first_item[1] == b"msg-7"
    assert queue.qsize() == 2
    items = []
    while not queue.empty():
        items.append(await queue.get())

    assert [item[1] for item in items if item is not None] == [b"msg-8", b"msg-9"]
    await bus.on_shutdown()


@pytest.mark.anyio
async def test_channel_bus_origin_node_deduplication() -> None:
    """Verify cross-node frames originating from the same node are not delivered twice."""
    source1 = _ConformingAsyncNotificationSource()
    source2 = _ConformingAsyncNotificationSource()

    bus1 = ChannelBus(node_id="node-alpha", notification_source=source1)
    bus2 = ChannelBus(node_id="node-beta", notification_source=source2)

    await bus1.on_startup()
    await bus2.on_startup()
    await bus1.subscribe(["chat"])
    await bus2.subscribe(["chat"])

    node1_received: list[bytes] = []
    node2_received: list[bytes] = []

    bus1.add_listener(lambda ch, payload: node1_received.append(payload))
    bus2.add_listener(lambda ch, payload: node2_received.append(payload))

    await bus1.publish(b"hello from alpha", ["chat"])

    assert node1_received == [b"hello from alpha"]
    assert len(source1.published) == 1
    remote_channel, remote_envelope = source1.published[0]

    pump_task1 = asyncio.create_task(bus1.pump_notifications(source1))
    pump_task2 = asyncio.create_task(bus2.pump_notifications(source2))

    source1.inject_event(remote_channel, remote_envelope)
    source2.inject_event(remote_channel, remote_envelope)

    await asyncio.sleep(0.05)

    assert node1_received == [b"hello from alpha"]
    assert node2_received == [b"hello from alpha"]

    pump_task1.cancel()
    pump_task2.cancel()
    await bus1.aclose()
    await bus2.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("backend_kind", ["memory", "redis", "valkey", "postgres", "oracle"])
async def test_channels_backend_factory_and_django_layer_integration(backend_kind: str) -> None:
    """Verify create_channels_backend and Django ChannelLayer across all 5 backend types."""
    async_source = _ConformingAsyncNotificationSource()
    sync_source = _ConformingSyncNotificationSource()

    backend = create_channels_backend(
        backend=backend_kind,
        notification_source=async_source,
        sync_notification_source=sync_source,
        node_id="test-node",
    )
    assert backend.backend_kind == backend_kind
    assert backend.node_id == "test-node"

    layer = ChannelLayer(bus=backend, capacity=50)
    ch1 = await layer.new_channel("user1")
    ch2 = await layer.new_channel("user2")

    await layer.group_add("alerts", ch1)
    await layer.group_add("alerts", ch2)

    await layer.group_send("alerts", {"level": "warning", "code": 101})

    msg1 = await layer.receive(ch1)
    msg2 = await layer.receive(ch2)

    assert msg1 == {"level": "warning", "code": 101}
    assert msg2 == {"level": "warning", "code": 101}

    layer.publish_sync(ch1, {"sync": True})
    assert len(sync_source.published) == 1

    await layer.group_discard("alerts", ch1)
    await layer.group_send("alerts", {"level": "info"})

    msg2_next = await layer.receive(ch2)
    assert msg2_next == {"level": "info"}

    await layer.flush()
    await layer.aclose()
    layer.close()
