from __future__ import annotations

import threading

import pytest

from vertebra.ipc.channel import BufferView, Channel, ChannelError, ZeroCopyChannel
from vertebra.procedures import ProcedureRegistry, procedure


def test_channel_pair_roundtrip() -> None:
    registry = ProcedureRegistry.instance()
    registry.clear()

    @procedure(name="ping")
    def ping(value: int) -> dict[str, int]:
        return {"ok": value}

    client, server = Channel.create_pair(slot_size=2048, slots=8)

    def server_loop() -> None:
        proc_id, params = server.receive_sync(timeout_ms=1000)
        assert proc_id == "ping"
        assert params["value"] == 42
        server.respond_sync(proc_id, {"ok": params["value"]})

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()
    result = client.send_sync("ping", {"value": 42}, timeout_ms=1000)
    thread.join(timeout=2)
    assert result["ok"] == 42


def test_buffer_view_memoryview() -> None:
    from vertebra.ipc import BufferPool

    pool = BufferPool(16, 1)
    buffer = pool.acquire()
    assert buffer is not None
    buffer.write(b"hello")
    view = BufferView(buffer, 5)
    mv = view.as_memoryview()
    assert bytes(mv) == b"hello"
    mv[0] = ord("H")
    assert buffer.read()[:1] == b"H"


def test_buffer_view_msgspec() -> None:
    msgspec = pytest.importorskip("msgspec")

    from vertebra.ipc import BufferPool

    class User(msgspec.Struct):
        id: int
        name: str

    payload = msgspec.json.encode(User(id=1, name="A"))
    pool = BufferPool(64, 1)
    buffer = pool.acquire()
    assert buffer is not None
    buffer.write(payload)
    view = BufferView(buffer, len(payload))
    assert view.as_msgspec(User) == User(id=1, name="A")


def test_zero_copy_channel_roundtrip() -> None:
    channel = ZeroCopyChannel(buffer_size=64, count=2, slots=4)
    view = channel.acquire()
    assert view is not None
    message = b"payload"
    mv = view.as_memoryview()
    mv[: len(message)] = message
    channel.send(view, len(message))
    recv_view = channel.recv()
    assert recv_view.as_bytes() == message


def test_channel_error_context() -> None:
    client, _ = Channel.create_pair(slot_size=64, slots=2)
    payload = {"data": "x" * 2048}
    with pytest.raises(ChannelError) as exc:
        client.send_sync("overflow", payload)
    err = exc.value
    assert err.procedure_id == "overflow"
    assert err.stage == "send_request"


def test_channel_close() -> None:
    client, _ = Channel.create_pair(slot_size=64, slots=2)
    client.close()
    with pytest.raises(ChannelError) as exc:
        client.send_sync("closed", {"value": 1})
    assert exc.value.stage == "closed"
