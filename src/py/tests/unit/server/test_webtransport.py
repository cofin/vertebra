"""Unit tests for embedded HTTP/3 WebTransport Python session and stream bridges."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, Protocol, cast

import pytest

import vertebra._vertebra as _native
from vertebra.server._webtransport import (
    WebTransportDispatcher,
    WebTransportSession,
    WebTransportStream,
    create_webtransport_dispatcher,
    register_webtransport_handler,
)


class _SyntheticStreamPeer(Protocol):
    @property
    def stream_id(self) -> int: ...

    def push_data(self, data: bytes | bytearray | memoryview) -> None: ...

    def finish_input(self) -> None: ...

    def take_written(self) -> bytes: ...

    def is_finished(self) -> bool: ...


class _SyntheticSessionPeer(Protocol):
    def push_client_stream(self, stream_id: int = 8) -> _SyntheticStreamPeer: ...

    def pop_server_stream(self) -> _SyntheticStreamPeer | None: ...

    def push_datagram(self, data: bytes | bytearray | memoryview) -> None: ...

    def pop_sent_datagram(self) -> bytes | None: ...

    def closed_info(self) -> tuple[bool, int | None, str | None]: ...

    def close_from_peer(self) -> None: ...


def _make_synthetic_pair(
    *,
    session_id: int = 4,
    path: str = "/wt?room=1",
    headers: list[tuple[bytes, bytes]] | None = None,
    submit_to_bridge: bool = False,
) -> tuple[Any, _SyntheticSessionPeer]:
    fn = cast(
        Callable[
            [int, str, list[tuple[bytes, bytes]] | None, bool],
            tuple[Any, _SyntheticSessionPeer],
        ],
        vars(_native)["_synthetic_webtransport_pair"],
    )
    return fn(session_id, path, headers, submit_to_bridge)


def _init_inprocess_transport() -> None:
    fn = cast(Callable[[str], None], vars(_native)["init_transport"])
    fn("inprocess")


@pytest.mark.parametrize(
    ("session_id", "path", "headers", "payload"),
    [
        (4, "/wt/echo?room=1", [(b"x-client", b"py")], b"hello-wt"),
        (12, "/wt/binary", [(b"origin", b"https://localhost")], b"\x00\x01\xfe\xff"),
    ],
)
def test_webtransport_session_and_stream_roundtrip(
    session_id: int,
    path: str,
    headers: list[tuple[bytes, bytes]],
    payload: bytes,
) -> None:
    """Verify WebTransportSession and WebTransportStream bidi and datagram operations."""

    async def _run() -> None:
        raw_session, peer = _make_synthetic_pair(
            session_id=session_id,
            path=path,
            headers=headers,
        )
        session = cast(WebTransportSession, raw_session)
        assert session.session_id == session_id
        assert session.path == path
        assert session.headers == headers

        client_stream_peer = peer.push_client_stream(8)
        client_stream_peer.push_data(payload)
        client_stream_peer.finish_input()

        accepted = await session.accept_bi()
        assert isinstance(accepted, WebTransportStream)
        assert accepted.stream_id == 8
        first_chunk = await accepted.recv(4)
        rest_chunk = await accepted.recv(64)
        eof_chunk = await accepted.recv(64)
        assert first_chunk + rest_chunk == payload
        assert eof_chunk == b""

        await accepted.send(bytearray(b"reply:" + payload))
        await accepted.finish()
        assert client_stream_peer.take_written() == b"reply:" + payload
        assert client_stream_peer.is_finished() is True

        outbound = await session.open_bi()
        outbound_peer = peer.pop_server_stream()
        assert outbound_peer is not None
        assert outbound.stream_id == outbound_peer.stream_id
        await outbound.send(memoryview(b"server-push"))
        await outbound.finish()
        assert outbound_peer.take_written() == b"server-push"
        assert outbound_peer.is_finished() is True

        peer.push_datagram(b"ping-dgram")
        assert await session.recv_datagram() == b"ping-dgram"

        dgram_view = memoryview(bytearray(b"pong-dgram"))
        session.send_datagram(dgram_view)
        assert peer.pop_sent_datagram() == b"pong-dgram"

        await session.close(1000, "done")
        assert peer.closed_info() == (True, 1000, "done")

    asyncio.run(_run())


def test_webtransport_closed_session_and_stream_errors() -> None:
    """Verify closed WebTransportSession and WebTransportStream raise ConnectionResetError."""

    async def _run() -> None:
        raw_session, peer = _make_synthetic_pair(session_id=20, path="/wt/closed")
        session = cast(WebTransportSession, raw_session)

        client_peer = peer.push_client_stream(12)
        stream = await session.accept_bi()
        await stream.finish()
        with pytest.raises(ConnectionResetError):
            await stream.send(b"after-finish")

        peer.close_from_peer()
        assert client_peer.is_finished() is True

        with pytest.raises(ConnectionResetError):
            await session.accept_bi()
        with pytest.raises(ConnectionResetError):
            await session.open_bi()
        with pytest.raises(ConnectionResetError):
            await session.recv_datagram()
        with pytest.raises(ConnectionResetError):
            session.send_datagram(b"after-close")
        with pytest.raises(ConnectionResetError):
            await stream.recv(16)
        with pytest.raises(ConnectionResetError):
            await stream.finish()

    asyncio.run(_run())


@pytest.mark.parametrize("use_native_register", [False, True])
def test_register_webtransport_handler_dispatches_through_batch_bridge(
    use_native_register: bool,
) -> None:
    """Verify BatchBridge dispatches WebTransportSession onto the registered event loop."""
    _init_inprocess_transport()

    async def _run() -> None:
        completed = asyncio.Event()
        observed_paths: list[str] = []

        async def _handler(session: WebTransportSession) -> None:
            observed_paths.append(session.path)
            stream = await session.accept_bi()
            data = await stream.recv(1024)
            await stream.send(b"echo:" + data)
            await stream.finish()
            dgram = await session.recv_datagram()
            session.send_datagram(memoryview(b"dg:" + dgram))
            await session.close(0, "ok")
            completed.set()

        try:
            if use_native_register:
                native_register = cast(
                    Callable[[Any], None],
                    vars(_native)["register_webtransport_handler"],
                )
                native_register(_handler)
            else:
                register_webtransport_handler(_handler)

            submitted, peer = _make_synthetic_pair(
                session_id=32,
                path="/wt/echo?id=9",
                submit_to_bridge=True,
            )
            assert submitted is True

            stream_peer = peer.push_client_stream(16)
            stream_peer.push_data(b"hello")
            stream_peer.finish_input()
            peer.push_datagram(b"pulse")

            await asyncio.wait_for(completed.wait(), timeout=2.0)
            assert observed_paths == ["/wt/echo?id=9"]
            assert stream_peer.take_written() == b"echo:hello"
            assert stream_peer.is_finished() is True
            assert peer.pop_sent_datagram() == b"dg:pulse"
            assert peer.closed_info() == (True, 0, "ok")
        finally:
            register_webtransport_handler(None)

    asyncio.run(_run())


def test_webtransport_dispatcher_routes_and_closes_unknown_paths() -> None:
    """Verify WebTransportDispatcher and create_webtransport_dispatcher route by path."""

    async def _run() -> None:
        dispatcher = WebTransportDispatcher()
        hits: list[str] = []

        @dispatcher.route("/wt/chat")
        async def _chat(session: WebTransportSession) -> None:
            hits.append(session.path)
            await session.close(0, "chat-done")

        chat_raw, chat_peer = _make_synthetic_pair(session_id=40, path="/wt/chat?room=7")
        await dispatcher(cast(WebTransportSession, chat_raw))
        assert hits == ["/wt/chat?room=7"]
        assert chat_peer.closed_info() == (True, 0, "chat-done")

        miss_raw, miss_peer = _make_synthetic_pair(session_id=44, path="/wt/missing")
        await dispatcher(cast(WebTransportSession, miss_raw))
        assert miss_peer.closed_info() == (True, 404, "Not Found")

        fn_dispatcher = create_webtransport_dispatcher({"/wt/fn": _chat})
        fn_raw, fn_peer = _make_synthetic_pair(session_id=48, path="/wt/fn")
        await fn_dispatcher(cast(WebTransportSession, fn_raw))
        assert fn_peer.closed_info() == (True, 0, "chat-done")

        miss_fn_raw, miss_fn_peer = _make_synthetic_pair(session_id=52, path="/wt/other")
        await fn_dispatcher(cast(WebTransportSession, miss_fn_raw))
        assert miss_fn_peer.closed_info() == (True, 404, "Not Found")

    asyncio.run(_run())


def test_register_webtransport_handler_drops_gracefully_after_loop_closes() -> None:
    """Verify BatchBridge drops incoming sessions safely after the registered loop closes."""
    _init_inprocess_transport()
    loop = asyncio.new_event_loop()
    invoked: list[int] = []

    async def _handler(session: WebTransportSession) -> None:
        invoked.append(session.session_id)

    try:
        register_webtransport_handler(_handler, loop=loop)
        loop.close()

        submitted, peer = _make_synthetic_pair(
            session_id=60,
            path="/wt/after-close",
            submit_to_bridge=True,
        )
        assert submitted is True
        assert invoked == []
        assert peer.closed_info()[0] is False
    finally:
        register_webtransport_handler(None)


def test_register_webtransport_handler_rejects_non_callable() -> None:
    """Verify non-callable registration raises TypeError."""
    _init_inprocess_transport()
    native_register = cast(
        Callable[[Any], None],
        vars(_native)["register_webtransport_handler"],
    )
    with pytest.raises(TypeError):
        native_register("not-callable")
