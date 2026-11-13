"""Unit tests for the Rust-backed RSGI v1.6 protocol, scope, headers, and adapter."""

import asyncio
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra._vertebra import (
    STREAM_FLAG_ERROR,
    STREAM_FLAG_FINAL,
    STREAM_FLAG_HEADERS_SENT,
    STREAM_FLAG_STREAMING,
    Headers,
    Request,
    Response,
    RsgiHttpProtocol,
    RsgiScope,
    RsgiTransport,
    _set_stream_sink,
    drive_rsgi_batch,
    init_transport,
)
from vertebra.cli import cli
from vertebra.server._dispatch import BatchDispatchPlan, dispatch_batch
from vertebra.server._protocol import detect_interface
from vertebra.server._rsgi import RsgiAdapter, is_rsgi_app
from vertebra.server.worker import _instantiate_mount_runners


class RsgiWebsocketMockTransport:
    """In-memory WebSocket transport fixture for RSGI WebSocket protocol tests."""

    __slots__ = ("_closed", "_incoming", "_sent")

    def __init__(self, incoming: Iterable[bytes | str] = ()) -> None:
        self._incoming = list(incoming)
        self._sent: list[bytes | str] = []
        self._closed = False

    async def receive(self) -> dict[str, Any]:
        """Return the next inbound WebSocket frame or a close frame when exhausted."""
        if self._incoming:
            item = self._incoming.pop(0)
            kind = "bytes" if isinstance(item, bytes) else "str"
            return {"type": kind, "data": item}
        self._closed = True
        return {"type": "close", "code": 1000}

    async def send_bytes(self, data: bytes) -> None:
        """Record an outbound binary WebSocket frame."""
        self._sent.append(bytes(data))

    async def send_str(self, data: str) -> None:
        """Record an outbound text WebSocket frame."""
        self._sent.append(str(data))


class RsgiWebsocketProtocol:
    """Test fixture implementing the RSGI WebSocket protocol handshake and framing."""

    __slots__ = ("_accepted", "_close_code", "_incoming", "_transport")

    def __init__(self, incoming: Iterable[bytes | str] = ()) -> None:
        self._incoming = list(incoming)
        self._accepted = False
        self._close_code: int | None = None
        self._transport = RsgiWebsocketMockTransport(self._incoming)

    @property
    def accepted(self) -> bool:
        """Return True if accept() was awaited."""
        return self._accepted

    @property
    def close_code(self) -> int | None:
        """Return the WebSocket close code if closed."""
        return self._close_code

    @property
    def sent_messages(self) -> list[bytes | str]:
        """Return all outbound frames sent on the transport."""
        return list(self._transport._sent)

    async def accept(self) -> RsgiWebsocketMockTransport:
        """Complete the WebSocket upgrade and return the frame transport."""
        self._accepted = True
        return self._transport

    def close(self, status: int = 1000) -> None:
        """Close the WebSocket session with ``status``."""
        self._close_code = status


class SampleRsgiApp:
    """Sample RSGI v1.6 application class exposing __rsgi__, __rsgi_init__, and __rsgi_del__."""

    def __init__(self) -> None:
        self.initialized_with_loop: Any = None
        self.cleaned_up_with_loop: Any = None

    def __rsgi_init__(self, loop: Any) -> None:
        self.initialized_with_loop = loop

    def __rsgi_del__(self, loop: Any) -> None:
        self.cleaned_up_with_loop = loop

    async def __rsgi__(self, scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
        if scope.path == "/empty":
            protocol.response_empty(204, [("x-mode", "empty")])
            return
        if scope.path == "/str":
            protocol.response_str(
                200,
                [("content-type", "text/plain")],
                f"hello:{scope.method}",
            )
            return
        if scope.path == "/bytes":
            body = await protocol()
            protocol.response_bytes(
                200,
                [("content-type", "application/octet-stream")],
                b"echo:" + body,
            )
            return
        if scope.path == "/stream":
            transport = protocol.response_stream(200, [("content-type", "text/plain")])
            await transport.send_str("chunk-1|")
            await transport.send_bytes(b"chunk-2")
            return
        protocol.response_str(404, [], "not found")


def test_rsgi_headers_lookup_and_iteration() -> None:
    """Verify Headers.from_pairs case-insensitive lookup, multi-value get_all, and iteration."""
    headers = Headers.from_pairs(
        [
            ("Content-Type", "application/json"),
            ("Set-Cookie", "a=1"),
            ("set-cookie", "b=2"),
            ("X-Custom", "value"),
        ]
    )
    assert len(headers) == 4
    assert "content-type" in headers
    assert "CONTENT-TYPE" in headers
    assert "missing" not in headers
    assert headers["Content-Type"] == "application/json"
    assert headers.get("x-custom") == "value"
    assert headers.get("missing", "fallback") == "fallback"
    assert headers.get_all("set-cookie") == ["a=1", "b=2"]
    assert headers.keys() == ["content-type", "set-cookie", "set-cookie", "x-custom"]
    assert headers.values() == ["application/json", "a=1", "b=2", "value"]
    assert len(headers.items()) == 4

    with pytest.raises(KeyError):
        _ = headers["nonexistent"]


def test_rsgi_scope_properties() -> None:
    """Verify RsgiScope exposes all RSGI v1.6 attributes in Rust."""
    scope = RsgiScope(
        proto="http",
        http_version="2",
        server="127.0.0.1:8443",
        client="10.0.0.1:54321",
        scheme="https",
        method="POST",
        path="/api/v1/items",
        query_string="limit=10",
        headers=[("host", "example.com"), ("x-trace", "abc")],
        authority="example.com",
    )
    assert scope.proto == "http"
    assert scope.rsgi_version == "1.6"
    assert scope.http_version == "2"
    assert scope.server == "127.0.0.1:8443"
    assert scope.client == "10.0.0.1:54321"
    assert scope.scheme == "https"
    assert scope.method == "POST"
    assert scope.path == "/api/v1/items"
    assert scope.query_string == "limit=10"
    assert scope.authority == "example.com"
    assert scope.headers.get("x-trace") == "abc"


@pytest.mark.anyio
async def test_rsgi_http_protocol_response_methods(tmp_path: Path) -> None:
    """Verify RsgiHttpProtocol handles str, bytes, empty, file, file_range, and stream."""
    app = SampleRsgiApp()
    adapter = RsgiAdapter(app)

    resp_empty = await adapter.handle_http(method="GET", path="/empty")
    assert resp_empty.status == 204
    assert resp_empty.body == b""
    assert (b"x-mode", b"empty") in resp_empty.headers

    resp_str = await adapter.handle_http(method="GET", path="/str")
    assert resp_str.status == 200
    assert resp_str.body == b"hello:GET"

    resp_bytes = await adapter.handle_http(method="POST", path="/bytes", body=b"payload")
    assert resp_bytes.status == 200
    assert resp_bytes.body == b"echo:payload"

    resp_stream = await adapter.handle_http(method="GET", path="/stream")
    assert resp_stream.status == 200
    assert resp_stream.body == b"chunk-1|chunk-2"

    sample_file = tmp_path / "asset.txt"
    sample_file.write_bytes(b"0123456789abcdef")

    proto_file = RsgiHttpProtocol(body=b"", request_id=11)
    proto_file.response_file(200, [("content-type", "text/plain")], str(sample_file))
    assert proto_file.file_path == str(sample_file)
    file_resp = proto_file.take_response()
    assert file_resp.status == 200
    assert file_resp.body == b"0123456789abcdef"

    proto_range = RsgiHttpProtocol(body=b"", request_id=12)
    proto_range.response_file_range(
        206,
        [("content-type", "text/plain")],
        str(sample_file),
        4,
        10,
    )
    assert proto_range.file_range == (4, 10)
    range_resp = proto_range.take_response()
    assert range_resp.status == 206
    assert range_resp.body == b"456789"


@pytest.mark.anyio
async def test_rsgi_transport_direct() -> None:
    """Verify RsgiTransport can be instantiated and used via response_stream."""
    proto = RsgiHttpProtocol(body=b"req-body", request_id=99)
    transport = proto.response_stream(200, [("x-stream", "1")])
    assert isinstance(transport, RsgiTransport)
    await transport.send_str("part1-")
    await transport.send_bytes(b"part2")
    proto.notify_disconnect()
    await proto.client_disconnect()
    resp = proto.take_response()
    assert resp.status == 200
    assert resp.body == b"part1-part2"
    assert resp.request_id == 99


@pytest.mark.anyio
async def test_rsgi_client_disconnect_remains_pending_until_signaled() -> None:
    """Verify client_disconnect() waits until notify_disconnect() is signaled on an active protocol."""
    proto = RsgiHttpProtocol(body=b"active", request_id=105)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(proto.client_disconnect(), timeout=0.05)

    proto.notify_disconnect()
    await asyncio.wait_for(proto.client_disconnect(), timeout=0.5)


@pytest.mark.anyio
async def test_rsgi_websocket_protocol_and_interface_detection() -> None:
    """Verify RsgiWebsocketProtocol message exchange and automatic interface detection."""
    ws_proto = RsgiWebsocketProtocol(incoming=[b"ping", "text-frame"])
    ws_transport = await ws_proto.accept()
    assert ws_proto.accepted is True

    msg1 = await ws_transport.receive()
    assert msg1 == {"type": "bytes", "data": b"ping"}
    msg2 = await ws_transport.receive()
    assert msg2 == {"type": "str", "data": "text-frame"}
    msg3 = await ws_transport.receive()
    assert msg3["type"] == "close"

    await ws_transport.send_bytes(b"pong")
    await ws_transport.send_str("ack")
    ws_proto.close(1000)
    assert ws_proto.close_code == 1000
    assert ws_proto.sent_messages == [b"pong", "ack"]

    rsgi_app = SampleRsgiApp()
    assert is_rsgi_app(rsgi_app) is True
    assert detect_interface(rsgi_app, "auto") == "rsgi"
    assert detect_interface(rsgi_app, "asgi") == "asgi"

    async def dummy_asgi(scope: Any, receive: Any, send: Any) -> None:
        del scope, receive, send

    def dummy_wsgi(environ: Any, start_response: Any) -> list[bytes]:
        del environ, start_response
        return [b"ok"]

    assert detect_interface(dummy_asgi, "auto") == "asgi"
    assert detect_interface(dummy_wsgi, "auto") == "wsgi"


def test_cli_run_and_dev_interface_flag() -> None:
    """Verify `vertebra run --interface` and `vertebra dev --interface` accept rsgi/asgi/wsgi/auto."""
    runner = CliRunner()
    for subcmd in ("run", "dev"):
        result = runner.invoke(cli, [subcmd, "--help"])
        assert result.exit_code == 0
        assert "--interface" in result.output
        assert "rsgi" in result.output


def test_native_worker_mount_dispatch_for_rsgi() -> None:
    """Verify worker detects RSGI apps, instantiates RsgiAdapter, and dispatches requests."""
    app = SampleRsgiApp()
    assert detect_interface(app) == "rsgi"

    loop = asyncio.new_event_loop()
    try:
        mounts = _instantiate_mount_runners([("", app, "rsgi")], loop)
        assert len(mounts) == 1
        assert mounts[0][2] == "rsgi"
        assert app.initialized_with_loop is loop

        req = Request.from_parts("POST", "/str", b"", request_id=77, handler_id=0)
        plan = BatchDispatchPlan(mounts={"": [(0, req)]})
        results = dispatch_batch(
            plan,
            {},
            mounts,
            Response,
            loop,
            async_mode=False,
        )
        assert results is not None
        assert len(results) == 1
        assert results[0].status == 200
        assert results[0].body == b"hello:POST"
        assert results[0].request_id == 77
    finally:
        loop.close()


def test_rsgi_scope_lazy_cached_headers_root_path_and_interning() -> None:
    """Verify RsgiScope lazily resolves fields, caches headers, slices root_path, and interns strings."""
    req1 = Request.from_parts(
        "GET",
        "/api/v1/users?active=1",
        b"",
        request_id=801,
        headers=[("Host", "api.example.com"), ("X-Trace", "t1")],
    )
    req2 = Request.from_parts("GET", "/api/v1", b"", request_id=802)

    s1 = req1.rsgi_scope(root_path="/api/v1")
    s2 = req2.rsgi_scope(root_path="/api/v1")

    assert s1.path == "/users"
    assert s1.root_path == "/api/v1"
    assert s1.query_string == "active=1"
    assert s1.authority == "api.example.com"
    assert s2.path == "/"
    assert s2.query_string == ""
    assert s1.proto is s2.proto
    assert s1.rsgi_version is s2.rsgi_version
    assert s1.http_version is s2.http_version
    assert s1.scheme is s2.scheme
    assert s1.method is s2.method
    assert list(s1.headers.items()) == list(s1.headers.items())


def test_rsgi_done_concurrent_awaits_across_threads() -> None:
    """Verify concurrent threads awaiting RSGI_DONE never trigger PyBorrowMutError."""
    proto = RsgiHttpProtocol(body=b"", request_id=803)
    transport = proto.response_stream(200, [])

    def _worker(idx: int) -> None:
        awaitable = transport.send_str(f"c{idx}|")
        it = awaitable.__await__()
        with pytest.raises(StopIteration) as stop:
            next(it)
        assert stop.value.value is None

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(_worker, range(64)))

    resp = proto.take_response()
    assert resp.status == 200
    assert len(resp.body) > 128


@pytest.mark.anyio
async def test_rsgi_http_protocol_direct_submit_and_drive_rsgi_batch() -> None:
    """Verify direct-submitting RsgiHttpProtocol and zero-task unary drive_rsgi_batch."""
    init_transport()
    direct_proto = RsgiHttpProtocol(body=b"ping", request_id=810, direct_submit=True)
    direct_proto.response_str(200, [("content-type", "text/plain")], "pong")

    loop = asyncio.get_running_loop()
    orig_create_task = loop.create_task
    created_tasks: list[asyncio.Task[Any]] = []

    def tracked_create_task(coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        t = orig_create_task(coro, **kwargs)
        created_tasks.append(t)
        return t

    executed: list[str] = []

    class FastAndSlowRsgiApp:
        async def __rsgi__(self, scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
            if scope.path == "/fail":
                raise ValueError("rsgi failure")
            if scope.path == "/slow":
                await asyncio.sleep(0.005)
                stream = protocol.response_stream(200, [("content-type", "text/plain")])
                await stream.send_str("s1")
                await stream.send_bytes(b"s2")
                executed.append("/slow")
                return
            raw = await protocol()
            protocol.response_bytes(200, [("x-path", scope.path)], raw)
            executed.append(scope.path)

    app = FastAndSlowRsgiApp()
    unary_reqs = [
        Request.from_parts("POST", "/v1/a", b"1", request_id=811),
        Request.from_parts("POST", "/v1/b", b"2", request_id=812),
    ]
    streamed_frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        streamed_frames.append((req_id, status, flags, headers, body))
        return True

    _set_stream_sink(sink)
    create_task_attr = "create_task"
    setattr(loop, create_task_attr, tracked_create_task)
    try:
        inline_unary = drive_rsgi_batch(app, unary_reqs, loop_obj=loop, root_path="/v1")
        assert inline_unary == 2
        assert len(created_tasks) == 0

        mixed_reqs = [
            Request.from_parts("POST", "/c", b"3", request_id=813),
            Request.from_parts("GET", "/slow", b"", request_id=814),
            Request.from_parts("GET", "/fail", b"", request_id=815),
        ]
        inline_mixed = drive_rsgi_batch(app, mixed_reqs, loop_obj=loop)
        assert inline_mixed == 2
        assert len(created_tasks) == 1
        await asyncio.gather(*created_tasks)
    finally:
        setattr(loop, create_task_attr, orig_create_task)
        _set_stream_sink(None)

    assert executed == ["/a", "/b", "/c", "/slow"]
    assert streamed_frames == [
        (
            814,
            200,
            STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
            [(b"content-type", b"text/plain")],
            b"s1",
        ),
        (814, 200, STREAM_FLAG_STREAMING, [], b"s2"),
        (814, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b""),
    ]


@pytest.mark.anyio
@pytest.mark.parametrize("send_kind", ["str", "bytes"])
@pytest.mark.parametrize("scenario", ["live", "empty", "crash", "disconnect", "disconnect_before_first_send"])
async def test_rsgi_http_protocol_progressive_flush_error_and_disconnect(send_kind: str, scenario: str) -> None:
    """Verify RsgiTransport flushes progressive chunks, emits FLAG_ERROR, and handles disconnect cleanly."""
    init_transport()
    frames: list[tuple[int, int, int, list[tuple[bytes, bytes]], bytes]] = []
    gate = asyncio.Event()
    first_flushed = asyncio.Event()
    terminal_flushed = asyncio.Event()
    accept_client = scenario not in {"disconnect", "disconnect_before_first_send"}

    def sink(
        req_id: int,
        status: int,
        flags: int,
        headers: list[tuple[bytes, bytes]],
        body: bytes,
    ) -> bool:
        frames.append((req_id, status, flags, headers, body))
        if len(frames) == 1:
            first_flushed.set()
        if (flags & (STREAM_FLAG_FINAL | STREAM_FLAG_ERROR)) != 0:
            terminal_flushed.set()
        return accept_client

    class ProgressiveRsgiApp:
        async def __rsgi__(self, scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
            transport = protocol.response_stream(200, [("content-type", "text/event-stream")])
            if scope.path == "/empty-stream":
                return
            if scope.path == "/disc-early":
                protocol.notify_disconnect()
                await asyncio.sleep(0)
            if send_kind == "str":
                await transport.send_str("first-chunk\n")
                await transport.send_str("")
            else:
                await transport.send_bytes(b"first-chunk\n")
                await transport.send_bytes(b"")
            if scope.path == "/crash":
                await asyncio.sleep(0)
                msg = "rsgi mid-stream crash"
                raise RuntimeError(msg)
            await gate.wait()
            if send_kind == "str":
                await transport.send_str("second-chunk\n")
            else:
                await transport.send_bytes(b"second-chunk\n")

    loop = asyncio.get_running_loop()
    app = ProgressiveRsgiApp()
    _set_stream_sink(sink)
    try:
        if scenario == "live":
            drive_rsgi_batch(
                app,
                [Request.from_parts("GET", "/live", b"", request_id=850)],
                loop_obj=loop,
            )
            await asyncio.wait_for(first_flushed.wait(), timeout=1.0)
            assert not gate.is_set()
            assert frames == [
                (
                    850,
                    200,
                    STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                    [(b"content-type", b"text/event-stream")],
                    b"first-chunk\n",
                )
            ]
            gate.set()
            await asyncio.wait_for(terminal_flushed.wait(), timeout=1.0)
            assert frames == [
                (
                    850,
                    200,
                    STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                    [(b"content-type", b"text/event-stream")],
                    b"first-chunk\n",
                ),
                (850, 200, STREAM_FLAG_STREAMING, [], b"second-chunk\n"),
                (850, 200, STREAM_FLAG_STREAMING | STREAM_FLAG_FINAL, [], b""),
            ]
        elif scenario == "empty":
            drive_rsgi_batch(
                app,
                [Request.from_parts("GET", "/empty-stream", b"", request_id=851)],
                loop_obj=loop,
            )
            assert frames == [
                (
                    851,
                    200,
                    STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT | STREAM_FLAG_FINAL,
                    [(b"content-type", b"text/event-stream")],
                    b"",
                )
            ]
        elif scenario == "crash":
            drive_rsgi_batch(
                app,
                [Request.from_parts("GET", "/crash", b"", request_id=852)],
                loop_obj=loop,
            )
            await asyncio.wait_for(terminal_flushed.wait(), timeout=1.0)
            assert [f[2] for f in frames] == [
                STREAM_FLAG_STREAMING | STREAM_FLAG_HEADERS_SENT,
                STREAM_FLAG_STREAMING | STREAM_FLAG_ERROR,
            ]
        elif scenario == "disconnect":
            proto_disc = RsgiHttpProtocol(body=b"", request_id=853, direct_submit=True)
            disc_fut = proto_disc.client_disconnect()
            transport_disc = proto_disc.response_stream(200, [("x-stream", "1")])
            with pytest.raises(ConnectionResetError):
                if send_kind == "str":
                    await transport_disc.send_str("closed")
                else:
                    await transport_disc.send_bytes(b"closed")
            await asyncio.wait_for(disc_fut, timeout=1.0)
        else:
            early_done = asyncio.Event()

            class EarlyDisconnectApp:
                async def __rsgi__(self, _scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
                    transport = protocol.response_stream(200, [("content-type", "text/event-stream")])
                    protocol.notify_disconnect()
                    await asyncio.sleep(0)
                    try:
                        if send_kind == "str":
                            await transport.send_str("first-chunk\n")
                        else:
                            await transport.send_bytes(b"first-chunk\n")
                    finally:
                        early_done.set()

            drive_rsgi_batch(
                EarlyDisconnectApp(),
                [Request.from_parts("GET", "/disc-early", b"", request_id=854)],
                loop_obj=loop,
            )
            await asyncio.wait_for(early_done.wait(), timeout=1.0)
            await asyncio.sleep(0)
            assert frames == []
    finally:
        _set_stream_sink(None)
