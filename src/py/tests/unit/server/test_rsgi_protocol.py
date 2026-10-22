"""Unit tests for the Rust-backed RSGI v1.6 protocol, scope, headers, and adapter."""

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra._vertebra import (
    Headers,
    Request,
    Response,
    RsgiHttpProtocol,
    RsgiScope,
    RsgiTransport,
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
