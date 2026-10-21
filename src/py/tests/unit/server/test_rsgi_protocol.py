"""Unit tests for native RSGI v1.6 scope, protocol, stream transport, and adapter lifecycle."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vertebra._vertebra import (
    Request,
    Response,
    RsgiHeaders,
    RsgiHttpProtocol,
    RsgiScope,
    RsgiTransport,
    RsgiWebsocketProtocol,
)
from vertebra.cli.run import run as cli_run
from vertebra.server._native import (
    BatchDispatchPlan,
    _detect_protocol,
    _dispatch_batch,
    _instantiate_mount_runners,
)
from vertebra.server._rsgi import RsgiAdapter, detect_interface, is_rsgi_app


class SampleRsgiApp:
    """RSGI v1.6 test application implementing lifecycle and request hooks."""

    def __init__(self) -> None:
        self.initialized_with_loop: asyncio.AbstractEventLoop | None = None
        self.closed_with_loop: asyncio.AbstractEventLoop | None = None

    def __rsgi_init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the event loop provided at worker startup."""
        self.initialized_with_loop = loop

    async def __rsgi__(self, scope: RsgiScope, protocol: RsgiHttpProtocol) -> None:
        """Dispatch test endpoints using RSGI v1.6 protocol methods."""
        if scope.path == "/empty":
            protocol.response_empty(204, [("x-rsgi", "1.6")])
            return
        if scope.path == "/str":
            protocol.response_str(200, [("content-type", "text/plain")], f"hello:{scope.method}")
            return
        if scope.path == "/bytes":
            raw = await protocol()
            protocol.response_bytes(200, [("content-type", "application/octet-stream")], raw)
            return
        if scope.path == "/file":
            file_target = scope.query_string
            protocol.response_file(200, [("content-type", "text/plain")], file_target)
            return
        if scope.path == "/range":
            file_target = scope.query_string
            protocol.response_file_range(206, [("content-type", "text/plain")], file_target, 2, 7)
            return
        if scope.path == "/stream":
            transport: RsgiTransport = protocol.response_stream(200, [("content-type", "text/plain")])
            await transport.send_str("chunk-1:")
            await transport.send_bytes(b"chunk-2")
            return
        protocol.response_empty(404, [])

    def __rsgi_del__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Record the event loop provided at worker shutdown."""
        self.closed_with_loop = loop


def test_rsgi_headers_mapping_operations() -> None:
    """Verify RsgiHeaders case-insensitive lookup, multi-value get_all, and iteration."""
    headers = RsgiHeaders(
        [
            ("Content-Type", "application/json"),
            ("X-Custom", "one"),
            ("x-custom", "two"),
        ]
    )
    assert len(headers) == 3
    assert "content-type" in headers
    assert "CONTENT-TYPE" in headers
    assert headers["content-type"] == "application/json"
    assert headers.get("x-custom") == "one"
    assert headers.get("missing", "fallback") == "fallback"
    assert headers.get_all("X-Custom") == ["one", "two"]
    assert headers.keys() == ["content-type", "x-custom", "x-custom"]
    assert headers.values() == ["application/json", "one", "two"]
    assert headers.items() == [
        ("content-type", "application/json"),
        ("x-custom", "one"),
        ("x-custom", "two"),
    ]


def test_rsgi_scope_v16_attributes() -> None:
    """Verify RsgiScope exposes all RSGI v1.6 attributes with rsgi_version == '1.6'."""
    headers = RsgiHeaders([("host", "example.com"), ("x-trace", "abc")])
    scope = RsgiScope(
        proto="http",
        http_version="2",
        server="127.0.0.1:8000",
        client="127.0.0.1:54321",
        scheme="https",
        method="POST",
        path="/api/items",
        query_string="limit=10",
        headers=headers,
        authority="example.com",
    )
    assert scope.proto == "http"
    assert scope.http_version == "2"
    assert scope.rsgi_version == "1.6"
    assert scope.server == "127.0.0.1:8000"
    assert scope.client == "127.0.0.1:54321"
    assert scope.scheme == "https"
    assert scope.method == "POST"
    assert scope.path == "/api/items"
    assert scope.query_string == "limit=10"
    assert scope.authority == "example.com"
    assert scope.headers["x-trace"] == "abc"


def test_rsgi_protocol_file_range_validation(tmp_path: Path) -> None:
    """Verify response_file_range enforces start < end and records valid range metadata."""
    sample = tmp_path / "sample.txt"
    sample.write_bytes(b"0123456789abcdef")

    protocol = RsgiHttpProtocol(body=b"", request_id=42)
    with pytest.raises(ValueError, match="start must be less than end"):
        protocol.response_file_range(206, [], str(sample), 8, 8)

    with pytest.raises(ValueError, match="start must be less than end"):
        protocol.response_file_range(206, [], str(sample), 10, 4)

    protocol.response_file_range(
        206,
        [("content-type", "text/plain")],
        str(sample),
        2,
        7,
    )
    built = protocol.take_response()
    assert built.status == 206
    assert built.body == b"23456"
    assert protocol.file_range == (2, 7)
    assert protocol.file_path == str(sample)


def test_rsgi_adapter_lifecycle_and_dispatch(tmp_path: Path) -> None:
    """Verify RsgiAdapter runs __rsgi_init__, dispatches all response kinds, and runs __rsgi_del__."""
    sample = tmp_path / "payload.txt"
    sample.write_bytes(b"0123456789")

    app = SampleRsgiApp()
    adapter = RsgiAdapter(app)
    loop = asyncio.new_event_loop()
    try:
        adapter.startup(loop)
        assert app.initialized_with_loop is loop

        empty_resp = loop.run_until_complete(adapter.handle_http(method="GET", path="/empty"))
        assert empty_resp.status == 204
        assert empty_resp.body == b""

        str_resp = loop.run_until_complete(adapter.handle_http(method="POST", path="/str"))
        assert str_resp.status == 200
        assert str_resp.body == b"hello:POST"

        bytes_resp = loop.run_until_complete(adapter.handle_http(method="PUT", path="/bytes", body=b"raw-payload"))
        assert bytes_resp.status == 200
        assert bytes_resp.body == b"raw-payload"

        file_resp = loop.run_until_complete(adapter.handle_http(method="GET", path="/file", query_string=str(sample)))
        assert file_resp.status == 200
        assert file_resp.body == b"0123456789"

        range_resp = loop.run_until_complete(adapter.handle_http(method="GET", path="/range", query_string=str(sample)))
        assert range_resp.status == 206
        assert range_resp.body == b"23456"

        stream_resp = loop.run_until_complete(adapter.handle_http(method="GET", path="/stream"))
        assert stream_resp.status == 200
        assert stream_resp.body == b"chunk-1:chunk-2"

        adapter.shutdown(loop)
        assert app.closed_with_loop is loop
    finally:
        loop.close()


def test_rsgi_websocket_protocol_and_interface_detection() -> None:
    """Verify RsgiWebsocketProtocol message exchange and automatic interface detection."""
    ws_proto = RsgiWebsocketProtocol(incoming=[b"ping", "text-frame"])
    loop = asyncio.new_event_loop()
    try:
        accepted = loop.run_until_complete(ws_proto.accept())
        assert accepted is True
        first = loop.run_until_complete(ws_proto.receive())
        second = loop.run_until_complete(ws_proto.receive())
        assert first == b"ping"
        assert second == "text-frame"
        loop.run_until_complete(ws_proto.send_bytes(b"pong"))
        loop.run_until_complete(ws_proto.send_str("ack"))
        ws_proto.close(1000)
        assert ws_proto.sent_messages() == [b"pong", b"ack"]
        assert ws_proto.close_code == 1000
    finally:
        loop.close()

    rsgi_app = SampleRsgiApp()
    assert is_rsgi_app(rsgi_app) is True
    assert detect_interface(rsgi_app, "auto") == "rsgi"

    async def asgi_app(scope: Any, receive: Any, send: Any) -> None:
        _ = (scope, receive, send)

    def wsgi_app(environ: Any, start_response: Any) -> list[bytes]:
        _ = (environ, start_response)
        return [b"ok"]

    assert detect_interface(asgi_app, "auto") == "asgi"
    assert detect_interface(wsgi_app, "auto") == "wsgi"


def test_cli_run_interface_option() -> None:
    """Verify vertebra run CLI accepts --interface auto|asgi|rsgi|wsgi."""
    runner = CliRunner()
    result = runner.invoke(cli_run, ["--help"])
    assert result.exit_code == 0
    assert "--interface" in result.output
    assert "rsgi" in result.output


def test_native_worker_mount_dispatch_for_rsgi() -> None:
    """Verify _native.py detects RSGI apps, instantiates RsgiAdapter, and dispatches requests."""
    app = SampleRsgiApp()
    assert _detect_protocol(app) == "rsgi"

    loop = asyncio.new_event_loop()
    try:
        mounts = _instantiate_mount_runners([("", app, "rsgi")], loop)
        assert len(mounts) == 1
        assert mounts[0][2] == "rsgi"
        assert app.initialized_with_loop is loop

        req = Request.from_parts("POST", "/str", b"", request_id=77, handler_id=0)
        plan = BatchDispatchPlan(mounts={"": [(0, req)]})
        results = _dispatch_batch(
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
