"""Tests for the Vertebra ASGI adapter."""

from __future__ import annotations

import pytest

from vertebra.asgi import VertebraAdapter
from vertebra.core.lifecycle import Vertex
from vertebra.web.routing import get


@get("/hello")
def hello_handler() -> str:
    return "Hello, world!"


@get("/echo/{message:str}")
async def echo_handler(message: str) -> dict[str, str]:
    return {"message": message}


@pytest.mark.anyio
async def test_asgi_adapter_http_flow() -> None:
    app = Vertex()
    app.register(hello_handler)
    app.register(echo_handler)
    adapter = VertebraAdapter(app)

    # Test GET /hello
    scope = {"type": "http", "method": "GET", "path": "/hello"}

    events = []

    async def receive() -> dict[str, str]:
        return {"type": "http.request"}

    async def send(message) -> None:
        events.append(message)

    await adapter(scope, receive, send)

    assert len(events) == 2
    assert events[0]["type"] == "http.response.start"
    assert events[0]["status"] == 200
    assert events[1]["type"] == "http.response.body"
    assert events[1]["body"] == b"Hello, world!"

    # Test GET /echo/test
    events.clear()
    scope["path"] = "/echo/test"

    await adapter(scope, receive, send)

    assert len(events) == 2
    assert events[0]["type"] == "http.response.start"
    assert events[0]["status"] == 200
    assert events[1]["type"] == "http.response.body"
    assert events[1]["body"] == b'{"message": "test"}'


@pytest.mark.anyio
async def test_asgi_adapter_404() -> None:
    app = Vertex()
    adapter = VertebraAdapter(app)

    scope = {"type": "http", "method": "GET", "path": "/not-found"}
    events = []

    async def receive() -> dict[str, str]:
        return {}

    async def send(m) -> None:
        events.append(m)

    await adapter(scope, receive, send)

    assert len(events) >= 1
    assert events[0]["status"] == 404
