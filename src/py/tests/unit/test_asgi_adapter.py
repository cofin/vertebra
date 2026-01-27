"""Tests for the Vertebra ASGI adapter."""

from __future__ import annotations

from typing import Any

import pytest

from vertebra.core.asgi import VertebraAdapter
from vertebra.core.lifecycle import App
from vertebra.web.routing import get


@get("/hello")
def hello_handler() -> str:
    return "Hello, world!"


@get("/echo/{message:str}")
async def echo_handler(message: str) -> dict[str, str]:
    return {"message": message}


@pytest.mark.anyio
async def test_asgi_adapter_http_flow() -> None:
    app = App()
    app.register(hello_handler)
    app.register(echo_handler)
    adapter = VertebraAdapter(app)

    # Test GET /hello
    scope = {"type": "http", "method": "GET", "path": "/hello"}

    events = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request"}

    async def send(message: dict[str, Any]) -> None:
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
    app = App()
    adapter = VertebraAdapter(app)

    scope = {"type": "http", "method": "GET", "path": "/not-found"}
    events = []

    async def receive() -> dict[str, Any]:
        return {}

    async def send(m: dict[str, Any]) -> None:
        events.append(m)

    await adapter(scope, receive, send)

    assert len(events) >= 1
    assert events[0]["status"] == 404
