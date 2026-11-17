"""Spikard benchmark application.

Mirrors the ASGI benchmark endpoints using Spikard's native API.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from spikard import Response, ServerConfig
from spikard.app import App

app = App(
    config=ServerConfig(
        host=os.environ.get("SPIKARD_HOST", "127.0.0.1"),
        port=int(os.environ.get("SPIKARD_PORT", "8005")),
        compression=None,
        enable_request_id=False,
        rate_limit=None,
    )
)

PLAINTEXT_BODY = "This is a plaintext response for benchmarking."
BODY_1K = "0" * 1024
BODY_10K = "0" * 10240
BODY_100K = "0" * 102400
JSON_USERS = [{"id": i, "name": f"User {i}", "email": f"user{i}@example.com", "active": True} for i in range(10)]


def _route[FnT: Callable[..., Awaitable[Any]]](method: Literal["GET", "POST"], path: str) -> Callable[[FnT], FnT]:
    """Wrap Spikard route registration with a concrete decorator type for mypy."""
    inner: Any = app.get(path) if method == "GET" else app.post(path)

    def _decorator(fn: FnT) -> FnT:
        inner(fn)
        return fn

    return _decorator


@_route("GET", "/hello")
async def hello() -> dict[str, str]:
    """Return minimal JSON payload."""
    return {"hello": "world"}


@_route("GET", "/plaintext")
async def plaintext() -> Response:
    """Return plaintext response."""
    return Response(content=PLAINTEXT_BODY, headers={"content-type": "text/plain"})


@_route("GET", "/b1k")
async def b1k() -> Response:
    """Return 1KB plaintext response."""
    return Response(content=BODY_1K, headers={"content-type": "text/plain"})


@_route("GET", "/b10k")
async def b10k() -> Response:
    """Return 10KB plaintext response."""
    return Response(content=BODY_10K, headers={"content-type": "text/plain"})


@_route("GET", "/b100k")
async def b100k() -> Response:
    """Return 100KB plaintext response."""
    return Response(content=BODY_100K, headers={"content-type": "text/plain"})


@_route("GET", "/json")
async def json_endpoint() -> dict[str, list[dict[str, Any]]]:
    """Return nested JSON response."""
    return {"users": JSON_USERS}


@_route("POST", "/echo")
async def echo(request: bytes) -> Response:
    """Echo POST body back as plaintext."""
    return Response(content=request.decode("utf-8"), headers={"content-type": "text/plain"})


@_route("GET", "/io10")
async def io10() -> Response:
    """Simulate a 10ms async sleep."""
    await asyncio.sleep(0.01)
    return Response(content="ok", headers={"content-type": "text/plain"})


@_route("GET", "/health")
async def health() -> Response:
    """Return health check status."""
    return Response(content="ok", headers={"content-type": "text/plain"})


if __name__ == "__main__":
    app.run()
