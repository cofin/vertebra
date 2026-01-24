"""Starlette benchmark application.

Minimal Starlette app for framework overhead testing.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from starlette.applications import Starlette
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

if TYPE_CHECKING:
    from starlette.requests import Request

# Pre-computed response bodies
BODY_10B = b"0123456789"
BODY_1K = b"x" * 1024
BODY_10K = b"x" * 10240
BODY_100K = b"x" * 102400

# Load test file
ASSETS_DIR = Path(__file__).parent / "assets"
FILE_PATH = ASSETS_DIR / "test.bin"
FILE_CONTENT = FILE_PATH.read_bytes() if FILE_PATH.exists() else b"x" * 51200

# JSON payload
JSON_DATA = {
    "message": "Hello, World!",
    "items": [{"id": i, "name": f"Item {i}", "active": i % 2 == 0} for i in range(10)],
    "metadata": {"version": "1.0", "benchmark": True},
}


async def health(request: Request) -> JSONResponse:
    """Health check endpoint."""
    return JSONResponse({"status": "ok"})


async def hello(request: Request) -> JSONResponse:
    """Minimal JSON response."""
    return JSONResponse({"hello": "world"})


async def body_10b(request: Request) -> Response:
    """10-byte response."""
    return Response(BODY_10B, media_type="application/octet-stream")


async def body_1k(request: Request) -> Response:
    """1KB response."""
    return Response(BODY_1K, media_type="application/octet-stream")


async def body_10k(request: Request) -> Response:
    """10KB response."""
    return Response(BODY_10K, media_type="application/octet-stream")


async def body_100k(request: Request) -> Response:
    """100KB response."""
    return Response(BODY_100K, media_type="application/octet-stream")


async def echo(request: Request) -> Response:
    """Echo POST body."""
    body = await request.body()
    return Response(body, media_type="application/octet-stream")


async def io_10ms(request: Request) -> JSONResponse:
    """Simulate 10ms I/O."""
    await asyncio.sleep(0.01)
    return JSONResponse({"waited": 10})


async def io_100ms(request: Request) -> JSONResponse:
    """Simulate 100ms I/O."""
    await asyncio.sleep(0.1)
    return JSONResponse({"waited": 100})


async def file_response(request: Request) -> Response:
    """Static file response."""
    return Response(FILE_CONTENT, media_type="application/octet-stream")


async def json_response(request: Request) -> JSONResponse:
    """JSON serialization test."""
    return JSONResponse(JSON_DATA)


routes = [
    Route("/health", health),
    Route("/hello", hello),
    Route("/b10", body_10b),
    Route("/b1k", body_1k),
    Route("/b10k", body_10k),
    Route("/b100k", body_100k),
    Route("/echo", echo, methods=["POST"]),
    Route("/io10", io_10ms),
    Route("/io100", io_100ms),
    Route("/file", file_response),
    Route("/json", json_response),
]

app = Starlette(routes=routes)
