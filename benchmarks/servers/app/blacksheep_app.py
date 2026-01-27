"""BlackSheep benchmark application.

BlackSheep app for framework overhead testing.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from blacksheep import Application, Request, Response, get, json, post

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

app = Application(show_error_details=False)


@get("/health")
async def health() -> Response:
    """Health check endpoint."""
    return json({"status": "ok"})


@get("/hello")
async def hello() -> Response:
    """Minimal JSON response."""
    return json({"hello": "world"})


@get("/b10")
async def body_10b() -> Response:
    """10-byte response."""
    return Response(200, content=BODY_10B).with_content_type(b"application/octet-stream")


@get("/b1k")
async def body_1k() -> Response:
    """1KB response."""
    return Response(200, content=BODY_1K).with_content_type(b"application/octet-stream")


@get("/b10k")
async def body_10k() -> Response:
    """10KB response."""
    return Response(200, content=BODY_10K).with_content_type(b"application/octet-stream")


@get("/b100k")
async def body_100k() -> Response:
    """100KB response."""
    return Response(200, content=BODY_100K).with_content_type(b"application/octet-stream")


@post("/echo")
async def echo(request: Request) -> Response:
    """Echo request body."""
    body = await request.read()
    return Response(200, content=body).with_content_type(b"application/octet-stream")


@get("/io10")
async def io_10ms() -> Response:
    """10ms simulated I/O."""
    await asyncio.sleep(0.01)
    return json({"waited_ms": 10})


@get("/io100")
async def io_100ms() -> Response:
    """100ms simulated I/O."""
    await asyncio.sleep(0.1)
    return json({"waited_ms": 100})


@get("/file")
async def file_response() -> Response:
    """50KB file response."""
    return Response(200, content=FILE_CONTENT).with_content_type(b"application/octet-stream")


@get("/json")
async def json_response() -> Response:
    """Complex JSON response."""
    return json(JSON_DATA)
