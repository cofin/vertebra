"""Spikard benchmark application.

Spikard app for integrated runtime testing (Rust Axum/Tower-HTTP runtime).

Based on: https://github.com/Goldziher/spikard
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from spikard import Request, Response, Spikard, get, post

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

app = Spikard()


@get("/health")
async def health(request: Request) -> dict:
    """Health check endpoint."""
    return {"status": "ok"}


@get("/hello")
async def hello(request: Request) -> dict:
    """Minimal JSON response."""
    return {"hello": "world"}


@get("/b10")
async def body_10b(request: Request) -> Response:
    """10-byte response."""
    return Response(content=BODY_10B, media_type="application/octet-stream")


@get("/b1k")
async def body_1k(request: Request) -> Response:
    """1KB response."""
    return Response(content=BODY_1K, media_type="application/octet-stream")


@get("/b10k")
async def body_10k(request: Request) -> Response:
    """10KB response."""
    return Response(content=BODY_10K, media_type="application/octet-stream")


@get("/b100k")
async def body_100k(request: Request) -> Response:
    """100KB response."""
    return Response(content=BODY_100K, media_type="application/octet-stream")


@post("/echo")
async def echo(request: Request) -> Response:
    """Echo POST body."""
    body = await request.body()
    return Response(content=body, media_type="application/octet-stream")


@get("/io10")
async def io_10ms(request: Request) -> dict:
    """Simulate 10ms I/O."""
    await asyncio.sleep(0.01)
    return {"waited": 10}


@get("/io100")
async def io_100ms(request: Request) -> dict:
    """Simulate 100ms I/O."""
    await asyncio.sleep(0.1)
    return {"waited": 100}


@get("/file")
async def file_response(request: Request) -> Response:
    """Static file response."""
    return Response(content=FILE_CONTENT, media_type="application/octet-stream")


@get("/json")
async def json_response(request: Request) -> dict:
    """JSON serialization test."""
    return JSON_DATA


# Register routes with app
app.register([health, hello, body_10b, body_1k, body_10k, body_100k, echo, io_10ms, io_100ms, file_response, json_response])


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8012)
