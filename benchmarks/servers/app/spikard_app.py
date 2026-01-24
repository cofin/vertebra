"""Spikard benchmark application.

Spikard app for integrated runtime testing (Rust Tower-HTTP runtime).

Based on: https://github.com/Goldziher/spikard
"""

from __future__ import annotations

import asyncio
from pathlib import Path

try:
    from spikard import Handler, Request, Response, run_server
except ImportError:
    import sys

    print("Spikard not installed. Install with: cargo install spikard-cli && spikard init", file=sys.stderr)
    sys.exit(1)

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


@Handler("/health")
async def health(request: Request) -> Response:
    """Health check endpoint."""
    return Response({"status": "ok"})


@Handler("/hello")
async def hello(request: Request) -> Response:
    """Minimal JSON response."""
    return Response({"hello": "world"})


@Handler("/b10")
async def body_10b(request: Request) -> Response:
    """10-byte response."""
    return Response(BODY_10B, content_type="application/octet-stream")


@Handler("/b1k")
async def body_1k(request: Request) -> Response:
    """1KB response."""
    return Response(BODY_1K, content_type="application/octet-stream")


@Handler("/b10k")
async def body_10k(request: Request) -> Response:
    """10KB response."""
    return Response(BODY_10K, content_type="application/octet-stream")


@Handler("/b100k")
async def body_100k(request: Request) -> Response:
    """100KB response."""
    return Response(BODY_100K, content_type="application/octet-stream")


@Handler("/echo", methods=["POST"])
async def echo(request: Request) -> Response:
    """Echo POST body."""
    return Response(request.body, content_type="application/octet-stream")


@Handler("/io10")
async def io_10ms(request: Request) -> Response:
    """Simulate 10ms I/O."""
    await asyncio.sleep(0.01)
    return Response({"waited": 10})


@Handler("/io100")
async def io_100ms(request: Request) -> Response:
    """Simulate 100ms I/O."""
    await asyncio.sleep(0.1)
    return Response({"waited": 100})


@Handler("/file")
async def file_response(request: Request) -> Response:
    """Static file response."""
    return Response(FILE_CONTENT, content_type="application/octet-stream")


@Handler("/json")
async def json_response(request: Request) -> Response:
    """JSON serialization test."""
    return Response(JSON_DATA)


if __name__ == "__main__":
    # Run the Spikard Tower-HTTP server
    run_server(host="127.0.0.1", port=8012)
