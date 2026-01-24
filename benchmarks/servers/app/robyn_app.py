"""Robyn benchmark application.

Robyn app for integrated runtime testing (native interface, not ASGI).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from robyn import Request, Robyn

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

app = Robyn(__file__)


@app.get("/health")
async def health(request: Request):
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/hello")
async def hello(request: Request):
    """Minimal JSON response."""
    return {"hello": "world"}


@app.get("/b10")
async def body_10b(request: Request):
    """10-byte response."""
    return BODY_10B


@app.get("/b1k")
async def body_1k(request: Request):
    """1KB response."""
    return BODY_1K


@app.get("/b10k")
async def body_10k(request: Request):
    """10KB response."""
    return BODY_10K


@app.get("/b100k")
async def body_100k(request: Request):
    """100KB response."""
    return BODY_100K


@app.post("/echo")
async def echo(request: Request):
    """Echo POST body."""
    return request.body


@app.get("/io10")
async def io_10ms(request: Request):
    """Simulate 10ms I/O."""
    await asyncio.sleep(0.01)
    return {"waited": 10}


@app.get("/io100")
async def io_100ms(request: Request):
    """Simulate 100ms I/O."""
    await asyncio.sleep(0.1)
    return {"waited": 100}


@app.get("/file")
async def file_response(request: Request):
    """Static file response."""
    return FILE_CONTENT


@app.get("/json")
async def json_response(request: Request):
    """JSON serialization test."""
    return JSON_DATA


if __name__ == "__main__":
    app.start(host="127.0.0.1", port=8010)
