"""FastAPI benchmark application.

FastAPI app for framework overhead testing.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response

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

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


@app.get("/hello")
async def hello() -> dict[str, str]:
    """Minimal JSON response."""
    return {"hello": "world"}


@app.get("/b10")
async def body_10b() -> Response:
    """10-byte response."""
    return Response(BODY_10B, media_type="application/octet-stream")


@app.get("/b1k")
async def body_1k() -> Response:
    """1KB response."""
    return Response(BODY_1K, media_type="application/octet-stream")


@app.get("/b10k")
async def body_10k() -> Response:
    """10KB response."""
    return Response(BODY_10K, media_type="application/octet-stream")


@app.get("/b100k")
async def body_100k() -> Response:
    """100KB response."""
    return Response(BODY_100K, media_type="application/octet-stream")


@app.post("/echo")
async def echo(body: bytes) -> Response:
    """Echo POST body."""
    return Response(body, media_type="application/octet-stream")


@app.get("/io10")
async def io_10ms() -> dict[str, int]:
    """Simulate 10ms I/O."""
    await asyncio.sleep(0.01)
    return {"waited": 10}


@app.get("/io100")
async def io_100ms() -> dict[str, int]:
    """Simulate 100ms I/O."""
    await asyncio.sleep(0.1)
    return {"waited": 100}


@app.get("/file")
async def file_response() -> Response:
    """Static file response."""
    return Response(FILE_CONTENT, media_type="application/octet-stream")


@app.get("/json")
async def json_response() -> JSONResponse:
    """JSON serialization test."""
    return JSONResponse(JSON_DATA)
