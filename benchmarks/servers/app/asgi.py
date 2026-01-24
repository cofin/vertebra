"""Standardized ASGI test application for benchmarks.

This pure ASGI application provides baseline performance measurements.
All responses are pre-computed to minimize application-level overhead,
isolating the server's HTTP handling performance.

Endpoints:
    /hello    - 12-byte JSON response {"hello":"world"}
    /b10      - 10-byte plaintext response
    /b1k      - 1KB plaintext response
    /b10k     - 10KB plaintext response
    /b100k    - 100KB plaintext response
    /echo     - Echo POST body back
    /io10     - 10ms async sleep (I/O simulation)
    /io100    - 100ms async sleep (slow query simulation)
    /file     - 50KB static file response
    /json     - 1KB JSON response with nested structure
    /health   - Health check endpoint
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

# ASGI type aliases
ASGIScope = dict[str, Any]
ASGIReceive = Callable[[], Coroutine[Any, Any, dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]

# =============================================================================
# Pre-computed responses for minimal overhead
# =============================================================================

# Hello world JSON (12 bytes)
HELLO_BODY = b'{"hello":"world"}'

# Plaintext bodies of various sizes
BODY_10 = b"0" * 10
BODY_1K = b"0" * 1024
BODY_10K = b"0" * 10240
BODY_100K = b"0" * 102400

# JSON structure with users (~1KB)
JSON_RESPONSE = (
    b'{"users":['
    + b",".join([
        b'{"id":%d,"name":"User %d","email":"user%d@example.com","active":true}' % (i, i, i) for i in range(10)
    ])
    + b"]}"
)

# Load static file for /file endpoint
_ASSETS_DIR = Path(__file__).parent / "assets"
_FILE_PATH = _ASSETS_DIR / "image.jpg"

FILE_CONTENT = _FILE_PATH.read_bytes() if _FILE_PATH.exists() else b"0" * 51200


# =============================================================================
# Response helpers
# =============================================================================


async def send_response(send: ASGISend, body: bytes, content_type: str, status: int = 200) -> None:
    """Send a complete HTTP response."""
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [[b"content-type", content_type.encode()], [b"content-length", str(len(body)).encode()]],
    })
    await send({"type": "http.response.body", "body": body})


async def receive_body(receive: ASGIReceive) -> bytes:
    """Receive complete request body."""
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body", False):
            break
    return body


async def send_not_found(send: ASGISend) -> None:
    """Send 404 response."""
    await send({"type": "http.response.start", "status": 404, "headers": [[b"content-type", b"text/plain"]]})
    await send({"type": "http.response.body", "body": b"Not Found"})


# =============================================================================
# Route Handlers
# =============================================================================


async def handle_echo(receive: ASGIReceive, send: ASGISend) -> None:
    """Handle echo endpoint."""
    body = await receive_body(receive)
    await send_response(send, body, "text/plain")


async def handle_io10(_receive: ASGIReceive, send: ASGISend) -> None:
    """Handle 10ms I/O simulation."""
    await asyncio.sleep(0.010)
    await send_response(send, b"ok", "text/plain")


async def handle_io100(_receive: ASGIReceive, send: ASGISend) -> None:
    """Handle 100ms I/O simulation."""
    await asyncio.sleep(0.100)
    await send_response(send, b"ok", "text/plain")


# Static response routes: (body, content_type)
STATIC_ROUTES: dict[str, tuple[bytes, str]] = {
    "/hello": (HELLO_BODY, "application/json"),
    "/b10": (BODY_10, "text/plain"),
    "/b1k": (BODY_1K, "text/plain"),
    "/b10k": (BODY_10K, "text/plain"),
    "/b100k": (BODY_100K, "text/plain"),
    "/file": (FILE_CONTENT, "image/jpeg"),
    "/json": (JSON_RESPONSE, "application/json"),
    "/health": (b"ok", "text/plain"),
}

# Dynamic routes requiring special handling
DYNAMIC_ROUTES: dict[str, Callable[[ASGIReceive, ASGISend], Coroutine[Any, Any, None]]] = {
    "/echo": handle_echo,
    "/io10": handle_io10,
    "/io100": handle_io100,
}


# =============================================================================
# ASGI Application
# =============================================================================


async def app(scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
    """Main ASGI application."""
    if scope["type"] != "http":
        return

    path = scope["path"]

    # Check static routes first (most common)
    if path in STATIC_ROUTES:
        body, content_type = STATIC_ROUTES[path]
        await send_response(send, body, content_type)
        return

    # Check dynamic routes
    if path in DYNAMIC_ROUTES:
        await DYNAMIC_ROUTES[path](receive, send)
        return

    # Not found
    await send_not_found(send)
