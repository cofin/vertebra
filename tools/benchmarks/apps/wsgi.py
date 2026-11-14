"""Standardized WSGI test application for benchmarks.

Pure WSGI application with pre-computed responses, matching the same
endpoints as the ASGI benchmark app.
"""

from __future__ import annotations

import time
from typing import Any

from tools.benchmarks.apps.asgi import (
    BODY_1K,
    BODY_10K,
    BODY_100K,
    HELLO_BODY,
    JSON_RESPONSE,
    PLAINTEXT_BODY,
)

StartResponse = Any

_STATIC: dict[str, tuple[bytes, str]] = {
    "/hello": (HELLO_BODY, "application/json"),
    "/plaintext": (PLAINTEXT_BODY, "text/plain"),
    "/b1k": (BODY_1K, "text/plain"),
    "/b10k": (BODY_10K, "text/plain"),
    "/b100k": (BODY_100K, "text/plain"),
    "/json": (JSON_RESPONSE, "application/json"),
    "/health": (b"ok", "text/plain"),
}


def app(environ: dict[str, Any], start_response: StartResponse) -> list[bytes]:
    """WSGI application dispatching static and dynamic benchmark routes."""
    path = environ.get("PATH_INFO", "/")

    if path in _STATIC:
        body, ct = _STATIC[path]
        return _respond(start_response, body, ct)

    if path == "/echo":
        content_length = int(environ.get("CONTENT_LENGTH") or 0)
        body = environ["wsgi.input"].read(content_length) if content_length else b""
        return _respond(start_response, body, "text/plain")

    if path == "/io10":
        time.sleep(0.010)
        return _respond(start_response, b"ok", "text/plain")

    return _respond(start_response, b"Not Found", "text/plain", "404 Not Found")


def _respond(start_response: StartResponse, body: bytes, content_type: str, status: str = "200 OK") -> list[bytes]:
    headers = [("Content-Type", content_type), ("Content-Length", str(len(body)))]
    start_response(status, headers)
    return [body]
