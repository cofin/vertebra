"""Spikard benchmark application.

Mirrors the ASGI benchmark endpoints using Spikard's native API.
"""

from __future__ import annotations

import asyncio
import os

from spikard import Response, ServerConfig, Spikard

app = Spikard(
    config=ServerConfig(
        host="127.0.0.1",
        port=8005,
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


@app.get("/hello")
async def hello() -> dict:
    return {"hello": "world"}


@app.get("/plaintext")
async def plaintext() -> Response:
    return Response(content=PLAINTEXT_BODY, headers={"content-type": "text/plain"})


@app.get("/b1k")
async def b1k() -> Response:
    return Response(content=BODY_1K, headers={"content-type": "text/plain"})


@app.get("/b10k")
async def b10k() -> Response:
    return Response(content=BODY_10K, headers={"content-type": "text/plain"})


@app.get("/b100k")
async def b100k() -> Response:
    return Response(content=BODY_100K, headers={"content-type": "text/plain"})


@app.get("/json")
async def json_endpoint() -> dict:
    return {"users": JSON_USERS}


@app.post("/echo")
async def echo(request: bytes) -> Response:
    return Response(content=request, headers={"content-type": "text/plain"})


@app.get("/io10")
async def io10() -> Response:
    await asyncio.sleep(0.01)
    return Response(content="ok", headers={"content-type": "text/plain"})


@app.get("/health")
async def health() -> Response:
    return Response(content="ok", headers={"content-type": "text/plain"})


if __name__ == "__main__":
    host = os.environ.get("SPIKARD_HOST", "127.0.0.1")
    port = int(os.environ.get("SPIKARD_PORT", "8005"))
    app.run(host=host, port=port)
