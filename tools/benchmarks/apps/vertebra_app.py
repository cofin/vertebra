"""Vertebra App benchmark application.

Provides three exports:
    native_app - App with native handlers (batch dispatch)
    asgi_app   - App mounting the pure ASGI app under "/"
    wsgi_app   - App mounting the pure WSGI app under "/"
"""

from __future__ import annotations

import asyncio

from tools.benchmarks.apps.asgi import (
    BODY_1K,
    BODY_10K,
    BODY_100K,
    HELLO_BODY,
    JSON_RESPONSE,
    PLAINTEXT_BODY,
)
from tools.benchmarks.apps.asgi import (
    app as raw_asgi_app,
)
from tools.benchmarks.apps.wsgi import app as raw_wsgi_app
from vertebra.framework._app import App

native_app = App()


@native_app.get("/hello")
def hello() -> bytes:
    return HELLO_BODY


@native_app.get("/plaintext")
def plaintext() -> bytes:
    return PLAINTEXT_BODY


@native_app.get("/b1k")
def b1k() -> bytes:
    return BODY_1K


@native_app.get("/b10k")
def b10k() -> bytes:
    return BODY_10K


@native_app.get("/b100k")
def b100k() -> bytes:
    return BODY_100K


@native_app.get("/json")
def json_response() -> bytes:
    return JSON_RESPONSE


@native_app.get("/health")
def health() -> bytes:
    return b"ok"


@native_app.post("/echo")
def echo(body: bytes) -> bytes:
    return body


@native_app.get("/io10")
async def io10() -> bytes:
    await asyncio.sleep(0.010)
    return b"ok"


asgi_app = App()
asgi_app.mount("/", raw_asgi_app, protocol="asgi")

wsgi_app = App()
wsgi_app.mount("/", raw_wsgi_app, protocol="wsgi")
