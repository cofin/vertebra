"""RSGI v1.6 application adapter driving __rsgi_init__, __rsgi__, and __rsgi_del__."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
from typing import Any, Literal

Response: Any = None
RsgiHeaders: Any = None
RsgiHttpProtocol: Any = None
RsgiScope: Any = None
with contextlib.suppress(ImportError):
    from vertebra._vertebra import (
        Response,
        RsgiHeaders,
        RsgiHttpProtocol,
        RsgiScope,
    )

__all__ = (
    "InterfaceMode",
    "RsgiAdapter",
    "detect_interface",
    "is_rsgi_app",
)

log = logging.getLogger(__name__)

type InterfaceMode = Literal["auto", "asgi", "rsgi", "wsgi"]


def is_rsgi_app(app: Any) -> bool:
    """Return True if the application object exposes an RSGI __rsgi__ entrypoint."""
    return callable(getattr(app, "__rsgi__", None))


def detect_interface(app: Any, configured: InterfaceMode = "auto") -> Literal["asgi", "rsgi", "wsgi"]:
    """Resolve the gateway protocol interface for an application callable."""
    if configured != "auto":
        return configured
    if is_rsgi_app(app):
        return "rsgi"
    if inspect.iscoroutinefunction(app) or (callable(app) and inspect.iscoroutinefunction(type(app).__call__)):
        return "asgi"
    try:
        sig = inspect.signature(app)
        if len(sig.parameters) == 3:
            return "asgi"
        if len(sig.parameters) == 2:
            return "wsgi"
    except (TypeError, ValueError):
        pass
    return "asgi"


class RsgiAdapter:
    """Drives an RSGI v1.6 application through startup, request handling, and shutdown."""

    __slots__ = ("_app", "_client_addr", "_scheme", "_server_addr")

    def __init__(
        self,
        app: Any,
        *,
        server_addr: str = "127.0.0.1:8000",
        client_addr: str = "127.0.0.1:0",
        scheme: str = "http",
    ) -> None:
        self._app = app
        self._server_addr = server_addr
        self._client_addr = client_addr
        self._scheme = scheme

    def startup(self, loop: asyncio.AbstractEventLoop) -> None:
        """Invoke __rsgi_init__(loop) if defined on the application."""
        init_hook = getattr(self._app, "__rsgi_init__", None)
        if callable(init_hook):
            init_hook(loop)

    def shutdown(self, loop: asyncio.AbstractEventLoop) -> None:
        """Invoke __rsgi_del__(loop) if defined on the application."""
        del_hook = getattr(self._app, "__rsgi_del__", None)
        if callable(del_hook):
            del_hook(loop)

    async def run_single(self, request: Any) -> Response:
        """Execute __rsgi__(scope, protocol) for a native Request object."""
        req_id = int(getattr(request, "request_id", 0))
        raw_body = getattr(request, "body", None) or b""
        if hasattr(request, "rsgi_scope"):
            scope = request.rsgi_scope(self._scheme, self._server_addr, self._client_addr)
        else:
            hdr_pairs = list(request.headers.items()) if hasattr(request, "headers") else []
            scope = RsgiScope(
                proto="http",
                http_version=getattr(request, "http_version", "1.1"),
                server=self._server_addr,
                client=self._client_addr,
                scheme=self._scheme,
                method=request.method,
                path=request.path,
                query_string=getattr(request, "query", None) or "",
                headers=RsgiHeaders(hdr_pairs),
            )
        protocol = RsgiHttpProtocol(body=raw_body, request_id=req_id)
        entrypoint = getattr(self._app, "__rsgi__", self._app)
        try:
            result = entrypoint(scope, protocol)
            if inspect.isawaitable(result):
                await result
            return protocol.take_response()
        except Exception:
            log.exception("RSGI application raised unhandled exception for request %d", req_id)
            return Response(
                status=500,
                body=b"Internal Server Error",
                headers=[("content-type", "text/plain"), ("content-length", "21")],
                request_id=req_id,
            )

    async def handle_request(self, request: Any) -> Response:
        """Execute __rsgi__(scope, protocol) for a single native Request object."""
        return await self.run_single(request)

    async def handle_http(
        self,
        *,
        method: str = "GET",
        path: str = "/",
        query_string: str = "",
        headers: list[tuple[str, str]] | None = None,
        body: bytes = b"",
        http_version: str = "1.1",
        authority: str | None = None,
        request_id: int = 0,
    ) -> Response:
        """Execute __rsgi__(scope, protocol) from explicit HTTP components."""
        scope = RsgiScope(
            proto="http",
            http_version=http_version,
            server=self._server_addr,
            client=self._client_addr,
            scheme=self._scheme,
            method=method,
            path=path,
            query_string=query_string,
            headers=RsgiHeaders(headers or []),
            authority=authority,
        )
        protocol = RsgiHttpProtocol(body=body, request_id=request_id)
        entrypoint = getattr(self._app, "__rsgi__", self._app)
        result = entrypoint(scope, protocol)
        if inspect.isawaitable(result):
            await result
        return protocol.take_response()

    async def run_batch(self, requests: list[Any]) -> list[Response]:
        """Execute __rsgi__ concurrently across a batch of requests."""
        return list(await asyncio.gather(*(self.run_single(req) for req in requests)))
