"""ASGI adapter for Vertebra native applications."""

from __future__ import annotations

import inspect
import json
import logging
from typing import TYPE_CHECKING, Any

from vertebra.web.routing import extract_path_params

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from vertebra.core.lifecycle import Vertex

_logger = logging.getLogger(__name__)


class VertebraAdapter:
    """Adapts a Vertex native app to the ASGI protocol.

    This allows running Vertebra apps with standard ASGI servers (Uvicorn, etc.)
    and serves as a compatibility layer.
    """

    def __init__(self, app: Vertex) -> None:
        self.app = app

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        if scope["type"] == "lifespan":
            await self._handle_lifespan(scope, receive, send)
            return

        if scope["type"] != "http":
            await self._send_response(send, 404, b"Not Found", headers=[(b"content-type", b"text/plain")])
            return

        await self._handle_http(scope, receive, send)

    async def _handle_lifespan(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await self.app.startup()
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.app.shutdown()
                await send({"type": "lifespan.shutdown.complete"})
                return

    async def _handle_http(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        path = scope["path"]
        method = scope["method"]

        # Naive routing implementation for compatibility
        # For high performance, use AsgiWorker which uses the Rust router.
        for route in self.app.routes:
            if route.method != method:
                continue
            try:
                # TODO: Optimize this by compiling routes into a tree or using matchit-python
                params = extract_path_params(route.path, path)
            except ValueError:
                continue

            # Found match
            handler = route.handler

            # Simple dependency injection resolution (subset of full compiler)
            # This is a naive implementation; real implementation should use ProviderCompiler
            try:
                # Assuming handler takes params as kwargs for now
                # TODO: Integrate with self.app.compiler for full DI
                if inspect.iscoroutinefunction(handler):
                    result = await handler(**params)
                else:
                    result = handler(**params)
            except (TypeError, ValueError, KeyError):
                _logger.exception("Handler error")
                await self._send_response(send, 500, b"Internal Server Error")
                return
            except Exception:
                _logger.exception("Unexpected handler error")
                await self._send_response(send, 500, b"Internal Server Error")
                return
            else:
                await self._send_result(send, result)
                return

        await self._send_response(send, 404, b"Not Found")

    async def _send_result(self, send: Callable[[dict[str, Any]], Awaitable[None]], result: Any) -> None:
        if isinstance(result, (str, bytes)):
            body = result.encode("utf-8") if isinstance(result, str) else result
            await self._send_response(send, 200, body)
        elif isinstance(result, dict):
            body = json.dumps(result).encode("utf-8")
            await self._send_response(send, 200, body, headers=[(b"content-type", b"application/json")])
        else:
            # Fallback
            body = str(result).encode("utf-8")
            await self._send_response(send, 200, body)

    async def _send_response(
        self,
        send: Callable[[dict[str, Any]], Awaitable[None]],
        status: int,
        body: bytes,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> None:
        if headers is None:
            headers = [(b"content-type", b"text/plain")]

        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})
