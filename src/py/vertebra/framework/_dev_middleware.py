"""ASGI middleware for developer features: generated types, HMR WebSocket, and script injection."""

import asyncio
import contextlib
import logging
import pkgutil
import time
from typing import Any, Protocol, cast

from vertebra.framework._type_bridge import TypeBridge
from vertebra.utils.serializers._json import decode_json, encode_json

__all__ = ("ASGIApp", "DevMiddleware", "broadcast_hmr_event")

logger = logging.getLogger("vertebra.dev")

_ws_clients: set[asyncio.Queue[dict[str, Any]]] = set()


class ASGIApp(Protocol):
    """Protocol for an ASGI 3.0 callable."""

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None: ...


def broadcast_hmr_event(event: dict[str, Any]) -> None:
    """Push an HMR event to all connected WebSocket clients without blocking."""
    for queue in _ws_clients.copy():
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(event)


class DevMiddleware:
    """ASGI middleware for developer features.

    Serves generated types via ``/__vertebra/`` endpoints, provides a WebSocket
    channel for HMR notifications, and injects the HMR client script into HTML
    responses.
    """

    def __init__(self, app: ASGIApp, type_bridge: TypeBridge) -> None:
        self.app = app
        self.type_bridge = type_bridge
        self._original_invalidate = type_bridge.invalidate
        cast("Any", type_bridge).invalidate = self._invalidate_and_broadcast

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            path = scope["path"]
            if path.startswith("/__vertebra/"):
                await self.handle_dev_request(scope, receive, send)
                return

            async def wrapped_send(message: dict[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    headers = dict(message.get("headers", []))
                    content_type = headers.get(b"content-type", b"").decode()
                    scope["__vtb_is_html"] = "text/html" in content_type
                    await send(message)
                elif message["type"] == "http.response.body" and scope.get("__vtb_is_html"):
                    body = message.get("body", b"")
                    if b"</body>" in body:
                        script = b'<script type="module" src="/__vertebra/client/hmr.js"></script>'
                        message["body"] = body.replace(b"</body>", script + b"</body>")
                    await send(message)
                else:
                    await send(message)

            await self.app(scope, receive, wrapped_send)
        elif scope["type"] == "websocket" and scope["path"] == "/__vertebra/ws":
            await self.handle_ws(scope, receive, send)
        else:
            await self.app(scope, receive, send)

    async def handle_dev_request(self, scope: dict[str, Any], _receive: Any, send: Any) -> None:
        """Serve generated type declarations, manifests, and the HMR client bundle."""
        path = scope["path"]
        assets = self.type_bridge.get_assets()

        content = ""
        content_type = "text/plain"

        if path == "/__vertebra/types.d.ts":
            content = assets.ts_declarations
            content_type = "application/typescript"
        elif path == "/__vertebra/types/version":
            content = assets.version
        elif path == "/__vertebra/schemas.zod.ts":
            content = assets.zod_schemas
            content_type = "application/typescript"
        elif path == "/__vertebra/routes.ts":
            content = assets.routes
            content_type = "application/typescript"
        elif path == "/__vertebra/manifest.json":
            content = assets.manifest
            content_type = "application/json"
        elif path == "/__vertebra/client/hmr.js":
            js_data = None
            for name in ("server/_js/bundle/hmr.js", "server/_js/bundle/index.js"):
                try:
                    js_data = pkgutil.get_data("vertebra", name)
                    if js_data:
                        break
                except Exception:
                    pass
            if js_data:
                await self.send_response(send, js_data, "application/javascript")
                return
            content = "/* HMR client not found */"
            content_type = "application/javascript"
        else:
            await self.send_response(send, b"Not Found", "text/plain", status=404)
            return

        await self.send_response(send, content.encode(), content_type)

    async def send_response(self, send: Any, body: bytes, content_type: str, status: int = 200) -> None:
        """Send an HTTP response with no-cache headers."""
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", content_type.encode()),
                    (b"content-length", str(len(body)).encode()),
                    (b"cache-control", b"no-cache"),
                ],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": body,
            }
        )

    async def handle_ws(self, _scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Manage a WebSocket connection for HMR event broadcast."""
        await send({"type": "websocket.accept"})

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
        _ws_clients.add(queue)

        await send(
            {
                "type": "websocket.send",
                "text": encode_json({"type": "connected", "timestamp": time.time()}),
            }
        )

        async def writer() -> None:
            try:
                while True:
                    event = await queue.get()
                    await send(
                        {
                            "type": "websocket.send",
                            "text": encode_json(event),
                        }
                    )
            except Exception:
                pass

        writer_task = asyncio.create_task(writer())
        try:
            while True:
                message = await receive()
                if message["type"] == "websocket.disconnect":
                    break
                if message["type"] == "websocket.receive":
                    text = message.get("text", "")
                    if text:
                        try:
                            data = decode_json(text)
                            if isinstance(data, dict) and data.get("type") == "ping":
                                await send(
                                    {
                                        "type": "websocket.send",
                                        "text": encode_json({"type": "pong", "timestamp": time.time()}),
                                    }
                                )
                        except Exception:
                            pass
        finally:
            writer_task.cancel()
            _ws_clients.discard(queue)

    def _invalidate_and_broadcast(self) -> None:
        """Clear type cache and notify all HMR clients."""
        self._original_invalidate()
        assets = self.type_bridge.get_assets()
        broadcast_hmr_event(
            {
                "type": "types-update",
                "version": assets.version,
                "timestamp": time.time(),
            }
        )
