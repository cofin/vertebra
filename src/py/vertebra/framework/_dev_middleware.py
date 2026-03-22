import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Protocol

from vertebra.framework._type_bridge import TypeBridge

logger = logging.getLogger("vertebra.dev")


class ASGIApp(Protocol):
    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None: ...


# Module-level client tracking for WS broadcast.
_ws_clients: set["asyncio.Queue[dict[str, Any]]"] = set()


def broadcast_hmr_event(event: dict[str, Any]) -> None:
    """Push an HMR event to all connected WebSocket clients.

    Safe to call from any context — drops events for clients whose
    queues are full (non-blocking).
    """
    for queue in _ws_clients.copy():
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(event)


class DevMiddleware:
    """ASGI middleware for developer features.

    Handles:
    1. Serving generated types via /__vertebra/ endpoints.
    2. WebSocket for HMR notifications with broadcast.
    3. HTML injection of the HMR client script.
    """

    def __init__(self, app: ASGIApp, type_bridge: TypeBridge):
        self.app = app
        self.type_bridge = type_bridge
        # Wire type_bridge invalidation to HMR broadcast
        self._original_invalidate = type_bridge.invalidate
        type_bridge.invalidate = self._invalidate_and_broadcast  # type: ignore[method-assign]

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

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            path = scope["path"]
            if path.startswith("/__vertebra/"):
                await self.handle_dev_request(scope, receive, send)
                return

            # For regular requests, wrap send to inject HMR script into HTML
            async def wrapped_send(message: dict[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    # Check if it's an HTML response
                    headers = dict(message.get("headers", []))
                    content_type = headers.get(b"content-type", b"").decode()
                    scope["__vtb_is_html"] = "text/html" in content_type
                    await send(message)
                elif message["type"] == "http.response.body" and scope.get("__vtb_is_html"):
                    body = message.get("body", b"")
                    if b"</body>" in body:
                        # Simple injection before </body>
                        script = b'<script type="module" src="/__vertebra/client/hmr.js"></script>'
                        injected = body.replace(b"</body>", script + b"</body>")
                        message["body"] = injected
                    await send(message)
                else:
                    await send(message)

            await self.app(scope, receive, wrapped_send)

        elif scope["type"] == "websocket" and scope["path"] == "/__vertebra/ws":
            await self.handle_ws(scope, receive, send)
        else:
            await self.app(scope, receive, send)

    async def handle_dev_request(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
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
            # Serve the pre-built HMR client bundle from the Python package.
            # tsup syncs dist/hmr/index.js → bundle/hmr.js
            import pkgutil

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

    async def handle_ws(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """WebSocket handler for HMR event broadcast.

        Each connected client gets an asyncio.Queue. A writer task reads
        from the queue and sends JSON messages. The reader task handles
        incoming messages (ping/pong) and disconnect cleanup.
        """
        await send({"type": "websocket.accept"})

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=64)
        _ws_clients.add(queue)

        # Send initial connected message
        await send(
            {
                "type": "websocket.send",
                "text": json.dumps({"type": "connected", "timestamp": time.time()}),
            }
        )

        # Writer: drains the broadcast queue and sends to this client
        async def writer() -> None:
            try:
                while True:
                    event = await queue.get()
                    await send(
                        {
                            "type": "websocket.send",
                            "text": json.dumps(event),
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
                    # Handle ping from client
                    text = message.get("text", "")
                    if text:
                        try:
                            data = json.loads(text)
                            if data.get("type") == "ping":
                                await send(
                                    {
                                        "type": "websocket.send",
                                        "text": json.dumps({"type": "pong", "timestamp": time.time()}),
                                    }
                                )
                        except (json.JSONDecodeError, TypeError):
                            pass
        finally:
            writer_task.cancel()
            _ws_clients.discard(queue)
