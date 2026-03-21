from typing import Any, Protocol

from vertebra.framework._type_bridge import TypeBridge


class ASGIApp(Protocol):
    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None: ...


class DevMiddleware:
    """ASGI middleware for developer features.

    Handles:
    1. Serving generated types via /__vertebra/ endpoints.
    2. WebSocket for HMR notifications.
    3. HTML injection of the HMR client script.
    """

    def __init__(self, app: ASGIApp, type_bridge: TypeBridge):
        self.app = app
        self.type_bridge = type_bridge

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
            # Serve the pre-built HMR client bundle from the Python package
            import pkgutil

            try:
                js_data = pkgutil.get_data("vertebra", "server/_js/bundle/index.js")
                if js_data:
                    await self.send_response(send, js_data, "application/javascript")
                    return
            except Exception:
                pass
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
        # Basic WebSocket handler for HMR updates
        # In a real implementation, we'd keep track of connections
        # and send updates when type_bridge.invalidate() is called.
        await send({"type": "websocket.accept"})

        while True:
            message = await receive()
            if message["type"] == "websocket.disconnect":
                break
            if message["type"] == "websocket.receive":
                # Handle potential ping/pong or other messages
                pass
