"""Developer middleware and frontend type bridge.

Serves generated types, route manifests, HMR WebSocket channel, and script injection.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import pkgutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Protocol

import msgspec

from vertebra.utils.serializers import decode_json, encode_json

__all__ = (
    "ASGIApp",
    "DevMiddleware",
    "TypeAssets",
    "TypeBridge",
    "broadcast_hmr_event",
)

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


class TypeAssets(msgspec.Struct, frozen=True, gc=False):
    """Container for generated type assets."""

    version: str = ""
    ts_declarations: str = ""
    zod_schemas: str = ""
    routes: str = ""
    manifest: str = ""
    timestamp: float = msgspec.field(default_factory=time.time)


class TypeBridge:
    """Manage delivery of generated types, Zod schemas, and route manifests."""

    def __init__(
        self,
        codegen_dir: Path | str,
        on_invalidate: Callable[[TypeAssets], None] | None = None,
    ) -> None:
        self.codegen_dir = Path(codegen_dir)
        self._on_invalidate = on_invalidate
        self._cache: TypeAssets | None = None

    @property
    def on_invalidate(self) -> Callable[[TypeAssets], None] | None:
        """Return the invalidation callback."""
        return self._on_invalidate

    @on_invalidate.setter
    def on_invalidate(self, callback: Callable[[TypeAssets], None] | None) -> None:
        """Set the invalidation callback."""
        self._on_invalidate = callback

    @property
    def version(self) -> str:
        """Return the current assets version hash."""
        return self.get_assets().version

    def get_assets(self) -> TypeAssets:
        """Return the current type assets, rebuilding the cache if needed."""
        if self._cache is not None:
            return self._cache

        ts_path = self.codegen_dir / "types.d.ts"
        zod_path = self.codegen_dir / "schemas.zod.ts"
        routes_path = self.codegen_dir / "routes.ts"
        manifest_path = self.codegen_dir / "manifest.json"

        ts_content = ts_path.read_text() if ts_path.exists() else ""
        zod_content = zod_path.read_text() if zod_path.exists() else ""
        routes_content = routes_path.read_text() if routes_path.exists() else ""
        manifest_content = manifest_path.read_text() if manifest_path.exists() else "{}"

        combined = ts_content + zod_content + routes_content + manifest_content
        version = hashlib.sha256(combined.encode()).hexdigest()[:12]

        self._cache = TypeAssets(
            version=version,
            ts_declarations=ts_content,
            zod_schemas=zod_content,
            routes=routes_content,
            manifest=manifest_content,
        )
        return self._cache

    def invalidate(self) -> None:
        """Clear the asset cache to force a reload on next access and notify callback."""
        self._cache = None
        if self._on_invalidate is not None:
            self._on_invalidate(self.get_assets())


class DevMiddleware:
    """ASGI middleware for developer features.

    Serves generated types via ``/__vertebra/`` endpoints, provides a WebSocket
    channel for HMR notifications, and injects the HMR client script into HTML
    responses.
    """

    _ASSET_ROUTES: ClassVar[dict[str, tuple[str, str]]] = {
        "/__vertebra/types.d.ts": ("ts_declarations", "application/typescript"),
        "/__vertebra/types/version": ("version", "text/plain"),
        "/__vertebra/schemas.zod.ts": ("zod_schemas", "application/typescript"),
        "/__vertebra/routes.ts": ("routes", "application/typescript"),
        "/__vertebra/manifest.json": ("manifest", "application/json"),
    }

    def __init__(self, app: ASGIApp, type_bridge: TypeBridge) -> None:
        self.app = app
        self.type_bridge = type_bridge
        type_bridge.on_invalidate = self._handle_assets_invalidated

    def _handle_assets_invalidated(self, assets: TypeAssets) -> None:
        """Notify all HMR clients of updated type assets."""
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

        if path in self._ASSET_ROUTES:
            attr_name, content_type = self._ASSET_ROUTES[path]
            content = getattr(assets, attr_name)
            await self.send_response(send, content.encode("utf-8"), content_type)
            return

        if path == "/__vertebra/client/hmr.js":
            js_data = None
            for name in ("server/_js/bundle/hmr.js", "server/_js/bundle/index.js"):
                try:
                    js_data = pkgutil.get_data("vertebra", name)
                    if js_data:
                        break
                except Exception:
                    logger.debug("Failed loading HMR client resource %s", name, exc_info=True)
            if js_data:
                await self.send_response(send, js_data, "application/javascript")
                return
            content = "/* HMR client not found */"
            await self.send_response(send, content.encode("utf-8"), "application/javascript")
            return

        await self.send_response(send, b"Not Found", "text/plain", status=404)

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
                logger.debug("HMR WebSocket writer loop terminated", exc_info=True)

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
                            logger.debug("Failed parsing WebSocket ping message", exc_info=True)
        finally:
            writer_task.cancel()
            _ws_clients.discard(queue)
