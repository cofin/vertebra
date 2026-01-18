"""Type bridge HTTP endpoint and cache management."""

from __future__ import annotations

import gzip
import hashlib
import threading
import time
from dataclasses import dataclass
from typing import Iterable

from vertebra.procedures.registry import ProcedureRegistry
from vertebra.types.typescript import TypeScriptGenerator, format_typescript

DEFAULT_TYPES_ROUTE = "/__vertebra/types.d.ts"
DEFAULT_COMPRESS_THRESHOLD = 10_000


@dataclass(frozen=True, slots=True)
class TypeBridgeSnapshot:
    content: str
    etag: str
    version: str
    generated_at: float


class TypeBridge:
    """Generate and cache TypeScript definitions for registered procedures."""

    def __init__(
        self,
        registry: ProcedureRegistry | None = None,
        *,
        compress_threshold: int = DEFAULT_COMPRESS_THRESHOLD,
    ) -> None:
        self._registry = registry or ProcedureRegistry.instance()
        self._compress_threshold = compress_threshold
        self._lock = threading.Lock()
        self._snapshot: TypeBridgeSnapshot | None = None
        self._dirty = True

    def invalidate(self) -> None:
        with self._lock:
            self._dirty = True

    def generate(self) -> str:
        generator = TypeScriptGenerator()
        output = generator.generate(self._registry.list())
        return format_typescript(output)

    def get_snapshot(self) -> TypeBridgeSnapshot:
        with self._lock:
            if self._snapshot is None or self._dirty:
                content = self.generate()
                digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
                etag = f'"{digest}"'
                self._snapshot = TypeBridgeSnapshot(
                    content=content,
                    etag=etag,
                    version=digest,
                    generated_at=time.time(),
                )
                self._dirty = False
            return self._snapshot

    def build_response(
        self,
        *,
        accept_encoding: str | None = None,
        if_none_match: str | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        snapshot = self.get_snapshot()
        if if_none_match and if_none_match == snapshot.etag:
            headers = {
                "ETag": snapshot.etag,
                "Cache-Control": "no-cache",
                "X-TypeScript-Version": snapshot.version,
            }
            return 304, headers, b""

        body = snapshot.content.encode("utf-8")
        headers = {
            "Content-Type": "application/typescript; charset=utf-8",
            "Cache-Control": "no-cache",
            "ETag": snapshot.etag,
            "X-TypeScript-Version": snapshot.version,
        }

        if (
            accept_encoding
            and "gzip" in accept_encoding.lower()
            and len(body) > self._compress_threshold
        ):
            body = gzip.compress(body)
            headers["Content-Encoding"] = "gzip"

        return 200, headers, body


class TypeBridgeApp:
    """ASGI app serving the TypeScript type bridge endpoint."""

    def __init__(
        self,
        bridge: TypeBridge | None = None,
        *,
        route: str = DEFAULT_TYPES_ROUTE,
    ) -> None:
        self._bridge = bridge or TypeBridge()
        self._route = route

    async def __call__(self, scope: dict, receive, send) -> None:  # type: ignore[override]
        if scope.get("type") != "http":
            return
        if scope.get("path") != self._route:
            await send(
                {
                    "type": "http.response.start",
                    "status": 404,
                    "headers": [(b"content-type", b"text/plain; charset=utf-8")],
                }
            )
            await send({"type": "http.response.body", "body": b"Not Found"})
            return

        headers = {k.decode(): v.decode() for k, v in scope.get("headers", [])}
        status, response_headers, body = self._bridge.build_response(
            accept_encoding=headers.get("accept-encoding"),
            if_none_match=headers.get("if-none-match"),
        )
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(k.encode(), v.encode()) for k, v in response_headers.items()],
            }
        )
        await send({"type": "http.response.body", "body": body})


__all__ = ["TypeBridge", "TypeBridgeApp", "TypeBridgeSnapshot"]
