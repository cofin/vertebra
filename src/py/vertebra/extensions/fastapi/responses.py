"""High-throughput MsgspecJSONResponse for FastAPI applications."""

from __future__ import annotations

from typing import Any

from vertebra.utils.serializers import encode_json_bytes

__all__ = ("MsgspecJSONResponse",)


class MsgspecJSONResponse:
    """FastAPI-compatible JSON response class backed by Vertebra's msgspec serializer."""

    media_type = "application/json"
    charset = "utf-8"

    __slots__ = ("background", "body", "raw_headers", "status_code")

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None,
        media_type: str | None = None,
        background: Any = None,
    ) -> None:
        self.status_code = int(status_code)
        self.background = background
        resolved_media = media_type or self.media_type
        self.body = self.render(content)
        raw_hdrs: list[tuple[bytes, bytes]] = [
            (b"content-type", resolved_media.encode("latin-1")),
            (b"content-length", str(len(self.body)).encode("ascii")),
        ]
        if headers:
            for k, v in headers.items():
                raw_hdrs.append((k.lower().encode("latin-1"), str(v).encode("latin-1")))
        self.raw_headers = raw_hdrs

    def render(self, content: Any) -> bytes:
        """Encode ``content`` directly to UTF-8 JSON bytes via msgspec."""
        if isinstance(content, bytes):
            return content
        if isinstance(content, memoryview):
            return bytes(content)
        return encode_json_bytes(content)

    async def __call__(self, _scope: Any, _receive: Any, send: Any) -> None:
        """ASGI 3.0 callable interface for fallback execution."""
        await send(
            {
                "type": "http.response.start",
                "status": self.status_code,
                "headers": self.raw_headers,
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": self.body,
                "more_body": False,
            }
        )
        if self.background is not None and callable(self.background):
            res = self.background()
            if hasattr(res, "__await__"):
                await res
