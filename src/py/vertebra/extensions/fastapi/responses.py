"""High-throughput MsgspecJSONResponse for FastAPI applications."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from vertebra.utils.serializers import encode_json_bytes

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

__all__ = ("MsgspecJSONResponse",)


class _RawHeadersView:
    """Lightweight MutableHeaders-compatible view over ASGI raw_headers tuples."""

    __slots__ = ("raw",)

    def __init__(self, raw: list[tuple[bytes, bytes]]) -> None:
        self.raw = raw

    def get(self, key: str, default: str | None = None) -> str | None:
        """Return the decoded header value for ``key`` when present."""
        target = key.lower().encode("latin-1")
        for raw_key, raw_val in reversed(self.raw):
            if raw_key.lower() == target:
                return raw_val.decode("latin-1")
        return default

    def items(self) -> list[tuple[str, str]]:
        """Return decoded ``(name, value)`` header pairs."""
        return [(k.decode("latin-1"), v.decode("latin-1")) for k, v in self.raw]

    def setdefault(self, key: str, value: str) -> str:
        """Set ``key`` to ``value`` when absent and return the resulting header value."""
        existing = self.get(key)
        if existing is not None:
            return existing
        self.raw.append((key.lower().encode("latin-1"), value.encode("latin-1")))
        return value

    def __getitem__(self, key: str) -> str:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: str) -> None:
        target = key.lower().encode("latin-1")
        encoded_val = value.encode("latin-1")
        self.raw[:] = [(k, v) for k, v in self.raw if k.lower() != target]
        self.raw.append((target, encoded_val))

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self.get(key) is not None

    def __iter__(self) -> Iterator[str]:
        seen: set[str] = set()
        for raw_key, _ in self.raw:
            name = raw_key.decode("latin-1").lower()
            if name not in seen:
                seen.add(name)
                yield name

    def __len__(self) -> int:
        return len(self.raw)


class MsgspecJSONResponse:
    """FastAPI-compatible JSON response class backed by Vertebra's msgspec serializer."""

    media_type = "application/json"
    charset = "utf-8"

    __slots__ = ("background", "body", "headers", "raw_headers", "status_code")

    def __init__(
        self,
        content: Any = None,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
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
                raw_hdrs.append((str(k).lower().encode("latin-1"), str(v).encode("latin-1")))
        self.raw_headers = raw_hdrs
        self.headers = _RawHeadersView(raw_hdrs)

    def render(self, content: Any) -> bytes:
        """Encode ``content`` directly to UTF-8 JSON bytes via msgspec."""
        if isinstance(content, bytes):
            return content
        if isinstance(content, memoryview):
            return bytes(content)
        return encode_json_bytes(content)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """ASGI 3.0 callable interface for fallback execution."""
        _ = (scope, receive)
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
            if inspect.isawaitable(res):
                await res
