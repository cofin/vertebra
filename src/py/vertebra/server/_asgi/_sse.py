"""Server-Sent Events (SSE) ASGI response helper and http.response.sse extension support."""

from collections.abc import AsyncIterable, Callable, Iterable
from typing import Any

import msgspec

__all__ = (
    "DEFAULT_SSE_HEADERS",
    "SseEvent",
    "SseResponse",
    "extract_last_event_id",
)

DEFAULT_SSE_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"content-type", b"text/event-stream; charset=utf-8"),
    (b"cache-control", b"no-cache, no-transform"),
    (b"connection", b"keep-alive"),
    (b"x-accel-buffering", b"no"),
)


def extract_last_event_id(scope: dict[str, Any]) -> str | None:
    """Extract the Last-Event-ID header value from an ASGI HTTP scope if present."""
    headers: Iterable[tuple[bytes, bytes]] = scope.get("headers", ())
    for raw_name, raw_value in headers:
        if raw_name.lower() == b"last-event-id":
            decoded = raw_value.decode("utf-8", errors="replace").strip()
            return _sanitize_field(decoded) if decoded else None
    return None


class SseEvent(msgspec.Struct, frozen=True, gc=False, kw_only=True, cache_hash=True):
    """Structured Server-Sent Events message frame."""

    data: str = ""
    event: str | None = None
    id: str | None = None
    retry: int | None = None
    comment: str | None = None

    def encode(self) -> bytes:
        """Encode this event into an RFC 8895 / WHATWG EventSource UTF-8 byte frame."""
        parts: list[str] = []
        if self.comment is not None:
            for line in _split_whatwg_lines(self.comment):
                parts.append(f": {line}\r\n")
        if self.id is not None:
            clean_id = _sanitize_field(self.id)
            if clean_id:
                parts.append(f"id: {clean_id}\r\n")
        if self.event is not None:
            clean_event = _sanitize_field(self.event)
            if clean_event:
                parts.append(f"event: {clean_event}\r\n")
        if self.retry is not None and self.retry >= 0:
            parts.append(f"retry: {self.retry}\r\n")
        for line in _split_whatwg_lines(self.data):
            parts.append(f"data: {line}\r\n")
        parts.append("\r\n")
        return "".join(parts).encode("utf-8")

    def to_asgi_message(self, *, more_body: bool = True) -> dict[str, Any]:
        """Convert this event into an ASGI http.response.sse extension message."""
        message: dict[str, Any] = {
            "type": "http.response.sse",
            "data": self.data,
            "more_body": more_body,
        }
        if self.event is not None:
            message["event"] = _sanitize_field(self.event)
        if self.id is not None:
            message["id"] = _sanitize_field(self.id)
        if self.retry is not None:
            message["retry"] = self.retry
        if self.comment is not None:
            message["comment"] = self.comment
        return message


class SseResponse:
    """ASGI 3.0 callable that streams SseEvent items via http.response.sse or http.response.body."""

    __slots__ = ("_headers", "_status", "_stream")

    def __init__(
        self,
        stream: AsyncIterable[SseEvent | str | dict[str, Any]],
        *,
        status: int = 200,
        headers: Iterable[tuple[bytes, bytes]] | None = None,
    ) -> None:
        self._stream = stream
        self._status = status
        merged: dict[bytes, bytes] = dict(DEFAULT_SSE_HEADERS)
        if headers is not None:
            for name, value in headers:
                merged[name.lower()] = value
        self._headers: list[tuple[bytes, bytes]] = list(merged.items())

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Any],
        send: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Execute the ASGI response lifecycle."""
        del receive
        extensions = scope.get("extensions")
        supports_sse_ext = isinstance(extensions, dict) and "http.response.sse" in extensions

        await send(
            {
                "type": "http.response.start",
                "status": self._status,
                "headers": self._headers,
            }
        )

        async for item in self._stream:
            event = _coerce_sse_event(item)
            if supports_sse_ext:
                await send(event.to_asgi_message(more_body=True))
            else:
                await send(
                    {
                        "type": "http.response.body",
                        "body": event.encode(),
                        "more_body": True,
                    }
                )

        if supports_sse_ext:
            await send({"type": "http.response.sse", "data": "", "more_body": False})
        else:
            await send({"type": "http.response.body", "body": b"", "more_body": False})


def _sanitize_field(value: str) -> str:
    """Remove CR, LF, and NUL characters from single-line SSE header values."""
    if "\r" not in value and "\n" not in value and "\x00" not in value:
        return value
    return value.replace("\r", "").replace("\n", "").replace("\x00", "")


def _split_whatwg_lines(value: str) -> list[str]:
    """Split a string across CRLF, CR, or LF per the WHATWG EventSource grammar."""
    if not value:
        return [""]
    return value.replace("\r\n", "\n").replace("\r", "\n").split("\n")


def _coerce_sse_event(item: SseEvent | str | dict[str, Any]) -> SseEvent:
    """Coerce a raw string or dictionary into an SseEvent instance."""
    if isinstance(item, SseEvent):
        return item
    if isinstance(item, str):
        return SseEvent(data=item)
    return SseEvent(
        data=str(item.get("data", "")),
        event=item.get("event"),
        id=item.get("id"),
        retry=item.get("retry"),
        comment=item.get("comment"),
    )
