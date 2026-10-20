"""ASGI receive and send protocol callables with progressive streaming and wire extensions."""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from vertebra._vertebra import stream_file_chunks

__all__ = (
    "AsgiReceive",
    "AsgiResponse",
    "AsgiSend",
    "ChunkCallback",
)

_STATE_BODY = 0
_STATE_DISCONNECT = 1
_DISCONNECT_MSG: dict[str, Any] = {"type": "http.disconnect"}

type ChunkCallback = Callable[[int, bytes, bool], None]


@dataclasses.dataclass(slots=True)
class AsgiResponse:
    """Response container carrying unary or streamed body bytes and wire extension metadata."""

    status: int
    headers: Any
    body: bytes
    request_id: int = 0
    streaming: bool = False
    trailers: list[tuple[bytes, bytes]] = dataclasses.field(default_factory=list)
    early_hints: list[list[tuple[bytes, bytes]]] = dataclasses.field(default_factory=list)
    file_path: str | None = None
    file_range: tuple[int, int] | None = None
    zero_copy_view: memoryview | None = None
    debug_info: dict[str, Any] | None = None


class AsgiReceive:
    """Async callable that delivers ASGI http.request then http.disconnect."""

    __slots__ = ("_request", "_state")

    def __init__(self, request: Any) -> None:
        self._request = request
        self._state: int = _STATE_BODY

    async def __call__(self) -> dict[str, Any]:
        if self._state == _STATE_BODY:
            self._state = _STATE_DISCONNECT
            raw_body = getattr(self._request, "body", b"")
            body = raw_body if isinstance(raw_body, bytes) else bytes(raw_body or b"")
            return {
                "type": "http.request",
                "body": body,
                "more_body": False,
            }
        return _DISCONNECT_MSG


class AsgiSend:
    """Async callable that collects or progressively flushes ASGI send messages."""

    __slots__ = (
        "_body",
        "_debug_info",
        "_early_hints",
        "_extra_chunks",
        "_file_path",
        "_file_range",
        "_headers",
        "_on_chunk",
        "_started",
        "_status",
        "_streaming",
        "_trailers",
        "_zero_copy_view",
    )

    def __init__(self, *, on_chunk: ChunkCallback | None = None) -> None:
        self._status: int = 0
        self._headers: list[Any] = []
        self._body: bytes = b""
        self._extra_chunks: list[bytes] | None = None
        self._started: bool = False
        self._streaming: bool = False
        self._on_chunk: ChunkCallback | None = on_chunk
        self._trailers: list[tuple[bytes, bytes]] = []
        self._early_hints: list[list[tuple[bytes, bytes]]] = []
        self._file_path: str | None = None
        self._file_range: tuple[int, int] | None = None
        self._zero_copy_view: memoryview | None = None
        self._debug_info: dict[str, Any] | None = None

    @property
    def response(self) -> AsgiResponse:
        """Build the final AsgiResponse once http.response.start has been received."""
        if not self._started:
            msg = "ASGI app completed without sending http.response.start"
            raise RuntimeError(msg)
        body = b"".join(self._extra_chunks) if self._extra_chunks is not None else self._body
        return AsgiResponse(
            status=self._status,
            headers=self._headers,
            body=body,
            streaming=self._streaming,
            trailers=list(self._trailers),
            early_hints=list(self._early_hints),
            file_path=self._file_path,
            file_range=self._file_range,
            zero_copy_view=self._zero_copy_view,
            debug_info=self._debug_info,
        )

    async def __call__(self, message: dict[str, Any]) -> None:
        msg_type: str = message["type"]

        if msg_type in ("http.response.start", "websocket.http.response.start"):
            self._status = int(message["status"])
            self._headers = list(message.get("headers", []))
            self._started = True
            return

        if msg_type in ("http.response.body", "websocket.http.response.body"):
            raw_chunk = message.get("body", b"")
            chunk = raw_chunk if isinstance(raw_chunk, bytes) else bytes(raw_chunk)
            more_body = bool(message.get("more_body", False))
            if more_body:
                self._streaming = True
            if chunk:
                if not self._body and self._extra_chunks is None:
                    self._body = chunk
                else:
                    if self._extra_chunks is None:
                        self._extra_chunks = [self._body] if self._body else []
                    self._extra_chunks.append(chunk)
            if self._streaming and self._on_chunk is not None:
                self._on_chunk(self._status, chunk, not more_body)
            return

        if msg_type == "http.response.zerocopysend":
            buf = message["body"]
            view = buf if isinstance(buf, memoryview) else memoryview(buf)
            self._zero_copy_view = view
            chunk = bytes(view)
            more_body = bool(message.get("more_body", False))
            if more_body:
                self._streaming = True
            if not self._body and self._extra_chunks is None:
                self._body = chunk
            else:
                if self._extra_chunks is None:
                    self._extra_chunks = [self._body] if self._body else []
                self._extra_chunks.append(chunk)
            if self._streaming and self._on_chunk is not None:
                self._on_chunk(self._status, chunk, not more_body)
            return

        if msg_type == "http.response.pathsend":
            path = str(message["path"])
            start = message.get("start")
            end = message.get("end")
            self._file_path = path
            if start is not None and end is not None:
                self._file_range = (int(start), int(end))
            file_chunks = stream_file_chunks(path, start, end)
            joined = b"".join(file_chunks)
            self._body = joined
            self._headers.append((b"x-vertebra-sendfile", path.encode("utf-8")))
            return

        if msg_type == "http.response.trailers":
            raw_trailers = message.get("headers", [])
            for name, val in raw_trailers:
                n_bytes = name if isinstance(name, bytes) else str(name).encode("latin-1")
                v_bytes = val if isinstance(val, bytes) else str(val).encode("latin-1")
                self._trailers.append((n_bytes, v_bytes))
            return

        if msg_type == "http.response.early_hint":
            links = message.get("links", [])
            hint_headers: list[tuple[bytes, bytes]] = []
            for link in links:
                link_bytes = link if isinstance(link, bytes) else str(link).encode("latin-1")
                hint_headers.append((b"link", link_bytes))
            self._early_hints.append(hint_headers)
            return

        if msg_type == "http.response.debug":
            info = message.get("info", {})
            self._debug_info = dict(info) if isinstance(info, dict) else {"value": info}
            return

        msg = f"AsgiSend received unexpected ASGI message type: {msg_type!r}"
        raise RuntimeError(msg)
