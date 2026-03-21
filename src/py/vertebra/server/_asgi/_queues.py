"""ASGI receive/send protocol callables."""

import dataclasses
from typing import Any

__all__ = ["ReceiveChannel", "SendCollector"]

_STATE_BODY = 0
_STATE_DISCONNECT = 1


@dataclasses.dataclass
class AsgiResponse:
    """Duck-type compatible with VertebraResponse for batch runner."""

    status: int
    headers: list[tuple[str, str]]
    body: bytes
    request_id: int = 0


class ReceiveChannel:
    """Async callable that delivers ASGI http.request then http.disconnect."""

    __slots__ = ("_body", "_state")

    def __init__(self, request: Any) -> None:
        body = request.body
        self._body: bytes = body if isinstance(body, bytes) else bytes(body or b"")
        self._state: int = _STATE_BODY

    async def __call__(self) -> dict[str, Any]:
        if self._state == _STATE_BODY:
            self._state = _STATE_DISCONNECT
            return {
                "type": "http.request",
                "body": self._body,
                "more_body": False,
            }
        return {"type": "http.disconnect"}


class SendCollector:
    """Async callable that collects ASGI send messages into a response.

    Optimised for the common single-chunk case: stores the first body chunk
    directly, only allocates a list when multiple chunks arrive.
    """

    __slots__ = ("_status", "_headers", "_body", "_extra_chunks", "_started")

    def __init__(self) -> None:
        self._status: int = 0
        self._headers: list[tuple[str, str]] = []
        self._body: bytes = b""
        self._extra_chunks: list[bytes] | None = None
        self._started: bool = False

    async def __call__(self, message: dict[str, Any]) -> None:
        msg_type: str = message["type"]

        if msg_type == "http.response.start":
            self._status = int(message["status"])
            raw_headers: list[Any] = message.get("headers", [])
            self._headers = [(name.decode("latin-1"), value.decode("latin-1")) for name, value in raw_headers]
            self._started = True

        elif msg_type == "http.response.body":
            chunk: bytes = message.get("body", b"")
            if chunk:
                if not self._body:
                    # Fast path: first chunk stored directly, no list.
                    self._body = chunk if isinstance(chunk, bytes) else bytes(chunk)
                else:
                    # Slow path: multiple chunks — accumulate in list.
                    if self._extra_chunks is None:
                        self._extra_chunks = [self._body]
                    self._extra_chunks.append(chunk if isinstance(chunk, bytes) else bytes(chunk))

        else:
            msg = f"SendCollector received unexpected ASGI message type: {msg_type!r}"
            raise RuntimeError(msg)

    @property
    def response(self) -> AsgiResponse:
        if not self._started:
            msg = "ASGI app completed without sending http.response.start"
            raise RuntimeError(msg)
        body = b"".join(self._extra_chunks) if self._extra_chunks is not None else self._body
        return AsgiResponse(status=self._status, headers=self._headers, body=body)
