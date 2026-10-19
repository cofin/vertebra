"""ASGI receive/send protocol callables."""

import dataclasses
from typing import Any

__all__ = ["AsgiResponse", "ReceiveChannel", "SendCollector"]

_STATE_BODY = 0
_STATE_DISCONNECT = 1
_DISCONNECT_MSG: dict[str, Any] = {"type": "http.disconnect"}


@dataclasses.dataclass(slots=True)
class AsgiResponse:
    """Duck-type compatible with VertebraResponse for the batch runner."""

    status: int
    headers: Any
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
        return _DISCONNECT_MSG


class SendCollector:
    """Async callable that collects ASGI send messages into a response.

    Optimized for the common single-chunk case by storing the first body chunk
    directly and only allocating a list when multiple chunks arrive.
    """

    __slots__ = ("_body", "_extra_chunks", "_headers", "_started", "_status")

    def __init__(self) -> None:
        self._status: int = 0
        self._headers: list[tuple[str, str]] = []
        self._body: bytes = b""
        self._extra_chunks: list[bytes] | None = None
        self._started: bool = False

    @property
    def response(self) -> AsgiResponse:
        """Build the final AsgiResponse once http.response.start has been received."""
        if not self._started:
            msg = "ASGI app completed without sending http.response.start"
            raise RuntimeError(msg)
        body = b"".join(self._extra_chunks) if self._extra_chunks is not None else self._body
        return AsgiResponse(status=self._status, headers=self._headers, body=body)

    async def __call__(self, message: dict[str, Any]) -> None:
        msg_type: str = message["type"]

        if msg_type == "http.response.start":
            self._status = int(message["status"])
            self._headers = message.get("headers", [])
            self._started = True
        elif msg_type == "http.response.body":
            chunk: bytes = message.get("body", b"")
            if chunk:
                if not self._body:
                    self._body = chunk if isinstance(chunk, bytes) else bytes(chunk)
                else:
                    if self._extra_chunks is None:
                        self._extra_chunks = [self._body]
                    self._extra_chunks.append(chunk if isinstance(chunk, bytes) else bytes(chunk))
        else:
            msg = f"SendCollector received unexpected ASGI message type: {msg_type!r}"
            raise RuntimeError(msg)
