"""ASGI 3.0 WebSocket session lifecycle and pre-upgrade websocket.http.response support."""

import asyncio
from collections.abc import Callable, Iterable
from typing import Any

import msgspec

__all__ = (
    "WebSocketDisconnect",
    "WebSocketHandshakeResult",
    "WebSocketSession",
    "run_asgi_websocket",
)


class WebSocketDisconnect(Exception):
    """Exception raised when a client disconnects from an ASGI WebSocket session."""

    __slots__ = ("code", "reason")

    def __init__(self, code: int = 1000, reason: str = "") -> None:
        super().__init__(f"WebSocket disconnected ({code}): {reason}")
        self.code = code
        self.reason = reason


class WebSocketHandshakeResult(msgspec.Struct, frozen=True, gc=False):
    """Outcome of the pre-upgrade ASGI WebSocket handshake."""

    accepted: bool
    subprotocol: str | None = None
    headers: list[tuple[bytes, bytes]] = msgspec.field(default_factory=list)
    status: int = 403
    body: bytes = b""


class WebSocketSession:
    """Manages ASGI receive and send callables for a single WebSocket connection."""

    __slots__ = (
        "_accepted",
        "_closed",
        "_connect_delivered",
        "_handshake_future",
        "_incoming",
        "_outgoing",
        "_reject_chunks",
        "_reject_headers",
        "_reject_started",
        "_reject_status",
    )

    def __init__(
        self,
        incoming: asyncio.Queue[dict[str, Any]],
        outgoing: asyncio.Queue[dict[str, Any]],
        handshake_future: asyncio.Future[WebSocketHandshakeResult],
    ) -> None:
        self._incoming = incoming
        self._outgoing = outgoing
        self._handshake_future = handshake_future
        self._connect_delivered = False
        self._accepted = False
        self._closed = False
        self._reject_started = False
        self._reject_status = 403
        self._reject_headers: list[tuple[bytes, bytes]] = []
        self._reject_chunks: list[bytes] = []

    async def receive(self) -> dict[str, Any]:
        """Deliver websocket.connect followed by inbound frames from the queue."""
        if not self._connect_delivered:
            self._connect_delivered = True
            return {"type": "websocket.connect"}
        if self._closed:
            return {"type": "websocket.disconnect", "code": 1000}
        msg = await self._incoming.get()
        if msg.get("type") == "websocket.disconnect":
            self._closed = True
        return msg

    async def send(self, message: dict[str, Any]) -> None:
        """Process outbound ASGI WebSocket messages and pre-upgrade HTTP rejections."""
        msg_type: str = message["type"]

        if msg_type == "websocket.accept":
            if self._accepted or self._reject_started:
                msg = "WebSocket handshake already completed"
                raise RuntimeError(msg)
            self._accepted = True
            subprotocol: str | None = message.get("subprotocol")
            raw_headers: Iterable[tuple[bytes, bytes]] = message.get("headers", ())
            headers = [(bytes(k), bytes(v)) for k, v in raw_headers]
            if not self._handshake_future.done():
                self._handshake_future.set_result(
                    WebSocketHandshakeResult(
                        accepted=True,
                        subprotocol=subprotocol,
                        headers=headers,
                        status=101,
                    )
                )
            return

        if msg_type == "websocket.http.response.start":
            if self._accepted or self._reject_started:
                msg = "Cannot start websocket.http.response after handshake decision"
                raise RuntimeError(msg)
            self._reject_started = True
            self._reject_status = int(message.get("status", 403))
            raw_headers = message.get("headers", ())
            self._reject_headers = [(bytes(k), bytes(v)) for k, v in raw_headers]
            return

        if msg_type == "websocket.http.response.body":
            if not self._reject_started:
                msg = "websocket.http.response.start must precede websocket.http.response.body"
                raise RuntimeError(msg)
            chunk = message.get("body", b"")
            if chunk:
                self._reject_chunks.append(bytes(chunk))
            more_body = bool(message.get("more_body", False))
            if not more_body and not self._handshake_future.done():
                self._closed = True
                self._handshake_future.set_result(
                    WebSocketHandshakeResult(
                        accepted=False,
                        headers=self._reject_headers,
                        status=self._reject_status,
                        body=b"".join(self._reject_chunks),
                    )
                )
            return

        if msg_type == "websocket.send":
            if not self._accepted:
                msg = "websocket.accept must be sent before websocket.send"
                raise RuntimeError(msg)
            text = message.get("text")
            data = message.get("bytes")
            if text is not None:
                await self._outgoing.put({"type": "websocket.send", "text": str(text)})
            elif data is not None:
                await self._outgoing.put({"type": "websocket.send", "bytes": bytes(data)})
            else:
                msg = "websocket.send requires either 'text' or 'bytes'"
                raise ValueError(msg)
            return

        if msg_type == "websocket.close":
            code = int(message.get("code", 1000))
            reason = str(message.get("reason") or "")
            self._closed = True
            if not self._accepted and not self._handshake_future.done():
                self._handshake_future.set_result(
                    WebSocketHandshakeResult(
                        accepted=False,
                        status=403,
                        body=reason.encode("utf-8"),
                    )
                )
            else:
                await self._outgoing.put({"type": "websocket.close", "code": code, "reason": reason})
            return

        msg = f"Unsupported ASGI WebSocket message type: {msg_type!r}"
        raise RuntimeError(msg)


async def run_asgi_websocket(
    app: Callable[[dict[str, Any], Any, Any], Any],
    scope: dict[str, Any],
    *,
    incoming: asyncio.Queue[dict[str, Any]],
    outgoing: asyncio.Queue[dict[str, Any]],
    handshake_future: asyncio.Future[WebSocketHandshakeResult],
) -> None:
    """Drive an ASGI 3.0 application over a WebSocket session."""
    extensions = scope.setdefault("extensions", {})
    if isinstance(extensions, dict):
        extensions.setdefault("websocket.http.response", {})
    session = WebSocketSession(incoming, outgoing, handshake_future)
    try:
        await app(scope, session.receive, session.send)
    finally:
        if not handshake_future.done():
            handshake_future.set_result(WebSocketHandshakeResult(accepted=False, status=403, body=b""))
