"""ASGI receive and send callables with bounded streaming backpressure."""

import asyncio
from collections.abc import Buffer, Callable, Iterable
from typing import Any

import msgspec

__all__ = (
    "DEFAULT_STREAM_HIGH_WATERMARK",
    "DEFAULT_STREAM_LOW_WATERMARK",
    "DEFAULT_STREAM_QUEUE_MAXSIZE",
    "AsgiProtocolError",
    "AsgiReceive",
    "AsgiResponse",
    "AsgiSend",
    "AsgiStreamQueuePair",
    "AsgiStreamReceive",
    "AsgiStreamSend",
    "ChunkCallback",
)

DEFAULT_STREAM_QUEUE_MAXSIZE = 256
DEFAULT_STREAM_HIGH_WATERMARK = 192
DEFAULT_STREAM_LOW_WATERMARK = 64

ChunkCallback = Callable[[int, bytes], None]


class AsgiProtocolError(RuntimeError):
    """Raised when an ASGI application violates the ASGI 3.0 state machine."""


class AsgiResponse(msgspec.Struct, gc=False):
    """Collected HTTP response from an ASGI application."""

    status: int
    headers: list[tuple[str, str]]
    body: bytes | memoryview
    request_id: int = 0
    file_path: str | None = None
    trailers: list[tuple[bytes, bytes]] = msgspec.field(default_factory=list)
    zero_copy_buffer: memoryview | None = None


class AsgiReceive:
    """Single-body ASGI ``receive`` callable with disconnect waiting."""

    __slots__ = ("_body", "_body_sent", "_disconnect_event")

    def __init__(
        self,
        body: Buffer = b"",
        disconnect_event: asyncio.Event | None = None,
    ) -> None:
        self._body: bytes = bytes(body) if not isinstance(body, bytes) else body
        self._body_sent = False
        self._disconnect_event = disconnect_event

    async def __call__(self) -> dict[str, Any]:
        """Deliver http.request once, then wait for http.disconnect."""
        if not self._body_sent:
            self._body_sent = True
            return {
                "type": "http.request",
                "body": self._body,
                "more_body": False,
            }

        if self._disconnect_event is not None:
            await self._disconnect_event.wait()
        else:
            await asyncio.Future()
        return {"type": "http.disconnect"}


class AsgiStreamReceive:
    """Multi-chunk ASGI ``receive`` callable for streaming request bodies."""

    __slots__ = ("_closed", "_queue")

    def __init__(self, maxsize: int = 64) -> None:
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self._closed = False

    async def feed_chunk(self, data: bytes, *, more_body: bool = True) -> None:
        """Enqueue a request body chunk, applying backpressure when full."""
        if self._closed:
            return
        await self._queue.put(
            {
                "type": "http.request",
                "body": bytes(data),
                "more_body": more_body,
            }
        )

    async def feed_disconnect(self) -> None:
        """Signal client disconnect to the ASGI application."""
        if not self._closed:
            self._closed = True
            await self._queue.put({"type": "http.disconnect"})

    def feed_chunk_nowait(self, data: bytes, *, more_body: bool = True) -> None:
        """Non-blocking variant of ``feed_chunk``."""
        if not self._closed:
            self._queue.put_nowait(
                {
                    "type": "http.request",
                    "body": bytes(data),
                    "more_body": more_body,
                }
            )

    def feed_disconnect_nowait(self) -> None:
        """Non-blocking variant of ``feed_disconnect``."""
        if not self._closed:
            self._closed = True
            self._queue.put_nowait({"type": "http.disconnect"})

    async def __call__(self) -> dict[str, Any]:
        """Return the next inbound ASGI event."""
        if self._closed and self._queue.empty():
            return {"type": "http.disconnect"}
        msg = await self._queue.get()
        if msg.get("type") == "http.disconnect":
            self._closed = True
        return msg


class AsgiSend:
    """Collects ``http.response.start``, ``http.response.body``, and wire extension events."""

    __slots__ = (
        "_body_chunks",
        "_complete",
        "_expects_trailers",
        "_file_path",
        "_headers",
        "_on_chunk",
        "_request_id",
        "_started",
        "_status",
        "_trailers",
        "_zero_copy_buffers",
    )

    def __init__(
        self,
        request_id: int = 0,
        on_chunk: ChunkCallback | None = None,
    ) -> None:
        self._request_id = request_id
        self._on_chunk = on_chunk
        self._started = False
        self._complete = False
        self._expects_trailers = False
        self._status = 200
        self._headers: list[tuple[str, str]] = []
        self._body_chunks: list[bytes] = []
        self._zero_copy_buffers: list[memoryview] = []
        self._file_path: str | None = None
        self._trailers: list[tuple[bytes, bytes]] = []

    @property
    def started(self) -> bool:
        """Return True once http.response.start has been processed."""
        return self._started

    @property
    def complete(self) -> bool:
        """Return True once the final body chunk or file path has been processed."""
        return self._complete

    @property
    def response(self) -> AsgiResponse:
        """Build the final AsgiResponse once http.response.start has been received."""
        if not self._started:
            msg = "ASGI application did not send 'http.response.start' before completing"
            raise AsgiProtocolError(msg)
        if len(self._zero_copy_buffers) == 1 and not self._body_chunks:
            zc_buf = self._zero_copy_buffers[0]
            return AsgiResponse(
                status=self._status,
                headers=self._headers,
                body=zc_buf,
                request_id=self._request_id,
                file_path=self._file_path,
                trailers=self._trailers,
                zero_copy_buffer=zc_buf,
            )
        if self._zero_copy_buffers:
            combined_parts = [*self._body_chunks, *(bytes(mv) for mv in self._zero_copy_buffers)]
            body_bytes = b"".join(combined_parts)
        else:
            body_bytes = b"".join(self._body_chunks)
        return AsgiResponse(
            status=self._status,
            headers=self._headers,
            body=body_bytes,
            request_id=self._request_id,
            file_path=self._file_path,
            trailers=self._trailers,
            zero_copy_buffer=None,
        )

    async def __call__(self, message: dict[str, Any]) -> None:
        """Process an outbound ASGI event dictionary."""
        msg_type: str = message["type"]

        if msg_type == "http.response.start":
            if self._started:
                msg = "Duplicate 'http.response.start' message"
                raise AsgiProtocolError(msg)
            self._started = True
            self._status = int(message["status"])
            self._expects_trailers = bool(message.get("trailers", False))
            raw_headers: Iterable[tuple[bytes | str, bytes | str]] = message.get("headers", ())
            self._headers = [
                (
                    k.decode("latin-1") if isinstance(k, bytes) else str(k),
                    v.decode("latin-1") if isinstance(v, bytes) else str(v),
                )
                for k, v in raw_headers
            ]
            return

        if msg_type == "http.response.body":
            self._assert_started(msg_type)
            if self._complete:
                msg = "Received 'http.response.body' after response was already complete"
                raise AsgiProtocolError(msg)
            chunk: bytes = message.get("body", b"")
            if chunk:
                raw = bytes(chunk)
                self._body_chunks.append(raw)
                if self._on_chunk is not None:
                    self._on_chunk(self._request_id, raw)
            more_body: bool = bool(message.get("more_body", False))
            if not more_body:
                self._complete = True
            return

        if msg_type == "http.response.pathsend":
            self._assert_started(msg_type)
            file_path = str(message["path"])
            self._file_path = file_path
            self._complete = True
            return

        if msg_type == "http.response.zerocopysend":
            self._assert_started(msg_type)
            buf = message.get("buffer", b"")
            if buf:
                mv = memoryview(buf)
                self._zero_copy_buffers.append(mv)
                if self._on_chunk is not None:
                    self._on_chunk(self._request_id, bytes(mv))
            if not bool(message.get("more_body", False)):
                self._complete = True
            return

        if msg_type == "http.response.trailers":
            self._assert_started(msg_type)
            raw_trailers: Iterable[tuple[bytes | str, bytes | str]] = message.get("headers", ())
            for k, v in raw_trailers:
                kb = k if isinstance(k, bytes) else str(k).encode("latin-1")
                vb = v if isinstance(v, bytes) else str(v).encode("latin-1")
                self._trailers.append((kb, vb))
            return

        if msg_type == "http.response.sse":
            self._assert_started(msg_type)
            sse_chunk = _encode_sse_message(message)
            if sse_chunk:
                self._body_chunks.append(sse_chunk)
                if self._on_chunk is not None:
                    self._on_chunk(self._request_id, sse_chunk)
            if not bool(message.get("more_body", False)):
                self._complete = True
            return

        msg = f"Unexpected ASGI send message type: {msg_type!r}"
        raise AsgiProtocolError(msg)

    def _assert_started(self, msg_type: str) -> None:
        if not self._started:
            msg = f"Received {msg_type!r} before 'http.response.start'"
            raise AsgiProtocolError(msg)


class AsgiStreamSend:
    """Queue-backed ASGI ``send`` callable for real-time streaming responses."""

    __slots__ = ("_complete", "_headers", "_queue", "_request_id", "_started", "_status")

    def __init__(self, request_id: int = 0, maxsize: int = 64) -> None:
        self._request_id = request_id
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self._started = False
        self._complete = False
        self._status = 200
        self._headers: list[tuple[str, str]] = []

    @property
    def started(self) -> bool:
        """Return True once http.response.start has been processed."""
        return self._started

    @property
    def complete(self) -> bool:
        """Return True once the stream has completed."""
        return self._complete

    @property
    def status(self) -> int:
        """Return the HTTP status code from http.response.start."""
        return self._status

    @property
    def headers(self) -> list[tuple[str, str]]:
        """Return the HTTP headers from http.response.start."""
        return self._headers

    @property
    def queue(self) -> asyncio.Queue[dict[str, Any]]:
        """Return the bounded outbound message queue."""
        return self._queue

    async def get_event(self) -> dict[str, Any]:
        """Dequeue the next outbound ASGI event sent by the application."""
        return await self._queue.get()

    async def __call__(self, message: dict[str, Any]) -> None:
        """Validate and enqueue an outbound ASGI message."""
        msg_type: str = message["type"]
        if msg_type == "http.response.start":
            if self._started:
                msg = "Duplicate 'http.response.start' message"
                raise AsgiProtocolError(msg)
            self._started = True
            self._status = int(message["status"])
            raw_headers: Iterable[tuple[bytes | str, bytes | str]] = message.get("headers", ())
            self._headers = [
                (
                    k.decode("latin-1") if isinstance(k, bytes) else str(k),
                    v.decode("latin-1") if isinstance(v, bytes) else str(v),
                )
                for k, v in raw_headers
            ]
            await self._queue.put(message)
            return

        if not self._started:
            msg = f"Received {msg_type!r} before 'http.response.start'"
            raise AsgiProtocolError(msg)

        if msg_type in ("http.response.body", "http.response.sse", "http.response.zerocopysend"):
            if not bool(message.get("more_body", False)):
                self._complete = True
            await self._queue.put(message)
            return

        if msg_type in ("http.response.pathsend", "http.response.trailers"):
            self._complete = True
            await self._queue.put(message)
            return

        msg = f"Unexpected ASGI stream send message type: {msg_type!r}"
        raise AsgiProtocolError(msg)


class AsgiStreamQueuePair:
    """Bounded receive/send queue pair for streaming ASGI SSE and WebSocket sessions."""

    __slots__ = (
        "_high_watermark",
        "_low_watermark",
        "_receive_queue",
        "_resume_event",
        "_send_paused",
        "_send_queue",
    )

    def __init__(
        self,
        maxsize: int = DEFAULT_STREAM_QUEUE_MAXSIZE,
        *,
        high_watermark: int = DEFAULT_STREAM_HIGH_WATERMARK,
        low_watermark: int = DEFAULT_STREAM_LOW_WATERMARK,
    ) -> None:
        bounded_max = max(1, maxsize)
        self._receive_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=bounded_max)
        self._send_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=bounded_max)
        self._high_watermark = min(bounded_max, max(1, high_watermark))
        self._low_watermark = max(0, min(self._high_watermark - 1, low_watermark))
        self._send_paused = False
        self._resume_event = asyncio.Event()
        self._resume_event.set()

    @property
    def receive_queue(self) -> asyncio.Queue[dict[str, Any]]:
        """Return the bounded inbound ASGI message queue."""
        return self._receive_queue

    @property
    def send_queue(self) -> asyncio.Queue[dict[str, Any]]:
        """Return the bounded outbound ASGI message queue."""
        return self._send_queue

    @property
    def is_send_paused(self) -> bool:
        """Return True when the outbound queue has crossed high_watermark."""
        return self._send_paused

    async def push_inbound(self, message: dict[str, Any]) -> None:
        """Enqueue an inbound ASGI message for the application."""
        await self._receive_queue.put(message)

    async def receive(self) -> dict[str, Any]:
        """ASGI receive callable backed by the bounded receive queue."""
        return await self._receive_queue.get()

    async def send(self, message: dict[str, Any]) -> None:
        """ASGI send callable that pauses when buffered items reach high_watermark."""
        await self._send_queue.put(message)
        if self._send_queue.qsize() >= self._high_watermark:
            self._send_paused = True
            self._resume_event.clear()

    async def wait_until_send_resumed(self) -> None:
        """Block until the consumer drains the outbound queue to low_watermark."""
        await self._resume_event.wait()

    async def pop_outbound(self) -> dict[str, Any]:
        """Dequeue the next outbound ASGI message and resume producers at low_watermark."""
        message = await self._send_queue.get()
        if self._send_paused and self._send_queue.qsize() <= self._low_watermark:
            self._send_paused = False
            self._resume_event.set()
        return message


def _encode_sse_message(message: dict[str, Any]) -> bytes:
    """Encode an ``http.response.sse`` dictionary into wire SSE bytes."""
    data = str(message.get("data", ""))
    event = message.get("event")
    msg_id = message.get("id")
    retry = message.get("retry")
    comment = message.get("comment")

    if not data and event is None and msg_id is None and retry is None and comment is None:
        return b""

    parts: list[str] = []
    if comment is not None:
        for line in str(comment).splitlines() or [""]:
            parts.append(f": {line}\r\n")
    if msg_id is not None:
        clean_id = str(msg_id).replace("\r", "").replace("\n", "").replace("\x00", "")
        if clean_id:
            parts.append(f"id: {clean_id}\r\n")
    if event is not None:
        clean_ev = str(event).replace("\r", "").replace("\n", "").replace("\x00", "")
        if clean_ev:
            parts.append(f"event: {clean_ev}\r\n")
    if isinstance(retry, int) and retry >= 0:
        parts.append(f"retry: {retry}\r\n")
    for line in data.splitlines() or [""]:
        parts.append(f"data: {line}\r\n")
    parts.append("\r\n")
    return "".join(parts).encode("utf-8")
