"""Embedded HTTP/3 WebTransport session dispatch helpers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Buffer, Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import vertebra._vertebra as _native

if TYPE_CHECKING:

    @runtime_checkable
    class WebTransportStream(Protocol):
        """Bidirectional WebTransport stream over HTTP/3."""

        @property
        def stream_id(self) -> int:
            """Return the underlying QUIC stream identifier."""
            ...

        async def recv(self, max_bytes: int = 65536) -> bytes:
            """Read up to ``max_bytes`` from the stream receive half."""
            ...

        async def send(self, data: Buffer) -> None:
            """Write all bytes in ``data`` to the stream send half."""
            ...

        async def finish(self) -> None:
            """Send FIN on the stream send half."""
            ...

    @runtime_checkable
    class WebTransportSession(Protocol):
        """Accepted HTTP/3 WebTransport session."""

        @property
        def session_id(self) -> int:
            """Return the WebTransport session identifier."""
            ...

        @property
        def path(self) -> str:
            """Return the request path and query from the extended CONNECT request."""
            ...

        @property
        def headers(self) -> list[tuple[bytes, bytes]]:
            """Return the request headers from the extended CONNECT request."""
            ...

        async def accept_bi(self) -> WebTransportStream:
            """Accept the next client-initiated bidirectional stream."""
            ...

        async def open_bi(self) -> WebTransportStream:
            """Open a new server-initiated bidirectional stream."""
            ...

        async def recv_datagram(self) -> bytes:
            """Receive the next incoming QUIC datagram for this session."""
            ...

        def send_datagram(self, data: Buffer) -> None:
            """Send a QUIC datagram framed for this WebTransport session."""
            ...

        async def close(self, error_code: int = 0, reason: str = "") -> None:
            """Close this WebTransport session and finish its CONNECT stream."""
            ...

else:
    WebTransportStream = vars(_native)["WebTransportStream"]
    WebTransportSession = vars(_native)["WebTransportSession"]

type WebTransportHandler = Callable[[WebTransportSession], Awaitable[None]]

__all__ = (
    "WebTransportDispatcher",
    "WebTransportHandler",
    "WebTransportSession",
    "WebTransportStream",
    "create_webtransport_dispatcher",
    "register_webtransport_handler",
)

logger = logging.getLogger(__name__)


def _strip_query(path: str) -> str:
    return path.split("?", 1)[0] or "/"


def _log_session_task_error(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error("WebTransport session handler failed", exc_info=exc)


def register_webtransport_handler(
    handler: WebTransportHandler | None = None,
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> None:
    """Register or clear the embedded Python WebTransport session handler.

    Args:
        handler: Async callable invoked with each accepted ``WebTransportSession``,
            or ``None`` to clear the active sink.
        loop: Target ``asyncio`` event loop. Defaults to the running event loop.
    """
    native_register = cast(
        Callable[[Callable[[WebTransportSession], None] | None, asyncio.AbstractEventLoop | None], None],
        vars(_native)["register_webtransport_handler"],
    )
    if handler is None:
        native_register(None, None)
        return

    target_loop = loop if loop is not None else asyncio.get_running_loop()
    active_tasks: set[asyncio.Task[None]] = set()

    def _on_session(session: WebTransportSession) -> None:
        if target_loop.is_closed():
            return

        async def _run_handler() -> None:
            await handler(session)

        task: asyncio.Task[None] = target_loop.create_task(_run_handler())
        active_tasks.add(task)
        task.add_done_callback(active_tasks.discard)
        task.add_done_callback(_log_session_task_error)

    native_register(_on_session, target_loop)


def create_webtransport_dispatcher(
    routes: Mapping[str, WebTransportHandler],
    *,
    fallback: WebTransportHandler | None = None,
) -> WebTransportHandler:
    """Build a path-routing async WebTransport session dispatcher.

    Args:
        routes: Mapping of exact request paths (without query string) to handlers.
        fallback: Optional fallback handler invoked when no exact path matches.

    Returns:
        An async handler suitable for ``register_webtransport_handler``.
    """
    compiled = {path or "/": fn for path, fn in routes.items()}

    async def _dispatch(session: WebTransportSession) -> None:
        route_path = _strip_query(session.path)
        target = compiled.get(route_path, fallback)
        if target is None:
            await session.close(404, "Not Found")
            return
        await target(session)

    return _dispatch


class WebTransportDispatcher:
    """Mutable route registry for embedded WebTransport session dispatch."""

    __slots__ = ("_fallback", "_routes")

    def __init__(self, *, fallback: WebTransportHandler | None = None) -> None:
        """Initialize an empty ``WebTransportDispatcher``.

        Args:
            fallback: Optional fallback handler when no registered path matches.
        """
        self._routes: dict[str, WebTransportHandler] = {}
        self._fallback = fallback

    def add_route(self, path: str, handler: WebTransportHandler) -> None:
        """Register ``handler`` for exact WebTransport CONNECT ``path``.

        Args:
            path: Exact request path without query string.
            handler: Async WebTransport session handler.
        """
        self._routes[path or "/"] = handler

    def route(self, path: str) -> Callable[[WebTransportHandler], WebTransportHandler]:
        """Decorate an async handler and register it for ``path``."""

        def _decorator(handler: WebTransportHandler) -> WebTransportHandler:
            self.add_route(path, handler)
            return handler

        return _decorator

    async def __call__(self, session: WebTransportSession) -> None:
        """Dispatch ``session`` to the matching route handler or close with 404."""
        target = self._routes.get(_strip_query(session.path), self._fallback)
        if target is None:
            await session.close(404, "Not Found")
            return
        await target(session)
