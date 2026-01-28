"""ASGI worker harness for Bun IPC bridge."""

import argparse
import asyncio
import atexit
import contextlib
import importlib
import logging
import os
import signal
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import msgspec

from vertebra._vertebra import AsyncRing, shm_unlink
from vertebra.core.serializers import MessagePackCodec, encode_json
from vertebra.messaging.protocol import HEADER_SIZE, Flags, RequestHeader, ResponseHeader
from vertebra.typing import LITESTAR_INSTALLED, LitestarResponse


class WorkerConfig(msgspec.Struct, frozen=True):
    """Configuration for an ASGI worker process."""

    name: str = "vertebra-asgi"
    worker_id: str | None = None
    slot_size: int = 65536
    slots: int = 1024
    timeout: float = 5.0
    host: str = "127.0.0.1"
    port: int = 8000
    # External notifier fds passed from ProcessManager (for shared notification)
    notify_read_fd: int | None = None
    notify_write_fd: int | None = None
    # Managed mode fields (from Rust supervisor)
    req_shm_fd: int | None = None
    resp_shm_fd: int | None = None
    shm_size: int | None = None


class AsgiRequest(msgspec.Struct, rename="camel", dict=True):
    """ASGI HTTP request deserialized from IPC.

    Uses camelCase wire format for cross-language compatibility with Rust server.
    Single-word fields (id, method, path, etc.) are identical in both formats.
    """
    id: str
    method: str
    path: str
    query: str
    headers: list[tuple[str, str]]
    body: bytes = b""
    scheme: str = "http"
    http_version: str = "1.1"
    raw_path: bytes | None = None
    query_string: bytes | None = None
    root_path: str = ""
    server: tuple[str, int | None] | None = None
    client: tuple[str, int | None] | None = None
    extensions: dict[str, dict[str, Any]] = msgspec.field(default_factory=dict)
    path_params: dict[str, str] = msgspec.field(default_factory=dict)
    auth_claims: dict[str, Any] | None = None


class AsgiResponse(msgspec.Struct, rename="camel", dict=True):
    """ASGI HTTP response serialized for IPC.

    Uses camelCase wire format for cross-language compatibility with Rust server.
    """
    id: str
    status: int
    headers: list[tuple[str, str]]
    body: bytes | None = None
    path: str | None = None
    offset: int | None = None
    count: int | None = None


class RequestMeta(msgspec.Struct, frozen=True):
    """HTTP/WebSocket request metadata parsed from IPC messages."""

    scope_type: str
    id: str
    method: str
    path: str
    query: str
    headers: list[tuple[str, str]]
    scheme: str
    http_version: str
    raw_path: bytes | None
    query_string: bytes | None
    root_path: str
    server: tuple[str, int | None] | None
    client: tuple[str, int | None] | None
    extensions: dict[str, dict[str, Any]]
    subprotocols: list[str] = msgspec.field(default_factory=list)
    path_params: dict[str, str] = msgspec.field(default_factory=dict)
    auth_claims: dict[str, Any] | None = None


class RequestBodyState(msgspec.Struct):
    """Mutable state for tracking request body streaming."""

    queue: asyncio.Queue[dict[str, Any]]
    done: bool = False
    disconnected: bool = False


class RequestContext(msgspec.Struct, frozen=True):
    """Context for an in-flight request."""

    meta: RequestMeta
    body: RequestBodyState
    task: asyncio.Task[None]


class ClientState(msgspec.Struct, frozen=True):
    """State for a connected IPC client."""

    id: str
    request: AsyncRing
    response: AsyncRing
    # Response notifier fd from TypeScript for bidirectional notification
    # When set, we signal this fd after sending responses so TypeScript wakes immediately
    resp_notify_fd: int | None = None


class ResponseWriter:
    """Writer that sends responses to TypeScript via shared memory ring.

    Implements notification suppression: only signals TypeScript when the ring
    was previously empty (transition from empty→non-empty). This batches
    notifications and reduces syscall overhead from O(n) to O(1) per batch.
    """

    # Track pending notification per fd to implement suppression
    _pending_notifications: dict[int, bool] = {}

    def __init__(
        self,
        ring: AsyncRing,
        codec: MessagePackCodec,
        timeout_ms: int,
        correlation_id: int = 0,
        resp_notify_fd: int | None = None,
        *,
        managed_mode: bool = False,
    ) -> None:
        self._ring = ring
        self._codec = codec
        self._timeout_ms = timeout_ms
        self._correlation_id = correlation_id
        self._resp_notify_fd = resp_notify_fd
        self._managed_mode = managed_mode
        # Aggregation state for managed mode (Rust server expects single AsgiResponse)
        self._agg_status: int = 200
        self._agg_headers: list[tuple[str, str]] = []
        self._agg_body_parts: list[bytes] = []
        self._agg_request_id: str = ""

    async def send(self, frame: dict[str, Any]) -> None:
        """Send a response frame.

        In managed mode (Rust server), frames are aggregated and sent as a single
        AsgiResponse when the response is complete (more_body=False).
        """
        if self._managed_mode:
            await self._send_managed(frame)
        else:
            await self._send_frame(frame)

    async def _send_frame(self, frame: dict[str, Any]) -> None:
        """Send a single ASGI frame (legacy mode for TypeScript broker)."""
        payload = self._codec.encode(frame)
        header = ResponseHeader(id=self._correlation_id, flags=Flags.NONE, payload_len=len(payload))
        message = header.to_bytes() + payload
        # AsyncRing.send() automatically notifies waiters
        self._ring.send(message, 0)
        # Signal TypeScript that response is ready (notification suppression)
        # Only signal if we haven't already signaled (batches multiple responses)
        if self._resp_notify_fd is not None:
            already_pending = ResponseWriter._pending_notifications.get(self._resp_notify_fd, False)
            if not already_pending:
                try:
                    # eventfd signal: write 8-byte u64 counter (value 1)
                    os.write(self._resp_notify_fd, b"\x01\x00\x00\x00\x00\x00\x00\x00")
                    ResponseWriter._pending_notifications[self._resp_notify_fd] = True
                except OSError:
                    pass  # Ignore errors - client may have disconnected

    async def _send_managed(self, frame: dict[str, Any]) -> None:
        """Aggregate ASGI frames and send as single AsgiResponse (managed mode).

        Rust server expects a single aggregated response, not multiple ASGI frames.
        We buffer http.response.start and http.response.body frames, then send
        a complete AsgiResponse when more_body=False.
        """
        frame_type = frame.get("type")
        self._agg_request_id = frame.get("id", self._agg_request_id)

        if frame_type == "http.response.start":
            self._agg_status = frame.get("status", 200)
            # ASGI headers are [[b"name", b"value"], ...], convert to [(str, str), ...]
            raw_headers = frame.get("headers", [])
            self._agg_headers = [
                (
                    h[0].decode() if isinstance(h[0], bytes) else h[0],
                    h[1].decode() if isinstance(h[1], bytes) else h[1],
                )
                for h in raw_headers
            ]
        elif frame_type == "http.response.body":
            body = frame.get("body")
            if body:
                self._agg_body_parts.append(body if isinstance(body, bytes) else body.encode())

            # Send aggregated response when complete
            if not frame.get("more_body", False):
                response = AsgiResponse(
                    id=self._agg_request_id,
                    status=self._agg_status,
                    headers=self._agg_headers,
                    body=b"".join(self._agg_body_parts) if self._agg_body_parts else None,
                )
                payload = self._codec.encode(response)
                header = ResponseHeader(id=self._correlation_id, flags=Flags.NONE, payload_len=len(payload))
                message = header.to_bytes() + payload
                self._ring.send(message, 0)

                # Signal Rust server
                if self._resp_notify_fd is not None:
                    try:
                        # print(f"DEBUG: Python notifying Rust on fd {self._resp_notify_fd}", file=sys.stderr)
                        os.write(self._resp_notify_fd, b"\x01\x00\x00\x00\x00\x00\x00\x00")
                    except OSError:
                        pass

                # Reset aggregation state
                self._agg_status = 200
                self._agg_headers = []
                self._agg_body_parts = []

    @classmethod
    def clear_pending(cls, fd: int) -> None:
        """Mark notification as consumed. Called by TypeScript after draining ring."""
        cls._pending_notifications.pop(fd, None)


class VertebraWebSocket:
    def __init__(self, msg: dict[str, Any], writer: "ResponseWriter") -> None:
        self._msg = msg
        self._w = writer

    def receive_text(self) -> str | None:
        return self._msg.get("text")

    async def send_text(self, data: str) -> None:
        await self._w.send({"id": self._msg.get("id"), "type": "ws.message", "text": data})


AsgiApp = Callable[
    [dict[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]

HEALTH_PATH = "/__vertebra__/health"
PING_SCOPE_TYPE = "vertebra.ping"
ASGI_VERSION = "3.0"
ASGI_HTTP_SPEC_VERSION = "2.5"


class AsgiWorker:
    def __init__(self, app: AsgiApp, config: WorkerConfig | None = None) -> None:
        self._app = app
        self._config = config or WorkerConfig()
        self._codec = MessagePackCodec()
        self._control: AsyncRing | None = None
        self._clients: dict[str, "ClientState"] = {}
        self._contexts: dict[str, dict[str, RequestContext]] = {}
        self._running = False
        self._stop_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client_callbacks: dict[str, tuple[int, Callable[[], None]]] = {}
        self._lifespan_task: asyncio.Task[None] | None = None
        self._lifespan_queue: asyncio.Queue[dict[str, Any]] | None = None
        self._lifespan_state: dict[str, Any] = {}
        self._lifespan_startup = asyncio.Event()
        self._lifespan_shutdown = asyncio.Event()
        self._lifespan_failed: str | None = None
        self._lifespan_receive_called = False
        self._lifespan_supported: bool | None = None
        self._native_runners: dict[int, Callable] = {}

    async def start(self) -> None:
        if self._control is not None:
            return
        await self._start_lifespan()

        # Check if native integration is enabled
        native_enabled = os.environ.get("VERTEBRA_NATIVE", "1") == "1"

        # If the app is a Litestar instance, we can reflect routes and compile pipelines
        if native_enabled and LITESTAR_INSTALLED:
            from litestar import Litestar

            if isinstance(self._app, Litestar):
                from vertebra.extensions.litestar.compiler import PipelineCompiler
                from vertebra.extensions.litestar.native import RouteCapability, classify_route, reflect_routes

                routes = reflect_routes(self._app)
                compiler = PipelineCompiler()
                logger = logging.getLogger("vertebra.worker")
                for route in routes:
                    if classify_route(route) == RouteCapability.NATIVE:
                        try:
                            runner = compiler.compile_route(route.handler)
                            self._native_runners[route.handler_id] = runner
                        except (TypeError, ValueError, AttributeError) as exc:
                            logger.debug("Route %s will use ASGI fallback: %s", route.path, exc)

        # Managed mode: Rust supervisor provides rings via FDs/env
        if self._config.req_shm_fd is not None and self._config.resp_shm_fd is not None:
            # We are managed by Rust supervisor.
            if hasattr(AsyncRing, "from_raw_fd_handle"):
                try:
                    from vertebra._vertebra import AsyncMpmcRing
                except ImportError:
                    # Fallback or error if bindings not updated
                    raise ImportError("AsyncMpmcRing not available in vertebra._vertebra") from None

                req_fd = self._config.req_shm_fd
                resp_fd = self._config.resp_shm_fd

                # Request Ring is MPMC (Broker -> Worker)
                req_ring = AsyncMpmcRing.from_raw_fd_handle(req_fd, self._config.slot_size, self._config.slots)

                # Response Ring is SPSC (Worker -> Broker)
                resp_ring = AsyncRing.from_raw_fd_handle(resp_fd, self._config.slot_size, self._config.slots)

                # Register "server" client directly
                client_id = "server"
                client = ClientState(
                    id=client_id,
                    request=req_ring,
                    response=resp_ring,
                    resp_notify_fd=self._config.notify_write_fd,
                )
                self._clients[client_id] = client
                self._contexts[client_id] = {}

        control_name = self._control_ring_name()
        if self._config.req_shm_fd is None:
            # Legacy mode: Create named ring
            self._control = AsyncRing.create_named(control_name, self._config.slot_size, self._config.slots)

            # Register atexit handler to clean up SHM on unexpected exit
            def cleanup_shm() -> None:
                # PyO3 raises RuntimeError for syscall failures, not OSError
                with contextlib.suppress(OSError, RuntimeError):
                    shm_unlink(control_name)

            atexit.register(cleanup_shm)

    async def serve_forever(self) -> None:
        await self.start()
        self._running = True
        try:
            await self._serve_loop()
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running and self._control is None and self._lifespan_task is None:
            return
        self._running = False

        # Signal the serve loop to exit
        if self._stop_event is not None:
            self._stop_event.set()

        for contexts in self._contexts.values():
            for context in contexts.values():
                context.task.cancel()
        self._contexts.clear()
        # Clear clients - rings are released when objects go out of scope
        self._clients.clear()

        # Clean up shared memory to prevent /dev/shm leakage
        control_name = self._control_ring_name()
        self._control = None
        # PyO3 raises RuntimeError for syscall failures, not OSError
        with contextlib.suppress(OSError, RuntimeError):
            shm_unlink(control_name)

        await self._shutdown_lifespan()

    async def _serve_loop(self) -> None:
        """Main event loop using reactor pattern with add_reader().

        Instead of polling with asyncio.sleep(), we register notification FDs
        with the event loop. The loop wakes us when data arrives (3-10us latency).
        """
        # In managed mode, we might not have a control ring, but we have clients.
        if self._control is None and not self._clients:
            return

        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()

        # Use external notifier fd if provided (from ProcessManager), otherwise use ring's internal
        external_notify_fd = self._config.notify_read_fd

        # PERF: Maximum batch size for request processing.
        # Limits memory usage while enabling efficient batching.
        max_batch_size = 64

        def on_control_readable() -> None:
            """Callback when notification fd has data available."""
            # Clear notification - use external notifier if available
            if external_notify_fd is not None:
                # External eventfd is cleared by reading 8 bytes (u64 counter)
                with contextlib.suppress(OSError):
                    os.read(external_notify_fd, 8)
            elif self._control is not None:
                self._control.clear_notification()

            # Drain control ring if it exists using batch API
            # PERF: try_recv_batch() amortizes Python/Rust boundary crossings
            if self._control is not None:
                batch = self._control.try_recv_batch(max_batch_size)
                for payload, _flags in batch:
                    asyncio.create_task(self._handle_control_payload(payload))

            # In supervisor mode, also drain all client request rings
            # (they share the same notification fd)
            # PERF: Batch receive reduces per-message overhead
            if external_notify_fd is not None:
                tasks: list[tuple[str, ClientState, bytes]] = []
                for client_id, client in list(self._clients.items()):
                    # Use try_recv_batch for efficient batch receive
                    remaining = max_batch_size - len(tasks)
                    if remaining <= 0:
                        break
                    batch = client.request.try_recv_batch(remaining)
                    for payload, _flags in batch:
                        tasks.append((client_id, client, payload))

                # PERF: Create all tasks at once after batch collection
                # This reduces per-task overhead vs creating in the inner loop
                for client_id, client, payload in tasks:
                    asyncio.create_task(self._handle_client_payload(client_id, client, payload))

        # Register notification FD with the event loop
        # Use external notifier fd from ProcessManager if provided, otherwise ring's internal
        control_fd = external_notify_fd if external_notify_fd is not None else self._control.notifier_fd()
        self._loop.add_reader(control_fd, on_control_readable)

        try:
            # Wait until stop is signaled via event
            await self._stop_event.wait()
        finally:
            # Clean up: remove all FD readers
            self._loop.remove_reader(control_fd)
            for _, (fd, _) in list(self._client_callbacks.items()):
                self._loop.remove_reader(fd)
            self._client_callbacks.clear()

    async def _handle_control_payload(self, payload: bytes) -> None:
        if len(payload) < HEADER_SIZE:
            return
        header = RequestHeader.from_bytes(payload[:HEADER_SIZE])
        try:
            message = self._codec.decode(payload[HEADER_SIZE:])
        except Exception:  # noqa: BLE001
            return
        if not isinstance(message, dict):
            return
        kind = message.get("kind")
        if kind == "register_client":
            await self._register_client(message, header.id)
        elif kind == "unregister_client":
            client_id = message.get("client_id")
            if isinstance(client_id, str):
                await self._unregister_client(client_id)

    async def _register_client(self, message: dict[str, Any], correlation_id: int) -> None:
        client_id = message.get("client_id")
        if not isinstance(client_id, str):
            return
        if client_id in self._clients:
            await self._send_control_ack(message, ok=True, correlation_id=correlation_id)
            return
        req_name = message.get("req_ring") or self._request_ring_name(client_id)
        resp_name = message.get("resp_ring") or self._response_ring_name(client_id)
        # Get response notifier fd for bidirectional notification
        resp_notify_fd = message.get("resp_notify_fd")
        if resp_notify_fd is not None and not isinstance(resp_notify_fd, int):
            resp_notify_fd = None
        try:
            request_ring = AsyncRing.open_named(str(req_name), self._config.slot_size, self._config.slots)
            response_ring = AsyncRing.open_named(str(resp_name), self._config.slot_size, self._config.slots)
        except Exception as exc:  # noqa: BLE001 - defensive for IPC setup
            await self._send_control_ack(message, ok=False, error=str(exc), correlation_id=correlation_id)
            return

        client = ClientState(id=client_id, request=request_ring, response=response_ring, resp_notify_fd=resp_notify_fd)
        self._clients[client_id] = client
        self._contexts[client_id] = {}

        # Store client for polling in _drain_all_rings().
        # When using external notifier (supervisor mode), all rings share one notification fd,
        # so we poll all client rings when ANY notification arrives.
        # The external notifier is already registered in _serve_loop() for control ring.
        # No need to add_reader for each client ring - they all share the same fd.
        self._client_callbacks[client_id] = (0, lambda: None)  # Placeholder, not used in supervisor mode

        await self._send_control_ack(message, ok=True, correlation_id=correlation_id)

    async def _unregister_client(self, client_id: str) -> None:
        # Remove FD reader before releasing rings
        if self._loop is not None:
            callback_info = self._client_callbacks.pop(client_id, None)
            if callback_info is not None:
                fd, _ = callback_info
                self._loop.remove_reader(fd)

        # Ring objects released when client goes out of scope
        self._clients.pop(client_id, None)
        self._contexts.pop(client_id, None)

    async def _send_control_ack(
        self, message: dict[str, Any], *, ok: bool, correlation_id: int, error: str | None = None
    ) -> None:
        client_id = message.get("client_id")
        resp_name = message.get("resp_ring")
        if not isinstance(client_id, str):
            return
        client = self._clients.get(client_id)
        response_ring = client.response if client is not None else None
        resp_notify_fd = client.resp_notify_fd if client is not None else None
        if response_ring is None and isinstance(resp_name, str):
            with contextlib.suppress(Exception):
                response_ring = AsyncRing.open_named(resp_name, self._config.slot_size, self._config.slots)
        if response_ring is None:
            return
        payload = {"id": message.get("id"), "ok": ok}
        if error is not None:
            payload["error"] = error
        encoded = self._codec.encode(payload)
        header = ResponseHeader(id=correlation_id, flags=Flags.NONE, payload_len=len(encoded))
        message_bytes = header.to_bytes() + encoded
        # AsyncRing.send() automatically notifies waiters
        response_ring.send(message_bytes, 0)
        # Signal TypeScript that response is ready (bidirectional notification)
        if resp_notify_fd is not None:
            with contextlib.suppress(OSError):
                os.write(resp_notify_fd, b"\x01\x00\x00\x00\x00\x00\x00\x00")

    async def _handle_client_payload(self, client_id: str, client: ClientState, payload: bytes) -> None:
        if len(payload) < HEADER_SIZE:
            return
        header = RequestHeader.from_bytes(payload[:HEADER_SIZE])
        try:
            message = self._codec.decode(payload[HEADER_SIZE:])
        except Exception:  # noqa: BLE001
            return
        
        writer = ResponseWriter(
            client.response,
            self._codec,
            int(self._config.timeout * 1000),
            correlation_id=header.id,
            resp_notify_fd=client.resp_notify_fd,
            managed_mode=True,  # Rust server expects aggregated AsgiResponse
        )
        contexts = self._contexts.setdefault(client_id, {})

        if isinstance(message, list):
            await self._handle_unary_request(message, contexts, writer)
            return

        if not isinstance(message, dict):
            return
        
        frame_type = message.get("type")
        if frame_type is None:
            await self._handle_unary_request(message, contexts, writer)
            return
        await self._handle_frame(message, contexts, writer)

    async def _handle_frame(
        self, message: dict[str, Any], contexts: dict[str, RequestContext], writer: "ResponseWriter"
    ) -> None:
        frame_type = message.get("type")
        if frame_type is None:
            await self._handle_unary_request(message, contexts, writer)
            return
        # Handle ping requests for health checks (no ASGI app invocation)
        if frame_type == PING_SCOPE_TYPE:
            await self._handle_ping(message, writer)
            return
        if frame_type == "direct.http":
            await self._handle_direct_http(message, writer, contexts)
            return
        if frame_type in {"ws.open", "ws.message", "ws.close"}:
            await self._handle_direct_websocket(message, writer)
            return
        if frame_type == "http.request.start":
            meta = self._parse_request_meta(message, scope_type="http")
            if meta is None:
                return
            await self._start_request(meta, None, contexts, writer)
            return
        if frame_type == "websocket.connect":
            meta = self._parse_request_meta(message, scope_type="websocket")
            if meta is None:
                return
            await self._start_request(meta, None, contexts, writer)
            return
        request_id = message.get("id")
        if not isinstance(request_id, str):
            return
        context = contexts.get(request_id)
        if context is None:
            return
        await self._enqueue_request_frame(context, message)

    async def _handle_ping(self, message: dict[str, Any], writer: "ResponseWriter") -> None:
        """Handle ping requests for health checks. Returns immediate success without invoking ASGI app."""
        request_id = message.get("id")
        await writer.send({"id": request_id, "ok": True, "type": "vertebra.pong"})

    def _worker_id(self) -> str:
        if self._config.worker_id is not None:
            return str(self._config.worker_id)
        return os.environ.get("VERTEBRA_WORKER_ID", "0")

    def _control_ring_name(self) -> str:
        worker_id = self._worker_id()
        return f"/{self._config.name}.asgi.{worker_id}.control"

    def _request_ring_name(self, client_id: str) -> str:
        worker_id = self._worker_id()
        return f"/{self._config.name}.asgi.{worker_id}.client.{client_id}.req"

    def _response_ring_name(self, client_id: str) -> str:
        worker_id = self._worker_id()
        return f"/{self._config.name}.asgi.{worker_id}.client.{client_id}.resp"

    async def _handle_direct_websocket(self, message: dict[str, Any], writer: "ResponseWriter") -> None:
        handler_id = message.get("handler_id")
        if handler_id is None:
            return

        runner = self._native_runners.get(handler_id)
        if runner is None:
            return

        socket = VertebraWebSocket(message, writer)
        await runner(socket)

    async def _handle_direct_http(
        self, message: dict[str, Any], writer: "ResponseWriter", contexts: dict[str, RequestContext] | None = None
    ) -> None:
        if contexts is None:
            contexts = {}
        handler_id = message.get("handler_id")
        request_id = message.get("id")

        runner = self._native_runners.get(handler_id)
        if runner is None:
            # Fallback to standard ASGI if handler not found in native runners
            # But we need a RequestMeta for that.
            meta = self._parse_request_meta(message, scope_type="http")
            if meta:
                # We don't have a context yet, we'll create one via _start_request
                # but wait, _handle_frame usually does this.
                # If we are here, it means it WAS marked as direct but we don't have the runner.
                await self._start_request(meta, message.get("body"), contexts, writer)
                return

            await writer.send({"id": request_id, "type": "http.response.start", "status": 404, "headers": []})
            await writer.send({
                "id": request_id,
                "type": "http.response.body",
                "body": b"Native runner not found",
                "more_body": False,
            })
            return

        # Ensure Litestar is available if we have a native runner (which implies Litestar)
        if LITESTAR_INSTALLED:
            from vertebra.extensions.litestar.native import VertebraRequest

            conn = VertebraRequest(message)
        else:
            # Should be unreachable if _native_runners is populated correctly
            return

        try:
            result = await runner(conn)

            status = 200
            headers = [("content-type", "application/json")]
            body = b""

            if isinstance(result, LitestarResponse):
                status = result.status_code
                # Copy headers
                for k, v in result.headers.items():
                    headers.append((k.lower(), v))

                # Handle content
                content = result.content
                if content is None:
                    body = b""
                elif isinstance(content, (bytes, bytearray)):
                    body = bytes(content)
                elif isinstance(content, str):
                    body = content.encode()
                else:
                    # Assume JSON serializable (dict, Pydantic, etc)
                    # We override content-type if it was default
                    body = encode_json(content, as_bytes=True)
            else:
                # Default behavior: assume raw data -> JSON 200 OK
                body = encode_json(result, as_bytes=True)

            await writer.send({"id": request_id, "type": "http.response.start", "status": status, "headers": headers})
            await writer.send({"id": request_id, "type": "http.response.body", "body": body, "more_body": False})
        except Exception as exc:  # noqa: BLE001
            await writer.send({"id": request_id, "type": "http.response.start", "status": 500, "headers": []})
            await writer.send({
                "id": request_id,
                "type": "http.response.body",
                "body": str(exc).encode(),
                "more_body": False,
            })

    def _parse_request_meta(self, message: dict[str, Any], scope_type: str = "http") -> RequestMeta | None:
        try:
            request_id = message.get("id")
            if not isinstance(request_id, str):
                return None
            headers_raw = message.get("headers", [])
            headers: list[tuple[str, str]] = []
            for item in headers_raw:
                if not isinstance(item, (list, tuple)) or len(item) != 2:
                    continue
                key, value = item
                headers.append((str(key), str(value)))
            subprotocols_raw = message.get("subprotocols", []) or []
            subprotocols: list[str] = [str(value) for value in subprotocols_raw]
            return RequestMeta(
                scope_type=scope_type,
                id=request_id,
                method=str(message.get("method", "GET")),
                path=str(message.get("path", "/")),
                query=str(message.get("query", "")),
                headers=headers,
                scheme=str(message.get("scheme", "http")),
                http_version=str(message.get("httpVersion") or message.get("http_version") or "1.1"),
                raw_path=message.get("rawPath"),
                query_string=message.get("queryString"),
                root_path=str(message.get("rootPath", "")),
                server=message.get("server"),
                client=message.get("client"),
                extensions=message.get("extensions") or {},
                subprotocols=subprotocols,
                path_params=message.get("path_params") or {},
                auth_claims=message.get("auth_claims"),
            )
        except Exception:  # noqa: BLE001 - defensive parsing of untrusted IPC data
            return None

    def _extract_trace_id(self, headers: list[tuple[str, str]]) -> str | None:
        for key, value in headers:
            lowered = key.lower()
            if lowered == "x-request-id":
                return value
            if lowered == "traceparent":
                return value
        return None

    def _log(self, level: str, message: str, **fields: Any) -> None:
        logger = logging.getLogger("vertebra.worker")
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        log_level = level_map.get(level, logging.INFO)
        if not logger.isEnabledFor(log_level):
            return
        if fields:
            logger.log(log_level, "%s %s", message, fields)
        else:
            logger.log(log_level, "%s", message)

    async def _handle_unary_request(
        self, message: Any, contexts: dict[str, RequestContext], writer: "ResponseWriter"
    ) -> None:
        """Handle unary request format (single message with inline body)."""
        try:
            request = msgspec.convert(message, AsgiRequest)
        except Exception as e:  # noqa: BLE001 - defensive conversion of untrusted IPC data
            logging.getLogger("vertebra.worker").error(
                "Failed to convert IPC message to AsgiRequest: %s (message type: %s)", e, type(message)
            )
            return
        meta = RequestMeta(
            scope_type="http",
            id=request.id,
            method=request.method,
            path=request.path,
            query=request.query,
            headers=request.headers,
            scheme=request.scheme,
            http_version=request.http_version,
            raw_path=request.raw_path,
            query_string=request.query_string,
            root_path=request.root_path,
            server=request.server,
            client=request.client,
            extensions=request.extensions,
            subprotocols=[],
            auth_claims=request.auth_claims,
        )
        await self._start_request(meta, request.body, contexts, writer)

    async def _start_request(
        self,
        meta: RequestMeta,
        initial_body: bytes | None,
        contexts: dict[str, RequestContext],
        writer: "ResponseWriter",
    ) -> None:
        if meta.id in contexts:
            return
        body_state = RequestBodyState(queue=asyncio.Queue(maxsize=1))
        task = asyncio.create_task(self._run_http_request(meta, body_state, writer, contexts))
        contexts[meta.id] = RequestContext(meta=meta, body=body_state, task=task)
        if meta.scope_type == "websocket":
            await body_state.queue.put({"type": "websocket.connect"})
        if initial_body is not None:
            body_state.done = True
            await body_state.queue.put({"type": "http.request", "body": initial_body, "more_body": False})

    async def _enqueue_request_frame(self, context: RequestContext, message: dict[str, Any]) -> None:
        if message.get("type") in {"http.disconnect", "websocket.disconnect"}:
            context.body.disconnected = True
            context.body.done = True
        if message.get("type") == "http.request" and not message.get("more_body"):
            context.body.done = True
        await context.body.queue.put(message)

    async def _receive_body(self, state: RequestBodyState) -> dict[str, Any]:
        if state.disconnected:
            return {"type": "http.disconnect"}
        if state.done and state.queue.empty():
            return {"type": "http.disconnect"}
        message = await state.queue.get()
        if message.get("type") == "http.disconnect":
            state.disconnected = True
        if message.get("type") == "http.request" and not message.get("more_body", False):
            state.done = True
        return message

    async def _run_http_request(
        self,
        meta: RequestMeta,
        body_state: RequestBodyState,
        writer: "ResponseWriter",
        contexts: dict[str, RequestContext],
    ) -> None:
        response_started = False
        try:
            if meta.scope_type == "websocket":
                await self._run_websocket_request(meta, body_state, writer)
                return
            if meta.path == HEALTH_PATH:
                await writer.send({
                    "id": meta.id,
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [("content-type", "text/plain")],
                })
                await writer.send({"id": meta.id, "type": "http.response.body", "body": b"ok", "more_body": False})
                return

            from vertebra.security.auth.state import reset_user, set_user

            claims = meta.auth_claims
            if claims:
                from vertebra.security.auth.models import VertebraUserMixin
                from vertebra.security.auth.proxy import LazyUserProxy

                async def noop_fetcher(user_id: str, ctx: Any) -> Any:
                    return VertebraUserMixin(**claims)

                user = LazyUserProxy(VertebraUserMixin, claims, noop_fetcher, context=meta)
            else:
                from vertebra.security.auth.models import AnonymousUser

                user = AnonymousUser()

            token = set_user(user)

            try:
                raw_path = meta.raw_path if meta.raw_path is not None else meta.path.encode()
                query_string = meta.query_string if meta.query_string is not None else meta.query.encode()
                scope = {
                    "type": "http",
                    "asgi": {"version": ASGI_VERSION, "spec_version": ASGI_HTTP_SPEC_VERSION},
                    "http_version": meta.http_version,
                    "method": meta.method,
                    "path": meta.path,
                    "raw_path": raw_path,
                    "query_string": query_string,
                    "headers": [(k.lower().encode(), v.encode()) for k, v in meta.headers],
                    "scheme": meta.scheme,
                    "root_path": meta.root_path,
                    "server": meta.server or (self._config.host, self._config.port),
                    "client": meta.client or ("127.0.0.1", 0),
                    "extensions": meta.extensions,
                    "state": self._lifespan_state,
                    "user": user,
                }

                queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)

                async def receive() -> dict[str, Any]:
                    return await self._receive_body(body_state)

                async def send(message: dict[str, Any]) -> None:
                    await queue.put(message)

                app_task: asyncio.Task[None] = asyncio.create_task(self._app(scope, receive, send))  # type: ignore[arg-type]  # type: ignore[arg-type]
                try:
                    while True:
                        if app_task.done() and queue.empty():
                            break
                        message = await queue.get()
                        frame = self._format_response_frame(meta.id, message)
                        if frame is None:
                            continue
                        if frame.get("type") == "http.response.start":
                            response_started = True
                        await writer.send(frame)
                finally:
                    try:
                        await app_task
                    except Exception as exc:  # noqa: BLE001 - must catch all ASGI app errors
                        trace_id = self._extract_trace_id(meta.headers)
                        self._log(
                            "error",
                            "request.error",
                            trace_id=trace_id,
                            method=meta.method,
                            path=meta.path,
                            error=str(exc),
                        )
                        if not response_started:
                            await writer.send({
                                "id": meta.id,
                                "type": "http.response.start",
                                "status": 500,
                                "headers": [("content-type", "text/plain")],
                            })
                            await writer.send({
                                "id": meta.id,
                                "type": "http.response.body",
                                "body": b"Internal Server Error",
                                "more_body": False,
                            })
                        return
            finally:
                reset_user(token)
        finally:
            contexts.pop(meta.id, None)

    async def _run_websocket_request(
        self, meta: RequestMeta, body_state: RequestBodyState, writer: "ResponseWriter"
    ) -> None:
        from vertebra.security.auth.state import reset_user, set_user

        claims = meta.auth_claims
        if claims:
            from vertebra.security.auth.models import VertebraUserMixin
            from vertebra.security.auth.proxy import LazyUserProxy

            async def noop_fetcher(user_id: str, ctx: Any) -> Any:
                return VertebraUserMixin(**claims)

            user = LazyUserProxy(VertebraUserMixin, claims, noop_fetcher, context=meta)
        else:
            from vertebra.security.auth.models import AnonymousUser

            user = AnonymousUser()

        token = set_user(user)

        try:
            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
            raw_path = meta.raw_path if meta.raw_path is not None else meta.path.encode()
            query_string = meta.query_string if meta.query_string is not None else meta.query.encode()
            scope = {
                "type": "websocket",
                "asgi": {"version": ASGI_VERSION, "spec_version": ASGI_HTTP_SPEC_VERSION},
                "http_version": meta.http_version,
                "path": meta.path,
                "raw_path": raw_path,
                "query_string": query_string,
                "headers": [(k.lower().encode(), v.encode()) for k, v in meta.headers],
                "scheme": meta.scheme,
                "root_path": meta.root_path,
                "server": meta.server or (self._config.host, self._config.port),
                "client": meta.client or ("127.0.0.1", 0),
                "subprotocols": meta.subprotocols,
                "extensions": meta.extensions,
                "state": self._lifespan_state,
                "user": user,
            }

            async def receive() -> dict[str, Any]:
                return await self._receive_body(body_state)

            async def send(message: dict[str, Any]) -> None:
                await queue.put(message)

            app_task: asyncio.Task[None] = asyncio.create_task(self._app(scope, receive, send))  # type: ignore[arg-type]
            try:
                while True:
                    if app_task.done() and queue.empty():
                        break
                    message = await queue.get()
                    frame = self._format_response_frame(meta.id, message)
                    if frame is None:
                        continue
                    await writer.send(frame)
            finally:
                try:
                    await app_task
                except Exception as exc:  # noqa: BLE001 - must catch all ASGI app errors
                    trace_id = self._extract_trace_id(meta.headers)
                    self._log("error", "websocket.error", trace_id=trace_id, path=meta.path, error=str(exc))
                    await writer.send({
                        "id": meta.id,
                        "type": "websocket.close",
                        "code": 1011,
                        "reason": "Internal Server Error",
                    })
        finally:
            reset_user(token)

    def _format_response_frame(self, request_id: str, message: dict[str, Any]) -> dict[str, Any] | None:
        message_type = message.get("type")
        if message_type == "http.response.start":
            status = int(message.get("status", 500))
            headers = [(key.decode(), value.decode()) for key, value in message.get("headers", [])]
            return {"id": request_id, "type": message_type, "status": status, "headers": headers}
        if message_type == "http.response.body":
            return {
                "id": request_id,
                "type": message_type,
                "body": message.get("body", b""),
                "more_body": bool(message.get("more_body")),
            }
        if message_type == "http.response.pathsend":
            return {"id": request_id, "type": message_type, "path": message.get("path")}
        if message_type == "http.response.zerocopysend":
            file_obj = message.get("file")
            path = getattr(file_obj, "name", None) if file_obj is not None else message.get("path")
            return {
                "id": request_id,
                "type": message_type,
                "path": path,
                "offset": message.get("offset"),
                "count": message.get("count"),
            }
        if message_type == "http.response.early_hint":
            headers = [(key.decode(), value.decode()) for key, value in message.get("headers", [])]
            return {"id": request_id, "type": message_type, "headers": headers}
        if message_type == "http.response.trailers":
            headers = [(key.decode(), value.decode()) for key, value in message.get("headers", [])]
            return {"id": request_id, "type": message_type, "headers": headers}
        if message_type == "websocket.accept":
            headers = [(key.decode(), value.decode()) for key, value in message.get("headers", [])]
            return {"id": request_id, "type": message_type, "headers": headers}
        if message_type == "websocket.send":
            return {"id": request_id, "type": message_type, "bytes": message.get("bytes"), "text": message.get("text")}
        if message_type == "websocket.close":
            return {
                "id": request_id,
                "type": message_type,
                "code": message.get("code"),
                "reason": message.get("reason"),
            }
        return None

    async def _start_lifespan(self) -> None:
        if self._lifespan_task is not None:
            return
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._lifespan_queue = queue
        self._lifespan_startup.clear()
        self._lifespan_shutdown.clear()
        self._lifespan_failed = None
        self._lifespan_receive_called = False
        self._lifespan_supported = None
        self._lifespan_task = asyncio.create_task(self._run_lifespan(queue))
        await queue.put({"type": "lifespan.startup"})
        try:
            await asyncio.wait_for(self._lifespan_startup.wait(), timeout=self._config.timeout)
        except TimeoutError as exc:
            msg = "ASGI lifespan startup timed out"
            raise RuntimeError(msg) from exc
        self._check_lifespan_status()

    def _check_lifespan_status(self) -> None:
        if self._lifespan_supported is False:
            return
        if self._lifespan_failed is not None:
            msg = f"ASGI lifespan startup failed: {self._lifespan_failed}"
            raise RuntimeError(msg)

    async def _shutdown_lifespan(self) -> None:
        if self._lifespan_task is None or self._lifespan_queue is None:
            return
        if self._lifespan_supported is False:
            self._lifespan_task.cancel()
            self._lifespan_task = None
            return
        await self._lifespan_queue.put({"type": "lifespan.shutdown"})
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._lifespan_shutdown.wait(), timeout=self._config.timeout)
        if self._lifespan_task is not None:
            self._lifespan_task.cancel()
            self._lifespan_task = None

    async def _run_lifespan(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        scope = {
            "type": "lifespan",
            "asgi": {"version": ASGI_VERSION, "spec_version": "2.0"},
            "state": self._lifespan_state,
        }

        async def receive() -> dict[str, Any]:
            self._lifespan_receive_called = True
            return await queue.get()

        async def send(message: dict[str, Any]) -> None:
            if not self._lifespan_receive_called:
                msg = "Lifespan send before receive"
                raise RuntimeError(msg)
            message_type = message.get("type")
            if message_type == "lifespan.startup.complete":
                # Inject static mounts if available
                native_enabled = os.environ.get("VERTEBRA_NATIVE", "1") == "1"
                # Check for Litestar
                try:
                    from litestar import Litestar

                    if native_enabled and isinstance(self._app, Litestar):
                        from vertebra.extensions.litestar.native import reflect_static_mounts

                        mounts = reflect_static_mounts(self._app)
                        if mounts:
                            extensions = message.setdefault("extensions", {})
                            extensions["vertebra"] = {"static_mounts": mounts}
                except ImportError:
                    pass

                self._lifespan_supported = True
                self._lifespan_startup.set()
            elif message_type == "lifespan.startup.failed":
                self._lifespan_supported = True
                self._lifespan_failed = str(message.get("message", ""))
                self._lifespan_startup.set()
            elif message_type == "lifespan.shutdown.complete":
                self._lifespan_supported = True
                self._lifespan_shutdown.set()
            elif message_type == "lifespan.shutdown.failed":
                self._lifespan_supported = True
                self._lifespan_failed = str(message.get("message", ""))
                self._lifespan_shutdown.set()

        try:
            await self._app(scope, receive, send)
        except Exception:
            if not self._lifespan_receive_called:
                self._lifespan_supported = False
                self._lifespan_startup.set()
                self._lifespan_shutdown.set()
                return
            raise
        if self._lifespan_supported is None:
            self._lifespan_supported = False
            self._lifespan_startup.set()
            self._lifespan_shutdown.set()


def _is_file_path(path: str) -> bool:
    """Check if the path looks like a file path rather than a module path."""
    return os.path.sep in path or (os.path.altsep and os.path.altsep in path) or path.endswith(".py")


def _import_app(app_path: str) -> AsgiApp:
    """Import an ASGI application from a module path or file path.

    Supports both formats:
    - Module path: "mypackage.module:app" or "mypackage.module:factory()"
    - File path: "path/to/app.py:app" or "path/to/app.py" (auto-detects app/application)
    """
    module_path, _, attr_path = app_path.partition(":")

    # Handle file paths (contain / or \ or end with .py)
    if _is_file_path(module_path):
        # Resolve to absolute path and normalize
        resolved_path = Path(module_path).resolve()

        # Handle .py extension
        if resolved_path.suffix == ".py":
            if not resolved_path.is_file():
                msg = f"Application file not found: {resolved_path}"
                raise FileNotFoundError(msg)
        # Try adding .py extension
        elif resolved_path.with_suffix(".py").is_file():
            resolved_path = resolved_path.with_suffix(".py")
        elif not resolved_path.is_file():
            msg = f"Application file not found: {resolved_path}"
            raise FileNotFoundError(msg)

        # Add parent directory to sys.path for imports within the app
        parent_dir = str(resolved_path.parent)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)

        # Convert to module name (strip .py extension)
        module_name = resolved_path.stem
        module = importlib.import_module(module_name)
    else:
        # Standard module path import
        module = importlib.import_module(module_path)

    # Get the target attribute
    target: Any = module
    if attr_path:
        for attr in attr_path.split("."):
            target = getattr(target, attr)
    else:
        # No attribute specified - try common ASGI app names
        for default_attr in ("app", "application", "create_app"):
            if hasattr(module, default_attr):
                target = getattr(module, default_attr)
                # If it's a factory function, call it
                if default_attr == "create_app" and callable(target):
                    target = target()
                break

    if not callable(target):
        msg = f"ASGI app '{app_path}' is not callable"
        raise TypeError(msg)
    app: AsgiApp = target
    return app


async def _serve_worker(app_path: str, config: WorkerConfig) -> None:
    app = _import_app(app_path)
    worker = AsgiWorker(app, config)
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop_event.set)

    serve_task = asyncio.create_task(worker.serve_forever())
    stop_task = asyncio.create_task(stop_event.wait())
    done, pending = await asyncio.wait([serve_task, stop_task], return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    if serve_task in done:
        await serve_task
        return
    serve_task.cancel()
    await worker.stop()


def _resolve_notify_fd(cli_value: int | None, env_name: str) -> int | None:
    """Resolve notify fd from CLI arg or environment variable.

    Priority: CLI arg > env var
    Supports both ProcessManager (CLI args) and Rust supervisor (env vars).
    """
    if cli_value is not None:
        return cli_value

    env_value = os.environ.get(env_name)
    if env_value:
        try:
            return int(env_value)
        except ValueError:
            pass

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Vertebra ASGI worker process")
    parser.add_argument("--app", required=True, help="ASGI app path (module:attr)")
    parser.add_argument("--name", default="vertebra-asgi")
    parser.add_argument("--worker-id", default=None)
    parser.add_argument("--slot-size", type=int, default=65536)
    parser.add_argument("--slots", type=int, default=1024)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--timeout", type=float, default=5.0)
    # External notifier fds inherited from ProcessManager or Rust supervisor
    parser.add_argument("--notify-read-fd", type=int, default=None)
    parser.add_argument("--notify-write-fd", type=int, default=None)
    args = parser.parse_args()

    # Resolve notify fds: CLI args take priority, then env vars (for Rust supervisor)
    # VERTEBRA_REQ_NOTIFY_FD: Rust→Python request notification (Python reads this)
    # VERTEBRA_RESP_NOTIFY_FD: Python→Rust response notification (Python writes this)
    notify_read_fd = _resolve_notify_fd(args.notify_read_fd, "VERTEBRA_REQ_NOTIFY_FD")
    notify_write_fd = _resolve_notify_fd(args.notify_write_fd, "VERTEBRA_RESP_NOTIFY_FD")

    # Resolve SHM FDs (managed mode)
    req_shm_fd = _resolve_notify_fd(None, "VERTEBRA_REQ_SHM_FD")
    resp_shm_fd = _resolve_notify_fd(None, "VERTEBRA_RESP_SHM_FD")
    shm_size = _resolve_notify_fd(None, "VERTEBRA_SHM_SIZE")  # Reuse int resolver

    config = WorkerConfig(
        name=args.name,
        worker_id=args.worker_id,
        slot_size=args.slot_size,
        slots=args.slots,
        host=args.host,
        port=args.port,
        timeout=args.timeout,
        notify_read_fd=notify_read_fd,
        notify_write_fd=notify_write_fd,
        req_shm_fd=req_shm_fd,
        resp_shm_fd=resp_shm_fd,
        shm_size=shm_size,
    )
    asyncio.run(_serve_worker(args.app, config))


if __name__ == "__main__":
    main()


__all__ = ("HEALTH_PATH", "AsgiRequest", "AsgiResponse", "AsgiWorker", "WorkerConfig")
