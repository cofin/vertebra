"""AsgiRunner: unified ASGI 3.0 application driver, lifespan state machine, and batch runner."""

import asyncio
import contextlib
import logging
import math
import os
from collections.abc import Awaitable, Buffer, Callable, Iterable, Mapping
from typing import Any, cast

import msgspec

from vertebra._vertebra import (
    STREAM_FLAG_FINAL,
    STREAM_FLAG_STREAMING,
    STREAM_FLAG_TRAILERS,
    AsgiScope,
    RequestBatch,
    drive_asgi_batch,
)
from vertebra._vertebra import (
    Request as _NativeRequest,
)
from vertebra._vertebra import (
    Response as _NativeResponse,
)
from vertebra._vertebra import (
    submit_raw_response as _native_submit_raw_response,
)
from vertebra._vertebra import (
    submit_response as _native_submit_response,
)
from vertebra._vertebra import (
    submit_stream_frame as _native_submit_stream_frame,
)
from vertebra.server._asgi._queues import (
    DEFAULT_STREAM_HIGH_WATERMARK,
    DEFAULT_STREAM_LOW_WATERMARK,
    DEFAULT_STREAM_QUEUE_MAXSIZE,
    AsgiReceive,
    AsgiResponse,
    AsgiSend,
    AsgiStreamQueuePair,
    AsgiStreamReceive,
    AsgiStreamSend,
    ChunkCallback,
    StreamSubmitter,
)
from vertebra.server._fastpath import PayloadTooLargeError, attach_framework_fastpath
from vertebra.server._fastpath_core import get_attached_fastpath
from vertebra.server._loops import EventLoopPool
from vertebra.typing import ImproperConfigurationError

__all__ = (
    "FLAG_FINAL",
    "FLAG_STREAMING",
    "FLAG_TRAILERS",
    "AsgiLifespan",
    "AsgiRunner",
)

logger = logging.getLogger(__name__)

FLAG_STREAMING: int = STREAM_FLAG_STREAMING
FLAG_FINAL: int = STREAM_FLAG_FINAL
FLAG_TRAILERS: int = STREAM_FLAG_TRAILERS


class _InlineAsgiRequest(msgspec.Struct, frozen=True, gc=False):
    """Lightweight request carrier used by AsgiRunner.handle_path."""

    method: str = "GET"
    path: str = "/"
    query: str | None = None
    headers: Any = None
    body: bytes | None = None
    request_id: int = 0


class AsgiLifespan:
    """ASGI 3.0 lifespan state machine driving startup/shutdown hooks and scope['state']."""

    __slots__ = (
        "_app",
        "_loop",
        "_owns_loop",
        "_receive_queue",
        "_received_startup",
        "_shutdown_complete",
        "_startup_complete",
        "_startup_error",
        "_state",
        "_supported",
        "_task",
        "_timeout",
    )

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        state: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> None:
        self._app = app
        self._loop = loop
        self._owns_loop = False
        self._state: dict[str, Any] = dict(state) if state is not None else {}
        if timeout is None:
            raw_timeout = os.environ.get("VERTEBRA_LIFESPAN_TIMEOUT", "").strip()
            if not raw_timeout:
                self._timeout = 30.0
            else:
                try:
                    parsed = float(raw_timeout)
                except ValueError as exc:
                    msg = f"VERTEBRA_LIFESPAN_TIMEOUT must be a positive finite number of seconds, got {raw_timeout!r}"
                    raise ImproperConfigurationError(msg) from exc
                if not math.isfinite(parsed) or parsed <= 0.0:
                    msg = f"VERTEBRA_LIFESPAN_TIMEOUT must be a positive finite number of seconds, got {raw_timeout!r}"
                    raise ImproperConfigurationError(msg)
                self._timeout = parsed
        else:
            self._timeout = timeout
        self._supported = False
        self._received_startup = False
        self._startup_error: BaseException | None = None
        self._receive_queue: asyncio.Queue[dict[str, Any]] | None = None
        self._startup_complete: asyncio.Event | None = None
        self._shutdown_complete: asyncio.Event | None = None
        self._task: asyncio.Task[Any] | None = None

    @property
    def state(self) -> dict[str, Any]:
        """Return the shared lifespan state dictionary."""
        return self._state

    @property
    def supported(self) -> bool:
        """Return True if the application completed lifespan.startup.complete."""
        return self._supported

    async def startup(self) -> dict[str, Any]:
        """Execute the ASGI lifespan.startup handshake on the active event loop."""
        loop = asyncio.get_running_loop()
        self._receive_queue = asyncio.Queue()
        self._startup_complete = asyncio.Event()
        self._shutdown_complete = asyncio.Event()
        self._received_startup = False
        self._startup_error = None
        self._supported = False

        await self._receive_queue.put({"type": "lifespan.startup"})
        self._task = loop.create_task(self._run_app())
        try:
            await asyncio.wait_for(self._startup_complete.wait(), timeout=self._timeout)
        except BaseException:
            if self._task is not None and not self._task.done():
                self._task.cancel()
                with contextlib.suppress(BaseException):
                    await self._task
            raise
        if self._startup_error is not None:
            if self._task is not None and not self._task.done():
                self._task.cancel()
                with contextlib.suppress(BaseException):
                    await self._task
            raise self._startup_error
        if not self._supported and self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task
        return self._state

    async def shutdown(self) -> None:
        """Execute the ASGI lifespan.shutdown handshake if startup succeeded."""
        if not self._supported or self._task is None or self._receive_queue is None or self._shutdown_complete is None:
            if self._task is not None and not self._task.done():
                self._task.cancel()
                with contextlib.suppress(BaseException):
                    await self._task
            return
        if not self._task.done():
            await self._receive_queue.put({"type": "lifespan.shutdown"})
            await asyncio.wait_for(self._shutdown_complete.wait(), timeout=self._timeout)
        if not self._task.done():
            await asyncio.wait_for(self._task, timeout=self._timeout)

    async def __aenter__(self) -> dict[str, Any]:
        """Enter the lifespan context asynchronously and return the state dictionary."""
        return await self.startup()

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the lifespan context asynchronously."""
        del exc_type, exc, tb
        await self.shutdown()

    def __enter__(self) -> dict[str, Any]:
        """Enter the lifespan context synchronously on the configured event loop."""
        loop = self._ensure_loop()
        if loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(self.startup(), loop)
            return fut.result(timeout=self._timeout + 1.0)
        return loop.run_until_complete(self.startup())

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        """Exit the lifespan context synchronously on the configured event loop."""
        del exc_type, exc, tb
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            if loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(self.shutdown(), loop)
                fut.result(timeout=self._timeout)
            else:
                loop.run_until_complete(self.shutdown())
        finally:
            if self._owns_loop and not loop.is_running():
                loop.close()
                self._loop = None
                self._owns_loop = False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._owns_loop = True
        return self._loop

    async def _run_app(self) -> None:
        receive_queue = self._receive_queue
        startup_complete = self._startup_complete
        shutdown_complete = self._shutdown_complete
        if receive_queue is None or startup_complete is None or shutdown_complete is None:
            msg = "AsgiLifespan queues are not initialized"
            raise RuntimeError(msg)

        scope: dict[str, Any] = {
            "type": "lifespan",
            "asgi": {"version": "3.0", "spec_version": "2.0"},
            "state": self._state,
        }

        async def receive() -> dict[str, Any]:
            msg = await receive_queue.get()
            if msg.get("type") == "lifespan.startup":
                self._received_startup = True
            return msg

        async def send(message: Mapping[str, Any]) -> None:
            msg_type = str(message.get("type", ""))
            if msg_type == "lifespan.startup.complete":
                scope_state = scope.get("state")
                if isinstance(scope_state, Mapping) and scope_state is not self._state:
                    self._state.update(scope_state)
                self._supported = True
                startup_complete.set()
            elif msg_type == "lifespan.startup.failed":
                err_msg = str(message.get("message", "") or "ASGI lifespan startup failed")
                self._startup_error = RuntimeError(err_msg)
                startup_complete.set()
            elif msg_type == "lifespan.shutdown.complete":
                shutdown_complete.set()
            elif msg_type == "lifespan.shutdown.failed":
                err_msg = str(message.get("message", "") or "ASGI lifespan shutdown failed")
                logger.error("ASGI lifespan shutdown failed: %s", err_msg)
                shutdown_complete.set()
            elif not startup_complete.is_set():
                self._supported = False
                startup_complete.set()

        try:
            await self._app(scope, receive, send)
        except BaseException as exc:
            if not startup_complete.is_set():
                if self._received_startup:
                    self._startup_error = exc
                else:
                    self._supported = False
                startup_complete.set()
            elif self._supported and not shutdown_complete.is_set():
                logger.exception("Unhandled exception during ASGI lifespan")
        finally:
            if not startup_complete.is_set():
                startup_complete.set()
            if not shutdown_complete.is_set():
                shutdown_complete.set()


class AsgiRunner:
    """Drives ASGI 3.0 applications across single, batch, and streaming lifecycles."""

    __slots__ = (
        "_app",
        "_dispatch_app",
        "_fastpath",
        "_last_trailer_frames",
        "_lifespan_state",
        "_loop",
        "_mount_prefix",
        "_on_chunk",
        "_owned_pool",
        "_owns_loop",
    )

    def __init__(
        self,
        app: Callable[..., Awaitable[Any]],
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        mount_prefix: str = "",
        lifespan_state: dict[str, Any] | None = None,
        on_chunk: ChunkCallback | None = None,
    ) -> None:
        self._mount_prefix = mount_prefix.rstrip("/")
        self._app = app
        wrapped_app = (
            attach_framework_fastpath(app, mount_prefix=self._mount_prefix, protocol="asgi")
            if on_chunk is None
            else app
        )
        self._fastpath = self._resolve_fastpath(wrapped_app) or self._resolve_fastpath(app)
        if self._fastpath is not None and self._mount_prefix and hasattr(self._fastpath, "_mount_prefix"):
            self._fastpath._mount_prefix = self._mount_prefix
        self._dispatch_app: Callable[..., Any] = (
            self._fastpath.wrap_asgi(app)
            if (
                self._fastpath is not None
                and on_chunk is None
                and not getattr(wrapped_app, "__vertebra_fastpath_attached__", False)
            )
            else wrapped_app
        )
        self._loop = loop
        self._owns_loop = loop is None
        self._owned_pool: EventLoopPool | None = None
        self._lifespan_state: dict[str, Any] = lifespan_state if lifespan_state is not None else {}
        self._on_chunk = on_chunk
        self._last_trailer_frames: list[bytes] = []

    @property
    def __vertebra_fastpath_attached__(self) -> bool:
        """Return True when a zero-copy FastPath wrapper is attached to this runner."""
        return bool(
            getattr(self._dispatch_app, "__vertebra_fastpath_attached__", False)
            or getattr(self._app, "__vertebra_fastpath_attached__", False)
            or self._fastpath is not None
        )

    @property
    def dispatch_app(self) -> Callable[..., Any]:
        """Return the ASGI callable wired with any attached fastpath bypass."""
        return self._dispatch_app

    @property
    def lifespan_state(self) -> dict[str, Any]:
        """Return the shared ASGI lifespan state dictionary."""
        return self._lifespan_state

    @property
    def mount_prefix(self) -> str:
        """Return the normalized mount prefix."""
        return self._mount_prefix

    @property
    def last_trailer_frames(self) -> list[bytes]:
        """Return the most recent encoded trailer frames emitted via submit_raw_response."""
        return self._last_trailer_frames

    async def run_single(self, request: Any) -> AsgiResponse:
        """Drive ``self._app`` for a single request and return its ``AsgiResponse``."""
        scope = self._build_scope(request)
        body = self._extract_body(request)
        request_id: int = getattr(request, "request_id", 0)

        receive = AsgiReceive(body=body)
        send = AsgiSend(request_id=request_id, on_chunk=self._on_chunk)

        try:
            resp = await self._drive_asgi_once(scope, body, receive, send, request_id)
            if resp is None:
                msg = "Unary ASGI execution returned no response"
                raise RuntimeError(msg)
            return resp
        except PayloadTooLargeError:
            return self._payload_too_large_response(send, request_id)
        except Exception:
            logger.exception("Unhandled exception in ASGI application for %s", getattr(request, "path", "/"))
            if not send.started:
                return AsgiResponse(
                    status=500,
                    headers=[("content-type", "text/plain")],
                    body=b"Internal Server Error",
                    request_id=request_id,
                )
            return AsgiResponse(
                status=500,
                headers=send.response.headers if send.started else [],
                body=b"",
                request_id=request_id,
            )

    async def handle_path(
        self,
        path: str,
        *,
        method: str = "GET",
        headers: Any = None,
        body: bytes | None = None,
        query: str | None = None,
        request_id: int = 0,
    ) -> AsgiResponse:
        """Convenience helper that builds a request from ``path`` and calls ``run_single``."""
        req = _InlineAsgiRequest(
            method=method,
            path=path,
            query=query,
            headers=headers,
            body=body,
            request_id=request_id,
        )
        return await self.run_single(req)

    async def run_batch(self, requests: list[Any]) -> list[AsgiResponse]:
        """Drive ``self._app`` concurrently across ``requests``."""
        if not requests:
            return []
        tasks = [self.run_single(req) for req in requests]
        return list(await asyncio.gather(*tasks))

    def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run a batch of requests synchronously, returning their ``AsgiResponse`` objects."""
        if not requests:
            return []
        loop = self._ensure_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self.run_batch(requests), loop)
            return future.result(timeout=10.0)
        return loop.run_until_complete(self.run_batch(requests))

    def spawn_single(self, request: Any) -> Any:
        """Spawn a single request as a background task on the event loop."""
        loop = self._ensure_loop()
        req_id: int = getattr(request, "request_id", 0)
        return asyncio.run_coroutine_threadsafe(self._execute_and_submit(request, req_id), loop)

    def spawn_batch(self, requests: RequestBatch | Iterable[Any]) -> list[Any]:
        """Drive a batch of ASGI requests via Rust drive_asgi_batch or fallback tasks."""
        if self._on_chunk is None:
            if isinstance(requests, RequestBatch):
                if len(requests) == 0:
                    return []
                loop = self._ensure_loop()
                drive_asgi_batch(
                    self._dispatch_app,
                    requests,
                    loop_obj=loop,
                    root_path=self._mount_prefix,
                    lifespan_state=self._lifespan_state or None,
                )
                return []

            req_list = requests if isinstance(requests, list | tuple) else list(requests)
            if not req_list:
                return []
            if isinstance(req_list[0], _NativeRequest):
                loop = self._ensure_loop()
                drive_asgi_batch(
                    self._dispatch_app,
                    req_list,
                    loop_obj=loop,
                    root_path=self._mount_prefix,
                    lifespan_state=self._lifespan_state or None,
                )
                return []
            return [self.spawn_single(req) for req in req_list]

        req_items = requests if isinstance(requests, list | tuple) else list(requests)
        if not req_items:
            return []
        return [self.spawn_single(req) for req in req_items]

    def create_stream_queues(
        self,
        maxsize: int = DEFAULT_STREAM_QUEUE_MAXSIZE,
        *,
        high_watermark: int = DEFAULT_STREAM_HIGH_WATERMARK,
        low_watermark: int = DEFAULT_STREAM_LOW_WATERMARK,
    ) -> AsgiStreamQueuePair:
        """Create a bounded AsgiStreamQueuePair for SSE or WebSocket streaming."""
        return AsgiStreamQueuePair(
            maxsize=maxsize,
            high_watermark=high_watermark,
            low_watermark=low_watermark,
        )

    async def run_stream_session(
        self,
        scope_or_request: Any,
        queues: AsgiStreamQueuePair | AsgiStreamReceive,
        send: AsgiStreamSend | None = None,
    ) -> None:
        """Run the wrapped ASGI application against a bounded AsgiStreamQueuePair or stream queues."""
        if isinstance(queues, AsgiStreamQueuePair):
            scope_arg = (
                scope_or_request
                if isinstance(scope_or_request, dict | AsgiScope)
                else self._build_scope(scope_or_request)
            )
            await self._app(scope_arg, queues.receive, queues.send)
            return

        if send is None:
            msg = "send queue is required when queues is an AsgiStreamReceive"
            raise ValueError(msg)
        scope = self._build_scope(scope_or_request)
        try:
            await self._app(scope, queues, send)
        except Exception:
            logger.exception(
                "Unhandled exception in streaming ASGI session for %s", getattr(scope_or_request, "path", "/")
            )
            if not send.started:
                await send(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [(b"content-type", b"text/plain")],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": b"Internal Server Error",
                        "more_body": False,
                    }
                )

    def close(self) -> None:
        """Shut down the owned event loop pool if one was created."""
        if self._owned_pool is not None:
            self._owned_pool.stop()
            self._owned_pool = None
            self._loop = None

    def _build_scope(self, request: Any) -> AsgiScope:
        """Build a native ``AsgiScope`` from a native ``Request`` or a Python test stub."""
        if isinstance(request, _NativeRequest):
            scope = request.asgi_scope(self._mount_prefix or None)
        else:
            asgi_scope_fn = getattr(request, "asgi_scope", None)
            if callable(asgi_scope_fn):
                scope = asgi_scope_fn(self._mount_prefix or None)
            else:
                scope = self._build_scope_from_stub(request)
        if self._lifespan_state:
            scope["state"] = self._lifespan_state.copy()
        return scope

    def _build_scope_from_stub(self, request: Any) -> AsgiScope:
        """Build an ``AsgiScope`` from a duck-typed request object."""
        method: str = getattr(request, "method", "GET")
        full_path: str = getattr(request, "path", "/")
        prefix = self._mount_prefix
        if prefix and full_path.startswith(prefix):
            stripped_path = full_path[len(prefix) :] or "/"
            root_path = prefix
        else:
            stripped_path = full_path
            root_path = prefix

        query: str = getattr(request, "query", None) or ""
        http_version: str = getattr(request, "http_version", "1.1")
        scheme: str = getattr(request, "scheme", "http")

        raw_headers = getattr(request, "headers", None)
        header_pairs: list[tuple[bytes, bytes]] = []
        if raw_headers is not None:
            asgi_items = getattr(raw_headers, "asgi_items", None)
            if callable(asgi_items):
                header_pairs = list(asgi_items())
            else:
                items = raw_headers.items() if hasattr(raw_headers, "items") else raw_headers
                for k, v in items:
                    kb = k.lower().encode("latin-1") if isinstance(k, str) else bytes(k).lower()
                    vb = v.encode("latin-1") if isinstance(v, str) else bytes(v)
                    header_pairs.append((kb, vb))

        scope = AsgiScope.from_parts(
            method=method,
            path=stripped_path,
            query_string=query,
            headers=header_pairs,
            root_path=root_path,
            http_version=http_version,
            scheme=scheme,
        )

        raw_path_attr = getattr(request, "raw_path", None)
        if isinstance(raw_path_attr, bytes):
            if prefix and raw_path_attr.startswith(prefix.encode("latin-1")):
                rest = raw_path_attr[len(prefix) :] or b"/"
                scope["raw_path"] = rest
            else:
                scope["raw_path"] = raw_path_attr

        raw_query_attr = getattr(request, "raw_query", None)
        if isinstance(raw_query_attr, bytes) and not query:
            scope["query_string"] = raw_query_attr

        client = getattr(request, "client", None)
        if client is not None:
            scope["client"] = client
        server = getattr(request, "server", None)
        if server is not None:
            scope["server"] = server
        return scope

    @staticmethod
    def _resolve_fastpath(app: Any) -> Any:
        """Return an attached fastpath bypass dispatcher if exposed by ``app``."""
        for candidate_app in (app, getattr(app, "_inner", None)):
            if candidate_app is None:
                continue
            cand_state = getattr(candidate_app, "state", None)
            if cand_state is not None and hasattr(cand_state, "get"):
                plugin = cand_state.get("vertebra_plugin")
                ensure_fn = getattr(plugin, "ensure_finalized", None)
                if callable(ensure_fn):
                    ensure_fn(candidate_app)

        candidate = get_attached_fastpath(app)
        if candidate is not None and callable(getattr(candidate, "dispatch", None)):
            return candidate
        return None

    @staticmethod
    def _extract_body(request: Any) -> Buffer:
        """Extract a zero-copy slab view when available, otherwise fall back to ``body``."""
        if isinstance(request, _NativeRequest):
            try:
                return request.body_view
            except BufferError:
                return request.body or b""
        body: bytes | memoryview | None = getattr(request, "body", None)
        return body if body is not None else b""

    def _emit_trailers(self, resp: AsgiResponse, request_id: int) -> None:
        """Encode and submit trailing HTTP headers via ``submit_raw_response``."""
        req_id = resp.request_id or request_id
        trailer_pairs = [
            (
                k.decode("latin-1") if isinstance(k, bytes) else str(k),
                v.decode("latin-1") if isinstance(v, bytes) else str(v),
            )
            for k, v in resp.trailers
        ]
        self._last_trailer_frames = _native_submit_raw_response(
            req_id,
            resp.status,
            trailer_pairs,
            b"",
            FLAG_TRAILERS | FLAG_FINAL,
        )

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._owned_pool = EventLoopPool(size=1)
            self._loop = self._owned_pool.primary_loop
        return self._loop

    async def _execute_and_submit(
        self,
        request: Any,
        request_id: int,
        *,
        stream_submitter: StreamSubmitter | None = None,
        unary_submitter: Callable[[_NativeResponse], None] | None = None,
    ) -> None:
        active_stream = _native_submit_stream_frame if stream_submitter is None else stream_submitter
        active_unary = _native_submit_response if unary_submitter is None else unary_submitter
        req_id = int(getattr(request, "request_id", 0)) or request_id
        scope = self._build_scope(request)
        body = self._extract_body(request)
        disconnect_event = asyncio.Event()
        receive = AsgiReceive(body=body, disconnect_event=disconnect_event)

        def _forward_stream(
            rid: int,
            status: int,
            flags: int,
            headers: list[tuple[bytes, bytes]],
            chunk: bytes,
        ) -> None:
            try:
                active_stream(rid, status, flags, headers, chunk)
            except ConnectionResetError:
                disconnect_event.set()
                raise

        send = AsgiSend(
            request_id=req_id,
            on_chunk=self._on_chunk,
            stream_submitter=_forward_stream,
        )
        try:
            resp_or_none = await self._drive_asgi_once(scope, body, receive, send, req_id, allow_stream=True)
        except ConnectionResetError:
            disconnect_event.set()
            return
        except asyncio.CancelledError:
            if send.streamed:
                send.finish_stream(error=True)
            raise
        except PayloadTooLargeError:
            if send.streamed:
                send.finish_stream(error=True)
                return
            active_unary(self._to_native_response(self._payload_too_large_response(send, req_id), req_id))
            return
        except Exception:
            logger.exception("Unhandled error in spawned ASGI task (request_id=%d)", req_id)
            if send.streamed:
                send.finish_stream(error=True)
                return
            self._submit_fallback_500(active_unary, req_id)
            return

        if resp_or_none is None:
            return
        try:
            active_unary(self._to_native_response(resp_or_none, req_id))
        except Exception:
            logger.exception("Unhandled error finalizing spawned ASGI response (request_id=%d)", req_id)
            self._submit_fallback_500(active_unary, req_id)

    async def _drive_asgi_once(
        self,
        scope: Any,
        body: Any,
        receive: AsgiReceive,
        send: AsgiSend,
        request_id: int,
        *,
        allow_stream: bool = False,
    ) -> AsgiResponse | None:
        if self._fastpath is not None and self._on_chunk is None:
            raw_req_body = body if isinstance(body, bytes) else bytes(body)
            direct = await self._fastpath.dispatch(scope, receive, send, body=raw_req_body)
            if direct is not None:
                status_code, encoded_headers, raw_body = direct
                decoded_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in encoded_headers]
                zc_buf = raw_body if isinstance(raw_body, memoryview) else None
                return AsgiResponse(
                    status=status_code,
                    headers=decoded_headers,
                    body=raw_body,
                    request_id=request_id,
                    zero_copy_buffer=zc_buf,
                    raw_headers=encoded_headers,
                )
            if allow_stream and send.streamed:
                send.finish_stream()
                return None
            if send.started:
                resp = send.response
                if resp.trailers:
                    self._emit_trailers(resp, request_id)
                return resp

        await self._app(scope, receive, send)
        if allow_stream and send.streamed:
            send.finish_stream()
            return None
        resp = send.response
        if resp.trailers:
            self._emit_trailers(resp, request_id)
        return resp

    @staticmethod
    def _payload_too_large_response(send: AsgiSend, request_id: int) -> AsgiResponse:
        if not send.started:
            return AsgiResponse(
                status=413,
                headers=[("content-type", "text/plain; charset=utf-8"), ("content-length", "17")],
                body=b"Payload Too Large",
                request_id=request_id,
                raw_headers=[(b"content-type", b"text/plain; charset=utf-8"), (b"content-length", b"17")],
            )
        return send.response

    @staticmethod
    def _submit_fallback_500(active_unary: Callable[[_NativeResponse], None], request_id: int) -> None:
        try:
            active_unary(
                _NativeResponse(
                    500,
                    b"Internal Server Error",
                    [("content-type", "text/plain")],
                    request_id,
                )
            )
        except Exception:
            logger.exception("Failed to submit error response for request_id=%d", request_id)

    def _to_native_response(self, resp: AsgiResponse, request_id: int) -> _NativeResponse:
        """Convert an ``AsgiResponse`` to a Rust ``Response`` without extra body copies."""
        hdr_arg: Any = resp.raw_headers if resp.raw_headers else (list(resp.headers) or None)
        req_id = resp.request_id or request_id
        if resp.file_path is not None:
            try:
                return _NativeResponse.from_file(
                    resp.file_path,
                    status=resp.status,
                    headers=cast("Any", hdr_arg),
                    request_id=req_id,
                )
            except OSError:
                return _NativeResponse(resp.status, bytes(resp.body), cast("Any", hdr_arg), req_id)
        if resp.zero_copy_buffer is not None:
            return _NativeResponse.from_buffer(
                resp.status,
                resp.zero_copy_buffer,
                cast("Any", hdr_arg),
                req_id,
            )
        return _NativeResponse(
            resp.status,
            bytes(resp.body),
            cast("Any", hdr_arg),
            req_id,
        )
