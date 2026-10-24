"""AsgiRunner: unified ASGI 3.0 application driver and batch runner."""

import asyncio
import logging
from collections.abc import Awaitable, Buffer, Callable, Iterable
from typing import Any

import msgspec

from vertebra._vertebra import (
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
)
from vertebra.server._loops import EventLoopPool

__all__ = (
    "FLAG_FINAL",
    "FLAG_STREAMING",
    "FLAG_TRAILERS",
    "AsgiRunner",
)

logger = logging.getLogger(__name__)

FLAG_STREAMING: int = 1 << 0
FLAG_FINAL: int = 1 << 1
FLAG_TRAILERS: int = 1 << 5


class _InlineAsgiRequest(msgspec.Struct, frozen=True, gc=False):
    """Lightweight request carrier used by AsgiRunner.handle_path."""

    method: str = "GET"
    path: str = "/"
    query: str | None = None
    headers: Any = None
    body: bytes | None = None
    request_id: int = 0


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
        self._app = app
        self._fastpath = self._resolve_fastpath(app)
        self._dispatch_app: Callable[..., Any] = (
            self._fastpath.wrap_asgi(app) if (self._fastpath is not None and on_chunk is None) else app
        )
        self._loop = loop
        self._owns_loop = loop is None
        self._owned_pool: EventLoopPool | None = None
        self._mount_prefix = mount_prefix.rstrip("/")
        self._lifespan_state: dict[str, Any] = lifespan_state if lifespan_state is not None else {}
        self._on_chunk = on_chunk
        self._last_trailer_frames: list[bytes] = []

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
            if self._fastpath is not None and self._on_chunk is None:
                raw_req_body = body if isinstance(body, bytes) else bytes(body)
                direct = await self._fastpath.dispatch(scope, receive, send, body=raw_req_body)
                if direct is not None:
                    status_code, encoded_headers, raw_body = direct
                    decoded_headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in encoded_headers]
                    return AsgiResponse(
                        status=status_code,
                        headers=decoded_headers,
                        body=raw_body,
                        request_id=request_id,
                    )
                if send.started:
                    resp = send.response
                    if resp.trailers:
                        self._emit_trailers(resp, request_id)
                    return resp

            await self._app(scope, receive, send)
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

        resp = send.response
        if resp.trailers:
            self._emit_trailers(resp, request_id)
        return resp

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
            return future.result()
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
        direct = getattr(app, "__vertebra_fastpath__", None)
        if direct is not None and callable(getattr(direct, "dispatch", None)):
            return direct
        state = getattr(app, "state", None)
        if state is not None and hasattr(state, "get"):
            candidate = state.get("vertebra_fastpath_bypass")
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

    async def _execute_and_submit(self, request: Any, request_id: int) -> None:
        try:
            resp = await self.run_single(request)
            native_resp = self._to_native_response(resp, request_id)
            _native_submit_response(native_resp)
        except Exception:
            logger.exception("Unhandled error in spawned ASGI task (request_id=%d)", request_id)
            try:
                err_resp = _NativeResponse(500, b"Internal Server Error", None, request_id)
                _native_submit_response(err_resp)
            except Exception:
                logger.exception("Failed to submit error response for request_id=%d", request_id)

    def _to_native_response(self, resp: AsgiResponse, request_id: int) -> _NativeResponse:
        """Convert an ``AsgiResponse`` to a Rust ``Response`` without extra body copies."""
        hdr_pairs: list[tuple[str, str]] = list(resp.headers)
        req_id = resp.request_id or request_id
        if resp.file_path is not None:
            try:
                return _NativeResponse.from_file(
                    resp.file_path,
                    status=resp.status,
                    headers=hdr_pairs or None,
                    request_id=req_id,
                )
            except OSError:
                return _NativeResponse(resp.status, bytes(resp.body), hdr_pairs or None, req_id)
        if resp.zero_copy_buffer is not None:
            return _NativeResponse.from_buffer(
                resp.status,
                resp.zero_copy_buffer,
                hdr_pairs or None,
                req_id,
            )
        return _NativeResponse(
            resp.status,
            bytes(resp.body),
            hdr_pairs or None,
            req_id,
        )
