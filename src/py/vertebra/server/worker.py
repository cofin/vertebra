"""ASGI worker harness for Bun IPC bridge."""

import argparse
import asyncio
import contextlib
import importlib
import os
import signal
from collections.abc import Awaitable, Callable
from typing import Any

import msgspec

from vertebra._vertebra import SpscRing
from vertebra.serializers import MessagePackCodec


class WorkerConfig(msgspec.Struct, frozen=True):
    """Configuration for an ASGI worker process."""

    name: str = "vertebra-asgi"
    worker_id: str | None = None
    slot_size: int = 65536
    slots: int = 1024
    timeout: float = 5.0
    host: str = "127.0.0.1"
    port: int = 8000


class AsgiRequest(msgspec.Struct):
    id: str
    method: str
    path: str
    query: str
    headers: list[tuple[str, str]]
    body: bytes = b""
    scheme: str = "http"
    http_version: str = msgspec.field(default="1.1", name="httpVersion")
    raw_path: bytes | None = msgspec.field(default=None, name="rawPath")
    query_string: bytes | None = msgspec.field(default=None, name="queryString")
    root_path: str = msgspec.field(default="", name="rootPath")
    server: tuple[str, int | None] | None = None
    client: tuple[str, int | None] | None = None
    extensions: dict[str, dict[str, Any]] = msgspec.field(default_factory=dict)
    path_params: dict[str, str] = msgspec.field(default_factory=dict)


class AsgiResponse(msgspec.Struct):
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
    request: SpscRing
    response: SpscRing


class ResponseWriter:
    def __init__(self, ring: SpscRing, codec: MessagePackCodec, timeout_ms: int) -> None:
        self._ring = ring
        self._codec = codec
        self._timeout_ms = timeout_ms

    async def send(self, frame: dict[str, Any]) -> None:
        payload = self._codec.encode(frame)
        self._ring.send_timeout(payload, 0, self._timeout_ms)


AsgiApp = Callable[
    [dict[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]

HEALTH_PATH = "/__vertebra__/health"
ASGI_VERSION = "3.0"
ASGI_HTTP_SPEC_VERSION = "2.5"


class AsgiWorker:
    def __init__(self, app: AsgiApp, config: WorkerConfig | None = None) -> None:
        self._app = app
        self._config = config or WorkerConfig()
        self._codec = MessagePackCodec()
        self._control: SpscRing | None = None
        self._clients: dict[str, "ClientState"] = {}
        self._running = False
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
        from litestar import Litestar

        if native_enabled and isinstance(self._app, Litestar):
            from vertebra.integration.litestar import reflect_routes
            from vertebra.integration.litestar.compiler import PipelineCompiler

            routes = reflect_routes(self._app)
            compiler = PipelineCompiler()
            for route in routes:
                if not route.is_websocket:
                    runner = compiler.compile_route(next(h for h in self._app.route_handlers if h.fn == route.handler))
                    self._native_runners[route.handler_id] = runner

        control_name = self._control_ring_name()
        self._control = SpscRing.create_named(control_name, self._config.slot_size, self._config.slots)

    async def _handle_frame(
        self, message: dict[str, Any], contexts: dict[str, RequestContext], writer: "ResponseWriter"
    ) -> None:
        frame_type = message.get("type")
        if frame_type == "direct.http":
            await self._handle_direct_http(message, writer)
            return
        if frame_type in ("ws.open", "ws.message", "ws.close"):
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

    async def _handle_direct_websocket(self, message: dict[str, Any], writer: "ResponseWriter") -> None:
        handler_id = message.get("handler_id")
        if handler_id is None:
            return

        runner = self._native_runners.get(handler_id)
        if runner is None:
            return

        class VertebraWebSocket:
            def __init__(self, msg, w) -> None:
                self._msg = msg
                self._w = w

            def receive_text(self):
                return self._msg.get("text")

            async def send_text(self, data) -> None:
                await self._w.send({"id": self._msg.get("id"), "type": "ws.message", "text": data})

        socket = VertebraWebSocket(message, writer)
        await runner(socket)

    async def _handle_direct_http(self, message: dict[str, Any], writer: "ResponseWriter") -> None:
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
                await self._start_request(meta, message.get("body"), {}, writer)
                return

            await writer.send({"id": request_id, "type": "http.response.start", "status": 404, "headers": []})
            await writer.send({
                "id": request_id,
                "type": "http.response.body",
                "body": b"Native runner not found",
                "more_body": False,
            })
            return

        from vertebra.integration.litestar import VertebraRequest

        conn = VertebraRequest(message)

        try:
            result = await runner(conn)

            import json

            body = json.dumps(result).encode()

            await writer.send({
                "id": request_id,
                "type": "http.response.start",
                "status": 200,
                "headers": [("content-type", "application/json")],
            })
            await writer.send({"id": request_id, "type": "http.response.body", "body": body, "more_body": False})
        except Exception as exc:
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
        pass  # Placeholder for actual logging implementation

    async def _handle_unary_request(
        self, message: dict[str, Any], contexts: dict[str, RequestContext], writer: "ResponseWriter"
    ) -> None:
        """Handle unary request format (single message with inline body)."""
        try:
            request = msgspec.convert(message, AsgiRequest)
        except Exception:  # noqa: BLE001 - defensive conversion of untrusted IPC data
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
                        "error", "request.error", trace_id=trace_id, method=meta.method, path=meta.path, error=str(exc)
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
            contexts.pop(meta.id, None)

    async def _run_websocket_request(
        self, meta: RequestMeta, body_state: RequestBodyState, writer: "ResponseWriter"
    ) -> None:
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

    def _format_response_frame(self, request_id: str, message: dict[str, Any]) -> dict[str, Any] | None:  # noqa: PLR0911
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


def _import_app(app_path: str) -> AsgiApp:
    module_path, _, attr_path = app_path.partition(":")
    module = importlib.import_module(module_path)
    target: Any = module
    if attr_path:
        for attr in attr_path.split("."):
            target = getattr(target, attr)
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
    args = parser.parse_args()

    config = WorkerConfig(
        name=args.name,
        worker_id=args.worker_id,
        slot_size=args.slot_size,
        slots=args.slots,
        host=args.host,
        port=args.port,
        timeout=args.timeout,
    )
    asyncio.run(_serve_worker(args.app, config))


if __name__ == "__main__":
    main()


__all__ = ("HEALTH_PATH", "AsgiRequest", "AsgiResponse", "AsgiWorker", "WorkerConfig")
