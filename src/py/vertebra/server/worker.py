"""ASGI worker harness for Bun IPC bridge."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgspec


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    transport: str = "tcp"
    host: str = "127.0.0.1"
    port: int = 8008
    path: str | None = None
    timeout: float = 5.0


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


class AsgiResponse(msgspec.Struct):
    id: str
    status: int
    headers: list[tuple[str, str]]
    body: bytes | None = None
    path: str | None = None
    offset: int | None = None
    count: int | None = None


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
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(AsgiRequest)
        self._server: asyncio.base_events.Server | None = None
        self._lifespan_task: asyncio.Task[None] | None = None
        self._lifespan_queue: asyncio.Queue[dict[str, Any]] | None = None
        self._lifespan_state: dict[str, Any] = {}
        self._lifespan_startup = asyncio.Event()
        self._lifespan_shutdown = asyncio.Event()
        self._lifespan_failed: str | None = None
        self._lifespan_receive_called = False
        self._lifespan_supported: bool | None = None

    async def start(self) -> None:
        if self._server is not None:
            return
        await self._start_lifespan()
        if self._config.transport == "uds":
            if not self._config.path:
                msg = "UDS transport requires path"
                raise RuntimeError(msg)
            path = Path(self._config.path)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists():
                path.unlink()
            self._server = await asyncio.start_unix_server(self._handle_client, path=self._config.path)
        else:
            self._server = await asyncio.start_server(self._handle_client, self._config.host, self._config.port)

    async def serve_forever(self) -> None:
        await self.start()
        if self._server is None:
            msg = "ASGI server failed to start"
            raise RuntimeError(msg)
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None
        if self._config.transport == "uds" and self._config.path:
            path = Path(self._config.path)
            if path.exists():
                path.unlink()
        await self._shutdown_lifespan()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(4)
                size = int.from_bytes(header, "big")
                payload = await reader.readexactly(size)
                request = self._decoder.decode(payload)
                async for frame in self._handle_request(request):
                    encoded = self._encoder.encode(frame)
                    writer.write(len(encoded).to_bytes(4, "big") + encoded)
                    await writer.drain()
        except asyncio.IncompleteReadError:
            return
        finally:
            writer.close()
            await writer.wait_closed()

    async def _handle_request(self, request: AsgiRequest):
        if request.path == HEALTH_PATH:
            yield {
                "id": request.id,
                "type": "http.response.start",
                "status": 200,
                "headers": [("content-type", "text/plain")],
            }
            yield {"id": request.id, "type": "http.response.body", "body": b"ok", "more_body": False}
            return

        raw_path = request.raw_path if request.raw_path is not None else request.path.encode()
        query_string = request.query_string if request.query_string is not None else request.query.encode()
        scope = {
            "type": "http",
            "asgi": {"version": ASGI_VERSION, "spec_version": ASGI_HTTP_SPEC_VERSION},
            "http_version": request.http_version,
            "method": request.method,
            "path": request.path,
            "raw_path": raw_path,
            "query_string": query_string,
            "headers": [(k.lower().encode(), v.encode()) for k, v in request.headers],
            "scheme": request.scheme,
            "root_path": request.root_path,
            "server": request.server or (self._config.host, self._config.port),
            "client": request.client or ("127.0.0.1", 0),
            "extensions": request.extensions,
            "state": self._lifespan_state,
        }

        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1)
        consumed = False
        response_started = False

        async def receive() -> dict[str, Any]:
            nonlocal consumed
            if consumed:
                return {"type": "http.disconnect"}
            consumed = True
            return {"type": "http.request", "body": request.body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            await queue.put(message)

        app_task = asyncio.create_task(self._app(scope, receive, send))
        try:
            while True:
                if app_task.done() and queue.empty():
                    break
                message = await queue.get()
                frame = self._format_response_frame(request.id, message)
                if frame is None:
                    continue
                if frame.get("type") == "http.response.start":
                    response_started = True
                yield frame
        finally:
            try:
                await app_task
            except Exception:
                if not response_started:
                    yield {
                        "id": request.id,
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [("content-type", "text/plain")],
                    }
                    yield {
                        "id": request.id,
                        "type": "http.response.body",
                        "body": b"Internal Server Error",
                        "more_body": False,
                    }
                return

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
                "more_body": bool(message.get("more_body", False)),
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
    return target


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
    parser.add_argument("--transport", choices=("tcp", "uds"), default="tcp")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8008)
    parser.add_argument("--path", default=None)
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    config = WorkerConfig(
        transport=args.transport, host=args.host, port=args.port, path=args.path, timeout=args.timeout
    )
    asyncio.run(_serve_worker(args.app, config))


if __name__ == "__main__":
    main()


__all__ = ("HEALTH_PATH", "AsgiRequest", "AsgiResponse", "AsgiWorker", "WorkerConfig")
