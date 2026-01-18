"""ASGI worker harness for Bun IPC bridge."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Iterable

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
    httpVersion: str = "1.1"


class AsgiResponse(msgspec.Struct):
    id: str
    status: int
    headers: list[tuple[str, str]]
    body: bytes


AsgiApp = Callable[[dict[str, Any], Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]], Awaitable[None]]


class AsgiWorker:
    def __init__(self, app: AsgiApp, config: WorkerConfig | None = None) -> None:
        self._app = app
        self._config = config or WorkerConfig()
        self._encoder = msgspec.msgpack.Encoder()
        self._decoder = msgspec.msgpack.Decoder(AsgiRequest)
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        if self._server is not None:
            return
        if self._config.transport == "uds":
            if not self._config.path:
                raise RuntimeError("UDS transport requires path")
            self._server = await asyncio.start_unix_server(self._handle_client, path=self._config.path)
        else:
            self._server = await asyncio.start_server(self._handle_client, self._config.host, self._config.port)

    async def serve_forever(self) -> None:
        await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                header = await reader.readexactly(4)
                size = int.from_bytes(header, "big")
                payload = await reader.readexactly(size)
                request = self._decoder.decode(payload)
                response = await self._handle_request(request)
                encoded = self._encoder.encode(response)
                writer.write(len(encoded).to_bytes(4, "big") + encoded)
                await writer.drain()
        except asyncio.IncompleteReadError:
            return
        finally:
            writer.close()
            await writer.wait_closed()

    async def _handle_request(self, request: AsgiRequest) -> AsgiResponse:
        scope = {
            "type": "http",
            "http_version": request.httpVersion,
            "method": request.method,
            "path": request.path,
            "raw_path": request.path.encode(),
            "query_string": request.query.encode(),
            "headers": [(k.lower().encode(), v.encode()) for k, v in request.headers],
            "scheme": request.scheme,
            "server": (self._config.host, self._config.port),
            "client": ("0.0.0.0", 0),
        }

        body_chunks: list[bytes] = []
        status: int | None = None
        headers: list[tuple[str, str]] = []
        consumed = False

        async def receive() -> dict[str, Any]:
            nonlocal consumed
            if consumed:
                return {"type": "http.disconnect"}
            consumed = True
            return {"type": "http.request", "body": request.body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            nonlocal status, headers
            message_type = message.get("type")
            if message_type == "http.response.start":
                status = int(message.get("status", 500))
                headers = [
                    (key.decode(), value.decode())
                    for key, value in message.get("headers", [])
                ]
            elif message_type == "http.response.body":
                body_chunks.append(message.get("body", b""))

        await self._app(scope, receive, send)
        response_body = b"".join(body_chunks)
        return AsgiResponse(
            id=request.id,
            status=status or 500,
            headers=headers,
            body=response_body,
        )


__all__ = ["AsgiWorker", "AsgiRequest", "AsgiResponse", "WorkerConfig"]
