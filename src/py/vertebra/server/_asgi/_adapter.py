"""AsgiAdapter: drives ASGI 3.0 applications with lifespan state and wire extensions."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from vertebra._vertebra import AsgiScope as NativeAsgiScope
from vertebra.server._asgi._queues import AsgiReceive, AsgiResponse, AsgiSend, ChunkCallback
from vertebra.server._asgi._scope import build_scope

__all__ = ("AsgiAdapter",)

log = logging.getLogger(__name__)


class AsgiAdapter:
    """Drives an ASGI 3.0 application over single requests or batches."""

    __slots__ = ("_app", "_lifespan_state", "_mount_prefix", "_on_chunk")

    def __init__(
        self,
        app: Any,
        mount_prefix: str = "",
        *,
        lifespan_state: dict[str, Any] | None = None,
        on_chunk: ChunkCallback | None = None,
    ) -> None:
        self._app = app
        self._mount_prefix = mount_prefix
        self._lifespan_state: dict[str, Any] | None = lifespan_state
        self._on_chunk: ChunkCallback | None = on_chunk

    async def run_single(self, request: Any) -> AsgiResponse:
        """Run the ASGI app for a single request with isolated lifespan state."""
        sender = AsgiSend(on_chunk=self._on_chunk)
        scope = build_scope(request, root_path=self._mount_prefix)
        if self._lifespan_state is not None:
            scope.setdefault("state", self._lifespan_state.copy())
        receive = AsgiReceive(request)
        req_id = int(getattr(request, "request_id", 0))

        try:
            await self._app(scope, receive, sender)
            try:
                resp = sender.response
                resp.request_id = req_id
                return resp
            except RuntimeError:
                log.error("ASGI app for request %d never sent http.response.start", req_id)
                return AsgiResponse(
                    status=500,
                    headers=[("content-type", "text/plain"), ("content-length", "21")],
                    body=b"Internal Server Error",
                    request_id=req_id,
                )
        except Exception:
            log.exception("ASGI app raised unhandled exception for request %d", req_id)
            return AsgiResponse(
                status=500,
                headers=[("content-type", "text/plain"), ("content-length", "21")],
                body=b"Internal Server Error",
                request_id=req_id,
            )

    async def handle_path(
        self,
        path: str,
        *,
        method: str = "GET",
        query_string: str = "",
        body: bytes = b"",
    ) -> AsgiResponse:
        """Execute the ASGI application directly against a synthetic native AsgiScope."""
        scope = NativeAsgiScope.from_parts(
            method=method,
            path=path,
            query_string=query_string,
            root_path=self._mount_prefix,
        )
        if self._lifespan_state is not None:
            scope.setdefault("state", self._lifespan_state.copy())
        sender = AsgiSend(on_chunk=self._on_chunk)

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        await self._app(scope, receive, sender)
        return sender.response

    async def run_batch(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run the ASGI app for every request in the batch concurrently."""
        return list(await asyncio.gather(*(self.run_single(req) for req in requests)))
