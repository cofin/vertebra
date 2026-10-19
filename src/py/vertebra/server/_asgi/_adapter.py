"""AsgiAdapter: drives ASGI apps through the protocol for a request batch."""

import asyncio
import logging
from typing import Any

from vertebra.server._asgi._queues import AsgiReceive, AsgiResponse, AsgiSend
from vertebra.server._asgi._scope import build_scope

__all__ = ("AsgiAdapter",)

log = logging.getLogger(__name__)


class AsgiAdapter:
    """Drives an ASGI 3.0 application over a batch of Requests."""

    __slots__ = ("_app", "_mount_prefix")

    def __init__(self, app: Any, mount_prefix: str = "") -> None:
        self._app = app
        self._mount_prefix = mount_prefix

    async def run_single(self, request: Any) -> AsgiResponse:
        """Run the ASGI app for a single request."""
        collector = AsgiSend()
        scope = build_scope(request, root_path=self._mount_prefix)
        receive = AsgiReceive(request)
        req_id = getattr(request, "request_id", 0)

        try:
            await self._app(scope, receive, collector)
            try:
                resp = collector.response
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

    async def run_batch(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run the ASGI app for every request in the batch concurrently."""
        return await asyncio.gather(*(self.run_single(req) for req in requests))
