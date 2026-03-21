"""AsgiAdapter: drives ASGI apps through the protocol for a request batch."""

import asyncio
import logging
from typing import Any

from vertebra.server._asgi._queues import AsgiResponse, ReceiveChannel, SendCollector
from vertebra.server._asgi._scope import build_scope

__all__ = ["AsgiAdapter"]

log = logging.getLogger(__name__)


class AsgiAdapter:
    """Drives an ASGI 3.0 application over a batch of VertebraRequests."""

    __slots__ = ("_app", "_mount_prefix")

    def __init__(self, app: Any, mount_prefix: str = "") -> None:
        self._app = app
        self._mount_prefix = mount_prefix

    async def run_batch(self, requests: list[Any]) -> list[AsgiResponse]:
        """Run the ASGI app for every request in the batch concurrently."""
        n = len(requests)
        collectors: list[SendCollector] = [SendCollector() for _ in range(n)]
        scopes = [build_scope(req, root_path=self._mount_prefix) for req in requests]
        receives = [ReceiveChannel(req) for req in requests]

        raw_results = await asyncio.gather(
            *(self._app(scopes[i], receives[i], collectors[i]) for i in range(n)),
            return_exceptions=True,
        )

        results: list[AsgiResponse] = []
        for i in range(n):
            req_id = getattr(requests[i], "request_id", 0)
            if isinstance(raw_results[i], BaseException):
                log.exception(
                    "ASGI app raised unhandled exception for request %d",
                    i,
                    exc_info=raw_results[i],
                )
                results.append(
                    AsgiResponse(
                        status=500,
                        headers=[("content-type", "text/plain"), ("content-length", "21")],
                        body=b"Internal Server Error",
                        request_id=req_id,
                    )
                )
            else:
                try:
                    resp = collectors[i].response
                    resp.request_id = req_id
                    results.append(resp)
                except RuntimeError:
                    log.error("ASGI app for request %d never sent http.response.start", i)
                    results.append(
                        AsgiResponse(
                            status=500,
                            headers=[("content-type", "text/plain"), ("content-length", "21")],
                            body=b"Internal Server Error",
                            request_id=req_id,
                        )
                    )

        return results
