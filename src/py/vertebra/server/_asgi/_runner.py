"""AsgiRunner: integrates AsgiAdapter with the native batch runner."""

import asyncio
import concurrent.futures
import logging
from typing import TYPE_CHECKING, Any

from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra.server._asgi._queues import AsgiResponse

__all__ = ["AsgiRunner"]

if TYPE_CHECKING:
    from vertebra.server._native import CallbackScheduler

log = logging.getLogger(__name__)


class AsgiRunner:
    """Synchronous batch runner wrapper around AsgiAdapter."""

    __slots__ = ("_adapter", "_loop", "_owns_loop", "_scheduler")

    def __init__(
        self,
        app: Any,
        mount_prefix: str = "",
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._adapter = AsgiAdapter(app, mount_prefix=mount_prefix)
        self._scheduler: CallbackScheduler | None = None
        if loop is not None:
            self._loop = loop
            self._owns_loop = False
        else:
            from vertebra.server._native import CallbackScheduler as _CallbackScheduler

            self._scheduler = _CallbackScheduler()
            self._loop = self._scheduler.loop
            self._owns_loop = True

    def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
        """Process a batch of requests synchronously."""
        if self._scheduler:
            fut = self._scheduler.schedule(self._adapter.run_batch(requests))
            concurrent.futures.wait([fut])
            return fut.result()

        if self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._adapter.run_batch(requests), self._loop)
            return future.result()

        return self._loop.run_until_complete(self._adapter.run_batch(requests))

    def spawn_single(self, request: Any) -> None:
        """Process a single request asynchronously and submit its response."""
        from vertebra._vertebra import submit_response
        from vertebra._vertebra import VertebraResponse as VR

        async def _dispatch() -> None:
            resp = await self._adapter.run_single(request)
            # Individual submission immediately notifies supervisor
            submit_response(
                VR(
                    status=resp.status,
                    body=resp.body,
                    headers=getattr(resp, "headers", []),
                    request_id=request.request_id,
                )
            )

        if self._scheduler:
            self._scheduler.spawn(_dispatch())
        elif self._loop.is_running():
            self._loop.call_soon_threadsafe(lambda: self._loop.create_task(_dispatch()))
        else:
            # Fallback for synchronous startup if loop not yet running
            self._loop.create_task(_dispatch())

    def close(self) -> None:
        """Shut down the event loop (only if we created it)."""
        if self._scheduler:
            self._scheduler.stop()
        elif self._owns_loop:
            self._loop.close()
