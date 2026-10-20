"""AsgiRunner: integrates AsgiAdapter with the native batch runner.

Request dispatch has two paths:

- **Direct coroutine driver** (default): call ``coro.send(None)`` directly and
  chain a :class:`_CoroResumer` as a ``future.add_done_callback`` on each
  yielded future. Skips the ``asyncio.Task`` object entirely for coroutines
  that complete synchronously via ``PyIterAwaitable`` (or any other
  short-circuit), saving the per-request Task allocation and tracking cost.

- **Task fallback**: the original ``loop.create_task(coro)`` path. Selected
  when ``VERTEBRA_ASGI_DIRECT_SEND`` is set to ``0``/``false``, or when the
  request dispatch is running on a non-asyncio scheduler (which owns its
  own spawn policy).

``asyncio.current_task()`` returns ``None`` inside the direct driver because
no Task wrapper exists. Middleware or tracing libraries that walk task
context should either be written to tolerate ``None`` or the operator
should flip ``VERTEBRA_ASGI_DIRECT_SEND=0`` to fall back to the Task path.
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import Iterable
from typing import Any

from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra.server._asgi._queues import AsgiResponse
from vertebra.server._native import AsyncRuntime

__all__ = ("AsgiRunner",)

_NativeResponse: Any = None
_native_submit_response: Any = None
with contextlib.suppress(ImportError):
    from vertebra._vertebra import Response as _NativeResponse
    from vertebra._vertebra import submit_response as _native_submit_response

log = logging.getLogger(__name__)
_RAW_DIRECT_SEND = os.environ.get("VERTEBRA_ASGI_DIRECT_SEND")
_DIRECT_SEND = (
    True if _RAW_DIRECT_SEND is None else _RAW_DIRECT_SEND.strip().lower() not in ("0", "false", "no", "off", "")
)


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
        self._scheduler: AsyncRuntime | None = None
        if loop is not None:
            self._loop = loop
            self._owns_loop = False
        else:
            self._scheduler = AsyncRuntime()
            self._loop = self._scheduler.loop
            self._owns_loop = True

    def __call__(self, requests: list[Any]) -> list[AsgiResponse]:
        """Process a batch of requests synchronously."""
        if self._scheduler:
            fut = self._scheduler.schedule(self._adapter.run_batch(requests))
            return fut.result()

        if self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(self._adapter.run_batch(requests), self._loop)
            return future.result()

        return self._loop.run_until_complete(self._adapter.run_batch(requests))

    def spawn_single(self, request: Any) -> None:
        """Process a single request asynchronously and submit its response."""
        coro = self._make_dispatch_coro(request)

        if self._scheduler:
            self._scheduler.spawn(coro)
            return

        if self._loop.is_running():
            if _DIRECT_SEND:
                self._loop.call_soon_threadsafe(_drive_coro, coro)
            else:
                loop = self._loop
                loop.call_soon_threadsafe(loop.create_task, coro)
            return

        self._loop.create_task(coro)

    def spawn_batch(self, requests: Iterable[Any]) -> None:
        """Process many requests with a single cross-thread wake."""
        req_list = list(requests)
        if not req_list:
            return

        if self._scheduler is not None:
            if len(req_list) == 1:
                self.spawn_single(req_list[0])
                return
            self._scheduler.spawn_batch(self._make_dispatch_coro(r) for r in req_list)
            return

        if not self._loop.is_running() or len(req_list) == 1:
            for req in req_list:
                self.spawn_single(req)
            return

        coros = [self._make_dispatch_coro(r) for r in req_list]
        if _DIRECT_SEND:
            self._loop.call_soon_threadsafe(_BatchProcessor(coros))
        else:
            loop = self._loop

            def _create_all() -> None:
                for c in coros:
                    loop.create_task(c)

            loop.call_soon_threadsafe(_create_all)

    def close(self) -> None:
        """Shut down the event loop when owned by this runner."""
        if self._scheduler:
            self._scheduler.stop()
        elif self._owns_loop:
            self._loop.close()

    def _make_dispatch_coro(self, request: Any) -> Any:
        """Build the dispatch coroutine that runs the ASGI app and submits its response."""
        vr_cls, submit = _ensure_ffi()
        adapter = self._adapter

        async def _dispatch() -> None:
            try:
                resp = await adapter.run_single(request)
                try:
                    vtb_resp = vr_cls(
                        status=resp.status,
                        body=resp.body,
                        headers=getattr(resp, "headers", None),
                        request_id=request.request_id,
                    )
                    submit(vtb_resp)
                except Exception:
                    log.exception(
                        "failed to construct or submit Response for request %d",
                        request.request_id,
                    )
                    submit(
                        vr_cls(
                            status=500,
                            body=b"Internal Server Error (FFI Error)",
                            request_id=request.request_id,
                        )
                    )
            except Exception:
                log.exception("unhandled error in AsgiRunner dispatch loop")

        return _dispatch()


class _CoroResumer:
    """Callback target for ``add_done_callback`` that resumes a coroutine on future completion."""

    __slots__ = ("_coro",)

    def __init__(self, coro: Any) -> None:
        self._coro = coro

    def __call__(self, fut: Any) -> None:
        coro = self._coro
        try:
            exc = fut.exception()
        except asyncio.CancelledError:
            try:
                yielded = coro.throw(asyncio.CancelledError())
            except StopIteration:
                return
            except BaseException:
                log.exception("coroutine raised after cancellation")
                return
        else:
            try:
                yielded = coro.throw(type(exc), exc, exc.__traceback__) if exc is not None else coro.send(fut.result())
            except StopIteration:
                return
            except BaseException:
                log.exception("coroutine raised unhandled exception")
                return

        _handle_yielded(coro, yielded)


class _BatchProcessor:
    """Single callable that drives many coroutines under one event-loop wake."""

    __slots__ = ("_coros",)

    def __init__(self, coros: list[Any]) -> None:
        self._coros = coros

    def __call__(self) -> None:
        for coro in self._coros:
            _drive_coro(coro)


def _ensure_ffi() -> tuple[Any, Any]:
    """Return the native Response class and submit_response function."""
    if _NativeResponse is not None and _native_submit_response is not None:
        return _NativeResponse, _native_submit_response
    from vertebra._vertebra import Response, submit_response

    return Response, submit_response


def _current_loop() -> asyncio.AbstractEventLoop | None:
    """Return the running event loop, falling back to the policy loop when set."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        with contextlib.suppress(RuntimeError):
            return asyncio.get_event_loop_policy().get_event_loop()
        return None


def _handle_yielded(coro: Any, yielded: Any) -> None:
    """Route whatever a coroutine yielded into the appropriate continuation."""
    if yielded is None:
        loop = _current_loop()
        if loop is not None:
            loop.call_soon(_step_coro, coro)
        return
    add_cb = getattr(yielded, "add_done_callback", None)
    if add_cb is None:
        loop = _current_loop()
        if loop is not None:
            loop.create_task(_wrap_remainder(coro, yielded))
        return
    add_cb(_CoroResumer(coro))


def _step_coro(coro: Any, value: Any = None) -> None:
    """Send ``value`` into the coroutine and dispatch whatever it yields."""
    try:
        yielded = coro.send(value)
    except StopIteration:
        return
    except BaseException:
        log.exception("coroutine raised on send")
        return
    _handle_yielded(coro, yielded)


async def _wrap_remainder(coro: Any, yielded: Any) -> None:
    """Turn an unexpected non-future yield into an awaited step."""
    try:
        value = await yielded
        _step_coro(coro, value)
    except BaseException:
        log.exception("coroutine raised in remainder wrap")


def _drive_coro(coro: Any) -> None:
    """Step a coroutine once and attach a resumer on any yielded future."""
    _step_coro(coro)
