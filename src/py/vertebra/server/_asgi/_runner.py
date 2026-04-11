"""AsgiRunner: integrates AsgiAdapter with the native batch runner.

Request dispatch has two paths:

- **Direct coroutine driver** (default): call ``coro.send(None)`` directly and
  chain a :class:`_CoroResumer` as a ``future.add_done_callback`` on each
  yielded future. Skips the ``asyncio.Task`` object entirely for coroutines
  that complete synchronously via ``PyIterAwaitable`` (or any other
  short-circuit), saving the per-request Task allocation + tracking cost.

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
import logging
import os
from typing import TYPE_CHECKING, Any

from vertebra.server._asgi._adapter import AsgiAdapter
from vertebra.server._asgi._queues import AsgiResponse

__all__ = ["AsgiRunner"]

if TYPE_CHECKING:
    from collections.abc import Iterable
    from vertebra.server._native import CallbackScheduler

log = logging.getLogger(__name__)

# Deferred imports cached at module level on first use.
_VR: type | None = None
_submit_response: Any = None


def _ensure_ffi() -> "tuple[type, Any]":
    global _VR, _submit_response  # noqa: PLW0603
    if _VR is None:
        from vertebra._vertebra import VertebraResponse, submit_response
        _VR = VertebraResponse
        _submit_response = submit_response
    return _VR, _submit_response  # type: ignore[return-value]


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


_DIRECT_SEND = _env_flag("VERTEBRA_ASGI_DIRECT_SEND", True)


def _handle_yielded(coro: "Any", yielded: "Any") -> None:
    """Route whatever a coroutine yielded into the correct continuation.

    Mirrors CPython's ``Task.__step`` dispatch: ``None`` reschedules on the
    next loop iteration, futures get an ``add_done_callback`` resumer,
    and anything else is a protocol error that falls back to a real Task
    so we don't lose the coroutine.
    """
    if yielded is None:
        # `await asyncio.sleep(0)` and other "yield once to the scheduler"
        # primitives yield None. Reschedule the next step via call_soon
        # so the loop can process other ready callbacks first — this is
        # what Task.__step does.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return
        loop.call_soon(_step_coro, coro)
        return
    add_cb = getattr(yielded, "add_done_callback", None)
    if add_cb is None:
        # Unexpected yield (not None, not a future). Fall back to wrapping
        # the remainder as a real Task so we don't silently drop it.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return
        loop.create_task(_wrap_remainder(coro, yielded))
        return
    add_cb(_CoroResumer(coro))


def _step_coro(coro: "Any", value: "Any" = None) -> None:
    """Send ``value`` into the coroutine and dispatch whatever it yields."""
    try:
        yielded = coro.send(value)
    except StopIteration:
        return
    except BaseException:
        log.exception("coroutine raised on send")
        return
    _handle_yielded(coro, yielded)


class _CoroResumer:
    """``add_done_callback`` target that resumes a coroutine on future completion.

    On each completion, pulls the future's result (or exception) and drives
    the coroutine forward via ``send`` / ``throw``. If the coroutine yields
    another future, chains a fresh resumer onto it. This keeps the entire
    dispatch off the Task path while still honoring the async/await
    protocol.
    """

    __slots__ = ("_coro",)

    def __init__(self, coro: "Any") -> None:
        self._coro = coro

    def __call__(self, fut: "Any") -> None:
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
                if exc is not None:
                    yielded = coro.throw(type(exc), exc, exc.__traceback__)
                else:
                    yielded = coro.send(fut.result())
            except StopIteration:
                return
            except BaseException:
                log.exception("coroutine raised unhandled exception")
                return

        _handle_yielded(coro, yielded)


async def _wrap_remainder(coro: "Any", yielded: "Any") -> None:
    """Fallback: turn an unexpected yield into a proper awaited step.

    Only used when ``_handle_yielded`` encounters a yielded value that is
    not ``None`` and not a future — shouldn't happen in normal asyncio but
    keeps us robust.
    """
    try:
        value = await yielded
        _step_coro(coro, value)
    except BaseException:
        log.exception("coroutine raised in remainder wrap")


def _drive_coro(coro: "Any") -> None:
    """Step a coroutine once and attach a resumer on any yielded future.

    Call this from the event loop thread. If the coroutine completes
    synchronously (``StopIteration`` on the first ``send``), no Task is
    ever created.
    """
    _step_coro(coro)


class _BatchProcessor:
    """Single callable that drives many coroutines under one event-loop wake.

    Wrapping a batch of coroutines in a single ``call_soon_threadsafe``
    amortizes the cross-thread hop cost across the whole batch instead of
    paying it per request.
    """

    __slots__ = ("_coros",)

    def __init__(self, coros: "list[Any]") -> None:
        self._coros = coros

    def __call__(self) -> None:
        for coro in self._coros:
            _drive_coro(coro)


class AsgiRunner:
    """Synchronous batch runner wrapper around AsgiAdapter."""

    __slots__ = ("_adapter", "_loop", "_owns_loop", "_scheduler")

    def __init__(
        self,
        app: Any,
        mount_prefix: str = "",
        loop: "asyncio.AbstractEventLoop | None" = None,
    ) -> None:
        self._adapter = AsgiAdapter(app, mount_prefix=mount_prefix)
        self._scheduler: "CallbackScheduler | None" = None
        if loop is not None:
            self._loop = loop
            self._owns_loop = False
        else:
            from vertebra.server._native import CallbackScheduler as _CallbackScheduler

            self._scheduler = _CallbackScheduler()
            self._loop = self._scheduler.loop
            self._owns_loop = True

    def __call__(self, requests: "list[Any]") -> "list[AsgiResponse]":
        """Process a batch of requests synchronously."""
        if self._scheduler:
            fut = self._scheduler.schedule(self._adapter.run_batch(requests))
            # Wait for the future from the CallbackScheduler thread
            return fut.result()

        if self._loop.is_running():
            # If the loop is running elsewhere (e.g. from AsgiRunner created in shm_worker_loop)
            future = asyncio.run_coroutine_threadsafe(self._adapter.run_batch(requests), self._loop)
            return future.result()

        # Fallback for sync run (loop not yet running)
        return self._loop.run_until_complete(self._adapter.run_batch(requests))

    def _make_dispatch_coro(self, request: Any) -> "Any":
        """Build the dispatch coroutine that runs the ASGI app and submits."""
        VR, submit = _ensure_ffi()
        adapter = self._adapter

        async def _dispatch() -> None:
            try:
                resp = await adapter.run_single(request)
                try:
                    hdrs = getattr(resp, "headers", [])
                    # Defensive: convert list of lists/tuples to tuple of tuples.
                    # Some PyO3 versions are picky about this when multiple
                    # fields exist.
                    hdrs_tuple = tuple(tuple(h) for h in hdrs)

                    vtb_resp = VR(
                        status=resp.status,
                        body=resp.body,
                        headers=hdrs_tuple,
                        request_id=request.request_id,
                    )
                    submit(vtb_resp)
                except Exception:
                    log.exception(
                        "failed to construct or submit VertebraResponse for request %d",
                        request.request_id,
                    )
                    submit(
                        VR(
                            status=500,
                            body=b"Internal Server Error (FFI Error)",
                            request_id=request.request_id,
                        )
                    )
            except Exception:
                log.exception("unhandled error in AsgiRunner dispatch loop")

        return _dispatch()

    def spawn_single(self, request: Any) -> None:
        """Process a single request asynchronously and submit its response.

        Routing rules (see module docstring):

        - Custom scheduler → delegate to the scheduler's spawn policy.
        - Running asyncio loop + ``_DIRECT_SEND`` → direct drive via
          :func:`_drive_coro` under a single ``call_soon_threadsafe``.
        - Running loop, direct send disabled → ``create_task`` fallback.
        - No running loop → eager ``create_task`` (startup path).
        """
        coro = self._make_dispatch_coro(request)

        if self._scheduler:
            self._scheduler.spawn(coro)
            return

        if self._loop.is_running():
            if _DIRECT_SEND:
                self._loop.call_soon_threadsafe(_drive_coro, coro)
            else:
                loop = self._loop
                loop.call_soon_threadsafe(lambda: loop.create_task(coro))
            return

        # Fallback for synchronous startup if loop not yet running.
        self._loop.create_task(coro)

    def spawn_batch(self, requests: "Iterable[Any]") -> None:
        """Process many requests with a single cross-thread wake.

        The caller should prefer this over a loop of :meth:`spawn_single`
        when dispatching a whole batch — it amortizes the
        ``call_soon_threadsafe`` hop across the batch via
        :class:`_BatchProcessor`.
        """
        req_list = list(requests)
        if not req_list:
            return

        # Small batches (or custom scheduler, or no running loop) go through
        # the per-request path so startup and single-call cases stay simple.
        if self._scheduler or not self._loop.is_running() or len(req_list) == 1:
            for req in req_list:
                self.spawn_single(req)
            return

        if _DIRECT_SEND:
            coros = [self._make_dispatch_coro(r) for r in req_list]
            self._loop.call_soon_threadsafe(_BatchProcessor(coros))
        else:
            loop = self._loop
            coros = [self._make_dispatch_coro(r) for r in req_list]
            loop.call_soon_threadsafe(
                lambda: [loop.create_task(c) for c in coros]
            )

    def close(self) -> None:
        """Shut down the event loop (only if we created it)."""
        if self._scheduler:
            self._scheduler.stop()
        elif self._owns_loop:
            self._loop.close()
