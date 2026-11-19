"""Multi-core runtime tuning, CPU affinity pinning, and sharded event loop management."""

import asyncio
import concurrent.futures
import itertools
import logging
import os
import sys
import threading
import time
from collections.abc import Coroutine, Iterable
from contextlib import suppress
from enum import StrEnum
from typing import Any, Literal

import msgspec

from vertebra.server._gil_audit import GilAuditReport
from vertebra.server.platform import CgroupQuota
from vertebra.telemetry.otel import EventLoopMetricSample, EventLoopRecorder
from vertebra.utils.module_loader import import_optional

__all__ = (
    "LOOP_BACKEND_CHOICES",
    "EventLoopPool",
    "LoopBackend",
    "LoopBackendSpec",
    "LoopTaskSnapshot",
    "capture_loop_task_snapshots",
    "configure_worker_affinity",
    "create_event_loop",
    "format_task_call_graphs",
    "resolve_loop_backend",
    "resolve_loop_shards",
)

logger = logging.getLogger(__name__)
_SHARD_START_TIMEOUT_SECONDS: float = 5.0
_STOP_TIMEOUT_SECONDS: float = 5.0
_TASKZ_PROBE_TASK_NAME: str = "vertebra.zpages.taskz_probe"


class LoopBackend(StrEnum):
    """Supported asyncio event loop backend implementations."""

    AUTO = "auto"
    UVLOOP = "uvloop"
    WINLOOP = "winloop"
    RLOOP = "rloop"
    ZUVLOOP = "zuvloop"
    ASYNCIO = "asyncio"


LOOP_BACKEND_CHOICES: tuple[str, ...] = tuple(backend.value for backend in LoopBackend)


class LoopBackendSpec(msgspec.Struct, frozen=True, gc=False):
    """Declarative specification for an optional event-loop backend module."""

    backend: LoopBackend
    """Target ``LoopBackend`` enum variant."""

    module_name: str
    """Importable module name checked against free-threaded quarantine and ``import_optional``."""

    platforms: Literal["all", "win32", "posix"] = "all"
    """Platform family considered during ``LoopBackend.AUTO`` discovery."""

    auto_discover: bool = True
    """Whether ``LoopBackend.AUTO`` evaluates this backend in priority order."""


class LoopTaskSnapshot(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Point-in-time diagnostic snapshot of an asyncio Task and its async call graph."""

    task_name: str
    """Structured or user-assigned name of the ``asyncio.Task``."""

    coro_qualname: str
    """Qualified name of the root coroutine wrapped by the task."""

    done: bool
    """Whether the task has finished execution."""

    cancelled: bool
    """Whether the task was cancelled."""

    shard_id: int | None = None
    """Zero-based ``EventLoopPool`` shard identifier when bound to a pool shard."""

    call_graph: str = ""
    """Formatted Python 3.14+ async call graph or Python 3.12-3.13 stack trace."""


_LOOP_BACKEND_SPECS: tuple[LoopBackendSpec, ...] = (
    LoopBackendSpec(backend=LoopBackend.WINLOOP, module_name="winloop", platforms="win32"),
    LoopBackendSpec(backend=LoopBackend.UVLOOP, module_name="uvloop", platforms="posix"),
    LoopBackendSpec(backend=LoopBackend.ZUVLOOP, module_name="zuvloop", platforms="all"),
    LoopBackendSpec(backend=LoopBackend.RLOOP, module_name="rloop", platforms="posix"),
)
_LOOP_SPEC_BY_BACKEND: dict[LoopBackend, LoopBackendSpec] = {spec.backend: spec for spec in _LOOP_BACKEND_SPECS}


def resolve_loop_backend(
    backend: LoopBackend | str | None = None,
    *,
    quarantined_modules: frozenset[str] | tuple[str, ...] = frozenset(),
) -> LoopBackend:
    """Resolve the active event loop backend honoring explicit overrides, env vars, and GIL quarantine."""
    raw = str(backend if backend is not None else os.environ.get("VERTEBRA_LOOP", "auto")).strip().lower()
    try:
        requested = LoopBackend(raw) if raw else LoopBackend.AUTO
    except ValueError:
        requested = LoopBackend.AUTO

    if requested == LoopBackend.ASYNCIO:
        return LoopBackend.ASYNCIO

    quarantined_prefixes = _collect_quarantined_prefixes(quarantined_modules)

    if requested != LoopBackend.AUTO:
        spec = _LOOP_SPEC_BY_BACKEND.get(requested)
        if spec is not None and _is_backend_available(spec, quarantined_prefixes):
            return spec.backend
        return LoopBackend.ASYNCIO

    active_platform: Literal["win32", "posix"] = "win32" if sys.platform == "win32" else "posix"
    for spec in _LOOP_BACKEND_SPECS:
        if (
            spec.auto_discover
            and spec.platforms in {"all", active_platform}
            and _is_backend_available(spec, quarantined_prefixes)
        ):
            return spec.backend
    return LoopBackend.ASYNCIO


def create_event_loop(
    backend: LoopBackend | str | None = None,
    *,
    quarantined_modules: frozenset[str] | tuple[str, ...] = frozenset(),
) -> asyncio.AbstractEventLoop:
    """Create a high-performance event loop for the resolved LoopBackend."""
    resolved = resolve_loop_backend(backend, quarantined_modules=quarantined_modules)
    loop: asyncio.AbstractEventLoop | None = None
    spec = _LOOP_SPEC_BY_BACKEND.get(resolved)
    if spec is not None:
        loop_mod = import_optional(spec.module_name)
        if loop_mod is not None:
            loop = loop_mod.new_event_loop()

    if loop is None:
        loop = asyncio.new_event_loop()

    if hasattr(asyncio, "eager_task_factory"):
        with suppress(AttributeError, NotImplementedError, TypeError):
            loop.set_task_factory(asyncio.eager_task_factory)
    return loop


def configure_worker_affinity(worker_index: int, *, enable_pinning: bool = False) -> bool:
    """Pin the calling worker process or thread to an allowed CPU core on Linux."""
    if not enable_pinning or not hasattr(os, "sched_setaffinity"):
        return False
    allowed_cpus: list[int] = []
    if hasattr(os, "sched_getaffinity"):
        try:
            allowed_cpus = sorted(os.sched_getaffinity(0))
        except OSError:
            allowed_cpus = []
    if allowed_cpus:
        target_core = allowed_cpus[abs(worker_index) % len(allowed_cpus)]
    else:
        cpu_count = os.cpu_count() or 1
        target_core = abs(worker_index) % cpu_count
    try:
        os.sched_setaffinity(0, {target_core})
    except OSError:
        return False
    return True


def resolve_loop_shards(
    explicit: int | None = None,
    audit_report: GilAuditReport | None = None,
) -> int:
    """Resolve the event loop pool size from explicit value, env, or hardware."""
    if explicit is not None and explicit >= 1:
        shards = explicit
    elif env_val := os.environ.get("VERTEBRA_LOOP_SHARDS"):
        try:
            shards = max(1, int(env_val))
        except ValueError:
            shards = 1
    elif GilAuditReport.gil_enabled():
        shards = 1
    else:
        shards = CgroupQuota.detect().effective_workers

    if audit_report is not None:
        shards = audit_report.quarantine_unsafe_modules(shards)
    return shards


def capture_loop_task_snapshots(
    loop: asyncio.AbstractEventLoop | None = None,
    *,
    shard_id: int | None = None,
) -> list[LoopTaskSnapshot]:
    """Capture diagnostic snapshots and call graphs for active tasks on ``loop``."""
    target_loop = loop
    if target_loop is None:
        try:
            target_loop = asyncio.get_running_loop()
        except RuntimeError:
            return []
    if target_loop.is_closed():
        return []
    try:
        raw_tasks = list(asyncio.all_tasks(target_loop))
    except RuntimeError:
        return []
    raw_tasks.sort(key=lambda item: item.get_name())
    return [_build_task_snapshot(task, shard_id=shard_id) for task in raw_tasks]


def format_task_call_graphs(loop: asyncio.AbstractEventLoop | None = None) -> str:
    """Format async call graphs across all active tasks on ``loop``."""
    snapshots = capture_loop_task_snapshots(loop)
    if not snapshots:
        return ""
    sections = [snap.call_graph for snap in snapshots if snap.call_graph]
    return "\n\n".join(sections)


class EventLoopPool:
    """Sharded pool of persistent event loops with optional inline single-thread mode.

    An `EventLoopRecorder` attached through the `recorder` keyword or `attach_recorder` receives one
    `EventLoopMetricSample` per shard per interval, recorded on that shard's own loop thread. A pool
    without a recorder schedules no sampling timers.
    """

    __slots__ = (
        "_close_on_exit",
        "_counter",
        "_inline_drain_task",
        "_inline_drained",
        "_inline_run_depth",
        "_inline_single",
        "_loop_backend",
        "_loops",
        "_quarantined_modules",
        "_recorder",
        "_recorder_backend",
        "_recorder_interval",
        "_sampler_handles",
        "_size",
        "_stopped",
        "_task_counters",
        "_task_sets",
        "_threads",
    )

    def __init__(
        self,
        size: int | None = None,
        *,
        inline_single_loop: bool = False,
        loop_backend: LoopBackend | str | None = None,
        gil_report: GilAuditReport | None = None,
        recorder: EventLoopRecorder | None = None,
        recorder_interval_seconds: float = 1.0,
    ) -> None:
        if recorder is not None and not recorder_interval_seconds > 0:
            msg = "EventLoopPool recorder_interval_seconds must be > 0"
            raise ValueError(msg)
        resolved_size = size if size is not None else resolve_loop_shards(audit_report=gil_report)
        if resolved_size < 1:
            msg = "EventLoopPool size must be >= 1"
            raise ValueError(msg)

        self._size = resolved_size
        self._inline_single = inline_single_loop and resolved_size == 1
        self._loop_backend = loop_backend
        self._quarantined_modules: frozenset[str] = (
            frozenset(gil_report.quarantined_modules) if gil_report is not None else frozenset()
        )
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._threads: list[threading.Thread] = []
        self._task_sets: list[set[asyncio.Task[Any]]] = []
        self._task_counters: list[itertools.count[int]] = []
        self._counter = itertools.count()
        self._stopped = False
        self._close_on_exit = False
        self._inline_drained = False
        self._inline_drain_task: asyncio.Task[None] | None = None
        self._inline_run_depth = 0
        self._recorder: EventLoopRecorder | None = None
        self._recorder_backend = ""
        self._recorder_interval = 0.0
        self._sampler_handles: list[asyncio.TimerHandle | None] = [None] * resolved_size

        if self._inline_single:
            inline_loop = create_event_loop(
                backend=self._loop_backend,
                quarantined_modules=self._quarantined_modules,
            )
            self._loops.append(inline_loop)
            self._task_sets.append(set())
            self._task_counters.append(itertools.count(1))
            self._install_shard_task_naming(0, inline_loop)
            self._install_inline_exit_hook(inline_loop)
            if recorder is not None:
                self.attach_recorder(recorder, interval_seconds=recorder_interval_seconds)
            return

        for shard_idx in range(resolved_size):
            ready = threading.Event()
            loop = create_event_loop(
                backend=self._loop_backend,
                quarantined_modules=self._quarantined_modules,
            )
            self._loops.append(loop)
            self._task_sets.append(set())
            self._task_counters.append(itertools.count(1))
            self._install_shard_task_naming(shard_idx, loop)
            thread = threading.Thread(
                target=self._run_loop_shard,
                args=(loop, ready),
                name=f"vertebra-event-loop-{shard_idx}",
                daemon=True,
            )
            self._threads.append(thread)
            thread.start()
            if not ready.wait(timeout=_SHARD_START_TIMEOUT_SECONDS):
                self.stop()
                msg = f"Event loop shard {shard_idx} did not start within {_SHARD_START_TIMEOUT_SECONDS}s"
                raise RuntimeError(msg)

        if recorder is not None:
            self.attach_recorder(recorder, interval_seconds=recorder_interval_seconds)

    @property
    def size(self) -> int:
        """Return the number of event loop shards in the pool."""
        return self._size

    @property
    def loops(self) -> tuple[asyncio.AbstractEventLoop, ...]:
        """Return all event loop shards in the pool."""
        return tuple(self._loops)

    @property
    def primary_loop(self) -> asyncio.AbstractEventLoop:
        """Return the primary event loop shard without mutating round-robin state."""
        return self._loops[0]

    @property
    def recorder(self) -> EventLoopRecorder | None:
        """Return the attached `EventLoopRecorder`, or `None` when no loop telemetry is sampled."""
        return self._recorder

    def get_loop(self, worker_index: int) -> asyncio.AbstractEventLoop:
        """Return the event loop shard assigned to worker_index."""
        return self._loops[worker_index % self._size]

    def next_loop(self) -> asyncio.AbstractEventLoop:
        """Return the next event loop shard in round-robin order."""
        idx = next(self._counter) % self._size
        return self._loops[idx]

    def spawn(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        worker_index: int | None = None,
        name: str | None = None,
    ) -> None:
        """Schedule a fire-and-forget coroutine onto the target or next event loop shard."""
        idx = (worker_index if worker_index is not None else next(self._counter)) % self._size
        self._dispatch_shard_coros(idx, [coro], self._running_loop_or_none(), task_name=name)

    def spawn_batch(self, coros: Iterable[Coroutine[Any, Any, Any]]) -> None:
        """Partition and schedule a batch of coroutines with at most one wakeup per loop shard."""
        running = self._running_loop_or_none()
        if self._size == 1:
            batch = coros if isinstance(coros, list) else list(coros)
            self._dispatch_shard_coros(0, batch, running)
            return

        buckets: list[list[Coroutine[Any, Any, Any]]] = [[] for _ in range(self._size)]
        for coro in coros:
            buckets[next(self._counter) % self._size].append(coro)

        for idx, shard_coros in enumerate(buckets):
            self._dispatch_shard_coros(idx, shard_coros, running)

    def schedule[T](
        self,
        coro: Coroutine[Any, Any, T],
        *,
        worker_index: int | None = None,
        name: str | None = None,
    ) -> concurrent.futures.Future[T]:
        """Schedule a coroutine onto a specific or round-robin shard and return its Future."""
        idx = (worker_index if worker_index is not None else next(self._counter)) % self._size
        loop = self._loops[idx]
        if name is None:
            return asyncio.run_coroutine_threadsafe(coro, loop)
        if not asyncio.iscoroutine(coro):
            msg = "A coroutine object is required"
            raise TypeError(msg)
        fut: concurrent.futures.Future[T] = concurrent.futures.Future()
        tasks = self._task_sets[idx]

        def _schedule_named() -> None:
            if fut.cancelled():
                coro.close()
                return
            try:
                task = loop.create_task(coro, name=name)
            except Exception as exc:
                if not fut.done():
                    fut.set_exception(exc)
                return

            tasks.add(task)
            task.add_done_callback(tasks.discard)

            def _on_done(t: asyncio.Task[T]) -> None:
                try:
                    if t.cancelled():
                        fut.cancel()
                    elif exc := t.exception():
                        fut.set_exception(exc)
                    else:
                        fut.set_result(t.result())
                except Exception as exc2:
                    if not fut.done():
                        fut.set_exception(exc2)

            task.add_done_callback(_on_done)

            def _cancel_task(done: concurrent.futures.Future[T]) -> None:
                if done.cancelled() and not loop.is_closed():
                    loop.call_soon_threadsafe(task.cancel)

            fut.add_done_callback(_cancel_task)

        loop.call_soon_threadsafe(_schedule_named)
        return fut

    def schedule_batch[T](
        self,
        coros: list[Coroutine[Any, Any, T]],
    ) -> list[concurrent.futures.Future[T]]:
        """Schedule a batch of coroutines across all loop shards and return aligned Futures."""
        if not coros:
            return []
        futures: list[concurrent.futures.Future[T]] = [concurrent.futures.Future() for _ in coros]
        buckets: list[list[tuple[Coroutine[Any, Any, T], concurrent.futures.Future[T]]]] = [
            [] for _ in range(self._size)
        ]
        for pos, (coro, fut) in enumerate(zip(coros, futures, strict=True)):
            buckets[pos % self._size].append((coro, fut))

        for idx, entries in enumerate(buckets):
            if not entries:
                continue
            loop = self._loops[idx]
            tasks = self._task_sets[idx]

            def _schedule_shard(
                target_loop: asyncio.AbstractEventLoop = loop,
                target_tasks: set[asyncio.Task[Any]] = tasks,
                shard_entries: list[tuple[Coroutine[Any, Any, T], concurrent.futures.Future[T]]] = entries,
            ) -> None:
                discard = target_tasks.discard
                for c, f in shard_entries:
                    if f.cancelled():
                        c.close()
                        continue
                    try:
                        task = target_loop.create_task(c)
                    except Exception as exc:
                        if not f.done():
                            f.set_exception(exc)
                        continue

                    target_tasks.add(task)
                    task.add_done_callback(discard)

                    def _on_done(t: asyncio.Task[T], fut: concurrent.futures.Future[T] = f) -> None:
                        try:
                            if t.cancelled():
                                fut.cancel()
                            elif exc := t.exception():
                                fut.set_exception(exc)
                            else:
                                fut.set_result(t.result())
                        except Exception as exc2:
                            if not fut.done():
                                fut.set_exception(exc2)

                    task.add_done_callback(_on_done)

                    def _cancel_task(
                        done: concurrent.futures.Future[T],
                        target_task: asyncio.Task[T] = task,
                        owner_loop: asyncio.AbstractEventLoop = target_loop,
                    ) -> None:
                        if done.cancelled() and not owner_loop.is_closed():
                            owner_loop.call_soon_threadsafe(target_task.cancel)

                    f.add_done_callback(_cancel_task)

            loop.call_soon_threadsafe(_schedule_shard)

        return futures

    def attach_recorder(self, recorder: EventLoopRecorder, *, interval_seconds: float = 1.0) -> None:
        """Start sampling scheduling lag and active task counts on every shard into `recorder`.

        Each shard arms a self-rescheduling `loop.call_later` timer on its own loop. When the timer
        fires, the lag is the actual `time.monotonic()` fire time minus the expected one, and the
        shard records an `EventLoopMetricSample` on its loop thread. For `zuvloop` loops the shard also calls
        `recorder.configure_zuvloop` with the same interval. An inline loop starts sampling the first
        time it runs, and lag is only meaningful while that loop runs continuously. Sampling stops
        and every pending timer is cancelled when the pool stops.

        Args:
            recorder: Recorder receiving one sample per shard per interval.
            interval_seconds: Sampling interval in seconds; must be greater than zero.

        Raises:
            ValueError: When `interval_seconds` is not greater than zero.
            RuntimeError: When the pool is stopped or already has a recorder attached.
        """
        if not interval_seconds > 0:
            msg = "EventLoopPool recorder interval_seconds must be > 0"
            raise ValueError(msg)
        if self._stopped:
            msg = "Cannot attach a recorder to a stopped EventLoopPool"
            raise RuntimeError(msg)
        if self._recorder is not None:
            msg = "EventLoopPool already has a recorder attached"
            raise RuntimeError(msg)

        self._recorder_backend = resolve_loop_backend(
            self._loop_backend,
            quarantined_modules=self._quarantined_modules,
        ).value
        self._recorder_interval = float(interval_seconds)
        self._recorder = recorder
        for shard_idx, loop in enumerate(self._loops):
            if self._inline_single and not loop.is_running():
                loop.call_soon(self._start_shard_sampler, shard_idx)
            else:
                loop.call_soon_threadsafe(self._start_shard_sampler, shard_idx)

    def stop(self) -> None:
        """Drain and stop every loop shard, closing each loop only after its thread exits.

        Each running shard cancels its pending tasks, waits for the cancellations, and runs
        ``shutdown_asyncgens`` before the loop stops. A loop whose thread is still alive after the
        join timeout is logged and left open rather than closed under a running thread.
        """
        if self._stopped:
            return
        self._stopped = True
        self._cancel_shard_samplers()

        if self._inline_single:
            loop = self._loops[0]
            if loop.is_closed():
                return
            self._close_on_exit = True
            if loop.is_running():
                if self._running_loop_or_none() is loop:
                    self._inline_drain_task = loop.create_task(
                        self._drain_and_stop_inline(loop),
                        name="vertebra.loop.shard.0.drain",
                    )
                else:
                    drain_coro = self._drain_loop()
                    try:
                        drain = asyncio.run_coroutine_threadsafe(drain_coro, loop)
                    except RuntimeError:
                        drain_coro.close()
                    else:
                        try:
                            drain.result(timeout=_STOP_TIMEOUT_SECONDS)
                        except TimeoutError:
                            logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
                        except concurrent.futures.CancelledError:
                            pass
                        finally:
                            self._inline_drained = True
                        with suppress(RuntimeError):
                            loop.call_soon_threadsafe(loop.stop)
            else:
                self._close_on_exit = False
                try:
                    drain_task = loop.create_task(
                        asyncio.wait_for(self._drain_loop(), _STOP_TIMEOUT_SECONDS),
                        name="vertebra.loop.shard.0.drain",
                    )
                    loop.run_until_complete(drain_task)
                except TimeoutError:
                    logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
                finally:
                    self._inline_drained = True
                    self._task_sets[0].clear()
                if not loop.is_closed():
                    loop.close()
            return

        for idx, loop in enumerate(self._loops):
            if not loop.is_running():
                continue
            drain = asyncio.run_coroutine_threadsafe(self._drain_loop(), loop)
            try:
                drain.result(timeout=_STOP_TIMEOUT_SECONDS)
            except TimeoutError:
                logger.warning("Event loop shard %d did not drain within %.1fs", idx, _STOP_TIMEOUT_SECONDS)
            loop.call_soon_threadsafe(loop.stop)

        for thread in self._threads:
            thread.join(timeout=_STOP_TIMEOUT_SECONDS)

        for loop, thread in zip(self._loops, self._threads, strict=True):
            if thread.is_alive():
                logger.warning("Event loop thread %s is still running; leaving its loop open", thread.name)
                continue
            if not loop.is_closed():
                loop.close()

    def _start_shard_sampler(self, shard_idx: int) -> None:
        """Configure native zuvloop telemetry and arm the first lag timer on the shard's own loop."""
        recorder = self._recorder
        if recorder is None or self._stopped or self._sampler_handles[shard_idx] is not None:
            return
        loop = self._loops[shard_idx]
        if self._recorder_backend == LoopBackend.ZUVLOOP.value:
            recorder.configure_zuvloop(loop, metrics_interval_ms=max(1, round(self._recorder_interval * 1000.0)))
        self._arm_shard_sampler(shard_idx, loop)

    def _arm_shard_sampler(self, shard_idx: int, loop: asyncio.AbstractEventLoop) -> None:
        """Schedule the next lag sample for a shard and remember its timer handle."""
        interval = self._recorder_interval
        self._sampler_handles[shard_idx] = loop.call_later(
            interval,
            self._sample_shard,
            shard_idx,
            time.monotonic() + interval,
        )

    def _sample_shard(self, shard_idx: int, expected_at: float) -> None:
        """Record one lag and active-task sample on the shard's loop thread, then re-arm the timer."""
        self._sampler_handles[shard_idx] = None
        recorder = self._recorder
        if recorder is None or self._stopped:
            return
        loop = self._loops[shard_idx]
        lag_ms = max(0.0, (time.monotonic() - expected_at) * 1000.0)
        try:
            recorder.record(
                EventLoopMetricSample(
                    shard_id=shard_idx,
                    backend=self._recorder_backend,
                    lag_ms=lag_ms,
                    active_tasks=len(asyncio.all_tasks(loop)),
                )
            )
        except Exception:
            logger.exception("Event loop shard %d telemetry sample failed", shard_idx)
        self._arm_shard_sampler(shard_idx, loop)

    def _cancel_shard_sampler(self, shard_idx: int) -> None:
        """Cancel a shard's pending lag timer; runs on the shard's loop thread or on an idle inline loop."""
        handle = self._sampler_handles[shard_idx]
        self._sampler_handles[shard_idx] = None
        if handle is not None:
            handle.cancel()

    def _cancel_shard_samplers(self) -> None:
        """Cancel every shard's pending lag timer, hopping onto running shard loops owned by other threads."""
        if self._recorder is None:
            return
        running = self._running_loop_or_none()
        for shard_idx, loop in enumerate(self._loops):
            if loop.is_closed():
                continue
            if loop.is_running() and loop is not running:
                with suppress(RuntimeError):
                    loop.call_soon_threadsafe(self._cancel_shard_sampler, shard_idx)
            else:
                self._cancel_shard_sampler(shard_idx)

    def _install_shard_task_naming(self, shard_idx: int, loop: asyncio.AbstractEventLoop) -> None:
        counter = self._task_counters[shard_idx]
        pending_name: str | None = None
        orig_create_task = loop.create_task
        prefix = f"vertebra.loop.shard.{shard_idx}.task."
        drain_name = f"vertebra.loop.shard.{shard_idx}.drain"

        def _default_name_for_coro(coro: Any) -> str:
            code = getattr(coro, "cr_code", None)
            if code is _CAPTURE_SNAPSHOTS_CODE:
                return _TASKZ_PROBE_TASK_NAME
            if code is EventLoopPool._drain_loop.__code__:
                return drain_name
            return f"{prefix}{next(counter)}"

        if hasattr(asyncio, "create_eager_task_factory"):

            def _build_shard_task(
                coro: Any,
                *,
                loop: asyncio.AbstractEventLoop | None = None,
                name: str | None = None,
                context: Any = None,
                eager_start: bool = False,
            ) -> asyncio.Task[Any]:
                nonlocal pending_name
                resolved = name
                if resolved is None:
                    if pending_name is not None:
                        resolved = pending_name
                        pending_name = None
                    else:
                        resolved = _default_name_for_coro(coro)
                else:
                    pending_name = None
                target = loop if loop is not None else orig_loop
                return asyncio.Task(
                    coro,
                    loop=target,
                    name=resolved,
                    context=context,
                    eager_start=eager_start,
                )

            orig_loop = loop
            with suppress(AttributeError, NotImplementedError, TypeError):
                loop.set_task_factory(asyncio.create_eager_task_factory(_build_shard_task))

        def _named_create_task(
            coro: Any,
            *,
            name: str | None = None,
            context: Any = None,
        ) -> asyncio.Task[Any]:
            nonlocal pending_name
            resolved = name if name is not None else _default_name_for_coro(coro)
            prev = pending_name
            pending_name = resolved
            try:
                if context is None:
                    return orig_create_task(coro, name=resolved)
                return orig_create_task(coro, name=resolved, context=context)
            finally:
                pending_name = prev

        loop_any: Any = loop
        with suppress(AttributeError, TypeError):
            loop_any.create_task = _named_create_task

    def _install_inline_exit_hook(self, loop: asyncio.AbstractEventLoop) -> None:
        orig_run_forever = loop.run_forever
        orig_run_until_complete = loop.run_until_complete

        def _finalize_exit() -> None:
            if self._inline_run_depth != 0 or not self._close_on_exit or loop.is_running() or loop.is_closed():
                return
            self._close_on_exit = False
            if not self._inline_drained:
                self._inline_drained = True
                try:
                    if self._inline_drain_task is not None and not self._inline_drain_task.done():
                        orig_run_until_complete(self._inline_drain_task)
                    else:
                        drain_task = loop.create_task(
                            asyncio.wait_for(self._drain_loop(), _STOP_TIMEOUT_SECONDS),
                            name="vertebra.loop.shard.0.drain",
                        )
                        orig_run_until_complete(drain_task)
                except TimeoutError:
                    logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
            self._inline_drain_task = None
            self._task_sets[0].clear()
            if not loop.is_closed():
                loop.close()

        def _wrapped_run_forever() -> None:
            self._inline_run_depth += 1
            try:
                orig_run_forever()
            finally:
                self._inline_run_depth -= 1
                _finalize_exit()

        def _wrapped_run_until_complete(future: Any) -> Any:
            self._inline_run_depth += 1
            try:
                return orig_run_until_complete(future)
            finally:
                self._inline_run_depth -= 1
                _finalize_exit()

        loop_any: Any = loop
        with suppress(AttributeError, TypeError):
            loop_any.run_forever = _wrapped_run_forever
        with suppress(AttributeError, TypeError):
            loop_any.run_until_complete = _wrapped_run_until_complete

    async def _drain_and_stop_inline(self, loop: asyncio.AbstractEventLoop) -> None:
        await asyncio.sleep(0)
        try:
            await asyncio.wait_for(self._drain_loop(), _STOP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
        finally:
            self._inline_drained = True
            if loop.is_running():
                loop.stop()

    def _dispatch_shard_coros(
        self,
        shard_idx: int,
        items: list[Coroutine[Any, Any, Any]],
        running: asyncio.AbstractEventLoop | None,
        *,
        task_name: str | None = None,
    ) -> None:
        if not items:
            return
        loop = self._loops[shard_idx]
        if running is loop:
            self._create_tracked_tasks(shard_idx, items, task_name=task_name)
            return
        if self._inline_single and not loop.is_running():
            driver = loop.create_task(
                self._run_tracked_inline(0, items, task_name=task_name),
                name="vertebra.loop.shard.0.inline_driver",
            )
            loop.run_until_complete(driver)
            return
        loop.call_soon_threadsafe(self._create_tracked_tasks, shard_idx, items, task_name)

    async def _run_tracked_inline(
        self,
        shard_idx: int,
        items: list[Coroutine[Any, Any, Any]],
        *,
        task_name: str | None = None,
    ) -> None:
        tasks = self._create_tracked_tasks(shard_idx, items, task_name=task_name)
        if tasks:
            await asyncio.gather(*tasks)

    def _create_tracked_tasks(
        self,
        shard_idx: int,
        items: list[Coroutine[Any, Any, Any]],
        task_name: str | None = None,
    ) -> list[asyncio.Task[Any]]:
        target_loop = self._loops[shard_idx]
        target_tasks = self._task_sets[shard_idx]
        discard = target_tasks.discard
        created: list[asyncio.Task[Any]] = []
        for item in items:
            task = (
                target_loop.create_task(item, name=task_name)
                if task_name is not None
                else target_loop.create_task(item)
            )
            if not task.done():
                target_tasks.add(task)
                task.add_done_callback(discard)
            created.append(task)
        return created

    @staticmethod
    def _running_loop_or_none() -> asyncio.AbstractEventLoop | None:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    @staticmethod
    async def _drain_loop() -> None:
        """Cancel every other task on the running loop, await the cancellations, and finalize async generators."""
        current = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.get_running_loop().shutdown_asyncgens()

    @staticmethod
    def _run_loop_shard(loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()


def _collect_quarantined_prefixes(quarantined_modules: frozenset[str] | tuple[str, ...]) -> set[str]:
    """Normalize explicit and ``VERTEBRA_QUARANTINED_LOOPS`` entries into lowercase module prefixes."""
    prefixes: set[str] = {mod.split("==", 1)[0].strip().lower() for mod in quarantined_modules if mod}
    env_quarantined = os.environ.get("VERTEBRA_QUARANTINED_LOOPS", "").strip()
    if env_quarantined:
        for token in env_quarantined.split(","):
            cleaned = token.split("==", 1)[0].strip().lower()
            if cleaned:
                prefixes.add(cleaned)
    return prefixes


def _is_backend_available(spec: LoopBackendSpec, quarantined_prefixes: set[str]) -> bool:
    """Return True when ``spec.module_name`` is neither quarantined nor missing."""
    return spec.module_name not in quarantined_prefixes and import_optional(spec.module_name) is not None


def _build_task_snapshot(
    task: asyncio.Task[Any],
    *,
    shard_id: int | None = None,
) -> LoopTaskSnapshot:
    """Build a ``LoopTaskSnapshot`` for ``task`` including its call graph or stack fallback."""
    coro: Any = task.get_coro()
    coro_qualname = str(
        getattr(coro, "__qualname__", None) or getattr(coro, "__name__", None) or type(coro).__qualname__
    )
    return LoopTaskSnapshot(
        task_name=task.get_name(),
        coro_qualname=coro_qualname,
        done=task.done(),
        cancelled=task.cancelled(),
        shard_id=shard_id,
        call_graph=_format_task_call_graph(task),
    )


def _format_task_call_graph(task: asyncio.Task[Any]) -> str:
    """Format the Python 3.14+ async call graph for ``task`` or fall back to ``get_stack(limit=8)``."""
    capture_cg = getattr(asyncio, "capture_call_graph", None)
    format_cg = getattr(asyncio, "format_call_graph", None)

    captured_graph: Any = None
    if callable(capture_cg):
        with suppress(Exception):
            captured_graph = capture_cg(task)

    if callable(format_cg):
        try:
            formatted = format_cg(task)
        except Exception:
            formatted = ""
        if isinstance(formatted, str) and formatted.strip():
            return formatted.rstrip()

    if captured_graph is not None:
        rendered = _render_captured_graph(task, captured_graph)
        if rendered:
            return rendered

    try:
        frames = task.get_stack(limit=8)
    except Exception:
        frames = []

    lines = [f"* Task(name={task.get_name()!r})"]
    for frame in frames:
        qualname = getattr(frame.f_code, "co_qualname", frame.f_code.co_name)
        lines.append(f"  File {frame.f_code.co_filename!r}, line {frame.f_lineno}, in {qualname}")
    return "\n".join(lines)


def _render_captured_graph(task: asyncio.Task[Any], graph: Any) -> str:
    """Format a Python 3.14+ ``FutureCallGraph`` object when ``format_call_graph`` is unavailable."""
    call_stack = getattr(graph, "call_stack", None)
    if not isinstance(call_stack, tuple | list):
        return ""
    lines = [f"* Task(name={task.get_name()!r})"]
    for entry in call_stack[:8]:
        frame = getattr(entry, "frame", None)
        if frame is None:
            continue
        code = getattr(frame, "f_code", None)
        if code is None:
            continue
        qualname = getattr(code, "co_qualname", getattr(code, "co_name", "<unknown>"))
        lines.append(f"  File {code.co_filename!r}, line {frame.f_lineno}, in {qualname}")
    return "\n".join(lines)


async def _capture_loop_task_snapshots_remote(
    loop: asyncio.AbstractEventLoop,
    *,
    shard_id: int | None = None,
) -> list[LoopTaskSnapshot]:
    """Capture ``LoopTaskSnapshot`` entries on ``loop`` while excluding this diagnostic probe task."""
    probe = asyncio.current_task()
    if probe is not None:
        probe.set_name(_TASKZ_PROBE_TASK_NAME)
    snapshots = capture_loop_task_snapshots(loop, shard_id=shard_id)
    if probe is None:
        return snapshots
    return [snap for snap in snapshots if snap.task_name != _TASKZ_PROBE_TASK_NAME]


_CAPTURE_SNAPSHOTS_CODE = _capture_loop_task_snapshots_remote.__code__
