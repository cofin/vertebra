"""Multi-core runtime tuning, CPU affinity pinning, and sharded event loop management."""

import asyncio
import concurrent.futures
import itertools
import logging
import os
import sys
import threading
from collections.abc import Coroutine, Iterable
from contextlib import suppress
from enum import StrEnum
from typing import Any, Literal

import msgspec

from vertebra.server._gil_audit import GilAuditReport
from vertebra.server.platform import CgroupQuota
from vertebra.utils.module_loader import import_optional

__all__ = (
    "EventLoopPool",
    "LoopBackend",
    "LoopBackendSpec",
    "configure_worker_affinity",
    "create_event_loop",
    "resolve_loop_backend",
    "resolve_loop_shards",
)

logger = logging.getLogger(__name__)
_SHARD_START_TIMEOUT_SECONDS: float = 5.0
_STOP_TIMEOUT_SECONDS: float = 5.0


class LoopBackend(StrEnum):
    """Supported asyncio event loop backend implementations."""

    AUTO = "auto"
    UVLOOP = "uvloop"
    WINLOOP = "winloop"
    RLOOP = "rloop"
    ASYNCIO = "asyncio"


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


_LOOP_BACKEND_SPECS: tuple[LoopBackendSpec, ...] = (
    LoopBackendSpec(backend=LoopBackend.WINLOOP, module_name="winloop", platforms="win32"),
    LoopBackendSpec(backend=LoopBackend.UVLOOP, module_name="uvloop", platforms="posix"),
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
                loop.set_task_factory(_eager_task_factory)
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


class EventLoopPool:
    """Sharded pool of persistent event loops with optional inline single-thread mode."""

    __slots__ = (
        "_counter",
        "_inline_single",
        "_loop_backend",
        "_loops",
        "_quarantined_modules",
        "_size",
        "_stopped",
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
    ) -> None:
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
        self._counter = itertools.count()
        self._stopped = False

        if self._inline_single:
            self._loops.append(
                create_event_loop(
                    backend=self._loop_backend,
                    quarantined_modules=self._quarantined_modules,
                )
            )
            self._task_sets.append(set())
            return

        for shard_idx in range(resolved_size):
            ready = threading.Event()
            loop = create_event_loop(
                backend=self._loop_backend,
                quarantined_modules=self._quarantined_modules,
            )
            self._loops.append(loop)
            self._task_sets.append(set())
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
    ) -> None:
        """Schedule a fire-and-forget coroutine onto the target or next event loop shard."""
        idx = (worker_index if worker_index is not None else next(self._counter)) % self._size
        self._dispatch_shard_coros(idx, [coro], self._running_loop_or_none())

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
    ) -> concurrent.futures.Future[T]:
        """Schedule a coroutine onto a specific or round-robin shard and return its Future."""
        idx = (worker_index if worker_index is not None else next(self._counter)) % self._size
        loop = self._loops[idx]
        return asyncio.run_coroutine_threadsafe(coro, loop)

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

    def stop(self) -> None:
        """Drain and stop every loop shard, closing each loop only after its thread exits.

        Each running shard cancels its pending tasks, waits for the cancellations, and runs
        ``shutdown_asyncgens`` before the loop stops. A loop whose thread is still alive after the
        join timeout is logged and left open rather than closed under a running thread.
        """
        if self._stopped:
            return
        self._stopped = True

        if self._inline_single:
            loop = self._loops[0]
            if loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            elif not loop.is_closed():
                try:
                    loop.run_until_complete(asyncio.wait_for(self._drain_loop(), _STOP_TIMEOUT_SECONDS))
                except TimeoutError:
                    logger.warning("Inline event loop did not drain within %.1fs", _STOP_TIMEOUT_SECONDS)
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

    def _dispatch_shard_coros(
        self,
        shard_idx: int,
        items: list[Coroutine[Any, Any, Any]],
        running: asyncio.AbstractEventLoop | None,
    ) -> None:
        if not items:
            return
        loop = self._loops[shard_idx]
        tasks = self._task_sets[shard_idx]
        if running is loop:
            self._create_tracked_tasks(loop, tasks, items)
            return
        loop.call_soon_threadsafe(self._create_tracked_tasks, loop, tasks, items)

    @staticmethod
    def _create_tracked_tasks(
        target_loop: asyncio.AbstractEventLoop,
        target_tasks: set[asyncio.Task[Any]],
        items: list[Coroutine[Any, Any, Any]],
    ) -> None:
        discard = target_tasks.discard
        for item in items:
            task = target_loop.create_task(item)
            target_tasks.add(task)
            task.add_done_callback(discard)

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


def _eager_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Any,
    **kwargs: Any,
) -> asyncio.Task[Any]:
    """Execute task immediately via asyncio.eager_task_factory with safe argument absorption."""
    kwargs.pop("eager_start", None)
    return asyncio.eager_task_factory(loop, coro, **kwargs)
