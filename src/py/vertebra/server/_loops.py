"""Multi-core runtime tuning, CPU affinity pinning, and sharded event loop management."""

import asyncio
import concurrent.futures
import itertools
import os
import sys
import threading
from collections.abc import Coroutine, Iterable
from typing import Any

from vertebra.server._gil_audit import GilAuditReport
from vertebra.server.platform import CgroupQuota
from vertebra.utils.module_loader import import_optional

__all__ = (
    "EventLoopPool",
    "configure_worker_affinity",
    "create_event_loop",
    "resolve_loop_shards",
)


def create_event_loop() -> asyncio.AbstractEventLoop:
    """Create a high-performance event loop preferring uvloop on POSIX and winloop on Windows."""
    loop: asyncio.AbstractEventLoop | None = None
    if sys.platform == "win32":
        winloop = import_optional("winloop")
        if winloop is not None:
            loop = winloop.new_event_loop()
    else:
        uvloop = import_optional("uvloop")
        if uvloop is not None:
            loop = uvloop.new_event_loop()
    if loop is None:
        loop = asyncio.new_event_loop()
    if hasattr(asyncio, "eager_task_factory"):
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


class EventLoopPool:
    """Sharded pool of persistent event loops with optional inline single-thread mode."""

    __slots__ = ("_counter", "_inline_single", "_loops", "_size", "_stopped", "_task_sets", "_threads")

    def __init__(self, size: int | None = None, *, inline_single_loop: bool = False) -> None:
        resolved_size = size if size is not None else resolve_loop_shards()
        if resolved_size < 1:
            msg = "EventLoopPool size must be >= 1"
            raise ValueError(msg)

        self._size = resolved_size
        self._inline_single = inline_single_loop and resolved_size == 1
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._threads: list[threading.Thread] = []
        self._task_sets: list[set[asyncio.Task[Any]]] = []
        self._counter = itertools.count()
        self._stopped = False

        if self._inline_single:
            self._loops.append(create_event_loop())
            self._task_sets.append(set())
            return

        for shard_idx in range(resolved_size):
            ready = threading.Event()
            loop = create_event_loop()
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
            ready.wait(timeout=5.0)

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
        loop = self._loops[idx]
        tasks = self._task_sets[idx]
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running is loop:
            task = loop.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            return

        def _callback() -> None:
            task = loop.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        loop.call_soon_threadsafe(_callback)

    def spawn_batch(self, coros: Iterable[Coroutine[Any, Any, Any]]) -> None:
        """Partition and schedule a batch of coroutines with at most one wakeup per loop shard."""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if self._size == 1:
            batch = coros if isinstance(coros, list) else list(coros)
            if not batch:
                return
            loop = self._loops[0]
            tasks = self._task_sets[0]
            if running is loop:
                discard = tasks.discard
                for item in batch:
                    task = loop.create_task(item)
                    tasks.add(task)
                    task.add_done_callback(discard)
                return

            def _single_callback(
                target_loop: asyncio.AbstractEventLoop = loop,
                target_tasks: set[asyncio.Task[Any]] = tasks,
                items: list[Coroutine[Any, Any, Any]] = batch,
            ) -> None:
                discard = target_tasks.discard
                for item in items:
                    task = target_loop.create_task(item)
                    target_tasks.add(task)
                    task.add_done_callback(discard)

            loop.call_soon_threadsafe(_single_callback)
            return

        buckets: list[list[Coroutine[Any, Any, Any]]] = [[] for _ in range(self._size)]
        for coro in coros:
            idx = next(self._counter) % self._size
            buckets[idx].append(coro)

        for idx, shard_coros in enumerate(buckets):
            if not shard_coros:
                continue
            loop = self._loops[idx]
            tasks = self._task_sets[idx]
            if running is loop:
                discard = tasks.discard
                for item in shard_coros:
                    task = loop.create_task(item)
                    tasks.add(task)
                    task.add_done_callback(discard)
                continue

            def _callback(
                target_loop: asyncio.AbstractEventLoop = loop,
                target_tasks: set[asyncio.Task[Any]] = tasks,
                batch: list[Coroutine[Any, Any, Any]] = shard_coros,
            ) -> None:
                discard = target_tasks.discard
                for item in batch:
                    task = target_loop.create_task(item)
                    target_tasks.add(task)
                    task.add_done_callback(discard)

            loop.call_soon_threadsafe(_callback)

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

            def _schedule_shard(
                target_loop: asyncio.AbstractEventLoop = loop,
                shard_entries: list[tuple[Coroutine[Any, Any, T], concurrent.futures.Future[T]]] = entries,
            ) -> None:
                for c, f in shard_entries:
                    task = target_loop.create_task(c)

                    def _on_done(t: asyncio.Task[T], fut: concurrent.futures.Future[T] = f) -> None:
                        if t.cancelled():
                            fut.cancel()
                        elif exc := t.exception():
                            fut.set_exception(exc)
                        else:
                            fut.set_result(t.result())

                    task.add_done_callback(_on_done)

            loop.call_soon_threadsafe(_schedule_shard)

        return futures

    def stop(self) -> None:
        """Stop all event loop shards and join their background threads."""
        if self._stopped:
            return
        self._stopped = True

        if self._inline_single:
            loop = self._loops[0]
            if loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            elif not loop.is_closed():
                loop.close()
            return

        for idx, loop in enumerate(self._loops):
            if not loop.is_running():
                continue
            tasks = self._task_sets[idx]

            def _shutdown(
                target_loop: asyncio.AbstractEventLoop = loop,
                target_tasks: set[asyncio.Task[Any]] = tasks,
            ) -> None:
                async def _wait_tasks() -> None:
                    if target_tasks:
                        await asyncio.wait(list(target_tasks), timeout=2.0)
                    target_loop.stop()

                target_loop.create_task(_wait_tasks())

            loop.call_soon_threadsafe(_shutdown)

        for thread in self._threads:
            thread.join(timeout=5.0)

        for loop in self._loops:
            if not loop.is_closed():
                loop.close()

    @staticmethod
    def _run_loop_shard(loop: asyncio.AbstractEventLoop, ready: threading.Event) -> None:
        asyncio.set_event_loop(loop)
        loop.call_soon(ready.set)
        loop.run_forever()
