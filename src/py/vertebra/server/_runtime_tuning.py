"""Multi-core runtime tuning, CPU affinity pinning, and sharded event loop management."""

from __future__ import annotations

import asyncio
import concurrent.futures
import itertools
import os
import sys
import threading
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine, Iterable

__all__ = (
    "AsgiEventLoopPool",
    "configure_worker_affinity",
    "is_free_threaded",
)


def is_free_threaded() -> bool:
    """Return True when running on a free-threaded Python interpreter with the GIL disabled."""
    checker = getattr(sys, "_is_gil_enabled", None)
    return bool(checker is not None and not checker())


def configure_worker_affinity(worker_index: int, *, enable_pinning: bool = False) -> bool:
    """Pin the calling worker process or thread to ``worker_index % os.cpu_count()`` on Linux."""
    if not enable_pinning or not hasattr(os, "sched_setaffinity"):
        return False
    cpu_count = os.cpu_count() or 1
    target_core = abs(worker_index) % cpu_count
    try:
        os.sched_setaffinity(0, {target_core})
    except OSError:
        return False
    return True


class AsgiEventLoopPool:
    """Sharded pool of persistent asyncio event loop threads (one loop per worker shard)."""

    __slots__ = ("_counter", "_loops", "_size", "_stopped", "_task_sets", "_threads")

    def __init__(self, size: int | None = None) -> None:
        resolved_size = size if size is not None else (os.cpu_count() or 1)
        if resolved_size < 1:
            msg = "AsgiEventLoopPool size must be >= 1"
            raise ValueError(msg)

        self._size = resolved_size
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._threads: list[threading.Thread] = []
        self._task_sets: list[set[asyncio.Task[Any]]] = []
        self._counter = itertools.count()
        self._stopped = False

        for shard_idx in range(resolved_size):
            ready = threading.Event()
            loop = asyncio.new_event_loop()
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
    def loop(self) -> asyncio.AbstractEventLoop:
        """Return the next event loop shard via round-robin selection."""
        return self.next_loop()

    def get_loop(self, worker_index: int) -> asyncio.AbstractEventLoop:
        """Return the event loop shard assigned to ``worker_index``."""
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

        def _callback() -> None:
            task = loop.create_task(coro)
            tasks.add(task)
            task.add_done_callback(tasks.discard)

        loop.call_soon_threadsafe(_callback)

    def spawn_batch(self, coros: Iterable[Coroutine[Any, Any, Any]]) -> None:
        """Partition and schedule a batch of coroutines across all event loop shards."""
        buckets: list[list[Coroutine[Any, Any, Any]]] = [[] for _ in range(self._size)]
        for coro in coros:
            idx = next(self._counter) % self._size
            buckets[idx].append(coro)

        for idx, shard_coros in enumerate(buckets):
            if not shard_coros:
                continue
            loop = self._loops[idx]
            tasks = self._task_sets[idx]

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
        future: concurrent.futures.Future[T] = concurrent.futures.Future()

        def _callback() -> None:
            async def _wrap() -> None:
                try:
                    future.set_result(await coro)
                except Exception as exc:
                    future.set_exception(exc)

            loop.create_task(_wrap())

        loop.call_soon_threadsafe(_callback)
        return future

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

            def _callback(
                target_loop: asyncio.AbstractEventLoop = loop,
                shard_entries: list[tuple[Coroutine[Any, Any, T], concurrent.futures.Future[T]]] = entries,
            ) -> None:
                for c, f in shard_entries:

                    async def _wrap(
                        coro_item: Coroutine[Any, Any, T] = c,
                        fut_item: concurrent.futures.Future[T] = f,
                    ) -> None:
                        try:
                            fut_item.set_result(await coro_item)
                        except Exception as exc:
                            fut_item.set_exception(exc)

                    target_loop.create_task(_wrap())

            loop.call_soon_threadsafe(_callback)

        return futures

    def stop(self) -> None:
        """Stop all event loop shards and join their background threads."""
        if self._stopped:
            return
        self._stopped = True

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
