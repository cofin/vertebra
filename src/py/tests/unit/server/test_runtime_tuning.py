"""Unit tests for AsgiEventLoopPool sharding, CPU affinity pinning, and 3.14t detection."""

from __future__ import annotations

import asyncio
import sys
import threading
from unittest.mock import patch

import pytest

from vertebra.server._runtime_tuning import (
    AsgiEventLoopPool,
    configure_worker_affinity,
    is_free_threaded,
)


def test_is_free_threaded_reflects_interpreter_gil_state() -> None:
    """Verify is_free_threaded matches sys._is_gil_enabled when present."""
    checker = getattr(sys, "_is_gil_enabled", None)
    expected = bool(checker is not None and not checker())
    assert is_free_threaded() is expected


def test_asgi_event_loop_pool_shards_across_distinct_event_loops() -> None:
    """Verify AsgiEventLoopPool starts N distinct running event loops and shards coroutines."""
    pool = AsgiEventLoopPool(size=4)
    try:
        assert pool.size == 4
        loops = [pool.get_loop(i) for i in range(4)]
        assert len({id(loop) for loop in loops}) == 4
        assert all(loop.is_running() for loop in loops)
        assert pool.get_loop(4) is loops[0]
        assert pool.get_loop(5) is loops[1]

        async def _capture_loop_and_thread(idx: int) -> tuple[int, int, int]:
            running = asyncio.get_running_loop()
            await asyncio.sleep(0.001)
            return (idx, id(running), threading.get_ident())

        futures = [pool.schedule(_capture_loop_and_thread(i), worker_index=i) for i in range(4)]
        results = [fut.result(timeout=2.0) for fut in futures]

        loop_ids = {res[1] for res in results}
        thread_ids = {res[2] for res in results}
        assert len(loop_ids) == 4
        assert len(thread_ids) == 4

        batch_futures = pool.schedule_batch([_capture_loop_and_thread(i) for i in range(8)])
        batch_results = [fut.result(timeout=2.0) for fut in batch_futures]
        assert len({res[1] for res in batch_results}) == 4
    finally:
        pool.stop()

    assert all(not loop.is_running() for loop in loops)


def test_asgi_event_loop_pool_rejects_invalid_size() -> None:
    """Verify AsgiEventLoopPool raises ValueError when size < 1."""
    with pytest.raises(ValueError, match="size must be >= 1"):
        AsgiEventLoopPool(size=0)


def test_configure_worker_affinity_disabled_and_enabled() -> None:
    """Verify configure_worker_affinity respects enable_pinning and targets worker_index % cpu_count."""
    assert configure_worker_affinity(0, enable_pinning=False) is False

    with (
        patch("os.cpu_count", return_value=4),
        patch("os.sched_setaffinity", create=True) as mock_setaffinity,
    ):
        assert configure_worker_affinity(6, enable_pinning=True) is True
        mock_setaffinity.assert_called_once_with(0, {2})

    with (
        patch("os.cpu_count", return_value=4),
        patch("os.sched_setaffinity", side_effect=OSError("permission denied"), create=True),
    ):
        assert configure_worker_affinity(1, enable_pinning=True) is False
