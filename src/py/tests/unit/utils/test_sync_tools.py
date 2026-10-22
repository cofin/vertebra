"""Unit tests for SyncPool, run_, async_, LoopShardedResource, CapacityLimiter, and format_json."""

from __future__ import annotations

import asyncio

import vertebra.utils.sync_tools as sync_tools_mod
from vertebra.utils.portal import PortalProvider
from vertebra.utils.serializers import decode_json, format_json
from vertebra.utils.sync_tools import (
    CapacityLimiter,
    LoopShardedResource,
    SyncPool,
    async_,
    run_,
)


def test_sync_pool_and_run_and_async_wrappers() -> None:
    """Verify SyncPool, run_, and async_ execute synchronous and asynchronous callables."""
    pool = SyncPool(max_workers=2)
    try:
        assert pool.max_workers == 2
        fut = pool.submit(lambda x, y: x + y, 20, 22)
        assert fut.result(timeout=5.0) == 42
    finally:
        pool.shutdown()

    async def _compute_async(val: int) -> int:
        await asyncio.sleep(0)
        return val * 2

    blocking_fn = run_(_compute_async)
    assert blocking_fn(21) == 42

    async def _run_wrapped() -> int:
        async_fn = async_(lambda v: v + 10)
        return await async_fn(32)

    assert asyncio.run(_run_wrapped()) == 42


def test_loop_sharded_resource_and_capacity_limiter() -> None:
    """Verify LoopShardedResource isolates state per loop and CapacityLimiter bounds concurrency."""
    resource: LoopShardedResource[list[str]] = LoopShardedResource(lambda: ["init"])
    limiter = CapacityLimiter(2)

    async def _append_and_count(item: str) -> int:
        async with limiter:
            bucket = resource.get()
            bucket.append(item)
            return len(bucket)

    assert asyncio.run(_append_and_count("a")) == 2
    assert asyncio.run(_append_and_count("b")) == 2


def test_format_json_pretty_prints_payload() -> None:
    """Verify format_json returns indented UTF-8 JSON text."""
    formatted = format_json({"b": 2, "a": [1, 2]}, indent=2)
    assert "\n  " in formatted
    assert decode_json(formatted) == {"b": 2, "a": [1, 2]}


def test_portal_provider_default_and_call() -> None:
    """Verify PortalProvider.default() creates a singleton portal and runs callables."""
    portal = PortalProvider.default()
    assert portal is not None
    assert PortalProvider.default() is portal

    async def _add(a: int, b: int) -> int:
        return a + b

    result = portal.call(_add, 10, 32)
    assert result == 42


def test_sync_tools_dead_functions_removed() -> None:
    """Verify dead sync utility functions and sentinels have been removed."""
    for dead_name in (
        "NoValue",
        "NO_VALUE",
        "await_",
        "ensure_async_",
        "with_ensure_async_",
        "get_next",
        "_Awaiter",
    ):
        assert not hasattr(sync_tools_mod, dead_name)
