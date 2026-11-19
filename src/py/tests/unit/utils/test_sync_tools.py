"""Unit tests for SyncPool, run_, async_, loop-sharded resources, CapacityLimiter, and format_json."""

from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

import pytest

from vertebra.server._loops import create_event_loop
from vertebra.utils import sync_tools as sync_tools_mod
from vertebra.utils.module_loader import import_optional
from vertebra.utils.portal import PortalProvider
from vertebra.utils.serializers import decode_json, format_json
from vertebra.utils.sync_tools import (
    AsyncLoopShardedResource,
    CapacityLimiter,
    SyncLoopShardedResource,
    SyncPool,
    async_,
    run_,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture
def sync_pool() -> Iterator[SyncPool]:
    """Provide a bounded SyncPool instance and shut it down after the test."""
    pool = SyncPool(max_workers=2)
    try:
        yield pool
    finally:
        pool.shutdown()


def test_sync_pool_and_run_and_async_wrappers(sync_pool: SyncPool) -> None:
    """Verify SyncPool, run_, and async_ execute synchronous and asynchronous callables."""
    assert sync_pool.max_workers == 2
    fut = sync_pool.submit(lambda x, y: x + y, 20, 22)
    assert fut.result(timeout=5.0) == 42

    async def _compute_async(val: int) -> int:
        await asyncio.sleep(0)
        return val * 2

    blocking_fn = run_(_compute_async)
    assert blocking_fn(21) == 42

    def _add_ten(val: int) -> int:
        return val + 10

    async def _run_wrapped() -> int:
        async_fn = async_(_add_ten)
        return await async_fn(32)

    assert asyncio.run(_run_wrapped()) == 42


def test_loop_sharded_resource_and_capacity_limiter() -> None:
    """Verify SyncLoopShardedResource and AsyncLoopShardedResource isolate state per loop and CapacityLimiter bounds concurrency."""
    sync_resource: SyncLoopShardedResource[list[str]] = SyncLoopShardedResource(lambda: ["init"])
    async_resource: AsyncLoopShardedResource[list[str]] = AsyncLoopShardedResource(
        lambda: asyncio.sleep(0, result=["async_init"])
    )
    limiter = CapacityLimiter(2)

    async def _append_and_count(item: str) -> int:
        async with limiter:
            bucket = sync_resource.get()
            bucket.append(item)
            async_bucket = await async_resource.get()
            async_bucket.append(item)
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


@pytest.mark.parametrize(
    ("loop_env", "quarantined_env", "expected_prefix"),
    [
        ("asyncio", None, "asyncio"),
        ("auto", "uvloop,winloop,rloop,zuvloop", "asyncio"),
        ("uvloop", "uvloop==0.21.0", "asyncio"),
        (
            "uvloop",
            "",
            "uvloop" if sys.platform != "win32" and import_optional("uvloop") is not None else "asyncio",
        ),
    ],
    ids=["explicit-asyncio", "quarantined-all", "quarantined-uvloop", "uvloop-or-fallback"],
)
def test_run_wrapper_delegates_to_create_event_loop_and_honors_env(
    monkeypatch: pytest.MonkeyPatch,
    loop_env: str,
    quarantined_env: str | None,
    expected_prefix: str,
) -> None:
    """Verify run_() delegates to create_event_loop and honors VERTEBRA_LOOP and VERTEBRA_QUARANTINED_LOOPS."""
    monkeypatch.setenv("VERTEBRA_LOOP", loop_env)
    if quarantined_env is None:
        monkeypatch.delenv("VERTEBRA_QUARANTINED_LOOPS", raising=False)
    else:
        monkeypatch.setenv("VERTEBRA_QUARANTINED_LOOPS", quarantined_env)

    factory_calls: list[asyncio.AbstractEventLoop] = []

    def _spy_factory() -> asyncio.AbstractEventLoop:
        created = create_event_loop()
        factory_calls.append(created)
        return created

    monkeypatch.setattr(sync_tools_mod, "create_event_loop", _spy_factory)

    async def _inspect_loop(multiplier: int) -> tuple[str, list[str], int]:
        running = asyncio.get_running_loop()
        steps: list[str] = []

        async def _eager_probe() -> None:
            steps.append("eager_before_suspend")
            await asyncio.sleep(0)
            steps.append("resumed")

        probe_task = running.create_task(_eager_probe())
        steps.append("after_create_task")
        await probe_task
        return type(running).__module__, steps, 21 * multiplier

    blocking_inspect = run_(_inspect_loop)
    module_name, step_order, value = blocking_inspect(2)

    assert len(factory_calls) == 1
    assert factory_calls[0].is_closed()
    assert module_name.startswith(expected_prefix)
    assert step_order == ["eager_before_suspend", "after_create_task", "resumed"]
    assert value == 42

    async def _call_from_running_loop() -> int:
        _, _, nested_val = blocking_inspect(3)
        return nested_val

    assert asyncio.run(_call_from_running_loop(), loop_factory=create_event_loop) == 63
    assert len(factory_calls) == 1
