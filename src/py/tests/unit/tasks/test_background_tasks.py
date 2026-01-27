from __future__ import annotations

import asyncio

import pytest

from vertebra.core.tasks import BackgroundTaskError, BackgroundTasks
from vertebra.di import Inject, ProviderRegistry

pytestmark = pytest.mark.unit


class Dependency:
    label: str = "dep"


@pytest.mark.anyio
async def test_background_tasks_runs_sync_and_async() -> None:
    tasks = BackgroundTasks()
    seen: list[str] = []

    def sync_task(value: str) -> None:
        seen.append(value)

    async def async_task(value: str) -> None:
        seen.append(f"async:{value}")

    await tasks.schedule(sync_task, "one").wait()
    await tasks.schedule(async_task, "two").wait()

    assert seen == ["one", "async:two"]


@pytest.mark.anyio
async def test_background_tasks_injects_dependencies() -> None:
    registry = ProviderRegistry()
    registry.provide(Dependency)

    tasks = BackgroundTasks(compiler=registry.compile())
    seen: list[str] = []

    def handler(value: str, dep: Inject[Dependency]) -> None:
        seen.append(f"{value}:{dep.label}")

    await tasks.schedule(handler, "ping").wait()

    assert seen == ["ping:dep"]


@pytest.mark.anyio
async def test_background_tasks_require_compiler_for_inject() -> None:
    tasks = BackgroundTasks()

    def handler(value: str, dep: Inject[Dependency]) -> None:
        _ = (value, dep)

    with pytest.raises(BackgroundTaskError):
        await tasks.schedule(handler, "ping").wait()


@pytest.mark.anyio
async def test_background_tasks_cancel_and_cleanup() -> None:
    tasks = BackgroundTasks()
    started = asyncio.Event()
    finished = asyncio.Event()

    async def long_task() -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        finally:
            finished.set()

    handle = tasks.schedule(long_task)
    await started.wait()
    await tasks.shutdown()

    assert handle.task.cancelled()
    assert finished.is_set()
