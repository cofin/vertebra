from __future__ import annotations

import asyncio

import pytest

from vertebra.di import Inject, ProviderRegistry
from vertebra.events import EventBus, EventHandler

pytestmark = pytest.mark.unit


class Ping:
    def __init__(self, value: str) -> None:
        self.value = value


class Dependency:
    label: str = "dep"


def test_event_bus_dispatches_sync_handlers() -> None:
    bus = EventBus()
    received: list[str] = []

    @bus.on(Ping)
    def handle(event: Ping) -> None:
        received.append(event.value)

    bus.publish(Ping("first"))
    bus.publish(Ping("second"))

    assert received == ["first", "second"]


@pytest.mark.anyio
async def test_event_bus_dispatches_async_handlers() -> None:
    bus = EventBus()
    received: list[str] = []

    @bus.on(Ping)
    async def handle(event: Ping) -> None:
        received.append(event.value)

    await bus.publish_async(Ping("hello"))

    assert received == ["hello"]


@pytest.mark.anyio
async def test_event_bus_injects_dependencies() -> None:
    registry = ProviderRegistry()
    registry.provide(Dependency)
    compiler = registry.compile()

    bus = EventBus(compiler=compiler)
    received: list[str] = []

    @bus.on(Ping)
    def handle(event: Ping, dep: Inject[Dependency]) -> None:
        received.append(f"{event.value}:{dep.label}")

    await bus.publish_async(Ping("ping"))

    assert received == ["ping:dep"]


def test_event_bus_raises_on_missing_dependency() -> None:
    bus = EventBus()

    with pytest.raises(RuntimeError):

        @bus.on(Ping)
        def handle(event: Ping, dep: Inject[Dependency]) -> None:
            raise AssertionError("handler should not run")


@pytest.mark.anyio
async def test_event_bus_tracks_handlers() -> None:
    bus = EventBus()

    @bus.on(Ping)
    def handle(event: Ping) -> None:
        return None

    handlers = bus.handlers(Ping)
    assert len(handlers) == 1
    assert isinstance(handlers[0], EventHandler)
