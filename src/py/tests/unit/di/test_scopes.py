from __future__ import annotations

import asyncio

from vertebra.di import ProviderRegistry


class Widget:
    def __init__(self, value: int) -> None:
        self.value = value


def _next(counter: dict[str, int]) -> int:
    counter["value"] += 1
    return counter["value"]


def test_singleton_scope_reuses_instance() -> None:
    registry = ProviderRegistry()
    counter = {"value": 0}

    @registry.provide
    def widget() -> Widget:
        return Widget(_next(counter))

    runner = registry.compile().compile(Widget)
    first = asyncio.run(runner())
    second = asyncio.run(runner())

    assert first is second
    assert first.value == 1


def test_request_scope_reuses_within_request_cache() -> None:
    registry = ProviderRegistry()
    counter = {"value": 0}

    @registry.provide(scope="request")
    def widget() -> Widget:
        return Widget(_next(counter))

    runner = registry.compile().compile(Widget)
    request_cache: dict[type[Widget], Widget] = {}

    first = asyncio.run(runner(request_cache))
    second = asyncio.run(runner(request_cache))
    third = asyncio.run(runner({}))

    assert first is second
    assert third is not first


def test_transient_scope_always_creates_new_instance() -> None:
    registry = ProviderRegistry()
    counter = {"value": 0}

    @registry.provide(scope="transient")
    def widget() -> Widget:
        return Widget(_next(counter))

    runner = registry.compile().compile(Widget)
    request_cache: dict[type[Widget], Widget] = {}

    first = asyncio.run(runner(request_cache))
    second = asyncio.run(runner(request_cache))

    assert first is not second
