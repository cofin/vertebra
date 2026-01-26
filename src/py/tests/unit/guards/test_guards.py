from __future__ import annotations

import asyncio

import pytest

from vertebra.core.context import VertebraContext
from vertebra.di import Inject, ProviderRegistry
from vertebra.web.guards import GuardDenied, compile_guards, requires


class Token:
    def __init__(self, value: int) -> None:
        self.value = value


def test_requires_attaches_guards() -> None:
    def guard_a(ctx) -> bool:
        return True

    def guard_b(ctx) -> bool:
        return True

    @requires(guard_a, guard_b)
    def handler() -> None:
        return None

    assert getattr(handler, "__vertebra_guards__", None) == [guard_a, guard_b]


def test_guard_runner_caches_results_per_request() -> None:
    calls = {"count": 0}

    def guard(ctx) -> bool:
        calls["count"] += 1
        return True

    runner = compile_guards([guard])
    ctx = VertebraContext.http({"path": "/"})

    asyncio.run(runner(ctx))
    asyncio.run(runner(ctx))

    assert calls["count"] == 1


def test_guard_runner_denies_on_false() -> None:
    def guard(ctx) -> bool:
        return False

    runner = compile_guards([guard])
    ctx = VertebraContext.http({"path": "/"})

    with pytest.raises(GuardDenied):
        asyncio.run(runner(ctx))


def test_guard_di_injection_uses_request_cache() -> None:
    registry = ProviderRegistry()
    counter = {"value": 0}

    @registry.provide(scope="request")
    def token() -> Token:
        counter["value"] += 1
        return Token(counter["value"])

    seen: dict[str, Token] = {}

    def guard_a(token: Inject[Token]) -> bool:
        seen["a"] = token
        return True

    def guard_b(token: Inject[Token]) -> bool:
        seen["b"] = token
        return True

    runner = compile_guards([guard_a, guard_b], compiler=registry.compile())
    ctx = VertebraContext.http({"path": "/"})

    asyncio.run(runner(ctx))

    assert seen["a"] is seen["b"]
    assert counter["value"] == 1
