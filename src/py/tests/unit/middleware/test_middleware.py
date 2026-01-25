from __future__ import annotations

import asyncio

from vertebra.context import VertebraContext
from vertebra.di import Inject, ProviderRegistry
from vertebra.middleware import compile_middleware, middleware


class Token:
    def __init__(self, value: int) -> None:
        self.value = value


def test_middleware_order_and_reverse_after() -> None:
    events: list[str] = []

    def before_a(ctx) -> None:
        events.append("before_a")

    def after_a(ctx, result) -> None:
        events.append("after_a")

    def before_b(ctx) -> None:
        events.append("before_b")

    def after_b(ctx, result) -> None:
        events.append("after_b")

    chain = [
        middleware(before=before_a, after=after_a, priority=10),
        middleware(before=before_b, after=after_b, priority=0),
    ]

    before_runner, after_runner = compile_middleware(chain)
    ctx = VertebraContext.http({"path": "/"})
    asyncio.run(before_runner(ctx))
    asyncio.run(after_runner(ctx, {"ok": True}))

    assert events == ["before_b", "before_a", "after_a", "after_b"]


def test_middleware_di_injection_uses_request_cache() -> None:
    registry = ProviderRegistry()
    counter = {"value": 0}

    @registry.provide(scope="request")
    def token() -> Token:
        counter["value"] += 1
        return Token(counter["value"])

    seen: list[Token] = []

    def before(ctx, token: Inject[Token]) -> None:
        seen.append(token)

    def after(ctx, result, token: Inject[Token]) -> None:
        seen.append(token)

    before_runner, after_runner = compile_middleware(
        [middleware(before=before, after=after)],
        compiler=registry.compile(),
    )
    ctx = VertebraContext.http({"path": "/"})
    asyncio.run(before_runner(ctx))
    asyncio.run(after_runner(ctx, {"ok": True}))

    assert len(seen) == 2
    assert seen[0] is seen[1]
    assert counter["value"] == 1


def test_middleware_supports_async_hooks() -> None:
    events: list[str] = []

    async def before(ctx) -> None:
        events.append("before")

    async def after(ctx, result) -> None:
        events.append("after")

    before_runner, after_runner = compile_middleware([middleware(before=before, after=after)])
    ctx = VertebraContext.http({"path": "/"})
    asyncio.run(before_runner(ctx))
    asyncio.run(after_runner(ctx, {"ok": True}))

    assert events == ["before", "after"]
