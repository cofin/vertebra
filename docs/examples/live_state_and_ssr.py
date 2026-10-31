"""Reactive LiveState component, LiveStateRoom, and Inertia v3 page object example."""

import asyncio

from vertebra.framework.inertia import InertiaConfig, InertiaMiddleware, inertia
from vertebra.framework.live import LiveState, LiveStateRoom, live_action

__all__ = (
    "CounterLive",
    "main",
    "run_live_and_inertia_demo",
)


# --8<-- [start:example]
class CounterLive(LiveState):
    """Server-owned reactive counter component with LWW delta synchronization."""

    count: int = 0
    step: int = 1

    @live_action
    async def increment(self) -> int:
        """Increment the counter by the configured step."""
        self.count += self.step
        return self.count


async def run_live_and_inertia_demo() -> None:
    """Mutate a LiveState component, broadcast deltas, and build an Inertia page."""
    state = CounterLive(count=0, step=2)
    room = LiveStateRoom("counter:room", state)

    await state.increment()
    assert state.count == 2

    frames = await room.broadcast_pending()
    assert len(frames) == 1
    snap = state.snapshot()
    assert snap["count"] == 2 and snap["step"] == 2

    middleware = InertiaMiddleware(InertiaConfig(enabled=True, version="1.0.0"))
    response = inertia("Dashboard/Overview", {"count": state.count})

    class DummyRequest:
        method = "GET"
        path = "/dashboard"
        headers: dict[str, str] = {"X-Inertia": "true"}

    page = middleware.build_page_object(DummyRequest(), response)
    assert page["component"] == "Dashboard/Overview"
    assert page["props"]["count"] == 2


def main() -> None:
    """Execute the LiveState and Inertia verification workflow."""
    asyncio.run(run_live_and_inertia_demo())


# --8<-- [end:example]


if __name__ == "__main__":
    main()
