"""Push server-owned counter updates to a browser over server-sent events."""

__all__ = ("CounterLive", "app", "counter_events", "counter_page")

# --8<-- [start:example]
import asyncio
from collections.abc import AsyncIterator

from vertebra import App, get
from vertebra.framework.live import LiveState, LiveStateRoom, live_action
from vertebra.utils.serializers import encode_json_bytes


class CounterLive(LiveState):
    """Track a server-owned counter and record each mutation."""

    count: int = 0

    @live_action
    def increment(self) -> None:
        """Increment the counter."""
        self.count += 1


@get("/live")
def counter_page() -> tuple[int, list[tuple[str, str]], bytes]:
    """Serve a page that subscribes to counter updates."""
    return (
        200,
        [("content-type", "text/html; charset=utf-8")],
        b"""<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Live counter</title>
<h1>Live counter</h1>
<p>Count: <output id="count" aria-live="polite">Connecting</output></p>
<script type="module">
  const count = document.querySelector("#count");
  const events = new EventSource("/live/events");

  events.onmessage = (event) => {
    count.textContent = String(JSON.parse(event.data).count);
  };
  events.addEventListener("complete", () => events.close());
</script>
</html>""",
    )


@get("/live/events")
async def counter_events() -> tuple[int, list[tuple[str, str]], AsyncIterator[bytes]]:
    """Stream one counter per subscription and close after three updates."""
    state = CounterLive()
    room = LiveStateRoom("counter", state)

    async def events() -> AsyncIterator[bytes]:
        yield b"data: " + encode_json_bytes(state.snapshot()) + b"\n\n"
        for _ in range(3):
            await asyncio.sleep(0.5)
            state.increment()
            if await room.broadcast_pending():
                yield b"data: " + encode_json_bytes(state.snapshot()) + b"\n\n"
        yield b"event: complete\ndata: {}\n\n"

    return (
        200,
        [("content-type", "text/event-stream"), ("cache-control", "no-cache")],
        events(),
    )


app = App(route_handlers=[counter_page, counter_events])

# --8<-- [end:example]
