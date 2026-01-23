"""Native State primitive for Vertebra."""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any, TypeVar

import msgspec

T = TypeVar("T", bound="NativeState")


class NativeState(msgspec.Struct):
    """Base class for synchronized state objects.

    Subclasses must be defined as msgspec Structs.
    Changes to fields are automatically broadcast to subscribers.
    """

    # Instance-level subscribers
    _subscribers: set[asyncio.Queue] = msgspec.field(default_factory=set)

    def __post_init__(self) -> None:
        pass

    def broadcast(self) -> None:
        """Broadcast current state to all subscribers."""
        # We push self (the object) to queues.
        # The subscriber (StreamingResponse) will handle serialization.
        # This assumes self is immutable-ish enough or copied during serialization?
        # msgspec serialization is fast.
        # But if we push 'self', and then 'self' changes again before serialization happens in the generator task?
        # Race condition!
        # We MUST copy the state or serialize it immediately.
        #
        # Serialize immediately -> bytes.
        # So queues should hold bytes.
        #
        # If queues hold bytes, then StreamingResponse needs to handle pre-serialized data.
        # OR we rely on `msgspec.structs.replace(self)` (shallow copy) to snapshot state.

        # Let's try copy for now.
        snapshot = msgspec.structs.replace(self)
        for queue in self._subscribers:
            queue.put_nowait(snapshot)

    async def subscribe(self) -> AsyncGenerator[Any, None]:
        """Create a subscription to this state."""
        queue = asyncio.Queue()

        # Send initial state
        queue.put_nowait(msgspec.structs.replace(self))

        self._subscribers.add(queue)

        try:
            while True:
                data = await queue.get()
                yield data
        finally:
            self._subscribers.remove(queue)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if not name.startswith("_") and hasattr(self, "_subscribers"):
            self.broadcast()


def register_state(state: NativeState, name: str) -> None:
    """Register a NativeState instance as a procedure."""
    from vertebra.procedures.decorator import procedure

    # We define the handler with correct type hint so extraction works

    # We can't easily dynamically annotate the return type with `state.__class__` for static analysis tools/decorators
    # but we can do it at runtime.

    @procedure(name=name)
    async def subscription_handler() -> AsyncGenerator[state.__class__, None]:  # type: ignore
        async for item in state.subscribe():
            yield item
