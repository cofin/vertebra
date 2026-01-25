"""Native State primitive for Vertebra."""

import time
import asyncio
from collections.abc import AsyncGenerator
from typing import Any, TypeVar

import msgspec

from vertebra.protocol.state import StateDelta, StateOpcode, encode_delta, field_id, state_id

T = TypeVar("T", bound="NativeState")


class NativeState(msgspec.Struct):
    """Base class for synchronized state objects.

    Subclasses must be defined as msgspec Structs.
    Changes to fields are automatically broadcast to subscribers.
    """

    # Instance-level subscribers
    _subscribers: set[asyncio.Queue] = msgspec.field(default_factory=set)
    _delta_subscribers: set[asyncio.Queue] = msgspec.field(default_factory=set)
    _initialized: bool = msgspec.field(default=False)
    _state_id: int = msgspec.field(default=0)

    def __post_init__(self) -> None:
        self._state_id = state_id(self.__class__.__name__)
        self._initialized = True

    def _snapshot(self) -> "NativeState":
        return msgspec.structs.replace(
            self,
            _subscribers=set(),
            _delta_subscribers=set(),
            _initialized=True,
            _state_id=self._state_id,
        )

    @staticmethod
    def _is_object_replace(value: Any) -> bool:
        return isinstance(value, (dict, list, tuple, msgspec.Struct))

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

        # Snapshot without internal queues.
        snapshot = self._snapshot()
        for queue in self._subscribers:
            queue.put_nowait(snapshot)

    def broadcast_delta(self, delta: StateDelta) -> None:
        payload = encode_delta(delta)
        for queue in self._delta_subscribers:
            queue.put_nowait(payload)

    async def subscribe(self) -> AsyncGenerator[Any, None]:
        """Create a subscription to this state."""
        queue = asyncio.Queue()

        # Send initial state
        queue.put_nowait(self._snapshot())

        self._subscribers.add(queue)

        try:
            while True:
                data = await queue.get()
                yield data
        finally:
            self._subscribers.remove(queue)

    async def subscribe_deltas(self) -> AsyncGenerator[bytes, None]:
        """Create a subscription to state deltas."""
        queue = asyncio.Queue()
        self._delta_subscribers.add(queue)
        try:
            while True:
                data = await queue.get()
                yield data
        finally:
            self._delta_subscribers.remove(queue)

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name.startswith("_") or not getattr(self, "_initialized", False):
            return
        if not hasattr(self, "_subscribers"):
            return

        opcode = StateOpcode.OBJECT_REPLACE if self._is_object_replace(value) else StateOpcode.FIELD_SET
        delta = StateDelta(
            opcode=opcode,
            state_id=self._state_id,
            field_id=field_id(name),
            timestamp_ns=time.time_ns(),
            value=value,
        )
        if hasattr(self, "_delta_subscribers"):
            self.broadcast_delta(delta)
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
