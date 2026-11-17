"""Live Event State subdomain with MessagePack binary deltas and HLC Last-Write-Wins sync."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from enum import IntEnum
from typing import Any, ClassVar

import msgspec

from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

try:
    import vertebra._vertebra as _vtb

    _native_field_id: Callable[[str], int] | None = getattr(_vtb, "field_id", None)
    _native_state_id: Callable[[str], int] | None = getattr(_vtb, "state_id", None)
except ImportError:
    _native_field_id = None
    _native_state_id = None

__all__ = (
    "HybridLogicalClock",
    "LiveState",
    "LiveStateRoom",
    "StateDeltaFrame",
    "StateOpcode",
    "live_action",
)

_FNV64_OFFSET = 0xCBF29CE484222325
_FNV64_PRIME = 0x100000001B3
_FNV64_MASK = 0xFFFFFFFFFFFFFFFF

_FNV32_OFFSET = 0x811C9DC5
_FNV32_PRIME = 0x01000193
_FNV32_MASK = 0xFFFFFFFF
_HLC_WALL_MASK = 0x0000FFFFFFFFFFFF


class StateOpcode(IntEnum):
    """Binary opcodes for Live Event State MessagePack delta frames."""

    FIELD_SET = 0x01
    FIELD_DELETE = 0x02
    OBJECT_REPLACE = 0x03
    SYNC_REQUEST = 0x04
    SYNC_RESPONSE = 0x05
    ACTION_CALL = 0x06
    ACTION_RESULT = 0x07
    BATCH_PATCH = 0x08


_OPCODE_ARITY: dict[StateOpcode, int] = {
    StateOpcode.FIELD_SET: 5,
    StateOpcode.FIELD_DELETE: 4,
    StateOpcode.OBJECT_REPLACE: 5,
    StateOpcode.SYNC_REQUEST: 2,
    StateOpcode.SYNC_RESPONSE: 3,
    StateOpcode.ACTION_CALL: 5,
    StateOpcode.ACTION_RESULT: 4,
    StateOpcode.BATCH_PATCH: 4,
}


def _require_wire_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = f"State delta frame {field_name} must be an integer, got {type(value).__name__}"
        raise ValueError(msg)
    return value


class HybridLogicalClock:
    """64-bit Hybrid Logical Clock packing 48-bit physical milliseconds and 16-bit logical counter."""

    __slots__ = ("logical", "wall_ms")

    def __init__(self, wall_ms: int = 0, logical: int = 0) -> None:
        """Initialize hybrid logical clock with wall clock milliseconds and logical counter."""
        self.wall_ms = wall_ms & _HLC_WALL_MASK
        self.logical = logical & 0xFFFF

    def pack(self) -> int:
        """Pack the clock state into an unsigned 64-bit integer."""
        return ((self.wall_ms & _HLC_WALL_MASK) << 16) | (self.logical & 0xFFFF)

    @classmethod
    def unpack(cls, packed: int) -> HybridLogicalClock:
        """Unpack a 64-bit integer into a HybridLogicalClock instance."""
        return cls(wall_ms=(packed >> 16) & _HLC_WALL_MASK, logical=packed & 0xFFFF)

    def tick(self, now_ms: int | None = None) -> int:
        """Advance the clock for a local event and return the packed 64-bit timestamp."""
        current_ms = (int(time.time() * 1000) if now_ms is None else now_ms) & _HLC_WALL_MASK
        if current_ms > self.wall_ms:
            self.wall_ms = current_ms
            self.logical = 0
        else:
            self.logical = min(0xFFFF, self.logical + 1)
        return self.pack()

    def receive(self, remote_packed: int, now_ms: int | None = None) -> int:
        """Merge a remote HLC timestamp with local wall time and return the new packed timestamp."""
        remote = self.unpack(remote_packed)
        current_ms = (int(time.time() * 1000) if now_ms is None else now_ms) & _HLC_WALL_MASK
        if current_ms > self.wall_ms and current_ms > remote.wall_ms:
            self.wall_ms = current_ms
            self.logical = 0
        elif self.wall_ms == remote.wall_ms:
            self.logical = min(0xFFFF, max(self.logical, remote.logical) + 1)
        elif remote.wall_ms > self.wall_ms:
            self.wall_ms = remote.wall_ms
            self.logical = min(0xFFFF, remote.logical + 1)
        else:
            self.logical = min(0xFFFF, self.logical + 1)
        return self.pack()


class StateDeltaFrame(msgspec.Struct, frozen=True, gc=False, kw_only=True, array_like=True):
    """Decoded representation of a Live Event State wire frame."""

    opcode: StateOpcode
    state_id: int
    field_id: int = 0
    timestamp_ns: int = 0
    value: Any = None
    call_id: int = 0

    def encode(self) -> bytes:
        """Encode this StateDeltaFrame into a compact MessagePack array matching vertebra-core."""
        op = int(self.opcode)
        if self.opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
            payload: list[Any] = [
                op,
                self.state_id,
                self.field_id,
                self.timestamp_ns,
                self.value,
            ]
        elif self.opcode == StateOpcode.FIELD_DELETE:
            payload = [op, self.state_id, self.field_id, self.timestamp_ns]
        elif self.opcode == StateOpcode.SYNC_REQUEST:
            payload = [op, self.state_id]
        elif self.opcode == StateOpcode.SYNC_RESPONSE:
            payload = [op, self.state_id, self.value]
        elif self.opcode == StateOpcode.ACTION_CALL:
            payload = [op, self.state_id, self.field_id, self.call_id, self.value]
        elif self.opcode == StateOpcode.ACTION_RESULT:
            payload = [op, self.state_id, self.call_id, self.value]
        elif self.opcode == StateOpcode.BATCH_PATCH:
            payload = [op, self.state_id, self.timestamp_ns, self.value]
        else:
            msg = f"Unsupported state opcode: {self.opcode}"
            raise ValueError(msg)
        return encode_msgpack_bytes(payload)

    @classmethod
    def decode(cls, data: bytes | bytearray | memoryview) -> StateDeltaFrame:
        """Decode a compact MessagePack array into a StateDeltaFrame."""
        try:
            raw = decode_msgpack(data)
        except msgspec.DecodeError as exc:
            msg = f"Invalid MessagePack state delta frame: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(raw, list | tuple) or len(raw) < 2:
            msg = "State delta wire payload must be a sequence of at least 2 elements"
            raise ValueError(msg)
        opcode_int = _require_wire_int(raw[0], "opcode")
        opcode = StateOpcode(opcode_int)
        expected_len = _OPCODE_ARITY.get(opcode)
        if expected_len is None or len(raw) != expected_len:
            msg = f"State delta opcode {opcode.name} requires {expected_len} elements, got {len(raw)}"
            raise ValueError(msg)
        sid = _require_wire_int(raw[1], "state_id")
        if opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
            return cls(
                opcode=opcode,
                state_id=sid,
                field_id=_require_wire_int(raw[2], "field_id"),
                timestamp_ns=_require_wire_int(raw[3], "timestamp_ns"),
                value=raw[4],
            )
        if opcode == StateOpcode.FIELD_DELETE:
            return cls(
                opcode=opcode,
                state_id=sid,
                field_id=_require_wire_int(raw[2], "field_id"),
                timestamp_ns=_require_wire_int(raw[3], "timestamp_ns"),
            )
        if opcode == StateOpcode.SYNC_REQUEST:
            return cls(opcode=opcode, state_id=sid)
        if opcode == StateOpcode.SYNC_RESPONSE:
            return cls(opcode=opcode, state_id=sid, value=raw[2])
        if opcode == StateOpcode.ACTION_CALL:
            return cls(
                opcode=opcode,
                state_id=sid,
                field_id=_require_wire_int(raw[2], "field_id"),
                call_id=_require_wire_int(raw[3], "call_id"),
                value=raw[4],
            )
        if opcode == StateOpcode.ACTION_RESULT:
            return cls(
                opcode=opcode,
                state_id=sid,
                call_id=_require_wire_int(raw[2], "call_id"),
                value=raw[3],
            )
        return cls(
            opcode=opcode,
            state_id=sid,
            timestamp_ns=_require_wire_int(raw[2], "timestamp_ns"),
            value=raw[3],
        )


class LiveState:
    """Reactive server-client synchronized state object with LWW conflict resolution."""

    state_id: ClassVar[int] = 0
    _field_ids: ClassVar[dict[str, int]] = {}
    __vertebra_state_id__: ClassVar[int] = 0
    __vertebra_field_map__: ClassVar[dict[int, str]] = {}
    __vertebra_actions__: ClassVar[dict[int, str]] = {}
    _max_pending_deltas: ClassVar[int] = 256

    _hlc: HybridLogicalClock
    _field_timestamps: dict[int, int]
    _pending_deltas: dict[int, StateDeltaFrame]
    _field_names: dict[int, str]

    @staticmethod
    def compute_state_id(name: str) -> int:
        """Compute the deterministic 64-bit FNV-1a state identifier matching Rust state_id."""
        if _native_state_id is not None:
            return int(_native_state_id(name))
        digest = _FNV64_OFFSET
        for byte in name.encode("utf-8"):
            digest ^= byte
            digest = (digest * _FNV64_PRIME) & _FNV64_MASK
        return digest

    @staticmethod
    def compute_field_id(name: str) -> int:
        """Compute the deterministic 32-bit FNV-1a field identifier matching Rust field_id."""
        if _native_field_id is not None:
            return int(_native_field_id(name))
        digest = _FNV32_OFFSET
        for byte in name.encode("utf-8"):
            digest ^= byte
            digest = (digest * _FNV32_PRIME) & _FNV32_MASK
        return digest

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Compile state and field identifier maps across the LiveState MRO."""
        super().__init_subclass__(**kwargs)
        sid = cls.compute_state_id(cls.__name__)
        cls.state_id = sid
        cls.__vertebra_state_id__ = sid
        field_ids: dict[str, int] = {}
        field_map: dict[int, str] = {}
        actions: dict[int, str] = {}
        for base in reversed(cls.__mro__):
            if base is LiveState or not issubclass(base, LiveState):
                continue
            for name in base.__dict__.get("__annotations__", ()):
                if not name.startswith("_"):
                    fid = cls.compute_field_id(name)
                    field_ids[name] = fid
                    field_map[fid] = name
            for attr_name, attr_val in base.__dict__.items():
                if getattr(attr_val, "__vertebra_live_action__", False):
                    actions[cls.compute_field_id(attr_name)] = attr_name
        cls._field_ids = field_ids
        cls.__vertebra_field_map__ = field_map
        cls.__vertebra_actions__ = actions

    def __init__(self, **initial: Any) -> None:
        """Initialize reactive state instance with initial field values."""
        object.__setattr__(self, "_hlc", HybridLogicalClock())
        object.__setattr__(self, "_field_timestamps", {})
        object.__setattr__(self, "_pending_deltas", {})
        object.__setattr__(self, "_field_names", dict(self.__class__.__vertebra_field_map__))
        for base in reversed(type(self).__mro__):
            if base is LiveState or not issubclass(base, LiveState):
                continue
            for key, value in base.__dict__.items():
                if (
                    key != "state_id"
                    and not key.startswith("_")
                    and not callable(value)
                    and not isinstance(value, (classmethod, staticmethod, property))
                ):
                    fid = self._field_ids.get(key) or self.compute_field_id(key)
                    self._field_names[fid] = key
                    object.__setattr__(self, key, value)
        for key, value in initial.items():
            setattr(self, key, value)
        self._pending_deltas.clear()

    def __setattr__(self, name: str, value: Any) -> None:
        """Record reactive mutations, advancing the HLC clock and emitting an LWW delta frame."""
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        fid = self._field_ids.get(name)
        if fid is None:
            fid = self.compute_field_id(name)
        self._field_names[fid] = name
        ts = self._hlc.tick()
        self._field_timestamps[fid] = ts
        object.__setattr__(self, name, value)
        frame = StateDeltaFrame(
            opcode=StateOpcode.FIELD_SET,
            state_id=self.state_id,
            field_id=fid,
            timestamp_ns=ts,
            value=value,
        )
        if fid not in self._pending_deltas and len(self._pending_deltas) >= self._max_pending_deltas:
            oldest_key = next(iter(self._pending_deltas))
            del self._pending_deltas[oldest_key]
        self._pending_deltas[fid] = frame

    def delete_field(self, name: str) -> None:
        """Delete a reactive field and emit a FIELD_DELETE tombstone delta."""
        fid = self._field_ids.get(name) or self.compute_field_id(name)
        ts = self._hlc.tick()
        self._field_timestamps[fid] = ts
        if hasattr(self, name):
            object.__delattr__(self, name)
        frame = StateDeltaFrame(
            opcode=StateOpcode.FIELD_DELETE,
            state_id=self.state_id,
            field_id=fid,
            timestamp_ns=ts,
        )
        if fid not in self._pending_deltas and len(self._pending_deltas) >= self._max_pending_deltas:
            oldest_key = next(iter(self._pending_deltas))
            del self._pending_deltas[oldest_key]
        self._pending_deltas[fid] = frame

    def drain_deltas(self) -> list[StateDeltaFrame]:
        """Drain and return all pending state delta frames."""
        deltas = list(self._pending_deltas.values())
        self._pending_deltas.clear()
        return deltas

    def flush_deltas(self) -> list[StateDeltaFrame]:
        """Flush and return all pending state delta frames."""
        return self.drain_deltas()

    def drain_encoded_deltas(self) -> list[bytes]:
        """Drain and encode all pending state deltas as MessagePack frames."""
        return [frame.encode() for frame in self.drain_deltas()]

    def snapshot(self) -> dict[str, Any]:
        """Return a dictionary snapshot of all current reactive fields."""
        result: dict[str, Any] = {}
        for name in self._field_names.values():
            if hasattr(self, name):
                result[name] = getattr(self, name)
        return result

    def encode_sync_response(self) -> bytes:
        """Encode a full SYNC_RESPONSE frame for initial client hydration."""
        return StateDeltaFrame(
            opcode=StateOpcode.SYNC_RESPONSE,
            state_id=self.state_id,
            value=self.snapshot(),
        ).encode()

    def apply_delta(self, frame: StateDeltaFrame) -> bool:
        """Apply an incoming client or peer delta using HLC Last-Write-Wins resolution."""
        if frame.state_id != self.state_id:
            return False
        if frame.opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
            current_ts = self._field_timestamps.get(frame.field_id, 0)
            if frame.timestamp_ns < current_ts:
                return False
            self._hlc.receive(frame.timestamp_ns)
            self._field_timestamps[frame.field_id] = frame.timestamp_ns
            field_name = self._field_names.get(frame.field_id)
            if field_name is not None:
                object.__setattr__(self, field_name, frame.value)
            return True
        if frame.opcode == StateOpcode.FIELD_DELETE:
            current_ts = self._field_timestamps.get(frame.field_id, 0)
            if frame.timestamp_ns < current_ts:
                return False
            self._hlc.receive(frame.timestamp_ns)
            self._field_timestamps[frame.field_id] = frame.timestamp_ns
            field_name = self._field_names.get(frame.field_id)
            if field_name is not None and hasattr(self, field_name):
                object.__delattr__(self, field_name)
            return True
        return False

    async def dispatch_action(self, frame: StateDeltaFrame) -> StateDeltaFrame:
        """Execute a @live_action method from an ACTION_CALL frame and return ACTION_RESULT."""
        if frame.opcode != StateOpcode.ACTION_CALL or frame.state_id != self.state_id:
            msg = "Expected ACTION_CALL frame for matching state_id"
            raise ValueError(msg)
        action_name = self.__class__.__vertebra_actions__.get(frame.field_id)
        if action_name is None:
            msg = f"Unknown live action id: {frame.field_id}"
            raise KeyError(msg)
        method = getattr(self, action_name)
        args = frame.value if isinstance(frame.value, dict) else {}
        outcome = method(**args)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        return StateDeltaFrame(
            opcode=StateOpcode.ACTION_RESULT,
            state_id=self.state_id,
            call_id=frame.call_id,
            value=outcome,
        )


def live_action[F: Callable[..., Any]](fn: F) -> F:
    """Decorate a method on a LiveState subclass as an invokable client action."""
    fn.__dict__["__vertebra_live_action__"] = True
    return fn


class LiveStateRoom:
    """Synchronizes a LiveState across connected WebSocket or Litestar Channels subscribers."""

    __slots__ = ("_history", "_publish_fn", "channel", "state")

    def __init__(
        self,
        channel: str,
        state: LiveState,
        publish_fn: Callable[[str, bytes], Awaitable[None]] | None = None,
    ) -> None:
        """Initialize live state room for channel and backing state instance."""
        self.channel = channel
        self.state = state
        self._publish_fn = publish_fn
        self._history: list[bytes] = []

    @property
    def published_frames(self) -> list[bytes]:
        """Return the history of encoded MessagePack delta frames broadcast to the room."""
        return list(self._history)

    async def broadcast_pending(self) -> list[bytes]:
        """Drain pending mutations from state and broadcast them to channel."""
        frames = self.state.drain_encoded_deltas()
        for payload in frames:
            self._history.append(payload)
            if self._publish_fn is not None:
                await self._publish_fn(self.channel, payload)
        return frames

    async def handle_client_frame(self, raw_bytes: bytes) -> list[bytes]:
        """Process an incoming client delta or action frame and return outbound response frames."""
        frame = StateDeltaFrame.decode(raw_bytes)
        if frame.opcode == StateOpcode.SYNC_REQUEST:
            return [self.state.encode_sync_response()]
        if frame.opcode == StateOpcode.ACTION_CALL:
            result_frame = await self.state.dispatch_action(frame)
            outbound = await self.broadcast_pending()
            outbound.append(result_frame.encode())
            return outbound
        if self.state.apply_delta(frame):
            encoded = frame.encode()
            self._history.append(encoded)
            if self._publish_fn is not None:
                await self._publish_fn(self.channel, encoded)
            return [encoded]
        return []
