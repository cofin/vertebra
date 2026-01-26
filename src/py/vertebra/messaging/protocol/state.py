"""State synchronization protocol definitions."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

import msgspec

__all__ = ["StateDelta", "StateOpcode", "decode_delta", "encode_delta", "field_id", "state_id"]


class StateOpcode(IntEnum):
    """Opcode identifiers for state deltas."""

    FIELD_SET = 0x01
    FIELD_DELETE = 0x02
    OBJECT_REPLACE = 0x03
    SYNC_REQUEST = 0x04
    SYNC_RESPONSE = 0x05


@dataclass(frozen=True, slots=True)
class StateDelta:
    """Decoded state delta message."""

    opcode: StateOpcode
    state_id: int
    field_id: int | None = None
    timestamp_ns: int | None = None
    value: Any | None = None


def _fnv1a_32(value: str) -> int:
    h = 0x811C9DC5
    for byte in value.encode("utf-8"):
        h ^= byte
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


def _fnv1a_64(value: str) -> int:
    h = 0xCBF29CE484222325
    for byte in value.encode("utf-8"):
        h ^= byte
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def state_id(name: str) -> int:
    """Return the deterministic 64-bit state ID for a state class name."""

    return _fnv1a_64(name)


def field_id(name: str) -> int:
    """Return the deterministic 32-bit field ID for a state field name."""

    return _fnv1a_32(name)


def encode_delta(delta: StateDelta) -> bytes:
    """Encode a state delta into MessagePack bytes."""

    opcode = int(delta.opcode)
    if delta.opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
        if delta.field_id is None or delta.timestamp_ns is None:
            msg = "field_id and timestamp_ns are required for field updates"
            raise ValueError(msg)
        payload = [opcode, delta.state_id, delta.field_id, delta.timestamp_ns, delta.value]
    elif delta.opcode is StateOpcode.FIELD_DELETE:
        if delta.field_id is None or delta.timestamp_ns is None:
            msg = "field_id and timestamp_ns are required for field deletes"
            raise ValueError(msg)
        payload = [opcode, delta.state_id, delta.field_id, delta.timestamp_ns]
    elif delta.opcode is StateOpcode.SYNC_REQUEST:
        payload = [opcode, delta.state_id]
    elif delta.opcode is StateOpcode.SYNC_RESPONSE:
        payload = [opcode, delta.state_id, delta.value]
    else:
        msg = f"Unsupported opcode: {delta.opcode}"
        raise ValueError(msg)

    return msgspec.msgpack.encode(payload)


def decode_delta(data: bytes | bytearray | memoryview) -> StateDelta:
    """Decode MessagePack bytes into a state delta."""

    raw = msgspec.msgpack.decode(data)
    if not isinstance(raw, (list, tuple)) or len(raw) < 2:
        msg = "State delta payload must be a list with at least 2 elements"
        raise ValueError(msg)

    opcode = StateOpcode(int(raw[0]))
    state = int(raw[1])

    if opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
        if len(raw) != 5:
            msg = "Field update delta must have 5 elements"
            raise ValueError(msg)
        return StateDelta(opcode=opcode, state_id=state, field_id=int(raw[2]), timestamp_ns=int(raw[3]), value=raw[4])
    if opcode is StateOpcode.FIELD_DELETE:
        if len(raw) != 4:
            msg = "Field delete delta must have 4 elements"
            raise ValueError(msg)
        return StateDelta(opcode=opcode, state_id=state, field_id=int(raw[2]), timestamp_ns=int(raw[3]))
    if opcode is StateOpcode.SYNC_REQUEST:
        if len(raw) != 2:
            msg = "Sync request delta must have 2 elements"
            raise ValueError(msg)
        return StateDelta(opcode=opcode, state_id=state)
    if opcode is StateOpcode.SYNC_RESPONSE:
        if len(raw) != 3:
            msg = "Sync response delta must have 3 elements"
            raise ValueError(msg)
        return StateDelta(opcode=opcode, state_id=state, value=raw[2])

    msg = f"Unsupported opcode: {opcode}"
    raise ValueError(msg)
