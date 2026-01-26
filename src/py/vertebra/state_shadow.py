"""Shadow state mirror for opcode-based deltas."""

from __future__ import annotations

from typing import Any

import msgspec

from vertebra.protocol.state import StateDelta, StateOpcode, decode_delta


class ShadowField(msgspec.Struct, frozen=True):
    timestamp_ns: int
    value: Any
    deleted: bool = False


class ShadowState:
    """In-memory mirror of NativeState deltas with LWW resolution."""

    __slots__ = ("_states",)

    def __init__(self) -> None:
        self._states: dict[int, dict[int, ShadowField]] = {}

    def apply_delta(self, delta: StateDelta) -> bool:
        """Apply a decoded delta. Returns True if applied."""

        if delta.opcode in (StateOpcode.FIELD_SET, StateOpcode.OBJECT_REPLACE):
            return self._apply_field_delta(delta)
        if delta.opcode is StateOpcode.FIELD_DELETE:
            return self._apply_delete_delta(delta)
        if delta.opcode in (StateOpcode.SYNC_REQUEST, StateOpcode.SYNC_RESPONSE):
            return False
        return False

    def apply_payload(self, payload: bytes | bytearray | memoryview) -> bool:
        """Decode and apply a delta payload."""

        return self.apply_delta(decode_delta(payload))

    def get_field(self, state_id: int, field_id: int) -> ShadowField | None:
        return self._states.get(state_id, {}).get(field_id)

    def _apply_field_delta(self, delta: StateDelta) -> bool:
        if delta.field_id is None or delta.timestamp_ns is None:
            msg = "field_id and timestamp_ns are required for field updates"
            raise ValueError(msg)

        fields = self._states.setdefault(delta.state_id, {})
        existing = fields.get(delta.field_id)
        if existing and existing.timestamp_ns >= delta.timestamp_ns:
            return False

        fields[delta.field_id] = ShadowField(timestamp_ns=delta.timestamp_ns, value=delta.value, deleted=False)
        return True

    def _apply_delete_delta(self, delta: StateDelta) -> bool:
        if delta.field_id is None or delta.timestamp_ns is None:
            msg = "field_id and timestamp_ns are required for field deletes"
            raise ValueError(msg)

        fields = self._states.setdefault(delta.state_id, {})
        existing = fields.get(delta.field_id)
        if existing and existing.timestamp_ns >= delta.timestamp_ns:
            return False

        fields[delta.field_id] = ShadowField(timestamp_ns=delta.timestamp_ns, value=None, deleted=True)
        return True
