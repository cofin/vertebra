"""Client helpers for state synchronization endpoints."""

from __future__ import annotations

import base64
from typing import Any

import msgspec

from vertebra.messaging.protocol.state import StateDelta, decode_delta
from vertebra.state.shadow import ShadowState

__all__ = [
    "ShadowState",
    "SseEvent",
    "StateSubscription",
    "decode_sse_event",
    "decode_state_payload",
    "parse_state_stream",
    "sse_endpoint",
    "ws_endpoint",
]


class SseEvent(msgspec.Struct, frozen=True):
    event: str
    data: str
    event_id: str | None = None


class StateSubscription(msgspec.Struct):
    shadow: ShadowState = msgspec.field(default_factory=ShadowState)
    last_event_id: int = 0

    def apply_payload(self, payload: str | bytes) -> bool:
        delta = decode_state_payload(payload)
        return self.shadow.apply_delta(delta)

    def apply_sse_event(self, raw: str) -> bool:
        event = decode_sse_event(raw)
        if event.event_id:
            self.last_event_id = int(event.event_id)
        return self.apply_payload(event.data)

    def apply_ws_message(self, message: str | bytes) -> bool:
        data = msgspec.json.decode(message)
        if not isinstance(data, dict) or data.get("type") != "delta":
            return False
        event_id = data.get("id")
        if isinstance(event_id, int):
            self.last_event_id = event_id
        payload = data.get("payload")
        if not isinstance(payload, str):
            return False
        return self.apply_payload(payload)


def sse_endpoint(func: Any) -> Any:
    """Decorator to tag an SSE endpoint for state sync."""

    setattr(func, "__vertebra_sync__", "sse")
    return func


def ws_endpoint(func: Any) -> Any:
    """Decorator to tag a WebSocket endpoint for state sync."""

    setattr(func, "__vertebra_sync__", "ws")
    return func


def decode_sse_event(line: str) -> SseEvent:
    """Decode a single SSE event block."""

    parts = [part for part in line.strip().split("\n") if part]
    event_type = "message"
    event_id: str | None = None
    data_lines: list[str] = []
    for part in parts:
        if part.startswith("event:"):
            event_type = part.split(":", 1)[1].strip()
        elif part.startswith("id:"):
            event_id = part.split(":", 1)[1].strip()
        elif part.startswith("data:"):
            data_lines.append(part.split(":", 1)[1].strip())
    return SseEvent(event=event_type, data="\n".join(data_lines), event_id=event_id)


def decode_state_payload(payload: str | bytes) -> StateDelta:
    """Decode a base64 state payload into a StateDelta."""

    if isinstance(payload, str):
        raw = base64.b64decode(payload)
        return decode_delta(raw)
    try:
        return decode_delta(payload)
    except Exception:  # noqa: BLE001 - Try raw first, fallback to base64 decode
        raw = base64.b64decode(payload)
        return decode_delta(raw)


def parse_state_stream(payload: bytes) -> StateDelta:
    """Decode base64+msgpack state payload into a StateDelta."""

    decoded = msgspec.msgpack.decode(payload)
    if isinstance(decoded, (bytes, bytearray, memoryview)):
        return decode_delta(decoded)
    return decode_delta(decoded)
