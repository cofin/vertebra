"""State management module for Vertebra.

Provides:
- manager: NativeState base class and registration
- shadow: ShadowState for tracking remote state
- sync: Sync helpers, subscriptions, and SSE/WebSocket endpoints
"""

from vertebra.state.manager import NativeState, register_state
from vertebra.state.shadow import ShadowState
from vertebra.state.sync import (
    SseEvent,
    StateSubscription,
    decode_sse_event,
    decode_state_payload,
    parse_state_stream,
    sse_endpoint,
    ws_endpoint,
)

__all__ = [
    "NativeState",
    "ShadowState",
    "SseEvent",
    "StateSubscription",
    "decode_sse_event",
    "decode_state_payload",
    "parse_state_stream",
    "register_state",
    "sse_endpoint",
    "ws_endpoint",
]
