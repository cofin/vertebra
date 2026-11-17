"""Unit tests for Live Event State MessagePack deltas, HLC LWW, and LiveStateRoom."""

from __future__ import annotations

from typing import Any

import pytest

from vertebra.framework.live import (
    HybridLogicalClock,
    LiveState,
    LiveStateRoom,
    StateDeltaFrame,
    StateOpcode,
    live_action,
)
from vertebra.utils.serializers import encode_msgpack_bytes


class GameState(LiveState):
    """Test LiveState matching Rust state_id('GameState') and field_id('score')."""

    score: int = 0
    player: str = "anon"

    @live_action
    async def add_points(self, delta: int) -> int:
        self.score += delta
        return self.score


def test_fnv1a_hashes_match_rust_core() -> None:
    """Verify Python FNV-1a state_id and field_id match vertebra-core::protocol::state."""
    assert LiveState.compute_state_id("GameState") == 0x374157F8DDD4C9A8
    assert LiveState.compute_field_id("score") == 0xD22F9095


def test_hlc_tick_and_receive_monotonicity() -> None:
    """Verify HybridLogicalClock advances monotonically across same-millisecond ticks."""
    clock = HybridLogicalClock(wall_ms=1000, logical=0)
    ts1 = clock.tick(now_ms=1000)
    ts2 = clock.tick(now_ms=1000)
    assert ts2 > ts1

    remote_clock = HybridLogicalClock(wall_ms=2000, logical=5)
    merged = clock.receive(remote_clock.pack(), now_ms=1500)
    unpacked = HybridLogicalClock.unpack(merged)
    assert unpacked.wall_ms == 2000
    assert unpacked.logical == 6


def test_live_state_mutation_and_lww_resolution() -> None:
    """Verify field mutations emit MessagePack deltas and resolve LWW conflicts."""
    state = GameState(score=10)
    state.score = 25
    deltas = state.drain_deltas()
    assert len(deltas) == 1
    assert deltas[0].opcode == StateOpcode.FIELD_SET
    assert deltas[0].field_id == LiveState.compute_field_id("score")
    assert deltas[0].value == 25

    stale_frame = StateDeltaFrame(
        opcode=StateOpcode.FIELD_SET,
        state_id=state.state_id,
        field_id=LiveState.compute_field_id("score"),
        timestamp_ns=deltas[0].timestamp_ns - 1,
        value=5,
    )
    assert not state.apply_delta(stale_frame)
    assert state.score == 25

    newer_frame = StateDeltaFrame(
        opcode=StateOpcode.FIELD_SET,
        state_id=state.state_id,
        field_id=LiveState.compute_field_id("score"),
        timestamp_ns=deltas[0].timestamp_ns + 100,
        value=99,
    )
    assert state.apply_delta(newer_frame)
    assert state.score == 99


@pytest.mark.anyio
async def test_live_state_room_action_call_and_broadcast() -> None:
    """Verify LiveStateRoom processes ACTION_CALL frames and broadcasts state deltas."""
    published: list[tuple[str, bytes]] = []

    async def publish_fn(channel: str, payload: bytes) -> None:
        published.append((channel, payload))

    state = GameState(score=10)
    room = LiveStateRoom("room:1", state, publish_fn=publish_fn)

    call_frame = StateDeltaFrame(
        opcode=StateOpcode.ACTION_CALL,
        state_id=state.state_id,
        field_id=LiveState.compute_field_id("add_points"),
        call_id=42,
        value={"delta": 15},
    ).encode()
    responses = await room.handle_client_frame(call_frame)
    assert len(responses) == 2
    delta_out = StateDeltaFrame.decode(responses[0])
    result_out = StateDeltaFrame.decode(responses[1])
    assert delta_out.opcode == StateOpcode.FIELD_SET
    assert delta_out.value == 25
    assert result_out.opcode == StateOpcode.ACTION_RESULT
    assert result_out.call_id == 42
    assert result_out.value == 25
    assert len(published) == 1


def test_live_state_coalesces_and_bounds_pending_deltas() -> None:
    """Verify mutating a field 1,000 times coalesces into 1 delta with latest value and bounded pending capacity."""
    state = GameState(score=0)
    for i in range(1, 1001):
        state.score = i
    deltas = state.flush_deltas()
    assert len(deltas) == 1
    assert deltas[0].value == 1000
    assert deltas[0].field_id == LiveState.compute_field_id("score")

    for i in range(300):
        setattr(state, f"dynamic_field_{i}", i)
    dynamic_deltas = state.flush_deltas()
    assert len(dynamic_deltas) <= state._max_pending_deltas


def test_live_state_wire_hex_format() -> None:
    """Verify StateDeltaFrame MessagePack encoding matches expected wire hex string."""
    frame = StateDeltaFrame(
        opcode=StateOpcode.FIELD_SET,
        state_id=0x374157F8DDD4C9A8,
        field_id=0xD22F9095,
        timestamp_ns=1000,
        value=42,
    )
    assert frame.encode().hex() == "9501cf374157f8ddd4c9a8ced22f9095cd03e82a"


@pytest.mark.parametrize(
    "payload",
    [
        {"not": "a-list"},
        [StateOpcode.FIELD_SET],
        [StateOpcode.FIELD_SET, 1, 2],
        [StateOpcode.SYNC_REQUEST, 1, 99],
        [StateOpcode.FIELD_DELETE, 1, 2, 3, 4],
        [StateOpcode.SYNC_RESPONSE, 1],
        [StateOpcode.ACTION_CALL, 1, 2, 3],
        [StateOpcode.ACTION_RESULT, 1, 2],
        [StateOpcode.BATCH_PATCH, 1, 2, 3, 4],
        [True, 1],
        ["FIELD_SET", 1, 2, 3, 4],
        [0xFE, 1, 2],
        [StateOpcode.FIELD_SET, 1, True, 3, "x"],
    ],
    ids=[
        "non-list",
        "single-item",
        "field-set-short",
        "sync-request-extra",
        "field-delete-extra",
        "sync-response-short",
        "action-call-short",
        "action-result-short",
        "batch-patch-extra",
        "bool-opcode",
        "str-opcode",
        "unknown-opcode",
        "bool-field-id",
    ],
)
def test_state_delta_frame_decode_rejects_invalid_arity_and_types(payload: Any) -> None:
    """Verify StateDeltaFrame.decode raises ValueError on wrong sequence arity, non-list, or bad opcode."""
    encoded = encode_msgpack_bytes(payload)
    with pytest.raises(ValueError):
        StateDeltaFrame.decode(encoded)


@pytest.mark.anyio
async def test_live_state_subclass_inherits_mro_annotations_and_actions() -> None:
    """Verify LiveState subclasses inherit base annotations, defaults, deltas, and @live_action methods."""

    class BaseState(LiveState):
        count: int = 0

        @live_action
        async def bump(self, amount: int = 1) -> int:
            self.count += amount
            return self.count

    class ChildState(BaseState):
        label: str = "init"

    state = ChildState()
    assert state.snapshot() == {"count": 0, "label": "init"}

    state.count = 5
    state.label = "updated"
    deltas = state.drain_deltas()
    assert len(deltas) == 2
    field_ids = {d.field_id for d in deltas}
    assert field_ids == {LiveState.compute_field_id("count"), LiveState.compute_field_id("label")}

    room = LiveStateRoom("room:mro", state)
    call_frame = StateDeltaFrame(
        opcode=StateOpcode.ACTION_CALL,
        state_id=state.state_id,
        field_id=LiveState.compute_field_id("bump"),
        call_id=7,
        value={"amount": 3},
    ).encode()
    responses = await room.handle_client_frame(call_frame)
    assert len(responses) == 2
    assert state.count == 8
