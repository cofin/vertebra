"""Unit tests for Live Event State MessagePack deltas, HLC LWW, and LiveStateRoom."""

from __future__ import annotations

import pytest

from vertebra.framework.live import (
    HybridLogicalClock,
    LiveState,
    LiveStateRoom,
    StateDeltaFrame,
    StateOpcode,
    decode_state_delta,
    encode_state_delta,
    fnv1a_field_id,
    fnv1a_state_id,
    live_action,
)


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
    assert fnv1a_state_id("GameState") == 0x374157F8DDD4C9A8
    assert fnv1a_field_id("score") == 0xD22F9095


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
    assert deltas[0].field_id == fnv1a_field_id("score")
    assert deltas[0].value == 25

    stale_frame = StateDeltaFrame(
        opcode=StateOpcode.FIELD_SET,
        state_id=state.state_id,
        field_id=fnv1a_field_id("score"),
        timestamp_ns=deltas[0].timestamp_ns - 1,
        value=5,
    )
    assert not state.apply_delta(stale_frame)
    assert state.score == 25

    newer_frame = StateDeltaFrame(
        opcode=StateOpcode.FIELD_SET,
        state_id=state.state_id,
        field_id=fnv1a_field_id("score"),
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

    call_frame = encode_state_delta(
        StateDeltaFrame(
            opcode=StateOpcode.ACTION_CALL,
            state_id=state.state_id,
            field_id=fnv1a_field_id("add_points"),
            call_id=42,
            value={"delta": 15},
        )
    )
    responses = await room.handle_client_frame(call_frame)
    assert len(responses) == 2
    delta_out = decode_state_delta(responses[0])
    result_out = decode_state_delta(responses[1])
    assert delta_out.opcode == StateOpcode.FIELD_SET
    assert delta_out.value == 25
    assert result_out.opcode == StateOpcode.ACTION_RESULT
    assert result_out.call_id == 42
    assert result_out.value == 25
    assert len(published) == 1
