import asyncio

import msgspec
import pytest

from vertebra.protocol.state import StateOpcode, decode_delta, field_id, state_id
from vertebra.state import NativeState


class Counter(NativeState):
    count: int = 0


class GameState(NativeState):
    players: dict[str, int] = msgspec.field(default_factory=dict)


@pytest.mark.anyio
async def test_field_set_delta() -> None:
    counter = Counter()
    iterator = counter.subscribe_deltas()

    receive_task = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0)
    counter.count = 2

    payload = await receive_task
    delta = decode_delta(payload)

    assert delta.opcode is StateOpcode.FIELD_SET
    assert delta.state_id == state_id("Counter")
    assert delta.field_id == field_id("count")
    assert delta.value == 2
    assert delta.timestamp_ns is not None
    assert delta.timestamp_ns > 0


@pytest.mark.anyio
async def test_object_replace_delta() -> None:
    game = GameState()
    iterator = game.subscribe_deltas()

    receive_task = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0)
    game.players = {"alice": 1}

    payload = await receive_task
    delta = decode_delta(payload)

    assert delta.opcode is StateOpcode.OBJECT_REPLACE
    assert delta.state_id == state_id("GameState")
    assert delta.field_id == field_id("players")
    assert delta.value == {"alice": 1}
