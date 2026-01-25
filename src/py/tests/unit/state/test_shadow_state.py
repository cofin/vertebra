import pytest

from vertebra.protocol.state import StateDelta, StateOpcode, field_id, state_id
from vertebra.state_shadow import ShadowState


def test_shadow_state_lww() -> None:
    shadow = ShadowState()
    state = state_id("GameState")
    field = field_id("score")

    newer = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state,
        field_id=field,
        timestamp_ns=2,
        value=10,
    )
    older = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state,
        field_id=field,
        timestamp_ns=1,
        value=1,
    )

    assert shadow.apply_delta(newer) is True
    assert shadow.apply_delta(older) is False

    entry = shadow.get_field(state, field)
    assert entry is not None
    assert entry.value == 10


def test_shadow_state_delete() -> None:
    shadow = ShadowState()
    state = state_id("GameState")
    field = field_id("score")

    shadow.apply_delta(
        StateDelta(
            opcode=StateOpcode.FIELD_SET,
            state_id=state,
            field_id=field,
            timestamp_ns=1,
            value=3,
        )
    )

    shadow.apply_delta(
        StateDelta(
            opcode=StateOpcode.FIELD_DELETE,
            state_id=state,
            field_id=field,
            timestamp_ns=2,
        )
    )

    entry = shadow.get_field(state, field)
    assert entry is not None
    assert entry.deleted is True
    assert entry.value is None


@pytest.mark.anyio
async def test_shadow_state_apply_payload() -> None:
    shadow = ShadowState()
    state = state_id("GameState")
    field = field_id("score")

    payload = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state,
        field_id=field,
        timestamp_ns=5,
        value=42,
    )
    encoded = payload  # reuse StateDelta for encoding

    from vertebra.protocol.state import encode_delta

    shadow.apply_payload(encode_delta(encoded))

    entry = shadow.get_field(state, field)
    assert entry is not None
    assert entry.value == 42
