import msgspec

from vertebra.protocol.state import StateDelta, StateOpcode, decode_delta, encode_delta, field_id, state_id


def test_state_id_hashing() -> None:
    assert state_id("GameState") == 0x374157F8DDD4C9A8
    assert field_id("score") == 0xD22F9095


def test_encode_decode_field_set() -> None:
    delta = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state_id("GameState"),
        field_id=field_id("score"),
        timestamp_ns=1234,
        value={"count": 1},
    )

    encoded = encode_delta(delta)
    raw = msgspec.msgpack.decode(encoded)

    assert raw == [int(StateOpcode.FIELD_SET), delta.state_id, delta.field_id, delta.timestamp_ns, delta.value]

    decoded = decode_delta(encoded)
    assert decoded == delta


def test_encode_decode_field_delete() -> None:
    delta = StateDelta(
        opcode=StateOpcode.FIELD_DELETE, state_id=state_id("GameState"), field_id=field_id("score"), timestamp_ns=999
    )

    encoded = encode_delta(delta)
    raw = msgspec.msgpack.decode(encoded)

    assert raw == [int(StateOpcode.FIELD_DELETE), delta.state_id, delta.field_id, delta.timestamp_ns]

    decoded = decode_delta(encoded)
    assert decoded == delta
