import base64

from vertebra.messaging.protocol.state import StateDelta, StateOpcode, encode_delta, field_id, state_id
from vertebra.sync import StateSubscription, decode_sse_event, decode_state_payload


def test_decode_sse_event() -> None:
    raw = "id: 5\nevent: state\ndata: abc123\n\n"
    event = decode_sse_event(raw)
    assert event.event_id == "5"
    assert event.event == "state"
    assert event.data == "abc123"


def test_decode_state_payload_base64() -> None:
    delta = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state_id("GameState"),
        field_id=field_id("score"),
        timestamp_ns=123,
        value=42,
    )
    payload = base64.b64encode(encode_delta(delta)).decode()
    decoded = decode_state_payload(payload)
    assert decoded == delta


def test_state_subscription_applies_ws_message() -> None:
    sub = StateSubscription()
    delta = StateDelta(
        opcode=StateOpcode.FIELD_SET,
        state_id=state_id("GameState"),
        field_id=field_id("score"),
        timestamp_ns=10,
        value=7,
    )
    payload = base64.b64encode(encode_delta(delta)).decode()
    message = f'{{"type":"delta","id":3,"payload":"{payload}"}}'

    assert sub.apply_ws_message(message) is True
    assert sub.last_event_id == 3
    entry = sub.shadow.get_field(delta.state_id, delta.field_id)
    assert entry is not None
    assert entry.value == 7
