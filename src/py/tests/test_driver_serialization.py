from __future__ import annotations

from vertebra._internal.serialization import MessagePackCodec


def test_messagepack_roundtrip() -> None:
    codec = MessagePackCodec()
    payload = {"name": "vertebra", "values": [1, 2, 3], "ok": True}
    encoded = codec.encode(payload)
    decoded = codec.decode(encoded)
    assert decoded == payload
