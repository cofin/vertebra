from __future__ import annotations

import pytest

from vertebra._internal.serialization import MessagePackCodec

pytestmark = pytest.mark.unit


def test_messagepack_roundtrip() -> None:
    codec = MessagePackCodec()
    payload = {"name": "vertebra", "values": [1, 2, 3], "ok": True}
    encoded = codec.encode(payload)
    decoded = codec.decode(encoded)
    assert decoded == payload
