import datetime
import decimal
import enum
import uuid

import pytest

from vertebra._internal.serialization import decode_json, decode_msgpack, encode_json, encode_msgpack


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


@pytest.mark.unit
@pytest.mark.parametrize("codec", ["json", "msgpack"])
def test_serialization_hardening_roundtrip(codec: str) -> None:
    data = {
        "uuid": uuid.uuid4(),
        "date": datetime.date(2026, 1, 21),
        "datetime": datetime.datetime(2026, 1, 21, 12, 0, 0, tzinfo=datetime.UTC),
        "enum": Color.RED,
        "decimal": decimal.Decimal("123.45"),
        "list": [1, 2, 3],
        "dict": {"a": 1},
        "none": None,
        "bool": True,
        "int": 42,
        "float": 3.14,
    }

    if codec == "json":
        encoded = encode_json(data)
        decoded = decode_json(encoded)
        # JSON decodes datetime as string by default in msgspec if no type hint
        assert decoded["datetime"] == data["datetime"].isoformat().replace("+00:00", "Z")
        assert decoded["date"] == data["date"].isoformat()
    else:
        encoded = encode_msgpack(data)
        decoded = decode_msgpack(encoded)
        # MsgPack may decode datetime as native datetime object
        if isinstance(decoded["datetime"], str):
            assert decoded["datetime"] == data["datetime"].isoformat().replace("+00:00", "Z")
        else:
            assert decoded["datetime"] == data["datetime"]

        if isinstance(decoded["date"], str):
            assert decoded["date"] == data["date"].isoformat()
        else:
            assert decoded["date"] == data["date"]

    # Check UUID (serialized as string by _default_encoder)
    assert decoded["uuid"] == str(data["uuid"])

    # Check Decimal (serialized as string by _default_encoder)
    assert decoded["decimal"] == str(data["decimal"])

    # Check Enum (serialized as value)
    assert decoded["enum"] == data["enum"].value

    # Check simple types
    assert decoded["list"] == data["list"]
    assert decoded["dict"] == data["dict"]
    assert decoded["none"] == data["none"]
    assert decoded["bool"] == data["bool"]
    assert decoded["int"] == data["int"]
    assert pytest.approx(decoded["float"]) == data["float"]


@pytest.mark.unit
def test_serialization_typed_decoding() -> None:
    # Test decoding into a specific type if needed
    pass
