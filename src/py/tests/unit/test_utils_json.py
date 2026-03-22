import dataclasses
import datetime
import decimal
import enum
import uuid

import msgspec

from vertebra.utils.serializers import from_json, to_json


class MyEnum(enum.Enum):
    A = "a"
    B = "b"


@dataclasses.dataclass
class MyData:
    name: str
    age: int


class MyStruct(msgspec.Struct):
    id: int
    data: str


def test_encode_decode_basic():
    data = {"a": 1, "b": "test"}
    encoded = to_json(data, as_bytes=True)
    assert from_json(encoded) == data


def test_encode_datetime():
    dt = datetime.datetime(2026, 3, 20, 10, 30, tzinfo=datetime.UTC)
    encoded = to_json({"dt": dt}, as_bytes=True)
    # msgspec encodes datetime to ISO format
    assert b"2026-03-20T10:30:00Z" in encoded


def test_encode_uuid():
    u = uuid.uuid4()
    encoded = to_json({"u": u}, as_bytes=True)
    assert str(u).encode() in encoded


def test_encode_decimal():
    d = decimal.Decimal("42.5")
    encoded = to_json({"d": d}, as_bytes=True)
    assert b"42.5" in encoded


def test_encode_enum():
    e = MyEnum.A
    encoded = to_json({"e": e}, as_bytes=True)
    assert b'"a"' in encoded


def test_encode_dataclass():
    d = MyData(name="test", age=25)
    encoded = to_json(d, as_bytes=True)
    assert b'"name":"test"' in encoded
    assert b'"age":25' in encoded


def test_encode_struct():
    s = MyStruct(id=1, data="hello")
    encoded = to_json(s, as_bytes=True)
    assert b'"id":1' in encoded
    assert b'"data":"hello"' in encoded


def test_encode_complex_types():
    data = {
        "set": {1, 2, 3},
        "frozenset": frozenset([4, 5]),
        "decimal": decimal.Decimal("10.0"),
    }
    encoded = to_json(data, as_bytes=True)
    decoded = from_json(encoded)
    assert set(decoded["set"]) == {1, 2, 3}
    assert set(decoded["frozenset"]) == {4, 5}
    assert decoded["decimal"] == "10.0"


def test_to_json_as_string():
    data = {"a": 1}
    encoded = to_json(data, as_bytes=False)
    assert isinstance(encoded, str)
    assert encoded == '{"a":1}'


def test_from_json_string():
    data = '{"a":1}'
    decoded = from_json(data)
    assert decoded == {"a": 1}
