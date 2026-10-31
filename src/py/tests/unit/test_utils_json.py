import dataclasses
import datetime
import decimal
import enum
import gc
import uuid
from pathlib import Path

import msgspec
import pytest

from vertebra.typing import (
    BaseStruct,
    CamelizedBaseStruct,
    FrozenBaseStruct,
    FrozenCamelizedStruct,
)
from vertebra.utils.serializers import (
    MsgPackSerializer,
    decode_json,
    decode_msgpack,
    encode_json,
    encode_json_bytes,
    encode_msgpack_bytes,
    encode_msgpack_into,
    get_default_json_serializer,
    get_default_msgpack_serializer,
)


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
    encoded = encode_json(data, as_bytes=True)
    assert decode_json(encoded) == data


def test_encode_datetime():
    dt = datetime.datetime(2026, 3, 20, 10, 30, tzinfo=datetime.UTC)
    encoded = encode_json({"dt": dt}, as_bytes=True)
    assert b"2026-03-20T10:30:00Z" in encoded


def test_encode_uuid():
    u = uuid.uuid4()
    encoded = encode_json({"u": u}, as_bytes=True)
    assert str(u).encode() in encoded


def test_encode_decimal():
    d = decimal.Decimal("42.5")
    encoded = encode_json({"d": d}, as_bytes=True)
    assert b"42.5" in encoded


def test_encode_enum():
    e = MyEnum.A
    encoded = encode_json({"e": e}, as_bytes=True)
    assert b'"a"' in encoded


def test_encode_dataclass():
    d = MyData(name="test", age=25)
    encoded = encode_json(d, as_bytes=True)
    assert b'"name":"test"' in encoded
    assert b'"age":25' in encoded


def test_encode_struct():
    s = MyStruct(id=1, data="hello")
    encoded = encode_json(s, as_bytes=True)
    assert b'"id":1' in encoded
    assert b'"data":"hello"' in encoded


def test_encode_complex_types():
    data = {
        "set": {1, 2, 3},
        "frozenset": frozenset([4, 5]),
        "decimal": decimal.Decimal("10.0"),
    }
    encoded = encode_json(data, as_bytes=True)
    decoded = decode_json(encoded)
    assert set(decoded["set"]) == {1, 2, 3}
    assert set(decoded["frozenset"]) == {4, 5}
    assert decoded["decimal"] == "10.0"


def test_encode_json_as_string():
    data = {"a": 1}
    encoded = encode_json(data, as_bytes=False)
    assert isinstance(encoded, str)
    assert encoded == '{"a":1}'


def test_decode_json_string():
    data = '{"a":1}'
    decoded = decode_json(data)
    assert decoded == {"a": 1}


def test_msgpack_roundtrip_basic_and_memoryview() -> None:
    """Verify MsgPackSerializer round-trips basic primitives, buffer writes, and memoryview inputs."""
    payload = {"key": "value", "count": 42, "items": [1, 2, 3]}
    encoded = encode_msgpack_bytes(payload)
    assert isinstance(encoded, bytes)

    decoded = decode_msgpack(encoded)
    assert decoded == payload

    view_decoded = decode_msgpack(memoryview(encoded))
    assert view_decoded == payload

    buf = bytearray(b"prefix:")
    offset = len(buf)
    encode_msgpack_into(payload, buf, offset=offset)
    assert decode_msgpack(memoryview(buf)[offset:]) == payload
    assert get_default_json_serializer() is get_default_json_serializer()
    assert get_default_msgpack_serializer() is MsgPackSerializer.default()


def test_msgpack_struct_and_typed_decode() -> None:
    """Verify MsgPackSerializer encodes msgspec.Struct and decodes typed schemas."""
    struct_inst = MyStruct(id=99, data="structured")
    encoded = encode_msgpack_bytes(struct_inst)

    decoded = decode_msgpack(encoded, type=MyStruct)
    assert isinstance(decoded, MyStruct)
    assert decoded.id == 99
    assert decoded.data == "structured"


def test_msgpack_uuid_datetime_path_hooks() -> None:
    """Verify MsgPackSerializer hooks for UUID, datetime, and Path objects."""
    uid = uuid.uuid4()
    now = datetime.datetime(2026, 10, 22, 12, 0, tzinfo=datetime.UTC)
    pth = Path("/var/log/vertebra.log")

    payload = {"uid": uid, "time": now, "path": pth}
    encoded = encode_msgpack_bytes(payload)
    assert isinstance(encoded, bytes)

    decoded = decode_msgpack(encoded)
    assert decoded["uid"] == str(uid)
    assert decoded["path"] == str(pth)

    serializer = MsgPackSerializer.default()
    assert serializer.decode(encoded) == decoded

    path_encoded = encode_msgpack_bytes(pth)
    decoded_path = decode_msgpack(path_encoded, type=Path)
    assert isinstance(decoded_path, Path)
    assert decoded_path == pth

    uuid_encoded = encode_msgpack_bytes(uid)
    decoded_uuid = decode_msgpack(uuid_encoded, type=uuid.UUID)
    assert isinstance(decoded_uuid, uuid.UUID)
    assert decoded_uuid == uid


class SampleBaseItem(BaseStruct):
    """Mutable snake_case base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleCamelItem(CamelizedBaseStruct):
    """Mutable camelCase base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleFrozenItem(FrozenBaseStruct):
    """Immutable snake_case base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


class SampleFrozenCamelItem(FrozenCamelizedStruct):
    """Immutable camelCase base struct with omitted defaults."""

    item_name: str
    optional_tag: str | None = None


def test_base_struct_hierarchy_configs_immutability_and_wire_encoding() -> None:
    """Verify BaseStruct, CamelizedBaseStruct, FrozenBaseStruct, and FrozenCamelizedStruct contracts."""
    for cls in (SampleBaseItem, SampleCamelItem, SampleFrozenItem, SampleFrozenCamelItem):
        cfg = cls.__struct_config__
        assert cfg.gc is False
        assert cfg.omit_defaults is True
        assert cfg.repr_omit_defaults is True

    assert SampleBaseItem.__struct_config__.frozen is False
    assert SampleCamelItem.__struct_config__.frozen is False
    assert SampleFrozenItem.__struct_config__.frozen is True
    assert SampleFrozenCamelItem.__struct_config__.frozen is True

    mutable_base = SampleBaseItem(item_name="alpha")
    mutable_camel = SampleCamelItem(item_name="beta")
    frozen_base = SampleFrozenItem(item_name="gamma")
    frozen_camel = SampleFrozenCamelItem(item_name="delta")

    assert not gc.is_tracked(mutable_base)
    assert not gc.is_tracked(mutable_camel)
    assert not gc.is_tracked(frozen_base)
    assert not gc.is_tracked(frozen_camel)

    mutable_base.optional_tag = "updated"
    assert mutable_base.to_dict() == {"item_name": "alpha", "optional_tag": "updated"}

    with pytest.raises(AttributeError):
        frozen_base.item_name = "mutated"

    with pytest.raises(AttributeError):
        frozen_camel.item_name = "mutated"

    assert decode_json(encode_json_bytes(frozen_base)) == {"item_name": "gamma"}
    assert decode_json(encode_json_bytes(frozen_camel)) == {"itemName": "delta"}
    assert decode_json(encode_json_bytes(mutable_camel)) == {"itemName": "beta"}
    assert "optional_tag" not in repr(frozen_base)
    assert "optional_tag" not in repr(frozen_camel)
