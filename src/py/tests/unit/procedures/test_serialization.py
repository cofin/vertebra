from typing import Any, TypeVar, overload

import pytest

from vertebra.procedures.serialization import (
    MsgspecSerializer,
    PydanticSerializer,
    detect_serializer,
    get_serializer,
    register_serializer,
)
from vertebra.types.ir import ObjectType

pytestmark = pytest.mark.unit

T = TypeVar("T")


def test_detect_serializer_defaults() -> None:
    assert detect_serializer(int) == "msgspec"


def test_msgspec_serializer_roundtrip() -> None:
    msgspec = pytest.importorskip("msgspec")

    class User(msgspec.Struct):
        id: int
        name: str

    serializer = MsgspecSerializer()
    payload = User(id=1, name="A")
    data = serializer.serialize(payload, User)
    assert serializer.deserialize(data, User) == payload
    metadata = serializer.get_type_metadata(User)
    assert isinstance(metadata, ObjectType)
    assert "id" in metadata.fields


def test_pydantic_serializer_roundtrip() -> None:
    pydantic = pytest.importorskip("pydantic")
    base_model = pydantic.BaseModel

    class User(base_model):
        id: int
        name: str

    serializer = PydanticSerializer()
    payload = User(id=2, name="B")
    data = serializer.serialize(payload, User)
    result = serializer.deserialize(data, User)
    assert isinstance(result, User)
    metadata = serializer.get_type_metadata(User)
    assert isinstance(metadata, ObjectType)
    assert "name" in metadata.fields


def test_serializer_registry_custom() -> None:
    class DummySerializer:
        def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
            return b"dummy"

        @overload
        def deserialize(self, data: bytes, type_hint: type[T]) -> T: ...

        @overload
        def deserialize(self, data: bytes, type_hint: None = ...) -> Any: ...

        def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
            return {"ok": True}

        def get_type_metadata(self, type_hint: type | None = None) -> None:
            return None

    register_serializer("dummy", DummySerializer())
    resolved = get_serializer("dummy")
    assert isinstance(resolved, DummySerializer)
