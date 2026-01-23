"""Serialization backends for Vertebra procedures."""

import contextlib
import typing
from typing import Any, TypeVar, overload

from vertebra.protocols import Serializer
from vertebra.serializers import JSONCodec, decode_json, encode_json, get_json_codec
from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import NamedTypeRef, ObjectType, TypeNode

if typing.TYPE_CHECKING:
    from collections.abc import AsyncIterable

T = TypeVar("T")


class StreamingResponse:
    """Wraps an AsyncIterable to provide streaming serialization."""

    def __init__(self, iterator: "AsyncIterable[Any]", serializer: Serializer | None = None) -> None:
        self.iterator = iterator
        self.serializer = serializer or get_serializer()

    async def stream(self) -> "AsyncIterable[bytes]":
        """Yield serialized chunks (NDJSON)."""
        async for item in self.iterator:
            chunk = self.serializer.serialize(item)
            yield chunk + b"\n"


class MsgspecSerializer:
    """msgspec-backed JSON serializer."""

    def __init__(self) -> None:
        import msgspec

        self._msgspec = msgspec
        self._codec: JSONCodec = get_json_codec()
        self._extractor = TypeExtractor()

    def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
        # Procedures expect bytes
        return self._codec.encode(value, as_bytes=True)  # type: ignore

    @overload
    def deserialize(self, data: bytes, type_hint: type[T]) -> T: ...

    @overload
    def deserialize(self, data: bytes, type_hint: None = ...) -> Any: ...

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        return self._codec.decode(data, type_hint=type_hint)

    def get_type_metadata(self, type_hint: type | None = None) -> TypeNode | None:
        if not isinstance(type_hint, type):
            return None
        if not issubclass(type_hint, self._msgspec.Struct):
            return None
        inspect = getattr(self._msgspec, "inspect", None)
        type_info = getattr(inspect, "type_info", None) if inspect else None
        if not callable(type_info):
            return None
        info = type_info(type_hint)
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        for field in getattr(info, "fields", ()) or ():
            field_type = self._convert_hint(getattr(field, "type", object()))
            fields[field.name] = field_type
            if not getattr(field, "required", True):
                optional_fields.add(field.name)
        return ObjectType(name=type_hint.__name__, fields=fields, optional_fields=optional_fields)

    def _convert_hint(self, type_hint: object) -> TypeNode:
        if isinstance(type_hint, type) and issubclass(type_hint, self._msgspec.Struct):
            return NamedTypeRef(type_hint.__name__)
        return self._extractor.from_hint(type_hint)


class PydanticSerializer:
    """Pydantic-backed JSON serializer."""

    def __init__(self) -> None:
        from pydantic import BaseModel

        self._BaseModel = BaseModel
        self._extractor = TypeExtractor()

    def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
        if isinstance(value, self._BaseModel):
            return value.model_dump_json().encode()
        return encode_json(value, as_bytes=True)

    @overload
    def deserialize(self, data: bytes, type_hint: type[T]) -> T: ...

    @overload
    def deserialize(self, data: bytes, type_hint: None = ...) -> Any: ...

    def deserialize(self, data: bytes, type_hint: type[Any] | None = None) -> Any:
        if isinstance(type_hint, type) and issubclass(type_hint, self._BaseModel):
            return type_hint.model_validate_json(data)
        return decode_json(data, type_hint=type_hint)

    def get_type_metadata(self, type_hint: type | None = None) -> TypeNode | None:
        if not isinstance(type_hint, type) or not issubclass(type_hint, self._BaseModel):
            return None
        fields: dict[str, TypeNode] = {}
        optional_fields: set[str] = set()
        for name, field in type_hint.model_fields.items():
            annotation = field.annotation or object()
            fields[name] = self._convert_hint(annotation)
            if not field.is_required():
                optional_fields.add(name)
        return ObjectType(name=type_hint.__name__, fields=fields, optional_fields=optional_fields)

    def _convert_hint(self, type_hint: object) -> TypeNode:
        if isinstance(type_hint, type) and issubclass(type_hint, self._BaseModel):
            return NamedTypeRef(type_hint.__name__)
        return self._extractor.from_hint(type_hint)


_SERIALIZERS: dict[str, Serializer] = {}


def register_serializer(name: str, serializer: Serializer) -> None:
    _SERIALIZERS[name] = serializer


def detect_serializer(type_hint: type | None) -> str:
    if isinstance(type_hint, type):
        msgspec_module: Any = None
        try:
            import msgspec

            msgspec_module = msgspec
        except ImportError:
            pass
        if msgspec_module is not None:
            try:
                if issubclass(type_hint, msgspec_module.Struct):
                    return "msgspec"
            except TypeError:
                pass
        try:
            from pydantic import BaseModel
        except ImportError:
            base_model = None
        else:
            base_model = BaseModel
        if base_model is not None:
            try:
                if issubclass(type_hint, base_model):
                    return "pydantic"
            except TypeError:
                pass
    return "msgspec"


def get_serializer(serializer: str | Serializer | None = None, *, type_hint: type | None = None) -> Serializer:
    if serializer is None:
        serializer = detect_serializer(type_hint)
    if isinstance(serializer, str):
        _ensure_default_serializers()
        resolved = _SERIALIZERS.get(serializer)
        if resolved is None:
            msg = f"Serializer '{serializer}' is not registered"
            raise ValueError(msg)
        return resolved
    return serializer


def _ensure_default_serializers() -> None:
    if "msgspec" not in _SERIALIZERS:
        with contextlib.suppress(Exception):
            register_serializer("msgspec", MsgspecSerializer())
    if "pydantic" not in _SERIALIZERS:
        with contextlib.suppress(Exception):
            register_serializer("pydantic", PydanticSerializer())
