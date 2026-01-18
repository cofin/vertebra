"""Serialization backends for Vertebra procedures."""

from __future__ import annotations

import contextlib
import json
from typing import Any, Protocol, runtime_checkable

from vertebra.types.extractor import TypeExtractor
from vertebra.types.ir import NamedTypeRef, ObjectType, TypeNode


@runtime_checkable
class Serializer(Protocol):
    """Protocol for serialization backends."""

    def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
        """Serialize a Python value to bytes."""

    def deserialize(self, data: bytes, type_hint: type | None = None) -> Any:
        """Deserialize bytes to a Python value."""

    def get_type_metadata(self, type_hint: type | None = None) -> TypeNode | None:
        """Extract type metadata for Type IR."""


class MsgspecSerializer:
    """msgspec-backed JSON serializer."""

    def __init__(self) -> None:
        import msgspec

        self._msgspec = msgspec
        self._encoder = msgspec.json.Encoder()
        self._decoder_cache: dict[type, msgspec.json.Decoder] = {}
        self._extractor = TypeExtractor()

    def serialize(self, value: Any, type_hint: type | None = None) -> bytes:
        return self._encoder.encode(value)

    def deserialize(self, data: bytes, type_hint: type | None = None) -> Any:
        if type_hint is None:
            return self._msgspec.json.decode(data)
        decoder = self._decoder_cache.get(type_hint)
        if decoder is None:
            decoder = self._msgspec.json.Decoder(type_hint)
            self._decoder_cache[type_hint] = decoder
        return decoder.decode(data)

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
        return ObjectType(
            name=type_hint.__name__,
            fields=fields,
            optional_fields=optional_fields,
        )

    def _convert_hint(self, type_hint: object) -> TypeNode:
        if (
            isinstance(type_hint, type)
            and issubclass(type_hint, self._msgspec.Struct)
        ):
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
        return json.dumps(value).encode()

    def deserialize(self, data: bytes, type_hint: type | None = None) -> Any:
        if isinstance(type_hint, type) and issubclass(type_hint, self._BaseModel):
            return type_hint.model_validate_json(data)
        return json.loads(data)

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
        return ObjectType(
            name=type_hint.__name__,
            fields=fields,
            optional_fields=optional_fields,
        )

    def _convert_hint(self, type_hint: object) -> TypeNode:
        if (
            isinstance(type_hint, type)
            and issubclass(type_hint, self._BaseModel)
        ):
            return NamedTypeRef(type_hint.__name__)
        return self._extractor.from_hint(type_hint)


_SERIALIZERS: dict[str, Serializer] = {}


def register_serializer(name: str, serializer: Serializer) -> None:
    _SERIALIZERS[name] = serializer


def detect_serializer(type_hint: type | None) -> str:
    if isinstance(type_hint, type):
        try:
            import msgspec

            if issubclass(type_hint, msgspec.Struct):
                return "msgspec"
        except Exception:
            pass
        try:
            from pydantic import BaseModel

            if issubclass(type_hint, BaseModel):
                return "pydantic"
        except Exception:
            pass
    return "msgspec"


def get_serializer(
    serializer: str | Serializer | None = None,
    *,
    type_hint: type | None = None,
) -> Serializer:
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
