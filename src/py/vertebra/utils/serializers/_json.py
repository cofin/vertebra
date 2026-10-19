"""Private JSON serialization engine for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

import datetime
import enum
import json
import uuid as _uuid_mod
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Final, Literal, cast, overload

from msgspec.json import Decoder, Encoder

from vertebra import _typing
from vertebra._typing import (
    NUMPY_INSTALLED,
    PYDANTIC_INSTALLED,
    UUID_UTILS_INSTALLED,
)
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers._numpy import numpy_array_enc_hook
from vertebra.utils.serializers._schema import _dump_msgspec_fields
from vertebra.utils.type_guards import (
    dataclass_to_dict,
    is_attrs_instance,
    is_dataclass_instance,
    is_msgspec_struct,
)
from vertebra.utils.uuids import _load_uuid_utils

__all__ = (
    "BaseJSONSerializer",
    "JSONSerializer",
    "MsgspecSerializer",
    "OrjsonSerializer",
    "StandardLibSerializer",
    "convert_date_to_iso",
    "convert_datetime_to_gmt_iso",
    "decode_json",
    "encode_json",
    "encode_json_bytes",
    "get_default_serializer",
)


class BaseJSONSerializer(ABC):
    """Base class shared by JSON serializer implementations."""

    __slots__ = ()

    @abstractmethod
    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode Python data into JSON."""
        ...

    @abstractmethod
    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode JSON into Python data."""
        ...


class JSONSerializer(BaseJSONSerializer):
    """Msgspec-based JSON serializer."""

    __slots__ = ("_decoder", "_encoder")

    def __init__(self) -> None:
        self._encoder: Final[Encoder] = Encoder(enc_hook=_normalize_supported_value)
        self._decoder: Final[Decoder[Any]] = Decoder()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode Python data into JSON string or UTF-8 bytes."""
        encoded = self.encode_bytes(data)
        return encoded if as_bytes else encoded.decode("utf-8")

    def encode_bytes(self, data: Any) -> bytes:
        """Encode Python data directly into UTF-8 JSON bytes."""
        try:
            return self._encoder.encode(data)
        except TypeError as exc:
            if _is_explicit_unsupported_error(exc):
                raise
            return cast("bytes", _get_stdlib_fallback().encode(data, as_bytes=True))
        except ValueError:
            return cast("bytes", _get_stdlib_fallback().encode(data, as_bytes=True))

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode JSON string or bytes into Python data."""
        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            try:
                return self._decoder.decode(data)
            except (TypeError, ValueError):
                return _get_stdlib_fallback().decode(data, decode_bytes=decode_bytes)
        try:
            return self._decoder.decode(data.encode("utf-8"))
        except (TypeError, ValueError):
            return _get_stdlib_fallback().decode(data, decode_bytes=decode_bytes)


MsgspecSerializer = JSONSerializer


class OrjsonSerializer(BaseJSONSerializer):
    """Orjson-based JSON serializer retained for compatibility."""

    __slots__ = ()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode Python data into JSON via orjson or standard library fallback."""
        orjson_mod = import_optional("orjson")
        if orjson_mod is None:
            return _get_stdlib_fallback().encode(data, as_bytes=as_bytes)

        options = orjson_mod.OPT_NAIVE_UTC | orjson_mod.OPT_SERIALIZE_UUID
        if NUMPY_INSTALLED:
            options |= orjson_mod.OPT_SERIALIZE_NUMPY

        try:
            encoded = cast(
                "bytes",
                orjson_mod.dumps(data, default=_normalize_supported_value, option=options),
            )
        except TypeError as exc:
            if _is_explicit_unsupported_error(exc):
                raise
            if "type is not json serializable" in str(exc).lower():
                unsupported_msg = "unsupported JSON value"
                raise TypeError(unsupported_msg) from exc
            raise
        return encoded if as_bytes else encoded.decode("utf-8")

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode JSON into Python data via orjson or standard library fallback."""
        orjson_mod = import_optional("orjson")
        if orjson_mod is None:
            return _get_stdlib_fallback().decode(data, decode_bytes=decode_bytes)
        if isinstance(data, bytes) and not decode_bytes:
            return data
        return orjson_mod.loads(data)


class StandardLibSerializer(BaseJSONSerializer):
    """Standard library JSON serializer fallback."""

    __slots__ = ()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode Python data into JSON using the standard library."""
        encoded = json.dumps(data, default=_normalize_supported_value)
        return encoded.encode("utf-8") if as_bytes else encoded

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode JSON into Python data using the standard library."""
        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            return json.loads(data.decode("utf-8"))
        return json.loads(data)


def convert_datetime_to_gmt_iso(value: datetime.datetime) -> str:
    """Normalize datetime values to ISO 8601 strings."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(value: datetime.date) -> str:
    """Normalize date values to ISO 8601 strings."""
    return value.isoformat()


def get_default_serializer() -> JSONSerializer:
    """Return the default msgspec-backed JSON serializer."""
    global _default_serializer

    if _default_serializer is None:
        _default_serializer = JSONSerializer()
    return _default_serializer


def encode_json_bytes(data: Any) -> bytes:
    """Encode Python data directly into UTF-8 JSON bytes."""
    return get_default_serializer().encode_bytes(data)


@overload
def encode_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def encode_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


@overload
def encode_json(data: Any, *, as_bytes: bool = ...) -> str | bytes: ...


def encode_json(data: Any, *, as_bytes: bool = False) -> str | bytes:
    """Encode Python data into JSON."""
    encoded = get_default_serializer().encode_bytes(data)
    return encoded if as_bytes else encoded.decode("utf-8")


def decode_json(data: str | bytes, *, decode_bytes: bool = True) -> Any:
    """Decode JSON input into Python data."""
    return get_default_serializer().decode(data, decode_bytes=decode_bytes)


_UNLOADED_UUID_TYPE: Final[object] = object()
_uuid_utils_type: Any = _UNLOADED_UUID_TYPE
_stdlib_fallback: StandardLibSerializer | None = None
_default_serializer: JSONSerializer | None = None


def _get_uuid_utils_type() -> type[Any] | None:
    """Lazy-resolve and cache the uuid-utils UUID type when installed."""
    global _uuid_utils_type
    if _uuid_utils_type is not _UNLOADED_UUID_TYPE:
        return cast("type[Any] | None", _uuid_utils_type)
    if not UUID_UTILS_INSTALLED:
        _uuid_utils_type = None
        return None
    module = _load_uuid_utils()
    _uuid_utils_type = None if module is None else cast("type[Any]", module.UUID)
    return cast("type[Any] | None", _uuid_utils_type)


def _is_uuid_utils_instance(value: Any) -> bool:
    """Return True when value is an instance of uuid_utils.UUID."""
    uuid_type = _get_uuid_utils_type()
    return uuid_type is not None and isinstance(value, uuid_type)


def _dump_pydantic_model(value: Any) -> Any:
    """Dump a Pydantic model instance to a dictionary."""
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return value.dict()


def _normalize_supported_value(value: Any) -> Any:
    """Convert supported non-native values into JSON-compatible objects."""
    if isinstance(value, datetime.datetime):
        return convert_datetime_to_gmt_iso(value)
    if isinstance(value, datetime.date):
        return convert_date_to_iso(value)
    if isinstance(value, _uuid_mod.UUID) or _is_uuid_utils_instance(value):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, enum.Enum):
        return value.value
    if PYDANTIC_INSTALLED and isinstance(value, _typing.BaseModel):
        return _dump_pydantic_model(value)
    if is_dataclass_instance(value):
        return dataclass_to_dict(value)
    if is_attrs_instance(value):
        return _typing.attrs_asdict(value, recurse=True)
    if is_msgspec_struct(value):
        return _dump_msgspec_fields(value)
    numpy_value = numpy_array_enc_hook(value)
    if numpy_value is not value:
        return numpy_value

    msg = f"unsupported JSON value: {type(value).__name__}"
    raise TypeError(msg)


def _is_explicit_unsupported_error(exc: Exception) -> bool:
    """Return True when TypeError originates from an unsupported JSON value."""
    return "unsupported json value" in str(exc).lower()


def _get_stdlib_fallback() -> StandardLibSerializer:
    """Return the cached standard library JSON serializer fallback."""
    global _stdlib_fallback
    if _stdlib_fallback is None:
        _stdlib_fallback = StandardLibSerializer()
    return _stdlib_fallback
