"""Private JSON serialization engine for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

from __future__ import annotations

import datetime
import enum
import uuid as _uuid_mod
from decimal import Decimal
from typing import Any, ClassVar, Final, Literal, cast, overload

import msgspec
from msgspec.json import Decoder, Encoder

from vertebra import _typing
from vertebra._typing import (
    PYDANTIC_INSTALLED,
    UUID_UTILS_INSTALLED,
)
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
    "JSONSerializer",
    "convert_date_to_iso",
    "convert_datetime_to_gmt_iso",
    "decode_json",
    "encode_json",
    "encode_json_bytes",
    "encode_json_into",
    "format_json",
    "get_default_serializer",
)


class JSONSerializer:
    """Msgspec-based JSON serializer."""

    __slots__ = ("_decoder", "_encoder")

    _default: ClassVar[JSONSerializer | None] = None

    def __init__(self) -> None:
        self._encoder: Final[Encoder] = Encoder(enc_hook=_normalize_supported_value)
        self._decoder: Final[Decoder[Any]] = Decoder()

    @classmethod
    def default(cls) -> JSONSerializer:
        """Return process-global default JSONSerializer instance."""
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode Python data into JSON string or UTF-8 bytes."""
        encoded = self.encode_bytes(data)
        return encoded if as_bytes else encoded.decode("utf-8")

    def encode_bytes(self, data: Any) -> bytes:
        """Encode Python data directly into UTF-8 JSON bytes."""
        return self._encoder.encode(data)

    def encode_into(self, data: Any, buffer: bytearray, offset: int = 0) -> None:
        """Encode Python data directly into a pre-allocated ``bytearray`` at ``offset`` with zero allocation."""
        self._encoder.encode_into(data, buffer, offset)

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode JSON string or bytes into Python data."""
        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            return self._decoder.decode(data)
        return self._decoder.decode(data.encode("utf-8"))


def convert_datetime_to_gmt_iso(value: datetime.datetime) -> str:
    """Normalize datetime values to ISO 8601 strings."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(value: datetime.date) -> str:
    """Normalize date values to ISO 8601 strings."""
    return value.isoformat()


_UNLOADED_UUID_TYPE: Final[object] = object()


class _UuidTypeCache:
    """Class-scoped holder for lazy UUID backend."""

    uuid_utils_type: ClassVar[Any] = _UNLOADED_UUID_TYPE


def get_default_serializer() -> JSONSerializer:
    """Return the default msgspec-backed JSON serializer."""
    return JSONSerializer.default()


def encode_json_bytes(data: Any) -> bytes:
    """Encode Python data directly into UTF-8 JSON bytes."""
    return get_default_serializer().encode_bytes(data)


def encode_json_into(data: Any, buffer: bytearray, offset: int = 0) -> None:
    """Encode Python data directly into a pre-allocated ``bytearray`` at ``offset``."""
    get_default_serializer().encode_into(data, buffer, offset)


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


def format_json(value: Any, *, indent: int = 2) -> str:
    """Format Python data into a pretty-printed JSON string."""
    return msgspec.json.format(encode_json_bytes(value), indent=indent).decode("utf-8")


def _get_uuid_utils_type() -> type[Any] | None:
    """Lazy-resolve and cache the uuid-utils UUID type when installed."""
    if _UuidTypeCache.uuid_utils_type is not _UNLOADED_UUID_TYPE:
        return cast("type[Any] | None", _UuidTypeCache.uuid_utils_type)
    if not UUID_UTILS_INSTALLED:
        _UuidTypeCache.uuid_utils_type = None
        return None
    module = _load_uuid_utils()
    _UuidTypeCache.uuid_utils_type = None if module is None else cast("type[Any]", module.UUID)
    return cast("type[Any] | None", _UuidTypeCache.uuid_utils_type)


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
