"""Private JSON serialization engine for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

from __future__ import annotations

import datetime
import enum
import threading
from collections import OrderedDict
from decimal import Decimal
from typing import Any, ClassVar, Final, Literal, overload

import msgspec
from msgspec.json import Decoder, Encoder

from vertebra import typing as vertebra_typing
from vertebra.typing import PYDANTIC_INSTALLED
from vertebra.utils.serializers._numpy import numpy_array_enc_hook
from vertebra.utils.serializers._schema import _dump_msgspec_fields, is_uuid_instance
from vertebra.utils.type_guards import (
    dataclass_to_dict,
    is_attrs_instance,
    is_dataclass_instance,
    is_msgspec_struct,
)

__all__ = (
    "JSONSerializer",
    "convert_date_to_iso",
    "convert_datetime_to_gmt_iso",
    "decode_json",
    "encode_json",
    "encode_json_bytes",
    "encode_json_into",
    "format_json",
    "get_default_json_serializer",
)

_DEFAULT_LOCK: Final[threading.Lock] = threading.Lock()
_MAX_TYPED_DECODERS: Final[int] = 1024


class JSONSerializer:
    """Msgspec-based JSON serializer."""

    __slots__ = ("_decoder", "_decoder_lock", "_encoder", "_typed_decoders")

    _default: ClassVar[JSONSerializer | None] = None

    def __init__(self) -> None:
        self._encoder: Final[Encoder] = Encoder(enc_hook=_normalize_supported_value)
        self._decoder: Final[Decoder[Any]] = Decoder()
        self._decoder_lock: Final[threading.Lock] = threading.Lock()
        self._typed_decoders: OrderedDict[Any, Decoder[Any]] = OrderedDict()

    @classmethod
    def default(cls) -> JSONSerializer:
        """Return process-global default JSONSerializer instance."""
        if cls._default is None:
            with _DEFAULT_LOCK:
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

    def decode(self, data: str | bytes, *, decode_bytes: bool = True, type: Any = Any) -> Any:
        """Decode JSON string or bytes into Python data, optionally into a typed schema."""
        if isinstance(data, bytes) and not decode_bytes:
            return data
        raw = data if isinstance(data, bytes) else data.encode("utf-8")
        if type is Any or type is object:
            return self._decoder.decode(raw)
        try:
            with self._decoder_lock:
                decoder = self._typed_decoders.get(type)
                if decoder is not None:
                    self._typed_decoders.move_to_end(type)
                else:
                    decoder = Decoder(type=type)
                    if len(self._typed_decoders) >= _MAX_TYPED_DECODERS:
                        self._typed_decoders.popitem(last=False)
                    self._typed_decoders[type] = decoder
        except TypeError:
            return Decoder(type=type).decode(raw)
        return decoder.decode(raw)


def convert_datetime_to_gmt_iso(value: datetime.datetime) -> str:
    """Normalize datetime values to ISO 8601 strings."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.UTC)
    return value.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(value: datetime.date) -> str:
    """Normalize date values to ISO 8601 strings."""
    return value.isoformat()


def get_default_json_serializer() -> JSONSerializer:
    """Return the default msgspec-backed JSON serializer."""
    return JSONSerializer.default()


def encode_json_bytes(data: Any) -> bytes:
    """Encode Python data directly into UTF-8 JSON bytes."""
    return get_default_json_serializer().encode_bytes(data)


def encode_json_into(data: Any, buffer: bytearray, offset: int = 0) -> None:
    """Encode Python data directly into a pre-allocated ``bytearray`` at ``offset``."""
    get_default_json_serializer().encode_into(data, buffer, offset)


@overload
def encode_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def encode_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


@overload
def encode_json(data: Any, *, as_bytes: bool = ...) -> str | bytes: ...


def encode_json(data: Any, *, as_bytes: bool = False) -> str | bytes:
    """Encode Python data into JSON."""
    encoded = get_default_json_serializer().encode_bytes(data)
    return encoded if as_bytes else encoded.decode("utf-8")


def decode_json(data: str | bytes, *, decode_bytes: bool = True, type: Any = Any) -> Any:
    """Decode JSON input into Python data, optionally into a typed schema."""
    return get_default_json_serializer().decode(data, decode_bytes=decode_bytes, type=type)


def format_json(value: Any, *, indent: int = 2) -> str:
    """Format Python data into a pretty-printed JSON string."""
    return msgspec.json.format(encode_json_bytes(value), indent=indent).decode("utf-8")


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
    if is_uuid_instance(value):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, enum.Enum):
        return value.value
    if PYDANTIC_INSTALLED and isinstance(value, vertebra_typing.BaseModel):
        return _dump_pydantic_model(value)
    if is_dataclass_instance(value):
        return dataclass_to_dict(value)
    if is_attrs_instance(value):
        return vertebra_typing.attrs_asdict(value, recurse=True)
    if is_msgspec_struct(value):
        return _dump_msgspec_fields(value)
    numpy_value = numpy_array_enc_hook(value)
    if numpy_value is not value:
        return numpy_value

    msg = f"unsupported JSON value: {type(value).__name__}"
    raise TypeError(msg)
