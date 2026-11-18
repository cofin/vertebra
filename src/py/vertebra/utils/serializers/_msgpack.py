"""MessagePack serialization engine for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

from __future__ import annotations

import datetime
import enum
import os
import pathlib
import threading
import uuid as _uuid_mod
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Final

from msgspec.msgpack import Decoder, Encoder

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
    "MsgPackSerializer",
    "decode_msgpack",
    "encode_msgpack_bytes",
    "encode_msgpack_into",
    "get_default_msgpack_serializer",
)

_DEFAULT_LOCK: Final[threading.Lock] = threading.Lock()
_MAX_TYPED_DECODERS: Final[int] = 1024


class MsgPackSerializer:
    """Msgspec-based MessagePack serializer supporting custom hooks and typed decoding."""

    __slots__ = ("_decoder", "_decoder_lock", "_encoder", "_typed_decoders")

    _default: ClassVar[MsgPackSerializer | None] = None

    def __init__(self) -> None:
        self._encoder: Final[Encoder] = Encoder(enc_hook=_normalize_msgpack_enc_value)
        self._decoder: Final[Decoder[Any]] = Decoder(dec_hook=_normalize_msgpack_dec_value)
        self._decoder_lock: Final[threading.Lock] = threading.Lock()
        self._typed_decoders: OrderedDict[Any, Decoder[Any]] = OrderedDict()

    @classmethod
    def default(cls) -> MsgPackSerializer:
        """Return the process-global default MsgPackSerializer instance."""
        if cls._default is None:
            with _DEFAULT_LOCK:
                if cls._default is None:
                    cls._default = cls()
        return cls._default

    def encode(self, value: Any) -> bytes:
        """Encode Python data into MessagePack bytes."""
        return self.encode_bytes(value)

    def encode_bytes(self, value: Any) -> bytes:
        """Encode Python data directly into MessagePack bytes."""
        return self._encoder.encode(value)

    def encode_into(self, value: Any, buffer: bytearray, offset: int = 0) -> None:
        """Encode Python data directly into a pre-allocated ``bytearray`` at ``offset``."""
        self._encoder.encode_into(value, buffer, offset)

    def decode(self, raw: bytes | bytearray | memoryview, *, type: Any = Any) -> Any:
        """Decode MessagePack bytes into Python data, optionally into a typed schema."""
        if type is Any or type is object:
            return self._decoder.decode(raw)
        try:
            with self._decoder_lock:
                decoder = self._typed_decoders.get(type)
                if decoder is not None:
                    self._typed_decoders.move_to_end(type)
                else:
                    decoder = Decoder(type=type, dec_hook=_normalize_msgpack_dec_value)
                    if len(self._typed_decoders) >= _MAX_TYPED_DECODERS:
                        self._typed_decoders.popitem(last=False)
                    self._typed_decoders[type] = decoder
        except TypeError:
            decoder = Decoder(type=type, dec_hook=_normalize_msgpack_dec_value)
            return decoder.decode(raw)
        return decoder.decode(raw)


def get_default_msgpack_serializer() -> MsgPackSerializer:
    """Return the default msgspec-backed MessagePack serializer."""
    return MsgPackSerializer.default()


def encode_msgpack_bytes(value: Any) -> bytes:
    """Encode Python data directly into MessagePack bytes."""
    return get_default_msgpack_serializer().encode_bytes(value)


def encode_msgpack_into(value: Any, buffer: bytearray, offset: int = 0) -> None:
    """Encode Python data directly into a pre-allocated ``bytearray`` at ``offset``."""
    get_default_msgpack_serializer().encode_into(value, buffer, offset)


def decode_msgpack(raw: bytes | bytearray | memoryview, *, type: Any = Any) -> Any:
    """Decode MessagePack bytes into Python data, optionally into a typed schema."""
    return get_default_msgpack_serializer().decode(raw, type=type)


def _dump_pydantic_model(value: Any) -> Any:
    """Dump a Pydantic model instance to a dictionary."""
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return value.dict()


def _normalize_msgpack_enc_value(value: Any) -> Any:
    """Convert non-native values into MessagePack-compatible types."""
    if isinstance(value, (os.PathLike, Path)):
        return str(value)
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

    msg = f"unsupported MessagePack value: {type(value).__name__}"
    raise TypeError(msg)


def _normalize_msgpack_dec_value(target_type: type[Any], value: Any) -> Any:
    """Decode non-native MessagePack values into target types."""
    if isinstance(target_type, type):
        if issubclass(target_type, (Path, pathlib.PurePath)):
            return target_type(value)
        if issubclass(target_type, _uuid_mod.UUID) and isinstance(value, str):
            return target_type(value)
        if issubclass(target_type, datetime.datetime) and isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        if issubclass(target_type, datetime.date) and isinstance(value, str):
            return datetime.date.fromisoformat(value)
    msg = f"unsupported MessagePack decode target: {target_type!r}"
    raise TypeError(msg)
