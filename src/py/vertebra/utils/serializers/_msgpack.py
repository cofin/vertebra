"""MessagePack serialization engine for ``vertebra.utils.serializers``.

Aligned with vtb.
"""

from __future__ import annotations

import datetime
import enum
import os
import pathlib
import uuid as _uuid_mod
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Final, cast

from msgspec.msgpack import Decoder, Encoder

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

__all__ = ("MsgpackCodec", "decode_msgpack", "encode_msgpack")

_UNLOADED_UUID_TYPE: Final[object] = object()


class _MsgpackUuidTypeCache:
    """Class-scoped holder for lazy UUID backend."""

    uuid_utils_type: ClassVar[Any] = _UNLOADED_UUID_TYPE


def _get_msgpack_uuid_utils_type() -> type[Any] | None:
    """Lazy-resolve and cache the uuid-utils UUID type when installed."""
    if _MsgpackUuidTypeCache.uuid_utils_type is not _UNLOADED_UUID_TYPE:
        return cast("type[Any] | None", _MsgpackUuidTypeCache.uuid_utils_type)
    if not UUID_UTILS_INSTALLED:
        _MsgpackUuidTypeCache.uuid_utils_type = None
        return None
    module = _load_uuid_utils()
    _MsgpackUuidTypeCache.uuid_utils_type = None if module is None else module.UUID
    return cast("type[Any] | None", _MsgpackUuidTypeCache.uuid_utils_type)


def _is_uuid_instance(value: Any) -> bool:
    """Return True when value is an instance of stdlib UUID or uuid_utils.UUID."""
    if isinstance(value, _uuid_mod.UUID):
        return True
    uuid_type = _get_msgpack_uuid_utils_type()
    return uuid_type is not None and isinstance(value, uuid_type)


def _dump_pydantic_model(value: Any) -> Any:
    """Dump a Pydantic model instance to a dictionary."""
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return value.dict()


def _normalize_msgpack_enc_value(value: Any) -> Any:
    """Convert non-native values into MessagePack-compatible types."""
    if isinstance(value, (os.PathLike, Path)):
        return str(value)
    if _is_uuid_instance(value):
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


class MsgpackCodec:
    """Msgspec-based MessagePack codec supporting custom hooks and typed decoding."""

    __slots__ = ("_decoder", "_encoder", "_typed_decoders")

    _default: ClassVar[MsgpackCodec | None] = None

    def __init__(self) -> None:
        self._encoder: Final[Encoder] = Encoder(enc_hook=_normalize_msgpack_enc_value)
        self._decoder: Final[Decoder[Any]] = Decoder(dec_hook=_normalize_msgpack_dec_value)
        self._typed_decoders: dict[Any, Decoder[Any]] = {}

    @classmethod
    def default(cls) -> MsgpackCodec:
        """Return the process-global default MsgpackCodec instance."""
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def encode(self, value: Any) -> bytes:
        """Encode Python data into MessagePack bytes."""
        return self._encoder.encode(value)

    def decode(self, raw: bytes | bytearray | memoryview, *, type: Any = Any) -> Any:
        """Decode MessagePack bytes into Python data, optionally into a typed schema."""
        if type is Any or type is object:
            return self._decoder.decode(raw)
        try:
            decoder = self._typed_decoders.get(type)
            if decoder is None:
                decoder = Decoder(type=type, dec_hook=_normalize_msgpack_dec_value)
                self._typed_decoders[type] = decoder
            return decoder.decode(raw)
        except TypeError:
            decoder = Decoder(type=type, dec_hook=_normalize_msgpack_dec_value)
            return decoder.decode(raw)


def encode_msgpack(value: Any) -> bytes:
    """Encode Python data into MessagePack bytes."""
    return MsgpackCodec.default().encode(value)


def decode_msgpack(raw: bytes | bytearray | memoryview, *, type: Any = Any) -> Any:
    """Decode MessagePack bytes into Python data, optionally into a typed schema."""
    return MsgpackCodec.default().decode(raw, type=type)
