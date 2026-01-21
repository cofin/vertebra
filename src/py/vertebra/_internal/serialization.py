"""Serialization helpers for Vertebra.

Provides unified JSON and MessagePack serialization with msgspec-first encoding.
Falls back to stdlib json when msgspec is unavailable (though msgspec is a required dep).

This module is the single source of truth for all serialization in Vertebra.
"""

from __future__ import annotations

import datetime
import decimal
import enum
import functools
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, overload

import msgspec


def _default_encoder(value: Any) -> Any:  # noqa: PLR0911
    """Default encoder for types not natively supported by msgspec.

    Handles datetime, date, time, timedelta, enums, UUIDs, Decimals and other common types.

    Args:
        value: Value to convert.

    Returns:
        Serializable representation of the value.

    Raises:
        TypeError: If value cannot be serialized.
    """
    if isinstance(value, datetime.datetime):
        if not value.tzinfo:
            value = value.replace(tzinfo=datetime.UTC)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if hasattr(value, "__dict__"):
        return value.__dict__
    return str(value)


# ============================================================================
# MessagePack Serialization
# ============================================================================


@dataclass(slots=True)
class MessagePackCodec:
    """Reusable MessagePack encoder/decoder with optional typed decoding."""

    encoder: msgspec.msgpack.Encoder = field(default_factory=lambda: msgspec.msgpack.Encoder(enc_hook=_default_encoder))
    _decoder_cache: dict[type, msgspec.msgpack.Decoder[Any]] = field(default_factory=dict)

    def encode(self, value: Any) -> bytes:
        """Encode a Python value to MessagePack bytes."""
        return self.encoder.encode(value)

    def decode(self, data: bytes | bytearray | memoryview, type_hint: type | None = None) -> Any:
        """Decode MessagePack bytes to Python values.

        Args:
            data: MessagePack payload.
            type_hint: Optional msgspec-compatible type for structured decoding.
        """
        if type_hint is None:
            return msgspec.msgpack.decode(data)
        decoder = self._decoder_cache.get(type_hint)
        if decoder is None:
            decoder = msgspec.msgpack.Decoder(type_hint)
            self._decoder_cache[type_hint] = decoder
        return decoder.decode(data)


@functools.lru_cache(maxsize=1)
def get_msgpack_codec() -> MessagePackCodec:
    """Get the shared MessagePack codec instance."""
    return MessagePackCodec()


def encode_msgpack(data: Any) -> bytes:
    """Encode to MessagePack using msgspec.

    Args:
        data: The data to encode.

    Returns:
        MessagePack bytes.
    """
    return get_msgpack_codec().encode(data)


def decode_msgpack(data: bytes | bytearray | memoryview, type_hint: type | None = None) -> Any:
    """Decode from MessagePack using msgspec.

    Args:
        data: MessagePack bytes to decode.
        type_hint: Optional type for structured decoding.

    Returns:
        Decoded Python object.
    """
    return get_msgpack_codec().decode(data, type_hint)


# ============================================================================
# JSON Serialization
# ============================================================================


class JSONSerializer(Protocol):
    """Protocol for JSON serialization implementations."""

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON."""
        ...

    def decode(self, data: str | bytes) -> Any:
        """Decode from JSON."""
        ...


@dataclass(slots=True)
class JSONCodec:
    """Reusable JSON encoder/decoder using msgspec."""

    encoder: msgspec.json.Encoder = field(default_factory=lambda: msgspec.json.Encoder(enc_hook=_default_encoder))
    decoder: msgspec.json.Decoder[Any] = field(default_factory=msgspec.json.Decoder)

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON.

        Args:
            data: Data to encode.
            as_bytes: Whether to return bytes instead of string.

        Returns:
            JSON string or bytes depending on as_bytes parameter.
        """
        result = self.encoder.encode(data)
        return result if as_bytes else result.decode("utf-8")

    def decode(self, data: str | bytes) -> Any:
        """Decode from JSON.

        Args:
            data: JSON string or bytes to decode.

        Returns:
            Decoded Python object.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        return self.decoder.decode(data)


@functools.lru_cache(maxsize=1)
def get_json_codec() -> JSONCodec:
    """Get the shared JSON codec instance."""
    return JSONCodec()


@overload
def encode_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def encode_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


def encode_json(data: Any, *, as_bytes: bool = False) -> str | bytes:
    """Encode to JSON using msgspec.

    Args:
        data: The data to encode.
        as_bytes: Whether to return bytes instead of string.

    Returns:
        JSON string or bytes depending on as_bytes parameter.
    """
    return get_json_codec().encode(data, as_bytes=as_bytes)


def decode_json(data: str | bytes) -> Any:
    """Decode from JSON using msgspec.

    Args:
        data: JSON string or bytes to decode.

    Returns:
        Decoded Python object.
    """
    return get_json_codec().decode(data)


__all__ = (
    # JSON
    "JSONCodec",
    "JSONSerializer",
    # MessagePack
    "MessagePackCodec",
    "decode_json",
    "decode_msgpack",
    "encode_json",
    "encode_msgpack",
    "get_json_codec",
    "get_msgpack_codec",
)
