"""JSON and MessagePack serialization codecs.

This module provides unified codec implementations using msgspec as the
backend for both JSON and MessagePack serialization.
"""

import functools
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar, overload

import msgspec

from vertebra.serializers.decoders import default_decoder
from vertebra.serializers.encoders import default_encoder

__all__ = (
    # JSON
    "JSONCodec",
    # MessagePack
    "MessagePackCodec",
    "VertebraCodec",
    "decode_json",
    "decode_msgpack",
    "encode_json",
    "encode_msgpack",
    "get_json_codec",
    "get_msgpack_codec",
)

T = TypeVar("T")


# ============================================================================
# JSON Codec
# ============================================================================


@dataclass(slots=True)
class JSONCodec:
    """Reusable JSON encoder/decoder using msgspec."""

    encoder: msgspec.json.Encoder = field(default_factory=lambda: msgspec.json.Encoder(enc_hook=default_encoder))
    _default_decoder: msgspec.json.Decoder[Any] = field(default_factory=msgspec.json.Decoder)
    _decoder_cache: dict[type, msgspec.json.Decoder[Any]] = field(default_factory=dict)

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

    @overload
    def decode(self, data: str | bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...

    @overload
    def decode(self, data: str | bytes | bytearray | memoryview, type_hint: None = ...) -> Any: ...

    def decode(self, data: str | bytes | bytearray | memoryview, type_hint: type[Any] | None = None) -> Any:
        """Decode from JSON.

        Args:
            data: JSON string, bytes, or memoryview to decode.
            type_hint: Optional type for structured decoding.

        Returns:
            Decoded Python object.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")

        if type_hint is None:
            return self._default_decoder.decode(data)

        decoder = self._decoder_cache.get(type_hint)
        if decoder is None:
            decoder = msgspec.json.Decoder(type_hint, dec_hook=default_decoder)
            self._decoder_cache[type_hint] = decoder
        return decoder.decode(data)


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


@overload
def decode_json[T](data: str | bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...


@overload
def decode_json(data: str | bytes | bytearray | memoryview, type_hint: None = ...) -> Any: ...


def decode_json(data: str | bytes | bytearray | memoryview, type_hint: type[Any] | None = None) -> Any:
    """Decode from JSON using msgspec.

    Args:
        data: JSON string, bytes, or memoryview to decode.
        type_hint: Optional type for structured decoding.

    Returns:
        Decoded Python object.
    """
    return get_json_codec().decode(data, type_hint)


# ============================================================================
# MessagePack Codec
# ============================================================================


@dataclass(slots=True)
class MessagePackCodec:
    """Reusable MessagePack encoder/decoder with optional typed decoding."""

    encoder: msgspec.msgpack.Encoder = field(default_factory=lambda: msgspec.msgpack.Encoder(enc_hook=default_encoder))
    _decoder_cache: dict[type, msgspec.msgpack.Decoder[Any]] = field(default_factory=dict)

    def encode(self, value: Any) -> bytes:
        """Encode a Python value to MessagePack bytes."""
        return self.encoder.encode(value)

    @overload
    def decode(self, data: bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...

    @overload
    def decode(self, data: bytes | bytearray | memoryview, type_hint: None = ...) -> Any: ...

    def decode(self, data: bytes | bytearray | memoryview, type_hint: type[Any] | None = None) -> Any:
        """Decode MessagePack bytes to Python values.

        Args:
            data: MessagePack payload.
            type_hint: Optional msgspec-compatible type for structured decoding.
        """
        if type_hint is None:
            return msgspec.msgpack.decode(data)
        decoder = self._decoder_cache.get(type_hint)
        if decoder is None:
            decoder = msgspec.msgpack.Decoder(type_hint, dec_hook=default_decoder)
            self._decoder_cache[type_hint] = decoder
        return decoder.decode(data)


# Alias for consistency with new API specs
VertebraCodec = MessagePackCodec


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


@overload
def decode_msgpack[T](data: bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...


@overload
def decode_msgpack(data: bytes | bytearray | memoryview, type_hint: None = ...) -> Any: ...


def decode_msgpack(data: bytes | bytearray | memoryview, type_hint: type[Any] | None = None) -> Any:
    """Decode from MessagePack using msgspec.

    Args:
        data: MessagePack bytes to decode.
        type_hint: Optional type for structured decoding.

    Returns:
        Decoded Python object.
    """
    return get_msgpack_codec().decode(data, type_hint)
