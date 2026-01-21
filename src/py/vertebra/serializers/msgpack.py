"""MessagePack serialization utilities."""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import Any, TypeVar, overload

import msgspec

from vertebra.serializers.encoders import default_encoder

__all__ = (
    "MessagePackCodec",
    "VertebraCodec",
    "decode_msgpack",
    "encode_msgpack",
    "get_msgpack_codec",
)

T = TypeVar("T")


@dataclass(slots=True)
class MessagePackCodec:
    """Reusable MessagePack encoder/decoder with optional typed decoding."""

    encoder: msgspec.msgpack.Encoder = field(
        default_factory=lambda: msgspec.msgpack.Encoder(enc_hook=default_encoder)
    )
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
            decoder = msgspec.msgpack.Decoder(type_hint)
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
def decode_msgpack(data: bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...


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
