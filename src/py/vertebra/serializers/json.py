"""JSON serialization utilities."""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar, overload

import msgspec

from vertebra.serializers.encoders import default_encoder

__all__ = (
    "JSONCodec",
    "decode_json",
    "encode_json",
    "get_json_codec",
)

T = TypeVar("T")


@dataclass(slots=True)
class JSONCodec:
    """Reusable JSON encoder/decoder using msgspec."""

    encoder: msgspec.json.Encoder = field(
        default_factory=lambda: msgspec.json.Encoder(enc_hook=default_encoder)
    )
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
            decoder = msgspec.json.Decoder(type_hint)
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
def decode_json(data: str | bytes | bytearray | memoryview, type_hint: type[T]) -> T: ...


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