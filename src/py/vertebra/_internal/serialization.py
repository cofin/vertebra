"""MessagePack serialization helpers for Bun-backed drivers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import msgspec


@dataclass(slots=True)
class MessagePackCodec:
    """Reusable MessagePack encoder/decoder with optional typed decoding."""

    encoder: msgspec.msgpack.Encoder = field(
        default_factory=msgspec.msgpack.Encoder
    )
    _decoder_cache: dict[type, msgspec.msgpack.Decoder] = field(
        default_factory=dict
    )

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


__all__ = ["MessagePackCodec"]
