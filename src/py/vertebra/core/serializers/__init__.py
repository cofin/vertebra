"""Serialization codecs for Vertebra."""

from __future__ import annotations

from typing import Any

import msgspec


def decode_json[T](data: bytes, *, type_hint: type[T] = dict) -> T:  # type: ignore[assignment]
    """Decode JSON bytes into a Python object."""
    return msgspec.json.decode(data, type=type_hint)


def encode_json(obj: Any) -> bytes:
    """Encode a Python object to JSON bytes."""
    return msgspec.json.encode(obj)


__all__ = ("decode_json", "encode_json")
