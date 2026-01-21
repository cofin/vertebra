"""Internal helpers for Vertebra runtime.

This module centralizes internal utilities:
- Serialization (JSON and MessagePack via msgspec)
- Error types
- Dependency flags
- Shared memory bindings
"""

from vertebra._internal.dependencies import FSSPEC_INSTALLED, LITESTAR_INSTALLED, SQLSPEC_INSTALLED, WATCHDOG_INSTALLED
from vertebra._internal.errors import MissingDependencyError
from vertebra.serializers import (
    JSONCodec,
    MessagePackCodec,
    decode_json,
    decode_msgpack,
    encode_json,
    encode_msgpack,
    get_json_codec,
    get_msgpack_codec,
)

__all__ = (
    # Dependency flags
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "SQLSPEC_INSTALLED",
    "WATCHDOG_INSTALLED",
    # Serialization
    "JSONCodec",
    "MessagePackCodec",
    # Errors
    "MissingDependencyError",
    "decode_json",
    "decode_msgpack",
    "encode_json",
    "encode_msgpack",
    "get_json_codec",
    "get_msgpack_codec",
)
