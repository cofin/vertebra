"""Backwards-compatible re-export from vertebra.common.serializers.

This module is deprecated. Import from vertebra.common.serializers instead.
"""

from vertebra.common.serializers import (
    JSONCodec,
    JSONSerializer,
    MessagePackCodec,
    VertebraCodec,
    decode_json,
    decode_msgpack,
    default_encoder,
    encode_json,
    encode_msgpack,
    get_json_codec,
    get_msgpack_codec,
)

__all__ = (
    "JSONCodec",
    "JSONSerializer",
    "MessagePackCodec",
    "VertebraCodec",
    "decode_json",
    "decode_msgpack",
    "default_encoder",
    "encode_json",
    "encode_msgpack",
    "get_json_codec",
    "get_msgpack_codec",
)
