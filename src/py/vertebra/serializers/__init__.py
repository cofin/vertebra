"""Vertebra serializers package.

Provides unified JSON and MessagePack serialization with msgspec-first encoding.
"""

from vertebra.protocols import JSONSerializer
from vertebra.serializers.codecs import (
    JSONCodec,
    MessagePackCodec,
    VertebraCodec,
    decode_json,
    decode_msgpack,
    encode_json,
    encode_msgpack,
    get_json_codec,
    get_msgpack_codec,
)
from vertebra.utils.encoders import default_encoder

__all__ = (
    # JSON
    "JSONCodec",
    "JSONSerializer",
    # MessagePack
    "MessagePackCodec",
    "VertebraCodec",
    "decode_json",
    "decode_msgpack",
    # Encoders
    "default_encoder",
    "encode_json",
    "encode_msgpack",
    "get_json_codec",
    "get_msgpack_codec",
)
