"""Vertebra serializers package.

Provides unified JSON and MessagePack serialization with msgspec-first encoding.
"""

from vertebra.serializers.encoders import default_encoder
from vertebra.serializers.json import (
    JSONCodec,
    decode_json,
    encode_json,
    get_json_codec,
)
from vertebra.serializers.msgpack import (
    MessagePackCodec,
    VertebraCodec,
    decode_msgpack,
    encode_msgpack,
    get_msgpack_codec,
)
from vertebra.serializers.types import JSONSerializer

__all__ = (
    # JSON
    "JSONCodec",
    "JSONSerializer",
    "decode_json",
    "encode_json",
    "get_json_codec",
    # MessagePack
    "MessagePackCodec",
    "VertebraCodec",
    "decode_msgpack",
    "encode_msgpack",
    "get_msgpack_codec",
    # Encoders
    "default_encoder",
)