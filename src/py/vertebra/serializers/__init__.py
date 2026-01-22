"""Vertebra serialization codecs.

This package provides unified JSON and MessagePack serialization using msgspec:

JSON Codecs:
    JSONCodec - Reusable JSON encoder/decoder with type-aware decoding
    JSONSerializer - Protocol for JSON implementations
    encode_json - Encode data to JSON string or bytes
    decode_json - Decode JSON data with optional type hint
    get_json_codec - Get shared JSON codec instance

MessagePack Codecs:
    MessagePackCodec - Reusable MessagePack encoder/decoder
    VertebraCodec - Alias for MessagePackCodec (data plane default)
    encode_msgpack - Encode data to MessagePack bytes
    decode_msgpack - Decode MessagePack data with optional type hint
    get_msgpack_codec - Get shared MessagePack codec instance

Encoding Utilities:
    default_encoder - Encoder for types not natively supported by msgspec
        (datetime, date, time, timedelta, enums, UUIDs, Decimals)
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
from vertebra.serializers.encoders import default_encoder

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
