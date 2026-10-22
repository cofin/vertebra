"""Serialization utilities for Vertebra.

Aligned with vtb.
"""

from vertebra.utils.serializers._json import (
    JSONSerializer,
    decode_json,
    encode_json,
    encode_json_bytes,
    encode_json_into,
    format_json,
    get_default_serializer,
)
from vertebra.utils.serializers._msgpack import (
    MsgpackCodec,
    decode_msgpack,
    encode_msgpack,
)

__all__ = (
    "JSONSerializer",
    "MsgpackCodec",
    "decode_json",
    "decode_msgpack",
    "encode_json",
    "encode_json_bytes",
    "encode_json_into",
    "encode_msgpack",
    "format_json",
    "get_default_serializer",
)
