"""Serialization utilities for Vertebra."""

from vertebra.utils.serializers._json import (
    JSONSerializer,
    decode_json,
    encode_json,
    encode_json_bytes,
    encode_json_into,
    format_json,
    get_default_json_serializer,
)
from vertebra.utils.serializers._msgpack import (
    MsgPackSerializer,
    decode_msgpack,
    encode_msgpack_bytes,
    encode_msgpack_into,
    get_default_msgpack_serializer,
)
from vertebra.utils.serializers._schema import (
    BaseStruct,
    CamelizedBaseStruct,
    FrozenBaseStruct,
    FrozenCamelizedStruct,
)

__all__ = (
    "BaseStruct",
    "CamelizedBaseStruct",
    "FrozenBaseStruct",
    "FrozenCamelizedStruct",
    "JSONSerializer",
    "MsgPackSerializer",
    "decode_json",
    "decode_msgpack",
    "encode_json",
    "encode_json_bytes",
    "encode_json_into",
    "encode_msgpack_bytes",
    "encode_msgpack_into",
    "format_json",
    "get_default_json_serializer",
    "get_default_msgpack_serializer",
)
