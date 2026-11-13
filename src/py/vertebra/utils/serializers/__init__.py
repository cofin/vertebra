"""Serialization utilities for Vertebra."""

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports
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

if TYPE_CHECKING:
    from vertebra.utils.serializers._zero_copy import (
        GRPC_LPM_HEADER_LEN,
        TypeAdapterKind,
        TypeAdapterPlan,
        ZeroCopyCodecRegistry,
        decode_grpc_frame,
        decode_grpc_frames,
        encode_grpc_frame_bytes,
        encode_grpc_frame_into,
        encode_json_into_arena,
        encode_msgpack_into_arena,
    )

__all__ = (
    "GRPC_LPM_HEADER_LEN",
    "BaseStruct",
    "CamelizedBaseStruct",
    "FrozenBaseStruct",
    "FrozenCamelizedStruct",
    "JSONSerializer",
    "MsgPackSerializer",
    "TypeAdapterKind",
    "TypeAdapterPlan",
    "ZeroCopyCodecRegistry",
    "decode_grpc_frame",
    "decode_grpc_frames",
    "decode_json",
    "decode_msgpack",
    "encode_grpc_frame_bytes",
    "encode_grpc_frame_into",
    "encode_json",
    "encode_json_bytes",
    "encode_json_into",
    "encode_json_into_arena",
    "encode_msgpack_bytes",
    "encode_msgpack_into",
    "encode_msgpack_into_arena",
    "format_json",
    "get_default_json_serializer",
    "get_default_msgpack_serializer",
)

_ZERO_COPY_MODULE = "vertebra.utils.serializers._zero_copy"
__getattr__, __dir__ = lazy_exports(
    __name__,
    __all__,
    {
        "GRPC_LPM_HEADER_LEN": (_ZERO_COPY_MODULE, "GRPC_LPM_HEADER_LEN"),
        "TypeAdapterKind": (_ZERO_COPY_MODULE, "TypeAdapterKind"),
        "TypeAdapterPlan": (_ZERO_COPY_MODULE, "TypeAdapterPlan"),
        "ZeroCopyCodecRegistry": (_ZERO_COPY_MODULE, "ZeroCopyCodecRegistry"),
        "decode_grpc_frame": (_ZERO_COPY_MODULE, "decode_grpc_frame"),
        "decode_grpc_frames": (_ZERO_COPY_MODULE, "decode_grpc_frames"),
        "encode_grpc_frame_bytes": (_ZERO_COPY_MODULE, "encode_grpc_frame_bytes"),
        "encode_grpc_frame_into": (_ZERO_COPY_MODULE, "encode_grpc_frame_into"),
        "encode_json_into_arena": (_ZERO_COPY_MODULE, "encode_json_into_arena"),
        "encode_msgpack_into_arena": (_ZERO_COPY_MODULE, "encode_msgpack_into_arena"),
    },
)
