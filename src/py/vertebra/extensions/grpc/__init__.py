"""Native gRPC extension for Vertebra supporting msgspec.Struct RPCs and Protobuf v3 schema generation."""

from __future__ import annotations

from vertebra.extensions.grpc._decorator import (
    GrpcContext,
    GrpcError,
    GrpcMethodKind,
    GrpcMethodSpec,
    GrpcRegistry,
    GrpcServiceRegistry,
    GrpcStatus,
    ProtobufSchemaGenerator,
    _ProtobufSchemaGenerator,
    decode_lpm_frames,
    encode_lpm_frame,
    grpc_method,
    percent_encode_grpc_message,
)

__all__ = [
    "GrpcContext",
    "GrpcError",
    "GrpcMethodKind",
    "GrpcMethodSpec",
    "GrpcRegistry",
    "GrpcServiceRegistry",
    "GrpcStatus",
    "ProtobufSchemaGenerator",
    "_ProtobufSchemaGenerator",
    "decode_lpm_frames",
    "encode_lpm_frame",
    "grpc_method",
    "percent_encode_grpc_message",
]
