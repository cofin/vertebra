---
type: Concept
title: "gRPC Services & Wire Framing"
description: "Native 5-byte LPM gRPC framing in Rust (vertebra-http) and typed @grpc_method services, ASGI dispatch, and Protobuf v3 schema generation in Python."
status: stable
tags:
  - domains
  - extensions
  - grpc
  - http2
  - protobuf
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: 9af810f5
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
  - id: src-vertebra-http-src-grpc-rs
    resource: src/rs/vertebra-http/src/grpc.rs
    title: "Rust implementation module (src/rs/vertebra-http/src/grpc.rs)"
  - id: src-extensions-grpc-init-py
    resource: src/py/vertebra/extensions/grpc/__init__.py
    title: "Python implementation module (src/py/vertebra/extensions/grpc/__init__.py)"
  - id: src-build-codegen-protobuf-py
    resource: src/py/vertebra/build/codegen/protobuf.py
    title: "Python implementation module (src/py/vertebra/build/codegen/protobuf.py)"
---

# gRPC Services & Wire Framing

<!-- truth: start -->
- `vertebra-http/src/grpc.rs` frames HTTP/2 5-byte Length-Prefixed Messages (`GrpcFrame`, `GrpcStreamBody`, `GrpcStatus`) across Protobuf and `+msgpack` wire formats.
- `GrpcServiceRegistry`, `@grpc_method`, `ProtobufSchemaGenerator`, `ProtobufGenerator`, and `ProtobufParser` expose typed unary and streaming gRPC handlers paired with bidirectional `.proto` route codegen.
<!-- truth: end -->

Vertebra pairs native HTTP/2 5-byte Length-Prefixed Message (LPM) framing in [`src/rs/vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs)[^src-vertebra-http-src-grpc-rs] with `msgspec.Struct`-typed Python handlers and `.proto` v3 schema generation in [`vertebra.extensions.grpc`](../../../../../src/py/vertebra/extensions/grpc/__init__.py)[^src-extensions-grpc-init-py] and [`vertebra.build.codegen.protobuf`](../../../../../src/py/vertebra/build/codegen/protobuf.py)[^src-build-codegen-protobuf-py].

## 1. Rust HTTP/2 gRPC Wire Framing (`vertebra-http/src/grpc.rs`)

| Symbol | Responsibility |
|---|---|
| [`GrpcFrame`](../../../../../src/rs/vertebra-http/src/grpc.rs) / `encode_grpc_frame` / `decode_grpc_frames` | Encodes and decodes 5-byte LPM headers (`1` compression byte + `4` big-endian length bytes, `DEFAULT_MAX_GRPC_MESSAGE_SIZE = 4 MiB`). |
| [`GrpcWireFormat`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Negotiates `application/grpc+msgpack` (`Msgpack`) vs `application/grpc+proto` / `application/grpc` (`Proto`) from `content-type`. |
| [`GrpcStatus`](../../../../../src/rs/vertebra-http/src/grpc.rs) / [`GrpcError`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Canonical status codes `0..=16`, `parse_grpc_timeout` (`H`, `M`, `S`, `m`, `u`, `n`), and RFC 3986 `percent_encode_grpc_message` / `percent_decode_grpc_message`. |
| [`GrpcStreamBody`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Hyper `Body` streaming LPM data frames followed by HTTP/2 `Frame::trailers` when `FLAG_TRAILERS` (`1 << 5`) is set (`build_grpc_response_with_trailers`, `build_grpc_trailers_only_response`). |

## 2. Python `@grpc_method`, Registry & Protobuf v3 Codegen (`extensions/grpc/__init__.py`, `build/codegen/protobuf.py`)

- **[`@grpc_method(service=..., package="vertebra", name=...)`](../../../../../src/py/vertebra/extensions/grpc/__init__.py)**: Introspects type annotations on async handlers to build a frozen [`GrpcMethodSpec`](../../../../../src/py/vertebra/extensions/grpc/__init__.py) supporting all four [`GrpcMethodKind`](../../../../../src/py/vertebra/extensions/grpc/__init__.py) cardinalities (`UNARY`, `SERVER_STREAMING`, `CLIENT_STREAMING`, `BIDI_STREAMING`) over `msgspec.Struct` and `AsyncIterator[Struct]` types.
- **[`GrpcServiceRegistry`](../../../../../src/py/vertebra/extensions/grpc/__init__.py)**: Registers `@grpc_method` callables by `/<package>.<service>/<Method>` path and dispatches ASGI HTTP/2 requests (`handle_asgi`) using `decode_lpm_frames`, `encode_lpm_frame`, `decode_msgpack`, and `encode_msgpack_bytes`, emitting `grpc-status` and percent-encoded `grpc-message` (`percent_encode_grpc_message`) trailers.
- **[`ProtobufSchemaGenerator`](../../../../../src/py/vertebra/extensions/grpc/__init__.py), [`ProtobufGenerator`](../../../../../src/py/vertebra/build/codegen/protobuf.py), & [`ProtobufParser`](../../../../../src/py/vertebra/build/codegen/protobuf.py)**: Renders `.proto` v3 schemas from registered `GrpcServiceRegistry` definitions (`generate_from_registry`) and supports bidirectional `.proto` v3 route and client codegen.

[^src-vertebra-http-src-grpc-rs]: `src/rs/vertebra-http/src/grpc.rs` — Rust implementation module (src/rs/vertebra-http/src/grpc.rs).
[^src-extensions-grpc-init-py]: `src/py/vertebra/extensions/grpc/__init__.py` — Python implementation module (src/py/vertebra/extensions/grpc/__init__.py).
[^src-build-codegen-protobuf-py]: `src/py/vertebra/build/codegen/protobuf.py` — Python implementation module (src/py/vertebra/build/codegen/protobuf.py).
