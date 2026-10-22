---
type: Concept
title: "gRPC Services & Wire Framing"
description: "Native 5-byte LPM gRPC framing in Rust (vertebra-http) and typed @grpc_method services, ASGI dispatch, and Protobuf v3 schema generation in Python."
status: stable
tags: [domains, extensions, grpc, http2, protobuf]
sources:
  - resource: src/rs/vertebra-http/src/grpc.rs
  - resource: src/py/vertebra/extensions/grpc/__init__.py
  - resource: src/py/vertebra/extensions/grpc/_decorator.py
  - resource: src/py/vertebra/build/codegen/protobuf.py
---

# gRPC Services & Wire Framing

Vertebra pairs native HTTP/2 5-byte Length-Prefixed Message (LPM) framing in [`src/rs/vertebra-http/src/grpc.rs`](../../../../../src/rs/vertebra-http/src/grpc.rs) with `msgspec.Struct`-typed Python handlers and `.proto` v3 schema generation in [`vertebra.extensions.grpc`](../../../../../src/py/vertebra/extensions/grpc/__init__.py) and [`vertebra.build.codegen.protobuf`](../../../../../src/py/vertebra/build/codegen/protobuf.py).

## 1. Rust HTTP/2 gRPC Wire Framing (`vertebra-http/src/grpc.rs`)

| Symbol | Responsibility |
|---|---|
| [`GrpcFrame`](../../../../../src/rs/vertebra-http/src/grpc.rs) / `encode_grpc_frame` / `decode_grpc_frames` | Encodes and decodes 5-byte LPM headers (`1` compression byte + `4` big-endian length bytes, `DEFAULT_MAX_GRPC_MESSAGE_SIZE = 4 MiB`). |
| [`GrpcWireFormat`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Negotiates `application/grpc+msgpack` (`Msgpack`) vs `application/grpc+proto` / `application/grpc` (`Proto`) from `content-type`. |
| [`GrpcStatus`](../../../../../src/rs/vertebra-http/src/grpc.rs) / [`GrpcError`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Canonical status codes `0..=16`, `parse_grpc_timeout` (`H`, `M`, `S`, `m`, `u`, `n`), and RFC 3986 `percent_encode_grpc_message` / `percent_decode_grpc_message`. |
| [`GrpcStreamBody`](../../../../../src/rs/vertebra-http/src/grpc.rs) | Hyper `Body` streaming LPM data frames followed by HTTP/2 `Frame::trailers` when `FLAG_TRAILERS` (`1 << 5`) is set (`build_grpc_response_with_trailers`, `build_grpc_trailers_only_response`). |

## 2. Python `@grpc_method`, Registry & Protobuf v3 Codegen (`extensions/grpc/_decorator.py`, `build/codegen/protobuf.py`)

- **[`@grpc_method(service=..., package="vertebra", name=...)`](../../../../../src/py/vertebra/extensions/grpc/_decorator.py)**: Introspects type annotations on async handlers to build a frozen [`GrpcMethodSpec`](../../../../../src/py/vertebra/extensions/grpc/_decorator.py) supporting all four [`GrpcMethodKind`](../../../../../src/py/vertebra/extensions/grpc/_decorator.py) cardinalities (`UNARY`, `SERVER_STREAMING`, `CLIENT_STREAMING`, `BIDI_STREAMING`) over `msgspec.Struct` and `AsyncIterator[Struct]` types.
- **[`GrpcServiceRegistry`](../../../../../src/py/vertebra/extensions/grpc/_decorator.py)**: Registers `@grpc_method` callables by `/<package>.<service>/<Method>` path and dispatches ASGI HTTP/2 requests via `handle_asgi`, decoding LPM payloads with `msgspec.msgpack` and emitting `grpc-status` and percent-encoded `grpc-message` trailers (mapping `GrpcError` and `msgspec.DecodeError`).
- **[`ProtobufSchemaGenerator`](../../../../../src/py/vertebra/extensions/grpc/_decorator.py)** (re-exported by [`vertebra.build.codegen.protobuf`](../../../../../src/py/vertebra/build/codegen/protobuf.py)): Traverses `msgspec.inspect.type_info` to render deterministic `syntax = "proto3";` message definitions (`repeated`, `map<K, V>`, `optional`, nested structs) and `service` RPC blocks.
