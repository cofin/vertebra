"""Unit tests for vertebra.extensions.grpc @grpc_method, GrpcServiceRegistry, and _ProtobufSchemaGenerator."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import msgspec
import pytest

from vertebra.extensions.grpc import (
    GrpcContext,
    GrpcError,
    GrpcMethodKind,
    GrpcRegistry,
    GrpcServiceRegistry,
    GrpcStatus,
    ProtobufSchemaGenerator,
    decode_lpm_frames,
    encode_lpm_frame,
    grpc_method,
)
from vertebra.extensions.grpc._decorator import _ProtobufSchemaGenerator


class EchoRequest(msgspec.Struct):
    """Input message for unary and streaming echo RPCs."""

    message: str
    count: int = 1
    tags: list[str] = []
    correlation_id: str | None = None


class EchoResponse(msgspec.Struct):
    """Output message for unary and streaming echo RPCs."""

    reply: str
    sequence: int
    metadata: dict[str, str] = {}


@grpc_method(service="EchoService", package="vertebra.echo")
async def echo_unary(req: EchoRequest) -> EchoResponse:
    """Handle a unary Echo RPC."""
    if req.count < 0:
        raise GrpcError(GrpcStatus.INVALID_ARGUMENT, "count must be non-negative")
    return EchoResponse(reply=f"ack:{req.message}", sequence=req.count)


@grpc_method(service="EchoService", package="vertebra.echo")
async def echo_server_stream(req: EchoRequest) -> AsyncIterator[EchoResponse]:
    """Handle a server-streaming Echo RPC."""
    for idx in range(req.count):
        yield EchoResponse(reply=f"{req.message}-{idx}", sequence=idx)


@grpc_method(service="EchoService", package="vertebra.echo")
async def echo_client_stream(stream: AsyncIterator[EchoRequest]) -> EchoResponse:
    """Handle a client-streaming Echo RPC."""
    parts: list[str] = []
    total = 0
    async for item in stream:
        parts.append(item.message)
        total += item.count
    return EchoResponse(reply="|".join(parts), sequence=total)


@grpc_method(service="EchoService", package="vertebra.echo")
async def echo_bidi_stream(stream: AsyncIterator[EchoRequest]) -> AsyncIterator[EchoResponse]:
    """Handle a bidirectional-streaming Echo RPC."""
    seq = 0
    async for item in stream:
        seq += 1
        yield EchoResponse(reply=item.message.upper(), sequence=seq)


def test_grpc_method_introspects_all_four_streaming_kinds() -> None:
    """Verify @grpc_method classifies unary, server-streaming, client-streaming, and bidi-streaming signatures."""
    assert echo_unary.__grpc_method__.kind is GrpcMethodKind.UNARY
    assert echo_unary.__grpc_method__.full_path == "/vertebra.echo.EchoService/EchoUnary"
    assert echo_server_stream.__grpc_method__.kind is GrpcMethodKind.SERVER_STREAMING
    assert echo_client_stream.__grpc_method__.kind is GrpcMethodKind.CLIENT_STREAMING
    assert echo_bidi_stream.__grpc_method__.kind is GrpcMethodKind.BIDI_STREAMING
    ctx = GrpcContext(peer="127.0.0.1:50051", metadata={"x-request-id": "req-1"})
    assert ctx.peer == "127.0.0.1:50051"
    assert GrpcRegistry is GrpcServiceRegistry


@pytest.mark.anyio
async def test_grpc_service_registry_unary_and_streaming_asgi_dispatch() -> None:
    """Verify GrpcServiceRegistry dispatches unary, server-stream, client-stream, and bidi-stream over ASGI."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)
    registry.register(echo_server_stream)
    registry.register(echo_client_stream)
    registry.register(echo_bidi_stream)

    unary_payload = encode_lpm_frame(msgspec.msgpack.encode(EchoRequest(message="ping", count=7)))
    unary_events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoUnary",
        body=unary_payload,
    )
    assert unary_events[0]["type"] == "http.response.start"
    assert unary_events[0]["status"] == 200
    body_frames = decode_lpm_frames(
        b"".join(e.get("body", b"") for e in unary_events if e["type"] == "http.response.body")
    )
    assert len(body_frames) == 1
    decoded_unary = msgspec.msgpack.decode(body_frames[0], type=EchoResponse)
    assert decoded_unary.reply == "ack:ping"
    assert decoded_unary.sequence == 7
    trailer_event = unary_events[-1]
    assert trailer_event["type"] == "http.response.trailers"
    assert (b"grpc-status", b"0") in trailer_event["headers"]

    stream_payload = encode_lpm_frame(msgspec.msgpack.encode(EchoRequest(message="item", count=3)))
    stream_events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoServerStream",
        body=stream_payload,
    )
    stream_frames = decode_lpm_frames(
        b"".join(e.get("body", b"") for e in stream_events if e["type"] == "http.response.body")
    )
    assert [msgspec.msgpack.decode(f, type=EchoResponse).reply for f in stream_frames] == [
        "item-0",
        "item-1",
        "item-2",
    ]

    client_stream_payload = b"".join(
        [
            encode_lpm_frame(msgspec.msgpack.encode(EchoRequest(message="a", count=2))),
            encode_lpm_frame(msgspec.msgpack.encode(EchoRequest(message="b", count=5))),
        ]
    )
    client_events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoClientStream",
        body=client_stream_payload,
    )
    client_frames = decode_lpm_frames(
        b"".join(e.get("body", b"") for e in client_events if e["type"] == "http.response.body")
    )
    decoded_client = msgspec.msgpack.decode(client_frames[0], type=EchoResponse)
    assert decoded_client.reply == "a|b"
    assert decoded_client.sequence == 7

    bidi_events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoBidiStream",
        body=client_stream_payload,
    )
    bidi_frames = decode_lpm_frames(
        b"".join(e.get("body", b"") for e in bidi_events if e["type"] == "http.response.body")
    )
    assert [msgspec.msgpack.decode(f, type=EchoResponse).reply for f in bidi_frames] == ["A", "B"]


@pytest.mark.anyio
async def test_grpc_error_maps_to_trailers_status_and_percent_encoded_message() -> None:
    """Verify GrpcError emits non-zero grpc-status and percent-encoded grpc-message trailers."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)

    bad_payload = encode_lpm_frame(msgspec.msgpack.encode(EchoRequest(message="bad", count=-1)))
    events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoUnary",
        body=bad_payload,
    )
    trailers = dict(events[-1]["headers"])
    assert trailers[b"grpc-status"] == b"3"
    assert trailers[b"grpc-message"] == b"count%20must%20be%20non-negative"


def test_protobuf_schema_generator_emits_proto3_definitions() -> None:
    """Verify _ProtobufSchemaGenerator renders syntax = proto3 messages and service RPCs."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)
    registry.register(echo_server_stream)
    registry.register(echo_client_stream)
    registry.register(echo_bidi_stream)

    generator = _ProtobufSchemaGenerator(package="vertebra.echo")
    assert ProtobufSchemaGenerator is _ProtobufSchemaGenerator
    proto_text = generator.generate_from_registry(registry)

    assert 'syntax = "proto3";' in proto_text
    assert "package vertebra.echo;" in proto_text
    assert "message EchoRequest {" in proto_text
    assert "optional string correlation_id = 1;" in proto_text
    assert "int64 count = 2;" in proto_text
    assert "string message = 3;" in proto_text
    assert "repeated string tags = 4;" in proto_text
    assert "message EchoResponse {" in proto_text
    assert "map<string, string> metadata = 1;" in proto_text
    assert "rpc EchoUnary(EchoRequest) returns (EchoResponse);" in proto_text
    assert "rpc EchoServerStream(EchoRequest) returns (stream EchoResponse);" in proto_text
    assert "rpc EchoClientStream(stream EchoRequest) returns (EchoResponse);" in proto_text
    assert "rpc EchoBidiStream(stream EchoRequest) returns (stream EchoResponse);" in proto_text


async def _invoke_asgi(
    registry: GrpcServiceRegistry,
    *,
    path: str,
    body: bytes,
) -> list[dict[str, Any]]:
    """Invoke GrpcServiceRegistry as an ASGI app and collect emitted send events."""
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": [(b"content-type", b"application/grpc+msgpack")],
        "extensions": {"http.response.trailers": {}},
    }
    sent: list[dict[str, Any]] = []
    consumed = False

    async def receive() -> dict[str, Any]:
        nonlocal consumed
        if not consumed:
            consumed = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(event: dict[str, Any]) -> None:
        sent.append(event)

    await registry.handle_asgi(scope, receive, send)
    return sent
