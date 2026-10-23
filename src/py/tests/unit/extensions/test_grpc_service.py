"""Unit tests for vertebra.extensions.grpc @grpc_method, GrpcServiceRegistry, ProtobufSchemaGenerator, and WasmPluginHost."""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import msgspec
import pytest

import vertebra.wasm as wasm_mod
from vertebra.extensions.grpc import (
    GrpcContext,
    GrpcError,
    GrpcMethodKind,
    GrpcServiceRegistry,
    GrpcStatus,
    ProtobufSchemaGenerator,
    decode_lpm_frames,
    encode_lpm_frame,
    grpc_method,
)
from vertebra.typing import MissingDependencyError
from vertebra.utils.serializers import decode_msgpack, encode_msgpack
from vertebra.wasm import (
    WASM_MAGIC_HEADER,
    WasmExecutionError,
    WasmModuleConfig,
    WasmPluginHost,
    WasmSandboxPolicy,
)


class EchoRequest(msgspec.Struct, gc=False):
    """Input message for unary and streaming echo RPCs."""

    message: str
    count: int = 1
    tags: list[str] = []
    correlation_id: str | None = None


class EchoResponse(msgspec.Struct, gc=False):
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


@pytest.mark.anyio
async def test_grpc_service_registry_unary_and_streaming_asgi_dispatch() -> None:
    """Verify GrpcServiceRegistry dispatches unary, server-stream, client-stream, and bidi-stream over ASGI."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)
    registry.register(echo_server_stream)
    registry.register(echo_client_stream)
    registry.register(echo_bidi_stream)

    unary_payload = encode_lpm_frame(encode_msgpack(EchoRequest(message="ping", count=7)))
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
    decoded_unary = decode_msgpack(body_frames[0], type=EchoResponse)
    assert decoded_unary.reply == "ack:ping"
    assert decoded_unary.sequence == 7
    trailer_event = unary_events[-1]
    assert trailer_event["type"] == "http.response.trailers"
    assert (b"grpc-status", b"0") in trailer_event["headers"]

    stream_payload = encode_lpm_frame(encode_msgpack(EchoRequest(message="item", count=3)))
    stream_events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoServerStream",
        body=stream_payload,
    )
    stream_frames = decode_lpm_frames(
        b"".join(e.get("body", b"") for e in stream_events if e["type"] == "http.response.body")
    )
    assert [decode_msgpack(f, type=EchoResponse).reply for f in stream_frames] == [
        "item-0",
        "item-1",
        "item-2",
    ]

    client_stream_payload = b"".join(
        [
            encode_lpm_frame(encode_msgpack(EchoRequest(message="a", count=2))),
            encode_lpm_frame(encode_msgpack(EchoRequest(message="b", count=5))),
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
    decoded_client = decode_msgpack(client_frames[0], type=EchoResponse)
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
    assert [decode_msgpack(f, type=EchoResponse).reply for f in bidi_frames] == ["A", "B"]


@pytest.mark.anyio
async def test_grpc_error_maps_to_trailers_status_and_percent_encoded_message() -> None:
    """Verify GrpcError emits non-zero grpc-status and percent-encoded grpc-message trailers."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)

    bad_payload = encode_lpm_frame(encode_msgpack(EchoRequest(message="bad", count=-1)))
    events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoUnary",
        body=bad_payload,
    )
    trailers = dict(events[-1]["headers"])
    assert trailers[b"grpc-status"] == b"3"
    assert trailers[b"grpc-message"] == b"count%20must%20be%20non-negative"


def test_decode_lpm_frames_rejects_trailing_partial_header() -> None:
    """Verify decode_lpm_frames raises GrpcError(INTERNAL) on trailing incomplete header bytes."""
    valid_frame = encode_lpm_frame(b"abc")
    with pytest.raises(GrpcError) as exc_info:
        decode_lpm_frames(valid_frame + b"\x00\x00")
    assert exc_info.value.status is GrpcStatus.INTERNAL


@pytest.mark.anyio
async def test_grpc_method_injects_grpc_context() -> None:
    """Verify @grpc_method supports 2-parameter handlers receiving GrpcContext from ASGI scope."""

    @grpc_method(service="EchoService", package="vertebra.echo")
    async def echo_with_ctx(req: EchoRequest, ctx: GrpcContext) -> EchoResponse:
        return EchoResponse(
            reply=f"{req.message}:{ctx.metadata.get('x-tenant', '')}",
            sequence=int(ctx.timeout_seconds or 0),
            metadata={"peer": ctx.peer},
        )

    registry = GrpcServiceRegistry()
    registry.register(echo_with_ctx)

    payload = encode_lpm_frame(encode_msgpack(EchoRequest(message="hello")))
    events = await _invoke_asgi(
        registry,
        path="/vertebra.echo.EchoService/EchoWithCtx",
        body=payload,
        extra_headers=[(b"x-tenant", b"acme"), (b"grpc-timeout", b"5S")],
    )
    frames = decode_lpm_frames(b"".join(e.get("body", b"") for e in events if e["type"] == "http.response.body"))
    decoded = decode_msgpack(frames[0], type=EchoResponse)
    assert decoded.reply == "hello:acme"
    assert decoded.sequence == 5
    assert decoded.metadata["peer"] == "127.0.0.1:50051"


def test_protobuf_schema_generator_emits_proto3_definitions() -> None:
    """Verify ProtobufSchemaGenerator renders syntax = proto3 messages in declaration order and service RPCs."""
    registry = GrpcServiceRegistry()
    registry.register(echo_unary)
    registry.register(echo_server_stream)
    registry.register(echo_client_stream)
    registry.register(echo_bidi_stream)

    generator = ProtobufSchemaGenerator(package="vertebra.echo")
    proto_text = generator.generate_from_registry(registry)

    assert 'syntax = "proto3";' in proto_text
    assert "package vertebra.echo;" in proto_text
    assert "message EchoRequest {" in proto_text
    assert "string message = 1;" in proto_text
    assert "int64 count = 2;" in proto_text
    assert "repeated string tags = 3;" in proto_text
    assert "optional string correlation_id = 4;" in proto_text
    assert "message EchoResponse {" in proto_text
    assert "string reply = 1;" in proto_text
    assert "int64 sequence = 2;" in proto_text
    assert "map<string, string> metadata = 3;" in proto_text
    assert "rpc EchoUnary(EchoRequest) returns (EchoResponse);" in proto_text
    assert "rpc EchoServerStream(EchoRequest) returns (stream EchoResponse);" in proto_text
    assert "rpc EchoClientStream(stream EchoRequest) returns (EchoResponse);" in proto_text
    assert "rpc EchoBidiStream(stream EchoRequest) returns (stream EchoResponse);" in proto_text


def test_wasm_plugin_host_execution_and_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify WasmPluginHost executes modules with fuel/memory limits and raises MissingDependencyError when wasmtime is absent."""
    wasm_add_bytes = bytes.fromhex("0061736d0100000001070160027f7f017f030201000707010361646400000a09010700200020016a0b")
    assert WasmModuleConfig.validate_binary(wasm_add_bytes) is True
    assert wasm_add_bytes.startswith(WASM_MAGIC_HEADER)

    host = WasmPluginHost()
    host.register_module(
        WasmModuleConfig(
            name="host_math",
            wasm_source=wasm_add_bytes,
            policy=WasmSandboxPolicy(max_fuel=50),
        ),
        exports={"add": lambda a, b: a + b},
    )
    assert host.call_export("host_math", "add", 20, 22, fuel_cost=10) == 42
    with pytest.raises(WasmExecutionError, match="exceeded max_fuel"):
        host.call_export("host_math", "add", 1, 2, fuel_cost=100)

    monkeypatch.setattr(wasm_mod, "_wasmtime", None)
    with pytest.raises(MissingDependencyError, match="wasmtime"):
        host.register_module(WasmModuleConfig(name="guest_missing", wasm_source=wasm_add_bytes))

    class _FakeStore:
        def __init__(self, _engine: Any) -> None:
            self.fuel = 0
            self.memory_size = 0

        def set_fuel(self, fuel: int) -> None:
            self.fuel = fuel

        def set_limits(self, *, memory_size: int) -> None:
            self.memory_size = memory_size

    class _FakeInstance:
        def __init__(self, _store: _FakeStore, _module: Any, _imports: list[Any]) -> None:
            pass

        def exports(self, _store: _FakeStore) -> dict[str, Any]:
            def _add(st: _FakeStore, a: int, b: int) -> int:
                if st.fuel <= 1:
                    msg = "all fuel consumed by WebAssembly"
                    raise RuntimeError(msg)
                return a + b

            return {"add": _add}

    fake_wasmtime = SimpleNamespace(
        Config=lambda: SimpleNamespace(consume_fuel=False),
        Engine=lambda cfg: SimpleNamespace(config=cfg),
        Module=lambda engine, raw: SimpleNamespace(engine=engine, raw=raw),
        Store=_FakeStore,
        Instance=_FakeInstance,
    )
    monkeypatch.setattr(wasm_mod, "_wasmtime", fake_wasmtime)
    host.register_module(WasmModuleConfig(name="math", wasm_source=wasm_add_bytes))
    assert host.call_export("math", "add", 20, 22) == 42

    host.register_module(
        WasmModuleConfig(
            name="starved",
            wasm_source=wasm_add_bytes,
            policy=WasmSandboxPolicy(max_fuel=1),
        )
    )
    with pytest.raises(WasmExecutionError, match="all fuel consumed"):
        host.call_export("starved", "add", 1, 2)


async def _invoke_asgi(
    registry: GrpcServiceRegistry,
    *,
    path: str,
    body: bytes,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> list[dict[str, Any]]:
    """Invoke GrpcServiceRegistry as an ASGI app and collect emitted send events."""
    headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/grpc+msgpack")]
    if extra_headers:
        headers.extend(extra_headers)
    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": path,
        "client": ("127.0.0.1", 50051),
        "headers": headers,
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
