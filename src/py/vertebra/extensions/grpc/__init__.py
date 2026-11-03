"""Native gRPC extension for Vertebra supporting msgspec.Struct RPCs and Protobuf v3 schema generation."""

from __future__ import annotations

import collections.abc
import enum
import inspect
import struct
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from typing import Any, Protocol, cast, get_args, get_origin, get_type_hints

import msgspec
import msgspec.inspect

from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.text import pascalize

__all__ = (
    "GrpcContext",
    "GrpcError",
    "GrpcMethodKind",
    "GrpcMethodSpec",
    "GrpcServiceRegistry",
    "GrpcStatus",
    "ProtobufSchemaGenerator",
    "decode_lpm_frames",
    "encode_lpm_frame",
    "grpc_method",
    "percent_encode_grpc_message",
)

_LPM_HEADER_STRUCT = struct.Struct(">BI")
_ASYNC_STREAM_ORIGINS = (
    collections.abc.AsyncIterator,
    collections.abc.AsyncIterable,
    collections.abc.AsyncGenerator,
)
_GRPC_TIMEOUT_UNITS: dict[str, float] = {
    "H": 3600.0,
    "M": 60.0,
    "S": 1.0,
    "m": 1e-3,
    "u": 1e-6,
    "n": 1e-9,
}


class GrpcStatus(enum.IntEnum):
    """Canonical gRPC status codes (0..16) per the gRPC over HTTP/2 specification."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


class GrpcError(Exception):
    """Exception raised by gRPC handlers to abort an RPC with a specific GrpcStatus and message."""

    def __init__(self, status: GrpcStatus, details: str = "") -> None:
        super().__init__(details or status.name)
        self.status = status
        self.details = details or status.name


class GrpcContext(msgspec.Struct, frozen=True, gc=False):
    """Invocation context carrying peer address, request metadata, and optional deadline."""

    peer: str = ""
    metadata: dict[str, str] = msgspec.field(default_factory=dict)
    timeout_seconds: float | None = None


class GrpcMethodKind(enum.Enum):
    """Streaming cardinality of a gRPC method."""

    UNARY = "unary"
    SERVER_STREAMING = "server_streaming"
    CLIENT_STREAMING = "client_streaming"
    BIDI_STREAMING = "bidi_streaming"


class GrpcMethodSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Introspected metadata for a decorated @grpc_method handler."""

    service: str
    package: str
    method_name: str
    full_path: str
    kind: GrpcMethodKind
    input_type: type[msgspec.Struct]
    output_type: type[msgspec.Struct]
    handler: Callable[..., Any]
    accepts_context: bool = False


class GrpcDecoratedCallable(Protocol):
    """Protocol for an async gRPC handler decorated with @grpc_method."""

    __grpc_method__: GrpcMethodSpec

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Invoke the underlying gRPC handler."""
        ...


def encode_lpm_frame(payload: bytes, *, compressed: bool = False) -> bytes:
    """Encode a message payload with a 5-byte gRPC Length-Prefixed Message header."""
    header = _LPM_HEADER_STRUCT.pack(1 if compressed else 0, len(payload))
    return header + payload


def decode_lpm_frames(data: bytes | bytearray | memoryview) -> list[bytes]:
    """Decode one or more complete 5-byte gRPC Length-Prefixed Message frames from a byte buffer."""
    view = memoryview(data)
    offset = 0
    total = len(view)
    frames: list[bytes] = []
    while offset + 5 <= total:
        compressed, length = _LPM_HEADER_STRUCT.unpack_from(view, offset)
        if compressed not in (0, 1):
            raise GrpcError(GrpcStatus.INTERNAL, f"invalid gRPC LPM compression flag: {compressed}")
        offset += 5
        if offset + length > total:
            raise GrpcError(GrpcStatus.INTERNAL, "truncated gRPC LPM frame")
        frames.append(bytes(view[offset : offset + length]))
        offset += length
    if offset < total:
        raise GrpcError(GrpcStatus.INTERNAL, "incomplete gRPC LPM header")
    return frames


def percent_encode_grpc_message(message: str) -> bytes:
    """Percent-encode a gRPC status detail string per RFC 3986 for the grpc-message trailer."""
    out = bytearray()
    for byte in message.encode("utf-8"):
        if 0x20 < byte <= 0x7E and byte != 0x25:
            out.append(byte)
        else:
            out.extend(f"%{byte:02X}".encode("ascii"))
    return bytes(out)


def grpc_method(
    *,
    service: str,
    package: str = "vertebra",
    name: str | None = None,
) -> Callable[[Callable[..., Any]], GrpcDecoratedCallable]:
    """Decorate an async function as a typed gRPC method over msgspec.Struct request and response types.

    Supports all four gRPC streaming cardinalities and an optional second ``GrpcContext`` parameter:
    - Unary: ``async def fn(req: Req[, ctx: GrpcContext]) -> Resp``
    - Server-streaming: ``async def fn(req: Req[, ctx: GrpcContext]) -> AsyncIterator[Resp]``
    - Client-streaming: ``async def fn(stream: AsyncIterator[Req][, ctx: GrpcContext]) -> Resp``
    - Bidirectional-streaming: ``async def fn(stream: AsyncIterator[Req][, ctx: GrpcContext]) -> AsyncIterator[Resp]``
    """

    def decorator(fn: Callable[..., Any]) -> GrpcDecoratedCallable:
        hints = get_type_hints(fn)
        params = [p for p in inspect.signature(fn).parameters.values() if p.name != "self"]
        if len(params) not in (1, 2):
            raise TypeError(
                f"@grpc_method handler {fn.__name__!r} must accept 1 request parameter (plus optional GrpcContext)"
            )

        raw_input = hints.get(params[0].name)
        raw_output = hints.get("return")
        if raw_input is None or raw_output is None:
            raise TypeError(f"@grpc_method handler {fn.__name__!r} requires input and return type annotations")

        accepts_context = len(params) == 2
        client_streaming, input_struct = _unwrap_stream_type(raw_input)
        server_streaming, output_struct = _unwrap_stream_type(raw_output)

        if client_streaming and server_streaming:
            kind = GrpcMethodKind.BIDI_STREAMING
        elif client_streaming:
            kind = GrpcMethodKind.CLIENT_STREAMING
        elif server_streaming:
            kind = GrpcMethodKind.SERVER_STREAMING
        else:
            kind = GrpcMethodKind.UNARY

        rpc_name = name or pascalize(fn.__name__)
        qualified_service = f"{package}.{service}" if package else service
        full_path = f"/{qualified_service}/{rpc_name}"

        spec = GrpcMethodSpec(
            service=service,
            package=package,
            method_name=rpc_name,
            full_path=full_path,
            kind=kind,
            input_type=input_struct,
            output_type=output_struct,
            handler=fn,
            accepts_context=accepts_context,
        )
        fn.__dict__["__grpc_method__"] = spec
        return cast("GrpcDecoratedCallable", fn)

    return decorator


class GrpcServiceRegistry:
    """Registry and ASGI dispatcher for @grpc_method handlers."""

    __slots__ = ("_methods",)

    def __init__(self) -> None:
        self._methods: dict[str, GrpcMethodSpec] = {}

    @property
    def methods(self) -> dict[str, GrpcMethodSpec]:
        """Return a mapping of full gRPC HTTP/2 paths to GrpcMethodSpec definitions."""
        return dict(self._methods)

    def register(self, handler: Callable[..., Any]) -> GrpcMethodSpec:
        """Register a @grpc_method-decorated callable and return its GrpcMethodSpec."""
        spec: GrpcMethodSpec | None = getattr(handler, "__grpc_method__", None)
        if spec is None:
            raise ValueError(f"Handler {handler!r} is not decorated with @grpc_method")
        self._methods[spec.full_path] = spec
        return spec

    async def handle_asgi(
        self,
        scope: dict[str, Any],
        receive: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Dispatch an inbound ASGI HTTP/2 gRPC request and emit LPM frames plus trailers."""
        path: str = scope.get("path", "")
        raw_headers: list[tuple[bytes, bytes]] = list(scope.get("headers", []))
        content_type = self._extract_content_type(raw_headers)
        spec = self._methods.get(path)

        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", content_type)],
                "trailers": True,
            }
        )

        if spec is None:
            await self._send_trailers(send, GrpcStatus.UNIMPLEMENTED, f"unknown method {path}")
            return

        ctx = self._build_context(scope, raw_headers)
        body_bytes = await self._read_full_body(receive)
        try:
            raw_frames = decode_lpm_frames(body_bytes)
            await self._dispatch_spec(spec, raw_frames, ctx, send)
        except GrpcError as exc:
            await self._send_trailers(send, exc.status, exc.details)
        except msgspec.DecodeError as exc:
            await self._send_trailers(send, GrpcStatus.INVALID_ARGUMENT, str(exc))
        except Exception as exc:
            await self._send_trailers(send, GrpcStatus.INTERNAL, str(exc))
        else:
            await self._send_trailers(send, GrpcStatus.OK, "")

    async def _dispatch_spec(
        self,
        spec: GrpcMethodSpec,
        raw_frames: list[bytes],
        ctx: GrpcContext,
        send: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        """Execute a GrpcMethodSpec across unary, server-streaming, client-streaming, or bidi-streaming modes."""
        if spec.kind is GrpcMethodKind.UNARY:
            if len(raw_frames) != 1:
                raise GrpcError(GrpcStatus.INVALID_ARGUMENT, "unary RPC requires exactly one request frame")
            req_obj = decode_msgpack(raw_frames[0], type=spec.input_type)
            resp_obj = await (spec.handler(req_obj, ctx) if spec.accepts_context else spec.handler(req_obj))
            encoded = encode_lpm_frame(encode_msgpack_bytes(resp_obj))
            await send({"type": "http.response.body", "body": encoded, "more_body": False})
            return

        if spec.kind is GrpcMethodKind.SERVER_STREAMING:
            if len(raw_frames) != 1:
                raise GrpcError(GrpcStatus.INVALID_ARGUMENT, "server-streaming RPC requires one request frame")
            req_obj = decode_msgpack(raw_frames[0], type=spec.input_type)
            raw_stream = spec.handler(req_obj, ctx) if spec.accepts_context else spec.handler(req_obj)
            stream_out: AsyncIterable[Any] = await self._resolve_async_iterable(raw_stream)
            async for item in stream_out:
                chunk = encode_lpm_frame(encode_msgpack_bytes(item))
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
            await send({"type": "http.response.body", "body": b"", "more_body": False})
            return

        if spec.kind is GrpcMethodKind.CLIENT_STREAMING:
            req_stream = self._iter_decoded_requests(raw_frames, spec.input_type)
            resp_obj = await (spec.handler(req_stream, ctx) if spec.accepts_context else spec.handler(req_stream))
            encoded = encode_lpm_frame(encode_msgpack_bytes(resp_obj))
            await send({"type": "http.response.body", "body": encoded, "more_body": False})
            return

        req_stream = self._iter_decoded_requests(raw_frames, spec.input_type)
        raw_bidi = spec.handler(req_stream, ctx) if spec.accepts_context else spec.handler(req_stream)
        bidi_out: AsyncIterable[Any] = await self._resolve_async_iterable(raw_bidi)
        async for item in bidi_out:
            chunk = encode_lpm_frame(encode_msgpack_bytes(item))
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    async def _resolve_async_iterable(self, result: Any) -> AsyncIterable[Any]:
        """Await coroutine wrappers when a streaming handler returns an awaitable async iterable."""
        if inspect.isawaitable(result):
            resolved: AsyncIterable[Any] = await result
            return resolved
        out: AsyncIterable[Any] = result
        return out

    async def _iter_decoded_requests(
        self,
        raw_frames: list[bytes],
        input_type: type[msgspec.Struct],
    ) -> AsyncIterator[Any]:
        """Yield decoded msgspec.Struct instances from a list of LPM frame payloads."""
        for frame in raw_frames:
            yield decode_msgpack(frame, type=input_type)

    async def _read_full_body(
        self,
        receive: Callable[[], Awaitable[dict[str, Any]]],
    ) -> bytes:
        """Read all ASGI http.request body chunks into a single byte string."""
        chunks: list[bytes] = []
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                break
            body = message.get("body", b"")
            if body:
                chunks.append(body)
            if not message.get("more_body", False):
                break
        return b"".join(chunks)

    async def _send_trailers(
        self,
        send: Callable[[dict[str, Any]], Awaitable[None]],
        status: GrpcStatus,
        details: str,
    ) -> None:
        """Emit the ASGI http.response.trailers event with grpc-status and grpc-message."""
        headers: list[tuple[bytes, bytes]] = [(b"grpc-status", str(int(status)).encode("ascii"))]
        if details:
            headers.append((b"grpc-message", percent_encode_grpc_message(details)))
        await send({"type": "http.response.trailers", "headers": headers, "more_trailers": False})

    def _extract_content_type(self, headers: list[tuple[bytes, bytes]]) -> bytes:
        """Extract the inbound gRPC content-type header or default to application/grpc+msgpack."""
        for name, value in headers:
            if name.lower() == b"content-type" and value.startswith(b"application/grpc"):
                return value
        return b"application/grpc+msgpack"

    def _build_context(
        self,
        scope: dict[str, Any],
        raw_headers: list[tuple[bytes, bytes]],
    ) -> GrpcContext:
        """Build a GrpcContext from ASGI scope client info, request metadata headers, and grpc-timeout."""
        client = scope.get("client")
        peer = f"{client[0]}:{client[1]}" if isinstance(client, tuple | list) and len(client) >= 2 else ""
        metadata: dict[str, str] = {}
        timeout_seconds: float | None = None
        for raw_k, raw_v in raw_headers:
            key = raw_k.decode("latin-1").lower()
            val = raw_v.decode("latin-1")
            metadata[key] = val
            if key == "grpc-timeout" and len(val) >= 2:
                unit_scale = _GRPC_TIMEOUT_UNITS.get(val[-1])
                if unit_scale is not None and val[:-1].isdigit():
                    timeout_seconds = int(val[:-1]) * unit_scale
        return GrpcContext(peer=peer, metadata=metadata, timeout_seconds=timeout_seconds)


class ProtobufSchemaGenerator:
    """Convert msgspec.Struct types and GrpcServiceRegistry definitions into .proto v3 schemas."""

    __slots__ = ("_package", "_sort_fields")

    def __init__(self, *, package: str = "vertebra", sort_fields: bool = False) -> None:
        self._package = package
        self._sort_fields = sort_fields

    def generate_from_registry(self, registry: GrpcServiceRegistry) -> str:
        """Render a complete .proto v3 file from all methods registered in GrpcServiceRegistry."""
        lines: list[str] = ['syntax = "proto3";', "", f"package {self._package};", ""]
        seen_structs: dict[str, str] = {}
        services: dict[str, list[GrpcMethodSpec]] = {}

        for spec in registry.methods.values():
            self._collect_struct_definitions(spec.input_type, seen_structs)
            self._collect_struct_definitions(spec.output_type, seen_structs)
            services.setdefault(spec.service, []).append(spec)

        for msg_def in seen_structs.values():
            lines.append(msg_def)
            lines.append("")

        for service_name, specs in services.items():
            lines.append(f"service {service_name} {{")
            for spec in specs:
                in_stream = (
                    "stream " if spec.kind in (GrpcMethodKind.CLIENT_STREAMING, GrpcMethodKind.BIDI_STREAMING) else ""
                )
                out_stream = (
                    "stream " if spec.kind in (GrpcMethodKind.SERVER_STREAMING, GrpcMethodKind.BIDI_STREAMING) else ""
                )
                lines.append(
                    f"  rpc {spec.method_name}({in_stream}{spec.input_type.__name__}) "
                    f"returns ({out_stream}{spec.output_type.__name__});"
                )
            lines.append("}")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def render_struct_message(self, struct_cls: type[msgspec.Struct]) -> str:
        """Render a single msgspec.Struct subclass as a deterministic proto3 message block."""
        seen: dict[str, str] = {}
        self._collect_struct_definitions(struct_cls, seen)
        return seen[struct_cls.__name__]

    def _collect_struct_definitions(
        self,
        struct_cls: type[msgspec.Struct],
        seen: dict[str, str],
    ) -> None:
        """Recursively collect proto3 message definitions for struct_cls and any nested Struct fields."""
        if struct_cls.__name__ in seen:
            return

        info = msgspec.inspect.type_info(struct_cls)
        if not isinstance(info, msgspec.inspect.StructType):
            return

        for fld in info.fields:
            self._discover_nested_structs(fld.type, seen)

        lines: list[str] = [f"message {struct_cls.__name__} {{"]
        ordered_fields = sorted(info.fields, key=lambda f: f.name) if self._sort_fields else list(info.fields)
        for index, fld in enumerate(ordered_fields, start=1):
            field_decl = self._format_field_declaration(fld.name, fld.type, index)
            lines.append(f"  {field_decl};")
        lines.append("}")
        seen[struct_cls.__name__] = "\n".join(lines)

    def _discover_nested_structs(
        self,
        type_info: msgspec.inspect.Type,
        seen: dict[str, str],
    ) -> None:
        """Traverse msgspec.inspect.Type nodes to register nested StructType dependencies first."""
        if isinstance(type_info, msgspec.inspect.StructType):
            self._collect_struct_definitions(type_info.cls, seen)
        elif isinstance(
            type_info,
            msgspec.inspect.ListType | msgspec.inspect.SetType | msgspec.inspect.FrozenSetType,
        ):
            self._discover_nested_structs(type_info.item_type, seen)
        elif isinstance(type_info, msgspec.inspect.DictType):
            self._discover_nested_structs(type_info.value_type, seen)
        elif isinstance(type_info, msgspec.inspect.UnionType):
            for member in type_info.types:
                self._discover_nested_structs(member, seen)

    def _format_field_declaration(
        self,
        field_name: str,
        type_info: msgspec.inspect.Type,
        field_number: int,
    ) -> str:
        """Format a single proto3 message field line with repeated, optional, or map qualifiers."""
        if isinstance(
            type_info,
            msgspec.inspect.ListType | msgspec.inspect.SetType | msgspec.inspect.FrozenSetType,
        ):
            item_proto = self._scalar_or_message_type(type_info.item_type)
            return f"repeated {item_proto} {field_name} = {field_number}"

        if isinstance(type_info, msgspec.inspect.DictType):
            key_proto = self._scalar_or_message_type(type_info.key_type)
            val_proto = self._scalar_or_message_type(type_info.value_type)
            return f"map<{key_proto}, {val_proto}> {field_name} = {field_number}"

        if isinstance(type_info, msgspec.inspect.UnionType):
            non_none = [t for t in type_info.types if not isinstance(t, msgspec.inspect.NoneType)]
            has_none = any(isinstance(t, msgspec.inspect.NoneType) for t in type_info.types)
            if len(non_none) == 1 and has_none:
                inner_proto = self._scalar_or_message_type(non_none[0])
                return f"optional {inner_proto} {field_name} = {field_number}"

        proto_type = self._scalar_or_message_type(type_info)
        return f"{proto_type} {field_name} = {field_number}"

    def _scalar_or_message_type(self, type_info: msgspec.inspect.Type) -> str:
        """Map a msgspec.inspect.Type node to its proto3 scalar or message type name."""
        if isinstance(type_info, msgspec.inspect.IntType):
            return "int64"
        if isinstance(type_info, msgspec.inspect.FloatType):
            return "double"
        if isinstance(type_info, msgspec.inspect.StrType | msgspec.inspect.UUIDType):
            return "string"
        if isinstance(type_info, msgspec.inspect.BoolType):
            return "bool"
        if isinstance(type_info, msgspec.inspect.BytesType | msgspec.inspect.ByteArrayType):
            return "bytes"
        if isinstance(type_info, msgspec.inspect.StructType):
            return type_info.cls.__name__
        return "bytes"


def _unwrap_stream_type(annotation: Any) -> tuple[bool, type[msgspec.Struct]]:
    """Unwrap an annotation into (is_streaming, msgspec_struct_cls)."""
    origin = get_origin(annotation)
    if origin in _ASYNC_STREAM_ORIGINS:
        args = get_args(annotation)
        if not args:
            raise TypeError(f"Streaming gRPC annotation {annotation!r} must specify a msgspec.Struct item type")
        inner = args[0]
        if isinstance(inner, type) and issubclass(inner, msgspec.Struct):
            return True, inner
        raise TypeError(f"Streaming gRPC item type {inner!r} must be a msgspec.Struct subclass")

    if isinstance(annotation, type) and issubclass(annotation, msgspec.Struct):
        return False, annotation

    raise TypeError(f"gRPC message annotation {annotation!r} must be a msgspec.Struct or AsyncIterator[Struct]")
