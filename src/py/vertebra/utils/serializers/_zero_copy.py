"""Pluggable zero-copy codec and type adapter registry for HTTP, gRPC, and Wasm."""

from __future__ import annotations

import dataclasses
import inspect
import struct
import threading
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

_native_vtb = import_optional("vertebra._vertebra")


def _py_pack_grpc_lpm_frame(payload: Any, compressed: bool = False) -> bytes:
    mv = memoryview(payload)
    return _LPM_STRUCT.pack(int(compressed), len(mv)) + bytes(mv)


def _py_instantiate_struct_vectorcall(cls: Any, args: Any = None, kwargs: Any = None) -> Any:
    return cls(*(args or ()), **(kwargs or {}))


def _py_hydrate_one_vectorcall(cls: Any, row: Any, columns: Any = None) -> Any:
    if isinstance(row, dict):
        return cls(**row)
    if columns is not None:
        return cls(**dict(zip(columns, row, strict=False)))
    if isinstance(row, tuple | list):
        return cls(*row)
    names = tuple(getattr(cls, "__struct_fields__", ()))
    return cls(**{k: getattr(row, k) for k in names})


def _py_hydrate_rows_vectorcall(cls: Any, rows: Any, columns: Any = None) -> list[Any]:
    cols = tuple(columns) if columns is not None else None
    return [_py_hydrate_one_vectorcall(cls, r, cols) for r in rows]


def _py_orm_to_struct_vectorcall(cls: Any, orm_obj: Any, fields: Any = None) -> Any:
    names = tuple(fields) if fields is not None else tuple(getattr(cls, "__struct_fields__", ()))
    if isinstance(orm_obj, list | tuple):
        return [cls(**{k: getattr(item, k) for k in names}) for item in orm_obj]
    return cls(**{k: getattr(orm_obj, k) for k in names})


hydrate_one_vectorcall: Callable[..., Any] = (
    getattr(_native_vtb, "hydrate_one_vectorcall", None) or _py_hydrate_one_vectorcall
)
hydrate_rows_vectorcall: Callable[..., list[Any]] = (
    getattr(_native_vtb, "hydrate_rows_vectorcall", None) or _py_hydrate_rows_vectorcall
)
instantiate_struct_vectorcall: Callable[..., Any] = (
    getattr(_native_vtb, "instantiate_struct_vectorcall", None) or _py_instantiate_struct_vectorcall
)
orm_to_struct_vectorcall: Callable[..., Any] = (
    getattr(_native_vtb, "orm_to_struct_vectorcall", None) or _py_orm_to_struct_vectorcall
)
pack_grpc_lpm_frame: Callable[..., bytes] = getattr(_native_vtb, "pack_grpc_lpm_frame", None) or _py_pack_grpc_lpm_frame

__all__ = (
    "GRPC_LPM_HEADER_LEN",
    "TypeAdapterKind",
    "TypeAdapterPlan",
    "ZeroCopyCodecRegistry",
    "decode_grpc_frame",
    "decode_grpc_frames",
    "encode_grpc_frame_bytes",
    "encode_grpc_frame_into",
    "encode_json_into_arena",
    "encode_msgpack_into_arena",
)

GRPC_LPM_HEADER_LEN = 5
_LPM_STRUCT = struct.Struct(">BI")

type TypeAdapterKind = Literal[
    "msgspec",
    "pydantic",
    "attrs",
    "dataclass",
    "protobuf",
    "arrow",
    "polars",
    "raw_buffer",
    "generic",
]


class _ThreadLocalArena(threading.local):
    """Per-worker-thread reusable bytearray buffers for zero-copy response and gRPC LPM encoding."""

    def __init__(self) -> None:
        self.json_buf = bytearray(4096)
        self.msgpack_buf = bytearray(4096)
        self.grpc_buf = bytearray(4096)
        self.json_encoder: msgspec.json.Encoder | None = None
        self.msgpack_encoder: msgspec.msgpack.Encoder | None = None


_ARENA = _ThreadLocalArena()


def _get_thread_json_encoder(enc_hook: Callable[[Any], Any]) -> msgspec.json.Encoder:
    """Return the thread-local msgspec JSON Encoder."""
    enc = _ARENA.json_encoder
    if enc is None:
        enc = msgspec.json.Encoder(enc_hook=enc_hook)
        _ARENA.json_encoder = enc
    return enc


def _get_thread_msgpack_encoder(enc_hook: Callable[[Any], Any]) -> msgspec.msgpack.Encoder:
    """Return the thread-local msgspec MsgPack Encoder."""
    enc = _ARENA.msgpack_encoder
    if enc is None:
        enc = msgspec.msgpack.Encoder(enc_hook=enc_hook)
        _ARENA.msgpack_encoder = enc
    return enc


def encode_json_into_arena(
    value: Any,
    *,
    enc_hook: Callable[[Any], Any],
    offset: int = 0,
) -> memoryview:
    """Encode value into the thread-local JSON bytearray arena at offset and return a memoryview."""
    buf = _ARENA.json_buf
    enc = _get_thread_json_encoder(enc_hook)
    try:
        enc.encode_into(value, buf, offset)
    except BufferError:
        buf = bytearray(4096)
        _ARENA.json_buf = buf
        enc.encode_into(value, buf, offset)
    return memoryview(buf)


def encode_msgpack_into_arena(
    value: Any,
    *,
    enc_hook: Callable[[Any], Any],
    offset: int = 0,
) -> memoryview:
    """Encode value into the thread-local MsgPack bytearray arena at offset and return a memoryview."""
    buf = _ARENA.msgpack_buf
    enc = _get_thread_msgpack_encoder(enc_hook)
    try:
        enc.encode_into(value, buf, offset)
    except BufferError:
        buf = bytearray(4096)
        _ARENA.msgpack_buf = buf
        enc.encode_into(value, buf, offset)
    return memoryview(buf)


def _acquire_writable_grpc_buf(target_buf: bytearray | None) -> bytearray:
    """Return a writable bytearray with at least 5 header bytes, rotating the thread-local buffer if locked."""
    if target_buf is not None:
        if len(target_buf) < GRPC_LPM_HEADER_LEN:
            target_buf.extend(b"\x00" * (GRPC_LPM_HEADER_LEN - len(target_buf)))
        return target_buf
    buf = _ARENA.grpc_buf
    try:
        if len(buf) < GRPC_LPM_HEADER_LEN:
            buf.extend(b"\x00" * (GRPC_LPM_HEADER_LEN - len(buf)))
        else:
            del buf[GRPC_LPM_HEADER_LEN:]
    except BufferError:
        buf = bytearray(GRPC_LPM_HEADER_LEN)
        _ARENA.grpc_buf = buf
    return buf


def encode_grpc_frame_into(
    value: Any,
    *,
    format: str = "msgpack",
    compressed: bool = False,
    enc_hook: Callable[[Any], Any] | None = None,
    target_buf: bytearray | None = None,
) -> memoryview:
    """Encode value directly at offset 5 of a bytearray and stamp the 5-byte gRPC LPM header in-place."""
    buf = _acquire_writable_grpc_buf(target_buf)

    if isinstance(value, bytes | bytearray | memoryview):
        raw_mv = memoryview(value)
        payload_len = len(raw_mv)
        del buf[GRPC_LPM_HEADER_LEN:]
        buf.extend(raw_mv)
        _LPM_STRUCT.pack_into(buf, 0, int(compressed), payload_len)
        return memoryview(buf)

    serialize_to_str = getattr(value, "SerializeToString", None)
    if callable(serialize_to_str) and ("proto" in format or format == "grpc"):
        proto_bytes: bytes = serialize_to_str()
        del buf[GRPC_LPM_HEADER_LEN:]
        buf.extend(proto_bytes)
        _LPM_STRUCT.pack_into(buf, 0, int(compressed), len(proto_bytes))
        return memoryview(buf)

    pydantic_ser = getattr(value, "__pydantic_serializer__", None)
    if pydantic_ser is not None and "json" in format:
        json_bytes: bytes = pydantic_ser.to_json(value)
        del buf[GRPC_LPM_HEADER_LEN:]
        buf.extend(json_bytes)
        _LPM_STRUCT.pack_into(buf, 0, int(compressed), len(json_bytes))
        return memoryview(buf)

    active_hook = enc_hook or _default_zero_copy_enc_hook
    if "json" in format:
        enc_json = _get_thread_json_encoder(active_hook)
        enc_json.encode_into(value, buf, GRPC_LPM_HEADER_LEN)
    else:
        enc_mp = _get_thread_msgpack_encoder(active_hook)
        enc_mp.encode_into(value, buf, GRPC_LPM_HEADER_LEN)

    payload_len = len(buf) - GRPC_LPM_HEADER_LEN
    _LPM_STRUCT.pack_into(buf, 0, int(compressed), payload_len)
    return memoryview(buf)


def encode_grpc_frame_bytes(
    value: Any,
    *,
    format: str = "msgpack",
    compressed: bool = False,
    enc_hook: Callable[[Any], Any] | None = None,
) -> bytes:
    """Encode value as a 5-byte LPM-framed gRPC message and return immutable bytes."""
    if isinstance(value, bytes | bytearray | memoryview):
        return pack_grpc_lpm_frame(value, compressed)
    mv = encode_grpc_frame_into(value, format=format, compressed=compressed, enc_hook=enc_hook)
    return bytes(mv)


def decode_grpc_frame(
    source: Any,
    *,
    target_type: Any = None,
    format: str = "msgpack",
) -> Any:
    """Decode a unary 5-byte LPM-framed gRPC message from a Request, WsgiInput, or buffer in zero copy."""
    decode_fn = getattr(source, "decode_grpc", None)
    if callable(decode_fn):
        return decode_fn(target_type, format)

    mv = memoryview(source.encode("utf-8") if isinstance(source, str) else source)
    if len(mv) >= GRPC_LPM_HEADER_LEN:
        flag, msg_len = _LPM_STRUCT.unpack_from(mv, 0)
        if flag <= 1 and GRPC_LPM_HEADER_LEN + msg_len <= len(mv):
            mv = mv[GRPC_LPM_HEADER_LEN : GRPC_LPM_HEADER_LEN + msg_len]

    plan = ZeroCopyCodecRegistry.compile(target_type)
    return plan.decode_buffer(mv, format=format)


def decode_grpc_frames(
    source: Any,
    *,
    target_type: Any = None,
    format: str = "msgpack",
) -> list[Any]:
    """Decode all concatenated 5-byte LPM gRPC frames from a Request, WsgiInput, or buffer in zero copy."""
    decode_frames_fn = getattr(source, "decode_grpc_frames", None)
    if callable(decode_frames_fn):
        res: list[Any] = decode_frames_fn(target_type, format)
        return res

    mv = memoryview(source.encode("utf-8") if isinstance(source, str) else source)
    if len(mv) == 0:
        return []

    plan = ZeroCopyCodecRegistry.compile(target_type)
    items: list[Any] = []
    offset = 0
    while offset + GRPC_LPM_HEADER_LEN <= len(mv):
        flag, msg_len = _LPM_STRUCT.unpack_from(mv, offset)
        if flag > 1 or offset + GRPC_LPM_HEADER_LEN + msg_len > len(mv):
            if offset == 0:
                items.append(plan.decode_buffer(mv, format=format))
            break
        frame_mv = mv[offset + GRPC_LPM_HEADER_LEN : offset + GRPC_LPM_HEADER_LEN + msg_len]
        items.append(plan.decode_buffer(frame_mv, format=format))
        offset += GRPC_LPM_HEADER_LEN + msg_len
    return items


def _default_zero_copy_enc_hook(val: Any) -> Any:
    """Fallback encoder hook for Pydantic models, Protobuf messages, Arrow tables, and Polars DataFrames."""
    model_dump = getattr(val, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    to_pylist = getattr(val, "to_pylist", None)
    if callable(to_pylist):
        return to_pylist()
    to_dicts = getattr(val, "to_dicts", None)
    if callable(to_dicts):
        return to_dicts()
    msg = f"Object of type {type(val).__name__} is not serializable"
    raise TypeError(msg)


class TypeAdapterPlan(msgspec.Struct, frozen=True, gc=False):
    """Pre-compiled zero-copy decode, encode, and row-hydration plan for a target type."""

    target_type: Any
    kind: TypeAdapterKind
    json_decoder: Callable[[Any], Any]
    msgpack_decoder: Callable[[Any], Any]
    json_encoder: Callable[[Any], bytes]
    msgpack_encoder: Callable[[Any], bytes]
    proto_decoder: Callable[[Any], Any] | None = None

    def decode_request(self, request: Any, *, format: str = "json") -> Any:
        """Decode a Request or WsgiInput directly from its PEP 3118 SHM buffer without a PyBytes copy."""
        if "grpc" in format:
            decode_grpc_fn = getattr(request, "decode_grpc", None)
            if callable(decode_grpc_fn):
                sub_fmt = "msgpack" if "msgpack" in format else ("proto" if "proto" in format else "json")
                return decode_grpc_fn(self.target_type, sub_fmt)
            raw_view = getattr(request, "body_view", None)
            if raw_view is not None:
                return decode_grpc_frame(raw_view, target_type=self.target_type, format=format)

        if self.kind == "raw_buffer":
            bv = getattr(request, "body_view", None)
            if bv is not None:
                return bv if self.target_type is memoryview else bytes(bv)
            body = getattr(request, "body", b"")
            return memoryview(body or b"") if self.target_type is memoryview else bytes(body or b"")

        if self.kind == "pydantic" and "msgpack" not in format:
            decode_pyd = getattr(request, "decode_pydantic_json", None)
            if callable(decode_pyd):
                return decode_pyd(self.target_type)

        decode_ms = getattr(request, "decode_msgspec", None)
        if callable(decode_ms) and self.kind in ("msgspec", "dataclass", "attrs", "generic"):
            return decode_ms(None if self.target_type is Any else self.target_type, format)

        bv = getattr(request, "body_view", None)
        if bv is not None:
            return self.decode_buffer(bv, format=format)
        raw_body = getattr(request, "body", None)
        if raw_body is None and hasattr(request, "read"):
            raw_body = request.read()
        return self.decode_buffer(raw_body or b"", format=format)

    def decode_buffer(self, buf: Any, *, format: str = "json") -> Any:
        """Decode a memoryview, bytes, or bytearray buffer into target_type."""
        if self.kind == "raw_buffer":
            return memoryview(buf) if self.target_type is memoryview else bytes(buf)
        if ("proto" in format or self.kind == "protobuf") and self.proto_decoder is not None:
            return self.proto_decoder(buf)
        if "msgpack" in format:
            return self.msgpack_decoder(buf)
        return self.json_decoder(buf)

    def encode(self, value: Any, *, format: str = "json") -> bytes:
        """Encode value into JSON, MsgPack, Protobuf, or gRPC LPM bytes."""
        if "grpc" in format:
            return encode_grpc_frame_bytes(value, format=format)
        if "msgpack" in format:
            return self.msgpack_encoder(value)
        return self.json_encoder(value)

    def hydrate_one(self, row: Any, columns: Sequence[str] | tuple[str, ...] | None = None) -> Any:
        """Hydrate a single database row or ORM instance into target_type via C-API vectorcall."""
        if self.target_type is None or self.target_type is Any or self.target_type is dict:
            return dict(zip(columns, row, strict=False)) if (columns is not None and not isinstance(row, dict)) else row
        return hydrate_one_vectorcall(self.target_type, row, columns)

    def hydrate_rows(self, rows: Any, columns: Sequence[str] | tuple[str, ...] | None = None) -> list[Any]:
        """Hydrate a sequence of database rows or ORM instances into list[target_type] via C-API vectorcall."""
        if self.target_type is None or self.target_type is Any or self.target_type is dict:
            if columns is not None:
                cols = tuple(columns)
                return [r if isinstance(r, dict) else dict(zip(cols, r, strict=False)) for r in rows]
            return list(rows)
        return hydrate_rows_vectorcall(self.target_type, rows, columns)

    def orm_to_struct(self, orm_obj: Any, fields: tuple[str, ...] | None = None) -> Any:
        """Project an ORM model (or sequence of ORM models) into target_type via C-API vectorcall."""
        return orm_to_struct_vectorcall(self.target_type, orm_obj, fields)


class ZeroCopyCodecRegistry:
    """Thread-safe registry compiling and caching TypeAdapterPlan instances per Python type."""

    _cache: dict[Any, TypeAdapterPlan] = {}
    _lock = threading.Lock()

    @classmethod
    def compile(cls, target_type: Any = Any) -> TypeAdapterPlan:
        """Return a cached or newly compiled TypeAdapterPlan for target_type."""
        key = Any if target_type is None else target_type
        cached = cls._cache.get(key)
        if cached is not None:
            return cached

        plan = cls._build_plan(key)
        with cls._lock:
            cls._cache[key] = plan
        return plan

    @classmethod
    def instantiate(cls, target_cls: type[Any], *args: Any, **kwargs: Any) -> Any:
        """Instantiate target_cls via C-API PyObject_Vectorcall."""
        return instantiate_struct_vectorcall(target_cls, args or None, kwargs or None)

    @classmethod
    def _build_plan(cls, target_type: Any) -> TypeAdapterPlan:
        """Inspect target_type and construct a specialized zero-copy TypeAdapterPlan."""
        if target_type in (bytes, bytearray, memoryview):
            return TypeAdapterPlan(
                target_type=target_type,
                kind="raw_buffer",
                json_decoder=lambda b: memoryview(b) if target_type is memoryview else bytes(b),
                msgpack_decoder=lambda b: memoryview(b) if target_type is memoryview else bytes(b),
                json_encoder=bytes,
                msgpack_encoder=bytes,
            )

        is_cls = inspect.isclass(target_type)

        if is_cls and issubclass(target_type, msgspec.Struct):
            json_dec = msgspec.json.Decoder(target_type).decode
            mp_dec = msgspec.msgpack.Decoder(target_type).decode
            json_enc = msgspec.json.Encoder(enc_hook=_default_zero_copy_enc_hook).encode
            mp_enc = msgspec.msgpack.Encoder(enc_hook=_default_zero_copy_enc_hook).encode
            return TypeAdapterPlan(
                target_type=target_type,
                kind="msgspec",
                json_decoder=json_dec,
                msgpack_decoder=mp_dec,
                json_encoder=json_enc,
                msgpack_encoder=mp_enc,
            )

        if (
            is_cls
            and hasattr(target_type, "__pydantic_validator__")
            and hasattr(target_type, "__pydantic_serializer__")
        ):
            validator = target_type.__pydantic_validator__
            serializer = target_type.__pydantic_serializer__
            val_json = validator.validate_json
            val_python = validator.validate_python
            to_json = serializer.to_json
            mp_raw_dec = msgspec.msgpack.decode
            mp_enc = msgspec.msgpack.Encoder(enc_hook=_default_zero_copy_enc_hook).encode

            def _pyd_json_dec(buf: Any, _vj: Any = val_json) -> Any:
                try:
                    return _vj(buf)
                except TypeError:
                    return _vj(bytes(buf))

            def _pyd_mp_dec(buf: Any, _vp: Any = val_python, _md: Any = mp_raw_dec) -> Any:
                return _vp(_md(buf))

            return TypeAdapterPlan(
                target_type=target_type,
                kind="pydantic",
                json_decoder=_pyd_json_dec,
                msgpack_decoder=_pyd_mp_dec,
                json_encoder=to_json,
                msgpack_encoder=lambda v: mp_enc(serializer.to_python(v, mode="json")),
            )

        if is_cls and hasattr(target_type, "FromString") and hasattr(target_type, "SerializeToString"):
            from_str = target_type.FromString

            def _proto_dec(buf: Any, _fs: Any = from_str) -> Any:
                try:
                    return _fs(buf)
                except TypeError:
                    return _fs(bytes(buf))

            return TypeAdapterPlan(
                target_type=target_type,
                kind="protobuf",
                json_decoder=_proto_dec,
                msgpack_decoder=_proto_dec,
                json_encoder=lambda v: v.SerializeToString(),
                msgpack_encoder=lambda v: v.SerializeToString(),
                proto_decoder=_proto_dec,
            )

        if is_cls and hasattr(target_type, "__attrs_attrs__"):
            try:
                json_dec = msgspec.json.Decoder(target_type).decode
                mp_dec = msgspec.msgpack.Decoder(target_type).decode
            except TypeError:
                raw_j = msgspec.json.decode
                raw_m = msgspec.msgpack.decode

                def json_dec(b: Any, _t: Any = target_type) -> Any:
                    return hydrate_one_vectorcall(_t, raw_j(b))

                def mp_dec(b: Any, _t: Any = target_type) -> Any:
                    return hydrate_one_vectorcall(_t, raw_m(b))

            def _attrs_enc_hook(obj: Any) -> Any:
                if hasattr(obj, "__attrs_attrs__"):
                    return {a.name: getattr(obj, a.name) for a in obj.__attrs_attrs__}
                return _default_zero_copy_enc_hook(obj)

            json_enc = msgspec.json.Encoder(enc_hook=_attrs_enc_hook).encode
            mp_enc = msgspec.msgpack.Encoder(enc_hook=_attrs_enc_hook).encode
            return TypeAdapterPlan(
                target_type=target_type,
                kind="attrs",
                json_decoder=json_dec,
                msgpack_decoder=mp_dec,
                json_encoder=json_enc,
                msgpack_encoder=mp_enc,
            )

        if is_cls and dataclasses.is_dataclass(target_type):
            json_dec = msgspec.json.Decoder(target_type).decode
            mp_dec = msgspec.msgpack.Decoder(target_type).decode
            json_enc = msgspec.json.Encoder(enc_hook=_default_zero_copy_enc_hook).encode
            mp_enc = msgspec.msgpack.Encoder(enc_hook=_default_zero_copy_enc_hook).encode
            return TypeAdapterPlan(
                target_type=target_type,
                kind="dataclass",
                json_decoder=json_dec,
                msgpack_decoder=mp_dec,
                json_encoder=json_enc,
                msgpack_encoder=mp_enc,
            )

        if target_type is Any:
            return TypeAdapterPlan(
                target_type=Any,
                kind="generic",
                json_decoder=msgspec.json.decode,
                msgpack_decoder=msgspec.msgpack.decode,
                json_encoder=msgspec.json.Encoder(enc_hook=_default_zero_copy_enc_hook).encode,
                msgpack_encoder=msgspec.msgpack.Encoder(enc_hook=_default_zero_copy_enc_hook).encode,
            )

        try:
            json_dec = msgspec.json.Decoder(target_type).decode
            mp_dec = msgspec.msgpack.Decoder(target_type).decode
        except TypeError:

            def json_dec(b: Any, _t: Any = target_type) -> Any:
                return msgspec.convert(msgspec.json.decode(b), type=_t)

            def mp_dec(b: Any, _t: Any = target_type) -> Any:
                return msgspec.convert(msgspec.msgpack.decode(b), type=_t)

        return TypeAdapterPlan(
            target_type=target_type,
            kind="generic",
            json_decoder=json_dec,
            msgpack_decoder=mp_dec,
            json_encoder=msgspec.json.Encoder(enc_hook=_default_zero_copy_enc_hook).encode,
            msgpack_encoder=msgspec.msgpack.Encoder(enc_hook=_default_zero_copy_enc_hook).encode,
        )
