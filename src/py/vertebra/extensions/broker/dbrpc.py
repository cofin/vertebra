"""Versioned binary DBRPC header codec and acyclic msgspec request/response frames."""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Any, Final, cast

import msgspec

from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes

__all__ = (
    "DBRPC_HEADER_SIZE",
    "DBRPC_HEADER_STRUCT",
    "DBRPC_VERSION",
    "DbRpcHeader",
    "DbRpcOp",
    "DbRpcRequest",
    "DbRpcResponse",
    "decode_dbrpc_request",
    "decode_dbrpc_response",
    "encode_dbrpc_request",
    "encode_dbrpc_response",
)

DBRPC_VERSION: Final[int] = 1
DBRPC_HEADER_STRUCT: Final[struct.Struct] = struct.Struct("<HBQIQ")
DBRPC_HEADER_SIZE: Final[int] = DBRPC_HEADER_STRUCT.size


class DbRpcOp(IntEnum):
    """Wire operation discriminants matching ``vertebra_core::dbrpc::DbRpcOp``."""

    EXECUTE = 1
    EXECUTE_MANY = 2
    BEGIN = 3
    COMMIT = 4
    ROLLBACK = 5
    SAVEPOINT = 6
    FETCH_CHUNK = 7
    CANCEL = 8
    CLOSE = 9


class DbRpcHeader(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Fixed 23-byte little-endian DBRPC header matching ``vertebra_core::dbrpc::DbRpcHeader``."""

    version: int
    op: int
    request_id: int
    lease_slot: int
    lease_generation: int

    def encode(self) -> bytes:
        """Encode the header into a 23-byte little-endian buffer."""
        if self.version != DBRPC_VERSION:
            raise ValueError(f"unsupported dbrpc version: {self.version}")
        op_code = DbRpcOp(self.op)
        return DBRPC_HEADER_STRUCT.pack(
            self.version,
            int(op_code),
            self.request_id,
            self.lease_slot,
            self.lease_generation,
        )

    @classmethod
    def decode(cls, data: bytes | bytearray | memoryview) -> DbRpcHeader:
        """Decode and validate a 23-byte little-endian header prefix."""
        view = memoryview(data)
        if len(view) < DBRPC_HEADER_SIZE:
            raise ValueError(f"truncated dbrpc header: expected {DBRPC_HEADER_SIZE} bytes, got {len(view)}")
        version, raw_op, request_id, lease_slot, lease_generation = DBRPC_HEADER_STRUCT.unpack_from(view, 0)
        if version != DBRPC_VERSION:
            raise ValueError(f"unsupported dbrpc version: {version}")
        op_code = DbRpcOp(raw_op)
        return cls(
            version=version,
            op=int(op_code),
            request_id=request_id,
            lease_slot=lease_slot,
            lease_generation=lease_generation,
        )


class DbRpcRequest(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Acyclic immutable DBRPC request frame carried across shared-memory rings."""

    version: int
    op: int
    request_id: int
    lease_slot: int
    lease_generation: int
    sql: str | None = None
    params: tuple[Any, ...] | None = None
    savepoint: str | None = None
    chunk_rows: int = 0

    def header(self) -> DbRpcHeader:
        """Construct the binary :class:`DbRpcHeader` corresponding to this request."""
        return DbRpcHeader(
            version=self.version,
            op=self.op,
            request_id=self.request_id,
            lease_slot=self.lease_slot,
            lease_generation=self.lease_generation,
        )


class DbRpcResponse(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Acyclic immutable DBRPC response frame carried across shared-memory rings."""

    request_id: int
    status: int
    rowcount: int = -1
    columns: tuple[str, ...] | None = None
    rows: tuple[tuple[Any, ...], ...] | None = None
    arrow_shm: str | None = None
    lastrowid: int | None = None
    error_class: str | None = None
    error_message: str | None = None


def encode_dbrpc_request(request: DbRpcRequest) -> bytes:
    """Serialize a :class:`DbRpcRequest` prefixed by its 23-byte binary header."""
    return request.header().encode() + encode_msgpack_bytes(request)


def decode_dbrpc_request(frame: bytes | bytearray | memoryview) -> tuple[DbRpcHeader, DbRpcRequest]:
    """Decode and validate a binary DBRPC request frame into ``(header, request)``."""
    view = memoryview(frame)
    hdr = DbRpcHeader.decode(view[:DBRPC_HEADER_SIZE])
    req = cast("DbRpcRequest", decode_msgpack(view[DBRPC_HEADER_SIZE:], type=DbRpcRequest))
    if req.version != hdr.version or req.op != hdr.op or req.request_id != hdr.request_id:
        raise ValueError("dbrpc header metadata does not match body payload")
    return hdr, req


def encode_dbrpc_response(response: DbRpcResponse) -> bytes:
    """Serialize a :class:`DbRpcResponse` payload."""
    return encode_msgpack_bytes(response)


def decode_dbrpc_response(frame: bytes | bytearray | memoryview) -> DbRpcResponse:
    """Deserialize a :class:`DbRpcResponse` payload."""
    return cast("DbRpcResponse", decode_msgpack(frame, type=DbRpcResponse))
