"""High-level IPC channel wrappers."""

from __future__ import annotations

import os
import struct
from typing import Any, Self, TypeVar, cast

from vertebra import _vertebra
from vertebra.core.exceptions import ChannelError, SerializationError
from vertebra.core.serializers import get_json_codec
from vertebra.messaging.protocol import HEADER_SIZE, Flags, RequestHeader, ResponseHeader
from vertebra.core.procedures import ProcedureRegistry, get_serializer

# Default spin duration in microseconds for hybrid wait.
# Read from VERTEBRA_SPIN_US environment variable (default: 5).
# Lower values (2-3) = lower latency, higher CPU
# Higher values (10+) = higher latency, lower CPU
DEFAULT_SPIN_US: int = int(os.environ.get("VERTEBRA_SPIN_US", "5"))

# Procedure ID encoding: length (u16) + bytes
_PROC_LEN = struct.Struct("<H")  # Little-endian u16
_MAX_PROC_LEN = 0xFFFF

T = TypeVar("T")


def encode_request(procedure_id: str, payload: bytes, *, correlation_id: int = 0, flags: int = 0) -> bytes:
    """Encode a request message.

    Wire format:
        [RequestHeader 16 bytes][procedure_id_len u16][procedure_id][payload]
    """
    proc_bytes = procedure_id.encode("utf-8")
    if len(proc_bytes) > _MAX_PROC_LEN:
        msg = "procedure_id too long"
        raise ChannelError(msg)
    body = _PROC_LEN.pack(len(proc_bytes)) + proc_bytes + payload
    header = RequestHeader(id=correlation_id, flags=flags, payload_len=len(body))
    return header.to_bytes() + body


def decode_request(data: bytes | memoryview) -> tuple[int, int, str, bytes]:
    """Decode a request message.

    Returns: (correlation_id, flags, procedure_id, payload)
    """
    if len(data) < HEADER_SIZE + _PROC_LEN.size:
        msg = f"request too small: got {len(data)} bytes"
        raise ChannelError(msg)
    header = RequestHeader.from_bytes(data[:HEADER_SIZE])
    body = data[HEADER_SIZE:]
    proc_len = _PROC_LEN.unpack_from(body)[0]
    if len(body) < _PROC_LEN.size + proc_len:
        msg = "request truncated"
        raise ChannelError(msg)
    procedure_id = bytes(body[_PROC_LEN.size : _PROC_LEN.size + proc_len]).decode("utf-8")
    payload = bytes(body[_PROC_LEN.size + proc_len :])
    return header.id, header.flags, procedure_id, payload


def encode_response(payload: bytes, *, correlation_id: int = 0, flags: int = 0) -> bytes:
    """Encode a response message.

    Wire format: [ResponseHeader 16 bytes][payload]
    """
    header = ResponseHeader(id=correlation_id, flags=flags, payload_len=len(payload))
    return header.to_bytes() + payload


def decode_response(data: bytes | memoryview) -> tuple[int, int, bytes]:
    """Decode a response message.

    Returns: (correlation_id, flags, payload)
    """
    if len(data) < HEADER_SIZE:
        msg = f"response too small: got {len(data)} bytes"
        raise ChannelError(msg)
    header = ResponseHeader.from_bytes(data[:HEADER_SIZE])
    payload = bytes(data[HEADER_SIZE:])
    return header.id, header.flags, payload


class BufferView:
    """Zero-copy view into a shared buffer."""

    def __init__(self, buffer: _vertebra.Buffer, length: int, flags: int = 0) -> None:
        self._buffer: _vertebra.Buffer | None = buffer
        self._length = length
        self._flags = flags
        self._view: Any = None

    @property
    def length(self) -> int:
        return self._length

    @property
    def flags(self) -> int:
        return self._flags

    def as_memoryview(self) -> memoryview:
        """Get a zero-copy memoryview into the buffer.

        Raises:
            ChannelError: If the buffer is released, doesn't support memoryview,
                          or only provides a read-only view.
        """
        if self._buffer is None:
            msg = "buffer released"
            raise ChannelError(msg)
        try:
            # Cast to Any because _vertebra.Buffer implements the buffer protocol but mypy doesn't know
            view = memoryview(cast("Any", self._buffer))[: self._length]
        except TypeError as exc:
            # Buffer doesn't support the buffer protocol - fail fast
            msg = "buffer does not support memoryview; zero-copy requires buffer protocol support"
            raise ChannelError(msg) from exc
        if view.readonly:
            # Read-only buffers can't be used for zero-copy writes
            msg = "buffer is read-only; zero-copy requires writable buffers"
            raise ChannelError(msg)
        self._view = view
        return view

    def as_bytes(self) -> bytes:
        view = self.as_memoryview()
        if hasattr(view, "tobytes"):
            return view.tobytes()
        return bytes(view)

    def as_msgspec(self, type_hint: type[T]) -> T:
        try:
            codec = get_json_codec()
        except Exception as exc:  # pragma: no cover - optional dependency
            msg = "msgspec is not available"
            raise SerializationError(msg) from exc

        view = self.as_memoryview()
        try:
            return codec.decode(view, type_hint=type_hint)
        except TypeError:
            # Fallback for buffers that don't support direct view decoding
            return codec.decode(self.as_bytes(), type_hint=type_hint)

    def release(self) -> None:
        self._release_view()
        self._buffer = None

    def _release_view(self) -> None:
        view = self._view
        if view is None:
            return
        if hasattr(view, "release"):
            view.release()
        self._view = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


class ZeroCopyChannel:
    """BufferChannel wrapper that returns BufferView objects."""

    def __init__(self, buffer_size: int, count: int, slots: int) -> None:
        self._channel = _vertebra.BufferChannel(buffer_size, count, slots)

    def acquire(self) -> BufferView | None:
        buffer = self._channel.acquire()
        if buffer is None:
            return None
        return BufferView(buffer, buffer.len(), 0)

    def send(self, buffer: BufferView, length: int, flags: int = 0) -> None:
        if buffer._buffer is None:
            msg = "buffer released"
            raise ChannelError(msg)
        try:
            buffer._release_view()
            self._channel.send(buffer._buffer, length, flags)
        except RuntimeError as exc:
            raise ChannelError(str(exc)) from exc
        buffer.release()

    def recv(self) -> BufferView:
        buffer, length, flags = self._channel.recv()
        return BufferView(buffer, length, flags)


class Channel:
    """High-level request/response channel."""

    def __init__(self, handle: _vertebra.Channel, max_payload: int | None = None) -> None:
        self._handle: _vertebra.Channel | None = handle
        self._max_payload = max_payload or handle.payload_capacity()
        self._seq = 0  # Correlation ID counter

    def _next_seq(self) -> int:
        """Generate next correlation ID."""
        seq = self._seq
        self._seq = (self._seq + 1) & 0xFFFFFFFFFFFFFFFF  # Wrap at u64 max
        return seq

    @property
    def payload_capacity(self) -> int:
        self._require_handle()
        return self._max_payload

    @classmethod
    def create_pair(cls, slot_size: int = 4096, slots: int = 64) -> tuple["Channel", "Channel"]:
        client, server = _vertebra.Channel.create_pair(slot_size, slots)
        return cls(client), cls(server)

    @classmethod
    def create_named(
        cls, channel_id: str, *, slot_size: int = 4096, slots: int = 64, role: str = "client"
    ) -> "Channel":
        handle = _vertebra.Channel.create_named(channel_id, slot_size, slots, role)
        return cls(handle)

    @classmethod
    def open_named(cls, channel_id: str, *, slot_size: int = 4096, slots: int = 64, role: str = "client") -> "Channel":
        handle = _vertebra.Channel.open_named(channel_id, slot_size, slots, role)
        return cls(handle)

    def close(self) -> None:
        """Close the channel handle (idempotent)."""
        self._handle = None

    def _require_handle(self) -> _vertebra.Channel:
        handle = self._handle
        if handle is None:
            msg = "channel is closed"
            raise ChannelError(msg, stage="closed")
        return handle

    def _do_send_recv(
        self, handle: _vertebra.Channel, message: bytes, timeout_ms: int | None, procedure_id: str
    ) -> bytes:
        """Execute send/receive with native timeout methods.

        Requires native timeout methods on the channel handle. Raises ChannelError
        if native timeout methods are not available.
        """
        if timeout_ms is None:
            handle.send_request(message)
            return handle.recv_response(self._max_payload)

        if not hasattr(handle, "send_request_timeout") or not hasattr(handle, "recv_response_timeout"):
            msg = "Native timeout methods required. Polling fallback has been removed."
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_recv")

        if not handle.send_request_timeout(message, timeout_ms):
            msg = "send timeout"
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_request")
        result = handle.recv_response_timeout(self._max_payload, timeout_ms)
        if result is None:
            msg = "response timeout"
            raise ChannelError(msg, procedure_id=procedure_id, stage="recv_response")
        return result

    def send_sync(
        self,
        procedure_id: str,
        params: dict[str, Any],
        *,
        timeout_ms: int | None = None,
        response_type: type | None = None,
        serializer: str | None = None,
    ) -> Any:
        """Send a request and wait for response synchronously."""
        handle = self._require_handle()
        correlation_id = self._next_seq()

        # Serialize params
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=response_type)
        try:
            payload = serializer_impl.serialize(params, dict)
        except Exception as exc:
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="serialize_params") from exc

        # Encode request with protocol header
        try:
            message = encode_request(procedure_id, payload, correlation_id=correlation_id)
        except ChannelError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="encode_request") from exc

        if len(message) > self._max_payload:
            msg = "payload too large"
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_request")

        # Send and receive
        try:
            response_data = self._do_send_recv(handle, message, timeout_ms, procedure_id)
        except RuntimeError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="ipc") from exc

        # Decode response
        _resp_id, resp_flags, resp_payload = decode_response(cast("bytes", response_data))

        # Check response flags for errors
        if resp_flags & Flags.ERROR:
            msg = "remote error"
            raise ChannelError(msg, procedure_id=procedure_id, stage="response")

        try:
            return serializer_impl.deserialize(resp_payload, response_type)
        except Exception as exc:
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="deserialize_response") from exc

    def receive_sync(
        self, *, timeout_ms: int | None = None, params_type: type | None = None, serializer: str | None = None
    ) -> tuple[str, Any]:
        """Receive a request synchronously."""
        handle = self._require_handle()

        # Receive raw message (requires native timeout methods)
        try:
            if timeout_ms is None:
                data = handle.recv_request(self._max_payload)
            elif hasattr(handle, "recv_request_timeout"):
                result = handle.recv_request_timeout(self._max_payload, timeout_ms)
                if result is None:
                    msg = "receive timeout"
                    raise ChannelError(msg, stage="recv_request")
                data = result
            else:
                msg = "Native timeout methods required. Polling fallback has been removed."
                raise ChannelError(msg, stage="recv_request")
        except RuntimeError as exc:
            raise ChannelError(str(exc), stage="recv_request") from exc

        # Decode request with protocol header
        try:
            correlation_id, _flags, procedure_id, payload = decode_request(data)
        except ChannelError as exc:
            raise ChannelError(str(exc), stage="decode_request") from exc

        # Store correlation ID for response (will be used by respond_sync)
        self._last_correlation_id = correlation_id

        # Deserialize params
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=params_type)
        try:
            params = serializer_impl.deserialize(payload, params_type)
        except Exception as exc:
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="deserialize_params") from exc

        return procedure_id, params

    def respond_sync(
        self, procedure_id: str, result: Any, *, response_type: type | None = None, serializer: str | None = None
    ) -> None:
        """Send a response synchronously."""
        handle = self._require_handle()
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=response_type)
        try:
            payload = serializer_impl.serialize(result, response_type)
        except Exception as exc:  # pragma: no cover - serializer dependent
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="serialize_response") from exc

        # Encode response with protocol header using stored correlation ID
        correlation_id = getattr(self, "_last_correlation_id", 0)
        try:
            message = encode_response(payload, correlation_id=correlation_id, flags=Flags.OK)
        except ChannelError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="encode_response") from exc

        if len(message) > self._max_payload:
            msg = "payload too large"
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_response")
        try:
            handle.send_response(message)
        except RuntimeError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="send_response") from exc
