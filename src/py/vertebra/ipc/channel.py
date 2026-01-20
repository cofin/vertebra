"""High-level IPC channel wrappers."""

from __future__ import annotations

import asyncio
import struct
import time
from typing import Any, cast

from vertebra import _vertebra
from vertebra.procedures import ProcedureRegistry, get_serializer

_HEADER = struct.Struct("!HI")  # procedure length (u16), payload length (u32)
_MAX_NAME_LEN = 0xFFFF


class ChannelError(RuntimeError):
    """Raised when channel operations fail."""

    def __init__(self, message: str, *, procedure_id: str | None = None, stage: str | None = None) -> None:
        super().__init__(_format_context(message, procedure_id, stage))
        self.procedure_id = procedure_id
        self.stage = stage


class SerializationError(RuntimeError):
    """Raised when serialization or deserialization fails."""

    def __init__(self, message: str, *, procedure_id: str | None = None, stage: str | None = None) -> None:
        super().__init__(_format_context(message, procedure_id, stage))
        self.procedure_id = procedure_id
        self.stage = stage


def _format_context(message: str, procedure_id: str | None, stage: str | None) -> str:
    context: list[str] = []
    if stage:
        context.append(f"stage={stage}")
    if procedure_id:
        context.append(f"procedure_id={procedure_id}")
    if not context:
        return message
    return f"{message} ({', '.join(context)})"


def _encode_message(procedure_id: str, payload: bytes) -> bytes:
    encoded = procedure_id.encode("utf-8")
    if len(encoded) > _MAX_NAME_LEN:
        msg = "procedure_id too long"
        raise ChannelError(msg)
    return _HEADER.pack(len(encoded), len(payload)) + encoded + payload


def _decode_message(data: bytes) -> tuple[str, bytes]:
    if len(data) < _HEADER.size:
        msg = "payload too small"
        raise ChannelError(msg)
    name_len, payload_len = _HEADER.unpack_from(data)
    start = _HEADER.size
    end = start + name_len
    if len(data) < end + payload_len:
        msg = "payload truncated"
        raise ChannelError(msg)
    procedure_id = data[start:end].decode("utf-8")
    payload = data[end : end + payload_len]
    return procedure_id, payload


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
        if self._buffer is None:
            msg = "buffer released"
            raise ChannelError(msg)
        try:
            # Cast to Any because _vertebra.Buffer implements the buffer protocol but mypy doesn't know
            view = memoryview(cast("Any", self._buffer))[: self._length]
        except TypeError:
            view_proxy = _BufferProxy(self._buffer, self._length)
            self._view = view_proxy
            return cast("memoryview", view_proxy)
        if view.readonly:
            view_proxy = _BufferProxy(self._buffer, self._length)
            self._view = view_proxy
            return cast("memoryview", view_proxy)
        self._view = view
        return view

    def as_bytes(self) -> bytes:
        view = self.as_memoryview()
        if hasattr(view, "tobytes"):
            return view.tobytes()
        return bytes(view)

    def as_msgspec(self, type_hint: type) -> Any:
        try:
            import msgspec
        except Exception as exc:  # pragma: no cover - optional dependency
            msg = "msgspec is not available"
            raise SerializationError(msg) from exc
        decoder: msgspec.json.Decoder[Any] = msgspec.json.Decoder(type_hint)
        view = self.as_memoryview()
        try:
            return decoder.decode(view)
        except TypeError:
            return decoder.decode(self.as_bytes())

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

    def __enter__(self) -> "BufferView":
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


class _BufferProxy:
    """Mutable fallback view for buffers without writable memoryview support."""

    def __init__(self, buffer: _vertebra.Buffer, length: int) -> None:
        self._buffer = buffer
        self._length = length
        payload = buffer.read()
        self._data = bytearray(payload[:length])

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, key: int | slice) -> Any:
        return self._data[key]

    def __setitem__(self, key: int | slice, value: Any) -> None:
        self._data[key] = value
        self._buffer.write(bytes(self._data))

    def __bytes__(self) -> bytes:
        return bytes(self._data)

    def tobytes(self) -> bytes:
        return bytes(self._data)


class Channel:
    """High-level request/response channel."""

    def __init__(self, handle: _vertebra.Channel, max_payload: int | None = None) -> None:
        self._handle: _vertebra.Channel | None = handle
        self._max_payload = max_payload or handle.payload_capacity()

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

    def send_sync(  # noqa: PLR0912
        self,
        procedure_id: str,
        params: dict[str, Any],
        *,
        timeout_ms: int | None = None,
        response_type: type | None = None,
        serializer: str | None = None,
    ) -> Any:
        handle = self._require_handle()
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=response_type)
        try:
            payload = serializer_impl.serialize(params, dict)
        except Exception as exc:  # pragma: no cover - serializer dependent
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="serialize_params") from exc
        try:
            message = _encode_message(procedure_id, payload)
        except ChannelError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="encode_message") from exc
        if len(message) > self._max_payload:
            msg = "payload too large"
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_request")
        try:
            supports_try = hasattr(handle, "try_send_request") and hasattr(handle, "try_recv_response")
            if timeout_ms is None or not supports_try:
                handle.send_request(message)
                response = handle.recv_response(self._max_payload)
            else:
                send_deadline = time.monotonic() + (timeout_ms / 1000.0)
                while True:
                    if handle.try_send_request(message):
                        break
                    if time.monotonic() >= send_deadline:
                        msg = "send timeout"
                        raise ChannelError(msg, procedure_id=procedure_id, stage="send_request")
                    time.sleep(0.001)
                recv_deadline = time.monotonic() + (timeout_ms / 1000.0)
                response = None
                while response is None:
                    response = handle.try_recv_response(self._max_payload)
                    if response is not None:
                        break
                    if time.monotonic() >= recv_deadline:
                        msg = "response timeout"
                        raise ChannelError(msg, procedure_id=procedure_id, stage="recv_response")
                    time.sleep(0.001)
        except RuntimeError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="ipc") from exc
        try:
            return serializer_impl.deserialize(cast("bytes", response), response_type)
        except Exception as exc:  # pragma: no cover - serializer dependent
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="deserialize_response") from exc

    async def send(
        self,
        procedure_id: str,
        params: dict[str, Any],
        *,
        timeout_ms: int | None = None,
        response_type: type | None = None,
        serializer: str | None = None,
    ) -> Any:
        return await asyncio.to_thread(
            self.send_sync,
            procedure_id,
            params,
            timeout_ms=timeout_ms,
            response_type=response_type,
            serializer=serializer,
        )

    def receive_sync(
        self, *, timeout_ms: int | None = None, params_type: type | None = None, serializer: str | None = None
    ) -> tuple[str, Any]:
        handle = self._require_handle()
        try:
            supports_try = hasattr(handle, "try_recv_request")
            if timeout_ms is None or not supports_try:
                data = handle.recv_request(self._max_payload)
            else:
                deadline = time.monotonic() + (timeout_ms / 1000.0)
                data = None
                while data is None:
                    data = handle.try_recv_request(self._max_payload)
                    if data is not None:
                        break
                    if time.monotonic() >= deadline:
                        msg = "receive timeout"
                        raise ChannelError(msg, stage="recv_request")
                    time.sleep(0.001)
        except RuntimeError as exc:
            raise ChannelError(str(exc), stage="recv_request") from exc
        assert data is not None  # noqa: S101  # Guaranteed by the loop logic above
        try:
            procedure_id, payload = _decode_message(data)
        except ChannelError as exc:
            raise ChannelError(str(exc), stage="decode_request") from exc
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=params_type)
        try:
            params = serializer_impl.deserialize(payload, params_type)
        except Exception as exc:  # pragma: no cover - serializer dependent
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="deserialize_params") from exc
        return procedure_id, params

    async def receive(
        self, *, timeout_ms: int | None = None, params_type: type | None = None, serializer: str | None = None
    ) -> tuple[str, Any]:
        return await asyncio.to_thread(
            self.receive_sync, timeout_ms=timeout_ms, params_type=params_type, serializer=serializer
        )

    def respond_sync(
        self, procedure_id: str, result: Any, *, response_type: type | None = None, serializer: str | None = None
    ) -> None:
        handle = self._require_handle()
        registry = ProcedureRegistry.instance()
        proc = registry.get(procedure_id)
        serializer_impl = get_serializer(serializer or (proc.serializer if proc else None), type_hint=response_type)
        try:
            payload = serializer_impl.serialize(result, response_type)
        except Exception as exc:  # pragma: no cover - serializer dependent
            raise SerializationError(str(exc), procedure_id=procedure_id, stage="serialize_response") from exc
        if len(payload) > self._max_payload:
            msg = "payload too large"
            raise ChannelError(msg, procedure_id=procedure_id, stage="send_response")
        try:
            handle.send_response(payload)
        except RuntimeError as exc:
            raise ChannelError(str(exc), procedure_id=procedure_id, stage="send_response") from exc

    async def respond(
        self, procedure_id: str, result: Any, *, response_type: type | None = None, serializer: str | None = None
    ) -> None:
        await asyncio.to_thread(
            self.respond_sync, procedure_id, result, response_type=response_type, serializer=serializer
        )
