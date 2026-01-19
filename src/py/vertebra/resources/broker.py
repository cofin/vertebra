"""Shared-memory broker client for resource management."""

from __future__ import annotations

import asyncio
import os
import pathlib
import uuid
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from vertebra._internal.async_transport import AsyncShmTransport

from vertebra._internal.serialization import MessagePackCodec

# Try to import the new async transport (PyO3-based)
_USE_ASYNC_TRANSPORT = False
_AsyncRing: type | None = None
_AsyncShmTransport: type | None = None
_create_transport: Any = None

try:
    from vertebra._internal.async_transport import AsyncShmTransport as _AsyncShmTransportImport
    from vertebra._internal.async_transport import create_transport as _create_transport_import
    from vertebra._vertebra import AsyncRing as _AsyncRingImport

    _AsyncRing = _AsyncRingImport
    _AsyncShmTransport = _AsyncShmTransportImport
    _create_transport = _create_transport_import
    _USE_ASYNC_TRANSPORT = True
except ImportError:
    pass

# Fallback to CFFI-based transport for backwards compatibility
_ShmRing: type | None = None
_ShmRegion: type | None = None
_ShmRingConfig: type | None = None
_ShmRegionConfig: type | None = None

try:
    from vertebra._internal.shm import ShmRegion as _ShmRegionImport
    from vertebra._internal.shm import ShmRegionConfig as _ShmRegionConfigImport
    from vertebra._internal.shm import ShmRing as _ShmRingImport
    from vertebra._internal.shm import ShmRingConfig as _ShmRingConfigImport

    _ShmRing = _ShmRingImport
    _ShmRegion = _ShmRegionImport
    _ShmRingConfig = _ShmRingConfigImport
    _ShmRegionConfig = _ShmRegionConfigImport
except ImportError:
    pass


class BrokerClientError(RuntimeError):
    """Raised for broker IPC errors."""


class RingConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for a ring buffer (transport-agnostic)."""

    name: str
    slot_size: int = 65536
    slots: int = 1024


class BrokerClientConfig(msgspec.Struct, frozen=True):
    """Configuration for connecting to the shared-memory broker."""

    name: str = "vertebra-broker"
    control_name: str | None = None
    lock_path: str | None = None
    slot_size: int = 65536
    slots: int = 1024
    timeout_ms: int = 5_000
    client_id: str | None = None
    weight: int = 1

    def control_ring(self) -> RingConfig:
        name = self.control_name or f"/{self.name}.control"
        return RingConfig(name=name, slot_size=self.slot_size, slots=self.slots)

    def request_ring(self, client_id: str) -> RingConfig:
        return RingConfig(name=f"/{self.name}.client.{client_id}.req", slot_size=self.slot_size, slots=self.slots)

    def response_ring(self, client_id: str) -> RingConfig:
        return RingConfig(name=f"/{self.name}.client.{client_id}.resp", slot_size=self.slot_size, slots=self.slots)

    def control_lock(self) -> str:
        import tempfile

        if self.lock_path:
            return self.lock_path
        return f"{tempfile.gettempdir()}/{self.name}.control.lock"


class BrokerClient:
    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        self._config = config or BrokerClientConfig()
        self._codec = MessagePackCodec()
        self._client_id = self._config.client_id or _default_client_id()
        self._seq = 0
        self._use_async_transport = _USE_ASYNC_TRANSPORT

        # Ring buffers (either CFFI or PyO3-based)
        self._control: Any = None
        self._request: Any = None
        self._response: Any = None

        # Async transport wrappers (only used with PyO3)
        self._request_transport: AsyncShmTransport | None = None
        self._response_transport: AsyncShmTransport | None = None

    async def connect(self) -> None:
        if self._control is not None:
            return

        if self._use_async_transport:
            await self._connect_async()
        else:
            await self._connect_cffi()

    async def _connect_async(self) -> None:
        """Connect using the PyO3-based async transport."""
        if _AsyncRing is None or _create_transport is None:
            msg = "Async transport not available"
            raise BrokerClientError(msg)

        # Create rings using the new AsyncRing from PyO3
        control_cfg = self._config.control_ring()
        req_cfg = self._config.request_ring(self._client_id)
        resp_cfg = self._config.response_ring(self._client_id)

        self._control = _AsyncRing.open_named(control_cfg.name, control_cfg.slot_size, control_cfg.slots)
        self._request = _AsyncRing.create_named(req_cfg.name, req_cfg.slot_size, req_cfg.slots)
        self._response = _AsyncRing.create_named(resp_cfg.name, resp_cfg.slot_size, resp_cfg.slots)

        # Wrap in async transports for efficient waiting
        self._request_transport = _create_transport(self._request)
        self._response_transport = _create_transport(self._response)

        # Register with broker
        payload = {
            "id": self._next_id(),
            "kind": "register_client",
            "client_id": self._client_id,
            "req_ring": req_cfg.name,
            "resp_ring": resp_cfg.name,
            "weight": self._config.weight,
        }

        # Send registration to control ring (blocking is OK for control plane)
        encoded = self._codec.encode(payload)
        _with_lock(self._config.control_lock(), lambda: self._control.send(encoded, 0))

        # Wait for response using the async transport
        response = await self._recv_response_async()
        _assert_ok(response)

    async def _connect_cffi(self) -> None:
        """Connect using the legacy CFFI-based transport."""
        if _ShmRing is None or _ShmRingConfig is None:
            msg = "CFFI transport not available"
            raise BrokerClientError(msg)

        control_cfg = self._config.control_ring()
        req_cfg = self._config.request_ring(self._client_id)
        resp_cfg = self._config.response_ring(self._client_id)

        self._control = _ShmRing.open(
            _ShmRingConfig(name=control_cfg.name, slot_size=control_cfg.slot_size, slots=control_cfg.slots)
        )
        self._request = _ShmRing.create(
            _ShmRingConfig(name=req_cfg.name, slot_size=req_cfg.slot_size, slots=req_cfg.slots)
        )
        self._response = _ShmRing.create(
            _ShmRingConfig(name=resp_cfg.name, slot_size=resp_cfg.slot_size, slots=resp_cfg.slots)
        )
        payload = {
            "id": self._next_id(),
            "kind": "register_client",
            "client_id": self._client_id,
            "req_ring": req_cfg.name,
            "resp_ring": resp_cfg.name,
            "weight": self._config.weight,
        }
        await asyncio.to_thread(self._send_control, payload)
        response = await self._recv_response()
        _assert_ok(response)

    async def close(self) -> None:
        if self._control is None:
            return

        # Clean up async transports
        if self._request_transport:
            self._request_transport.close()
            self._request_transport = None
        if self._response_transport:
            self._response_transport.close()
            self._response_transport = None

        # Clean up rings (CFFI rings have close(), PyO3 rings don't need explicit cleanup)
        if self._request and hasattr(self._request, "close"):
            self._request.close()
        if self._response and hasattr(self._response, "close"):
            self._response.close()

        self._control = None
        self._request = None
        self._response = None

    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._request is None or self._response is None:
            await self.connect()
        if self._request is None or self._response is None:
            msg = "Broker shared memory unavailable"
            raise BrokerClientError(msg)

        payload = dict(payload)
        payload["id"] = self._next_id()
        encoded = self._codec.encode(payload)

        if self._use_async_transport and self._request_transport:
            # Use async transport - no thread pool overhead
            await self._request_transport.send(encoded)
            return await self._recv_response_async()
        # Legacy CFFI path
        await asyncio.to_thread(self._request.send, encoded, 0, self._config.timeout_ms)
        return await self._recv_response()

    async def acquire(self, pool: str) -> str:
        response = await self.request({"kind": "acquire", "pool": pool, "timeout_ms": self._config.timeout_ms})
        data = _assert_ok(response)
        return str(data.get("token"))

    async def release(self, pool: str, token: str) -> bool:
        response = await self.request({"kind": "release", "pool": pool, "token": token})
        data = _assert_ok(response)
        return bool(data.get("ok", True))

    async def health(self, pool: str) -> bool:
        response = await self.request({"kind": "health", "pool": pool})
        data = _assert_ok(response)
        return bool(data.get("ok", True))

    async def execute(
        self,
        pool: str,
        token: str,
        sql: str,
        params: list[Any] | dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> Any:
        payload = {
            "kind": "execute",
            "pool": pool,
            "token": token,
            "sql": sql,
            "params": params if params is not None else [],
        }
        if correlation_id:
            payload["correlation_id"] = correlation_id
        response = await self.request(payload)
        return await self._decode_result(response)

    async def execute_many(
        self,
        pool: str,
        token: str,
        sql: str,
        params: list[list[Any] | dict[str, Any]],
        correlation_id: str | None = None,
    ) -> Any:
        payload = {"kind": "execute_many", "pool": pool, "token": token, "sql": sql, "params": params}
        if correlation_id:
            payload["correlation_id"] = correlation_id
        response = await self.request(payload)
        return await self._decode_result(response)

    async def command(
        self, pool: str, token: str, command: str, args: list[Any] | None = None, correlation_id: str | None = None
    ) -> Any:
        payload = {"kind": "command", "pool": pool, "token": token, "command": command, "args": args or []}
        if correlation_id:
            payload["correlation_id"] = correlation_id
        response = await self.request(payload)
        return await self._decode_result(response)

    async def begin(self, pool: str, token: str) -> Any:
        response = await self.request({"kind": "begin", "pool": pool, "token": token})
        return await self._decode_result(response)

    async def commit(self, pool: str, token: str) -> Any:
        response = await self.request({"kind": "commit", "pool": pool, "token": token})
        return await self._decode_result(response)

    async def rollback(self, pool: str, token: str) -> Any:
        response = await self.request({"kind": "rollback", "pool": pool, "token": token})
        return await self._decode_result(response)

    async def stats(self, pool: str | None = None) -> dict[str, Any]:
        response = await self.request({"kind": "stats", "pool": pool})
        data = _assert_ok(response)
        stats = data.get("stats")
        return stats if isinstance(stats, dict) else {}

    async def register_provider(self, name: str, driver: str, max_size: int, *, req_ring: str, resp_ring: str) -> bool:
        response = await self.request({
            "kind": "register_provider",
            "pool": name,
            "driver": driver,
            "max_size": max_size,
            "req_ring": req_ring,
            "resp_ring": resp_ring,
            "client_id": self._client_id,
        })
        data = _assert_ok(response)
        return bool(data.get("ok", True))

    def _send_control(self, payload: dict[str, Any]) -> None:
        if self._control is None:
            msg = "Control ring unavailable"
            raise BrokerClientError(msg)
        encoded = self._codec.encode(payload)
        _with_lock(self._config.control_lock(), lambda: self._control.send(encoded, 0, self._config.timeout_ms))

    async def _recv_response(self) -> dict[str, Any]:
        """Receive response using legacy CFFI transport."""
        if self._response is None:
            msg = "Response ring unavailable"
            raise BrokerClientError(msg)
        data, _flags = await asyncio.to_thread(self._response.recv, self._config.timeout_ms)
        result = self._codec.decode(data)
        return result if isinstance(result, dict) else {}

    async def _recv_response_async(self) -> dict[str, Any]:
        """Receive response using native async transport (no thread pool)."""
        if self._response_transport is None:
            msg = "Response transport unavailable"
            raise BrokerClientError(msg)
        data = await self._response_transport.recv()
        result = self._codec.decode(data)
        return result if isinstance(result, dict) else {}

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

    async def _release_arrow(self, name: str) -> None:
        try:
            await self.request({"kind": "arrow_release", "name": name})
        except BrokerClientError:
            return

    async def _decode_result(self, response: dict[str, Any]) -> Any:
        data = _assert_ok(response)
        if "arrow" not in data and "arrow_bytes" not in data:
            return data.get("result")
        if "arrow_bytes" in data:
            arrow_bytes = data.get("arrow_bytes") or b""
            return _decode_arrow_bytes(arrow_bytes)
        arrow = data["arrow"] or {}
        name = arrow.get("name")
        size = int(arrow.get("size", 0))
        if not name or size <= 0:
            msg = "Invalid Arrow response"
            raise BrokerClientError(msg)
        try:
            import pyarrow as pa
        except Exception as exc:  # pragma: no cover - optional dependency
            msg = "pyarrow is required for Arrow IPC results"
            raise BrokerClientError(msg) from exc

        # Use CFFI ShmRegion for Arrow IPC (PyO3 ShmRegion also works)
        if _ShmRegion is not None and _ShmRegionConfig is not None:
            region = _ShmRegion.open(_ShmRegionConfig(name=name, size=size))
            try:
                buffer = region.buffer()
                with pa.ipc.open_stream(buffer) as reader:
                    table = reader.read_all()
            finally:
                region.close()
                await self._release_arrow(str(name))
            return {"arrow": table}
        # Fallback: try using the PyO3 ShmRegion
        try:
            from vertebra._vertebra import ShmRegion as PyShmRegion

            region = PyShmRegion.open_named(name, size)
            buffer = region.read(0, size)
            with pa.ipc.open_stream(buffer) as reader:
                table = reader.read_all()
            await self._release_arrow(str(name))
        except Exception as exc:
            msg = "No shared memory region implementation available"
            raise BrokerClientError(msg) from exc
        return {"arrow": table}


def _default_client_id() -> str:
    value = os.environ.get("VERTEBRA_WORKER_ID")
    if value:
        return value
    return uuid.uuid4().hex


def _with_lock(path: str, action: Any) -> None:
    import fcntl

    pathlib.Path(pathlib.Path(path).parent).mkdir(exist_ok=True, parents=True)
    with pathlib.Path(path).open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            action()
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _assert_ok(response: dict[str, Any]) -> dict[str, Any]:
    if not response.get("ok"):
        error = response.get("error") or {}
        message = error.get("message") or "Broker error"
        raise BrokerClientError(message)
    return response


def _decode_arrow_bytes(payload: bytes) -> dict[str, Any]:
    try:
        import pyarrow as pa
    except Exception as exc:  # pragma: no cover - optional dependency
        msg = "pyarrow is required for Arrow IPC results"
        raise BrokerClientError(msg) from exc
    with pa.ipc.open_stream(payload) as reader:
        table = reader.read_all()
    return {"arrow": table}


__all__ = ("BrokerClient", "BrokerClientConfig", "BrokerClientError")
