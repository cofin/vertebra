"""Shared-memory broker client for resource management."""

import asyncio
import contextlib
import os
import pathlib
import uuid
from typing import Any, cast

import msgspec

from vertebra._vertebra import AsyncRing, ShmRegion, SpscRing
from vertebra.exceptions import BrokerError
from vertebra.ipc.async_transport import AsyncShmTransport, create_transport
from vertebra.ipc.channel import decode_response, encode_request
from vertebra.protocol import Flags
from vertebra.resources.arrow import decode_arrow
from vertebra.serializers import MessagePackCodec


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
    max_concurrent_requests: int = 64
    """Maximum number of concurrent in-flight requests."""

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


class _PendingRequest:
    """Represents a pending request awaiting response."""

    __slots__ = ("future", "timeout_handle")

    def __init__(self) -> None:
        self.future: asyncio.Future[dict[str, Any]] = asyncio.get_event_loop().create_future()
        self.timeout_handle: asyncio.TimerHandle | None = None


class AsyncBrokerClient:
    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        self._config = config or BrokerClientConfig()
        self._codec = MessagePackCodec()
        self._client_id = self._config.client_id or _default_client_id()
        self._seq = 0
        self._correlation_seq = 0  # Protocol-level correlation ID

        # Ring buffers (PyO3-based)
        self._control: AsyncRing | None = None
        self._request: AsyncRing | None = None
        self._response: AsyncRing | None = None

        # Async transport wrapper for RECEIVING responses only.
        self._response_transport: AsyncShmTransport | None = None

        # Multiplexing support
        self._pending: dict[int, _PendingRequest] = {}
        self._response_loop_task: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        if self._control is not None:
            return

        # Create rings using AsyncRing from PyO3
        control_cfg = self._config.control_ring()
        req_cfg = self._config.request_ring(self._client_id)
        resp_cfg = self._config.response_ring(self._client_id)

        self._control = AsyncRing.open_named(control_cfg.name, control_cfg.slot_size, control_cfg.slots)
        self._request = AsyncRing.create_named(req_cfg.name, req_cfg.slot_size, req_cfg.slots)
        self._response = AsyncRing.create_named(resp_cfg.name, resp_cfg.slot_size, resp_cfg.slots)

        # Wrap response ring in async transport for efficient waiting.
        self._response_transport = create_transport(self._response)

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

        # Wait for response using legacy path for registration
        response = await self._recv_response_legacy()
        _assert_ok(response)

        # Start response loop for multiplexing
        self._response_loop_task = asyncio.create_task(self._run_response_loop())

    async def close(self) -> None:
        if self._control is None:
            return

        # Cancel response loop
        if self._response_loop_task:
            self._response_loop_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._response_loop_task
            self._response_loop_task = None

        # Fail all pending requests
        for pending in self._pending.values():
            if pending.timeout_handle:
                pending.timeout_handle.cancel()
            if not pending.future.done():
                pending.future.set_exception(BrokerError("Connection closed"))
        self._pending.clear()

        # Clean up async transport
        if self._response_transport:
            self._response_transport.close()
            self._response_transport = None

        self._control = None
        self._request = None
        self._response = None

    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._request is None or self._response_transport is None:
            await self.connect()
        if self._request is None or self._response_transport is None:
            msg = "Broker shared memory unavailable"
            raise BrokerError(msg)
        if len(self._pending) >= self._config.max_concurrent_requests:
            msg = "Too many concurrent requests"
            raise BrokerError(msg)

        payload = dict(payload)
        payload["id"] = self._next_id()
        correlation_id = self._next_correlation_id()

        # Create pending request
        pending = _PendingRequest()
        self._pending[correlation_id] = pending

        # Set timeout
        loop = asyncio.get_event_loop()
        pending.timeout_handle = loop.call_later(
            self._config.timeout_ms / 1000.0, self._timeout_request, correlation_id
        )

        # Encode with protocol header and send
        encoded = self._codec.encode(payload)
        message = encode_request("broker.request", encoded, correlation_id=correlation_id)
        self._request.send(message, 0)

        try:
            return await pending.future
        finally:
            # Clean up
            if pending.timeout_handle:
                pending.timeout_handle.cancel()
            self._pending.pop(correlation_id, None)

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

    async def savepoint(self, pool: str, token: str, name: str) -> Any:
        response = await self.request({"kind": "savepoint", "pool": pool, "token": token, "name": name})
        return await self._decode_result(response)

    async def release_savepoint(self, pool: str, token: str, name: str) -> Any:
        response = await self.request({"kind": "release_savepoint", "pool": pool, "token": token, "name": name})
        return await self._decode_result(response)

    async def rollback_savepoint(self, pool: str, token: str, name: str) -> Any:
        response = await self.request({"kind": "rollback_savepoint", "pool": pool, "token": token, "name": name})
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

    async def storage_read(self, pool: str, path: str) -> bytes:
        response = await self.request({"kind": "storage_read", "pool": pool, "path": path})
        data = _assert_ok(response)
        return cast("bytes", data.get("data"))

    async def storage_write(self, pool: str, path: str, data: bytes) -> None:
        response = await self.request({"kind": "storage_write", "pool": pool, "path": path, "data": data})
        _assert_ok(response)

    async def storage_list(self, pool: str, prefix: str | None = None) -> list[str]:
        response = await self.request({"kind": "storage_list", "pool": pool, "prefix": prefix})
        data = _assert_ok(response)
        return cast("list[str]", data.get("paths", []))

    async def storage_exists(self, pool: str, path: str) -> bool:
        response = await self.request({"kind": "storage_exists", "pool": pool, "path": path})
        data = _assert_ok(response)
        return bool(data.get("exists", False))

    async def storage_delete(self, pool: str, path: str) -> None:
        response = await self.request({"kind": "storage_delete", "pool": pool, "path": path})
        _assert_ok(response)

    async def storage_copy(self, pool: str, src: str, dst: str) -> None:
        response = await self.request({"kind": "storage_copy", "pool": pool, "src": src, "dst": dst})
        _assert_ok(response)

    async def storage_move(self, pool: str, src: str, dst: str) -> None:
        response = await self.request({"kind": "storage_move", "pool": pool, "src": src, "dst": dst})
        _assert_ok(response)

    async def storage_glob(self, pool: str, pattern: str) -> list[str]:
        response = await self.request({"kind": "storage_glob", "pool": pool, "pattern": pattern})
        data = _assert_ok(response)
        return cast("list[str]", data.get("paths", []))

    async def _recv_response_legacy(self) -> dict[str, Any]:
        """Receive response using native async transport (legacy, for registration)."""
        if self._response_transport is None:
            msg = "Response transport unavailable"
            raise BrokerError(msg)
        data = await self._response_transport.recv()
        result = self._codec.decode(data)
        return result if isinstance(result, dict) else {}

    async def _run_response_loop(self) -> None:
        """Background task that dispatches responses to pending requests."""
        while self._response_transport is not None:
            try:
                data = await self._response_transport.recv()
                correlation_id, flags, payload = decode_response(data)
                decoded = self._codec.decode(payload)

                pending = self._pending.get(correlation_id)
                if pending is None:
                    # Response for unknown correlation ID (may have timed out)
                    continue

                if pending.timeout_handle:
                    pending.timeout_handle.cancel()

                if flags & Flags.ERROR:
                    pending.future.set_exception(BrokerError("Remote error"))
                elif isinstance(decoded, dict):
                    pending.future.set_result(decoded)
                else:
                    pending.future.set_result({})

            except asyncio.CancelledError:
                break
            except (OSError, ValueError, RuntimeError):
                # Decoding errors - continue processing other responses
                continue

    def _timeout_request(self, correlation_id: int) -> None:
        """Handle request timeout."""
        pending = self._pending.pop(correlation_id, None)
        if pending and not pending.future.done():
            pending.future.set_exception(BrokerError("Request timed out"))

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

    def _next_correlation_id(self) -> int:
        self._correlation_seq += 1
        return self._correlation_seq

    async def _release_arrow(self, name: str) -> None:
        try:
            await self.request({"kind": "arrow_release", "name": name})
        except BrokerError:
            return

    async def _decode_result(self, response: dict[str, Any]) -> Any:
        data = _assert_ok(response)
        if "arrow" not in data and "arrow_bytes" not in data:
            return data.get("result")
        if "arrow_bytes" in data:
            arrow_bytes = data.get("arrow_bytes") or b""
            return {"arrow": decode_arrow(arrow_bytes)}
        arrow = data["arrow"] or {}
        name = arrow.get("name")
        size = int(arrow.get("size", 0))
        if not name or size <= 0:
            msg = "Invalid Arrow response"
            raise BrokerError(msg)

        # Use PyO3 ShmRegion for Arrow IPC with buffer protocol
        region = ShmRegion.open_named(name, size)
        try:
            return {"arrow": decode_arrow(memoryview(region))}
        finally:
            region.close()
            await self._release_arrow(str(name))


class SyncBrokerClient:
    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        self._config = config or BrokerClientConfig()
        self._codec = MessagePackCodec()
        self._client_id = self._config.client_id or _default_client_id()
        self._seq = 0

        # Ring buffers (SpscRing)
        self._control: SpscRing | None = None
        self._request: SpscRing | None = None
        self._response: SpscRing | None = None

    def connect(self) -> None:
        if self._control is not None:
            return

        # Create rings using SpscRing from PyO3
        control_cfg = self._config.control_ring()
        req_cfg = self._config.request_ring(self._client_id)
        resp_cfg = self._config.response_ring(self._client_id)

        self._control = SpscRing.open_named(control_cfg.name, control_cfg.slot_size, control_cfg.slots)
        self._request = SpscRing.create_named(req_cfg.name, req_cfg.slot_size, req_cfg.slots)
        self._response = SpscRing.create_named(resp_cfg.name, resp_cfg.slot_size, resp_cfg.slots)

        # Register with broker
        payload = {
            "id": self._next_id(),
            "kind": "register_client",
            "client_id": self._client_id,
            "req_ring": req_cfg.name,
            "resp_ring": resp_cfg.name,
            "weight": self._config.weight,
        }

        # Send registration to control ring
        encoded = self._codec.encode(payload)
        _with_lock(self._config.control_lock(), lambda: self._control.send(encoded, 0))

        # Wait for response
        response = self._recv_response()
        _assert_ok(response)

    def close(self) -> None:
        if self._control is None:
            return

        self._control = None
        self._request = None
        self._response = None

    def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._request is None or self._response is None:
            self.connect()
        if self._request is None or self._response is None:
            msg = "Broker shared memory unavailable"
            raise BrokerError(msg)

        payload = dict(payload)
        payload["id"] = self._next_id()
        encoded = self._codec.encode(payload)

        self._request.send(encoded, 0)
        return self._recv_response()

    def acquire(self, pool: str) -> str:
        response = self.request({"kind": "acquire", "pool": pool, "timeout_ms": self._config.timeout_ms})
        data = _assert_ok(response)
        return str(data.get("token"))

    def release(self, pool: str, token: str) -> bool:
        response = self.request({"kind": "release", "pool": pool, "token": token})
        data = _assert_ok(response)
        return bool(data.get("ok", True))

    def health(self, pool: str) -> bool:
        response = self.request({"kind": "health", "pool": pool})
        data = _assert_ok(response)
        return bool(data.get("ok", True))

    def execute(
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
        response = self.request(payload)
        return self._decode_result(response)

    def execute_many(
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
        response = self.request(payload)
        return self._decode_result(response)

    def command(
        self, pool: str, token: str, command: str, args: list[Any] | None = None, correlation_id: str | None = None
    ) -> Any:
        payload = {"kind": "command", "pool": pool, "token": token, "command": command, "args": args or []}
        if correlation_id:
            payload["correlation_id"] = correlation_id
        response = self.request(payload)
        return self._decode_result(response)

    def begin(self, pool: str, token: str) -> Any:
        response = self.request({"kind": "begin", "pool": pool, "token": token})
        return self._decode_result(response)

    def commit(self, pool: str, token: str) -> Any:
        response = self.request({"kind": "commit", "pool": pool, "token": token})
        return self._decode_result(response)

    def rollback(self, pool: str, token: str) -> Any:
        response = self.request({"kind": "rollback", "pool": pool, "token": token})
        return self._decode_result(response)

    def savepoint(self, pool: str, token: str, name: str) -> Any:
        response = self.request({"kind": "savepoint", "pool": pool, "token": token, "name": name})
        return self._decode_result(response)

    def release_savepoint(self, pool: str, token: str, name: str) -> Any:
        response = self.request({"kind": "release_savepoint", "pool": pool, "token": token, "name": name})
        return self._decode_result(response)

    def rollback_savepoint(self, pool: str, token: str, name: str) -> Any:
        response = self.request({"kind": "rollback_savepoint", "pool": pool, "token": token, "name": name})
        return self._decode_result(response)

    def stats(self, pool: str | None = None) -> dict[str, Any]:
        response = self.request({"kind": "stats", "pool": pool})
        data = _assert_ok(response)
        stats = data.get("stats")
        return stats if isinstance(stats, dict) else {}

    def register_provider(self, name: str, driver: str, max_size: int, *, req_ring: str, resp_ring: str) -> bool:
        response = self.request({
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

    def storage_read(self, pool: str, path: str) -> bytes:
        response = self.request({"kind": "storage_read", "pool": pool, "path": path})
        data = _assert_ok(response)
        return cast("bytes", data.get("data"))

    def storage_write(self, pool: str, path: str, data: bytes) -> None:
        response = self.request({"kind": "storage_write", "pool": pool, "path": path, "data": data})
        _assert_ok(response)

    def storage_list(self, pool: str, prefix: str | None = None) -> list[str]:
        response = self.request({"kind": "storage_list", "pool": pool, "prefix": prefix})
        data = _assert_ok(response)
        return cast("list[str]", data.get("paths", []))

    def storage_exists(self, pool: str, path: str) -> bool:
        response = self.request({"kind": "storage_exists", "pool": pool, "path": path})
        data = _assert_ok(response)
        return bool(data.get("exists", False))

    def storage_delete(self, pool: str, path: str) -> None:
        response = self.request({"kind": "storage_delete", "pool": pool, "path": path})
        _assert_ok(response)

    def storage_copy(self, pool: str, src: str, dst: str) -> None:
        response = self.request({"kind": "storage_copy", "pool": pool, "src": src, "dst": dst})
        _assert_ok(response)

    def storage_move(self, pool: str, src: str, dst: str) -> None:
        response = self.request({"kind": "storage_move", "pool": pool, "src": src, "dst": dst})
        _assert_ok(response)

    def storage_glob(self, pool: str, pattern: str) -> list[str]:
        response = self.request({"kind": "storage_glob", "pool": pool, "pattern": pattern})
        data = _assert_ok(response)
        return cast("list[str]", data.get("paths", []))

    def _recv_response(self) -> dict[str, Any]:
        """Receive response using blocking SpscRing."""
        if self._response is None:
            msg = "Response ring unavailable"
            raise BrokerError(msg)
        # Using a fixed large buffer for now, similar to AsyncTransport
        data = self._response.recv(self._config.slot_size)
        result = self._codec.decode(data)
        return result if isinstance(result, dict) else {}

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

    def _release_arrow(self, name: str) -> None:
        try:
            self.request({"kind": "arrow_release", "name": name})
        except BrokerError:
            return

    def _decode_result(self, response: dict[str, Any]) -> Any:
        data = _assert_ok(response)
        if "arrow" not in data and "arrow_bytes" not in data:
            return data.get("result")
        if "arrow_bytes" in data:
            arrow_bytes = data.get("arrow_bytes") or b""
            return {"arrow": decode_arrow(arrow_bytes)}
        arrow = data["arrow"] or {}
        name = arrow.get("name")
        size = int(arrow.get("size", 0))
        if not name or size <= 0:
            msg = "Invalid Arrow response"
            raise BrokerError(msg)

        # Use PyO3 ShmRegion for Arrow IPC with buffer protocol
        region = ShmRegion.open_named(name, size)
        try:
            return {"arrow": decode_arrow(memoryview(region))}
        finally:
            region.close()
            self._release_arrow(str(name))


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
        raise BrokerError(message)
    return response


__all__ = ("AsyncBrokerClient", "BrokerClientConfig", "BrokerError", "SyncBrokerClient")
