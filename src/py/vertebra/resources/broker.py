"""Shared-memory broker client for resource management."""

import os
import pathlib
import uuid
from typing import Any, cast

import msgspec

from vertebra._vertebra import AsyncRing, ShmRegion, SpscRing
from vertebra.exceptions import BrokerError
from vertebra.ipc.async_transport import AsyncShmTransport, PollingTransport, create_transport
from vertebra.resources.arrow import decode_arrow_nano
from vertebra.serializers import MessagePackCodec
from vertebra.utils.module_loader import ensure_pyarrow


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
    prefer_eventfd: bool = True

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


class AsyncBrokerClient:
    def __init__(self, config: BrokerClientConfig | None = None) -> None:
        self._config = config or BrokerClientConfig()
        self._codec = MessagePackCodec()
        self._client_id = self._config.client_id or _default_client_id()
        self._seq = 0

        # Ring buffers (PyO3-based)
        self._control: AsyncRing | None = None
        self._request: AsyncRing | None = None
        self._response: AsyncRing | None = None

        # Async transport wrapper for RECEIVING responses only.
        # NOTE: We send directly via self._request.send() instead of using a transport,
        # because PollingTransport starts a poll thread that reads from the ring,
        # which would consume our own sent data before the broker receives it.
        self._response_transport: AsyncShmTransport | PollingTransport | None = None

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
        # The request ring is used directly for sending (no transport needed).
        self._response_transport = create_transport(self._response, prefer_eventfd=self._config.prefer_eventfd)

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
        response = await self._recv_response()
        _assert_ok(response)

    async def close(self) -> None:
        if self._control is None:
            return

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

        payload = dict(payload)
        payload["id"] = self._next_id()
        encoded = self._codec.encode(payload)

        # Send directly via ring (not via transport) to avoid PollingTransport's
        # poll thread consuming the data before the broker receives it.
        self._request.send(encoded, 0)
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

    async def _recv_response(self) -> dict[str, Any]:
        """Receive response using native async transport."""
        if self._response_transport is None:
            msg = "Response transport unavailable"
            raise BrokerError(msg)
        data = await self._response_transport.recv()
        result = self._codec.decode(data)
        return result if isinstance(result, dict) else {}

    def _next_id(self) -> int:
        self._seq += 1
        return self._seq

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
            try:
                table = _decode_arrow_bytes(arrow_bytes)
            except (ImportError, RuntimeError):
                return {"arrow": decode_arrow_nano(arrow_bytes)}
            else:
                return table
        arrow = data["arrow"] or {}
        name = arrow.get("name")
        size = int(arrow.get("size", 0))
        if not name or size <= 0:
            msg = "Invalid Arrow response"
            raise BrokerError(msg)

        # Use PyO3 ShmRegion for Arrow IPC with buffer protocol
        region = ShmRegion.open_named(name, size)
        try:
            try:
                # Ensure pyarrow is available with clear error message
                pa = ensure_pyarrow()
                with pa.ipc.open_stream(memoryview(region)) as reader:
                    table = reader.read_all()
            except (ImportError, RuntimeError):
                # Fallback to nanoarrow
                return {"arrow": decode_arrow_nano(memoryview(region))}
            else:
                return {"arrow": table}
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
            try:
                table = _decode_arrow_bytes(arrow_bytes)
            except (ImportError, RuntimeError):
                return {"arrow": decode_arrow_nano(arrow_bytes)}
            else:
                return table
        arrow = data["arrow"] or {}
        name = arrow.get("name")
        size = int(arrow.get("size", 0))
        if not name or size <= 0:
            msg = "Invalid Arrow response"
            raise BrokerError(msg)

        # Use PyO3 ShmRegion for Arrow IPC with buffer protocol
        region = ShmRegion.open_named(name, size)
        try:
            try:
                # Ensure pyarrow is available with clear error message
                pa = ensure_pyarrow()
                with pa.ipc.open_stream(memoryview(region)) as reader:
                    table = reader.read_all()
            except (ImportError, RuntimeError):
                # Fallback to nanoarrow
                return {"arrow": decode_arrow_nano(memoryview(region))}
            else:
                return {"arrow": table}
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


def _decode_arrow_bytes(payload: bytes) -> dict[str, Any]:
    # Ensure pyarrow is available with clear error message
    pa = ensure_pyarrow()
    with pa.ipc.open_stream(payload) as reader:
        table = reader.read_all()
    return {"arrow": table}


__all__ = ("AsyncBrokerClient", "BrokerClientConfig", "BrokerError", "SyncBrokerClient")
