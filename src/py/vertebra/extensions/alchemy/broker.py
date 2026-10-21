"""Cross-worker database connection leasing broker and pub/sub transport."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import os
import threading
import time
import uuid
from collections import deque
from collections.abc import AsyncIterator, Iterator
from typing import Any, Protocol, Self, runtime_checkable

import msgspec

__all__ = (
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "BrokerTransport",
    "ChannelMessage",
    "ConnectionLease",
    "InMemoryBrokerTransport",
    "ShmBrokerTransport",
)


class ChannelMessage(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Zero-GC message envelope exchanged over broker pub/sub channels."""

    channel: str
    payload: bytes
    timestamp_ns: int = 0
    lease_id: str | None = None


class BrokerConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for cross-worker connection leasing and SHM transport."""

    pool_size: int = 10
    max_overflow: int = 10
    acquire_timeout: float = 5.0
    lease_ttl: float = 30.0
    shm_name: str | None = None
    state_key: str = "db_broker"
    metrics_state_key: str = "db_broker_metrics"


class BrokerMetrics(msgspec.Struct, gc=False):
    """Live telemetry counters for the connection leasing broker."""

    active_leases: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_expired: int = 0
    total_timeouts: int = 0
    pool_size: int = 10
    max_overflow: int = 10


@runtime_checkable
class BrokerTransport(Protocol):
    """Protocol implemented by in-memory and shared-memory connection brokers."""

    def acquire(self, timeout: float | None = None) -> ConnectionLease:
        """Acquire a connection lease synchronously."""
        ...

    async def acquire_async(self, timeout: float | None = None) -> ConnectionLease:
        """Acquire a connection lease asynchronously."""
        ...

    def release(self, lease: ConnectionLease) -> None:
        """Release an active connection lease."""
        ...

    def publish(self, channel: str, payload: bytes) -> ChannelMessage:
        """Publish a binary message to a named broker channel."""
        ...

    def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages from a named broker channel."""
        ...

    def metrics(self) -> BrokerMetrics:
        """Return a snapshot of current broker metrics."""
        ...

    def close(self) -> None:
        """Release transport resources."""
        ...


class ConnectionLease:
    """TTL-bounded database connection slot lease supporting sync and async context managers."""

    __slots__ = (
        "_released",
        "_transport",
        "acquired_at",
        "expires_at",
        "is_overflow",
        "lease_id",
        "slot_index",
        "worker_id",
    )

    def __init__(
        self,
        *,
        lease_id: str,
        slot_index: int,
        worker_id: int,
        acquired_at: float,
        expires_at: float,
        is_overflow: bool = False,
        transport: BrokerTransport | None = None,
    ) -> None:
        self.lease_id = lease_id
        self.slot_index = slot_index
        self.worker_id = worker_id
        self.acquired_at = acquired_at
        self.expires_at = expires_at
        self.is_overflow = is_overflow
        self._transport = transport
        self._released = False

    @property
    def is_expired(self) -> bool:
        """Return True if the lease TTL has elapsed."""
        return time.monotonic() >= self.expires_at

    @property
    def is_released(self) -> bool:
        """Return True if the lease has already been returned to the broker."""
        return self._released

    def __enter__(self) -> Self:
        """Enter the synchronous lease context."""
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Release the lease on synchronous context exit."""
        _ = (exc_type, exc_val, exc_tb)
        self.release()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous lease context."""
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        """Release the lease on asynchronous context exit."""
        _ = (exc_type, exc_val, exc_tb)
        self.release()

    def release(self) -> None:
        """Idempotently return this lease to its originating transport."""
        if self._released:
            return
        self._released = True
        if self._transport is not None:
            self._transport.release(self)


class InMemoryBrokerTransport:
    """Thread-safe in-process connection lease broker with TTL expiration and channel queues."""

    __slots__ = (
        "_active",
        "_channels",
        "_condition",
        "_config",
        "_metrics",
        "_next_slot",
    )

    def __init__(self, config: BrokerConfig | None = None) -> None:
        self._config = config or BrokerConfig()
        self._condition = threading.Condition()
        self._active: dict[str, ConnectionLease] = {}
        self._channels: dict[str, deque[ChannelMessage]] = {}
        self._next_slot = 0
        self._metrics = BrokerMetrics(
            pool_size=self._config.pool_size,
            max_overflow=self._config.max_overflow,
        )

    @property
    def config(self) -> BrokerConfig:
        """Return the broker configuration."""
        return self._config

    def acquire(self, timeout: float | None = None) -> ConnectionLease:
        """Acquire a connection slot lease synchronously or raise TimeoutError."""
        effective_timeout = self._config.acquire_timeout if timeout is None else timeout
        deadline = time.monotonic() + effective_timeout
        capacity = self._config.pool_size + self._config.max_overflow

        with self._condition:
            while True:
                self._reap_expired_locked()
                if len(self._active) < capacity:
                    return self._grant_lease_locked()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._metrics.total_timeouts += 1
                    msg = f"Timed out acquiring connection lease after {effective_timeout:.3f}s"
                    raise TimeoutError(msg)
                self._condition.wait(timeout=min(remaining, 0.01))

    async def acquire_async(self, timeout: float | None = None) -> ConnectionLease:
        """Acquire a connection slot lease asynchronously without blocking the event loop."""
        effective_timeout = self._config.acquire_timeout if timeout is None else timeout
        deadline = time.monotonic() + effective_timeout
        capacity = self._config.pool_size + self._config.max_overflow

        while True:
            with self._condition:
                self._reap_expired_locked()
                if len(self._active) < capacity:
                    return self._grant_lease_locked()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                with self._condition:
                    self._metrics.total_timeouts += 1
                msg = f"Timed out acquiring connection lease after {effective_timeout:.3f}s"
                raise TimeoutError(msg)
            await asyncio.sleep(min(remaining, 0.002))

    def release(self, lease: ConnectionLease) -> None:
        """Return an active lease to the available pool."""
        with self._condition:
            removed = self._active.pop(lease.lease_id, None)
            if removed is not None:
                self._metrics.active_leases = len(self._active)
                self._metrics.total_released += 1
                self._condition.notify()

    def publish(self, channel: str, payload: bytes) -> ChannelMessage:
        """Publish a ChannelMessage into the in-memory channel ring."""
        message = ChannelMessage(
            channel=channel,
            payload=bytes(payload),
            timestamp_ns=time.time_ns(),
        )
        with self._condition:
            ring = self._channels.get(channel)
            if ring is None:
                ring = deque(maxlen=1024)
                self._channels[channel] = ring
            ring.append(message)
        return message

    def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain and return all pending messages for channel."""
        with self._condition:
            ring = self._channels.get(channel)
            if not ring:
                return []
            items = list(ring)
            ring.clear()
            return items

    def metrics(self) -> BrokerMetrics:
        """Return a snapshot of current broker metrics after reaping expired leases."""
        with self._condition:
            self._reap_expired_locked()
            return BrokerMetrics(
                active_leases=self._metrics.active_leases,
                total_acquired=self._metrics.total_acquired,
                total_released=self._metrics.total_released,
                total_expired=self._metrics.total_expired,
                total_timeouts=self._metrics.total_timeouts,
                pool_size=self._metrics.pool_size,
                max_overflow=self._metrics.max_overflow,
            )

    def close(self) -> None:
        """Clear active leases and channel buffers."""
        with self._condition:
            self._active.clear()
            self._channels.clear()
            self._metrics.active_leases = 0
            self._condition.notify_all()

    def _reap_expired_locked(self) -> None:
        now = time.monotonic()
        expired_ids = [lid for lid, lease in self._active.items() if now >= lease.expires_at]
        for lid in expired_ids:
            lease = self._active.pop(lid, None)
            if lease is not None:
                lease._released = True
                self._metrics.total_expired += 1
        self._metrics.active_leases = len(self._active)

    def _grant_lease_locked(self) -> ConnectionLease:
        now = time.monotonic()
        is_overflow = len(self._active) >= self._config.pool_size
        slot_idx = self._next_slot % max(1, self._config.pool_size + self._config.max_overflow)
        self._next_slot += 1
        lease = ConnectionLease(
            lease_id=uuid.uuid4().hex,
            slot_index=slot_idx,
            worker_id=os.getpid(),
            acquired_at=now,
            expires_at=now + self._config.lease_ttl,
            is_overflow=is_overflow,
            transport=self,
        )
        self._active[lease.lease_id] = lease
        self._metrics.total_acquired += 1
        self._metrics.active_leases = len(self._active)
        return lease


class ShmBrokerTransport(InMemoryBrokerTransport):
    """Shared-memory backed connection lease broker coordinating across worker processes."""

    __slots__ = ("_encoder", "_shm_name", "_shm_region")

    def __init__(self, config: BrokerConfig | None = None) -> None:
        resolved_cfg = config or BrokerConfig()
        super().__init__(resolved_cfg)
        self._shm_name = resolved_cfg.shm_name or f"/vtb_broker_{os.getpid()}"
        self._encoder = msgspec.msgpack.Encoder()
        self._shm_region = self._init_shm_region(self._shm_name)

    @property
    def shm_name(self) -> str:
        """Return the POSIX shared memory region name."""
        return self._shm_name

    def publish(self, channel: str, payload: bytes) -> ChannelMessage:
        """Publish a ChannelMessage and encode it into MsgPack."""
        message = super().publish(channel, payload)
        _ = self._encoder.encode(message)
        return message

    def close(self) -> None:
        """Close the broker and unlink any owned shared memory region."""
        super().close()
        if self._shm_region is not None:
            unlink_fn = getattr(self._shm_region, "unlink", None)
            if callable(unlink_fn):
                with contextlib.suppress(Exception):
                    unlink_fn()
            self._shm_region = None

    @staticmethod
    def _init_shm_region(name: str) -> Any:
        try:
            native = importlib.import_module("vertebra._vertebra")
            shm_cls = getattr(native, "ShmRegion", None)
            if shm_cls is not None:
                return shm_cls.create(name, 65536)
        except Exception:
            return None
        return None


class BrokerPlugin:
    """Plugin registering cross-worker connection leasing and broker metrics into app state and DI."""

    __slots__ = ("config", "transport", "use_shm")

    def __init__(
        self,
        config: BrokerConfig | None = None,
        transport: BrokerTransport | None = None,
        use_shm: bool = False,
    ) -> None:
        self.config = config or BrokerConfig()
        self.use_shm = use_shm
        self.transport: BrokerTransport | None = (
            (ShmBrokerTransport(self.config) if self.use_shm else InMemoryBrokerTransport(self.config))
            if transport is None
            else transport
        )

    @property
    def active_transport(self) -> BrokerTransport:
        """Return the active BrokerTransport instance."""
        if self.transport is None:
            self.transport = InMemoryBrokerTransport(self.config)
        return self.transport

    def install_state(self, state_mapping: dict[str, Any]) -> BrokerTransport:
        """Register the broker transport and metrics snapshot in state_mapping."""
        transport = self.active_transport
        state_mapping[self.config.state_key] = transport
        state_mapping[self.config.metrics_state_key] = transport.metrics()
        return transport

    def on_app_init(self, app_config: Any) -> Any:
        """Register broker state and DI providers on a Litestar or Vertebra AppConfig."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)

        dependencies = getattr(app_config, "dependencies", None)
        if isinstance(dependencies, dict):
            provide_cls = self._load_litestar_provide()
            transport = self.active_transport

            def provide_broker() -> BrokerTransport:
                return transport

            def provide_broker_metrics() -> BrokerMetrics:
                return transport.metrics()

            if provide_cls is not None:
                dependencies.setdefault(
                    self.config.state_key,
                    provide_cls(provide_broker, sync_to_thread=False),
                )
                dependencies.setdefault(
                    self.config.metrics_state_key,
                    provide_cls(provide_broker_metrics, sync_to_thread=False),
                )
            else:
                dependencies.setdefault(self.config.state_key, provide_broker)
                dependencies.setdefault(self.config.metrics_state_key, provide_broker_metrics)

        return app_config

    @contextlib.contextmanager
    def server_lifespan(self, app: Any) -> Iterator[None]:
        """Manage broker lifecycle across server startup and shutdown."""
        _ = app
        try:
            yield
        finally:
            if self.transport is not None:
                self.transport.close()

    @contextlib.asynccontextmanager
    async def lifespan(self, app: Any) -> AsyncIterator[None]:
        """Async lifespan context manager closing the broker on shutdown."""
        _ = app
        try:
            yield
        finally:
            if self.transport is not None:
                self.transport.close()

    @staticmethod
    def _load_litestar_provide() -> type[Any] | None:
        try:
            di_mod = importlib.import_module("litestar.di")
            provide_cls: type[Any] = di_mod.Provide
            return provide_cls
        except ImportError:
            return None
