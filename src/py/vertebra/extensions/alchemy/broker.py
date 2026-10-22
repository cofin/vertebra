"""SQLAlchemy session connection leasing broker and transport integration."""

from __future__ import annotations

import asyncio
import importlib
import threading
import time
from collections import deque
from typing import Any, Protocol, runtime_checkable

from vertebra.extensions.broker import (
    BrokerConfig,
    BrokerMetrics,
    BrokerPlugin,
    ChannelMessage,
    ConnectionLease,
    InMemoryBrokerTransport,
    ShmBrokerTransport,
    StatementPlan,
)

__all__ = (
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "BrokerTransport",
    "ChannelMessage",
    "ConnectionLease",
    "InMemoryBrokerTransport",
    "ShmBrokerTransport",
    "ShmSessionBroker",
    "StatementCache",
    "StatementPlan",
)


@runtime_checkable
class BrokerTransport(Protocol):
    """Protocol implemented by in-memory and shared-memory connection brokers."""

    def acquire(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a connection lease synchronously."""
        ...

    async def acquire_async(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a connection lease asynchronously."""
        ...

    def release(self, *args: Any, **kwargs: Any) -> Any:
        """Release an active connection lease."""
        ...

    def metrics(self) -> Any:
        """Return a snapshot of current broker metrics."""
        ...


class ShmSessionBroker:
    """SQLAlchemy connection pool and session broker backed by shared-memory lease table."""

    __slots__ = ("_active_leases", "_capacity", "_in_use", "_lock", "_waiters", "transport")

    def __init__(
        self,
        transport: ShmBrokerTransport | int | None = None,
        config: BrokerConfig | None = None,
        *,
        capacity: int = 10,
    ) -> None:
        if isinstance(transport, int):
            resolved_capacity = max(1, transport)
            resolved_transport = ShmBrokerTransport(config or BrokerConfig(pool_size=resolved_capacity, max_overflow=0))
        else:
            resolved_capacity = max(1, capacity)
            resolved_transport = transport or ShmBrokerTransport(config)
        self.transport = resolved_transport
        self._capacity = resolved_capacity
        self._in_use = 0
        self._active_leases: dict[int, ConnectionLease] = {}
        self._lock = threading.Lock()
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def capacity(self) -> int:
        """Return maximum concurrent session capacity."""
        return self._capacity

    @property
    def in_use(self) -> int:
        """Return number of currently active session leases."""
        with self._lock:
            return self._in_use

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a session slot synchronously."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock:
                if self._in_use < self._capacity:
                    self._in_use += 1
                    return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(0.001)

    async def acquire_async(self, timeout: float | None = None) -> bool:
        """Acquire a session slot asynchronously without blocking the event loop."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._in_use < self._capacity:
                self._in_use += 1
                return True
            waiter: asyncio.Future[None] = loop.create_future()
            self._waiters.append(waiter)

        try:
            if timeout is not None:
                await asyncio.wait_for(asyncio.shield(waiter), timeout=timeout)
            else:
                await waiter
            return True
        except (TimeoutError, asyncio.CancelledError):
            waiter.cancel()
            return False

    def release(self) -> None:
        """Release a session slot and wake the next pending waiter."""
        with self._lock:
            if self._in_use > 0:
                self._in_use -= 1
            while self._waiters:
                next_waiter = self._waiters.popleft()
                if not next_waiter.done():
                    self._in_use += 1
                    next_waiter.set_result(None)
                    break

    async def release_async(self) -> None:
        """Release a session slot asynchronously holding the lock."""
        self.release()

    def on_checkout(
        self,
        dbapi_connection: Any,
        connection_record: Any = None,
        connection_proxy: Any = None,
    ) -> None:
        """SQLAlchemy PoolEvents.checkout hook acquiring a lease."""
        _ = (connection_record, connection_proxy)
        conn_id = id(dbapi_connection)
        lease = self.transport.acquire(bind_key="sqlalchemy")
        with self._lock:
            self._active_leases[conn_id] = lease

    def on_checkin(
        self,
        dbapi_connection: Any,
        connection_record: Any = None,
    ) -> None:
        """SQLAlchemy PoolEvents.checkin hook releasing the lease."""
        _ = connection_record
        conn_id = id(dbapi_connection)
        with self._lock:
            lease = self._active_leases.pop(conn_id, None)
        if lease is not None:
            self.transport.release(lease.lease_id)

    def attach_to_engine(self, engine: Any) -> None:
        """Attach checkout and checkin listeners to a SQLAlchemy engine."""
        try:
            sa_event: Any = importlib.import_module("sqlalchemy.event")
            sa_event.listen(engine.pool, "checkout", self.on_checkout)
            sa_event.listen(engine.pool, "checkin", self.on_checkin)
        except (ImportError, AttributeError):
            pass


class StatementCache:
    """Pre-compilation and parameter ordering cache for SQLAlchemy statements."""

    __slots__ = ("_cache",)

    def __init__(self) -> None:
        """Initialize empty statement plan cache."""
        self._cache: dict[str, tuple[str, tuple[str, ...]]] = {}

    def compile_and_encode(self, statement: Any) -> tuple[str, tuple[str, ...]]:
        """Compile a statement or ClauseElement and extract SQL text and parameter order."""
        compile_fn = getattr(statement, "compile", None)
        if callable(compile_fn):
            compiled = compile_fn()
            sql_text = str(compiled)
            param_order = tuple(getattr(compiled, "positiontup", None) or tuple(getattr(compiled, "params", {}).keys()))
            return sql_text, param_order
        sql_text = str(statement)
        return sql_text, ()
