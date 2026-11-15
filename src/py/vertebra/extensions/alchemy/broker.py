"""SQLAlchemy session connection leasing broker and transport integration."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import threading
import time
from collections import deque
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from vertebra.extensions.broker import (
    AsyncConnectionLease,
    AsyncSHMBrokerTransport,
    BrokerConfig,
    BrokerMetrics,
    BrokerPlugin,
    ChannelMessage,
    StatementPlan,
    SyncConnectionLease,
    SyncSHMBrokerTransport,
)
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

__all__ = (
    "AsyncBrokerTransport",
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "AsyncSHMSessionBroker",
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "ChannelMessage",
    "StatementPlan",
    "SyncBrokerTransport",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
    "SyncSHMSessionBroker",
)


@runtime_checkable
class SyncBrokerTransport(Protocol):
    """Protocol implemented by synchronous connection broker transports."""

    def acquire(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a connection lease synchronously."""
        ...

    def release(self, *args: Any, **kwargs: Any) -> Any:
        """Release an active connection lease."""
        ...

    def metrics(self) -> Any:
        """Return a snapshot of current broker metrics."""
        ...


@runtime_checkable
class AsyncBrokerTransport(Protocol):
    """Protocol implemented by asynchronous connection broker transports."""

    async def acquire(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a connection lease asynchronously."""
        ...

    async def release(self, *args: Any, **kwargs: Any) -> Any:
        """Release an active connection lease asynchronously."""
        ...

    def metrics(self) -> Any:
        """Return a snapshot of current broker metrics."""
        ...


class _BaseSHMSessionBroker:
    """Shared state and coordinates extraction for session broker."""

    _DEFAULT_TRANSPORT_CLS: type[Any] = SyncSHMBrokerTransport

    __slots__ = (
        "_active_leases",
        "_capacity",
        "_in_use",
        "_lock",
        "bind_key",
        "transport",
    )

    def __init__(
        self,
        transport: Any = None,
        config: BrokerConfig | None = None,
        *,
        capacity: int = 10,
        bind_key: str = "sqlalchemy",
    ) -> None:
        if transport is None:
            resolved_capacity = max(1, capacity)
            resolved_transport: Any = self._DEFAULT_TRANSPORT_CLS(config)
        elif isinstance(transport, int):
            resolved_capacity = max(1, transport)
            resolved_transport = self._DEFAULT_TRANSPORT_CLS(
                config or BrokerConfig(pool_size=resolved_capacity, max_overflow=0)
            )
        else:
            resolved_capacity = max(1, capacity)
            resolved_transport = transport
        self.transport = resolved_transport
        self.bind_key = bind_key
        self._capacity = resolved_capacity
        self._in_use = 0
        self._active_leases: dict[int | str, Any] = {}
        self._lock = threading.Lock()

    @property
    def capacity(self) -> int:
        """Return maximum concurrent session capacity."""
        return self._capacity

    @property
    def in_use(self) -> int:
        """Return number of currently active session leases."""
        with self._lock:
            return self._in_use

    def on_before_cursor_execute(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        """SQLAlchemy ConnectionEvents.before_cursor_execute hook populating the statement cache."""
        _ = (cursor, parameters, context, executemany)
        compile_fn = getattr(self.transport, "get_or_compile_statement", None)
        if callable(compile_fn) and isinstance(statement, str) and statement:
            dialect_name = str(getattr(getattr(conn, "dialect", None), "name", self.bind_key))
            compile_fn(dialect_name, statement, lambda sql: (sql, ()))

    def attach_to_engine(self, engine: Any) -> None:
        """Attach checkout, checkin, and before_cursor_execute listeners to a SQLAlchemy engine."""
        sa_event = import_optional("sqlalchemy.event")
        if sa_event is None:
            return
        target_engine = getattr(engine, "sync_engine", engine)
        pool = getattr(target_engine, "pool", None)
        if pool is not None:
            with contextlib.suppress(AttributeError, TypeError, ValueError):
                sa_event.listen(pool, "checkout", self.on_checkout)
                sa_event.listen(pool, "checkin", self.on_checkin)
        with contextlib.suppress(AttributeError, TypeError, ValueError):
            sa_event.listen(target_engine, "before_cursor_execute", self.on_before_cursor_execute)

    def on_checkout(
        self,
        dbapi_connection: Any,
        connection_record: Any = None,
        connection_proxy: Any = None,
    ) -> None:
        """SQLAlchemy PoolEvents.checkout hook acquiring a lease."""
        _ = (connection_record, connection_proxy)
        conn_id = id(dbapi_connection)
        lease = self._acquire_lease_impl(pool_name=self.bind_key)
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
            self._release_lease_impl(lease, in_tx=False, pool_name=self.bind_key)

    def _on_slot_released_locked(self) -> None:
        """Notify any synchronous waiter when a session slot is released while holding self._lock."""

    @staticmethod
    def _call_transport_acquire(
        fn: Any,
        pool_name: str,
        worker_pid: int,
        timeout_ms: int | None,
    ) -> Any:
        """Invoke a transport acquire callable matching its parameter signature."""
        try:
            sig = inspect.signature(fn)
            params = sig.parameters
            kwargs: dict[str, Any] = {}
            if "pool_name" in params:
                kwargs["pool_name"] = pool_name
            elif "bind_key" in params:
                kwargs["bind_key"] = pool_name
            if "worker_pid" in params:
                kwargs["worker_pid"] = worker_pid
            elif "worker_id" in params:
                kwargs["worker_id"] = worker_pid
            if timeout_ms is not None and "timeout_ms" in params:
                kwargs["timeout_ms"] = timeout_ms
            if kwargs or not params:
                return fn(**kwargs)
        except (ValueError, TypeError):
            pass
        return fn()

    @staticmethod
    def _lease_key(lease: Any) -> int | str:
        """Return a stable dictionary key for a lease object, tuple, or identifier."""
        if isinstance(lease, (int, str)):
            return lease
        lease_id = getattr(lease, "lease_id", None)
        if isinstance(lease_id, str):
            return lease_id
        if isinstance(lease, tuple) and len(lease) >= 2:
            return f"{lease[0]}:{lease[1]}"
        slot_idx = getattr(lease, "slot_idx", getattr(lease, "slot_index", None))
        generation = getattr(lease, "generation", None)
        if slot_idx is not None and generation is not None:
            return f"{slot_idx}:{generation}"
        return id(lease)

    @staticmethod
    def _extract_coords(lease: Any, generation: int | None = None) -> tuple[int, int]:
        """Extract (slot_idx, generation) integers from a lease representation."""
        if isinstance(lease, int) and generation is not None:
            return lease, generation
        if isinstance(lease, tuple) and len(lease) >= 2:
            return int(lease[0]), int(lease[1])
        if isinstance(lease, str) and ":" in lease:
            parts = lease.split(":", 1)
            return int(parts[0]), int(parts[1])
        slot_idx = int(getattr(lease, "slot_idx", getattr(lease, "slot_index", 0)))
        gen = int(getattr(lease, "generation", generation or 1))
        return slot_idx, gen

    def _acquire_lease_impl(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Acquire a connection lease from the underlying transport synchronously."""
        eff_pool = pool_name or self.bind_key
        pid = worker_pid if worker_pid is not None else os.getpid()
        acquire_lease_fn = getattr(self.transport, "acquire_lease", None)
        acquire_fn = getattr(self.transport, "acquire", None)
        acquire_sync_fn = getattr(self.transport, "_acquire_sync", None)
        if callable(acquire_lease_fn) and not inspect.iscoroutinefunction(acquire_lease_fn):
            lease = self._call_transport_acquire(acquire_lease_fn, eff_pool, pid, timeout_ms)
        elif callable(acquire_fn) and not inspect.iscoroutinefunction(acquire_fn):
            lease = self._call_transport_acquire(acquire_fn, eff_pool, pid, timeout_ms)
        elif callable(acquire_sync_fn):
            lease = self._call_transport_acquire(acquire_sync_fn, eff_pool, pid, timeout_ms)
        else:
            msg = "Underlying broker transport does not implement synchronous acquire or acquire_lease"
            raise RuntimeError(msg)
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
            self._in_use += 1
        return lease

    def _release_lease_impl(
        self,
        lease_or_slot: Any = None,
        generation: int | None = None,
        *,
        in_tx: bool = False,
        pool_name: str | None = None,
    ) -> bool:
        """Release an acquired connection lease back to the underlying transport."""
        _ = pool_name
        key = (
            f"{lease_or_slot}:{generation}"
            if generation is not None and isinstance(lease_or_slot, int)
            else self._lease_key(lease_or_slot)
        )
        with self._lock:
            tracked = self._active_leases.pop(key, None)
            if self._in_use > 0:
                self._in_use -= 1
            self._on_slot_released_locked()

        target_lease = tracked if tracked is not None else lease_or_slot
        release_lease_fn = getattr(self.transport, "release_lease", None)
        if callable(release_lease_fn) and not inspect.iscoroutinefunction(release_lease_fn):
            slot_idx, gen = self._extract_coords(target_lease, generation)
            return bool(release_lease_fn(slot_idx, gen, in_tx=in_tx))

        lease_release_fn = getattr(target_lease, "release", None)
        if callable(lease_release_fn) and not inspect.iscoroutinefunction(lease_release_fn):
            lease_release_fn()
            return True

        release_fn = getattr(self.transport, "release", None)
        if callable(release_fn) and not inspect.iscoroutinefunction(release_fn):
            if isinstance(target_lease, str):
                return bool(release_fn(target_lease))
            slot_idx, gen = self._extract_coords(target_lease, generation)
            return bool(release_fn(f"{slot_idx}:{gen}"))

        release_sync_fn = getattr(self.transport, "_release_sync", None)
        if callable(release_sync_fn):
            if isinstance(target_lease, str):
                return bool(release_sync_fn(target_lease))
            slot_idx, gen = self._extract_coords(target_lease, generation)
            return bool(release_sync_fn(f"{slot_idx}:{gen}"))
        return False


class SyncSHMSessionBroker(_BaseSHMSessionBroker):
    """SQLAlchemy synchronous connection pool and session broker backed by shared-memory lease table."""

    _DEFAULT_TRANSPORT_CLS = SyncSHMBrokerTransport

    __slots__ = ("_cond",)

    def __init__(
        self,
        transport: Any = None,
        config: BrokerConfig | None = None,
        *,
        capacity: int = 10,
        bind_key: str = "sqlalchemy",
    ) -> None:
        super().__init__(
            transport=transport,
            config=config,
            capacity=capacity,
            bind_key=bind_key,
        )
        self._cond = threading.Condition(self._lock)

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a session slot synchronously."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while self._in_use >= self._capacity:
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cond.wait(timeout=remaining)
                else:
                    self._cond.wait()
            self._in_use += 1
            return True

    def release(self) -> None:
        """Release a session slot and wake the next pending waiter."""
        with self._cond:
            if self._in_use > 0:
                self._in_use -= 1
            self._cond.notify()

    def acquire_lease(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Acquire a connection lease from the underlying transport synchronously."""
        return self._acquire_lease_impl(pool_name=pool_name, worker_pid=worker_pid, timeout_ms=timeout_ms)

    def release_lease(
        self,
        lease_or_slot: Any = None,
        generation: int | None = None,
        *,
        in_tx: bool = False,
        pool_name: str | None = None,
    ) -> bool:
        """Release an acquired connection lease back to the underlying transport."""
        return self._release_lease_impl(lease_or_slot, generation, in_tx=in_tx, pool_name=pool_name)

    @contextlib.contextmanager
    def provide_session(
        self,
        session_factory: Any,
        *,
        pool_name: str | None = None,
        worker_pid: int | None = None,
    ) -> Iterator[Any]:
        """Acquire a lease, yield a synchronous session, roll back on error, and release in finally."""
        lease = self.acquire_lease(pool_name=pool_name, worker_pid=worker_pid)
        session = (
            session_factory.provide_session()
            if hasattr(session_factory, "provide_session")
            else (session_factory() if callable(session_factory) else session_factory)
        )
        released = False
        try:
            yield session
        except BaseException:
            rollback_fn = getattr(session, "rollback", None)
            if callable(rollback_fn):
                rollback_fn()
            self.release_lease(lease, in_tx=False, pool_name=pool_name)
            released = True
            raise
        finally:
            close_fn = getattr(session, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()
            if not released:
                self.release_lease(lease, in_tx=False, pool_name=pool_name)

    def _on_slot_released_locked(self) -> None:
        """Wake one waiting thread when a lease is released."""
        self._cond.notify()


class AsyncSHMSessionBroker(_BaseSHMSessionBroker):
    """SQLAlchemy asynchronous connection pool and session broker backed by shared-memory lease table."""

    _DEFAULT_TRANSPORT_CLS = AsyncSHMBrokerTransport

    __slots__ = ("_waiters",)

    def __init__(
        self,
        transport: Any = None,
        config: BrokerConfig | None = None,
        *,
        capacity: int = 10,
        bind_key: str = "sqlalchemy",
    ) -> None:
        super().__init__(
            transport=transport,
            config=config,
            capacity=capacity,
            bind_key=bind_key,
        )
        self._waiters: deque[asyncio.Future[None]] = deque()

    async def acquire(self, timeout: float | None = None) -> bool:
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

    async def release(self) -> None:
        """Release a session slot asynchronously holding the lock."""
        with self._lock:
            if self._in_use > 0:
                self._in_use -= 1
            while self._waiters:
                next_waiter = self._waiters.popleft()
                if not next_waiter.done():
                    self._in_use += 1
                    next_waiter.set_result(None)
                    break

    async def acquire_lease(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Acquire a connection lease from the underlying transport asynchronously."""
        eff_pool = pool_name or self.bind_key
        pid = worker_pid if worker_pid is not None else os.getpid()
        lease: Any = None
        for attr in ("acquire_lease", "acquire"):
            fn = getattr(self.transport, attr, None)
            if callable(fn):
                res = self._call_transport_acquire(fn, eff_pool, pid, timeout_ms)
                lease = await res if inspect.isawaitable(res) else res
                break
        if lease is None:
            msg = "Underlying broker transport does not implement lease acquisition"
            raise RuntimeError(msg)
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
            self._in_use += 1
        return lease

    async def release_lease(
        self,
        lease_or_slot: Any = None,
        generation: int | None = None,
        *,
        in_tx: bool = False,
        pool_name: str | None = None,
    ) -> bool:
        """Release an acquired connection lease back to the underlying transport asynchronously."""
        _ = pool_name
        key = (
            f"{lease_or_slot}:{generation}"
            if generation is not None and isinstance(lease_or_slot, int)
            else self._lease_key(lease_or_slot)
        )
        with self._lock:
            tracked = self._active_leases.pop(key, None)
            if self._in_use > 0:
                self._in_use -= 1
            while self._waiters:
                next_waiter = self._waiters.popleft()
                if not next_waiter.done():
                    self._in_use += 1
                    next_waiter.set_result(None)
                    break

        target_lease = tracked if tracked is not None else lease_or_slot
        release_lease_fn = getattr(self.transport, "release_lease", None)
        if callable(release_lease_fn):
            slot_idx, gen = self._extract_coords(target_lease, generation)
            res = release_lease_fn(slot_idx, gen, in_tx=in_tx)
            return bool(await res if inspect.isawaitable(res) else res)

        if hasattr(target_lease, "release") and callable(target_lease.release):
            res = target_lease.release()
            if inspect.isawaitable(res):
                await res
            return True

        release_fn = getattr(self.transport, "release", None)
        if callable(release_fn):
            if isinstance(target_lease, str):
                res = release_fn(target_lease)
            else:
                slot_idx, gen = self._extract_coords(target_lease, generation)
                res = release_fn(f"{slot_idx}:{gen}")
            return bool(await res if inspect.isawaitable(res) else res)
        return False

    @contextlib.asynccontextmanager
    async def provide_session(
        self,
        session_factory: Any,
        *,
        pool_name: str | None = None,
        worker_pid: int | None = None,
    ) -> AsyncIterator[Any]:
        """Acquire a lease asynchronously, yield an async session, roll back on error, and release in finally."""
        lease = await self.acquire_lease(pool_name=pool_name, worker_pid=worker_pid)
        raw_session = (
            session_factory.provide_session()
            if hasattr(session_factory, "provide_session")
            else (session_factory() if callable(session_factory) else session_factory)
        )
        session = await raw_session if inspect.isawaitable(raw_session) else raw_session
        released = False
        try:
            yield session
        except BaseException:
            rollback_fn = getattr(session, "rollback", None)
            if callable(rollback_fn):
                res = rollback_fn()
                if inspect.isawaitable(res):
                    await res
            await self.release_lease(lease, in_tx=False, pool_name=pool_name)
            released = True
            raise
        finally:
            close_fn = getattr(session, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    res = close_fn()
                    if inspect.isawaitable(res):
                        await res
            if not released:
                await self.release_lease(lease, in_tx=False, pool_name=pool_name)
