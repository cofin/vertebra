"""SQLAlchemy session connection leasing broker and transport integration."""

from __future__ import annotations

import contextlib
import inspect
import os
import threading
from typing import TYPE_CHECKING, Any, ClassVar

from vertebra.db.driver import InterfaceError
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
from vertebra.extensions.broker.transport import (
    AsyncBrokerLeaseSource,
    SyncBrokerLeaseSource,
)
from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

__all__ = (
    "AsyncBrokerLeaseSource",
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "AsyncSHMSessionBroker",
    "BrokerConfig",
    "BrokerMetrics",
    "BrokerPlugin",
    "ChannelMessage",
    "StatementPlan",
    "SyncBrokerLeaseSource",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
    "SyncSHMSessionBroker",
)


class _BaseSHMSessionBroker:
    """Shared state and lease tracking for SQLAlchemy session brokers."""

    _DEFAULT_TRANSPORT_CLS: ClassVar[type[SyncSHMBrokerTransport] | type[AsyncSHMBrokerTransport]] = (
        SyncSHMBrokerTransport
    )

    __slots__ = (
        "_active_leases",
        "_capacity",
        "_conn_leases",
        "_lease_stack",
        "_lock",
        "bind_key",
        "transport",
    )

    def __init__(
        self,
        transport: SyncBrokerLeaseSource | AsyncBrokerLeaseSource | int | None = None,
        config: BrokerConfig | None = None,
        *,
        capacity: int = 10,
        bind_key: str = "sqlalchemy",
    ) -> None:
        if transport is None:
            resolved_capacity = max(1, capacity)
            resolved_transport: (
                SyncSHMBrokerTransport | AsyncSHMBrokerTransport | SyncBrokerLeaseSource | AsyncBrokerLeaseSource
            ) = self._DEFAULT_TRANSPORT_CLS(config or BrokerConfig(pool_size=resolved_capacity, max_overflow=0))
        elif isinstance(transport, int):
            resolved_capacity = max(1, transport)
            resolved_transport = self._DEFAULT_TRANSPORT_CLS(
                config or BrokerConfig(pool_size=resolved_capacity, max_overflow=0)
            )
        else:
            resolved_capacity = max(1, capacity)
            resolved_transport = transport
        self.transport: (
            SyncSHMBrokerTransport | AsyncSHMBrokerTransport | SyncBrokerLeaseSource | AsyncBrokerLeaseSource
        ) = resolved_transport
        self.bind_key = bind_key
        self._capacity = resolved_capacity
        self._active_leases: dict[int | str, object] = {}
        self._conn_leases: dict[int, int | str] = {}
        self._lease_stack: list[int | str] = []
        self._lock = threading.Lock()

    @property
    def capacity(self) -> int:
        """Return maximum concurrent session capacity."""
        return self._capacity

    @property
    def in_use(self) -> int:
        """Return number of currently active session leases."""
        metrics_fn = getattr(self.transport, "metrics", None)
        if callable(metrics_fn):
            snapshot = metrics_fn()
            active = getattr(snapshot, "active_leases", None)
            if active is not None:
                return int(active)
        with self._lock:
            return len(self._active_leases)

    def attach_to_engine(self, engine: object) -> None:
        """Attach checkout and checkin listeners to a SQLAlchemy engine."""
        sa_event = import_optional("sqlalchemy.event")
        if sa_event is None:
            return
        target_engine = getattr(engine, "sync_engine", engine)
        pool = getattr(target_engine, "pool", None)
        if pool is not None:
            with contextlib.suppress(AttributeError, TypeError, ValueError):
                sa_event.listen(pool, "checkout", self.on_checkout)
                sa_event.listen(pool, "checkin", self.on_checkin)

    def on_checkout(
        self,
        dbapi_connection: object,
        connection_record: object = None,
        connection_proxy: object = None,
    ) -> None:
        """SQLAlchemy PoolEvents.checkout hook acquiring a lease."""
        _ = (connection_record, connection_proxy)
        conn_id = id(dbapi_connection)
        lease = self._acquire_transport_sync(pool_name=self.bind_key, worker_pid=os.getpid())
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
            self._conn_leases[conn_id] = key

    def on_checkin(
        self,
        dbapi_connection: object,
        connection_record: object = None,
    ) -> None:
        """SQLAlchemy PoolEvents.checkin hook releasing the lease."""
        _ = connection_record
        conn_id = id(dbapi_connection)
        with self._lock:
            key = self._conn_leases.pop(conn_id, None)
            lease = self._active_leases.pop(key, None) if key is not None else None
        if lease is not None:
            self._release_sync_target(lease, generation=None)

    @staticmethod
    def _lease_key(lease: object) -> int | str:
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
    def _extract_coords(lease: object, generation: int | None = None) -> tuple[int, int]:
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

    def _acquire_transport_sync(
        self,
        pool_name: str,
        worker_pid: int,
        timeout_ms: int | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Acquire a lease directly from the synchronous transport without signature inspection."""
        if isinstance(self.transport, SyncSHMBrokerTransport):
            return self.transport.acquire(
                worker_id=worker_pid,
                timeout_ms=timeout_ms,
                bind_key=pool_name,
                timeout=timeout,
            )
        acquire_fn = getattr(self.transport, "acquire", None)
        if callable(acquire_fn) and not inspect.iscoroutinefunction(acquire_fn):
            return acquire_fn(bind_key=pool_name, worker_id=worker_pid)
        msg = "Underlying broker transport does not implement synchronous acquire"
        raise RuntimeError(msg)

    def _release_sync_target(
        self,
        target_lease: object,
        generation: int | None,
        *,
        in_tx: bool = False,
    ) -> bool:
        """Release a tracked lease via ``lease.release()`` or transport fallback."""
        lease_release_fn = getattr(target_lease, "release", None)
        if callable(lease_release_fn) and not inspect.iscoroutinefunction(lease_release_fn):
            lease_release_fn()
            return True

        release_lease_fn = getattr(self.transport, "release_lease", None)
        if callable(release_lease_fn) and not inspect.iscoroutinefunction(release_lease_fn):
            slot_idx, gen = self._extract_coords(target_lease, generation)
            return bool(release_lease_fn(slot_idx, gen, in_tx=in_tx))

        release_fn = getattr(self.transport, "release", None)
        if callable(release_fn) and not inspect.iscoroutinefunction(release_fn):
            if isinstance(target_lease, str):
                return bool(release_fn(target_lease))
            slot_idx, gen = self._extract_coords(target_lease, generation)
            return bool(release_fn(f"{slot_idx}:{gen}"))
        return False

    def _acquire_lease_impl(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Acquire a connection lease from the underlying transport synchronously."""
        eff_pool = pool_name or self.bind_key
        pid = worker_pid if worker_pid is not None else os.getpid()
        lease = self._acquire_transport_sync(
            pool_name=eff_pool,
            worker_pid=pid,
            timeout_ms=timeout_ms,
        )
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
        return lease

    def _release_lease_impl(
        self,
        lease_or_slot: object = None,
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

        target_lease = tracked if tracked is not None else lease_or_slot
        return self._release_sync_target(target_lease, generation, in_tx=in_tx)


class SyncSHMSessionBroker(_BaseSHMSessionBroker):
    """SQLAlchemy synchronous connection pool and session broker backed by shared-memory lease table."""

    _DEFAULT_TRANSPORT_CLS = SyncSHMBrokerTransport

    __slots__ = ()

    def __init__(
        self,
        transport: SyncBrokerLeaseSource | int | None = None,
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

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a session lease synchronously from the underlying transport."""
        try:
            lease = self._acquire_transport_sync(
                pool_name=self.bind_key,
                worker_pid=os.getpid(),
                timeout=timeout,
            )
        except TimeoutError:
            return False
        if lease is None:
            return False
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
            self._lease_stack.append(key)
        return True

    def release(self) -> None:
        """Release one held session lease back to the underlying transport."""
        lease: object = None
        with self._lock:
            while self._lease_stack:
                key = self._lease_stack.pop()
                lease = self._active_leases.pop(key, None)
                if lease is not None:
                    break
        if lease is not None:
            self._release_sync_target(lease, generation=None)

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
        lease_or_slot: object = None,
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


class AsyncSHMSessionBroker(_BaseSHMSessionBroker):
    """SQLAlchemy asynchronous connection pool and session broker backed by shared-memory lease table."""

    _DEFAULT_TRANSPORT_CLS = AsyncSHMBrokerTransport

    __slots__ = ()

    def __init__(
        self,
        transport: AsyncBrokerLeaseSource | SyncBrokerLeaseSource | int | None = None,
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

    def on_checkout(
        self,
        dbapi_connection: object,
        connection_record: object = None,
        connection_proxy: object = None,
    ) -> None:
        """Reject synchronous SQLAlchemy checkout hooks on an asynchronous session broker."""
        _ = (dbapi_connection, connection_record, connection_proxy)
        msg = "AsyncSHMSessionBroker does not support synchronous pool checkout hooks; use SyncSHMSessionBroker"
        raise InterfaceError(msg)

    def on_checkin(
        self,
        dbapi_connection: object,
        connection_record: object = None,
    ) -> None:
        """Reject synchronous SQLAlchemy checkin hooks on an asynchronous session broker."""
        _ = (dbapi_connection, connection_record)
        msg = "AsyncSHMSessionBroker does not support synchronous pool checkin hooks; use SyncSHMSessionBroker"
        raise InterfaceError(msg)

    async def _acquire_transport_async(
        self,
        pool_name: str,
        worker_pid: int,
        timeout_ms: int | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Acquire a lease from the asynchronous or compatible transport without signature inspection."""
        if isinstance(self.transport, (AsyncSHMBrokerTransport, SyncSHMBrokerTransport)):
            res = self.transport.acquire(
                worker_id=worker_pid,
                timeout_ms=timeout_ms,
                bind_key=pool_name,
                timeout=timeout,
            )
            return await res if inspect.isawaitable(res) else res
        acquire_fn = getattr(self.transport, "acquire", None)
        if callable(acquire_fn):
            res = acquire_fn(bind_key=pool_name, worker_id=worker_pid)
            return await res if inspect.isawaitable(res) else res
        msg = "Underlying broker transport does not implement lease acquisition"
        raise RuntimeError(msg)

    async def _release_async_target(
        self,
        target_lease: object,
        generation: int | None,
        *,
        in_tx: bool = False,
    ) -> bool:
        """Release a tracked lease via ``lease.release()`` or transport fallback."""
        lease_release_fn = getattr(target_lease, "release", None)
        if callable(lease_release_fn):
            res = lease_release_fn()
            if inspect.isawaitable(res):
                await res
            return True

        release_lease_fn = getattr(self.transport, "release_lease", None)
        if callable(release_lease_fn):
            slot_idx, gen = self._extract_coords(target_lease, generation)
            res = release_lease_fn(slot_idx, gen, in_tx=in_tx)
            return bool(await res if inspect.isawaitable(res) else res)

        release_fn = getattr(self.transport, "release", None)
        if callable(release_fn):
            if isinstance(target_lease, str):
                res = release_fn(target_lease)
            else:
                slot_idx, gen = self._extract_coords(target_lease, generation)
                res = release_fn(f"{slot_idx}:{gen}")
            return bool(await res if inspect.isawaitable(res) else res)
        return False

    async def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a session lease asynchronously from the underlying transport."""
        try:
            lease = await self._acquire_transport_async(
                pool_name=self.bind_key,
                worker_pid=os.getpid(),
                timeout=timeout,
            )
        except TimeoutError:
            return False
        if lease is None:
            return False
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
            self._lease_stack.append(key)
        return True

    async def release(self) -> None:
        """Release one held session lease back to the underlying transport asynchronously."""
        lease: object = None
        with self._lock:
            while self._lease_stack:
                key = self._lease_stack.pop()
                lease = self._active_leases.pop(key, None)
                if lease is not None:
                    break
        if lease is not None:
            await self._release_async_target(lease, generation=None)

    async def acquire_lease(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        timeout_ms: int | None = None,
    ) -> Any:
        """Acquire a connection lease from the underlying transport asynchronously."""
        eff_pool = pool_name or self.bind_key
        pid = worker_pid if worker_pid is not None else os.getpid()
        lease = await self._acquire_transport_async(
            pool_name=eff_pool,
            worker_pid=pid,
            timeout_ms=timeout_ms,
        )
        key = self._lease_key(lease)
        with self._lock:
            self._active_leases[key] = lease
        return lease

    async def release_lease(
        self,
        lease_or_slot: object = None,
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

        target_lease = tracked if tracked is not None else lease_or_slot
        return await self._release_async_target(target_lease, generation, in_tx=in_tx)

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
