"""SQLAlchemy connection pools backed by Vertebra SHM lease brokers."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import weakref
from collections.abc import Awaitable, Callable, Coroutine
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import unquote

from sqlalchemy import event as sa_event
from sqlalchemy.pool import ConnectionPoolEntry, Pool
from sqlalchemy.pool.base import _AsyncConnDialect
from sqlalchemy.util import await_only
from sqlalchemy.util.concurrency import in_greenlet

from vertebra.db.dbapi import SyncDBAPIConnection
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.sync_tools import SyncPool, run_
from vertebra.utils.type_guards import is_coroutine_callable

if TYPE_CHECKING:
    from sqlalchemy.pool.base import _ConnDialect

__all__ = (
    "AsyncBrokerPool",
    "BrokerPool",
    "broker_creator",
)

logger = logging.getLogger(__name__)


def broker_creator(
    url: str,
    *,
    broker: Any,
    share_pool: bool = False,
    is_async: bool = False,
    connect_fn: Callable[[], Any] | None = None,
    **connect_kwargs: Any,
) -> Callable[[], Any]:
    """Build a zero-argument SQLAlchemy connection creator bound to ``url`` and ``broker``."""
    if broker is None:
        raise ImproperConfigurationError("broker_creator requires a configured broker transport or session broker")
    if not url or not url.strip():
        raise ImproperConfigurationError("broker_creator requires a non-empty database URL")

    if connect_fn is not None:
        return connect_fn

    conn_kwargs = dict(connect_kwargs)
    conn_kwargs.setdefault("lease_managed", True)
    if is_async:
        module = SQLAlchemyAsyncDBAPI()
        return lambda: module.connect(url, broker=broker, share_pool=share_pool, **conn_kwargs)

    return lambda: SyncDBAPIConnection(url, broker=broker, share_pool=share_pool, **conn_kwargs)


class BrokerPool(Pool):
    """Synchronous SQLAlchemy ``Pool`` leasing connections per config ``pool_key()`` partition."""

    _bound_broker: ClassVar[Any] = None
    _bound_config: ClassVar[Any] = None
    _bound_url: ClassVar[str | None] = None
    _bound_pool_key: ClassVar[str | None] = None
    _bound_worker_id: ClassVar[int | None] = None
    _bound_acquire_timeout_ms: ClassVar[int | None] = None
    _bound_on_rollback: ClassVar[Callable[[Any], None] | None] = None
    _bound_on_dispose: ClassVar[Callable[[Any], None] | None] = None
    _bound_counter: ClassVar[int] = 0
    _bound_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(
        self,
        creator: Callable[..., Any] | None = None,
        *,
        broker: Any = None,
        config: Any = None,
        url: str | None = None,
        pool_key: str | None = None,
        worker_id: int | None = None,
        acquire_timeout_ms: int | None = None,
        on_rollback: Callable[[Any], None] | None = None,
        on_dispose: Callable[[Any], None] | None = None,
        reset_on_return: Any = False,
        **pool_kw: Any,
    ) -> None:
        cls = type(self)
        resolved_broker = broker if broker is not None else cls._bound_broker
        resolved_config = config if config is not None else cls._bound_config
        resolved_url = url if url is not None else cls._bound_url
        resolved_key = pool_key if pool_key is not None else cls._bound_pool_key
        resolved_worker = worker_id if worker_id is not None else cls._bound_worker_id
        resolved_timeout = acquire_timeout_ms if acquire_timeout_ms is not None else cls._bound_acquire_timeout_ms
        resolved_on_rollback = on_rollback if on_rollback is not None else cls._bound_on_rollback
        resolved_on_dispose = on_dispose if on_dispose is not None else cls._bound_on_dispose

        if resolved_broker is None:
            raise ImproperConfigurationError("BrokerPool requires a configured broker transport or session broker")

        if resolved_config is None and resolved_url:
            try:
                resolved_config = DatabaseRegistry.parse_url(resolved_url, broker=resolved_broker)
            except ValueError:
                resolved_config = None

        partition_key = resolved_key
        if not partition_key and resolved_config is not None:
            pool_key_fn = getattr(resolved_config, "pool_key", None)
            if callable(pool_key_fn):
                formatted = self._format_pool_key(pool_key_fn())
                if formatted:
                    partition_key = formatted
        if not partition_key and resolved_url:
            decoded_url = unquote(resolved_url)
            is_memory = (
                ":memory:" in decoded_url or "mode=memory" in decoded_url or decoded_url.rstrip("/").endswith("memory:")
            )
            partition_key = f"{resolved_url}#pool-{id(self)}" if is_memory else resolved_url
        if not partition_key:
            raise ImproperConfigurationError("BrokerPool requires a config, url, or explicit pool_key")

        resolved_creator = creator
        if resolved_creator is None:
            if not resolved_url:
                raise ImproperConfigurationError("BrokerPool requires a creator callable or database url")
            resolved_creator = broker_creator(
                resolved_url,
                broker=resolved_broker,
                is_async=self._is_asyncio,
            )

        self._broker = resolved_broker
        self._config = resolved_config
        self._url = resolved_url
        self._partition_key = partition_key
        self._pool_key_override = resolved_key
        self._worker_id = resolved_worker
        self._acquire_timeout_ms = resolved_timeout
        self._on_rollback = resolved_on_rollback
        self._on_dispose = resolved_on_dispose
        self._pool_size: int | None = pool_kw.pop("pool_size", None)
        self._max_overflow: int | None = pool_kw.pop("max_overflow", None)
        self._reset_mode: Any = reset_on_return
        self._lease_lock = threading.Lock()
        self._register_lock = threading.Lock()
        self._async_register_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
            weakref.WeakKeyDictionary()
        )
        self._partition_registered = False
        self._entry_leases: dict[int, tuple[ConnectionPoolEntry, Any]] = {}

        super().__init__(resolved_creator, reset_on_return=False, **pool_kw)
        if "_dispatch" not in pool_kw:
            sa_event.listen(self, "invalidate", BrokerPool._on_connection_invalidate)

    @property
    def partition_key(self) -> str:
        """Return the broker partition identifier bound to this pool."""
        return self._partition_key

    @classmethod
    def bind_class(
        cls,
        *,
        broker: Any,
        config: Any = None,
        url: str | None = None,
        pool_key: str | None = None,
        worker_id: int | None = None,
        acquire_timeout_ms: int | None = None,
        on_rollback: Callable[[Any], None] | None = None,
        on_dispose: Callable[[Any], None] | None = None,
    ) -> type[BrokerPool]:
        """Bind broker parameters onto a subclass compatible with SQLAlchemy's ``poolclass`` API."""
        if broker is None:
            raise ImproperConfigurationError("BrokerPool.bind_class requires a non-None broker")
        with BrokerPool._bound_lock:
            BrokerPool._bound_counter += 1
            sub_name = f"{cls.__name__}Bound{BrokerPool._bound_counter}"
        bound_cls: Any = type(
            sub_name,
            (cls,),
            {
                "_bound_broker": broker,
                "_bound_config": config,
                "_bound_url": url,
                "_bound_pool_key": pool_key,
                "_bound_worker_id": worker_id,
                "_bound_acquire_timeout_ms": acquire_timeout_ms,
                "_bound_on_rollback": staticmethod(on_rollback) if on_rollback is not None else None,
                "_bound_on_dispose": staticmethod(on_dispose) if on_dispose is not None else None,
            },
        )
        return cast("type[BrokerPool]", bound_cls)

    def status(self) -> str:
        """Return human-readable pool status including active lease count."""
        with self._lease_lock:
            leased = len(self._entry_leases)
        return f"{type(self).__name__}(partition={self._partition_key!r}, active_leases={leased})"

    def recreate(self) -> BrokerPool:
        """Return a freshly initialized copy of this pool preserving broker configuration."""
        return self.__class__(
            self._creator,
            broker=self._broker,
            config=self._config,
            url=self._url,
            pool_key=self._pool_key_override or self._partition_key,
            worker_id=self._worker_id,
            acquire_timeout_ms=self._acquire_timeout_ms,
            on_rollback=self._on_rollback,
            on_dispose=self._on_dispose,
            pool_size=self._pool_size,
            max_overflow=self._max_overflow,
            recycle=self._recycle,
            echo=self.echo,
            logging_name=self._orig_logging_name,
            reset_on_return=self._reset_mode,
            pre_ping=self._pre_ping,
            _dispatch=self.dispatch,
            dialect=self._dialect,
        )

    def dispose(self) -> None:
        """Quarantine active checked-out leases safely during pool disposal."""
        with self._lease_lock:
            remaining = list(self._entry_leases.values())
            self._entry_leases.clear()
        first_error: BaseException | None = None
        for entry, lease in remaining:
            entry.info.pop("vtb_broker_pool", None)
            self._close_entry_safely(entry)
            try:
                self._quarantine_lease(lease, allow_sync_fallback=True)
                if self._on_dispose is not None:
                    self._on_dispose(lease)
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    @staticmethod
    def _format_pool_key(raw_key: Any) -> str | None:
        if raw_key is None:
            return None
        if isinstance(raw_key, tuple):
            return "|".join(str(part) for part in raw_key)
        rendered = str(raw_key)
        return rendered if rendered else None

    @staticmethod
    def _await_if_needed(value: Any) -> Any:
        if isinstance(value, Awaitable):
            if not in_greenlet():
                close_fn = getattr(value, "close", None)
                if callable(close_fn):
                    close_fn()
                msg = "Async broker operations in SQLAlchemy pools must execute within an async greenlet context"
                raise ImproperConfigurationError(msg)
            return await_only(value)
        return value

    @staticmethod
    def _run_coro_sync(coro: Coroutine[Any, Any, Any]) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:

            async def _runner() -> Any:
                return await coro

            return run_(_runner)()
        coro.close()
        msg = "Async broker operations cannot block an active event loop outside greenlet; dispose inside async greenlet or implement quarantine_sync"
        raise ImproperConfigurationError(msg)

    @staticmethod
    def _on_connection_invalidate(
        dbapi_connection: Any,
        connection_record: Any,
        exception: BaseException | None,
    ) -> None:
        if connection_record is None:
            return
        pool_ref = connection_record.info.pop("vtb_broker_pool", None)
        if pool_ref is not None:
            pool_ref._handle_invalidate(dbapi_connection, connection_record, exception)

    def _transport_target(self) -> Any:
        return getattr(self._broker, "transport", self._broker)

    def _register_partition(self, target: Any) -> None:
        register_fn = getattr(target, "register_partition", None)
        if not callable(register_fn):
            return
        if isinstance(target, (SyncSHMBrokerTransport, AsyncSHMBrokerTransport)):
            reg_kwargs: dict[str, Any] = {}
            pool_size = self._pool_size
            if pool_size is None and self._config is not None:
                pool_size = getattr(self._config, "pool_size", None)
            max_overflow = self._max_overflow
            if max_overflow is None and self._config is not None:
                max_overflow = getattr(self._config, "max_overflow", None)
            if isinstance(pool_size, int) and not isinstance(pool_size, bool):
                reg_kwargs["pool_size"] = pool_size
            if isinstance(max_overflow, int) and not isinstance(max_overflow, bool):
                reg_kwargs["max_overflow"] = max_overflow
            register_fn(self._partition_key, **reg_kwargs)
            return
        self._await_if_needed(register_fn(self._partition_key))

    def _ensure_partition_registered(self, target: Any) -> None:
        if self._partition_registered:
            return
        register_fn = getattr(target, "register_partition", None)
        if callable(register_fn) and is_coroutine_callable(register_fn) and in_greenlet() and self._is_asyncio:
            loop = asyncio.get_running_loop()
            with self._lease_lock:
                if self._partition_registered:
                    return
                async_lock = self._async_register_locks.get(loop)
                if async_lock is None:
                    async_lock = asyncio.Lock()
                    self._async_register_locks[loop] = async_lock
            await_only(async_lock.acquire())
            try:
                if not self._partition_registered:
                    self._register_partition(target)
                    self._partition_registered = True
            finally:
                async_lock.release()
            return
        with self._register_lock:
            if self._partition_registered:
                return
            self._register_partition(target)
            self._partition_registered = True

    def _acquire_partition_lease(self) -> Any:
        target = self._transport_target()
        self._ensure_partition_registered(target)

        worker = self._worker_id if self._worker_id is not None else os.getpid()
        acquire_fn = getattr(target, "acquire", None)
        if not callable(acquire_fn):
            raise ImproperConfigurationError(
                f"Configured broker {type(self._broker).__name__!r} does not implement acquire()"
            )
        kwargs: dict[str, Any] = {"bind_key": self._partition_key, "worker_id": worker}
        if self._acquire_timeout_ms is not None:
            kwargs["timeout_ms"] = self._acquire_timeout_ms
        if isinstance(target, AsyncSHMBrokerTransport):
            if self._is_asyncio or in_greenlet():
                return self._await_if_needed(target.acquire(**kwargs))
            return target.acquire_sync(**kwargs)
        if isinstance(target, SyncSHMBrokerTransport):
            if self._is_asyncio and in_greenlet():
                return await_only(SyncPool.default().submit_async(target.acquire, **kwargs))
            return target.acquire(**kwargs)
        lease_kwargs = {"bind_key": self._partition_key, "worker_id": worker}
        if is_coroutine_callable(acquire_fn):
            return self._await_if_needed(acquire_fn(**lease_kwargs))
        if self._is_asyncio and in_greenlet():
            return await_only(SyncPool.default().submit_async(acquire_fn, **lease_kwargs))
        return self._await_if_needed(acquire_fn(**lease_kwargs))

    def _release_lease(self, lease: Any) -> None:
        if not in_greenlet():
            lease_release_sync = getattr(lease, "release_sync", None)
            if callable(lease_release_sync):
                lease_release_sync()
                return
            target = self._transport_target()
            lease_id = getattr(lease, "lease_id", None)
            transport_release_sync = getattr(self._broker, "release_sync", None) or getattr(
                target, "release_sync", None
            )
            if lease_id is not None and callable(transport_release_sync):
                transport_release_sync(lease_id)
                return

        release_fn = getattr(lease, "release", None)
        if callable(release_fn):
            self._await_if_needed(release_fn())
            return
        target = self._transport_target()
        lease_id = getattr(lease, "lease_id", None)
        transport_release = getattr(target, "release", None)
        if lease_id is not None and callable(transport_release):
            self._await_if_needed(transport_release(lease_id))

    def _quarantine_lease(self, lease: Any, *, allow_sync_fallback: bool = False) -> None:
        target = self._transport_target()
        quarantine_sync_fn = getattr(self._broker, "quarantine_sync", None) or getattr(target, "quarantine_sync", None)
        if callable(quarantine_sync_fn) and (
            not in_greenlet() or not is_coroutine_callable(getattr(target, "quarantine", None))
        ):
            quarantine_sync_fn(lease)
            return

        quarantine_fn = getattr(self._broker, "quarantine", None) or getattr(target, "quarantine", None)
        if not callable(quarantine_fn):
            return

        is_async_quarantine = is_coroutine_callable(quarantine_fn)
        if not in_greenlet():
            if allow_sync_fallback and is_async_quarantine:
                self._run_coro_sync(quarantine_fn(lease))
                return
            if is_async_quarantine:
                msg = "Async broker operations in SQLAlchemy pools must execute within an async greenlet context"
                raise ImproperConfigurationError(msg)

        self._await_if_needed(quarantine_fn(lease))

    @staticmethod
    def _extract_lease_coords(lease: Any) -> tuple[int, int]:
        raw_slot = getattr(lease, "slot_index", None)
        slot_idx = int(getattr(lease, "slot_idx", 0) if raw_slot is None else raw_slot)
        generation = int(getattr(lease, "generation", 0))
        return slot_idx, generation

    def _run_rollback(self, entry: ConnectionPoolEntry) -> None:
        if self._on_rollback is not None:
            self._on_rollback(entry.dbapi_connection)
            return
        conn: Any = entry.dbapi_connection
        if conn is not None:
            if self._reset_mode == "commit" and hasattr(conn, "commit"):
                res = conn.commit()
            elif hasattr(conn, "rollback"):
                res = conn.rollback()
            else:
                return
            if isinstance(res, Awaitable):
                self._await_if_needed(res)

    @staticmethod
    def _close_entry_safely(entry: ConnectionPoolEntry) -> None:
        try:
            entry.close()
        except Exception as exc:
            logger.debug("Ignored entry close failure while quarantining broker lease: %s", exc)

    def _handle_invalidate(
        self,
        dbapi_connection: Any,
        connection_record: Any,
        exception: BaseException | None,
    ) -> None:
        """Quarantine active leases if SQLAlchemy invalidates a checked-out connection record."""
        _ = dbapi_connection
        if connection_record is None:
            return
        with self._lease_lock:
            entry_pair = self._entry_leases.pop(id(connection_record), None)
        if entry_pair is None:
            return
        _, lease = entry_pair
        if exception is not None:
            self._quarantine_lease(lease, allow_sync_fallback=True)
            if self._on_dispose is not None:
                self._on_dispose(lease)
        else:
            self._release_lease(lease)

    def _do_get(self) -> ConnectionPoolEntry:
        """Acquire a SHM broker lease for the config partition and open the connection entry."""
        lease = self._acquire_partition_lease()
        try:
            entry = self._create_connection()
        except BaseException:
            self._quarantine_lease(lease, allow_sync_fallback=True)
            if self._on_dispose is not None:
                self._on_dispose(lease)
            raise
        with self._lease_lock:
            self._entry_leases[id(entry)] = (entry, lease)
        entry.info["vtb_broker_lease"] = lease
        entry.info["vtb_broker_pool"] = self
        return entry

    def _do_return_conn(self, record: ConnectionPoolEntry) -> None:
        """Roll back on return, releasing healthy leases and quarantining dirty sessions."""
        record.info.pop("vtb_broker_pool", None)
        with self._lease_lock:
            entry_pair = self._entry_leases.pop(id(record), None)
        if entry_pair is None:
            record.close()
            return
        _, lease = entry_pair

        try:
            self._run_rollback(record)
        except BaseException as exc:
            self._close_entry_safely(record)
            self._quarantine_lease(lease, allow_sync_fallback=True)
            if self._on_dispose is not None:
                self._on_dispose(lease)
            if isinstance(exc, Exception):
                slot_idx, generation = self._extract_lease_coords(lease)
                raise LeaseQuarantinedError(slot_idx, generation, str(exc)) from exc
            raise

        try:
            record.close()
        except BaseException:
            self._quarantine_lease(lease, allow_sync_fallback=True)
            if self._on_dispose is not None:
                self._on_dispose(lease)
            raise
        self._release_lease(lease)


class AsyncBrokerPool(BrokerPool):
    """Asynchronous SQLAlchemy ``Pool`` coordinating async greenlet checkouts with SHM broker leases."""

    _is_asyncio = True
    _dialect: _ConnDialect = _AsyncConnDialect()
