"""SQLAlchemy connection pools backed by Vertebra SHM lease brokers."""

from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, ClassVar, cast

from sqlalchemy import event as sa_event
from sqlalchemy.pool import ConnectionPoolEntry, Pool
from sqlalchemy.pool.base import _AsyncConnDialect
from sqlalchemy.util import await_only
from sqlalchemy.util.concurrency import in_greenlet

from vertebra.db.dbapi import SyncDBAPIConnection
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from sqlalchemy.pool.base import _ConnDialect

__all__ = (
    "AsyncBrokerPool",
    "BrokerPool",
    "broker_creator",
)


def _format_pool_key(raw_key: Any) -> str:
    """Format a config ``pool_key()`` tuple or string into a canonical broker partition identifier."""
    if isinstance(raw_key, tuple):
        return "|".join(str(part) for part in raw_key)
    return str(raw_key)


def _await_if_needed(value: Any) -> Any:
    """Resolve an awaitable inside SQLAlchemy's active greenlet, or return synchronous values directly."""
    if isinstance(value, Awaitable):
        if not in_greenlet():
            msg = "Async broker operations in SQLAlchemy pools must execute within an async greenlet context"
            raise ImproperConfigurationError(msg)
        return await_only(value)
    return value


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

    if is_async:
        module = SQLAlchemyAsyncDBAPI()
        return lambda: module.connect(url, broker=None, share_pool=share_pool, **connect_kwargs)

    return lambda: SyncDBAPIConnection(url, broker=None, share_pool=share_pool, **connect_kwargs)


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
                partition_key = _format_pool_key(pool_key_fn())
        if not partition_key and resolved_url:
            partition_key = resolved_url
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
        self._lease_lock = threading.Lock()
        self._partition_registered = False
        self._entry_leases: dict[int, Any] = {}

        super().__init__(resolved_creator, reset_on_return=reset_on_return, **pool_kw)
        sa_event.listen(self, "invalidate", self._handle_invalidate)

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
        cls._bound_counter += 1
        sub_name = f"{cls.__name__}Bound{cls._bound_counter}"
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

    def _transport_target(self) -> Any:
        return getattr(self._broker, "transport", self._broker)

    def _acquire_partition_lease(self) -> Any:
        target = self._transport_target()
        with self._lease_lock:
            registered = self._partition_registered
        if not registered:
            register_fn = getattr(target, "register_partition", None)
            if callable(register_fn):
                _await_if_needed(register_fn(self._partition_key))
            with self._lease_lock:
                self._partition_registered = True

        worker = self._worker_id if self._worker_id is not None else os.getpid()
        acquire_fn = getattr(target, "acquire", None)
        if not callable(acquire_fn):
            raise ImproperConfigurationError(
                f"Configured broker {type(self._broker).__name__!r} does not implement acquire()"
            )
        kwargs: dict[str, Any] = {"bind_key": self._partition_key, "worker_id": worker}
        if self._acquire_timeout_ms is not None:
            kwargs["timeout_ms"] = self._acquire_timeout_ms
        return _await_if_needed(acquire_fn(**kwargs))

    def _release_lease(self, lease: Any) -> None:
        release_fn = getattr(lease, "release", None)
        if callable(release_fn):
            _await_if_needed(release_fn())
            return
        target = self._transport_target()
        lease_id = getattr(lease, "lease_id", None)
        transport_release = getattr(target, "release", None)
        if lease_id is not None and callable(transport_release):
            _await_if_needed(transport_release(lease_id))

    def _quarantine_lease(self, lease: Any) -> None:
        target = self._transport_target()
        quarantine_fn = getattr(self._broker, "quarantine", None) or getattr(target, "quarantine", None)
        if callable(quarantine_fn):
            try:
                _await_if_needed(quarantine_fn(lease))
            except TypeError:
                coords = (int(lease.slot_index), int(lease.generation))
                _await_if_needed(quarantine_fn(coords))

    def _extract_lease_coords(self, lease: Any) -> tuple[int, int]:
        slot_idx = int(getattr(lease, "slot_index", 0))
        generation = int(getattr(lease, "generation", 0))
        return slot_idx, generation

    def _run_rollback(self, entry: ConnectionPoolEntry) -> None:
        if self._on_rollback is not None:
            self._on_rollback(entry.dbapi_connection)
            return
        conn: Any = entry.dbapi_connection
        if conn is not None and hasattr(conn, "rollback"):
            rb_result = conn.rollback()
            if isinstance(rb_result, Awaitable):
                _await_if_needed(rb_result)

    @staticmethod
    def _close_entry_safely(entry: ConnectionPoolEntry) -> None:
        with contextlib.suppress(Exception):
            entry.close()

    def _handle_invalidate(
        self,
        dbapi_connection: Any,
        connection_record: Any,
        exception: BaseException | None,
    ) -> None:
        """Quarantine active leases if SQLAlchemy invalidates a checked-out connection record."""
        _ = (dbapi_connection, exception)
        if connection_record is None:
            return
        with self._lease_lock:
            lease = self._entry_leases.pop(id(connection_record), None)
        if lease is None:
            return
        if exception is not None:
            self._quarantine_lease(lease)
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
            self._quarantine_lease(lease)
            raise
        with self._lease_lock:
            self._entry_leases[id(entry)] = lease
        entry.info["vtb_broker_lease"] = lease
        return entry

    def _do_return_conn(self, record: ConnectionPoolEntry) -> None:
        """Roll back on return, releasing healthy leases and quarantining dirty sessions."""
        with self._lease_lock:
            lease = self._entry_leases.pop(id(record), None)
        if lease is None:
            record.close()
            return

        rollback_exc: Exception | None = None
        try:
            self._run_rollback(record)
        except Exception as exc:
            rollback_exc = exc

        if rollback_exc is not None:
            self._close_entry_safely(record)
            self._quarantine_lease(lease)
            if self._on_dispose is not None:
                self._on_dispose(lease)
            slot_idx, generation = self._extract_lease_coords(lease)
            raise LeaseQuarantinedError(slot_idx, generation, str(rollback_exc)) from rollback_exc

        record.close()
        self._release_lease(lease)

    def recreate(self) -> BrokerPool:
        """Return a freshly initialized copy of this pool preserving broker configuration."""
        return self.__class__(
            self._creator,
            broker=self._broker,
            config=self._config,
            url=self._url,
            pool_key=self._pool_key_override,
            worker_id=self._worker_id,
            acquire_timeout_ms=self._acquire_timeout_ms,
            on_rollback=self._on_rollback,
            on_dispose=self._on_dispose,
            recycle=self._recycle,
            echo=self.echo,
            logging_name=self._orig_logging_name,
            reset_on_return=self._reset_on_return,
            pre_ping=self._pre_ping,
            _dispatch=self.dispatch,
            dialect=self._dialect,
        )

    def dispose(self) -> None:
        """Release or quarantine any tracked entry leases during pool disposal."""
        with self._lease_lock:
            remaining = list(self._entry_leases.values())
            self._entry_leases.clear()
        for lease in remaining:
            self._release_lease(lease)


class AsyncBrokerPool(BrokerPool):
    """Asynchronous SQLAlchemy ``Pool`` coordinating async greenlet checkouts with SHM broker leases."""

    _is_asyncio = True
    _dialect: _ConnDialect = _AsyncConnDialect()
