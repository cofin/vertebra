"""Shared-memory connection broker transport and statement cache."""

from __future__ import annotations

import inspect
import os
import threading
import time
from typing import TYPE_CHECKING, Any, Literal, Self

import msgspec

from vertebra._vertebra import (
    LeaseBroker,
    StatementCache,
    stable_hash64,
)
from vertebra.extensions.broker.errors import BrokerAttachError, BrokerSegmentInUseError
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

__all__ = (
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "BrokerConfig",
    "BrokerMetrics",
    "ChannelMessage",
    "StatementPlan",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
)


class ChannelMessage(msgspec.Struct, frozen=True, gc=False, array_like=True):
    """Zero-GC message envelope exchanged over broker pub/sub channels."""

    channel: str
    payload: bytes
    timestamp_ns: int = 0
    lease_id: str | None = None


class BrokerConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for shared-memory connection leasing and statement caching."""

    pool_size: int = 20
    max_overflow: int = 10
    lease_ttl_ms: int = 30_000
    acquire_timeout_ms: int = 5_000
    statement_cache_capacity: int = 512
    shm_name: str | None = None
    role: Literal["owner", "worker"] = "owner"
    state_key: str = "db_broker"
    metrics_state_key: str = "db_broker_metrics"
    acquire_timeout: float = 5.0
    lease_ttl: float = 30.0


class BrokerMetrics(msgspec.Struct, frozen=True, gc=False):
    """Cumulative and point-in-time metrics for broker and statement cache."""

    active_leases: int = 0
    primary_in_use: int = 0
    overflow_in_use: int = 0
    waiting_threads: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_expired: int = 0
    total_timeouts: int = 0
    total_reclaimed: int = 0
    statement_cache_hits: int = 0
    statement_cache_misses: int = 0
    statement_cache_evictions: int = 0


class StatementPlan(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Compiled statement and ordered bind-parameter plan."""

    sql_hash: int
    dialect: str
    compiled_sql: str
    param_names: tuple[str, ...]


class _BaseConnectionLease:
    """Shared base representation for connection leases."""

    __slots__ = (
        "_released",
        "_transport",
        "acquired_at_ns",
        "bind_key",
        "generation",
        "is_overflow",
        "lease_id",
        "slot_index",
        "ttl_ns",
        "worker_id",
    )

    def __init__(
        self,
        lease_id: str,
        worker_id: int,
        acquired_at_ns: int,
        ttl_ns: int,
        bind_key: str,
        is_overflow: bool,
        slot_index: int,
        generation: int,
        transport: Any = None,
    ) -> None:
        self.lease_id = lease_id
        self.worker_id = worker_id
        self.acquired_at_ns = acquired_at_ns
        self.ttl_ns = ttl_ns
        self.bind_key = bind_key
        self.is_overflow = is_overflow
        self.slot_index = slot_index
        self.generation = generation
        self._transport = transport
        self._released: bool = False

    @property
    def is_released(self) -> bool:
        """Whether the lease has been released."""
        return self._released


class SyncConnectionLease(_BaseConnectionLease):
    """Synchronous connection lease context manager."""

    def release(self) -> None:
        """Release the connection lease."""
        if not self._released:
            self._released = True
            if self._transport is not None:
                release_sync_fn = getattr(self._transport, "_release_sync", None)
                if callable(release_sync_fn):
                    release_sync_fn(self.lease_id)
                else:
                    self._transport.release(self.lease_id)

    def __enter__(self) -> SyncConnectionLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()


class AsyncConnectionLease(_BaseConnectionLease):
    """Asynchronous connection lease context manager."""

    async def release(self) -> None:
        """Release the connection lease asynchronously."""
        if not self._released:
            self._released = True
            if self._transport is not None:
                res = self._transport.release(self.lease_id)
                if inspect.isawaitable(res):
                    await res

    async def __aenter__(self) -> AsyncConnectionLease:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.release()


class _BaseSHMBrokerTransport:
    """Shared base for shared-memory connection broker transports."""

    _DIALECT_TAGS = {
        "postgresql": 1,
        "postgres": 1,
        "mysql": 2,
        "mariadb": 2,
        "sqlite": 3,
        "oracle": 4,
        "mssql": 5,
        "sqlserver": 5,
        "duckdb": 6,
        "bigquery": 7,
        "spanner": 8,
        "spangres": 9,
    }

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        native: LeaseBroker | None = None,
    ) -> None:
        raw_cfg = config or BrokerConfig()
        acq_ms = (
            int(raw_cfg.acquire_timeout * 1000)
            if raw_cfg.acquire_timeout != 5.0 and raw_cfg.acquire_timeout_ms == 5_000
            else raw_cfg.acquire_timeout_ms
        )
        ttl_ms = (
            int(raw_cfg.lease_ttl * 1000)
            if raw_cfg.lease_ttl != 30.0 and raw_cfg.lease_ttl_ms == 30_000
            else raw_cfg.lease_ttl_ms
        )
        self._config = msgspec.structs.replace(raw_cfg, acquire_timeout_ms=acq_ms, lease_ttl_ms=ttl_ms)

        if raw_cfg.role not in ("owner", "worker"):
            raise ValueError(f"unsupported broker role: {raw_cfg.role!r}")

        self._created: bool = False
        if native is not None:
            self._native = native
        else:
            env_broker = LeaseBroker.from_env()
            if env_broker is not None:
                self._native = env_broker
            elif self._config.shm_name:
                self._native, self._created = self._attach_named_segment(self._config.shm_name)
            elif self._config.role == "worker":
                raise BrokerAttachError("worker broker role requires shm_name or inherited SHM descriptor")
            else:
                self._native = LeaseBroker(
                    self._config.pool_size,
                    self._config.max_overflow,
                    self._config.statement_cache_capacity,
                )
                self._created = True

        colocated_cache = self._native.statement_cache()
        self._stmt_cache = (
            colocated_cache if colocated_cache is not None else StatementCache(self._config.statement_cache_capacity)
        )
        self._active_leases: dict[str, _BaseConnectionLease] = {}
        self._partitions: dict[str, int] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _extract_owner_pid(name: str, exc: BaseException) -> int | None:
        pid = LeaseBroker.probe_owner_pid(name)
        if pid is not None:
            return pid
        text = str(exc)
        marker = "owner_pid="
        idx = text.find(marker)
        if idx >= 0:
            digits = []
            for ch in text[idx + len(marker) :]:
                if ch.isdigit():
                    digits.append(ch)
                else:
                    break
            if digits:
                return int("".join(digits))
        return None

    def _attach_named_segment(self, name: str) -> tuple[LeaseBroker, bool]:
        if self._config.role == "owner":
            try:
                return LeaseBroker.open_or_create_named(
                    name,
                    self._config.pool_size,
                    self._config.max_overflow,
                    self._config.statement_cache_capacity,
                )
            except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
                owner_pid = self._extract_owner_pid(name, exc)
                if isinstance(exc, FileExistsError) or owner_pid is not None:
                    raise BrokerSegmentInUseError(name, owner_pid=owner_pid, message=str(exc)) from exc
                raise BrokerAttachError(f"failed to attach owner broker segment {name!r}: {exc}") from exc

        try:
            native = LeaseBroker.open_named(
                name,
                self._config.pool_size,
                self._config.max_overflow,
                self._config.statement_cache_capacity,
            )
            return native, False
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            owner_pid = self._extract_owner_pid(name, exc)
            if isinstance(exc, FileExistsError) or (not isinstance(exc, FileNotFoundError) and owner_pid is not None):
                raise BrokerSegmentInUseError(name, owner_pid=owner_pid, message=str(exc)) from exc
            raise BrokerAttachError(f"worker failed to attach to broker segment {name!r}: {exc}") from exc

    @property
    def owner_pid(self) -> int | None:
        """Return PID owning the attached named segment, when available."""
        return self._native.owner_pid

    @property
    def created(self) -> bool:
        """Return whether this transport created the underlying segment."""
        return self._created

    def unlink(self) -> bool:
        """Unlink the owned POSIX shared-memory segment name while keeping active mappings valid."""
        return self._native.unlink()

    def register_partition(
        self,
        pool_key: str,
        pool_size: int | None = None,
        max_overflow: int | None = None,
    ) -> int:
        """Idempotently register a named partition keyed by deterministic ``stable_hash64(pool_key)``."""
        key_hash = stable_hash64(pool_key)
        eff_pool = self._config.pool_size if pool_size is None else pool_size
        eff_overflow = self._config.max_overflow if max_overflow is None else max_overflow
        try:
            part_id = self._native.register_partition(key_hash, eff_pool, eff_overflow)
        except (RuntimeError, ValueError) as exc:
            raise BrokerAttachError(f"failed to register partition {pool_key!r}: {exc}") from exc
        with self._lock:
            self._partitions[pool_key] = part_id
        return part_id

    @staticmethod
    def _bind_key_hash(bind_key: str | None) -> int:
        if bind_key is None:
            return 0
        return stable_hash64(bind_key)

    @property
    def shm_fd(self) -> int | None:
        """Return underlying shared-memory file descriptor when available."""
        return self._native.shm_fd

    @property
    def shm_size(self) -> int:
        """Return total size in bytes of the shared-memory broker region."""
        return self._native.shm_size

    @property
    def stmt_offset(self) -> int:
        """Return byte offset of the co-located statement cache within the SHM region."""
        return self._native.stmt_offset

    @property
    def stmt_capacity(self) -> int:
        """Return slot capacity of the co-located statement cache."""
        return self._native.stmt_capacity

    @classmethod
    def from_fd(
        cls,
        fd: int,
        size: int = 0,
        config: BrokerConfig | None = None,
    ) -> Self:
        """Attach a broker transport to an inherited shared-memory file descriptor."""
        cfg = config or BrokerConfig()
        native = LeaseBroker.from_fd(fd, size)
        return cls(config=cfg, native=native)

    def reclaim_worker(self, worker_pid: int) -> int:
        """Reclaim all leases held by a dead worker process."""
        with self._lock:
            to_remove = [lid for lid, lease in self._active_leases.items() if lease.worker_id == worker_pid]
            for lid in to_remove:
                del self._active_leases[lid]
        return self._native.reclaim_worker(worker_pid)

    def active_leases_for_worker(self, worker_pid: int) -> list[tuple[int, int, int]]:
        """Return active (pool_id, slot_index, generation) tuples for worker_pid."""
        return list(self._native.active_leases_for_worker(worker_pid))

    def invalidate_statements(self) -> None:
        """Invalidate all cached statement plans in shared memory."""
        self._stmt_cache.invalidate_all()

    def get_or_compile_statement(
        self,
        dialect: str,
        raw_sql: str,
        compiler: Callable[[str], tuple[str, tuple[str, ...]]],
    ) -> StatementPlan:
        """Query statement plan from shared memory or compile and publish it."""
        dialect_tag = self._DIALECT_TAGS.get(dialect, 0)
        raw_bytes = raw_sql.encode("utf-8")

        cached = self._stmt_cache.lookup(dialect_tag, raw_bytes)
        if cached is not None:
            param_names = tuple(decode_msgpack(cached.param_plan)) if cached.param_plan else ()
            return StatementPlan(
                sql_hash=cached.sql_hash,
                dialect=dialect,
                compiled_sql=cached.compiled_sql.decode("utf-8"),
                param_names=param_names,
            )

        compiled_sql, param_names = compiler(raw_sql)
        compiled_bytes = compiled_sql.encode("utf-8")
        param_bytes = encode_msgpack_bytes(list(param_names)) if param_names else b""

        self._stmt_cache.insert(
            dialect_tag=dialect_tag,
            raw_sql=raw_bytes,
            param_count=len(param_names),
            compiled_sql=compiled_bytes,
            param_plan=param_bytes,
        )

        return StatementPlan(
            sql_hash=stable_hash64(bytes((dialect_tag,)) + raw_bytes),
            dialect=dialect,
            compiled_sql=compiled_sql,
            param_names=param_names,
        )

    def metrics(self) -> BrokerMetrics:
        """Return live metrics snapshot across processes."""
        stats = self._native.stats()
        stmt_stats = self._stmt_cache.stats()
        return BrokerMetrics(
            active_leases=stats.active_leases,
            primary_in_use=stats.primary_in_use,
            overflow_in_use=stats.overflow_in_use,
            waiting_threads=stats.waiting_threads,
            total_acquired=stats.total_acquired,
            total_released=stats.total_released,
            total_expired=stats.total_expired,
            total_timeouts=stats.total_timeouts,
            total_reclaimed=stats.total_reclaimed,
            statement_cache_hits=stmt_stats.hits,
            statement_cache_misses=stmt_stats.misses,
            statement_cache_evictions=stmt_stats.evictions,
        )

    def _acquire_sync(
        self,
        pool_name: str | None = None,
        worker_pid: int | None = None,
        *,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str | None = None,
        timeout: float | None = None,
        partition: int | None = None,
    ) -> SyncConnectionLease:
        """Acquire a connection lease synchronously, detaching GIL during wait."""
        resolved_worker = worker_pid if worker_pid is not None else worker_id
        worker = resolved_worker if resolved_worker is not None else os.getpid()
        resolved_bind_key = bind_key if bind_key is not None else pool_name
        effective_timeout = (
            int(timeout * 1000)
            if timeout is not None
            else (timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms)
        )
        ttl_ns = self._config.lease_ttl_ms * 1_000_000
        bind_hash = self._bind_key_hash(resolved_bind_key)

        resolved_partition = partition
        if resolved_partition is None and resolved_bind_key is not None:
            with self._lock:
                resolved_partition = self._partitions.get(resolved_bind_key)

        if resolved_partition is not None:
            grant = self._native.acquire_in(
                partition=resolved_partition,
                worker_pid=worker,
                ttl_ns=ttl_ns,
                timeout_ms=effective_timeout,
                bind_key_hash=bind_hash,
            )
        else:
            grant = self._native.acquire(
                worker_pid=worker,
                ttl_ns=ttl_ns,
                timeout_ms=effective_timeout,
                bind_key_hash=bind_hash,
            )
        if grant is None:
            raise TimeoutError("Connection broker pool exhausted")

        lease_id = f"{grant.slot_index}:{grant.generation}"
        lease = SyncConnectionLease(
            lease_id=lease_id,
            worker_id=worker,
            acquired_at_ns=grant.acquired_ns,
            ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
            bind_key=resolved_bind_key if resolved_bind_key is not None else "",
            is_overflow=grant.is_overflow,
            slot_index=grant.slot_index,
            generation=grant.generation,
            transport=self,
        )
        with self._lock:
            self._active_leases[lease_id] = lease
        return lease

    def _release_sync(self, lease_id: str) -> bool:
        """Release an acquired connection lease synchronously."""
        return self._release_impl(lease_id)

    def _release_impl(self, lease_id: str) -> bool:
        """Release an acquired connection lease."""
        with self._lock:
            lease = self._active_leases.pop(lease_id, None)

        if lease is not None:
            return self._native.release(lease.slot_index, lease.generation)

        try:
            parts = lease_id.split(":")
            if len(parts) == 2:
                slot_idx = int(parts[0])
                gen = int(parts[1])
                return self._native.release(slot_idx, gen)
        except (ValueError, TypeError):
            pass
        return False

    def _publish_impl(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named shared-memory channel."""
        ts_ns = time.time_ns()
        self._native.publish_channel(channel, payload, ts_ns, lease_id)
        return ChannelMessage(
            channel=channel,
            payload=payload,
            timestamp_ns=ts_ns,
            lease_id=lease_id,
        )

    def _drain_channel_impl(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages for channel from the shared-memory channel ring."""
        frames = self._native.drain_channel(channel)
        return [
            ChannelMessage(
                channel=ch,
                payload=data,
                timestamp_ns=ts_ns,
                lease_id=lid,
            )
            for ch, data, ts_ns, lid in frames
        ]

    def _drain_all_channels_impl(self) -> list[ChannelMessage]:
        """Drain buffered messages across all channels from the shared-memory channel ring."""
        frames = self._native.drain_all_channels()
        return [
            ChannelMessage(
                channel=ch,
                payload=data,
                timestamp_ns=ts_ns,
                lease_id=lid,
            )
            for ch, data, ts_ns, lid in frames
        ]

    def _close_impl(self) -> None:
        """Release broker resources."""
        pass


class SyncSHMBrokerTransport(_BaseSHMBrokerTransport):
    """Production synchronous shared-memory connection broker and statement cache transport."""

    def acquire(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str | None = "default",
        timeout: float | None = None,
        partition: int | None = None,
    ) -> SyncConnectionLease:
        """Acquire a connection lease synchronously, detaching GIL during wait."""
        return self._acquire_sync(
            worker_id=worker_id,
            timeout_ms=timeout_ms,
            bind_key=bind_key,
            timeout=timeout,
            partition=partition,
        )

    def release(self, lease_id: str) -> bool:
        """Release an acquired connection lease synchronously."""
        return self._release_sync(lease_id)

    def publish(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named shared-memory channel."""
        return self._publish_impl(channel, payload, lease_id)

    def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages for channel from the shared-memory channel ring."""
        return self._drain_channel_impl(channel)

    def drain_all_channels(self) -> list[ChannelMessage]:
        """Drain buffered messages across all channels from the shared-memory channel ring."""
        return self._drain_all_channels_impl()

    def close(self) -> None:
        """Release broker resources."""
        self._close_impl()


class AsyncSHMBrokerTransport(_BaseSHMBrokerTransport):
    """Production asynchronous shared-memory connection broker and statement cache transport."""

    async def acquire(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str | None = "default",
        timeout: float | None = None,
        partition: int | None = None,
    ) -> AsyncConnectionLease:
        """Acquire a connection lease asynchronously without event loop blocking."""
        worker = worker_id if worker_id is not None else os.getpid()
        ttl_ns = self._config.lease_ttl_ms * 1_000_000
        bind_hash = self._bind_key_hash(bind_key)
        resolved_bind_key = bind_key if bind_key is not None else ""

        resolved_partition = partition
        if resolved_partition is None and bind_key is not None:
            with self._lock:
                resolved_partition = self._partitions.get(bind_key)

        if resolved_partition is not None:
            grant = self._native.try_acquire_in(
                partition=resolved_partition,
                worker_pid=worker,
                ttl_ns=ttl_ns,
                bind_key_hash=bind_hash,
            )
        else:
            grant = self._native.try_acquire(worker_pid=worker, ttl_ns=ttl_ns, bind_key_hash=bind_hash)
        if grant is not None:
            lease_id = f"{grant.slot_index}:{grant.generation}"
            lease = AsyncConnectionLease(
                lease_id=lease_id,
                worker_id=worker,
                acquired_at_ns=grant.acquired_ns,
                ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
                bind_key=resolved_bind_key,
                is_overflow=grant.is_overflow,
                slot_index=grant.slot_index,
                generation=grant.generation,
                transport=self,
            )
            with self._lock:
                self._active_leases[lease_id] = lease
            return lease

        effective_timeout = (
            int(timeout * 1000)
            if timeout is not None
            else (timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms)
        )
        if resolved_partition is not None:
            blocking_grant = await SyncPool.default().submit_async(
                self._native.acquire_in,
                resolved_partition,
                worker,
                ttl_ns,
                effective_timeout,
                bind_hash,
            )
        else:
            blocking_grant = await SyncPool.default().submit_async(
                self._native.acquire,
                worker,
                ttl_ns,
                effective_timeout,
                bind_hash,
            )
        if blocking_grant is None:
            raise TimeoutError("Connection broker pool exhausted")

        lease_id = f"{blocking_grant.slot_index}:{blocking_grant.generation}"
        lease = AsyncConnectionLease(
            lease_id=lease_id,
            worker_id=worker,
            acquired_at_ns=blocking_grant.acquired_ns,
            ttl_ns=blocking_grant.expiry_ns - blocking_grant.acquired_ns if blocking_grant.expiry_ns > 0 else 0,
            bind_key=resolved_bind_key,
            is_overflow=blocking_grant.is_overflow,
            slot_index=blocking_grant.slot_index,
            generation=blocking_grant.generation,
            transport=self,
        )
        with self._lock:
            self._active_leases[lease_id] = lease
        return lease

    async def release(self, lease_id: str) -> bool:
        """Release an acquired connection lease asynchronously."""
        return self._release_impl(lease_id)

    async def publish(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named shared-memory channel asynchronously."""
        return self._publish_impl(channel, payload, lease_id)

    async def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages for channel asynchronously."""
        return self._drain_channel_impl(channel)

    async def drain_all_channels(self) -> list[ChannelMessage]:
        """Drain buffered messages across all channels asynchronously."""
        return self._drain_all_channels_impl()

    async def close(self) -> None:
        """Release broker resources asynchronously."""
        self._close_impl()
