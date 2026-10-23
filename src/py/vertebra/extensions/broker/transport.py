"""Shared-memory connection broker transport and statement cache."""

from __future__ import annotations

import os
import threading
import time
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._vertebra import (
    NativeLeaseBroker,
    NativeStatementCache,
)
from vertebra.utils.serializers import decode_msgpack, encode_msgpack
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

__all__ = (
    "BrokerConfig",
    "BrokerMetrics",
    "ChannelMessage",
    "ConnectionLease",
    "InMemoryBrokerTransport",
    "ShmBrokerTransport",
    "StatementPlan",
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


class ConnectionLease:
    """Connection lease context manager."""

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

    def release(self) -> None:
        """Release the connection lease."""
        if not self._released:
            self._released = True
            if self._transport is not None:
                self._transport.release(self.lease_id)

    @property
    def is_released(self) -> bool:
        """Whether the lease has been released."""
        return self._released

    def __enter__(self) -> ConnectionLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()

    async def __aenter__(self) -> ConnectionLease:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()


class InMemoryBrokerTransport:
    """In-process thread-safe connection broker transport for tests and single-worker setups."""

    def __init__(self, config: BrokerConfig | None = None) -> None:
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
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._active_leases: dict[str, ConnectionLease] = {}
        self._channels: dict[str, list[ChannelMessage]] = {}
        self._waiters: int = 0
        self._total_acquired: int = 0
        self._total_released: int = 0
        self._total_timeouts: int = 0
        self._stmt_cache: dict[tuple[str, str], StatementPlan] = {}

    def acquire(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str = "default",
        timeout: float | None = None,
    ) -> ConnectionLease:
        """Acquire a connection lease synchronously."""
        worker = worker_id if worker_id is not None else os.getpid()
        eff_timeout_ms = (
            int(timeout * 1000)
            if timeout is not None
            else (timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms)
        )
        effective_timeout = eff_timeout_ms / 1000.0
        deadline = time.monotonic() + effective_timeout
        total_cap = self._config.pool_size + self._config.max_overflow

        with self._lock:
            while len(self._active_leases) >= total_cap:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._total_timeouts += 1
                    raise TimeoutError("Connection broker pool exhausted")
                self._waiters += 1
                try:
                    self._cond.wait(timeout=remaining)
                finally:
                    self._waiters -= 1

            slot = len(self._active_leases)
            gen = self._total_acquired + 1
            lease_id = f"{slot}:{gen}"
            now_ns = time.time_ns()
            ttl_ns = self._config.lease_ttl_ms * 1_000_000
            is_overflow = slot >= self._config.pool_size

            lease = ConnectionLease(
                lease_id=lease_id,
                worker_id=worker,
                acquired_at_ns=now_ns,
                ttl_ns=ttl_ns,
                bind_key=bind_key,
                is_overflow=is_overflow,
                slot_index=slot,
                generation=gen,
                transport=self,
            )
            self._active_leases[lease_id] = lease
            self._total_acquired += 1
            return lease

    async def acquire_async(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str = "default",
    ) -> ConnectionLease:
        """Acquire a connection lease asynchronously."""
        return await SyncPool.default().submit_async(self.acquire, worker_id, timeout_ms, bind_key)

    def release(self, lease_id: str) -> bool:
        """Release an acquired connection lease."""
        with self._lock:
            if lease_id in self._active_leases:
                del self._active_leases[lease_id]
                self._total_released += 1
                self._cond.notify()
                return True
            return False

    def reclaim_worker(self, worker_pid: int) -> int:
        """Reclaim all leases held by a terminated worker process."""
        with self._lock:
            reclaimed = 0
            to_remove = [lid for lid, lease in self._active_leases.items() if lease.worker_id == worker_pid]
            for lid in to_remove:
                del self._active_leases[lid]
                reclaimed += 1
                self._total_released += 1
            if reclaimed > 0:
                self._cond.notify_all()
            return reclaimed

    def active_leases_for_worker(self, worker_pid: int) -> list[tuple[int, int, int]]:
        """Return active `(pool_id, slot_index, generation)` tuples for `worker_pid`."""
        with self._lock:
            return [
                (
                    0,
                    lease.slot_index,
                    lease.generation,
                )
                for lease in self._active_leases.values()
                if lease.worker_id == worker_pid
            ]

    def invalidate_statements(self) -> None:
        """Clear all cached statement plans."""
        with self._lock:
            self._stmt_cache.clear()

    def get_or_compile_statement(
        self,
        dialect: str,
        raw_sql: str,
        compiler: Callable[[str], tuple[str, tuple[str, ...]]],
    ) -> StatementPlan:
        """Retrieve statement plan from cache or compile and store it."""
        key = (dialect, raw_sql)
        with self._lock:
            if key in self._stmt_cache:
                return self._stmt_cache[key]

        compiled_sql, param_names = compiler(raw_sql)
        plan = StatementPlan(
            sql_hash=hash(key),
            dialect=dialect,
            compiled_sql=compiled_sql,
            param_names=param_names,
        )
        with self._lock:
            self._stmt_cache[key] = plan
        return plan

    def metrics(self) -> BrokerMetrics:
        """Return snapshot of broker metrics."""
        with self._lock:
            primary = 0
            overflow = 0
            for lease in self._active_leases.values():
                if lease.is_overflow:
                    overflow += 1
                else:
                    primary += 1
            return BrokerMetrics(
                active_leases=len(self._active_leases),
                primary_in_use=primary,
                overflow_in_use=overflow,
                waiting_threads=self._waiters,
                total_acquired=self._total_acquired,
                total_released=self._total_released,
                total_timeouts=self._total_timeouts,
                statement_cache_hits=0,
                statement_cache_misses=len(self._stmt_cache),
            )

    def publish(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named channel."""
        msg = ChannelMessage(channel=channel, payload=payload, timestamp_ns=time.time_ns(), lease_id=lease_id)
        with self._lock:
            self._channels.setdefault(channel, []).append(msg)
        return msg

    def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages from a named channel."""
        with self._lock:
            return self._channels.pop(channel, [])

    def drain_all_channels(self) -> list[ChannelMessage]:
        """Drain buffered messages across all channels."""
        with self._lock:
            out: list[ChannelMessage] = []
            for messages in self._channels.values():
                out.extend(messages)
            self._channels.clear()
            return out

    def close(self) -> None:
        """Release broker resources."""
        pass


class ShmBrokerTransport:
    """Production shared-memory connection broker and statement cache transport."""

    _DIALECT_TAGS = {
        "postgresql": 1,
        "mysql": 2,
        "oracle": 3,
        "sqlite": 4,
    }

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        native: NativeLeaseBroker | None = None,
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

        if native is not None:
            self._native = native
        else:
            env_broker = NativeLeaseBroker.from_env()
            if env_broker is not None:
                self._native = env_broker
            elif self._config.shm_name:
                try:
                    self._native = NativeLeaseBroker.open_named(
                        self._config.shm_name,
                        self._config.pool_size,
                        self._config.max_overflow,
                        self._config.statement_cache_capacity,
                    )
                except Exception:
                    self._native = NativeLeaseBroker.create_named(
                        self._config.shm_name,
                        self._config.pool_size,
                        self._config.max_overflow,
                        self._config.statement_cache_capacity,
                    )
            else:
                self._native = NativeLeaseBroker(
                    self._config.pool_size,
                    self._config.max_overflow,
                    self._config.statement_cache_capacity,
                )

        colocated_cache = self._native.statement_cache()
        self._stmt_cache = (
            colocated_cache
            if colocated_cache is not None
            else NativeStatementCache(self._config.statement_cache_capacity)
        )
        self._active_leases: dict[str, ConnectionLease] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_fd(
        cls,
        fd: int,
        size: int = 0,
        config: BrokerConfig | None = None,
    ) -> ShmBrokerTransport:
        """Attach a broker transport to an inherited shared-memory file descriptor."""
        cfg = config or BrokerConfig()
        native = NativeLeaseBroker.from_fd(fd, size)
        return cls(config=cfg, native=native)

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

    def acquire(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str = "default",
        timeout: float | None = None,
    ) -> ConnectionLease:
        """Acquire a connection lease synchronously, detaching GIL during wait."""
        worker = worker_id if worker_id is not None else os.getpid()
        effective_timeout = (
            int(timeout * 1000)
            if timeout is not None
            else (timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms)
        )
        ttl_ns = self._config.lease_ttl_ms * 1_000_000
        bind_hash = hash(bind_key) & 0xFFFF_FFFF_FFFF_FFFF

        grant = self._native.acquire(
            worker_pid=worker,
            ttl_ns=ttl_ns,
            timeout_ms=effective_timeout,
            bind_key_hash=bind_hash,
        )
        if grant is None:
            raise TimeoutError("Connection broker pool exhausted")

        lease_id = f"{grant.slot_index}:{grant.generation}"
        lease = ConnectionLease(
            lease_id=lease_id,
            worker_id=worker,
            acquired_at_ns=grant.acquired_ns,
            ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
            bind_key=bind_key,
            is_overflow=grant.is_overflow,
            slot_index=grant.slot_index,
            generation=grant.generation,
            transport=self,
        )
        with self._lock:
            self._active_leases[lease_id] = lease
        return lease

    async def acquire_async(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str = "default",
    ) -> ConnectionLease:
        """Acquire a connection lease asynchronously without event loop blocking."""
        worker = worker_id if worker_id is not None else os.getpid()
        ttl_ns = self._config.lease_ttl_ms * 1_000_000
        bind_hash = hash(bind_key) & 0xFFFF_FFFF_FFFF_FFFF

        grant = self._native.try_acquire(worker_pid=worker, ttl_ns=ttl_ns, bind_key_hash=bind_hash)
        if grant is not None:
            lease_id = f"{grant.slot_index}:{grant.generation}"
            lease = ConnectionLease(
                lease_id=lease_id,
                worker_id=worker,
                acquired_at_ns=grant.acquired_ns,
                ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
                bind_key=bind_key,
                is_overflow=grant.is_overflow,
                slot_index=grant.slot_index,
                generation=grant.generation,
                transport=self,
            )
            with self._lock:
                self._active_leases[lease_id] = lease
            return lease

        effective_timeout = timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms
        return await SyncPool.default().submit_async(self.acquire, worker, effective_timeout, bind_key)

    def release(self, lease_id: str) -> bool:
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

    def reclaim_worker(self, worker_pid: int) -> int:
        """Reclaim all leases held by a dead worker process."""
        with self._lock:
            to_remove = [lid for lid, lease in self._active_leases.items() if lease.worker_id == worker_pid]
            for lid in to_remove:
                del self._active_leases[lid]
        return self._native.reclaim_worker(worker_pid)

    def active_leases_for_worker(self, worker_pid: int) -> list[tuple[int, int, int]]:
        """Return active `(pool_id, slot_index, generation)` tuples for `worker_pid`."""
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
        param_bytes = encode_msgpack(list(param_names)) if param_names else b""

        self._stmt_cache.insert(
            dialect_tag=dialect_tag,
            raw_sql=raw_bytes,
            param_count=len(param_names),
            compiled_sql=compiled_bytes,
            param_plan=param_bytes,
        )

        return StatementPlan(
            sql_hash=hash((dialect_tag, raw_sql)),
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

    def publish(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named shared-memory channel."""
        ts_ns = time.time_ns()
        self._native.publish_channel(channel, payload, ts_ns, lease_id)
        return ChannelMessage(
            channel=channel,
            payload=payload,
            timestamp_ns=ts_ns,
            lease_id=lease_id,
        )

    def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages for `channel` from the shared-memory channel ring."""
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

    def drain_all_channels(self) -> list[ChannelMessage]:
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

    def close(self) -> None:
        """Release broker resources."""
        pass
