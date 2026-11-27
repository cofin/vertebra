"""Shared-memory connection broker transport and statement cache."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import os
import threading
import time
from collections import deque
from typing import TYPE_CHECKING, Any, Literal, Protocol, Self, cast, runtime_checkable
from weakref import WeakKeyDictionary

import msgspec

from vertebra._vertebra import (
    BrokerAttachClient,
    BrokerEngineRegistry,
    DataError,
    InterfaceError,
    LeaseBroker,
    ReclaimedLease,
    StatementCache,
    stable_hash64,
)
from vertebra.extensions.broker.dbrpc import (
    DBRPC_VERSION,
    DbRpcOp,
    DbRpcRequest,
    DbRpcResponse,
    decode_dbrpc_response,
    encode_dbrpc_request,
)
from vertebra.extensions.broker.errors import (
    BrokerAttachError,
    BrokerAuthenticationError,
    BrokerSegmentInUseError,
    LeaseQuarantinedError,
)
from vertebra.utils.serializers import decode_msgpack, encode_msgpack_bytes
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable, Iterator
    from types import TracebackType

    from vertebra._vertebra import PartitionStats

__all__ = (
    "AsyncBrokerLeaseSource",
    "AsyncBrokerStream",
    "AsyncConnectionLease",
    "AsyncSHMBrokerTransport",
    "BrokerConfig",
    "BrokerMetrics",
    "SyncBrokerStream",
    "ChannelMessage",
    "PoolMetrics",
    "StatementPlan",
    "SyncBrokerLeaseSource",
    "SyncConnectionLease",
    "SyncSHMBrokerTransport",
)

_DEFAULT_BIND_KEY_HASH: int = stable_hash64("default")


class _PartitionWaitQueue:
    """Per-event-loop FIFO waiter deque and single-watcher state for a broker partition."""

    __slots__ = ("local_wakes", "waiters", "watcher_active", "watcher_task")

    def __init__(self) -> None:
        self.waiters: deque[asyncio.Future[bool]] = deque()
        self.local_wakes = 0
        self.watcher_active = False
        self.watcher_task: asyncio.Task[None] | None = None


@runtime_checkable
class SyncBrokerLeaseSource(Protocol):
    """Protocol for synchronous shared-memory broker lease acquisition and quarantine."""

    def acquire(
        self,
        *,
        bind_key: str | None = "default",
        worker_id: int | None = None,
    ) -> Any:
        """Acquire a leased connection slot for ``bind_key`` and ``worker_id``."""

    def quarantine(self, lease: Any) -> None:
        """Quarantine ``lease`` so its slot stays reserved until explicit disposal."""


@runtime_checkable
class AsyncBrokerLeaseSource(Protocol):
    """Protocol for asynchronous shared-memory broker lease acquisition and quarantine."""

    async def acquire(
        self,
        *,
        bind_key: str | None = "default",
        worker_id: int | None = None,
    ) -> Any:
        """Acquire a leased connection slot asynchronously for ``bind_key`` and ``worker_id``."""

    async def quarantine(self, lease: Any) -> Any:
        """Quarantine ``lease`` so its slot stays reserved until explicit disposal."""


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
    socket_path: str | None = None
    attach_secret: bytes | str | None = None

    def __repr__(self) -> str:
        """Return diagnostic string with attach credentials redacted."""
        secret_repr = "'[REDACTED]'" if self.attach_secret is not None else "None"
        return (
            f"BrokerConfig(pool_size={self.pool_size}, max_overflow={self.max_overflow}, "
            f"lease_ttl_ms={self.lease_ttl_ms}, acquire_timeout_ms={self.acquire_timeout_ms}, "
            f"statement_cache_capacity={self.statement_cache_capacity}, "
            f"shm_name={self.shm_name!r}, role={self.role!r}, "
            f"socket_path={self.socket_path!r}, attach_secret={secret_repr})"
        )


class PoolMetrics(msgspec.Struct, frozen=True, gc=False):
    """Point-in-time and cumulative lease metrics for a named broker partition."""

    pool_name: str
    pool_id: int
    active_leases: int = 0
    quarantined: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_reclaimed: int = 0
    total_quarantined: int = 0
    total_disposed: int = 0


class BrokerMetrics(msgspec.Struct, frozen=True):
    """Cumulative and point-in-time metrics for broker and statement cache."""

    active_leases: int = 0
    primary_in_use: int = 0
    overflow_in_use: int = 0
    waiting_threads: int = 0
    quarantined: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_expired: int = 0
    total_timeouts: int = 0
    total_reclaimed: int = 0
    total_quarantined: int = 0
    total_disposed: int = 0
    statement_cache_hits: int = 0
    statement_cache_misses: int = 0
    statement_cache_evictions: int = 0
    pools: dict[str, PoolMetrics] = msgspec.field(default_factory=dict)


class StatementPlan(msgspec.Struct, frozen=True, gc=False, cache_hash=True):
    """Compiled statement and ordered bind-parameter plan."""

    sql_hash: int
    dialect: str
    compiled_sql: str
    param_names: tuple[str, ...]


class _BaseConnectionLease:
    """Shared base representation for connection leases."""

    __slots__ = (
        "_in_transaction",
        "_pinned_session_id",
        "_quarantined",
        "_released",
        "_transport",
        "acquired_at_ns",
        "bind_key",
        "generation",
        "is_overflow",
        "lease_id",
        "pool_id",
        "pool_name",
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
        pool_id: int = 0,
        pool_name: str = "",
    ) -> None:
        self.lease_id = lease_id
        self.worker_id = worker_id
        self.acquired_at_ns = acquired_at_ns
        self.ttl_ns = ttl_ns
        self.bind_key = bind_key
        self.is_overflow = is_overflow
        self.slot_index = slot_index
        self.generation = generation
        self.pool_id = pool_id
        self.pool_name = pool_name
        self._transport = transport
        self._released: bool = False
        self._quarantined: bool = False
        self._pinned_session_id: str | None = None
        self._in_transaction: bool = False

    @property
    def is_released(self) -> bool:
        """Whether the lease has been released."""
        return self._released

    @property
    def quarantined(self) -> bool:
        """Whether the lease slot has been moved to quarantine."""
        return self._quarantined

    @property
    def pinned_session_id(self) -> str | None:
        """Logical session identifier pinned to this lease, if any."""
        return self._pinned_session_id

    @property
    def in_transaction(self) -> bool:
        """Whether an explicit transaction is currently bound to this lease."""
        return self._in_transaction


class SyncConnectionLease(_BaseConnectionLease):
    """Synchronous connection lease context manager."""

    def release(self) -> None:
        """Release the connection lease."""
        if self._quarantined:
            raise LeaseQuarantinedError(
                self.slot_index,
                self.generation,
                "quarantined lease cannot be released without disposal",
            )
        if not self._released:
            if self._transport is not None:
                release_sync_fn = getattr(self._transport, "_release_sync", None)
                if not callable(release_sync_fn):
                    release_sync_fn = getattr(self._transport, "release_sync", None)
                if callable(release_sync_fn):
                    release_sync_fn(self.lease_id)
                else:
                    self._transport.release(self.lease_id)
            self._released = True

    def __enter__(self) -> SyncConnectionLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not self._quarantined:
            self.release()


class AsyncConnectionLease(_BaseConnectionLease):
    """Asynchronous connection lease context manager."""

    async def release(self) -> None:
        """Release the connection lease asynchronously."""
        if self._quarantined:
            raise LeaseQuarantinedError(
                self.slot_index,
                self.generation,
                "quarantined lease cannot be released without disposal",
            )
        if not self._released:
            if self._transport is not None:
                res = self._transport.release(self.lease_id)
                if inspect.isawaitable(res):
                    await res
            self._released = True

    def release_sync(self) -> None:
        """Release the connection lease synchronously from non-greenlet teardown paths."""
        if self._quarantined:
            raise LeaseQuarantinedError(
                self.slot_index,
                self.generation,
                "quarantined lease cannot be released without disposal",
            )
        if not self._released:
            if self._transport is not None:
                release_sync_fn = getattr(self._transport, "_release_sync", None)
                if not callable(release_sync_fn):
                    release_sync_fn = getattr(self._transport, "release_sync", None)
                if not callable(release_sync_fn):
                    raise InterfaceError("transport does not support synchronous lease release")
                release_sync_fn(self.lease_id)
            self._released = True

    async def __aenter__(self) -> AsyncConnectionLease:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not self._quarantined:
            await self.release()


_STREAM_END = object()


class _BaseBrokerStream:
    """Shared streaming cursor state, DBRPC frame decoding, and cancellation helpers."""

    def __init__(
        self,
        transport: _BaseSHMBrokerTransport,
        dialect_tag: int,
        engine_pool_id: int | None,
        sql: str = "",
        *,
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        lease: _BaseConnectionLease | None = None,
        chunk_size: int = 1024,
        chunk_source: Iterable[DbRpcResponse | bytes | str]
        | Callable[[], Iterator[DbRpcResponse | bytes | str]]
        | None = None,
        on_cancel: Callable[[], None] | None = None,
    ) -> None:
        self._transport = transport
        self._dialect_tag = dialect_tag
        self._engine_pool_id = engine_pool_id
        self._sql = sql
        self._params = tuple(params.values()) if isinstance(params, dict) else tuple(params)
        self._lease = lease
        self._chunk_size = chunk_size
        self._on_cancel = on_cancel
        resolved_source = chunk_source() if callable(chunk_source) else chunk_source
        self._chunk_iter: Iterator[DbRpcResponse | bytes | str] | None = (
            iter(resolved_source) if resolved_source is not None else None
        )
        self._request_id = transport._next_request_id()
        self._started = False
        self._exhausted = False
        self._cancelled = False

    @property
    def cancelled(self) -> bool:
        """Return whether ``cancel()`` has aborted this stream."""
        return self._cancelled

    @property
    def lease(self) -> _BaseConnectionLease | None:
        """Return the broker lease bound to this stream, if any."""
        return self._lease

    def _ensure_active(self) -> None:
        if self._cancelled or (self._lease is not None and self._lease.quarantined):
            raise LeaseQuarantinedError(
                self._lease.slot_index if self._lease is not None else 0,
                self._lease.generation if self._lease is not None else 0,
                "broker stream cancelled and backing lease quarantined",
            )

    def _decode_chunk(self, chunk: DbRpcResponse | bytes | str) -> DbRpcResponse:
        if isinstance(chunk, DbRpcResponse):
            return chunk
        if isinstance(chunk, str):
            return DbRpcResponse(request_id=self._request_id, status=0, arrow_shm=chunk)
        try:
            return decode_dbrpc_response(chunk)
        except DataError:
            raise
        except (msgspec.MsgspecError, ValueError, TypeError) as exc:
            raise DataError(f"Malformed DBRPC response frame: {exc}") from exc

    def _execute_next_dbrpc(self, engine_pool_id: int) -> DbRpcResponse:
        if not self._started:
            self._started = True
            return self._transport._execute_dbrpc_impl(
                self._dialect_tag,
                engine_pool_id,
                DbRpcOp.EXECUTE,
                sql=self._sql,
                params=self._params,
                lease=self._lease,
                chunk_rows=self._chunk_size,
            )
        return self._transport._execute_dbrpc_impl(
            self._dialect_tag,
            engine_pool_id,
            DbRpcOp.FETCH_CHUNK,
            lease=self._lease,
            chunk_rows=self._chunk_size,
        )

    def _mark_exhausted_if_terminal(self, resp: DbRpcResponse) -> DbRpcResponse:
        if (
            resp.arrow_shm is not None
            or resp.rows is None
            or (self._chunk_size <= 0 or len(resp.rows) < self._chunk_size)
        ):
            self._exhausted = True
        return resp

    def _cancel_impl(self) -> None:
        if self._engine_pool_id is not None and self._dialect_tag > 0:
            with contextlib.suppress(Exception):
                req = DbRpcRequest(
                    version=DBRPC_VERSION,
                    op=int(DbRpcOp.CANCEL),
                    request_id=self._request_id,
                    lease_slot=self._lease.slot_index if self._lease is not None else 0,
                    lease_generation=self._lease.generation if self._lease is not None else 0,
                )
                BrokerEngineRegistry.execute(
                    self._dialect_tag,
                    self._engine_pool_id,
                    encode_dbrpc_request(req),
                )
        if self._on_cancel is not None:
            with contextlib.suppress(Exception):
                self._on_cancel()
        if self._lease is not None and not self._lease.quarantined:
            self._transport._quarantine_impl(self._lease)

    def _detach_chunk_iter(self) -> Iterator[DbRpcResponse | bytes | str] | None:
        self._exhausted = True
        chunk_iter = self._chunk_iter
        self._chunk_iter = None
        return chunk_iter


class SyncBrokerStream(_BaseBrokerStream):
    """Streaming cursor over DBRPC query execution or chunk sources with cancellation quarantine."""

    def __iter__(self) -> SyncBrokerStream:
        return self

    def __next__(self) -> DbRpcResponse:
        self._ensure_active()
        if self._exhausted:
            raise StopIteration

        if self._chunk_iter is not None:
            chunk = next(self._chunk_iter, _STREAM_END)
            if chunk is _STREAM_END:
                self._exhausted = True
                raise StopIteration
            self._ensure_active()
            return self._decode_chunk(cast("DbRpcResponse | bytes | str", chunk))

        pool_id = self._engine_pool_id
        if pool_id is None:
            self._exhausted = True
            raise StopIteration

        try:
            resp = self._execute_next_dbrpc(pool_id)
        except Exception:
            self._exhausted = True
            raise
        return self._mark_exhausted_if_terminal(resp)

    def cancel(self) -> None:
        """Abort the active stream and quarantine the backing connection lease."""
        if self._cancelled:
            return
        self._cancelled = True
        chunk_iter = self._detach_chunk_iter()
        close_fn = getattr(chunk_iter, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()
        self._cancel_impl()

    def close(self) -> None:
        """Mark the stream exhausted without quarantining unless already cancelled."""
        chunk_iter = self._detach_chunk_iter()
        close_fn = getattr(chunk_iter, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                close_fn()

    def __enter__(self) -> SyncBrokerStream:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None and not self._exhausted and self._lease is not None and not self._cancelled:
            self.cancel()
        else:
            self.close()


class AsyncBrokerStream(_BaseBrokerStream):
    """Asynchronous streaming cursor over DBRPC query execution or chunk sources."""

    def __aiter__(self) -> AsyncBrokerStream:
        return self

    async def __anext__(self) -> DbRpcResponse:
        self._ensure_active()
        if self._exhausted:
            raise StopAsyncIteration

        if self._chunk_iter is not None:
            chunk = await SyncPool.default().submit_async(next, self._chunk_iter, _STREAM_END)
            if chunk is _STREAM_END:
                self._exhausted = True
                raise StopAsyncIteration
            self._ensure_active()
            return self._decode_chunk(cast("DbRpcResponse | bytes | str", chunk))

        pool_id = self._engine_pool_id
        if pool_id is None:
            self._exhausted = True
            raise StopAsyncIteration

        try:
            resp = await SyncPool.default().submit_async(self._execute_next_dbrpc, pool_id)
        except Exception:
            self._exhausted = True
            raise
        self._ensure_active()
        return self._mark_exhausted_if_terminal(resp)

    async def _close_chunk_iter_async(self) -> None:
        chunk_iter = self._detach_chunk_iter()
        if chunk_iter is None:
            return
        aclose_fn: Callable[[], Awaitable[Any]] | None = getattr(chunk_iter, "aclose", None)
        if callable(aclose_fn):
            with contextlib.suppress(Exception):
                await aclose_fn()
            return
        close_fn: Callable[[], Any] | None = getattr(chunk_iter, "close", None)
        if callable(close_fn):
            with contextlib.suppress(Exception):
                await SyncPool.default().submit_async(close_fn)

    async def cancel(self) -> None:
        """Abort the active stream asynchronously and quarantine the backing connection lease."""
        if self._cancelled:
            return
        self._cancelled = True
        await self._close_chunk_iter_async()
        await SyncPool.default().submit_async(self._cancel_impl)

    async def aclose(self) -> None:
        """Mark the stream exhausted and release chunk resources idempotently."""
        await self._close_chunk_iter_async()

    async def __aenter__(self) -> AsyncBrokerStream:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if exc_type is not None and not self._exhausted and self._lease is not None and not self._cancelled:
            await self.cancel()
        else:
            await self.aclose()


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
        self._config = raw_cfg

        if raw_cfg.role not in ("owner", "worker"):
            raise ValueError(f"unsupported broker role: {raw_cfg.role!r}")

        self._created: bool = False
        if native is not None:
            self._native = native
        else:
            env_broker = LeaseBroker.from_env()
            if env_broker is not None:
                self._native = env_broker
            elif self._config.socket_path and self._config.role == "worker":
                self._native = self._attach_unix_socket(self._config.socket_path)
            elif self._config.shm_name:
                self._native, self._created = self._attach_named_segment(self._config.shm_name)
            elif self._config.role == "worker":
                raise BrokerAttachError("worker broker role requires shm_name, socket_path, or inherited SHM FD")
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
        self._pinned_sessions: dict[str, _BaseConnectionLease] = {}
        self._partitions: dict[str, int] = {}
        self._partition_names: dict[int, str] = {}
        self._pool_counters: dict[str, dict[str, int]] = {}
        self._lease_partitions: dict[tuple[int, int], str] = {}
        self._lease_partition_ids: dict[tuple[int, int], int] = {}
        self._quarantined_partitions: dict[tuple[int, int], str] = {}
        self._quarantined_partition_ids: dict[tuple[int, int], int] = {}
        self._has_partition_count = callable(getattr(self._native, "partition_count", None))
        self._request_seq = 0
        self._lock = threading.Lock()

    def _next_request_id(self) -> int:
        with self._lock:
            self._request_seq += 1
            return self._request_seq

    def _attach_unix_socket(self, socket_path: str) -> LeaseBroker:
        if self._config.attach_secret is None or (
            isinstance(self._config.attach_secret, (bytes, str)) and len(self._config.attach_secret) == 0
        ):
            raise BrokerAuthenticationError("attach_secret is required when attaching over Unix domain socket")
        raw_fd, shm_size = BrokerAttachClient.attach(
            socket_path,
            self._config.attach_secret,
            self._config.acquire_timeout_ms,
        )
        try:
            return LeaseBroker.from_fd(raw_fd, shm_size)
        finally:
            os.close(raw_fd)

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
    def config(self) -> BrokerConfig:
        """Return immutable broker configuration bound to this transport."""
        return self._config

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
            self._partition_names[part_id] = pool_key
            self._pool_counters.setdefault(pool_key, self._empty_pool_counters())
        return part_id

    @staticmethod
    def _empty_pool_counters() -> dict[str, int]:
        return {
            "active_leases": 0,
            "quarantined": 0,
            "total_acquired": 0,
            "total_released": 0,
            "total_reclaimed": 0,
            "total_quarantined": 0,
            "total_disposed": 0,
        }

    def _resolve_partition(self, partition: int | None, bind_key: str | None, bind_hash: int) -> int | None:
        if partition is not None or bind_key is None:
            return partition
        if self._has_partition_count and self._native.partition_count() == 0:
            return None
        with self._lock:
            resolved = self._partitions.get(bind_key)
        if resolved is not None:
            return resolved
        looked_up = self._native.lookup_partition(bind_hash)
        if looked_up is not None:
            with self._lock:
                self._partitions.setdefault(bind_key, looked_up)
                self._partition_names.setdefault(looked_up, bind_key)
            return looked_up
        return None

    def _register_acquired_lease(
        self,
        lease: _BaseConnectionLease,
        slot_index: int,
        generation: int,
        partition: int | None,
        bind_key: str,
    ) -> str:
        """Record active lease ownership and partition mapping under a single lock acquisition."""
        with self._lock:
            self._active_leases[lease.lease_id] = lease
            if partition is not None:
                pool_name = self._partition_names.get(partition, bind_key or f"partition-{partition}")
                self._lease_partitions[(slot_index, generation)] = pool_name
                self._lease_partition_ids[(slot_index, generation)] = partition
                self._pool_counters.setdefault(pool_name, self._empty_pool_counters())
                self._pool_counters[pool_name]["total_acquired"] += 1
                self._pool_counters[pool_name]["active_leases"] += 1
                return pool_name
        return bind_key

    @staticmethod
    def _bind_key_hash(bind_key: str | None) -> int:
        if bind_key is None:
            return 0
        if bind_key == "default":
            return _DEFAULT_BIND_KEY_HASH
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
        partition_stats_fn: Callable[[int], PartitionStats | None] | None = getattr(
            self._native, "partition_stats", None
        )
        with self._lock:
            partition_items = [
                (pool_name, pool_id, dict(self._pool_counters.get(pool_name, self._empty_pool_counters())))
                for pool_name, pool_id in self._partitions.items()
            ]
        pools: dict[str, PoolMetrics] = {}
        for pool_name, pool_id, counts in partition_items:
            shm_stats = partition_stats_fn(self._bind_key_hash(pool_name)) if callable(partition_stats_fn) else None
            if shm_stats is not None:
                pools[pool_name] = PoolMetrics(
                    pool_name=pool_name,
                    pool_id=pool_id,
                    active_leases=shm_stats.active_leases,
                    quarantined=shm_stats.quarantined,
                    total_acquired=shm_stats.total_acquired,
                    total_released=shm_stats.total_released,
                    total_reclaimed=shm_stats.total_reclaimed,
                    total_quarantined=shm_stats.total_quarantined,
                    total_disposed=shm_stats.total_disposed,
                )
            else:
                pools[pool_name] = PoolMetrics(
                    pool_name=pool_name,
                    pool_id=pool_id,
                    active_leases=counts["active_leases"],
                    quarantined=counts["quarantined"],
                    total_acquired=counts["total_acquired"],
                    total_released=counts["total_released"],
                    total_reclaimed=counts["total_reclaimed"],
                    total_quarantined=counts["total_quarantined"],
                    total_disposed=counts["total_disposed"],
                )
        return BrokerMetrics(
            active_leases=stats.active_leases,
            primary_in_use=stats.primary_in_use,
            overflow_in_use=stats.overflow_in_use,
            waiting_threads=stats.waiting_threads,
            quarantined=stats.quarantined,
            total_acquired=stats.total_acquired,
            total_released=stats.total_released,
            total_expired=stats.total_expired,
            total_timeouts=stats.total_timeouts,
            total_reclaimed=stats.total_reclaimed,
            total_quarantined=stats.total_quarantined,
            total_disposed=stats.total_disposed,
            statement_cache_hits=stmt_stats.hits,
            statement_cache_misses=stmt_stats.misses,
            statement_cache_evictions=stmt_stats.evictions,
            pools=pools,
        )

    def _record_pool_release(self, slot_index: int, generation: int) -> int | None:
        with self._lock:
            pool_name = self._lease_partitions.pop((slot_index, generation), None)
            partition_id = self._lease_partition_ids.pop((slot_index, generation), None)
            if pool_name and pool_name in self._pool_counters:
                counters = self._pool_counters[pool_name]
                if counters["active_leases"] > 0:
                    counters["active_leases"] -= 1
                counters["total_released"] += 1
            if pool_name and pool_name in self._partitions:
                return self._partitions[pool_name]
            return partition_id

    def _record_pool_quarantine(
        self,
        slot_index: int,
        old_generation: int,
        new_generation: int,
        pool_id: int | None = None,
        *,
        reclaimed: bool = False,
    ) -> None:
        with self._lock:
            pool_name = self._lease_partitions.pop((slot_index, old_generation), None)
            partition_id = self._lease_partition_ids.pop((slot_index, old_generation), None)
            if partition_id is None and pool_id is not None and pool_id in self._partition_names:
                partition_id = pool_id
            if not pool_name and pool_id is not None:
                pool_name = self._partition_names.get(pool_id, "")
            if partition_id is not None:
                self._quarantined_partition_ids[(slot_index, new_generation)] = partition_id
            if pool_name and pool_name in self._pool_counters:
                counters = self._pool_counters[pool_name]
                if counters["active_leases"] > 0:
                    counters["active_leases"] -= 1
                counters["quarantined"] += 1
                counters["total_quarantined"] += 1
                if reclaimed:
                    counters["total_reclaimed"] += 1
                self._quarantined_partitions[(slot_index, new_generation)] = pool_name

    def _record_pool_dispose(self, slot_index: int, generation: int) -> int | None:
        with self._lock:
            pool_name = self._quarantined_partitions.pop((slot_index, generation), None)
            partition_id = self._quarantined_partition_ids.pop((slot_index, generation), None)
            if pool_name and pool_name in self._pool_counters:
                counters = self._pool_counters[pool_name]
                if counters["quarantined"] > 0:
                    counters["quarantined"] -= 1
                counters["total_disposed"] += 1
            if pool_name and pool_name in self._partitions:
                return self._partitions[pool_name]
            return partition_id

    def _on_slot_freed(self, partition: int | None) -> None:
        """Hook invoked after a local release or quarantined disposal frees a slot."""

    def _acquire_sync(
        self,
        worker_id: int | None = None,
        timeout_ms: int | None = None,
        bind_key: str | None = "default",
        timeout: float | None = None,
        partition: int | None = None,
    ) -> SyncConnectionLease:
        """Acquire a connection lease synchronously, detaching GIL during wait."""
        worker = worker_id if worker_id is not None else os.getpid()
        resolved_bind_key = bind_key if bind_key is not None else ""
        effective_timeout = (
            int(timeout * 1000)
            if timeout is not None
            else (timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms)
        )
        ttl_ns = self._config.lease_ttl_ms * 1_000_000
        bind_hash = self._bind_key_hash(bind_key)

        resolved_partition = self._resolve_partition(partition, bind_key, bind_hash)

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

        resolved_pool_id = resolved_partition if resolved_partition is not None else 0
        lease_id = f"{grant.slot_index}:{grant.generation}"
        lease = SyncConnectionLease(
            lease_id=lease_id,
            worker_id=worker,
            acquired_at_ns=grant.acquired_ns,
            ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
            bind_key=resolved_bind_key,
            is_overflow=grant.is_overflow,
            slot_index=grant.slot_index,
            generation=grant.generation,
            transport=self,
            pool_id=resolved_pool_id,
            pool_name=resolved_bind_key,
        )
        lease.pool_name = self._register_acquired_lease(
            lease,
            grant.slot_index,
            grant.generation,
            resolved_partition,
            resolved_bind_key,
        )
        return lease

    def _release_sync(self, lease_id: str) -> bool:
        """Release an acquired connection lease synchronously."""
        return self._release_impl(lease_id)

    def _release_impl(self, lease_id: str) -> bool:
        """Release an acquired connection lease."""
        with self._lock:
            lease = self._active_leases.pop(lease_id, None)
            if lease is not None and lease._pinned_session_id is not None:
                self._pinned_sessions.pop(lease._pinned_session_id, None)

        if lease is not None:
            if lease._quarantined:
                raise LeaseQuarantinedError(
                    lease.slot_index,
                    lease.generation,
                    "quarantined lease cannot be released without disposal",
                )
            released = self._native.release(lease.slot_index, lease.generation)
            if released:
                freed_partition = self._record_pool_release(lease.slot_index, lease.generation)
                self._on_slot_freed(freed_partition)
            return released

        try:
            parts = lease_id.split(":")
            if len(parts) == 2:
                slot_idx = int(parts[0])
                gen = int(parts[1])
                released = self._native.release(slot_idx, gen)
                if released:
                    freed_partition = self._record_pool_release(slot_idx, gen)
                    self._on_slot_freed(freed_partition)
                return released
        except (ValueError, TypeError):
            pass
        return False

    def _resolve_slot_generation(
        self,
        target: _BaseConnectionLease | str | int | tuple[int, int],
        generation: int | None = None,
    ) -> tuple[_BaseConnectionLease | None, int, int]:
        if isinstance(target, _BaseConnectionLease):
            return target, target.slot_index, target.generation
        if isinstance(target, tuple):
            return None, int(target[0]), int(target[1])
        if generation is not None:
            if not isinstance(target, int | str):
                raise ValueError(f"cannot resolve broker lease target: {target!r}")
            return None, int(target), int(generation)
        if isinstance(target, str):
            with self._lock:
                pinned = self._pinned_sessions.get(target)
                if pinned is not None:
                    return pinned, pinned.slot_index, pinned.generation
                active = self._active_leases.get(target)
                if active is not None:
                    return active, active.slot_index, active.generation
            parts = target.split(":")
            if len(parts) == 2:
                return None, int(parts[0]), int(parts[1])
        raise ValueError(f"cannot resolve broker lease target: {target!r}")

    def _quarantine_impl(
        self,
        target: _BaseConnectionLease | str | tuple[int, int],
    ) -> None:
        lease, slot_idx, gen = self._resolve_slot_generation(target)
        self._native.quarantine(slot_idx, gen)
        pool_id = lease.pool_id if lease is not None else None
        if lease is not None:
            lease._quarantined = True
            lease._released = True
            lease._in_transaction = False
            with self._lock:
                self._active_leases.pop(lease.lease_id, None)
                if lease._pinned_session_id is not None:
                    self._pinned_sessions.pop(lease._pinned_session_id, None)
        self._record_pool_quarantine(slot_idx, gen, gen, pool_id=pool_id)

    def _dispose_quarantined_impl(
        self,
        target: _BaseConnectionLease | int | tuple[int, int],
        generation: int | None = None,
    ) -> None:
        _, slot_idx, gen = self._resolve_slot_generation(target, generation)
        self._native.dispose_quarantined(slot_idx, gen)
        freed_partition = self._record_pool_dispose(slot_idx, gen)
        self._on_slot_freed(freed_partition)

    def _reclaim_worker_leases_impl(self, worker_pid: int) -> list[ReclaimedLease]:
        reclaimed = self._native.reclaim_worker_leases(worker_pid)
        with self._lock:
            to_remove = [lid for lid, lease in self._active_leases.items() if lease.worker_id == worker_pid]
            for lid in to_remove:
                removed = self._active_leases.pop(lid)
                removed._quarantined = True
                removed._released = True
                if removed._pinned_session_id is not None:
                    self._pinned_sessions.pop(removed._pinned_session_id, None)
        for item in reclaimed:
            self._record_pool_quarantine(
                item.slot_index,
                item.generation - 1 if item.generation > 0 else 0,
                item.generation,
                pool_id=item.pool_id,
                reclaimed=True,
            )
        return reclaimed

    def _execute_dbrpc_impl(
        self,
        dialect_tag: int,
        engine_pool_id: int,
        op: DbRpcOp,
        *,
        sql: str = "",
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        savepoint: str | None = None,
        lease: _BaseConnectionLease | None = None,
        chunk_rows: int = 0,
    ) -> DbRpcResponse:
        if lease is not None and lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot execute DBRPC on a quarantined lease",
            )
        resolved_params = tuple(params.values()) if isinstance(params, dict) else tuple(params)
        req = DbRpcRequest(
            version=DBRPC_VERSION,
            op=int(op),
            request_id=self._next_request_id(),
            lease_slot=lease.slot_index if lease is not None else 0,
            lease_generation=lease.generation if lease is not None else 0,
            sql=sql or None,
            params=resolved_params if resolved_params else None,
            savepoint=savepoint,
            chunk_rows=chunk_rows,
        )
        raw = BrokerEngineRegistry.execute(dialect_tag, engine_pool_id, encode_dbrpc_request(req))
        resp = decode_dbrpc_response(raw)
        if resp.status != 0:
            op_name = getattr(op, "name", str(op))
            detail = (
                f"{resp.error_class}: {resp.error_message}"
                if resp.error_message
                else (resp.error_class or f"status={resp.status}")
            )
            raise BrokerAttachError(f"DBRPC {op_name} failed ({detail})")
        return resp

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

    def _get_pinned_session_impl(self, session_id: str) -> _BaseConnectionLease | None:
        """Return currently pinned lease for ``session_id`` if still valid."""
        with self._lock:
            return self._pinned_sessions.get(session_id)

    def _unpin_session_impl(self, session_id: str) -> _BaseConnectionLease | None:
        """Detach logical ``session_id`` affinity without releasing the underlying lease."""
        with self._lock:
            lease = self._pinned_sessions.pop(session_id, None)
            if lease is not None:
                lease._pinned_session_id = None
            return lease

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

    def quarantine(self, target: _BaseConnectionLease | str | tuple[int, int]) -> None:
        """Move an active lease into quarantine until its session is explicitly disposed."""
        self._quarantine_impl(target)

    def dispose_quarantined(
        self,
        target: _BaseConnectionLease | int | tuple[int, int],
        generation: int | None = None,
    ) -> None:
        """Release a quarantined slot back into the free pool."""
        self._dispose_quarantined_impl(target, generation)

    def quarantined_count(self) -> int:
        """Return count of currently quarantined connection slots."""
        return self._native.quarantined_count()

    def reclaim_worker_leases(self, worker_pid: int) -> list[ReclaimedLease]:
        """Transition all active leases held by ``worker_pid`` into quarantine."""
        return self._reclaim_worker_leases_impl(worker_pid)

    def pin_session(
        self,
        session_id: str,
        *,
        bind_key: str | None = None,
        timeout_ms: int | None = None,
        partition: int | None = None,
    ) -> SyncConnectionLease:
        """Acquire or reuse the lease pinned to logical ``session_id``."""
        with self._lock:
            existing = self._pinned_sessions.get(session_id)
            if isinstance(existing, SyncConnectionLease) and not existing.is_released and not existing.quarantined:
                return existing
        lease = self._acquire_sync(
            bind_key=bind_key if bind_key is not None else session_id,
            timeout_ms=timeout_ms,
            partition=partition,
        )
        lease._pinned_session_id = session_id
        with self._lock:
            self._pinned_sessions[session_id] = lease
        return lease

    def get_pinned_session(self, session_id: str) -> _BaseConnectionLease | None:
        """Return currently pinned lease for ``session_id`` if still valid."""
        return self._get_pinned_session_impl(session_id)

    def unpin_session(self, session_id: str) -> _BaseConnectionLease | None:
        """Detach logical ``session_id`` affinity without releasing the underlying lease."""
        return self._unpin_session_impl(session_id)

    def begin_transaction(
        self,
        session_or_lease: SyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        bind_key: str | None = None,
        partition: int | None = None,
    ) -> SyncConnectionLease:
        """Pin a lease to an explicit transaction and optionally send ``DbRpcOp.BEGIN``."""
        if isinstance(session_or_lease, str):
            lease = self.pin_session(session_or_lease, bind_key=bind_key, partition=partition)
        else:
            lease = session_or_lease
            if lease.quarantined:
                raise LeaseQuarantinedError(
                    lease.slot_index,
                    lease.generation,
                    "cannot begin transaction on a quarantined lease",
                )
            if lease._pinned_session_id is not None:
                with self._lock:
                    self._pinned_sessions[lease._pinned_session_id] = lease
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot begin transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            self._execute_dbrpc_impl(dialect_tag, engine_pool_id, DbRpcOp.BEGIN, lease=lease)
        lease._in_transaction = True
        return lease

    def commit_transaction(
        self,
        session_or_lease: SyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        release: bool = True,
    ) -> bool:
        """Commit an active transaction and optionally release the pinned lease."""
        lease = self._resolve_tx_lease(session_or_lease)
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot commit transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            self._execute_dbrpc_impl(dialect_tag, engine_pool_id, DbRpcOp.COMMIT, lease=lease)
        lease._in_transaction = False
        if lease._pinned_session_id is not None:
            self.unpin_session(lease._pinned_session_id)
        if release and not lease.is_released:
            lease.release()
            return True
        return False

    def rollback_transaction(
        self,
        session_or_lease: SyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        release: bool = True,
    ) -> bool:
        """Rollback an active transaction, quarantining the lease if rollback fails."""
        lease = self._resolve_tx_lease(session_or_lease)
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot rollback transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            try:
                self._execute_dbrpc_impl(dialect_tag, engine_pool_id, DbRpcOp.ROLLBACK, lease=lease)
            except Exception:
                self.quarantine(lease)
                raise
        lease._in_transaction = False
        if lease._pinned_session_id is not None:
            self.unpin_session(lease._pinned_session_id)
        if release and not lease.is_released:
            lease.release()
            return True
        return False

    def _resolve_tx_lease(self, session_or_lease: SyncConnectionLease | str) -> SyncConnectionLease:
        if isinstance(session_or_lease, SyncConnectionLease):
            return session_or_lease
        with self._lock:
            pinned = self._pinned_sessions.get(session_or_lease)
        if isinstance(pinned, SyncConnectionLease):
            return pinned
        raise LookupError(f"no pinned lease found for session {session_or_lease!r}")

    def execute_dbrpc(
        self,
        dialect_tag: int,
        engine_pool_id: int,
        op: DbRpcOp,
        *,
        sql: str = "",
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        savepoint: str | None = None,
        lease: _BaseConnectionLease | None = None,
        chunk_rows: int = 0,
    ) -> DbRpcResponse:
        """Execute a single binary DBRPC operation against a registered engine pool."""
        return self._execute_dbrpc_impl(
            dialect_tag,
            engine_pool_id,
            op,
            sql=sql,
            params=params,
            savepoint=savepoint,
            lease=lease,
            chunk_rows=chunk_rows,
        )

    def stream_dbrpc(
        self,
        dialect_tag: int,
        engine_pool_id: int | None,
        sql: str = "",
        *,
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        lease: _BaseConnectionLease | None = None,
        chunk_size: int = 1024,
        chunk_source: Iterable[DbRpcResponse | bytes | str]
        | Callable[[], Iterator[DbRpcResponse | bytes | str]]
        | None = None,
        on_cancel: Callable[[], None] | None = None,
    ) -> SyncBrokerStream:
        """Open a streaming DBRPC cursor supporting ``VTBARROW`` and row chunk responses."""
        return SyncBrokerStream(
            self,
            dialect_tag,
            engine_pool_id,
            sql,
            params=params,
            lease=lease,
            chunk_size=chunk_size,
            chunk_source=chunk_source,
            on_cancel=on_cancel,
        )

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

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        native: LeaseBroker | None = None,
    ) -> None:
        super().__init__(config=config, native=native)
        self._waiter_lock = threading.Lock()
        self._waiter_queues: WeakKeyDictionary[asyncio.AbstractEventLoop, dict[int | None, _PartitionWaitQueue]] = (
            WeakKeyDictionary()
        )
        self._has_native_wake = callable(getattr(self._native, "wait_for_wake_async", None)) and callable(
            getattr(self._native, "wake_seq", None)
        )
        self._has_record_timeout = callable(getattr(self._native, "record_timeout", None))

    @staticmethod
    def _resolve_future(fut: asyncio.Future[bool], from_broker: bool) -> None:
        if not fut.done():
            fut.set_result(from_broker)

    def _record_timeout(self) -> None:
        if self._has_record_timeout:
            self._native.record_timeout()

    def _try_acquire_once(
        self,
        resolved_partition: int | None,
        worker: int,
        ttl_ns: int,
        bind_hash: int,
    ) -> Any:
        if resolved_partition is not None:
            return self._native.try_acquire_in(
                partition=resolved_partition,
                worker_pid=worker,
                ttl_ns=ttl_ns,
                bind_key_hash=bind_hash,
            )
        return self._native.try_acquire(worker_pid=worker, ttl_ns=ttl_ns, bind_key_hash=bind_hash)

    def _get_wait_queue(
        self,
        loop: asyncio.AbstractEventLoop,
        partition: int | None,
    ) -> _PartitionWaitQueue:
        with self._waiter_lock:
            loop_queues = self._waiter_queues.get(loop)
            if loop_queues is None:
                loop_queues = {}
                self._waiter_queues[loop] = loop_queues
            queue = loop_queues.get(partition)
            if queue is None:
                queue = _PartitionWaitQueue()
                loop_queues[partition] = queue
            return queue

    def _pop_live_waiter_locked(self, queue: _PartitionWaitQueue) -> asyncio.Future[bool] | None:
        while queue.waiters:
            candidate = queue.waiters.popleft()
            if not candidate.done():
                return candidate
        return None

    def _discard_waiter(self, queue: _PartitionWaitQueue, waiter: asyncio.Future[bool]) -> None:
        task_to_cancel: asyncio.Task[None] | None = None
        with self._waiter_lock:
            with contextlib.suppress(ValueError):
                queue.waiters.remove(waiter)
            while queue.waiters and queue.waiters[0].done():
                queue.waiters.popleft()
            if not queue.waiters and queue.watcher_task is not None:
                task_to_cancel = queue.watcher_task
                queue.watcher_active = False
                queue.watcher_task = None
                queue.local_wakes = 0
        if task_to_cancel is not None and not task_to_cancel.done():
            task_to_cancel.cancel()

    async def _watch_broker_wakes(self, queue: _PartitionWaitQueue, initial_seq: int) -> None:
        """Watch cross-process SHM wake sequence updates for a non-empty partition waiter queue."""
        seq = initial_seq
        try:
            while True:
                with self._waiter_lock:
                    while queue.waiters and queue.waiters[0].done():
                        queue.waiters.popleft()
                    if not queue.waiters:
                        queue.local_wakes = 0
                        queue.watcher_active = False
                        queue.watcher_task = None
                        return

                next_seq = await self._native.wait_for_wake_async(seq, 25)
                to_wake: list[asyncio.Future[bool]] = []
                with self._waiter_lock:
                    while queue.waiters and queue.waiters[0].done():
                        queue.waiters.popleft()
                    if not queue.waiters:
                        queue.local_wakes = 0
                        queue.watcher_active = False
                        queue.watcher_task = None
                        return
                    if next_seq != seq:
                        delta = (next_seq - seq) & 0xFFFFFFFF
                        consumed_local = min(delta, queue.local_wakes)
                        queue.local_wakes -= consumed_local
                        remote_wakes = delta - consumed_local
                        for _ in range(min(remote_wakes, len(queue.waiters))):
                            fut = self._pop_live_waiter_locked(queue)
                            if fut is None:
                                break
                            to_wake.append(fut)
                    seq = next_seq
                for fut in to_wake:
                    self._resolve_future(fut, True)
        finally:
            current = asyncio.current_task()
            with self._waiter_lock:
                if queue.watcher_task is current:
                    queue.watcher_active = False
                    queue.watcher_task = None
                    if not queue.waiters:
                        queue.local_wakes = 0

    def _on_slot_freed(self, partition: int | None) -> None:
        """Wake a local async waiter directly on its event loop without touching SyncPool."""
        try:
            running_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        to_wake: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[bool]]] = []
        with self._waiter_lock:
            if not self._waiter_queues:
                return
            ordered_loops: list[tuple[asyncio.AbstractEventLoop, dict[int | None, _PartitionWaitQueue]]] = []
            if running_loop is not None and not running_loop.is_closed() and running_loop in self._waiter_queues:
                ordered_loops.append((running_loop, self._waiter_queues[running_loop]))
            for loop, loop_queues in self._waiter_queues.items():
                if loop is not running_loop and not loop.is_closed():
                    ordered_loops.append((loop, loop_queues))

            for loop, loop_queues in ordered_loops:
                queue = loop_queues.get(partition)
                if queue is not None:
                    fut = self._pop_live_waiter_locked(queue)
                    if fut is not None:
                        queue.local_wakes += 1
                        to_wake.append((loop, fut))
                        break

            if partition is not None:
                for loop, loop_queues in ordered_loops:
                    unpart_queue = loop_queues.get(None)
                    if unpart_queue is not None:
                        fut = self._pop_live_waiter_locked(unpart_queue)
                        if fut is not None:
                            unpart_queue.local_wakes += 1
                            to_wake.append((loop, fut))
                            break

            if not to_wake:
                for loop, loop_queues in ordered_loops:
                    for key, queue in loop_queues.items():
                        if key == partition or key is None:
                            continue
                        fut = self._pop_live_waiter_locked(queue)
                        if fut is not None:
                            queue.local_wakes += 1
                            to_wake.append((loop, fut))
                            break
                    if to_wake:
                        break

        for loop, fut in to_wake:
            if loop is running_loop:
                self._resolve_future(fut, True)
            else:
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(self._resolve_future, fut, True)

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

        resolved_partition = self._resolve_partition(partition, bind_key, bind_hash)

        grant = self._try_acquire_once(resolved_partition, worker, ttl_ns, bind_hash)
        if grant is not None:
            return self._build_async_lease(grant, worker, resolved_partition, resolved_bind_key)

        if timeout is not None:
            if timeout <= 0:
                self._record_timeout()
                raise TimeoutError("Connection broker pool exhausted")
            timeout_s = timeout
            effective_timeout_ms = max(1, int(timeout * 1000))
        else:
            effective_timeout_ms = timeout_ms if timeout_ms is not None else self._config.acquire_timeout_ms
            if effective_timeout_ms <= 0:
                self._record_timeout()
                raise TimeoutError("Connection broker pool exhausted")
            timeout_s = effective_timeout_ms / 1000.0

        if not self._has_native_wake:
            if resolved_partition is not None:
                blocking_grant = await SyncPool.default().submit_async(
                    self._native.acquire_in,
                    resolved_partition,
                    worker,
                    ttl_ns,
                    effective_timeout_ms,
                    bind_hash,
                )
            else:
                blocking_grant = await SyncPool.default().submit_async(
                    self._native.acquire,
                    worker,
                    ttl_ns,
                    effective_timeout_ms,
                    bind_hash,
                )
            if blocking_grant is None:
                raise TimeoutError("Connection broker pool exhausted")
            return self._build_async_lease(blocking_grant, worker, resolved_partition, resolved_bind_key)

        loop = asyncio.get_running_loop()
        queue = self._get_wait_queue(loop, resolved_partition)
        deadline = time.monotonic() + timeout_s

        while True:
            seq = self._native.wake_seq()
            grant = self._try_acquire_once(resolved_partition, worker, ttl_ns, bind_hash)
            if grant is not None:
                return self._build_async_lease(grant, worker, resolved_partition, resolved_bind_key)

            remaining_s = deadline - time.monotonic()
            if remaining_s <= 0:
                self._record_timeout()
                raise TimeoutError("Connection broker pool exhausted")

            waiter: asyncio.Future[bool] = loop.create_future()
            with self._waiter_lock:
                queue.waiters.append(waiter)
                if not queue.watcher_active:
                    queue.watcher_active = True
                    queue.local_wakes = 0
                    queue.watcher_task = loop.create_task(self._watch_broker_wakes(queue, seq))

            timer = loop.call_later(remaining_s, self._resolve_future, waiter, False)
            woken_by_broker = False
            try:
                woken_by_broker = await waiter
            except asyncio.CancelledError:
                consumed_wake = waiter.done() and not waiter.cancelled() and bool(waiter.result())
                if not waiter.done():
                    waiter.cancel()
                relay_fut: asyncio.Future[bool] | None = None
                if consumed_wake:
                    with self._waiter_lock:
                        relay_fut = self._pop_live_waiter_locked(queue)
                else:
                    self._discard_waiter(queue, waiter)
                woken_by_broker = True
                if relay_fut is not None:
                    self._resolve_future(relay_fut, True)
                raise
            finally:
                timer.cancel()
                if not woken_by_broker:
                    self._discard_waiter(queue, waiter)

    def _build_async_lease(
        self,
        grant: Any,
        worker: int,
        resolved_partition: int | None,
        resolved_bind_key: str,
    ) -> AsyncConnectionLease:
        """Construct and register an AsyncConnectionLease from a native SHM grant."""
        resolved_pool_id = resolved_partition if resolved_partition is not None else 0
        lease = AsyncConnectionLease(
            lease_id=f"{grant.slot_index}:{grant.generation}",
            worker_id=worker,
            acquired_at_ns=grant.acquired_ns,
            ttl_ns=grant.expiry_ns - grant.acquired_ns if grant.expiry_ns > 0 else 0,
            bind_key=resolved_bind_key,
            is_overflow=grant.is_overflow,
            slot_index=grant.slot_index,
            generation=grant.generation,
            transport=self,
            pool_id=resolved_pool_id,
            pool_name=resolved_bind_key,
        )
        lease.pool_name = self._register_acquired_lease(
            lease,
            grant.slot_index,
            grant.generation,
            resolved_partition,
            resolved_bind_key,
        )
        return lease

    async def release(self, lease_id: str) -> bool:
        """Release an acquired connection lease asynchronously without blocking on SyncPool waiters."""
        return self._release_impl(lease_id)

    async def quarantine(self, target: _BaseConnectionLease | str | tuple[int, int]) -> None:
        """Move an active lease into quarantine asynchronously without blocking on SyncPool waiters."""
        self._quarantine_impl(target)

    async def dispose_quarantined(
        self,
        target: _BaseConnectionLease | int | tuple[int, int],
        generation: int | None = None,
    ) -> None:
        """Return a quarantined slot back to the free pool asynchronously without blocking on SyncPool waiters."""
        self._dispose_quarantined_impl(target, generation)

    def quarantined_count(self) -> int:
        """Return count of currently quarantined connection slots."""
        return self._native.quarantined_count()

    async def reclaim_worker_leases(self, worker_pid: int) -> list[ReclaimedLease]:
        """Transition all active leases held by ``worker_pid`` into quarantine."""
        return await SyncPool.default().submit_async(self._reclaim_worker_leases_impl, worker_pid)

    async def pin_session(
        self,
        session_id: str,
        *,
        bind_key: str | None = None,
        timeout_ms: int | None = None,
        partition: int | None = None,
    ) -> AsyncConnectionLease:
        """Acquire or reuse the async lease pinned to logical ``session_id``."""
        with self._lock:
            existing = self._pinned_sessions.get(session_id)
            if isinstance(existing, AsyncConnectionLease) and not existing.is_released and not existing.quarantined:
                return existing
        lease = await self.acquire(
            bind_key=bind_key if bind_key is not None else session_id,
            timeout_ms=timeout_ms,
            partition=partition,
        )
        lease._pinned_session_id = session_id
        with self._lock:
            self._pinned_sessions[session_id] = lease
        return lease

    async def get_pinned_session(self, session_id: str) -> _BaseConnectionLease | None:
        """Return currently pinned lease for ``session_id`` asynchronously if still valid."""
        return await SyncPool.default().submit_async(self._get_pinned_session_impl, session_id)

    async def unpin_session(self, session_id: str) -> _BaseConnectionLease | None:
        """Detach logical ``session_id`` affinity asynchronously without releasing the underlying lease."""
        return await SyncPool.default().submit_async(self._unpin_session_impl, session_id)

    async def begin_transaction(
        self,
        session_or_lease: AsyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        bind_key: str | None = None,
        partition: int | None = None,
    ) -> AsyncConnectionLease:
        """Pin an async lease to an explicit transaction."""
        lease = (
            await self.pin_session(session_or_lease, bind_key=bind_key, partition=partition)
            if isinstance(session_or_lease, str)
            else session_or_lease
        )
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot begin transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            await SyncPool.default().submit_async(
                self._execute_dbrpc_impl,
                dialect_tag,
                engine_pool_id,
                DbRpcOp.BEGIN,
                lease=lease,
            )
        lease._in_transaction = True
        return lease

    async def commit_transaction(
        self,
        session_or_lease: AsyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        release: bool = True,
    ) -> bool:
        """Commit an active async transaction and optionally release the pinned lease."""
        lease = self._resolve_async_tx_lease(session_or_lease)
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot commit transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            await SyncPool.default().submit_async(
                self._execute_dbrpc_impl,
                dialect_tag,
                engine_pool_id,
                DbRpcOp.COMMIT,
                lease=lease,
            )
        lease._in_transaction = False
        if lease._pinned_session_id is not None:
            with self._lock:
                self._pinned_sessions.pop(lease._pinned_session_id, None)
            lease._pinned_session_id = None
        if release and not lease.is_released:
            await lease.release()
            return True
        return False

    async def rollback_transaction(
        self,
        session_or_lease: AsyncConnectionLease | str,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        release: bool = True,
    ) -> bool:
        """Rollback an active async transaction, quarantining the lease if rollback fails."""
        lease = self._resolve_async_tx_lease(session_or_lease)
        if lease.quarantined:
            raise LeaseQuarantinedError(
                lease.slot_index,
                lease.generation,
                "cannot rollback transaction on a quarantined lease",
            )
        if dialect_tag > 0 and engine_pool_id is not None:
            try:
                await SyncPool.default().submit_async(
                    self._execute_dbrpc_impl,
                    dialect_tag,
                    engine_pool_id,
                    DbRpcOp.ROLLBACK,
                    lease=lease,
                )
            except Exception:
                await self.quarantine(lease)
                raise
        lease._in_transaction = False
        if lease._pinned_session_id is not None:
            with self._lock:
                self._pinned_sessions.pop(lease._pinned_session_id, None)
            lease._pinned_session_id = None
        if release and not lease.is_released:
            await lease.release()
            return True
        return False

    def _resolve_async_tx_lease(self, session_or_lease: AsyncConnectionLease | str) -> AsyncConnectionLease:
        if isinstance(session_or_lease, AsyncConnectionLease):
            return session_or_lease
        with self._lock:
            pinned = self._pinned_sessions.get(session_or_lease)
        if isinstance(pinned, AsyncConnectionLease):
            return pinned
        raise LookupError(f"no pinned async lease found for session {session_or_lease!r}")

    async def execute_dbrpc(
        self,
        dialect_tag: int,
        engine_pool_id: int,
        op: DbRpcOp,
        *,
        sql: str = "",
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        savepoint: str | None = None,
        lease: _BaseConnectionLease | None = None,
        chunk_rows: int = 0,
    ) -> DbRpcResponse:
        """Execute a single binary DBRPC operation asynchronously off the event loop."""
        return await SyncPool.default().submit_async(
            self._execute_dbrpc_impl,
            dialect_tag,
            engine_pool_id,
            op,
            sql=sql,
            params=params,
            savepoint=savepoint,
            lease=lease,
            chunk_rows=chunk_rows,
        )

    def stream_dbrpc(
        self,
        dialect_tag: int,
        engine_pool_id: int | None,
        sql: str = "",
        *,
        params: tuple[Any, ...] | list[Any] | dict[str, Any] = (),
        lease: _BaseConnectionLease | None = None,
        chunk_size: int = 1024,
        chunk_source: Iterable[DbRpcResponse | bytes | str]
        | Callable[[], Iterator[DbRpcResponse | bytes | str]]
        | None = None,
        on_cancel: Callable[[], None] | None = None,
    ) -> AsyncBrokerStream:
        """Open an async streaming DBRPC cursor supporting ``VTBARROW`` and row chunk responses."""
        return AsyncBrokerStream(
            self,
            dialect_tag,
            engine_pool_id,
            sql,
            params=params,
            lease=lease,
            chunk_size=chunk_size,
            chunk_source=chunk_source,
            on_cancel=on_cancel,
        )

    async def publish(self, channel: str, payload: bytes, lease_id: str | None = None) -> ChannelMessage:
        """Publish a message to a named shared-memory channel asynchronously."""
        return await SyncPool.default().submit_async(self._publish_impl, channel, payload, lease_id)

    async def drain_channel(self, channel: str) -> list[ChannelMessage]:
        """Drain buffered messages for channel asynchronously."""
        return await SyncPool.default().submit_async(self._drain_channel_impl, channel)

    async def drain_all_channels(self) -> list[ChannelMessage]:
        """Drain buffered messages across all channels asynchronously."""
        return await SyncPool.default().submit_async(self._drain_all_channels_impl)

    async def close(self) -> None:
        """Release broker resources asynchronously."""
        await SyncPool.default().submit_async(self._close_impl)
