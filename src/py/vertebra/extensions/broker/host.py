"""Broker host supervision, session cleanup, and Click CLI commands."""

from __future__ import annotations

import contextlib
import queue
import threading
import time
from typing import TYPE_CHECKING, Any

import msgspec

from vertebra._vertebra import BrokerAttachServer, BrokerEngineRegistry, ReclaimedLease
from vertebra.extensions.broker.dbrpc import (
    DBRPC_VERSION,
    DbRpcOp,
    DbRpcRequest,
    decode_dbrpc_response,
    encode_dbrpc_request,
)
from vertebra.extensions.broker.errors import BrokerAttachError, BrokerAuthenticationError
from vertebra.extensions.broker.transport import BrokerConfig, SyncSHMBrokerTransport
from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

__all__ = ("BrokerHost", "ShutdownReport")


class ShutdownReport(msgspec.Struct, frozen=True, gc=False):
    """Structured summary produced by bounded ``BrokerHost.stop`` shutdown."""

    completed: bool
    closed_pools: int = 0
    rolled_back_leases: int = 0
    disposed_leases: int = 0
    rollback_errors: int = 0
    pending_leases: int = 0
    elapsed_ms: float = 0.0


class _SessionRegistration(msgspec.Struct, frozen=True):
    """Registered rollback and disposal metadata for a leased slot."""

    dialect_tag: int = 0
    engine_pool_id: int | None = None
    session_id: int = 0
    on_rollback: Callable[[ReclaimedLease], None] | None = None
    on_dispose: Callable[[ReclaimedLease], None] | None = None


class _HostExecutor:
    """Dedicated single-thread executor preserving C-driver thread affinity."""

    def __init__(self) -> None:
        self._queue: queue.Queue[tuple[Callable[[], Any], threading.Event, list[Any], list[BaseException]] | None] = (
            queue.Queue()
        )
        self._thread: threading.Thread | None = None
        self._thread_id: int | None = None
        self._ready = threading.Event()
        self._lock = threading.Lock()

    @property
    def owning_thread_id(self) -> int | None:
        """Return OS thread identifier of the dedicated broker host thread."""
        return self._thread_id

    def start(self) -> None:
        """Start the dedicated single-thread executor if not already active."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._queue = queue.Queue()
            self._ready.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="vtb-broker-host",
                daemon=True,
            )
            self._thread.start()
        self._ready.wait(timeout=5.0)

    def submit(self, fn: Callable[[], Any], *, timeout: float | None = 10.0) -> Any:
        """Execute ``fn`` synchronously on the owning executor thread."""
        self.start()
        done = threading.Event()
        result_box: list[Any] = []
        error_box: list[BaseException] = []
        self._queue.put((fn, done, result_box, error_box))
        if not done.wait(timeout=timeout):
            raise TimeoutError("broker host executor task timed out")
        if error_box:
            raise error_box[0]
        return result_box[0] if result_box else None

    def stop(self, timeout: float = 10.0) -> tuple[bool, int]:
        """Drain queued work and terminate the owning thread within ``timeout``."""
        with self._lock:
            thread = self._thread
        if thread is None or not thread.is_alive():
            pending = self._drain_pending_count()
            self._thread = None
            self._thread_id = None
            return True, pending
        self._queue.put(None)
        thread.join(timeout=max(0.0, timeout))
        if thread.is_alive():
            pending = 1 + self._drain_pending_count()
            return False, pending
        with self._lock:
            self._thread = None
            self._thread_id = None
        return True, 0

    def _run_loop(self) -> None:
        self._thread_id = threading.get_ident()
        self._ready.set()
        while True:
            item = self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            fn, done, result_box, error_box = item
            try:
                result_box.append(fn())
            except BaseException as exc:
                error_box.append(exc)
            finally:
                done.set()
                self._queue.task_done()

    def _drain_pending_count(self) -> int:
        count = 0
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is not None:
                count += 1
        return count


class BrokerHost:
    """Host supervisor owning broker SHM, UDS attachment, and crashed-worker session recovery."""

    def __init__(
        self,
        config: BrokerConfig | None = None,
        *,
        socket_path: str | None = None,
        secret: bytes | str | None = None,
        transport: SyncSHMBrokerTransport | None = None,
    ) -> None:
        base_cfg = config or BrokerConfig()
        if transport is None and base_cfg.role != "owner":
            base_cfg = msgspec.structs.replace(base_cfg, role="owner")
        self._config = base_cfg
        self._socket_path = socket_path if socket_path is not None else base_cfg.socket_path
        self._secret = secret if secret is not None else base_cfg.attach_secret
        self._transport = transport if transport is not None else SyncSHMBrokerTransport(base_cfg)
        self._executor = _HostExecutor()
        self._attach_server: BrokerAttachServer | None = None
        self._pools: dict[int, tuple[int, int]] = {}
        self._lease_sessions: dict[tuple[int, int], _SessionRegistration] = {}
        self._default_rollback: Callable[[ReclaimedLease], None] | None = None
        self._default_dispose: Callable[[ReclaimedLease], None] | None = None
        self._lock = threading.Lock()
        self._started = False
        self._stopped = False
        self._closed_pools = 0
        self._rolled_back_leases = 0
        self._disposed_leases = 0
        self._rollback_errors = 0

    def __repr__(self) -> str:
        """Render host summary with attach credentials redacted."""
        return f"BrokerHost(shm_name={self._config.shm_name!r}, socket_path={self._socket_path!r}, secret='[REDACTED]')"

    @property
    def config(self) -> BrokerConfig:
        """Return immutable broker configuration bound to this host."""
        return self._config

    @property
    def transport(self) -> SyncSHMBrokerTransport:
        """Return synchronous broker transport owned by this host."""
        return self._transport

    @property
    def socket_path(self) -> str | None:
        """Return configured Unix domain socket attachment path."""
        return self._socket_path

    @property
    def started(self) -> bool:
        """Return whether the host executor and optional UDS listener are active."""
        return self._started and not self._stopped

    @property
    def owning_thread_id(self) -> int | None:
        """Return OS thread ID of the single thread owning engine sessions."""
        return self._executor.owning_thread_id

    def start(self) -> None:
        """Start the owning session executor, discover installed engines, and bind UDS attach."""
        with self._lock:
            if self._started and not self._stopped:
                return
            self._stopped = False
            self._executor.start()
            engines_mod = ensure_module("vertebra.extensions.broker.engines")
            engines_mod.BrokerEngineDiscovery.register_installed()
            if self._socket_path is not None:
                if self._secret is None or (isinstance(self._secret, (bytes, str)) and len(self._secret) == 0):
                    raise BrokerAuthenticationError("attach secret is required when socket_path is configured")
                shm_fd = self._transport.shm_fd
                if shm_fd is None:
                    raise BrokerAttachError("broker SHM file descriptor is unavailable for UDS attach")
                server = BrokerAttachServer(
                    self._socket_path,
                    self._secret,
                    shm_fd,
                    self._transport.shm_size,
                )
                server.start()
                self._attach_server = server
            self._started = True

    def register_engine_pool(self, pool_id: int, dialect_tag: int, engine_pool_id: int) -> None:
        """Associate a broker partition ID with an open native engine pool handle."""
        with self._lock:
            self._pools[pool_id] = (dialect_tag, engine_pool_id)

    def open_engine_pool(self, pool_id: int, dialect_tag: int, dsn: str, max_size: int) -> int:
        """Open a native engine pool on the owning thread and register it with ``pool_id``."""
        self._executor.start()

        def _open() -> int:
            engine_pool_id = BrokerEngineRegistry.open_pool(dialect_tag, dsn, max_size)
            with self._lock:
                self._pools[pool_id] = (dialect_tag, engine_pool_id)
            return engine_pool_id

        return int(self._executor.submit(_open))

    def register_session_hooks(
        self,
        *,
        on_rollback: Callable[[ReclaimedLease], None] | None = None,
        on_dispose: Callable[[ReclaimedLease], None] | None = None,
    ) -> None:
        """Register default rollback and disposal callbacks invoked on crashed-worker recovery."""
        with self._lock:
            self._default_rollback = on_rollback
            self._default_dispose = on_dispose

    def register_lease_session(
        self,
        slot_index: int,
        generation: int,
        *,
        dialect_tag: int = 0,
        engine_pool_id: int | None = None,
        session_id: int = 0,
        on_rollback: Callable[[ReclaimedLease], None] | None = None,
        on_dispose: Callable[[ReclaimedLease], None] | None = None,
    ) -> None:
        """Attach explicit rollback/disposal hooks or engine pool metadata to a leased slot."""
        with self._lock:
            self._lease_sessions[(slot_index, generation)] = _SessionRegistration(
                dialect_tag=dialect_tag,
                engine_pool_id=engine_pool_id,
                session_id=session_id,
                on_rollback=on_rollback,
                on_dispose=on_dispose,
            )

    def on_worker_exit(self, worker_pid: int, *, timeout: float = 10.0) -> int:
        """Quarantine active leases held by ``worker_pid``, run owning-thread rollback, and dispose slots."""
        reclaimed = self._transport.reclaim_worker_leases(worker_pid)
        if not reclaimed:
            return 0
        self._executor.start()
        self._executor.submit(lambda: self._recover_batch(reclaimed), timeout=timeout)
        return len(reclaimed)

    def stop(self, timeout: float = 10.0) -> ShutdownReport:
        """Stop UDS attach serving, close registered pools within ``timeout``, and return a report."""
        start_ns = time.monotonic_ns()
        deadline = time.monotonic() + max(0.0, timeout)

        with self._lock:
            server = self._attach_server
            self._attach_server = None
            self._stopped = True

        if server is not None:
            server.stop()

        remaining_s = max(0.0, deadline - time.monotonic())
        close_timed_out = False
        if self._pools_to_close_count() > 0:
            try:
                self._executor.submit(self._close_registered_pools, timeout=remaining_s)
            except TimeoutError:
                close_timed_out = True

        remaining_s = max(0.0, deadline - time.monotonic())
        exec_completed, pending = self._executor.stop(timeout=remaining_s)
        completed = exec_completed and not close_timed_out

        if self._transport.created:
            self._transport.unlink()
        self._transport.close()

        elapsed_ms = (time.monotonic_ns() - start_ns) / 1_000_000.0
        with self._lock:
            return ShutdownReport(
                completed=completed,
                closed_pools=self._closed_pools,
                rolled_back_leases=self._rolled_back_leases,
                disposed_leases=self._disposed_leases,
                rollback_errors=self._rollback_errors,
                pending_leases=pending,
                elapsed_ms=elapsed_ms,
            )

    def __enter__(self) -> BrokerHost:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _pools_to_close_count(self) -> int:
        with self._lock:
            return len(self._pools)

    def _recover_batch(self, reclaimed: list[ReclaimedLease]) -> None:
        for lease in reclaimed:
            self._recover_single_lease(lease)

    def _recover_single_lease(self, lease: ReclaimedLease) -> None:
        with self._lock:
            reg = self._lease_sessions.pop((lease.slot_index, lease.generation), None)
            if reg is None and lease.generation > 0:
                reg = self._lease_sessions.pop((lease.slot_index, lease.generation - 1), None)
            pool_binding = self._pools.get(lease.pool_id) if lease.pool_id is not None else None
            default_rollback = self._default_rollback
            default_dispose = self._default_dispose

        rollback_fn = reg.on_rollback if reg is not None and reg.on_rollback is not None else default_rollback
        dispose_fn = reg.on_dispose if reg is not None and reg.on_dispose is not None else default_dispose

        try:
            if rollback_fn is not None:
                rollback_fn(lease)
            elif reg is not None and reg.engine_pool_id is not None and reg.dialect_tag > 0:
                self._send_rollback(
                    reg.dialect_tag,
                    reg.engine_pool_id,
                    lease.slot_index,
                    lease.generation,
                )
            elif pool_binding is not None:
                dialect_tag, engine_pool_id = pool_binding
                self._send_rollback(dialect_tag, engine_pool_id, lease.slot_index, lease.generation)
            with self._lock:
                self._rolled_back_leases += 1
        except Exception:
            with self._lock:
                self._rollback_errors += 1
            if dispose_fn is not None:
                with contextlib.suppress(Exception):
                    dispose_fn(lease)
        finally:
            with contextlib.suppress(Exception):
                self._transport.dispose_quarantined(lease.slot_index, lease.generation)
                with self._lock:
                    self._disposed_leases += 1

    @staticmethod
    def _send_rollback(
        dialect_tag: int,
        engine_pool_id: int,
        lease_slot: int,
        lease_generation: int,
    ) -> None:
        req = DbRpcRequest(
            version=DBRPC_VERSION,
            op=int(DbRpcOp.ROLLBACK),
            request_id=max(1, time.monotonic_ns() & 0x7FFF_FFFF_FFFF_FFFF),
            lease_slot=lease_slot,
            lease_generation=lease_generation,
        )
        raw_resp = BrokerEngineRegistry.execute(dialect_tag, engine_pool_id, encode_dbrpc_request(req))
        resp = decode_dbrpc_response(raw_resp)
        if resp.status != 0:
            raise BrokerAttachError(f"engine rollback returned non-zero status {resp.status}")

    def _close_registered_pools(self) -> None:
        with self._lock:
            items = list(self._pools.items())
            self._pools.clear()
        for _pool_id, (dialect_tag, engine_pool_id) in items:
            with contextlib.suppress(Exception):
                BrokerEngineRegistry.close_pool(dialect_tag, engine_pool_id)
            with self._lock:
                self._closed_pools += 1
