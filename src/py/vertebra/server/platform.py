"""Cloud and internal platform runtime adapter, cgroup v2 sizing, z-pages, and lame-duck drain."""

import array
import asyncio
import collections.abc
import concurrent.futures
import contextlib
import math
import os
import socket
import tempfile
import threading
import time
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Final, Protocol, cast

import msgspec

from vertebra.utils.module_loader import ensure_module
from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "CgroupQuota",
    "HealthProbeServer",
    "LameDuckController",
    "PlatformAdapter",
    "SHMAllocationPolicy",
    "ZPagesRegistry",
)

_HTTP_REASON_PHRASES: Final[dict[int, str]] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    503: "Service Unavailable",
}


class CgroupQuota(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Detected Linux cgroup v2 CPU and memory limits for container-aware auto-sizing."""

    cpu_quota_cores: float
    """Detected fractional CPU core quota from Linux cgroup v2 `cpu.max`."""
    effective_workers: int
    """Recommended worker process count (`max(1, ceil(cpu_quota_cores))`)."""
    memory_max_bytes: int | None
    """Maximum container memory limit in bytes from `memory.max`, or `None` when unbounded."""

    @classmethod
    def detect(
        cls,
        cpu_max_path: str | Path = "/sys/fs/cgroup/cpu.max",
        *,
        memory_max_path: str | Path = "/sys/fs/cgroup/memory.max",
        host_cpus: int | None = None,
        fallback_cpus: int | None = None,
    ) -> "CgroupQuota":
        """Read Linux cgroup v2 cpu.max and memory.max to compute container-safe worker counts."""
        resolved_cpus = host_cpus or fallback_cpus or (os.cpu_count() or 1)
        cpu_cores = float(resolved_cpus)
        cpu_file = Path(cpu_max_path)
        if cpu_file.is_file():
            try:
                raw_cpu = cpu_file.read_text(encoding="utf-8").strip().split()
                if len(raw_cpu) == 2 and raw_cpu[0] != "max":
                    quota_us = int(raw_cpu[0])
                    period_us = int(raw_cpu[1])
                    if quota_us > 0 and period_us > 0:
                        cpu_cores = quota_us / period_us
            except (OSError, ValueError):
                pass

        mem_bytes: int | None = None
        mem_file = Path(memory_max_path)
        if mem_file.is_file():
            try:
                raw_mem = mem_file.read_text(encoding="utf-8").strip()
                if raw_mem and raw_mem != "max":
                    mem_bytes = int(raw_mem)
            except (OSError, ValueError):
                pass

        workers = max(1, math.ceil(cpu_cores))
        return cls(
            cpu_quota_cores=cpu_cores,
            effective_workers=workers,
            memory_max_bytes=mem_bytes,
        )

    @classmethod
    def effective_cpu_count(
        cls,
        cpu_max_path: str | Path = "/sys/fs/cgroup/cpu.max",
    ) -> int:
        """Return container- and affinity-aware effective CPU count for thread/worker auto-sizing."""
        sched_getaffinity = getattr(os, "sched_getaffinity", None)
        if callable(sched_getaffinity):
            try:
                get_affinity = cast("collections.abc.Callable[[int], set[int]]", sched_getaffinity)
                affinity_cpus = max(1, len(get_affinity(0)))
            except OSError:
                affinity_cpus = os.cpu_count() or 1
        else:
            affinity_cpus = os.cpu_count() or 1
        quota = cls.detect(cpu_max_path=cpu_max_path, host_cpus=affinity_cpus)
        return max(1, min(affinity_cpus, quota.effective_workers))


class LameDuckController:
    """Manages graceful lame-duck health check drain before socket closure on SIGTERM."""

    def __init__(self, drain_grace_seconds: float = 10.0) -> None:
        self.drain_grace_seconds = drain_grace_seconds
        self._lame_duck = False
        self._entered_at: float | None = None
        self._in_flight: int = 0

    @property
    def is_lame_duck(self) -> bool:
        """Return whether the server has entered lame-duck drain mode."""
        return self._lame_duck

    @property
    def is_draining(self) -> bool:
        """Return True when the controller is actively draining requests."""
        return self._lame_duck

    @property
    def in_flight(self) -> int:
        """Return the number of currently active in-flight requests."""
        return self._in_flight

    @property
    def entered_at(self) -> float | None:
        """Return the Unix timestamp when lame-duck mode started."""
        return self._entered_at

    def acquire_request(self, count: int = 1) -> None:
        """Increment the count of active in-flight requests."""
        self._in_flight += count

    def release_request(self, count: int = 1) -> None:
        """Decrement the count of active in-flight requests."""
        self._in_flight = max(0, self._in_flight - count)

    def enter_lame_duck(self) -> None:
        """Transition the server into lame-duck mode so readiness checks return 503."""
        if not self._lame_duck:
            self._lame_duck = True
            self._entered_at = time.time()

    def readiness_status(self) -> tuple[int, dict[str, Any]]:
        """Return (http_status, payload) for /healthz or readiness probes."""
        if self._lame_duck:
            return 503, {"status": "lame_duck", "entered_at": self._entered_at}
        return 200, {"status": "ok"}


class SHMAllocationPolicy(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Shared-memory allocation policy for Kubernetes (64 MiB /dev/shm) and read-only rootfs."""

    prefer_memfd: bool = True
    """Use anonymous `memfd_create` file descriptors when supported by the kernel."""
    shm_dir: str = "/dev/shm"
    """Filesystem mount path inspected for free space when `memfd_create` is unavailable."""
    min_free_bytes: int = 16 * 1024 * 1024
    """Minimum free bytes required in `shm_dir` before falling back to temporary files."""

    def validate_environment(self) -> bool:
        """Validate whether the current host or container environment satisfies SHM policy requirements."""
        if self.prefer_memfd and hasattr(os, "memfd_create"):
            return True
        if os.path.isdir(self.shm_dir):
            try:
                st = os.statvfs(self.shm_dir)
                free_bytes = st.f_bavail * st.f_frsize
                return free_bytes >= self.min_free_bytes
            except OSError:
                return False
        return True

    def allocate_anonymous_shm_fd(self, name: str, size: int) -> tuple[int, str]:
        """Allocate an anonymous shared-memory file descriptor and return (fd, backend)."""
        if size <= 0:
            msg = f"size must be > 0 (got {size})"
            raise ValueError(msg)
        memfd_fn = getattr(os, "memfd_create", None)
        if self.prefer_memfd and callable(memfd_fn):
            create_memfd = cast("collections.abc.Callable[[str, int], int]", memfd_fn)
            fd = int(create_memfd(f"vertebra-{name}", os.MFD_CLOEXEC))
            backend = "memfd"
        else:
            with tempfile.TemporaryFile() as tmp:
                fd = os.dup(tmp.fileno())
            backend = "tmpfile"
        os.ftruncate(fd, size)
        return fd, backend

    @classmethod
    def allocate_fd(
        cls,
        name: str,
        size: int,
        policy: "SHMAllocationPolicy | None" = None,
    ) -> tuple[int, str]:
        """Allocate an anonymous shared-memory file descriptor using policy or defaults."""
        active_policy = policy if policy is not None else cls()
        return active_policy.allocate_anonymous_shm_fd(name, size)

    @staticmethod
    def send_fd(sock: socket.socket, fd: int, payload: bytes = b"VTB_SHM") -> None:
        """Send a file descriptor to a peer process over an AF_UNIX socket using SCM_RIGHTS."""
        fds = array.array("i", [fd])
        sock.sendmsg([payload], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, fds)])

    @staticmethod
    def recv_fd(sock: socket.socket, max_payload_bytes: int = 64) -> int:
        """Receive a file descriptor from an AF_UNIX socket via SCM_RIGHTS."""
        fds = array.array("i")
        _msg, ancdata, _flags, _addr = sock.recvmsg(
            max_payload_bytes,
            socket.CMSG_LEN(fds.itemsize),
        )
        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                fds.frombytes(cmsg_data[: len(cmsg_data) - (len(cmsg_data) % fds.itemsize)])
        if not fds:
            err = "No file descriptor received in SCM_RIGHTS ancillary data"
            raise RuntimeError(err)
        return int(fds[0])


class ZPagesRegistry:
    """Provides diagnostic payloads for /statusz, /healthz, /varz, /rpcz, /brokerz, and /taskz."""

    def __init__(
        self,
        service_name: str = "vertebra",
        lame_duck: LameDuckController | None = None,
    ) -> None:
        self.service_name = service_name
        self.started_at = time.time()
        self._started_monotonic = time.monotonic()
        self.lame_duck = lame_duck or LameDuckController()
        self._lock = threading.Lock()
        self._vars: dict[str, Any] = {}
        self._rpc_stats: dict[str, dict[str, int]] = {}
        self._broker_providers: dict[str, collections.abc.Callable[[], dict[str, object]]] = {}
        self._event_loops: dict[str, tuple[asyncio.AbstractEventLoop, int | None]] = {}
        self._loop_pools: dict[str, Any] = {}
        self._loop_recorders: dict[str, Any] = {}
        self._workers: dict[str, tuple[Any, int | None, str]] = {}

    def register_broker(
        self,
        name: str,
        provider: collections.abc.Callable[[], dict[str, object]],
    ) -> None:
        """Register a broker telemetry provider function."""
        with self._lock:
            self._broker_providers[name] = provider

    def register_event_loop(
        self,
        name: str,
        loop: asyncio.AbstractEventLoop,
        *,
        shard_id: int | None = None,
    ) -> None:
        """Register a named asyncio event loop for /taskz diagnostics."""
        with self._lock:
            self._event_loops[name] = (loop, shard_id)

    def register_loop_pool(self, name: str, pool: Any) -> None:
        """Register an EventLoopPool for /taskz diagnostics across all shards."""
        with self._lock:
            self._loop_pools[name] = pool

    def register_worker(
        self,
        name: str,
        lame_duck: Any,
        *,
        worker_id: int | None = None,
        state: str = "ready",
    ) -> None:
        """Register a worker lifecycle handle for lazy /varz active_requests and state reporting."""
        with self._lock:
            self._workers[name] = (lame_duck, worker_id, state)

    def set_worker_state(self, name: str, state: str) -> None:
        """Update a registered worker's lifecycle state without hot-path locking."""
        with self._lock:
            existing = self._workers.get(name)
            if existing is not None:
                lame_duck, worker_id, _prev_state = existing
                self._workers[name] = (lame_duck, worker_id, state)

    def register_loop_recorder(self, name: str, recorder: Any) -> None:
        """Register an EventLoopRecorder so /varz exports its latest lag and active-task snapshots."""
        with self._lock:
            self._loop_recorders[name] = recorder

    @staticmethod
    def discover_adapters(
        group: str = "vertebra.platform_adapters",
    ) -> "list[PlatformAdapter]":
        """Load installed PlatformAdapter plugins registered under group."""
        adapters: list[PlatformAdapter] = []
        for ep in entry_points(group=group):
            loaded = ep.load()
            instance = (
                cast("collections.abc.Callable[[], PlatformAdapter]", loaded)()
                if callable(loaded)
                else cast("PlatformAdapter", loaded)
            )
            adapters.append(instance)
        return adapters

    def set_var(self, name: str, value: Any) -> None:
        """Export a runtime variable in /varz."""
        with self._lock:
            self._vars[name] = value

    def record_rpc(self, method: str, *, ok: bool = True) -> None:
        """Record an RPC or route invocation for /rpcz."""
        with self._lock:
            stats = self._rpc_stats.setdefault(method, {"count": 0, "errors": 0})
            stats["count"] += 1
            if not ok:
                stats["errors"] += 1

    def get_vars_payload(self) -> dict[str, Any]:
        """Build the /varz payload, adding lazy worker stats and loop_telemetry when registered."""
        with self._lock:
            payload: dict[str, Any] = dict(self._vars)
            workers = list(self._workers.items())
            recorders = list(self._loop_recorders.items())

        if workers:
            if len(workers) == 1:
                _name, (ld, wid, st) = workers[0]
                payload["active_requests"] = int(getattr(ld, "in_flight", 0))
                payload["worker_state"] = st
                if wid is not None:
                    payload["worker_id"] = wid
            else:
                sorted_workers = sorted(workers, key=lambda item: item[0])
                worker_details: dict[str, dict[str, Any]] = {}
                total_active = 0
                states: set[str] = set()
                for name, (ld, wid, st) in sorted_workers:
                    in_flight = int(getattr(ld, "in_flight", 0))
                    total_active += in_flight
                    states.add(st)
                    worker_details[name] = {
                        "worker_id": wid,
                        "state": st,
                        "active_requests": in_flight,
                    }
                payload["active_requests"] = total_active
                if "ready" in states:
                    payload["worker_state"] = "ready"
                elif "draining" in states:
                    payload["worker_state"] = "draining"
                else:
                    payload["worker_state"] = "stopped"
                payload["workers"] = worker_details

        if recorders:
            payload["loop_telemetry"] = {
                name: self._serialize_loop_recorder(rec) for name, rec in sorted(recorders, key=lambda item: item[0])
            }

        return payload

    def has_remote_loops(self, caller_loop: asyncio.AbstractEventLoop | None = None) -> bool:
        """Return True when any registered active loop runs on a loop other than caller_loop."""
        active_caller = caller_loop if caller_loop is not None else self._running_loop_or_none()
        for _label, loop, _shard_id, _allow_idle_direct in self._collect_loop_targets(active_caller):
            if not loop.is_closed() and loop is not active_caller and loop.is_running():
                return True
        return False

    def get_tasks_payload(self) -> dict[str, Any]:
        """Collect synchronous /taskz diagnostic snapshots across active registered event loops."""
        loops_mod = ensure_module("vertebra.server._loops")
        capture_local = loops_mod.capture_loop_task_snapshots
        capture_remote = loops_mod._capture_loop_task_snapshots_remote
        probe_name: str = loops_mod._TASKZ_PROBE_TASK_NAME
        caller_loop = self._running_loop_or_none()
        entries = self._collect_loop_targets(caller_loop)

        per_loop: dict[str, list[dict[str, Any]]] = {}
        all_tasks: list[dict[str, Any]] = []
        for label, loop, shard_id, allow_idle_direct in entries:
            if loop.is_closed():
                continue
            if loop is caller_loop or (allow_idle_direct and not loop.is_running()):
                raw_snaps = [snap for snap in capture_local(loop, shard_id=shard_id) if snap.task_name != probe_name]
            elif loop.is_running():
                coro = capture_remote(loop, shard_id=shard_id)
                try:
                    fut = asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError:
                    coro.close()
                    continue
                try:
                    raw_snaps = fut.result(timeout=1.0)
                except (
                    TimeoutError,
                    concurrent.futures.TimeoutError,
                    concurrent.futures.CancelledError,
                    RuntimeError,
                ):
                    fut.cancel()
                    continue
            else:
                continue

            serialized = [self._snapshot_to_dict(snap) for snap in raw_snaps]
            per_loop[label] = serialized
            all_tasks.extend(serialized)

        return {
            "status": "ok",
            "service": self.service_name,
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "loops": per_loop,
        }

    async def get_tasks_payload_async(self) -> dict[str, Any]:
        """Collect non-blocking async /taskz snapshots across inline and background shard loops."""
        loops_mod = ensure_module("vertebra.server._loops")
        capture_local = loops_mod.capture_loop_task_snapshots
        capture_remote = loops_mod._capture_loop_task_snapshots_remote
        probe_name: str = loops_mod._TASKZ_PROBE_TASK_NAME
        caller_loop = self._running_loop_or_none()
        entries = self._collect_loop_targets(caller_loop)

        ordered_labels: list[str] = []
        local_results: dict[str, list[Any]] = {}
        remote_jobs: list[tuple[str, concurrent.futures.Future[Any]]] = []

        for label, loop, shard_id, allow_idle_direct in entries:
            if loop.is_closed():
                continue
            if loop is caller_loop or (allow_idle_direct and not loop.is_running()):
                ordered_labels.append(label)
                local_results[label] = [
                    snap for snap in capture_local(loop, shard_id=shard_id) if snap.task_name != probe_name
                ]
            elif loop.is_running():
                coro = capture_remote(loop, shard_id=shard_id)
                try:
                    fut = asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError:
                    coro.close()
                    continue
                ordered_labels.append(label)
                remote_jobs.append((label, fut))

        if remote_jobs:
            awaited = await asyncio.gather(
                *(asyncio.wait_for(asyncio.wrap_future(fut), timeout=1.0) for _, fut in remote_jobs),
                return_exceptions=True,
            )
            for (label, fut), outcome in zip(remote_jobs, awaited, strict=True):
                if isinstance(outcome, BaseException):
                    fut.cancel()
                    continue
                local_results[label] = outcome

        per_loop: dict[str, list[dict[str, Any]]] = {}
        all_tasks: list[dict[str, Any]] = []
        for label in ordered_labels:
            if label not in local_results:
                continue
            serialized = [self._snapshot_to_dict(snap) for snap in local_results[label]]
            per_loop[label] = serialized
            all_tasks.extend(serialized)

        return {
            "status": "ok",
            "service": self.service_name,
            "total_tasks": len(all_tasks),
            "tasks": all_tasks,
            "loops": per_loop,
        }

    def handle_zpage(self, path: str) -> tuple[int, bytes]:
        """Render a JSON diagnostic z-page for path."""
        if path == "/healthz":
            code, body = self.lame_duck.readiness_status()
            return code, encode_json_bytes(body)
        if path == "/statusz":
            uptime = max(0.0, time.monotonic() - self._started_monotonic)
            return 200, encode_json_bytes(
                {
                    "service": self.service_name,
                    "uptime_seconds": uptime,
                    "lame_duck": self.lame_duck.is_lame_duck,
                }
            )
        if path == "/varz":
            return 200, encode_json_bytes(self.get_vars_payload())
        if path == "/rpcz":
            with self._lock:
                rpc_snapshot = {method: dict(stats) for method, stats in self._rpc_stats.items()}
            return 200, encode_json_bytes(rpc_snapshot)
        if path in ("/brokerz", "/debug/zpages/broker"):
            with self._lock:
                providers = list(self._broker_providers.items())
            return 200, encode_json_bytes(
                {
                    "status": "ok",
                    "brokers": {name: provider() for name, provider in providers},
                }
            )
        if path in ("/taskz", "/debug/zpages/tasks"):
            return 200, encode_json_bytes(self.get_tasks_payload())
        return 404, encode_json_bytes({"error": "not_found"})

    def _collect_loop_targets(
        self,
        caller_loop: asyncio.AbstractEventLoop | None,
    ) -> list[tuple[str, asyncio.AbstractEventLoop, int | None, bool]]:
        with self._lock:
            event_loops = list(self._event_loops.items())
            loop_pools = list(self._loop_pools.items())

        targets: list[tuple[str, asyncio.AbstractEventLoop, int | None, bool]] = []
        for name, (loop, shard_id) in event_loops:
            if loop.is_closed():
                continue
            targets.append((name, loop, shard_id, False))

        for pool_name, pool in loop_pools:
            if getattr(pool, "_stopped", False):
                continue
            inline_single = bool(getattr(pool, "_inline_single", False))
            pool_loops = getattr(pool, "loops", ())
            for idx, loop in enumerate(pool_loops):
                if loop.is_closed():
                    continue
                targets.append((f"{pool_name}.shard.{idx}", loop, idx, inline_single))

        if not event_loops and not loop_pools and caller_loop is not None and not caller_loop.is_closed():
            targets.append(("default", caller_loop, None, False))

        return targets

    @staticmethod
    def _serialize_loop_recorder(recorder: Any) -> dict[str, Any]:
        """Serialize an EventLoopRecorder's per-shard latest samples and lag histogram for /varz."""
        latest = recorder.latest_by_shard()
        hist = recorder.lag_histogram()
        return {
            "shards": {
                str(shard_id): {
                    "shard_id": sample.shard_id,
                    "backend": sample.backend,
                    "lag_ms": sample.lag_ms,
                    "active_tasks": sample.active_tasks,
                    "pending_callbacks": sample.pending_callbacks,
                    "unhandled_exceptions": sample.unhandled_exceptions,
                    "recorded_at_ns": sample.recorded_at_ns,
                }
                for shard_id, sample in sorted(latest.items())
            },
            "lag_histogram": {
                "count": hist.count,
                "min_ms": hist.min,
                "max_ms": hist.max,
                "sum_ms": hist.sum,
                "p50_ms": hist.p50,
                "p90_ms": hist.p90,
                "p99_ms": hist.p99,
            },
        }

    @staticmethod
    def _running_loop_or_none() -> asyncio.AbstractEventLoop | None:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return None

    @staticmethod
    def _snapshot_to_dict(snap: Any) -> dict[str, Any]:
        return {
            "task_name": snap.task_name,
            "coro_qualname": snap.coro_qualname,
            "done": snap.done,
            "cancelled": snap.cancelled,
            "shard_id": snap.shard_id,
            "call_graph": snap.call_graph,
        }


class HealthProbeServer:
    """Lightweight asyncio HTTP/1.1 health probe server backed by ``ZPagesRegistry``."""

    __slots__ = ("_bound_port", "_host", "_port", "_server", "_zpages")

    def __init__(
        self,
        zpages: ZPagesRegistry,
        *,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> None:
        self._zpages = zpages
        self._host = host
        self._port = port
        self._bound_port = port
        self._server: asyncio.Server | None = None

    @property
    def bound_port(self) -> int:
        """Return the TCP port the probe server is bound to."""
        return self._bound_port

    async def start(self) -> None:
        """Start listening for incoming health probe HTTP connections."""
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_connection,
            self._host,
            self._port,
        )
        if self._server.sockets:
            sockname = self._server.sockets[0].getsockname()
            if isinstance(sockname, tuple) and len(sockname) >= 2:
                self._bound_port = int(sockname[1])

    async def stop(self) -> None:
        """Close the probe server and wait for active connections to finish."""
        server = self._server
        if server is None:
            return
        self._server = None
        server.close()
        await server.wait_closed()

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            raw_head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5.0)
        except (TimeoutError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, OSError):
            status = 400
            payload = encode_json_bytes({"error": "bad_request"})
            await self._write_response(writer, status, payload)
            return

        request_line = raw_head.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
        parts = request_line.split(" ", 2)
        if len(parts) < 2:
            await self._write_response(writer, 400, encode_json_bytes({"error": "bad_request"}))
            return

        method = parts[0].upper()
        raw_target = parts[1]
        path = raw_target.split("?", 1)[0]

        if method != "GET":
            await self._write_response(
                writer,
                405,
                encode_json_bytes({"error": "method_not_allowed"}),
                extra_headers=(("Allow", "GET"),),
            )
            return

        if path in {"/healthz", "/readyz"}:
            status, payload = self._zpages.handle_zpage("/healthz")
        elif path in {"/statusz", "/varz"}:
            status, payload = self._zpages.handle_zpage(path)
        elif path in {"/taskz", "/debug/zpages/tasks"}:
            status, payload = 200, encode_json_bytes(await self._zpages.get_tasks_payload_async())
        else:
            status, payload = 404, encode_json_bytes({"error": "not_found"})

        await self._write_response(writer, status, payload)

    @staticmethod
    async def _write_response(
        writer: asyncio.StreamWriter,
        status: int,
        payload: bytes,
        *,
        extra_headers: tuple[tuple[str, str], ...] = (),
    ) -> None:
        reason = _HTTP_REASON_PHRASES.get(status, "OK")
        header_lines = [
            f"HTTP/1.1 {status} {reason}",
            "Content-Type: application/json",
            f"Content-Length: {len(payload)}",
            *(f"{name}: {value}" for name, value in extra_headers),
            "Connection: close",
            "",
            "",
        ]
        try:
            writer.write("\r\n".join(header_lines).encode("ascii") + payload)
            await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()


class PlatformAdapter(Protocol):
    """Entry-point protocol (vertebra.platform_adapters) for cloud or internal platform plugins."""

    name: str

    def configure_server(self, zpages: ZPagesRegistry, quota: CgroupQuota) -> None:
        """Configure platform-specific z-pages, telemetry exporters, or credentials."""
        ...
