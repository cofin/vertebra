"""Cloud and internal platform runtime adapter, cgroup v2 sizing, z-pages, and lame-duck drain."""

import array
import asyncio
import collections.abc
import contextlib
import math
import os
import socket
import tempfile
import time
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Final, Protocol

import msgspec

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
                affinity_cpus = max(1, len(sched_getaffinity(0)))
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
            fd = int(memfd_fn(f"vertebra-{name}", os.MFD_CLOEXEC))
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
    """Provides diagnostic payloads for /statusz, /healthz, /varz, /rpcz, and /brokerz."""

    def __init__(
        self,
        service_name: str = "vertebra",
        lame_duck: LameDuckController | None = None,
    ) -> None:
        self.service_name = service_name
        self.started_at = time.time()
        self._started_monotonic = time.monotonic()
        self.lame_duck = lame_duck or LameDuckController()
        self._vars: dict[str, Any] = {}
        self._rpc_stats: dict[str, dict[str, int]] = {}
        self._broker_providers: dict[str, collections.abc.Callable[[], dict[str, object]]] = {}

    def register_broker(
        self,
        name: str,
        provider: collections.abc.Callable[[], dict[str, object]],
    ) -> None:
        """Register a broker telemetry provider function."""
        self._broker_providers[name] = provider

    @staticmethod
    def discover_adapters(
        group: str = "vertebra.platform_adapters",
    ) -> "list[PlatformAdapter]":
        """Load installed PlatformAdapter plugins registered under group."""
        adapters: list[PlatformAdapter] = []
        for ep in entry_points(group=group):
            loaded = ep.load()
            instance = loaded() if callable(loaded) else loaded
            adapters.append(instance)
        return adapters

    def set_var(self, name: str, value: Any) -> None:
        """Export a runtime variable in /varz."""
        self._vars[name] = value

    def record_rpc(self, method: str, *, ok: bool = True) -> None:
        """Record an RPC or route invocation for /rpcz."""
        stats = self._rpc_stats.setdefault(method, {"count": 0, "errors": 0})
        stats["count"] += 1
        if not ok:
            stats["errors"] += 1

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
            return 200, encode_json_bytes(self._vars)
        if path == "/rpcz":
            return 200, encode_json_bytes(self._rpc_stats)
        if path in ("/brokerz", "/debug/zpages/broker"):
            return 200, encode_json_bytes(
                {
                    "status": "ok",
                    "brokers": {name: provider() for name, provider in self._broker_providers.items()},
                }
            )
        return 404, encode_json_bytes({"error": "not_found"})


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
