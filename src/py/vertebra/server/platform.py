"""Cloud and internal platform runtime adapter, cgroup v2 sizing, z-pages, and lame-duck drain."""

import math
import os
import time
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Protocol

import msgspec

from vertebra.utils.serializers import encode_json_bytes

__all__ = (
    "CgroupQuota",
    "LameDuckController",
    "PlatformAdapter",
    "ZPagesRegistry",
)


class CgroupQuota(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Detected Linux cgroup v2 CPU and memory limits for container-aware auto-sizing."""

    cpu_quota_cores: float
    effective_workers: int
    memory_max_bytes: int | None

    @classmethod
    def detect(
        cls,
        *,
        cpu_max_path: str | Path = "/sys/fs/cgroup/cpu.max",
        memory_max_path: str | Path = "/sys/fs/cgroup/memory.max",
        host_cpus: int | None = None,
        fallback_cpus: int | None = None,
    ) -> "CgroupQuota":
        """Read Linux cgroup v2 ``cpu.max`` and ``memory.max`` to compute container-safe worker counts."""
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


class LameDuckController:
    """Manages graceful lame-duck health check drain before socket closure on SIGTERM."""

    def __init__(self, drain_grace_seconds: float = 10.0) -> None:
        self.drain_grace_seconds = drain_grace_seconds
        self._lame_duck = False
        self._entered_at: float | None = None

    @property
    def is_lame_duck(self) -> bool:
        """Return whether the server has entered lame-duck drain mode."""
        return self._lame_duck

    @property
    def entered_at(self) -> float | None:
        """Return the Unix timestamp when lame-duck mode started."""
        return self._entered_at

    def enter_lame_duck(self) -> None:
        """Transition the server into lame-duck mode so readiness checks return 503."""
        if not self._lame_duck:
            self._lame_duck = True
            self._entered_at = time.time()

    def readiness_status(self) -> tuple[int, dict[str, Any]]:
        """Return ``(http_status, payload)`` for ``/healthz`` or readiness probes."""
        if self._lame_duck:
            return 503, {"status": "lame_duck", "entered_at": self._entered_at}
        return 200, {"status": "ok"}


class ZPagesRegistry:
    """Provides diagnostic payloads for ``/statusz``, ``/healthz``, ``/varz``, and ``/rpcz``."""

    def __init__(
        self,
        service_name: str = "vertebra",
        lame_duck: LameDuckController | None = None,
    ) -> None:
        self.service_name = service_name
        self.started_at = time.time()
        self.lame_duck = lame_duck or LameDuckController()
        self._vars: dict[str, Any] = {}
        self._rpc_stats: dict[str, dict[str, int]] = {}

    @staticmethod
    def discover_adapters(
        group: str = "vertebra.platform_adapters",
    ) -> "list[PlatformAdapter]":
        """Load installed ``PlatformAdapter`` plugins registered under ``group``."""
        adapters: list[PlatformAdapter] = []
        for ep in entry_points(group=group):
            loaded = ep.load()
            instance = loaded() if callable(loaded) else loaded
            adapters.append(instance)
        return adapters

    def set_var(self, name: str, value: Any) -> None:
        """Export a runtime variable in ``/varz``."""
        self._vars[name] = value

    def record_rpc(self, method: str, *, ok: bool = True) -> None:
        """Record an RPC or route invocation for ``/rpcz``."""
        stats = self._rpc_stats.setdefault(method, {"count": 0, "errors": 0})
        stats["count"] += 1
        if not ok:
            stats["errors"] += 1

    def handle_zpage(self, path: str) -> tuple[int, bytes]:
        """Render a JSON diagnostic z-page for ``path``."""
        if path == "/healthz":
            code, body = self.lame_duck.readiness_status()
            return code, encode_json_bytes(body)
        if path == "/statusz":
            return 200, encode_json_bytes(
                {
                    "service": self.service_name,
                    "uptime_seconds": max(0.0, time.time() - self.started_at),
                    "lame_duck": self.lame_duck.is_lame_duck,
                }
            )
        if path == "/varz":
            return 200, encode_json_bytes(self._vars)
        if path == "/rpcz":
            return 200, encode_json_bytes(self._rpc_stats)
        return 404, encode_json_bytes({"error": "not_found"})


class PlatformAdapter(Protocol):
    """Entry-point protocol (`vertebra.platform_adapters`) for cloud or internal platform plugins."""

    name: str

    def configure_server(self, zpages: ZPagesRegistry, quota: CgroupQuota) -> None:
        """Configure platform-specific z-pages, telemetry exporters, or credentials."""
        ...
