"""Server environment assembly for CLI run and dev commands."""

import os
import sys
import sysconfig
from collections.abc import Mapping

import msgspec

from vertebra.server.platform import CgroupQuota

__all__ = ("ServerLaunchEnv",)


class ServerLaunchEnv(msgspec.Struct, frozen=True, gc=False):
    """Server environment configuration for running or developing Vertebra applications."""

    app_path: str
    host: str
    port: int
    workers: int | None = None
    log_level: str = "info"
    dev: bool = False
    native: bool = True
    topology: str | None = None
    threads: int | None = None
    jit: bool | None = None

    def to_environ(self, base: Mapping[str, str] | None = None) -> dict[str, str]:
        """Assemble environment variables dictionary while preserving omitted runtime overrides."""
        env = dict(base) if base is not None else os.environ.copy()
        env["VERTEBRA_SERVER_ACTIVE"] = "1"
        env["VERTEBRA_APP"] = self.app_path
        env["LITESTAR_APP"] = self.app_path
        env["VERTEBRA_PYTHON_APP"] = self.app_path
        env["VERTEBRA_PYTHON_BIN"] = sys.executable

        cwd = os.getcwd()
        path_entries: list[str] = [cwd]
        for key in ("purelib", "platlib"):
            candidate = sysconfig.get_path(key)
            if candidate and candidate not in path_entries:
                path_entries.append(candidate)
        existing = env.get("PYTHONPATH", "")
        if existing:
            for item in existing.split(os.pathsep):
                if item and item not in path_entries:
                    path_entries.append(item)
        env["PYTHONPATH"] = os.pathsep.join(path_entries)

        env["VERTEBRA_HOST"] = self.host
        env["VERTEBRA_PORT"] = str(self.port)
        env["VERTEBRA_LOG_LEVEL"] = self.log_level
        env["VERTEBRA_NATIVE"] = "1" if self.native else "0"

        resolved_topology = (self.topology or env.get("VERTEBRA_TOPOLOGY") or "embedded").lower()
        env["VERTEBRA_TOPOLOGY"] = resolved_topology

        if self.workers is not None:
            env["VERTEBRA_WORKERS"] = str(self.workers)
        elif "VERTEBRA_WORKERS" not in env or not env["VERTEBRA_WORKERS"].strip():
            env["VERTEBRA_WORKERS"] = "1" if resolved_topology == "shm" else "0"

        if self.threads is not None:
            env["VERTEBRA_THREADS"] = str(self.threads)
        elif "VERTEBRA_THREADS" not in env or not env["VERTEBRA_THREADS"].strip():
            env["VERTEBRA_THREADS"] = str(CgroupQuota.effective_cpu_count())

        if self.jit is True:
            env["PYTHON_JIT"] = "1"
        elif self.jit is False:
            env["PYTHON_JIT"] = "0"

        if self.dev:
            env["VERTEBRA_DEV"] = "1"

        return env
