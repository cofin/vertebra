"""Server environment assembly for CLI run and dev commands."""

import os
import sys
import sysconfig
import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import msgspec

from vertebra.server.platform import CgroupQuota

__all__ = (
    "ServerLaunchEnv",
    "is_free_threaded_runtime",
    "load_workers_pyproject_config",
    "parse_lane_concurrency_map",
)


def is_free_threaded_runtime() -> bool:
    """Return True when the active interpreter runs with Py_GIL_DISABLED."""
    gil_disabled = sysconfig.get_config_var("Py_GIL_DISABLED")
    if gil_disabled not in (None, 0, "0", ""):
        return True
    gil_check = getattr(sys, "_is_gil_enabled", None)
    if callable(gil_check):
        return not bool(gil_check())
    return False


def parse_lane_concurrency_map(raw: str | None) -> dict[str, int]:
    """Parse a comma-separated lane=concurrency specification into a dictionary."""
    if not raw:
        return {}
    result: dict[str, int] = {}
    for entry in raw.split(","):
        token = entry.strip()
        if not token or "=" not in token:
            continue
        lane_name, raw_limit = token.split("=", 1)
        lane_key = lane_name.strip()
        if not lane_key:
            continue
        try:
            result[lane_key] = max(1, int(raw_limit.strip()))
        except ValueError:
            continue
    return result


def load_workers_pyproject_config(
    project_dir: Path | None = None,
) -> tuple[bool, tuple[str, ...], dict[str, int]]:
    """Read [tool.vertebra.workers] settings from pyproject.toml when present."""
    root = project_dir if project_dir is not None else Path.cwd()
    pyproject_path = root / "pyproject.toml"
    if not pyproject_path.is_file():
        return False, ("default",), {}
    try:
        data: dict[str, Any] = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return False, ("default",), {}
    tool_cfg = data.get("tool")
    if not isinstance(tool_cfg, dict):
        return False, ("default",), {}
    vtb_cfg = tool_cfg.get("vertebra")
    if not isinstance(vtb_cfg, dict):
        return False, ("default",), {}
    workers_cfg = vtb_cfg.get("workers")
    if not isinstance(workers_cfg, dict):
        return False, ("default",), {}

    enabled = bool(workers_cfg.get("enabled_in_run", False))
    raw_queues = workers_cfg.get("queues")
    queues: tuple[str, ...] = ("default",)
    if isinstance(raw_queues, list | tuple):
        parsed_q = tuple(str(q).strip() for q in raw_queues if str(q).strip())
        if parsed_q:
            queues = parsed_q
    elif isinstance(raw_queues, str) and raw_queues.strip():
        parsed_q = tuple(q.strip() for q in raw_queues.split(",") if q.strip())
        if parsed_q:
            queues = parsed_q

    raw_lanes = workers_cfg.get("lanes")
    lane_map: dict[str, int] = {}
    if isinstance(raw_lanes, dict):
        for k, v in raw_lanes.items():
            key = str(k).strip()
            if not key:
                continue
            try:
                lane_map[key] = max(1, int(v))
            except (TypeError, ValueError):
                continue
    return enabled, queues, lane_map


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
    with_workers: bool = False
    worker_queues: tuple[str, ...] = ("default",)
    worker_lane_concurrency: dict[str, int] = msgspec.field(default_factory=dict)

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

        raw_topology = (self.topology or env.get("VERTEBRA_TOPOLOGY") or "auto").strip().lower()
        effective_workers = self.workers
        if effective_workers is None:
            raw_workers = env.get("VERTEBRA_WORKERS", "").strip()
            if raw_workers.lstrip("-").isdigit():
                effective_workers = int(raw_workers)

        if raw_topology in {"", "auto"}:
            if (
                effective_workers is not None
                and effective_workers > 1
                and os.name == "posix"
                and not is_free_threaded_runtime()
            ):
                resolved_topology = "shm"
            else:
                resolved_topology = "embedded"
        else:
            resolved_topology = raw_topology

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

        if self.with_workers:
            env["VERTEBRA_WITH_WORKERS"] = "1"
            queues_csv = ",".join(q for q in self.worker_queues if q) or "default"
            env["VERTEBRA_WORKER_QUEUES"] = queues_csv
            if self.worker_lane_concurrency:
                env["VERTEBRA_WORKER_LANE_CONCURRENCY"] = ",".join(
                    f"{k}={v}" for k, v in self.worker_lane_concurrency.items()
                )

        return env
