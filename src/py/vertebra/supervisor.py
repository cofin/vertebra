"""Process supervisor for Bun server + shared-memory broker."""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(slots=True)
class ProcessSpec:
    name: str
    cmd: list[str]
    env: dict[str, str]


class Supervisor:
    def __init__(self, processes: Iterable[ProcessSpec]) -> None:
        self._processes = list(processes)
        self._running: dict[str, subprocess.Popen[str]] = {}

    def start(self) -> None:
        for spec in self._processes:
            proc = subprocess.Popen(spec.cmd, env=spec.env)
            self._running[spec.name] = proc

    def stop(self, sig: int = signal.SIGTERM) -> None:
        for proc in self._running.values():
            with contextlib.suppress(Exception):
                proc.send_signal(sig)
        for proc in self._running.values():
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)
        for proc in self._running.values():
            if proc.poll() is None:
                with contextlib.suppress(Exception):
                    proc.kill()

    def wait(self, primary: str) -> int:
        proc = self._running.get(primary)
        if not proc:
            return 1
        return proc.wait()


def run_supervisor(
    *,
    bun_cmd: list[str],
    broker_cmd: list[str],
    broker_config: str,
    env: dict[str, str],
) -> int:
    processes = [
        ProcessSpec(name="broker", cmd=broker_cmd, env=env),
        ProcessSpec(name="bun", cmd=bun_cmd, env=env),
    ]
    supervisor = Supervisor(processes)

    def _forward(sig: int, _frame: object | None = None) -> None:
        supervisor.stop(sig=sig)

    signal.signal(signal.SIGTERM, _forward)
    signal.signal(signal.SIGINT, _forward)

    supervisor.start()
    exit_code = supervisor.wait(primary="bun")
    supervisor.stop()
    return exit_code


def find_repo_root(start: Path) -> Path | None:
    for parent in [start, *start.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return None


def find_broker_entrypoint() -> str:
    package_root = Path(__file__).resolve().parent
    repo_root = find_repo_root(package_root)
    if repo_root:
        entry = repo_root / "src" / "ts" / "src" / "broker" / "serve.ts"
        if entry.exists():
            return str(entry)
    raise SystemExit("Unable to locate broker entrypoint for Bun broker")


def shutil_which(name: str) -> str | None:
    path = os.getenv("PATH")
    if not path:
        return None
    for entry in path.split(os.pathsep):
        candidate = Path(entry) / name
        if candidate.exists():
            return str(candidate)
    return None


def build_broker_command(config_path: str) -> list[str]:
    bun = shutil_which("bun")
    if not bun:
        raise SystemExit("bun binary not found; install Bun or add it to PATH")
    entry = os.getenv("VERTEBRA_BROKER_ENTRY") or find_broker_entrypoint()
    return [bun, entry, "--config", config_path]


def build_bun_command(engine: str, bun: str | None = None) -> list[str]:
    bun_bin = bun or shutil_which("bun")
    if not bun_bin:
        raise SystemExit("bun binary not found; install Bun or add it to PATH")
    return [bun_bin, engine]
