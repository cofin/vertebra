from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from types import TracebackType


@dataclass(slots=True)
class BenchmarkResult:
    """Container for a single benchmark measurement."""

    target: str
    scenario: str
    rps: float
    latency_mean_s: float
    latency_p50_s: float
    latency_p99_s: float
    latency_max_s: float
    total_requests: int
    errors: int
    duration: int
    concurrency: int
    url: str
    timestamp: str


class OhaRunner:
    """Drives the `oha` HTTP load-testing tool and parses JSON output."""

    def __init__(
        self, url: str, duration: int = 10, concurrency: int = 64, method: str = "GET", body: str | None = None
    ) -> None:
        self.url = url
        self.duration = duration
        self.concurrency = concurrency
        self.method = method
        self.body = body

    def run(self) -> dict[str, Any]:
        """Execute oha and return parsed JSON results.

        Returns:
            Parsed result dict with normalized keys.

        Raises:
            subprocess.CalledProcessError: If oha exits non-zero.
            json.JSONDecodeError: If oha output is not valid JSON.
        """
        cmd = [
            "oha",
            "-z",
            f"{self.duration}s",
            "-c",
            str(self.concurrency),
            "--output-format",
            "json",
            "--no-tui",
            "--latency-correction",
            "-m",
            self.method,
        ]
        if self.body is not None:
            cmd.extend(["-d", self.body])
            cmd.extend(["-T", "application/octet-stream"])
        cmd.append(self.url)

        result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=self._build_env())
        raw = json.loads(result.stdout)
        return self._parse(raw)

    def _build_env(self) -> dict[str, str]:
        """Build a stable child environment for oha."""
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        return env

    def _parse(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Parse oha 1.12.1 JSON output into a flat result dict."""
        summary = raw.get("summary", {})
        latency_pcts = raw.get("latencyPercentiles", {})
        rps_data = raw.get("rps", {})
        status_dist = raw.get("statusCodeDistribution", {})
        error_dist = raw.get("errorDistribution", {})
        success_rate = float(summary.get("successRate") or 0.0)

        def _f(v: Any) -> float:
            return float(v) if v is not None else 0.0

        def _i(v: Any) -> int:
            return int(v) if v is not None else 0

        http_errors = sum(_i(count) for code, count in status_dist.items() if not str(code).startswith("2"))
        transport_errors = sum(
            _i(count) for kind, count in error_dist.items() if kind != "aborted due to deadline" or success_rate < 1.0
        )
        total_requests = sum(_i(count) for count in status_dist.values()) + transport_errors

        # Compute RPS from total successful requests / elapsed time.
        # Both rps.mean (instantaneous sample average) and summary.requestsPerSec
        # can be inaccurate for bursty batch-first workloads.
        elapsed = _f(summary.get("total"))
        successful = sum(_i(count) for code, count in status_dist.items() if str(code).startswith("2"))
        rps = successful / elapsed if elapsed > 0 and successful > 0 else _f(rps_data.get("mean"))

        return {
            "rps": rps,
            "latency_mean_s": _f(summary.get("average")),
            "latency_p50_s": _f(latency_pcts.get("p50")),
            "latency_p99_s": _f(latency_pcts.get("p99")),
            "latency_max_s": _f(summary.get("slowest")),
            "total_requests": total_requests,
            "errors": http_errors + transport_errors,
        }


class ServerRunner:
    """Manages server lifecycle as a context manager."""

    def __init__(
        self, cmd: list[str], host: str = "127.0.0.1", port: int = 8000, env: dict[str, str] | None = None
    ) -> None:
        self.cmd = cmd
        self.host = host
        self.port = port
        self.env = env or {}
        self._process: subprocess.Popen[str] | None = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        self.stop()

    def start(self) -> None:
        """Start the server subprocess in a new process group."""
        merged_env = {**os.environ, **self.env}
        self._process = subprocess.Popen(
            self.cmd,
            env=merged_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            process_group=0,
        )
        self.wait_for_port()

    def wait_for_port(self, timeout: float = 15.0) -> None:
        """Poll until the server port is accepting connections.

        Raises:
            TimeoutError: If the port does not open within the timeout.
            RuntimeError: If the server process exits before the port opens.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                stderr = ""
                if self._process.stderr:
                    stderr = self._process.stderr.read().decode(errors="replace").strip()
                    stderr = "\n".join(stderr.splitlines()[-20:])  # last 20 lines
                msg = f"Server process exited with code {self._process.returncode}"
                if stderr:
                    msg += f"\n{stderr}"
                raise RuntimeError(msg)
            try:
                with socket.create_connection((self.host, self.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.1)
        msg = f"Server did not start on {self.host}:{self.port} within {timeout}s"
        raise TimeoutError(msg)

    def stop(self) -> None:
        """Stop the server process group with SIGTERM, falling back to SIGKILL."""
        if self._process is None:
            return
        try:
            pgid = os.getpgid(self._process.pid)
            os.killpg(pgid, signal.SIGTERM)
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(pgid, signal.SIGKILL)
                self._process.wait(timeout=3)
        except ProcessLookupError:
            pass
        finally:
            self._process = None


def expand_scenario_runs(scenario_concurrency_levels: list[int] | None, default_concurrency: int) -> list[int]:
    """Return the list of concurrency levels to run for a scenario.

    If the scenario defines custom concurrency levels, use those.
    Otherwise, return a single-element list with the default concurrency.
    """
    if scenario_concurrency_levels:
        return list(scenario_concurrency_levels)
    return [default_concurrency]


def build_url(host: str, port: int, path: str) -> str:
    """Build an HTTP URL from host, port, and path."""
    return f"http://{host}:{port}{path}"
