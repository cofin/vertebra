from __future__ import annotations

import contextlib
import logging
import os
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, Self, runtime_checkable

import msgspec

from tools.benchmarks.drivers import LoadConfig, OhaDriver

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import TracebackType

logger = logging.getLogger(__name__)


class BenchmarkResult(msgspec.Struct, frozen=True, gc=False):
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


@runtime_checkable
class ServerLifecycleHook(Protocol):
    """Lifecycle hook invoked around ServerRunner subprocess startup and shutdown."""

    def pre_start(self) -> dict[str, str]:
        """Prepare external resources and return environment variables for the server."""
        ...

    def post_stop(self) -> None:
        """Tear down resources acquired during pre_start."""
        ...


class BrokerHookConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration for supervising a BrokerHost alongside a benchmark server."""

    shm_name: str
    pool_size: int = 40
    max_overflow: int = 0
    lease_ttl_ms: int = 30_000
    socket_path: str | None = None
    attach_secret: str | None = None
    shm_dir: str = "/dev/shm"


class BrokerHostHook:
    """ServerLifecycleHook supervising a BrokerHost and cleaning SHM and UDS paths."""

    def __init__(
        self,
        config: BrokerHookConfig,
        *,
        host_factory: Callable[[BrokerHookConfig], Any] | None = None,
    ) -> None:
        self.config = config
        self._host_factory = host_factory
        self._host: Any | None = None

    def pre_start(self) -> dict[str, str]:
        """Start the BrokerHost and return broker environment variables for the server.

        Returns:
            Environment variables describing the broker SHM segment, pool size,
            and optional UDS socket and attach secret.

        Raises:
            ValueError: If `socket_path` is set without a non-empty `attach_secret`.
        """
        if self.config.socket_path is not None and not self.config.attach_secret:
            msg = "attach_secret must be non-empty when socket_path is configured"
            raise ValueError(msg)

        if self._host_factory is not None:
            host = self._host_factory(self.config)
        else:
            from vertebra.extensions.broker import BrokerConfig, BrokerHost

            broker_config = BrokerConfig(
                shm_name=self.config.shm_name,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                lease_ttl_ms=self.config.lease_ttl_ms,
                role="owner",
                socket_path=self.config.socket_path,
                attach_secret=self.config.attach_secret,
            )
            host = BrokerHost(broker_config)

        self._host = host
        try:
            host.start()
        except BaseException:
            self.post_stop()
            raise

        env: dict[str, str] = {
            "VERTEBRA_BROKER_SHM": self.config.shm_name,
            "VERTEBRA_BROKER_POOL_SIZE": str(self.config.pool_size),
        }
        if self.config.socket_path is not None:
            env["VERTEBRA_BROKER_SOCKET"] = self.config.socket_path
        if self.config.attach_secret is not None:
            env["VERTEBRA_BROKER_SECRET"] = self.config.attach_secret
        return env

    def post_stop(self) -> None:
        """Stop the active BrokerHost and unlink SHM and socket paths idempotently."""
        host = self._host
        self._host = None
        try:
            if host is not None:
                host.stop()
        finally:
            shm_path = Path(self.config.shm_dir) / self.config.shm_name
            with contextlib.suppress(OSError):
                shm_path.unlink()
            if self.config.socket_path is not None:
                with contextlib.suppress(OSError):
                    Path(self.config.socket_path).unlink()


class OhaRunner:
    """Drives the `oha` HTTP load-testing tool and parses JSON output."""

    def __init__(
        self,
        url: str,
        duration: int = 10,
        concurrency: int = 64,
        method: str = "GET",
        body: str | None = None,
        rate: float | None = None,
    ) -> None:
        self.url = url
        self.duration = duration
        self.concurrency = concurrency
        self.method = method
        self.body = body
        self.rate = rate

    def run(self) -> dict[str, Any]:
        """Execute oha via OhaDriver and return parsed JSON results.

        Returns:
            Parsed result dict with normalized keys.

        Raises:
            DriverBinaryNotFoundError: If oha is absent from PATH.
            subprocess.CalledProcessError: If oha exits non-zero.
            json.JSONDecodeError: If oha output is not valid JSON.
        """
        config = LoadConfig(
            url=self.url,
            duration=self.duration,
            concurrency=self.concurrency,
            method=self.method,
            body=self.body,
            rate=self.rate,
        )
        return OhaDriver().run(config)

    def _build_env(self) -> dict[str, str]:
        """Build a stable child environment for oha."""
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        return env

    def _parse(self, raw: dict[str, Any]) -> dict[str, Any]:
        """Parse oha 1.12.1 JSON output into a flat result dict.

        Computes RPS from total successful requests divided by elapsed time,
        since instantaneous sample averages can skew on bursty batch-first workloads.
        """
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
        self,
        cmd: list[str],
        host: str = "127.0.0.1",
        port: int = 8000,
        env: dict[str, str] | None = None,
        hooks: Sequence[ServerLifecycleHook] | None = None,
    ) -> None:
        self.cmd = cmd
        self.host = host
        self.port = port
        self.env = env or {}
        self._hooks: tuple[ServerLifecycleHook, ...] = tuple(hooks) if hooks is not None else ()
        self._started_hooks: list[ServerLifecycleHook] = []
        self._process: subprocess.Popen[bytes] | None = None

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        self.stop()

    def start(self) -> None:
        """Run pre-start lifecycle hooks and start the server subprocess in a new process group."""
        hook_env: dict[str, str] = {}
        try:
            for hook in self._hooks:
                returned_env = hook.pre_start()
                self._started_hooks.append(hook)
                if returned_env:
                    hook_env.update(returned_env)
            merged_env = {**os.environ, **hook_env, **self.env}
            self._process = subprocess.Popen(
                self.cmd,
                env=merged_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                process_group=0,
            )
            self.wait_for_port()
        except BaseException:
            self.stop()
            raise

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
                    stderr = "\n".join(stderr.splitlines()[-20:])
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
        """Stop the server process group and tear down started lifecycle hooks in reverse order."""
        if self._process is not None:
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

        hooks_to_stop = list(reversed(self._started_hooks))
        self._started_hooks.clear()
        for hook in hooks_to_stop:
            try:
                hook.post_stop()
            except Exception:
                logger.exception("Lifecycle hook %r failed during post_stop", hook)


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
