"""Unified closed-loop and open-loop load-generator drivers for benchmarks."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import msgspec

if TYPE_CHECKING:
    from collections.abc import Sequence

DOCKER_DRIVER_IMAGES: dict[str, str] = {
    "oha": "ghcr.io/hatoo/oha:latest",
    "h2load": "ghcr.io/nghttp2/nghttp2:latest",
    "ghz": "ghcr.io/bojand/ghz:latest",
    "k6": "grafana/k6:latest",
}

_DURATION_MULTIPLIERS: dict[str, float] = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
}

_DURATION_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(ns|us|µs|ms|s)\s*$")
_H2LOAD_FINISHED_RE = re.compile(r"finished in\s+[^\s,]+,\s*([0-9]+(?:\.[0-9]+)?)\s*req/s")
_H2LOAD_REQUESTS_RE = re.compile(
    r"requests:\s*(\d+)\s+total,\s*\d+\s+started,\s*\d+\s+done,\s*(\d+)\s+succeeded,\s*(\d+)\s+failed,\s*(\d+)\s+errored"
)
_H2LOAD_TIME_RE = re.compile(
    r"time for request:\s*"
    r"([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s))\s+"
    r"([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s))\s+"
    r"([0-9]+(?:\.[0-9]+)?(?:ns|us|µs|ms|s))"
)


class DriverBinaryNotFoundError(FileNotFoundError):
    """Raised when a required load-generator executable is absent from PATH."""

    def __init__(self, driver_name: str, binary: str, install_hint: str) -> None:
        self.driver_name = driver_name
        self.binary = binary
        self.install_hint = install_hint
        super().__init__(f"Load driver '{driver_name}' requires '{binary}' on PATH. {install_hint}")


class LoadConfig(msgspec.Struct, frozen=True, gc=False):
    """Configuration shared across closed-loop and open-loop load drivers."""

    url: str
    duration: int = 10
    concurrency: int = 64
    method: str = "GET"
    body: str | None = None
    headers: tuple[tuple[str, str], ...] = ()
    rate: float | None = None
    max_concurrent_streams: int = 100
    grpc_call: str | None = None
    grpc_proto: str | None = None
    grpc_data: str | None = None
    k6_script: str | None = None
    insecure: bool = True


class LoadDriver(Protocol):
    """Protocol implemented by all benchmark load-generator drivers."""

    name: str
    binary: str
    install_hint: str

    def build_command(self, config: LoadConfig) -> list[str]:
        """Build the subprocess command line for the given load configuration."""
        ...

    def parse_output(self, stdout: str, config: LoadConfig) -> dict[str, Any]:
        """Parse raw load-generator output into the normalized benchmark metric contract."""
        ...

    def run(self, config: LoadConfig) -> dict[str, Any]:
        """Verify binary availability, execute the load generator, and return normalized metrics."""
        ...


def resolve_driver_binary(driver_name: str, binary: str, install_hint: str) -> str:
    """Resolve a load-driver executable from PATH or raise DriverBinaryNotFoundError."""
    resolved = shutil.which(binary)
    if resolved is None:
        raise DriverBinaryNotFoundError(driver_name, binary, install_hint)
    return resolved


def build_docker_driver_command(
    driver_name: str,
    driver_args: Sequence[str],
    *,
    network: str = "host",
) -> list[str]:
    """Build a containerized `docker run` invocation for a supported load driver.

    Args:
        driver_name: Logical driver name (`oha`, `h2load`, `ghz`, or `k6`).
        driver_args: Arguments forwarded to the container entrypoint.
        network: Docker network mode (defaults to `"host"`).

    Returns:
        Full `docker run` command argument list.

    Raises:
        ValueError: If `driver_name` is not a recognized load driver.
    """
    image = DOCKER_DRIVER_IMAGES.get(driver_name)
    if image is None:
        msg = f"Unknown load driver: {driver_name!r}"
        raise ValueError(msg)
    return ["docker", "run", "--rm", "--network", network, image, *driver_args]


def _validate_rate(rate: float | None) -> None:
    if rate is not None and rate <= 0:
        msg = "rate must be positive"
        raise ValueError(msg)


def _load_mode(config: LoadConfig) -> str:
    return "open_loop" if config.rate is not None else "closed_loop"


def _to_float(value: Any) -> float:
    return float(value) if value is not None else 0.0


def _to_int(value: Any) -> int:
    return int(value) if value is not None else 0


def _parse_duration_to_seconds(token: str) -> float:
    match = _DURATION_RE.match(token)
    if match is None:
        msg = f"Unparseable duration token: {token!r}"
        raise ValueError(msg)
    return float(match.group(1)) * _DURATION_MULTIPLIERS[match.group(2)]


def _strip_grpc_scheme(url: str) -> str:
    target = url
    for prefix in ("http://", "https://", "grpc://"):
        if target.startswith(prefix):
            target = target[len(prefix) :]
            break
    return target.rstrip("/")


def _k6_metric_values(metric: Any) -> dict[str, Any]:
    if not isinstance(metric, dict):
        return {}
    nested = metric.get("values")
    if isinstance(nested, dict):
        return nested
    return metric


class OhaDriver:
    """HTTP/1.1 load driver wrapping `oha` in closed-loop or open-loop mode."""

    name: str = "oha"
    binary: str = "oha"
    install_hint: str = "Run `make bench-tools` or install via `cargo install oha`."

    def _build_env(self) -> dict[str, str]:
        """Build a stable child environment for oha with NO_COLOR removed."""
        env = os.environ.copy()
        env.pop("NO_COLOR", None)
        return env

    def build_command(self, config: LoadConfig) -> list[str]:
        """Build the `oha` command line for closed-loop or open-loop execution."""
        _validate_rate(config.rate)
        cmd = [
            self.binary,
            "-z",
            f"{config.duration}s",
            "-c",
            str(config.concurrency),
            "--output-format",
            "json",
            "--no-tui",
        ]
        if config.rate is not None:
            cmd.extend(["-q", str(config.rate)])
        else:
            cmd.append("--latency-correction")
        cmd.extend(["-m", config.method])
        for key, value in config.headers:
            cmd.extend(["-H", f"{key}: {value}"])
        if config.body is not None:
            cmd.extend(["-d", config.body, "-T", "application/octet-stream"])
        cmd.append(config.url)
        return cmd

    def parse_output(self, stdout: str, config: LoadConfig) -> dict[str, Any]:
        """Parse `oha --output-format json` output into normalized benchmark metrics."""
        raw = json.loads(stdout)
        if not isinstance(raw, dict):
            msg = "Expected JSON object from oha output"
            raise ValueError(msg)
        summary = raw.get("summary")
        if not isinstance(summary, dict):
            msg = "oha output missing summary section"
            raise ValueError(msg)

        latency_pcts = raw.get("latencyPercentiles") or {}
        rps_data = raw.get("rps") or {}
        status_dist = raw.get("statusCodeDistribution") or {}
        error_dist = raw.get("errorDistribution") or {}
        success_rate = float(summary.get("successRate") or 0.0)

        http_errors = sum(_to_int(count) for code, count in status_dist.items() if not str(code).startswith("2"))
        transport_errors = sum(
            _to_int(count)
            for kind, count in error_dist.items()
            if kind != "aborted due to deadline" or success_rate < 1.0
        )
        total_requests = sum(_to_int(count) for count in status_dist.values()) + transport_errors
        if total_requests <= 0:
            msg = "oha output reported zero total requests"
            raise ValueError(msg)

        elapsed = _to_float(summary.get("total"))
        successful = sum(_to_int(count) for code, count in status_dist.items() if str(code).startswith("2"))
        rps = successful / elapsed if elapsed > 0 and successful > 0 else _to_float(rps_data.get("mean"))

        return {
            "rps": rps,
            "latency_mean_s": _to_float(summary.get("average")),
            "latency_p50_s": _to_float(latency_pcts.get("p50")),
            "latency_p95_s": _to_float(latency_pcts.get("p95")),
            "latency_p99_s": _to_float(latency_pcts.get("p99")),
            "latency_p999_s": _to_float(latency_pcts.get("p99.9")),
            "latency_max_s": _to_float(summary.get("slowest")),
            "total_requests": total_requests,
            "errors": http_errors + transport_errors,
            "load_mode": _load_mode(config),
            "target_rps": config.rate,
        }

    def run(self, config: LoadConfig) -> dict[str, Any]:
        """Run `oha` and return normalized benchmark metrics."""
        resolve_driver_binary(self.name, self.binary, self.install_hint)
        cmd = self.build_command(config)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            env=self._build_env(),
            timeout=config.duration + 30,
        )
        return self.parse_output(result.stdout, config)


class H2LoadDriver:
    """HTTP/2 multiplexed load driver wrapping `h2load`."""

    name: str = "h2load"
    binary: str = "h2load"
    install_hint: str = "Run `make bench-tools` or install `nghttp2-client` (`apt-get install nghttp2-client` / `brew install nghttp2`)."

    def build_command(self, config: LoadConfig) -> list[str]:
        """Build the `h2load` command line with per-client RPS conversion when rate-limited."""
        _validate_rate(config.rate)
        cmd = [
            self.binary,
            "-D",
            str(config.duration),
            "-c",
            str(config.concurrency),
            "-m",
            str(config.max_concurrent_streams),
        ]
        if config.rate is not None:
            if config.concurrency <= 0:
                msg = "concurrency must be positive"
                raise ValueError(msg)
            per_client_rps = config.rate / config.concurrency
            cmd.extend(["--rps", str(per_client_rps)])
        for key, value in config.headers:
            cmd.extend(["-H", f"{key}: {value}"])
        cmd.append(config.url)
        return cmd

    def parse_output(self, stdout: str, config: LoadConfig) -> dict[str, Any]:
        """Parse `h2load` stdout summary lines and convert latency units to seconds."""
        finished_match = _H2LOAD_FINISHED_RE.search(stdout)
        requests_match = _H2LOAD_REQUESTS_RE.search(stdout)
        time_match = _H2LOAD_TIME_RE.search(stdout)
        if finished_match is None or requests_match is None or time_match is None:
            msg = "Unable to parse h2load summary output"
            raise ValueError(msg)

        rps = float(finished_match.group(1))
        total_requests = int(requests_match.group(1))
        failed = int(requests_match.group(3))
        errored = int(requests_match.group(4))
        if total_requests <= 0:
            msg = "h2load output reported zero total requests"
            raise ValueError(msg)

        latency_max_s = _parse_duration_to_seconds(time_match.group(2))
        latency_mean_s = _parse_duration_to_seconds(time_match.group(3))

        return {
            "rps": rps,
            "latency_mean_s": latency_mean_s,
            "latency_p50_s": latency_mean_s,
            "latency_p95_s": latency_max_s,
            "latency_p99_s": latency_max_s,
            "latency_p999_s": latency_max_s,
            "latency_max_s": latency_max_s,
            "total_requests": total_requests,
            "errors": max(failed, errored),
            "load_mode": _load_mode(config),
            "target_rps": config.rate,
        }

    def run(self, config: LoadConfig) -> dict[str, Any]:
        """Run `h2load` and return normalized benchmark metrics."""
        resolve_driver_binary(self.name, self.binary, self.install_hint)
        cmd = self.build_command(config)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=config.duration + 30,
        )
        return self.parse_output(result.stdout, config)


class GhzDriver:
    """gRPC load driver wrapping `ghz`."""

    name: str = "ghz"
    binary: str = "ghz"
    install_hint: str = "Run `make bench-tools` or install ghz from https://ghz.sh/."

    def build_command(self, config: LoadConfig) -> list[str]:
        """Build the `ghz` command line for gRPC unary or streaming load."""
        _validate_rate(config.rate)
        if not config.grpc_call:
            msg = "GhzDriver requires LoadConfig.grpc_call"
            raise ValueError(msg)

        cmd = [
            self.binary,
            "--format",
            "json",
            "-z",
            f"{config.duration}s",
            "-c",
            str(config.concurrency),
            "--call",
            config.grpc_call,
        ]
        if config.insecure:
            cmd.append("--insecure")
        if config.grpc_proto is not None:
            cmd.extend(["--proto", config.grpc_proto])
        if config.grpc_data is not None:
            cmd.extend(["-d", config.grpc_data])
        if config.rate is not None:
            cmd.extend(["--rps", str(int(config.rate))])
        cmd.append(_strip_grpc_scheme(config.url))
        return cmd

    def parse_output(self, stdout: str, config: LoadConfig) -> dict[str, Any]:
        """Parse `ghz --format json` output and convert nanosecond latencies to seconds."""
        raw = json.loads(stdout)
        if not isinstance(raw, dict):
            msg = "Expected JSON object from ghz output"
            raise ValueError(msg)

        total_requests = _to_int(raw.get("count"))
        if total_requests <= 0:
            msg = "ghz output reported zero total requests"
            raise ValueError(msg)

        ns_to_s = 1e-9
        percentiles: dict[float, float] = {}
        raw_dist = raw.get("latencyDistribution")
        if isinstance(raw_dist, list):
            for entry in raw_dist:
                if isinstance(entry, dict) and "percentage" in entry:
                    percentiles[float(entry["percentage"])] = _to_float(entry.get("latency")) * ns_to_s

        status_dist = raw.get("statusCodeDistribution") or {}
        error_dist = raw.get("errorDistribution") or {}
        non_ok_errors = sum(_to_int(count) for code, count in status_dist.items() if str(code).upper() != "OK")
        error_dist_total = sum(_to_int(count) for count in error_dist.values())

        return {
            "rps": _to_float(raw.get("rps")),
            "latency_mean_s": _to_float(raw.get("average")) * ns_to_s,
            "latency_p50_s": percentiles.get(50.0, 0.0),
            "latency_p95_s": percentiles.get(95.0, 0.0),
            "latency_p99_s": percentiles.get(99.0, 0.0),
            "latency_p999_s": percentiles.get(99.9, 0.0),
            "latency_max_s": _to_float(raw.get("slowest")) * ns_to_s,
            "total_requests": total_requests,
            "errors": non_ok_errors + error_dist_total,
            "load_mode": _load_mode(config),
            "target_rps": config.rate,
        }

    def run(self, config: LoadConfig) -> dict[str, Any]:
        """Run `ghz` and return normalized benchmark metrics."""
        resolve_driver_binary(self.name, self.binary, self.install_hint)
        cmd = self.build_command(config)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=config.duration + 30,
        )
        return self.parse_output(result.stdout, config)


class K6Driver:
    """Open-loop and scenario load driver wrapping Grafana `k6`."""

    name: str = "k6"
    binary: str = "k6"
    install_hint: str = (
        "Run `make bench-tools` or install k6 from https://grafana.com/docs/k6/latest/set-up/install-k6/."
    )

    def build_command(self, config: LoadConfig, *, summary_path: str | Path | None = None) -> list[str]:
        """Build the `k6 run` command line with `--summary-export` and scenario env vars."""
        _validate_rate(config.rate)
        if not config.k6_script:
            msg = "K6Driver requires LoadConfig.k6_script"
            raise ValueError(msg)

        export_target = str(summary_path) if summary_path is not None else "summary.json"
        cmd = [
            self.binary,
            "run",
            "--summary-export",
            export_target,
            "-e",
            f"TARGET_URL={config.url}",
            "-e",
            f"DURATION={config.duration}s",
            "-e",
            f"CONCURRENCY={config.concurrency}",
        ]
        if config.rate is not None:
            cmd.extend(["-e", f"TARGET_RPS={config.rate}"])
        cmd.append(config.k6_script)
        return cmd

    def parse_output(self, stdout: str, config: LoadConfig) -> dict[str, Any]:
        """Parse `k6 --summary-export` JSON and convert millisecond latencies to seconds."""
        raw = json.loads(stdout)
        if not isinstance(raw, dict):
            msg = "Expected JSON object from k6 summary"
            raise ValueError(msg)

        metrics = raw.get("metrics")
        if not isinstance(metrics, dict):
            msg = "k6 summary missing metrics section"
            raise ValueError(msg)

        http_reqs = _k6_metric_values(metrics.get("http_reqs"))
        http_req_duration = _k6_metric_values(metrics.get("http_req_duration"))
        http_req_failed = _k6_metric_values(metrics.get("http_req_failed"))

        total_requests = _to_int(http_reqs.get("count"))
        if total_requests <= 0:
            msg = "k6 summary reported zero total requests"
            raise ValueError(msg)

        ms_to_s = 1e-3
        if "passes" in http_req_failed:
            errors = _to_int(http_req_failed.get("passes"))
        else:
            errors = _to_int(http_req_failed.get("fails"))

        return {
            "rps": _to_float(http_reqs.get("rate")),
            "latency_mean_s": _to_float(http_req_duration.get("avg")) * ms_to_s,
            "latency_p50_s": _to_float(http_req_duration.get("med")) * ms_to_s,
            "latency_p95_s": _to_float(http_req_duration.get("p(95)")) * ms_to_s,
            "latency_p99_s": _to_float(http_req_duration.get("p(99)")) * ms_to_s,
            "latency_p999_s": _to_float(http_req_duration.get("p(99.9)")) * ms_to_s,
            "latency_max_s": _to_float(http_req_duration.get("max")) * ms_to_s,
            "total_requests": total_requests,
            "errors": errors,
            "load_mode": _load_mode(config),
            "target_rps": config.rate,
        }

    def run(self, config: LoadConfig) -> dict[str, Any]:
        """Run `k6` with a temporary summary export file and return normalized metrics."""
        resolve_driver_binary(self.name, self.binary, self.install_hint)
        with tempfile.TemporaryDirectory(prefix="vertebra-k6-") as tmp_dir:
            summary_file = Path(tmp_dir) / "summary.json"
            cmd = self.build_command(config, summary_path=summary_file)
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=config.duration + 30,
            )
            if not summary_file.exists():
                msg = f"k6 did not write summary export file: {summary_file}"
                raise ValueError(msg)
            return self.parse_output(summary_file.read_text(encoding="utf-8"), config)


def create_driver(name: str) -> LoadDriver:
    """Instantiate a load driver by logical name (`oha`, `h2load`, `ghz`, or `k6`)."""
    drivers: dict[str, type[OhaDriver] | type[H2LoadDriver] | type[GhzDriver] | type[K6Driver]] = {
        "oha": OhaDriver,
        "h2load": H2LoadDriver,
        "ghz": GhzDriver,
        "k6": K6Driver,
    }
    driver_cls = drivers.get(name)
    if driver_cls is None:
        msg = f"Unknown load driver: {name!r}"
        raise ValueError(msg)
    return driver_cls()


__all__ = (
    "DOCKER_DRIVER_IMAGES",
    "DriverBinaryNotFoundError",
    "GhzDriver",
    "H2LoadDriver",
    "K6Driver",
    "LoadConfig",
    "LoadDriver",
    "OhaDriver",
    "build_docker_driver_command",
    "create_driver",
    "resolve_driver_binary",
)
