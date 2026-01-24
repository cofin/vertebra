"""Parser for oha load generator output.

oha is a Rust-based HTTP load generator that provides accurate latency
measurements with minimal client-side overhead.

Usage:
    oha -z 10s -c 64 --no-tui -j --latency-correction http://localhost:8000/
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any


class OhaError(Exception):
    """Error running oha."""


class OhaNotFoundError(OhaError):
    """oha is not installed."""


# Cache for oha binary path
_oha_binary: str | None = None


def find_oha_binary() -> str | None:
    """Find the oha binary, checking common installation locations.

    Returns:
        Path to oha binary or None if not found
    """
    global _oha_binary
    if _oha_binary is not None:
        return _oha_binary

    import os
    import shutil
    from pathlib import Path

    # Check PATH first
    oha_path = shutil.which("oha")
    if oha_path:
        _oha_binary = oha_path
        return _oha_binary

    # Check common cargo install locations
    home = Path.home()
    cargo_locations = [
        home / ".cargo" / "bin" / "oha",
        Path("/usr/local/bin/oha"),
        Path("/opt/homebrew/bin/oha"),  # macOS Homebrew ARM
    ]

    for location in cargo_locations:
        if location.is_file() and os.access(location, os.X_OK):
            _oha_binary = str(location)
            return _oha_binary

    # Check Homebrew Cellar (Intel macOS)
    cellar = Path("/usr/local/Cellar/oha")
    if cellar.exists():
        for version_dir in cellar.iterdir():
            oha_bin = version_dir / "bin" / "oha"
            if oha_bin.is_file():
                _oha_binary = str(oha_bin)
                return _oha_binary

    return None


@dataclass
class OhaResult:
    """Results from an oha benchmark run."""

    total_requests: int
    successful_requests: int
    failed_requests: int
    rps: float
    success_rate: float  # percentage (0-100)

    # Latencies in milliseconds
    latency_avg_ms: float
    latency_p50_ms: float
    latency_p75_ms: float
    latency_p90_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_max_ms: float
    latency_min_ms: float

    # Transfer stats
    total_data_bytes: int
    bytes_per_second: float

    # Raw data for additional analysis
    raw_data: dict[str, Any]

    @property
    def passes_sanity_check(self) -> bool:
        """Basic sanity check for valid results."""
        return (
            self.rps > 0
            and self.success_rate >= 99.0
            and self.latency_avg_ms > 0
            and self.latency_p99_ms < 60000  # Under 60 seconds
        )

    @property
    def error_rate(self) -> float:
        """Error rate as percentage."""
        return 100.0 - self.success_rate


def run_oha(
    url: str,
    duration_seconds: int = 10,
    concurrency: int = 64,
    method: str = "GET",
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: int = 120,
) -> OhaResult:
    """Run oha and parse results.

    Args:
        url: Target URL to benchmark
        duration_seconds: How long to run the benchmark
        concurrency: Number of concurrent connections
        method: HTTP method (GET, POST, etc.)
        body: Request body for POST/PUT
        headers: Additional headers to send
        timeout_seconds: Subprocess timeout

    Returns:
        OhaResult with all metrics

    Raises:
        OhaNotFoundError: If oha is not installed
        OhaError: If benchmark fails
    """
    oha_path = find_oha_binary()
    if oha_path is None:
        msg = "oha not found. Install with: cargo install oha\nOr via package manager: brew install oha (macOS)"
        raise OhaNotFoundError(msg)

    cmd = [
        oha_path,
        "-z",
        f"{duration_seconds}s",
        "-c",
        str(concurrency),
        "-j",  # JSON output
        "--no-tui",  # No terminal UI
        "--latency-correction",  # Coordinated omission correction
        "-m",
        method,
    ]

    if body is not None:
        cmd.extend(["-d", body.decode("utf-8", errors="replace")])

    if headers:
        for key, value in headers.items():
            cmd.extend(["-H", f"{key}: {value}"])

    cmd.append(url)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,  # We'll check manually for better error messages
        )
    except FileNotFoundError as e:
        msg = (
            "oha not found. Install with: cargo install oha\n"
            "Or via package manager: brew install oha (macOS), pacman -S oha (Arch)"
        )
        raise OhaNotFoundError(msg) from e
    except subprocess.TimeoutExpired as e:
        msg = f"oha timed out after {timeout_seconds}s"
        raise OhaError(msg) from e

    if result.returncode != 0:
        msg = f"oha failed (exit {result.returncode}): {result.stderr}"
        raise OhaError(msg)

    return parse_oha_output(result.stdout)


def parse_oha_output(json_output: str) -> OhaResult:
    """Parse oha JSON output into OhaResult.

    Args:
        json_output: Raw JSON string from oha

    Returns:
        OhaResult with all metrics
    """
    try:
        data = json.loads(json_output)
    except json.JSONDecodeError as e:
        msg = f"Failed to parse oha output: {e}"
        raise OhaError(msg) from e

    # Extract summary stats
    summary = data.get("summary", data)  # oha v1.x vs v0.x format

    # Handle different oha versions
    # v1.x uses "requestsPerSec", v0.x uses "rps"
    rps = summary.get("requestsPerSec", summary.get("rps", 0.0))

    # Success rate calculation
    total = summary.get("total", summary.get("totalRequests", 0))
    succeeded = summary.get("succeeded", summary.get("successfulRequests", total))
    failed = total - succeeded
    success_rate = (succeeded / total * 100) if total > 0 else 0.0

    # Latency distribution - convert seconds to milliseconds
    # oha reports latencies in seconds
    latency_dist = summary.get("latencyDistribution", {})

    def to_ms(key: str, default: float = 0.0) -> float:
        """Convert seconds to milliseconds."""
        val = latency_dist.get(key, summary.get(key, default))
        # If value looks like seconds (< 1000), convert to ms
        # If already in ms (> 1000), keep as is
        if isinstance(val, (int, float)) and val < 100:
            return val * 1000
        return float(val) if val else default

    # Extract latency percentiles
    latency_avg = summary.get("average", summary.get("latencyAvg", 0))
    if latency_avg < 100:  # Seconds, convert to ms
        latency_avg *= 1000

    latency_max = summary.get("slowest", summary.get("latencyMax", 0))
    if latency_max < 1000:  # Seconds, convert to ms
        latency_max *= 1000

    latency_min = summary.get("fastest", summary.get("latencyMin", 0))
    if latency_min < 100:  # Seconds, convert to ms
        latency_min *= 1000

    # Transfer stats
    total_data = summary.get("totalData", summary.get("totalBytes", 0))
    bps = summary.get("sizePerSec", summary.get("bytesPerSec", 0))

    return OhaResult(
        total_requests=total,
        successful_requests=succeeded,
        failed_requests=failed,
        rps=rps,
        success_rate=success_rate,
        latency_avg_ms=latency_avg,
        latency_p50_ms=to_ms("50", to_ms("p50")),
        latency_p75_ms=to_ms("75", to_ms("p75")),
        latency_p90_ms=to_ms("90", to_ms("p90")),
        latency_p95_ms=to_ms("95", to_ms("p95")),
        latency_p99_ms=to_ms("99", to_ms("p99")),
        latency_max_ms=latency_max,
        latency_min_ms=latency_min,
        total_data_bytes=total_data,
        bytes_per_second=bps,
        raw_data=data,
    )


def detect_oha_version() -> str | None:
    """Detect installed oha version.

    Returns:
        Version string or None if not installed
    """
    oha_path = find_oha_binary()
    if oha_path is None:
        return None

    try:
        result = subprocess.run([oha_path, "--version"], capture_output=True, text=True, timeout=5, check=False)
        if result.returncode == 0:
            # Output format: "oha 1.4.0"
            return result.stdout.strip().split()[-1]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None
