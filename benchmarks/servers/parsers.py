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
        "--output-format",
        "json",  # JSON output (--output-format in v1.x, was -j in older versions)
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

    summary = data.get("summary") or {}
    percentiles = data.get("latencyPercentiles") or {}

    if not summary:
        msg = f"Missing summary in oha output: {json_output[:200]}"
        raise OhaError(msg)

    # Helper to get value with default (handles explicit null)
    def get(d: dict, key: str, default: float = 0.0) -> float:
        val = d.get(key)
        return val if val is not None else default

    # RPS and duration
    rps = get(summary, "requestsPerSec")
    duration = get(summary, "total", 1.0)
    success_rate_ratio = get(summary, "successRate", 1.0)
    success_rate = success_rate_ratio * 100

    # Calculate request counts
    total = int(rps * duration) if rps else 0
    succeeded = int(total * success_rate_ratio)
    failed = total - succeeded

    # Latencies (seconds -> milliseconds)
    def to_ms(key: str, d: dict = summary) -> float:
        return get(d, key) * 1000

    return OhaResult(
        total_requests=total,
        successful_requests=succeeded,
        failed_requests=failed,
        rps=rps,
        success_rate=success_rate,
        latency_avg_ms=to_ms("average"),
        latency_p50_ms=to_ms("p50", percentiles),
        latency_p75_ms=to_ms("p75", percentiles),
        latency_p90_ms=to_ms("p90", percentiles),
        latency_p95_ms=to_ms("p95", percentiles),
        latency_p99_ms=to_ms("p99", percentiles),
        latency_max_ms=to_ms("slowest"),
        latency_min_ms=to_ms("fastest"),
        total_data_bytes=int(get(summary, "totalData")),
        bytes_per_second=get(summary, "sizePerSec"),
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
