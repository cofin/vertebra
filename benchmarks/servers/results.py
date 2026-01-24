"""Result dataclasses for ASGI benchmarks.

Follows the pattern from ipc_fast_path.py with dataclasses and computed
properties for consistent result handling.
"""

from __future__ import annotations

import json
import platform
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .config import BenchmarkCategory

if TYPE_CHECKING:
    from .parsers import OhaResult


@dataclass
class EnvironmentInfo:
    """System environment metadata for reproducibility."""

    cpu: str
    cpu_cores: int
    os: str
    os_version: str
    python_version: str
    oha_version: str | None
    timestamp: str  # ISO 8601 format

    @classmethod
    def detect(cls, oha_version: str | None = None) -> EnvironmentInfo:
        """Detect current environment."""
        return cls(
            cpu=_detect_cpu_name(),
            cpu_cores=_detect_cpu_cores(),
            os=platform.system(),
            os_version=platform.release(),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            oha_version=oha_version,
            timestamp=datetime.now(UTC).isoformat(),
        )


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run.

    One result per (server, scenario, concurrency) combination.
    """

    server: str
    server_display: str
    category: BenchmarkCategory
    scenario: str
    concurrency: int
    duration_seconds: int

    # Request metrics
    total_requests: int
    rps: float
    success_rate: float

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
    total_data_bytes: int = 0
    bytes_per_second: float = 0.0

    # Additional metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_oha(
        cls,
        oha_result: OhaResult,
        server: str,
        server_display: str,
        category: BenchmarkCategory,
        scenario: str,
        concurrency: int,
        duration_seconds: int,
        **metadata: Any,
    ) -> BenchmarkResult:
        """Create from OhaResult."""
        return cls(
            server=server,
            server_display=server_display,
            category=category,
            scenario=scenario,
            concurrency=concurrency,
            duration_seconds=duration_seconds,
            total_requests=oha_result.total_requests,
            rps=oha_result.rps,
            success_rate=oha_result.success_rate,
            latency_avg_ms=oha_result.latency_avg_ms,
            latency_p50_ms=oha_result.latency_p50_ms,
            latency_p75_ms=oha_result.latency_p75_ms,
            latency_p90_ms=oha_result.latency_p90_ms,
            latency_p95_ms=oha_result.latency_p95_ms,
            latency_p99_ms=oha_result.latency_p99_ms,
            latency_max_ms=oha_result.latency_max_ms,
            latency_min_ms=oha_result.latency_min_ms,
            total_data_bytes=oha_result.total_data_bytes,
            bytes_per_second=oha_result.bytes_per_second,
            metadata=dict(metadata),
        )

    @property
    def passes_sanity_check(self) -> bool:
        """Validate results are reasonable."""
        return (
            self.rps > 100  # At least 100 RPS
            and self.success_rate >= 99.0  # At least 99% success
            and self.latency_avg_ms > 0
            and self.latency_p99_ms < 60000  # Under 60 seconds
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d["category"] = self.category.value
        return d


@dataclass
class BenchmarkSuite:
    """Container for all benchmark results."""

    environment: EnvironmentInfo
    results: list[BenchmarkResult] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    def add_result(self, result: BenchmarkResult) -> None:
        """Add a benchmark result."""
        self.results.append(result)

    def add_error(self, server: str, scenario: str, error: str, **extra: Any) -> None:
        """Record a benchmark error."""
        self.errors.append({"server": server, "scenario": scenario, "error": error, **extra})

    def get_results_by_scenario(self, scenario: str) -> list[BenchmarkResult]:
        """Get all results for a specific scenario."""
        return [r for r in self.results if r.scenario == scenario]

    def get_results_by_server(self, server: str) -> list[BenchmarkResult]:
        """Get all results for a specific server."""
        return [r for r in self.results if r.server == server]

    def get_results_by_category(self, category: BenchmarkCategory) -> list[BenchmarkResult]:
        """Get all results for a specific category."""
        return [r for r in self.results if r.category == category]

    def get_scenarios(self) -> list[str]:
        """Get unique scenario names."""
        return sorted({r.scenario for r in self.results})

    def get_servers(self) -> list[str]:
        """Get unique server names."""
        return sorted({r.server for r in self.results})

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "environment": asdict(self.environment),
            "results": [r.to_dict() for r in self.results],
            "errors": self.errors,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def save_json(self, path: Path | str) -> None:
        """Save results to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json())

    @classmethod
    def from_json(cls, json_str: str) -> BenchmarkSuite:
        """Load from JSON string."""
        data = json.loads(json_str)
        env = EnvironmentInfo(**data["environment"])
        suite = cls(environment=env)

        for r in data.get("results", []):
            r["category"] = BenchmarkCategory(r["category"])
            suite.results.append(BenchmarkResult(**r))

        suite.errors = data.get("errors", [])
        return suite

    @classmethod
    def load_json(cls, path: Path | str) -> BenchmarkSuite:
        """Load from JSON file."""
        return cls.from_json(Path(path).read_text())


def _detect_cpu_name() -> str:
    """Detect CPU model name."""
    try:
        # Linux
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":")[1].strip()
    except (FileNotFoundError, PermissionError):
        pass

    try:
        # macOS
        import subprocess

        result = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, timeout=5, check=False
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    return platform.processor() or "Unknown"


def _detect_cpu_cores() -> int:
    """Detect number of CPU cores."""
    import os

    return os.cpu_count() or 1
