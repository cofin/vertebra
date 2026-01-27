"""Historical benchmark result tracking.

Stores benchmark runs over time for regression detection and trend analysis.
Results are saved as timestamped JSON files in benchmarks/results/runs/.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .results import BenchmarkResult, BenchmarkSuite

# Default storage location
HISTORY_DIR = Path(__file__).parent.parent / "results" / "runs"


@dataclass
class Comparison:
    """Comparison between two benchmark results."""

    server: str
    scenario: str
    current_rps: float
    previous_rps: float
    delta_rps: float
    delta_percent: float
    current_p99_ms: float
    previous_p99_ms: float
    delta_p99_ms: float

    @property
    def is_regression(self) -> bool:
        """True if RPS decreased by more than 5%."""
        return self.delta_percent < -5.0

    @property
    def is_improvement(self) -> bool:
        """True if RPS increased by more than 5%."""
        return self.delta_percent > 5.0


@dataclass
class HistoryEntry:
    """A historical benchmark run entry."""

    timestamp: str
    git_sha: str | None
    servers_tested: list[str]
    total_results: int
    path: Path


class BenchmarkHistory:
    """Manages historical benchmark storage and comparison."""

    def __init__(self, history_dir: Path | None = None) -> None:
        self.history_dir = history_dir or HISTORY_DIR
        self.history_dir.mkdir(parents=True, exist_ok=True)

    def save(self, suite: BenchmarkSuite, git_sha: str | None = None) -> Path:
        """Save a benchmark suite to history.

        Args:
            suite: The benchmark suite to save
            git_sha: Optional git commit SHA for tracking

        Returns:
            Path to the saved file
        """
        timestamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
        filename = f"{timestamp}.json"
        path = self.history_dir / filename

        # Add git sha to metadata
        data = suite.to_dict()
        data["git_sha"] = git_sha

        path.write_text(json.dumps(data, indent=2))
        return path

    def list_runs(self, limit: int | None = None) -> list[HistoryEntry]:
        """List historical runs, most recent first.

        Args:
            limit: Maximum number of entries to return

        Returns:
            List of history entries
        """
        entries = []

        for path in sorted(self.history_dir.glob("*.json"), reverse=True):
            try:
                data = json.loads(path.read_text())
                entries.append(
                    HistoryEntry(
                        timestamp=data.get("environment", {}).get("timestamp", path.stem),
                        git_sha=data.get("git_sha"),
                        servers_tested=list({r["server"] for r in data.get("results", [])}),
                        total_results=len(data.get("results", [])),
                        path=path,
                    )
                )
            except (json.JSONDecodeError, KeyError):
                continue

            if limit and len(entries) >= limit:
                break

        return entries

    def get_latest(self) -> BenchmarkSuite | None:
        """Get the most recent benchmark run.

        Returns:
            BenchmarkSuite or None if no history exists
        """
        runs = self.list_runs(limit=1)
        if not runs:
            return None

        from .results import BenchmarkSuite

        return BenchmarkSuite.load_json(runs[0].path)

    def get_previous(self, current_timestamp: str | None = None) -> BenchmarkSuite | None:
        """Get the run before the current one.

        Args:
            current_timestamp: Timestamp of current run to exclude

        Returns:
            BenchmarkSuite or None if no previous run exists
        """
        runs = self.list_runs(limit=2)

        # If we have a current timestamp, skip it
        if current_timestamp:
            runs = [r for r in runs if r.timestamp != current_timestamp]

        if not runs:
            return None

        from .results import BenchmarkSuite

        return BenchmarkSuite.load_json(runs[0].path)

    def compare(
        self, current: BenchmarkSuite, previous: BenchmarkSuite, servers: list[str] | None = None
    ) -> list[Comparison]:
        """Compare two benchmark runs.

        Args:
            current: Current benchmark suite
            previous: Previous benchmark suite
            servers: Optional filter for specific servers

        Returns:
            List of comparisons for matching (server, scenario) pairs
        """
        comparisons = []

        # Index previous results
        prev_index: dict[tuple[str, str], BenchmarkResult] = {}
        for r in previous.results:
            prev_index[(r.server, r.scenario)] = r

        # Compare current to previous
        for result in current.results:
            if servers and result.server not in servers:
                continue

            key = (result.server, result.scenario)
            prev_result = prev_index.get(key)

            if prev_result is None:
                continue

            delta_rps = result.rps - prev_result.rps
            delta_percent = (delta_rps / prev_result.rps * 100) if prev_result.rps > 0 else 0.0

            comparisons.append(
                Comparison(
                    server=result.server,
                    scenario=result.scenario,
                    current_rps=result.rps,
                    previous_rps=prev_result.rps,
                    delta_rps=delta_rps,
                    delta_percent=delta_percent,
                    current_p99_ms=result.latency_p99_ms,
                    previous_p99_ms=prev_result.latency_p99_ms,
                    delta_p99_ms=result.latency_p99_ms - prev_result.latency_p99_ms,
                )
            )

        return comparisons

    def get_vertebra_comparisons(self, current: BenchmarkSuite) -> list[Comparison]:
        """Compare current run against previous for Vertebra servers only.

        Args:
            current: Current benchmark suite

        Returns:
            List of comparisons for Vertebra servers
        """
        previous = self.get_previous(current.environment.timestamp)
        if previous is None:
            return []

        vertebra_servers = [r.server for r in current.results if r.server.startswith("vertebra")]
        return self.compare(current, previous, servers=vertebra_servers)


def get_git_sha() -> str | None:
    """Get current git commit SHA."""
    import subprocess

    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, timeout=5, check=False)
        if result.returncode == 0:
            return result.stdout.strip()[:12]
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def format_comparison_table(comparisons: list[Comparison]) -> str:
    """Format comparisons as a markdown table.

    Args:
        comparisons: List of comparisons to format

    Returns:
        Markdown table string
    """
    if not comparisons:
        return "No comparisons available (no previous run found)."

    lines = [
        "| Server | Scenario | Current RPS | Previous RPS | Delta | Change |",
        "|--------|----------|-------------|--------------|-------|--------|",
    ]

    for c in sorted(comparisons, key=lambda x: (x.server, x.scenario)):
        indicator = ""
        if c.is_regression:
            indicator = "⬇️"
        elif c.is_improvement:
            indicator = "⬆️"

        lines.append(
            f"| {c.server} | {c.scenario} | {c.current_rps:,.0f} | "
            f"{c.previous_rps:,.0f} | {c.delta_rps:+,.0f} | "
            f"{c.delta_percent:+.1f}% {indicator} |"
        )

    return "\n".join(lines)
