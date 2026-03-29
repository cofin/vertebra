from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.table import Table

console = Console()

_RPS_GREEN_THRESHOLD = 10000
_RPS_YELLOW_THRESHOLD = 5000
_DELTA_SIGNIFICANT_PCT = 2.0


def _fmt_rps(rps: float) -> str:
    """Format RPS with color coding."""
    formatted = f"{rps:,.0f}"
    if rps > _RPS_GREEN_THRESHOLD:
        return f"[green]{formatted}[/green]"
    if rps > _RPS_YELLOW_THRESHOLD:
        return f"[yellow]{formatted}[/yellow]"
    return f"[red]{formatted}[/red]"


def _fmt_ms(seconds: float) -> str:
    """Format seconds as milliseconds with 2 decimal places."""
    return f"{seconds * 1000:.2f}"


def _fmt_delta(pct: float) -> str:
    """Format a delta percentage with color (green=improvement, red=regression)."""
    if abs(pct) < _DELTA_SIGNIFICANT_PCT:
        return f"{pct:+.1f}%"
    if pct > 0:
        return f"[green]{pct:+.1f}%[/green]"
    return f"[red]{pct:+.1f}%[/red]"


def print_summary_table(results: list[dict[str, Any]]) -> None:
    """Print a Rich table summarizing benchmark results."""
    table = Table(title="Benchmark Results", show_lines=True)
    table.add_column("Target", style="cyan", no_wrap=True)
    table.add_column("Topology", style="yellow")
    table.add_column("Scenario", style="magenta")
    table.add_column("Conc", justify="right")
    table.add_column("RPS", justify="right")
    table.add_column("Avg (ms)", justify="right")
    table.add_column("P99 (ms)", justify="right")
    table.add_column("Errors", justify="right")

    for r in results:
        # Support both live results (seconds) and DuckDB results (ms)
        if "latency_mean_ms" in r:
            avg_ms = f"{r['latency_mean_ms']:.2f}"
            p99_ms = f"{r['latency_p99_ms']:.2f}"
        else:
            avg_ms = _fmt_ms(r.get("latency_mean_s", 0.0))
            p99_ms = _fmt_ms(r.get("latency_p99_s", 0.0))

        table.add_row(
            r.get("target", ""),
            r.get("topology", "unknown"),
            r.get("scenario", ""),
            str(r.get("concurrency", "")),
            _fmt_rps(r.get("rps", 0.0)),
            avg_ms,
            p99_ms,
            str(r.get("errors", 0)),
        )

    console.print(table)


def print_comparison_table(rows: list[dict[str, Any]], current_label: str, baseline_label: str) -> None:
    """Print a comparison table between two benchmark runs."""
    table = Table(title=f"Comparison: {current_label} vs {baseline_label}", show_lines=True)
    table.add_column("Target", style="cyan", no_wrap=True)
    table.add_column("Scenario", style="magenta")
    table.add_column("Conc", justify="right")
    table.add_column(f"RPS ({current_label})", justify="right")
    table.add_column(f"RPS ({baseline_label})", justify="right")
    table.add_column("RPS Delta", justify="right")
    table.add_column(f"P99 ({current_label})", justify="right")
    table.add_column(f"P99 ({baseline_label})", justify="right")
    table.add_column("P99 Delta", justify="right")

    for r in rows:
        rps_delta = r.get("rps_delta_pct", 0.0)
        p99_delta = r.get("p99_delta_pct", 0.0)

        table.add_row(
            r.get("target", ""),
            r.get("scenario", ""),
            str(r.get("concurrency", "")),
            f"{r.get('current_rps', 0.0):,.0f}",
            f"{r.get('baseline_rps', 0.0):,.0f}",
            _fmt_delta(rps_delta),
            f"{r.get('current_p99_ms', 0.0):.2f}",
            f"{r.get('baseline_p99_ms', 0.0):.2f}",
            _fmt_delta(-p99_delta),  # Lower latency is better, so invert sign
        )

    console.print(table)


def print_runs_table(runs: list[dict[str, Any]]) -> None:
    """Print a table of benchmark runs."""
    table = Table(title="Benchmark Runs", show_lines=True)
    table.add_column("Run ID", style="cyan", no_wrap=True)
    table.add_column("Branch", style="magenta")
    table.add_column("SHA", style="dim")
    table.add_column("OS/Python")
    table.add_column("Results", justify="right")
    table.add_column("Started At")
    table.add_column("Note")

    for r in runs:
        table.add_row(
            r.get("run_id", ""),
            r.get("git_branch", ""),
            r.get("git_sha", "")[:8] if r.get("git_sha") else "",
            f"{r.get('machine_os', '')}/{r.get('python_version', '')}",
            str(r.get("result_count", 0)),
            str(r.get("started_at", "")),
            r.get("note", ""),
        )

    console.print(table)
