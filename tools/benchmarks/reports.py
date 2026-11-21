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


def _has_matrix_dimensions(rows: list[dict[str, Any]]) -> bool:
    """Return True when any benchmark row contains non-default matrix dimensions."""
    for row in rows:
        if row.get("adapter") not in (None, "none"):
            return True
        if row.get("protocol") not in (None, "http1"):
            return True
        if row.get("transport") not in (None, "native"):
            return True
        if row.get("broker_mode") not in (None, "none"):
            return True
        if row.get("load_mode") not in (None, "closed_loop"):
            return True
        if row.get("target_rps") is not None:
            return True
        if row.get("gil_enabled") is False:
            return True
    return False


def _fmt_delta(pct: float | None) -> str:
    """Format a delta percentage with color (green=improvement, red=regression)."""
    if pct is None:
        return "-"
    if abs(pct) < _DELTA_SIGNIFICANT_PCT:
        return f"{pct:+.1f}%"
    if pct > 0:
        return f"[green]{pct:+.1f}%[/green]"
    return f"[red]{pct:+.1f}%[/red]"


def _fmt_load(load_mode: str, target_rps: float | None) -> str:
    """Format load mode and optional open-loop target RPS for table display."""
    if load_mode == "open_loop":
        if target_rps is not None:
            return f"open@{target_rps:,.0f}"
        return "open"
    return "closed"


def _fmt_gil(gil_enabled: bool | None) -> str:
    """Format GIL enabled state for table display."""
    if gil_enabled is False:
        return "off"
    if gil_enabled is True:
        return "on"
    return "-"


def _fmt_optional_rps(value: float | None) -> str:
    """Format an optional RPS figure with thousands separators or a dash."""
    if value is None:
        return "-"
    return f"{value:,.0f}"


def _fmt_optional_ms(value: float | None) -> str:
    """Format an optional millisecond latency with 2 decimal places or a dash."""
    if value is None:
        return "-"
    return f"{value:.2f}"


def print_summary_table(
    results: list[dict[str, Any]],
    *,
    show_matrix: bool | None = None,
    output_console: Console | None = None,
) -> None:
    """Print a Rich table summarizing both live (seconds) and stored DuckDB (ms) results."""
    target_console = output_console if output_console is not None else console
    include_matrix = _has_matrix_dimensions(results) if show_matrix is None else show_matrix

    table = Table(title="Benchmark Results", show_lines=True)
    table.add_column("Target", style="cyan", no_wrap=True)
    table.add_column("Topology", style="yellow")
    table.add_column("Scenario", style="magenta")
    if include_matrix:
        table.add_column("Protocol", style="blue")
        table.add_column("Adapter", style="green")
        table.add_column("Broker", style="cyan")
        table.add_column("Load")
        table.add_column("GIL", justify="center")
    table.add_column("Conc", justify="right")
    table.add_column("RPS", justify="right")
    table.add_column("Avg (ms)", justify="right")
    table.add_column("P99 (ms)", justify="right")
    table.add_column("Errors", justify="right")

    for r in results:
        if "latency_mean_ms" in r:
            avg_ms = f"{r['latency_mean_ms']:.2f}"
            p99_ms = f"{r['latency_p99_ms']:.2f}"
        else:
            avg_ms = _fmt_ms(r.get("latency_mean_s", 0.0))
            p99_ms = _fmt_ms(r.get("latency_p99_s", 0.0))

        row_values: list[str] = [
            str(r.get("target", "")),
            str(r.get("topology", "unknown")),
            str(r.get("scenario", "")),
        ]
        if include_matrix:
            row_values.extend(
                [
                    str(r.get("protocol") or "http1"),
                    str(r.get("adapter") or "none"),
                    str(r.get("broker_mode") or "none"),
                    _fmt_load(str(r.get("load_mode") or "closed_loop"), r.get("target_rps")),
                    _fmt_gil(r.get("gil_enabled")),
                ]
            )
        row_values.extend(
            [
                str(r.get("concurrency", "")),
                _fmt_rps(r.get("rps", 0.0)),
                avg_ms,
                p99_ms,
                str(r.get("errors", 0)),
            ]
        )
        table.add_row(*row_values)

    target_console.print(table)


def print_comparison_table(
    rows: list[dict[str, Any]],
    current_label: str,
    baseline_label: str,
    *,
    show_matrix: bool | None = None,
    output_console: Console | None = None,
) -> None:
    """Print a comparison table between two benchmark runs, inverting P99 delta sign."""
    target_console = output_console if output_console is not None else console
    include_matrix = _has_matrix_dimensions(rows) if show_matrix is None else show_matrix

    table = Table(title=f"Comparison: {current_label} vs {baseline_label}", show_lines=True)
    table.add_column("Target", style="cyan", no_wrap=True)
    table.add_column("Scenario", style="magenta")
    if include_matrix:
        table.add_column("Protocol", style="blue")
        table.add_column("Adapter", style="green")
        table.add_column("Broker", style="cyan")
        table.add_column("Load")
    table.add_column("Conc", justify="right")
    table.add_column(f"RPS ({current_label})", justify="right")
    table.add_column(f"RPS ({baseline_label})", justify="right")
    table.add_column("RPS Delta", justify="right")
    table.add_column(f"P99 ({current_label})", justify="right")
    table.add_column(f"P99 ({baseline_label})", justify="right")
    table.add_column("P99 Delta", justify="right")

    for r in rows:
        rps_delta = r.get("rps_delta_pct")
        p99_delta = r.get("p99_delta_pct")
        inverted_p99_delta = -p99_delta if p99_delta is not None else None

        row_values: list[str] = [
            str(r.get("target", "")),
            str(r.get("scenario", "")),
        ]
        if include_matrix:
            row_values.extend(
                [
                    str(r.get("protocol") or "http1"),
                    str(r.get("adapter") or "none"),
                    str(r.get("broker_mode") or "none"),
                    _fmt_load(str(r.get("load_mode") or "closed_loop"), r.get("target_rps")),
                ]
            )
        row_values.extend(
            [
                str(r.get("concurrency", "")),
                _fmt_optional_rps(r.get("current_rps")),
                _fmt_optional_rps(r.get("baseline_rps")),
                _fmt_delta(rps_delta),
                _fmt_optional_ms(r.get("current_p99_ms")),
                _fmt_optional_ms(r.get("baseline_p99_ms")),
                _fmt_delta(inverted_p99_delta),
            ]
        )
        table.add_row(*row_values)

    target_console.print(table)


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
