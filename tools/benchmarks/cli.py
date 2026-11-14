"""Benchmark CLI built with rich-click."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import rich_click as click
from rich.console import Console
from rich.table import Table

from tools.benchmarks.reports import print_comparison_table, print_runs_table, print_summary_table
from tools.benchmarks.runner import OhaRunner, ServerRunner, build_url, expand_scenario_runs
from tools.benchmarks.scenarios import (
    DEFAULT_SCENARIOS,
    FULL_SCENARIOS,
    MULTIWORKER_SCENARIOS,
    QUICK_SCENARIOS,
    SCENARIOS,
)
from tools.benchmarks.store import BenchmarkStore
from tools.benchmarks.targets import (
    ALL_TARGETS,
    BASELINE_TARGETS,
    FREETHREADED_TARGETS,
    VERTEBRA_TARGETS,
)

console = Console()
_RESULTS_DIR = Path(__file__).parent / "results"


@click.group()
def cli() -> None:
    """Vertebra benchmark suite."""


@cli.command()
@click.option(
    "-s", "--scenarios", "scenario_preset", default="default", help="Scenario preset or comma-separated names"
)
@click.option("-t", "--targets", "target_spec", default="vtb", help="Target spec: all, vtb, baselines, or name")
@click.option("-d", "--duration", default=10, type=int, help="Duration per scenario in seconds")
@click.option("-c", "--concurrency", default=128, type=int, help="Default concurrency level")
@click.option("-w", "--warmup", default=2, type=int, help="Warmup duration in seconds")
@click.option("-o", "--output", "output_path", default=None, type=click.Path(), help="JSONL output file path")
@click.option("--note", default="", help="Note to attach to the benchmark run")
def run(
    scenario_preset: str,
    target_spec: str,
    duration: int,
    concurrency: int,
    warmup: int,
    output_path: str | None,
    note: str,
) -> None:
    """Run benchmarks against server targets."""
    scenario_names = _resolve_scenarios(scenario_preset)
    target_names = _resolve_targets(target_spec)

    if output_path is None:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(_RESULTS_DIR / "benchmarks.jsonl")

    results: list[dict[str, Any]] = []
    jsonl_path = Path(output_path)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    for target_name in target_names:
        target = ALL_TARGETS[target_name]
        console.print(f"\n[bold cyan]>>> Target: {target.name}[/bold cyan] - {target.description}")

        try:
            with ServerRunner(cmd=target.cmd, host=target.host, port=target.port, env=target.env or None):
                for scenario_name in scenario_names:
                    scenario = SCENARIOS[scenario_name]
                    conc_levels = expand_scenario_runs(scenario.concurrency_levels, concurrency)

                    for conc in conc_levels:
                        url = build_url(target.host, target.port, scenario.path)
                        console.print(f"  [dim]{scenario_name}[/dim] c={conc} ({scenario.method} {scenario.path})")

                        if warmup > 0:
                            warmup_runner = OhaRunner(
                                url=url,
                                duration=warmup,
                                concurrency=conc,
                                method=scenario.method,
                                body=scenario.body,
                            )
                            try:
                                warmup_runner.run()
                            except Exception:
                                console.print("    [yellow]Warmup failed, continuing...[/yellow]")

                        runner = OhaRunner(
                            url=url, duration=duration, concurrency=conc, method=scenario.method, body=scenario.body
                        )
                        try:
                            raw_result = runner.run()
                        except Exception as exc:
                            console.print(f"    [red]Error: {exc}[/red]")
                            continue

                        timestamp = datetime.now(tz=UTC).isoformat()
                        result_row: dict[str, Any] = {
                            "target": target.name,
                            "topology": target.topology,
                            "scenario": scenario_name,
                            "concurrency": conc,
                            "duration": duration,
                            "url": url,
                            "timestamp": timestamp,
                            **raw_result,
                        }
                        results.append(result_row)

                        with jsonl_path.open("a") as f:
                            f.write(json.dumps(result_row) + "\n")

                        console.print(
                            f"    RPS: {raw_result['rps']:,.0f}  "
                            f"Avg: {raw_result['latency_mean_s'] * 1000:.2f}ms  "
                            f"P99: {raw_result['latency_p99_s'] * 1000:.2f}ms  "
                            f"Err: {raw_result['errors']}"
                        )

        except (TimeoutError, RuntimeError) as exc:
            console.print(f"  [red]Server startup failed: {exc}[/red]")
            continue
        finally:
            time.sleep(2.0)

    if results:
        console.print()
        print_summary_table(results)

        try:
            store = BenchmarkStore()
            run_id = store.create_run(note=note)
            for r in results:
                store.insert_result(run_id, r)
            store.close()
            console.print(f"\n[green]Results stored in DuckDB (run_id: {run_id})[/green]")
        except ImportError:
            console.print("\n[dim]DuckDB not available -- results saved to JSONL only[/dim]")
    else:
        console.print("[yellow]No results collected.[/yellow]")


@cli.command("list-scenarios")
def list_scenarios() -> None:
    """List available benchmark scenarios."""
    table = Table(title="Available Scenarios", show_lines=True)
    table.add_column("Name", style="cyan")
    table.add_column("Path", style="green")
    table.add_column("Method")
    table.add_column("Body")
    table.add_column("Description")

    for name, scenario in SCENARIOS.items():
        table.add_row(
            name,
            scenario.path,
            scenario.method,
            f"{len(scenario.body)} bytes" if scenario.body else "-",
            scenario.description,
        )

    console.print(table)
    console.print(f"\nPresets: quick={QUICK_SCENARIOS}, default={DEFAULT_SCENARIOS}")


@cli.command("list-targets")
def list_targets() -> None:
    """List available benchmark targets."""
    table = Table(title="Available Targets", show_lines=True)
    table.add_column("Name", style="cyan")
    table.add_column("Topology", style="yellow")
    table.add_column("Category", style="magenta")
    table.add_column("Port", justify="right")
    table.add_column("Description")

    for name, target in ALL_TARGETS.items():
        table.add_row(name, target.topology, target.category, str(target.port), target.description)

    console.print(table)


@cli.command()
@click.argument("jsonl_path", type=click.Path(exists=True))
@click.option("--note", default="", help="Note to attach to the ingested run")
def ingest(jsonl_path: str, note: str) -> None:
    """Ingest a JSONL results file into DuckDB."""
    store = BenchmarkStore()
    run_id = store.ingest_jsonl(jsonl_path, note=note)
    store.close()
    console.print(f"[green]Ingested into run_id: {run_id}[/green]")


@cli.command()
@click.option("--current", "current_id", default=None, help="Current run ID (default: latest)")
@click.option("--baseline", "baseline_id", default=None, help="Baseline run ID (default: previous)")
def compare(current_id: str | None, baseline_id: str | None) -> None:
    """Compare two benchmark runs."""
    store = BenchmarkStore()

    if current_id is None:
        current_id = store.get_latest_run_id()
        if current_id is None:
            console.print("[red]No benchmark runs found.[/red]")
            return

    if baseline_id is None:
        baseline_id = store.get_previous_run_id(current_id)
        if baseline_id is None:
            console.print("[red]No previous run found to compare against.[/red]")
            return

    rows = store.compare(current_id, baseline_id)
    store.close()

    if not rows:
        console.print("[yellow]No comparable results found between these runs.[/yellow]")
        return

    print_comparison_table(rows, current_id, baseline_id)


@cli.command()
@click.option("--run-id", default=None, help="Run ID to report (default: latest)")
def report(run_id: str | None) -> None:
    """Show results for a benchmark run."""
    store = BenchmarkStore()

    if run_id is None:
        run_id = store.get_latest_run_id()
        if run_id is None:
            console.print("[red]No benchmark runs found.[/red]")
            return

    results = store.get_run_results(run_id)
    store.close()

    if not results:
        console.print(f"[yellow]No results found for run {run_id}.[/yellow]")
        return

    console.print(f"[bold]Run: {run_id}[/bold]\n")
    print_summary_table(results)


@cli.command()
@click.option("-n", "--limit", default=20, type=int, help="Number of runs to show")
def runs(limit: int) -> None:
    """List benchmark runs stored in DuckDB."""
    store = BenchmarkStore()
    run_list = store.list_runs(limit=limit)
    store.close()

    if not run_list:
        console.print("[yellow]No benchmark runs found.[/yellow]")
        return

    print_runs_table(run_list)


def _resolve_scenarios(preset: str) -> list[str]:
    """Resolve a scenario preset or comma-separated names to a list of scenario keys."""
    presets: dict[str, list[str]] = {
        "quick": QUICK_SCENARIOS,
        "default": DEFAULT_SCENARIOS,
        "full": FULL_SCENARIOS,
        "multiworker": MULTIWORKER_SCENARIOS,
    }
    if preset in presets:
        return presets[preset]
    names = [s.strip() for s in preset.split(",")]
    for name in names:
        if name not in SCENARIOS:
            msg = f"Unknown scenario: {name}. Available: {', '.join(SCENARIOS)}"
            raise click.BadParameter(msg)
    return names


def _resolve_targets(target_spec: str) -> list[str]:
    """Resolve a target spec or comma-separated target names to a list of target keys."""
    if target_spec == "all":
        return list(ALL_TARGETS)
    if target_spec == "vtb":
        return VERTEBRA_TARGETS
    if target_spec == "baselines":
        return BASELINE_TARGETS
    if target_spec == "freethreaded":
        return FREETHREADED_TARGETS
    names = [t.strip() for t in target_spec.split(",")]
    for name in names:
        if name not in ALL_TARGETS:
            msg = f"Unknown target: {name}. Available: {', '.join(ALL_TARGETS)}"
            raise click.BadParameter(msg)
    return names
