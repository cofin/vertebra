"""Benchmark CLI built with rich-click."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import rich_click as click
from rich.console import Console
from rich.table import Table

from tools.benchmarks.drivers import LoadConfig, create_driver
from tools.benchmarks.reports import print_comparison_table, print_runs_table, print_summary_table
from tools.benchmarks.runner import (
    BrokerHookConfig,
    BrokerHostHook,
    OhaRunner,
    ServerRunner,
    build_url,
    expand_scenario_runs,
)
from tools.benchmarks.scenarios import (
    DATA_SCENARIOS,
    DEFAULT_SCENARIOS,
    FULL_SCENARIOS,
    GRPC_NATIVE_SCENARIOS,
    GRPC_SCENARIOS,
    MULTIWORKER_SCENARIOS,
    QUICK_SCENARIOS,
    SCENARIOS,
    STORAGE_SCENARIOS,
)
from tools.benchmarks.store import BenchmarkStore
from tools.benchmarks.targets import (
    ALL_TARGETS,
    BASELINE_TARGETS,
    DATA_BASELINE_TARGETS,
    DATA_FREETHREADED_TARGETS,
    DATA_TARGETS,
    FRAMEWORK_TARGETS,
    FREETHREADED_TARGETS,
    GRPC_TARGETS,
    VERTEBRA_TARGETS,
    Topology,
)

if TYPE_CHECKING:
    from tools.benchmarks.runner import ServerLifecycleHook
    from tools.benchmarks.scenarios import Scenario
    from tools.benchmarks.targets import Target

console = Console()
_RESULTS_DIR = Path(__file__).parent / "results"
_PROTOCOL_CHOICES = click.Choice(["http1", "http2", "grpc"], case_sensitive=False)
_TOPOLOGY_CHOICES = click.Choice(["embedded", "shm", "free-threaded", "baseline"], case_sensitive=False)
_BROKER_MODE_CHOICES = click.Choice(["none", "per_worker", "shm_broker"], case_sensitive=False)
_LOAD_MODE_CHOICES = click.Choice(["closed_loop", "open_loop"], case_sensitive=False)


def filter_targets(
    target_names: list[str],
    *,
    protocol: str | None = None,
    topology: str | None = None,
    adapter: str | None = None,
    broker_mode: str | None = None,
) -> list[str]:
    """Filter resolved target keys by Target metadata (case-insensitive) preserving order."""
    protocol_norm = protocol.strip().lower() if protocol is not None else None
    topology_norm = topology.strip().lower() if topology is not None else None
    adapter_norm = adapter.strip().lower() if adapter is not None else None
    broker_mode_norm = broker_mode.strip().lower() if broker_mode is not None else None

    matched: list[str] = []
    for name in target_names:
        target = ALL_TARGETS.get(name)
        if target is None:
            continue
        if protocol_norm is not None and target.protocol.lower() != protocol_norm:
            continue
        if topology_norm is not None and str(target.topology).lower() != topology_norm:
            continue
        if adapter_norm is not None and target.adapter.lower() != adapter_norm:
            continue
        if broker_mode_norm is not None and target.broker_mode.lower() != broker_mode_norm:
            continue
        matched.append(name)
    return matched


def _build_target_hooks(target: Target) -> list[ServerLifecycleHook]:
    """Build ServerRunner lifecycle hooks required for a target."""
    if target.broker_mode.lower() != "shm_broker":
        return []
    shm_name = f"vertebra-bench-{target.name}-{target.port}"
    pool_size = target.pool_size if target.pool_size is not None else 40
    return [BrokerHostHook(BrokerHookConfig(shm_name=shm_name, pool_size=pool_size))]


def _select_driver(target: Target, scenario: Scenario) -> str:
    """Select the load driver name for a target and scenario pair."""
    if scenario.driver != "oha":
        return scenario.driver
    if target.protocol.lower() == "grpc" or scenario.protocol.lower() == "grpc":
        return "ghz"
    if target.protocol.lower() == "http2" or scenario.protocol.lower() == "http2":
        return "h2load"
    return "oha"


def _build_load_config(
    target: Target,
    scenario: Scenario,
    *,
    driver_name: str,
    duration: int,
    concurrency: int,
    rate: float | None,
) -> tuple[str, LoadConfig]:
    """Build target URL and LoadConfig for the chosen load driver."""
    if driver_name == "ghz":
        url = f"grpc://{target.host}:{target.port}"
        config = LoadConfig(
            url=url,
            duration=duration,
            concurrency=concurrency,
            method=scenario.method,
            rate=rate,
            grpc_call=scenario.grpc_method,
            grpc_data=scenario.body,
        )
        return url, config

    url = build_url(target.host, target.port, scenario.path)
    config = LoadConfig(
        url=url,
        duration=duration,
        concurrency=concurrency,
        method=scenario.method,
        body=scenario.body,
        rate=rate,
    )
    return url, config


def _run_load(driver_name: str, config: LoadConfig) -> dict[str, Any]:
    """Execute a load test using OhaRunner or the matching load driver."""
    if driver_name == "oha":
        runner = OhaRunner(
            url=config.url,
            duration=config.duration,
            concurrency=config.concurrency,
            method=config.method,
            body=config.body,
            rate=config.rate,
        )
        return runner.run()
    driver = create_driver(driver_name)
    return driver.run(config)


@click.group()
def cli() -> None:
    """Vertebra benchmark suite."""


@cli.command()
@click.option(
    "-s", "--scenarios", "scenario_preset", default="default", help="Scenario preset or comma-separated names"
)
@click.option(
    "-t",
    "--targets",
    "target_spec",
    default="vtb",
    help="Target spec: all, vtb, baselines, freethreaded, data, data-baselines, frameworks, grpc, or names",
)
@click.option("-d", "--duration", default=10, type=int, help="Duration per scenario in seconds")
@click.option("-c", "--concurrency", default=128, type=int, help="Default concurrency level")
@click.option("-w", "--warmup", default=2, type=int, help="Warmup duration in seconds")
@click.option("-o", "--output", "output_path", default=None, type=click.Path(), help="JSONL output file path")
@click.option("--note", default="", help="Note to attach to the benchmark run")
@click.option("--protocol", default=None, type=_PROTOCOL_CHOICES, help="Filter targets by protocol")
@click.option("--topology", default=None, type=_TOPOLOGY_CHOICES, help="Filter targets by runtime topology")
@click.option("--adapter", default=None, type=str, help="Filter targets by adapter")
@click.option("--broker-mode", default=None, type=_BROKER_MODE_CHOICES, help="Filter targets by broker mode")
@click.option("--load-mode", default="closed_loop", type=_LOAD_MODE_CHOICES, help="Load generation mode")
@click.option("--rate", default=None, type=float, help="Target requests per second for open-loop runs")
def run(
    scenario_preset: str,
    target_spec: str,
    duration: int,
    concurrency: int,
    warmup: int,
    output_path: str | None,
    note: str,
    protocol: str | None,
    topology: str | None,
    adapter: str | None,
    broker_mode: str | None,
    load_mode: str,
    rate: float | None,
) -> None:
    """Run benchmarks against server targets."""
    normalized_load_mode = load_mode.strip().lower()
    if rate is not None and rate <= 0:
        msg = "--rate must be greater than 0"
        raise click.BadParameter(msg, param_hint="'--rate'")
    if normalized_load_mode == "open_loop" and rate is None:
        msg = "--rate is required when --load-mode is open_loop"
        raise click.BadParameter(msg, param_hint="'--rate'")

    effective_rate = rate if normalized_load_mode == "open_loop" else None
    scenario_names = _resolve_scenarios(scenario_preset)
    target_names = filter_targets(
        _resolve_targets(target_spec),
        protocol=protocol,
        topology=topology,
        adapter=adapter,
        broker_mode=broker_mode,
    )

    if output_path is None:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        output_path = str(_RESULTS_DIR / "benchmarks.jsonl")

    results: list[dict[str, Any]] = []
    jsonl_path = Path(output_path)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    for target_name in target_names:
        target = ALL_TARGETS[target_name]
        hooks = _build_target_hooks(target)
        console.print(f"\n[bold cyan]>>> Target: {target.name}[/bold cyan] - {target.description}")

        try:
            with ServerRunner(
                cmd=target.cmd,
                host=target.host,
                port=target.port,
                env=target.env or None,
                hooks=hooks or None,
            ):
                for scenario_name in scenario_names:
                    scenario = SCENARIOS[scenario_name]
                    driver_name = _select_driver(target, scenario)
                    conc_levels = expand_scenario_runs(scenario.concurrency_levels, concurrency)

                    for conc in conc_levels:
                        url, config = _build_load_config(
                            target,
                            scenario,
                            driver_name=driver_name,
                            duration=duration,
                            concurrency=conc,
                            rate=effective_rate,
                        )
                        console.print(f"  [dim]{scenario_name}[/dim] c={conc} ({scenario.method} {scenario.path})")

                        if warmup > 0:
                            _, warmup_config = _build_load_config(
                                target,
                                scenario,
                                driver_name=driver_name,
                                duration=warmup,
                                concurrency=conc,
                                rate=effective_rate,
                            )
                            try:
                                _run_load(driver_name, warmup_config)
                            except Exception:
                                console.print("    [yellow]Warmup failed, continuing...[/yellow]")

                        try:
                            raw_result = _run_load(driver_name, config)
                        except Exception as exc:
                            console.print(f"    [red]Error: {exc}[/red]")
                            continue

                        timestamp = datetime.now(tz=UTC).isoformat()
                        result_row: dict[str, Any] = {
                            "target": target.name,
                            "topology": str(target.topology),
                            "scenario": scenario_name,
                            "concurrency": conc,
                            "duration": duration,
                            "url": url,
                            "timestamp": timestamp,
                            **raw_result,
                            "adapter": target.adapter,
                            "protocol": target.protocol,
                            "transport": target.transport,
                            "broker_mode": target.broker_mode,
                            "load_mode": normalized_load_mode,
                            "target_rps": effective_rate,
                            "pool_size": target.pool_size,
                            "gil_enabled": target.topology != Topology.FREE_THREADED,
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
    console.print(
        f"\nPresets: quick={QUICK_SCENARIOS}, default={DEFAULT_SCENARIOS}, "
        f"data={DATA_SCENARIOS}, storage={STORAGE_SCENARIOS}, "
        f"grpc={GRPC_SCENARIOS}, grpc-native={GRPC_NATIVE_SCENARIOS}"
    )


@cli.command("list-targets")
@click.option("--protocol", default=None, type=_PROTOCOL_CHOICES, help="Filter targets by protocol")
@click.option("--topology", default=None, type=_TOPOLOGY_CHOICES, help="Filter targets by runtime topology")
@click.option("--adapter", default=None, type=str, help="Filter targets by adapter")
@click.option("--broker-mode", default=None, type=_BROKER_MODE_CHOICES, help="Filter targets by broker mode")
def list_targets(
    protocol: str | None,
    topology: str | None,
    adapter: str | None,
    broker_mode: str | None,
) -> None:
    """List available benchmark targets."""
    table = Table(title="Available Targets", show_lines=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Topology", style="yellow")
    table.add_column("Category", style="magenta")
    table.add_column("Protocol", style="green")
    table.add_column("Transport")
    table.add_column("Broker")
    table.add_column("Port", justify="right")
    table.add_column("Description")

    target_names = filter_targets(
        list(ALL_TARGETS),
        protocol=protocol,
        topology=topology,
        adapter=adapter,
        broker_mode=broker_mode,
    )
    for name in target_names:
        target = ALL_TARGETS[name]
        table.add_row(
            name,
            str(target.topology),
            target.category,
            target.protocol,
            target.transport,
            target.broker_mode,
            str(target.port),
            target.description,
        )

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
        "data": DATA_SCENARIOS,
        "storage": STORAGE_SCENARIOS,
        "grpc": GRPC_SCENARIOS,
        "grpc-native": GRPC_NATIVE_SCENARIOS,
    }
    if preset in presets:
        return list(presets[preset])
    names = [s.strip() for s in preset.split(",")]
    for name in names:
        if name not in SCENARIOS:
            msg = f"Unknown scenario: {name}. Available: {', '.join(SCENARIOS)}"
            raise click.BadParameter(msg)
    return names


def _resolve_targets(target_spec: str) -> list[str]:
    """Resolve a target spec or comma-separated target names to a list of target keys."""
    presets: dict[str, list[str]] = {
        "all": list(ALL_TARGETS),
        "vtb": VERTEBRA_TARGETS,
        "baselines": BASELINE_TARGETS,
        "freethreaded": FREETHREADED_TARGETS,
        "data": DATA_TARGETS,
        "data-baselines": DATA_BASELINE_TARGETS,
        "data-freethreaded": DATA_FREETHREADED_TARGETS,
        "frameworks": FRAMEWORK_TARGETS,
        "grpc": GRPC_TARGETS,
    }
    if target_spec in presets:
        return list(presets[target_spec])
    names = [t.strip() for t in target_spec.split(",")]
    for name in names:
        if name not in ALL_TARGETS:
            msg = f"Unknown target: {name}. Available: {', '.join(ALL_TARGETS)}"
            raise click.BadParameter(msg)
    return names
