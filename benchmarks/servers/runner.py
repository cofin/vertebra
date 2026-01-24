"""Benchmark orchestrator for ASGI server comparison.

Manages the lifecycle of benchmark execution:
1. Server startup with health check
2. Three-phase execution (primer → warmup → measure)
3. Result collection and aggregation
4. Server shutdown with cleanup

Usage:
    uv run python -m benchmarks.servers.runner
    uv run python -m benchmarks.servers.runner --quick
    uv run python -m benchmarks.servers.runner --servers vertebra,granian
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .config import (
    ALL_SERVERS,
    CATEGORY_1_SERVERS,
    CATEGORY_2_RUNTIMES,
    CATEGORY_3_FRAMEWORKS,
    BenchmarkCategory,
    get_available_servers,
)
from .parsers import OhaError, OhaNotFoundError, detect_oha_version, run_oha
from .reports import generate_markdown_report
from .results import BenchmarkResult, BenchmarkSuite, EnvironmentInfo
from .scenarios import Scenario, get_concurrency, get_durations, get_scenarios
from .utils import ServerStartupError, is_port_in_use, start_server, stop_server

if TYPE_CHECKING:
    import subprocess

    from .config import ServerConfig

# Default output directory
OUTPUT_DIR = Path(__file__).parent.parent / "results"

# Rich console for pretty output
console = Console()


class BenchmarkRunner:
    """Orchestrates benchmark execution across servers."""

    def __init__(
        self,
        servers: dict[str, ServerConfig] | None = None,
        scenarios: dict[str, Scenario] | None = None,
        concurrency: list[int] | None = None,
        output_dir: Path | None = None,
        quick: bool = False,
        verbose: bool = False,
        workers: int = 1,
        use_bench_venv: bool = True,
    ) -> None:
        self.servers = servers or ALL_SERVERS
        self.scenarios = scenarios or get_scenarios(quick=quick)
        self.concurrency = concurrency or get_concurrency(quick=quick)
        self.output_dir = output_dir or OUTPUT_DIR
        self.quick = quick
        self.verbose = verbose
        self.workers = workers
        self.use_bench_venv = use_bench_venv

        # Get durations
        self.primer_duration, self.warmup_duration, self.measure_duration = get_durations(quick=quick)

        # Detect environment
        oha_version = detect_oha_version()
        if oha_version is None:
            msg = "oha not found. Install with: cargo install oha"
            raise OhaNotFoundError(msg)

        self.suite = BenchmarkSuite(environment=EnvironmentInfo.detect(oha_version=oha_version))

        # Track current process for cleanup
        self._current_process: subprocess.Popen | None = None
        self._interrupted = False

        # Store results per category for table display
        self._category_results: dict[str, list[BenchmarkResult]] = {}

    def run(self) -> BenchmarkSuite:
        """Run all benchmarks."""
        original_sigint = signal.signal(signal.SIGINT, self._handle_interrupt)
        original_sigterm = signal.signal(signal.SIGTERM, self._handle_interrupt)

        try:
            self._print_header()

            for category, servers in [
                (BenchmarkCategory.SERVER, CATEGORY_1_SERVERS),
                (BenchmarkCategory.RUNTIME, CATEGORY_2_RUNTIMES),
                (BenchmarkCategory.FRAMEWORK, CATEGORY_3_FRAMEWORKS),
            ]:
                category_servers = {name: cfg for name, cfg in servers.items() if name in self.servers}
                if category_servers:
                    self._run_category(category, category_servers)

                if self._interrupted:
                    break

            self._print_final_summary()
            self._save_results()

        finally:
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            if self._current_process is not None:
                stop_server(self._current_process)

        return self.suite

    def _run_category(self, category: BenchmarkCategory, servers: dict[str, ServerConfig]) -> None:
        """Run benchmarks for a category of servers."""
        category_names = {
            BenchmarkCategory.SERVER: "ASGI Servers",
            BenchmarkCategory.RUNTIME: "Integrated Runtimes",
            BenchmarkCategory.FRAMEWORK: "Framework Overhead",
        }
        category_name = category_names[category]

        console.print()
        console.rule(f"[bold blue]{category_name}[/]", style="blue")

        self._category_results[category_name] = []

        for config in servers.values():
            if self._interrupted:
                break
            results = self._run_server(config)
            self._category_results[category_name].extend(results)

        # Print category results table
        if self._category_results[category_name]:
            self._print_category_table(category_name)

    def _run_server(self, config: ServerConfig) -> list[BenchmarkResult]:
        """Run all scenarios for a single server."""
        results = []

        # Check port
        if is_port_in_use(config.port):
            console.print(f"  [red]✗[/] {config.display_name}: Port {config.port} in use")
            self.suite.add_error(config.name, "*", f"Port {config.port} in use")
            return results

        # Start server with progress spinner
        with Progress(
            SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console, transient=True
        ) as progress:
            progress.add_task(f"Starting {config.display_name}...", total=None)

            try:
                self._current_process = start_server(
                    config, workers=self.workers, verbose=self.verbose, use_bench_venv=self.use_bench_venv
                )
            except ServerStartupError as e:
                console.print(f"  [red]✗[/] {config.display_name}: {e}")
                self.suite.add_error(config.name, "*", str(e))
                return results

        try:
            # Run scenarios - show inline progress
            for scenario in self.scenarios.values():
                if self._interrupted:
                    break

                console.print(f"  [dim]•[/] {config.display_name} → {scenario.name}", end=" ")
                result = self._run_scenario(config, scenario)

                if result:
                    results.append(result)
                    self.suite.add_result(result)
                    console.print(f"[green]{result.rps:,.0f} RPS[/]")
                else:
                    console.print("[red]FAILED[/]")

        finally:
            stop_server(self._current_process)
            self._current_process = None
            time.sleep(1.0)  # Cool-down

        return results

    def _run_scenario(self, config: ServerConfig, scenario: Scenario) -> BenchmarkResult | None:
        """Run a single scenario (first concurrency level only for quick display)."""
        url = scenario.get_url(config.port)
        concurrency = self.concurrency[0]  # Use first concurrency for table

        try:
            # Primer
            run_oha(
                url,
                duration_seconds=self.primer_duration,
                concurrency=min(concurrency, 8),
                method=scenario.method,
                body=scenario.body,
                headers=scenario.headers,
            )

            time.sleep(self.warmup_duration)

            # Measure
            oha_result = run_oha(
                url,
                duration_seconds=self.measure_duration,
                concurrency=concurrency,
                method=scenario.method,
                body=scenario.body,
                headers=scenario.headers,
            )

            return BenchmarkResult.from_oha(
                oha_result,
                server=config.name,
                server_display=config.display_name,
                category=config.category,
                scenario=scenario.name,
                concurrency=concurrency,
                duration_seconds=self.measure_duration,
            )

        except OhaError as e:
            self.suite.add_error(config.name, scenario.name, str(e), concurrency=concurrency)
            return None

    def _print_header(self) -> None:
        """Print benchmark header."""
        env = self.suite.environment

        info = Table.grid(padding=(0, 2))
        info.add_column(style="cyan", justify="right")
        info.add_column()

        info.add_row("Python", env.python_version)
        info.add_row("oha", env.oha_version)
        info.add_row("CPU", env.cpu)
        info.add_row("Workers", str(self.workers))
        info.add_row("Mode", "quick" if self.quick else "standard")
        info.add_row("Servers", str(len(self.servers)))
        info.add_row("Scenarios", str(len(self.scenarios)))

        console.print(Panel(info, title="[bold]ASGI Server Benchmarks[/]", border_style="blue"))

    def _print_category_table(self, category_name: str) -> None:
        """Print results table for a category."""
        results = self._category_results.get(category_name, [])
        if not results:
            return

        # Get unique scenarios
        scenarios = list(dict.fromkeys(r.scenario for r in results))

        # Build table
        table = Table(title=f"[bold]{category_name}[/] (c{self.concurrency[0]})", show_header=True, header_style="bold")
        table.add_column("Server", style="cyan", no_wrap=True)

        for scenario in scenarios:
            table.add_column(f"{scenario}\nRPS", justify="right")
            table.add_column("p99", justify="right", style="dim")

        # Group results by server
        servers = list(dict.fromkeys(r.server_display for r in results))

        for server in servers:
            row = [server]
            server_results = [r for r in results if r.server_display == server]

            for scenario in scenarios:
                scenario_result = next((r for r in server_results if r.scenario == scenario), None)
                if scenario_result:
                    rps = f"{scenario_result.rps:,.0f}"
                    p99 = f"{scenario_result.latency_p99_ms:.1f}ms"
                    row.extend([rps, p99])
                else:
                    row.extend(["—", "—"])

            table.add_row(*row)

        console.print()
        console.print(table)

    def _print_final_summary(self) -> None:
        """Print final summary with top performers."""
        console.print()
        console.rule("[bold green]Summary[/]", style="green")

        # Collect all results for ranking
        all_results = []
        for results in self._category_results.values():
            all_results.extend(results)

        if not all_results:
            console.print("[yellow]No results collected[/]")
            return

        # Top performers by scenario
        for scenario_name in dict.fromkeys(r.scenario for r in all_results):
            scenario_results = [r for r in all_results if r.scenario == scenario_name]
            scenario_results.sort(key=lambda r: -r.rps)

            table = Table(title=f"[bold]Top 5: {scenario_name}[/]", show_header=True, header_style="bold")
            table.add_column("#", style="dim", width=3)
            table.add_column("Server", style="cyan")
            table.add_column("RPS", justify="right", style="green")
            table.add_column("p99", justify="right")

            for i, r in enumerate(scenario_results[:5], 1):
                table.add_row(str(i), r.server_display, f"{r.rps:,.0f}", f"{r.latency_p99_ms:.2f}ms")

            console.print(table)
            console.print()

        # Errors
        if self.suite.errors:
            console.print(f"[red]Errors: {len(self.suite.errors)}[/]")
            for error in self.suite.errors[:5]:
                console.print(f"  [red]•[/] {error['server']}/{error['scenario']}: {error['error']}")

    def _save_results(self) -> None:
        """Save results to files."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        json_path = self.output_dir / "data.json"
        self.suite.save_json(json_path)

        report_path = self.output_dir / "report.md"
        report = generate_markdown_report(self.suite)
        report_path.write_text(report)

        console.print(f"\n[dim]Results saved to:[/] {self.output_dir}")

    def _handle_interrupt(self, signum: int, frame: object) -> None:
        """Handle interrupt signal."""
        if self._interrupted:
            console.print("\n[red]Forced exit[/]")
            sys.exit(1)

        console.print("\n[yellow]Interrupted, finishing...[/]")
        self._interrupted = True


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="ASGI Server Benchmarks")

    parser.add_argument("--servers", help="Comma-separated list of servers")
    parser.add_argument("--scenarios", help="Comma-separated scenarios, or 'all'")
    parser.add_argument("--quick", action="store_true", help="Quick mode (reduced duration)")
    parser.add_argument("--duration", type=int, help="Measurement duration in seconds")
    parser.add_argument("--concurrency", help="Comma-separated concurrency levels")
    parser.add_argument("--workers", type=int, default=1, help="Worker count")
    parser.add_argument("--output-dir", type=Path, help="Output directory")
    parser.add_argument("--verbose", action="store_true", help="Show server output")
    parser.add_argument("--no-venv", action="store_true", help="Don't use benchmark venv")
    parser.add_argument("--list-servers", action="store_true", help="List available servers")
    parser.add_argument("--list-scenarios", action="store_true", help="List available scenarios")

    args = parser.parse_args()

    if args.list_servers:
        available = get_available_servers()
        for name, cfg in ALL_SERVERS.items():
            status = "[green]✓[/]" if name in available else "[red]✗[/]"
            console.print(f"  {status} {name:30} {cfg.display_name}")
        return 0

    if args.list_scenarios:
        for name, scenario in get_scenarios().items():
            console.print(f"  {name:15} {scenario.description}")
        return 0

    # Parse servers
    servers = None
    if args.servers:
        server_names = [s.strip() for s in args.servers.split(",")]
        servers = {name: ALL_SERVERS[name] for name in server_names if name in ALL_SERVERS}

    # Parse scenarios
    scenarios = None
    if args.scenarios and args.scenarios != "all":
        scenario_names = [s.strip() for s in args.scenarios.split(",")]
        all_scenarios = get_scenarios()
        scenarios = {name: all_scenarios[name] for name in scenario_names if name in all_scenarios}

    # Parse concurrency
    concurrency = None
    if args.concurrency:
        concurrency = [int(c.strip()) for c in args.concurrency.split(",")]

    try:
        runner = BenchmarkRunner(
            servers=servers,
            scenarios=scenarios,
            concurrency=concurrency,
            output_dir=args.output_dir,
            quick=args.quick,
            verbose=args.verbose,
            workers=args.workers,
            use_bench_venv=not args.no_venv,
        )
        runner.run()
        return 0

    except OhaNotFoundError as e:
        console.print(f"[red]Error:[/] {e}")
        return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Aborted[/]")
        return 130


if __name__ == "__main__":
    sys.exit(main())
