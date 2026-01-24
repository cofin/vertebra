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
            msg = "oha not found. Install with: cargo install oha\nOr via package manager: brew install oha (macOS)"
            raise OhaNotFoundError(msg)

        self.suite = BenchmarkSuite(environment=EnvironmentInfo.detect(oha_version=oha_version))

        # Track current process for cleanup
        self._current_process: subprocess.Popen | None = None
        self._interrupted = False

    def run(self) -> BenchmarkSuite:
        """Run all benchmarks.

        Returns:
            BenchmarkSuite with all results
        """
        # Set up signal handler for graceful shutdown
        original_sigint = signal.signal(signal.SIGINT, self._handle_interrupt)
        original_sigterm = signal.signal(signal.SIGTERM, self._handle_interrupt)

        try:
            self._print_header()

            # Run benchmarks by category for organized output
            for category, servers in [
                (BenchmarkCategory.SERVER, CATEGORY_1_SERVERS),
                (BenchmarkCategory.RUNTIME, CATEGORY_2_RUNTIMES),
                (BenchmarkCategory.FRAMEWORK, CATEGORY_3_FRAMEWORKS),
            ]:
                # Filter to requested servers
                category_servers = {name: cfg for name, cfg in servers.items() if name in self.servers}

                if category_servers:
                    self._run_category(category, category_servers)

                if self._interrupted:
                    break

            self._print_summary()
            self._save_results()

        finally:
            # Restore signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

            # Ensure cleanup
            if self._current_process is not None:
                stop_server(self._current_process)

        return self.suite

    def _run_category(self, category: BenchmarkCategory, servers: dict[str, ServerConfig]) -> None:
        """Run benchmarks for a category of servers."""
        category_name = {
            BenchmarkCategory.SERVER: "Category 1: Raw ASGI Servers",
            BenchmarkCategory.RUNTIME: "Category 2: Integrated Runtimes",
            BenchmarkCategory.FRAMEWORK: "Category 3: Framework Overhead",
        }[category]

        print(f"\n{'=' * 60}")
        print(f"  {category_name}")
        print(f"{'=' * 60}")

        for config in servers.values():
            if self._interrupted:
                break
            self._run_server(config)

    def _run_server(self, config: ServerConfig) -> None:
        """Run all scenarios for a single server."""
        print(f"\n[{config.display_name}]")
        print(f"  Port: {config.port}")
        if config.notes:
            print(f"  Notes: {config.notes}")

        # Check if port is already in use
        if is_port_in_use(config.port):
            print(f"  ERROR: Port {config.port} is already in use")
            self.suite.add_error(config.name, "*", f"Port {config.port} in use")
            return

        # Start server
        try:
            print("  Starting server...", end="", flush=True)
            self._current_process = start_server(
                config, workers=self.workers, verbose=self.verbose, use_bench_venv=self.use_bench_venv
            )
            print(" OK")
        except ServerStartupError as e:
            print(" FAILED")
            print(f"    {e}")
            self.suite.add_error(config.name, "*", str(e))
            return

        try:
            # Run all scenarios
            for scenario in self.scenarios.values():
                if self._interrupted:
                    break
                self._run_scenario(config, scenario)

        finally:
            # Stop server with cool-down
            print("  Stopping server...", end="", flush=True)
            stop_server(self._current_process)
            self._current_process = None
            print(" OK")

            # Cool-down between servers
            if not self._interrupted:
                time.sleep(2.0)

    def _run_scenario(self, config: ServerConfig, scenario: Scenario) -> None:
        """Run a single scenario at all concurrency levels."""
        print(f"\n  Scenario: {scenario.name}")
        print(f"    {scenario.description}")

        url = scenario.get_url(config.port)

        for concurrency in self.concurrency:
            if self._interrupted:
                break

            print(f"    c{concurrency}: ", end="", flush=True)

            try:
                # Phase 1: Primer (warm up caches, JIT, etc.)
                run_oha(
                    url,
                    duration_seconds=self.primer_duration,
                    concurrency=min(concurrency, 8),  # Lower concurrency for primer
                    method=scenario.method,
                    body=scenario.body,
                    headers=scenario.headers,
                )

                # Phase 2: Warmup (stabilize)
                time.sleep(self.warmup_duration)

                # Phase 3: Measure
                oha_result = run_oha(
                    url,
                    duration_seconds=self.measure_duration,
                    concurrency=concurrency,
                    method=scenario.method,
                    body=scenario.body,
                    headers=scenario.headers,
                )

                result = BenchmarkResult.from_oha(
                    oha_result,
                    server=config.name,
                    server_display=config.display_name,
                    category=config.category,
                    scenario=scenario.name,
                    concurrency=concurrency,
                    duration_seconds=self.measure_duration,
                )

                self.suite.add_result(result)

                # Print result
                status = "OK" if result.passes_sanity_check else "WARN"
                print(f"{result.rps:>10,.0f} RPS | p99: {result.latency_p99_ms:>6.2f}ms | [{status}]")

            except OhaError as e:
                print(f"ERROR: {e}")
                self.suite.add_error(config.name, scenario.name, str(e), concurrency=concurrency)

    def _print_header(self) -> None:
        """Print benchmark header."""
        print("=" * 60)
        print("  ASGI Server Benchmarks")
        print("=" * 60)
        print(f"  Python: {self.suite.environment.python_version}")
        print(f"  oha: {self.suite.environment.oha_version}")
        print(f"  CPU: {self.suite.environment.cpu}")
        print(f"  Workers: {self.workers}")
        print(f"  Mode: {'quick' if self.quick else 'standard'}")
        print(f"  Servers: {len(self.servers)}")
        print(f"  Scenarios: {len(self.scenarios)}")
        print(f"  Concurrency: {', '.join(map(str, self.concurrency))}")

    def _print_summary(self) -> None:
        """Print benchmark summary."""
        print("\n" + "=" * 60)
        print("  Summary")
        print("=" * 60)

        # Group by scenario
        for scenario_name in self.suite.get_scenarios():
            results = self.suite.get_results_by_scenario(scenario_name)
            if not results:
                continue

            print(f"\n  {scenario_name}:")

            # Sort by RPS (highest first) for each concurrency level
            for concurrency in sorted({r.concurrency for r in results}):
                concurrency_results = [r for r in results if r.concurrency == concurrency]
                concurrency_results.sort(key=lambda r: -r.rps)

                print(f"    c{concurrency}:")
                for r in concurrency_results[:5]:  # Top 5
                    print(f"      {r.server_display:25} {r.rps:>10,.0f} RPS | p99: {r.latency_p99_ms:>6.2f}ms")

        if self.suite.errors:
            print(f"\n  Errors: {len(self.suite.errors)}")
            for error in self.suite.errors[:5]:
                print(f"    - {error['server']}/{error['scenario']}: {error['error']}")

        print()

    def _save_results(self) -> None:
        """Save results to files."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Save JSON
        json_path = self.output_dir / "data.json"
        self.suite.save_json(json_path)
        print(f"Results saved to: {json_path}")

        # Save markdown report
        report_path = self.output_dir / "report.md"
        report = generate_markdown_report(self.suite)
        report_path.write_text(report)
        print(f"Report saved to: {report_path}")

    def _handle_interrupt(self, signum: int, frame: object) -> None:
        """Handle interrupt signal."""
        if self._interrupted:
            # Second interrupt, exit immediately
            print("\nForced exit")
            sys.exit(1)

        print("\nInterrupted, finishing current benchmark...")
        self._interrupted = True


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="ASGI Server Benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run standard benchmarks
    uv run python -m benchmarks.servers.runner

    # Quick mode (shorter duration)
    uv run python -m benchmarks.servers.runner --quick

    # Specific servers
    uv run python -m benchmarks.servers.runner --servers vertebra,granian

    # Specific scenarios
    uv run python -m benchmarks.servers.runner --scenarios hello,json

    # Full comprehensive benchmark
    uv run python -m benchmarks.servers.runner --scenarios all --concurrency 8,64,256,512
        """,
    )

    parser.add_argument("--servers", type=str, help="Comma-separated list of servers to benchmark (default: all)")
    parser.add_argument(
        "--scenarios", type=str, help="Comma-separated list of scenarios, or 'all' (default: standard set)"
    )
    parser.add_argument("--quick", action="store_true", help="Quick mode with reduced duration")
    parser.add_argument("--duration", type=int, help="Measurement duration in seconds (default: 10, quick: 3)")
    parser.add_argument("--concurrency", type=str, help="Comma-separated concurrency levels (default: 64,256)")
    parser.add_argument("--workers", type=int, default=1, help="Worker count for servers (default: 1)")
    parser.add_argument("--output-dir", type=Path, help="Output directory for results (default: benchmarks/results/)")
    parser.add_argument("--verbose", action="store_true", help="Show server stdout/stderr")
    parser.add_argument("--no-venv", action="store_true", help="Don't use benchmark venv, use current environment")
    parser.add_argument("--list-servers", action="store_true", help="List available servers and exit")
    parser.add_argument("--list-scenarios", action="store_true", help="List available scenarios and exit")

    args = parser.parse_args()

    # Handle list commands
    if args.list_servers:
        print("Available servers:\n")
        for category_name, servers in [
            ("Category 1: Raw ASGI Servers", CATEGORY_1_SERVERS),
            ("Category 2: Integrated Runtimes", CATEGORY_2_RUNTIMES),
            ("Category 3: Framework Overhead", CATEGORY_3_FRAMEWORKS),
        ]:
            print(f"  {category_name}:")
            for name, config in servers.items():
                print(f"    {name:20} - {config.display_name}")
            print()
        return 0

    if args.list_scenarios:
        from .scenarios import SCENARIOS

        print("Available scenarios:\n")
        for name, scenario in SCENARIOS.items():
            print(f"  {name:15} - {scenario.description}")
        return 0

    # Parse server filter
    servers = None
    if args.servers:
        server_names = [s.strip() for s in args.servers.split(",")]
        servers = get_available_servers(server_names)
        if not servers:
            print(f"No valid servers found in: {args.servers}")
            print("Use --list-servers to see available options")
            return 1

    # Parse scenarios
    scenarios = None
    if args.scenarios:
        if args.scenarios.lower() == "all":
            from .scenarios import FULL_SCENARIOS, SCENARIOS

            scenarios = {name: SCENARIOS[name] for name in FULL_SCENARIOS}
        else:
            scenario_names = [s.strip() for s in args.scenarios.split(",")]
            scenarios = get_scenarios(names=scenario_names)
            if not scenarios:
                print(f"No valid scenarios found in: {args.scenarios}")
                print("Use --list-scenarios to see available options")
                return 1

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

        # Override duration if specified
        if args.duration:
            runner.measure_duration = args.duration

        runner.run()
        return 0

    except OhaNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\nAborted")
        return 130


if __name__ == "__main__":
    sys.exit(main())
