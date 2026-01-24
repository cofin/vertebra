"""Report generation for ASGI benchmarks.

Generates markdown reports with comparison tables and summaries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .config import BenchmarkCategory

if TYPE_CHECKING:
    from .results import BenchmarkResult, BenchmarkSuite


def generate_markdown_report(suite: BenchmarkSuite) -> str:
    """Generate a markdown report from benchmark results.

    Args:
        suite: BenchmarkSuite with all results

    Returns:
        Markdown-formatted report string
    """
    lines: list[str] = []

    # Header
    lines.extend([
        "# ASGI Server Benchmark Results",
        "",
        "## Environment",
        "",
        f"- **Date**: {suite.environment.timestamp}",
        f"- **CPU**: {suite.environment.cpu}",
        f"- **Cores**: {suite.environment.cpu_cores}",
        f"- **OS**: {suite.environment.os} {suite.environment.os_version}",
        f"- **Python**: {suite.environment.python_version}",
        f"- **oha**: {suite.environment.oha_version}",
        "",
    ])

    # Generate tables by category
    for category, category_name in [
        (BenchmarkCategory.SERVER, "Category 1: Raw ASGI Server Performance"),
        (BenchmarkCategory.RUNTIME, "Category 2: Integrated Runtime Performance"),
        (BenchmarkCategory.FRAMEWORK, "Category 3: Framework Overhead"),
    ]:
        category_results = suite.get_results_by_category(category)
        if category_results:
            lines.extend(_generate_category_section(category_name, category_results))

    # Summary rankings
    lines.extend(_generate_rankings(suite))

    # Errors section
    if suite.errors:
        lines.extend(["## Errors", "", "The following benchmarks encountered errors:", ""])
        for error in suite.errors:
            lines.append(f"- **{error['server']}** / {error['scenario']}: {error['error']}")
        lines.append("")

    # Methodology
    lines.extend([
        "## Methodology",
        "",
        "Benchmarks use a three-phase approach for accurate measurement:",
        "",
        "1. **Primer** (4s): Warm up caches, JIT, connection pools",
        "2. **Warmup** (3s): Stabilization period",
        "3. **Measurement** (10s): Actual data collection with `oha`",
        "",
        "All servers run with:",
        "- Single worker process (for fair comparison)",
        "- uvloop event loop (where supported)",
        "- Access logging disabled",
        "",
        "Load generation uses `oha` with latency correction to avoid",
        "coordinated omission bias.",
        "",
    ])

    return "\n".join(lines)


def _generate_category_section(category_name: str, results: list[BenchmarkResult]) -> list[str]:
    """Generate markdown section for a category."""
    lines = [f"## {category_name}", ""]

    # Group by scenario
    scenarios = sorted({r.scenario for r in results})

    for scenario in scenarios:
        scenario_results = [r for r in results if r.scenario == scenario]
        if not scenario_results:
            continue

        # Get description from first result's metadata or use scenario name
        scenario_results[0].metadata.get("description", scenario)

        lines.extend([f"### {scenario}", ""])

        # Group by concurrency
        concurrency_levels = sorted({r.concurrency for r in scenario_results})

        for concurrency in concurrency_levels:
            concurrency_results = [r for r in scenario_results if r.concurrency == concurrency]

            # Sort by RPS (highest first)
            concurrency_results.sort(key=lambda r: -r.rps)

            lines.extend([
                f"**Concurrency: {concurrency}**",
                "",
                "| Server | RPS | Avg (ms) | p50 (ms) | p95 (ms) | p99 (ms) | Max (ms) |",
                "|--------|----:|--------:|--------:|--------:|--------:|--------:|",
            ])

            for r in concurrency_results:
                lines.append(
                    f"| {r.server_display} | "
                    f"{r.rps:,.0f} | "
                    f"{r.latency_avg_ms:.2f} | "
                    f"{r.latency_p50_ms:.2f} | "
                    f"{r.latency_p95_ms:.2f} | "
                    f"{r.latency_p99_ms:.2f} | "
                    f"{r.latency_max_ms:.2f} |"
                )

            lines.append("")

    return lines


def _generate_rankings(suite: BenchmarkSuite) -> list[str]:
    """Generate overall rankings section."""
    lines = ["## Rankings", "", "Top performers by scenario (highest RPS):", ""]

    # Get main concurrency level (typically 64)
    all_concurrencies = {r.concurrency for r in suite.results}
    main_concurrency = 64 if 64 in all_concurrencies else min(all_concurrencies, default=64)

    scenarios = suite.get_scenarios()

    for scenario in scenarios:
        results = [r for r in suite.get_results_by_scenario(scenario) if r.concurrency == main_concurrency]

        if not results:
            continue

        results.sort(key=lambda r: -r.rps)

        lines.append(f"### {scenario} (c{main_concurrency})")
        lines.append("")

        for i, r in enumerate(results[:5], 1):
            medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
            lines.append(f"{medal} **{r.server_display}**: {r.rps:,.0f} RPS (p99: {r.latency_p99_ms:.2f}ms)")

        lines.append("")

    return lines


def generate_csv_report(suite: BenchmarkSuite) -> str:
    """Generate CSV format for spreadsheet analysis.

    Args:
        suite: BenchmarkSuite with all results

    Returns:
        CSV-formatted string
    """
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow([
        "server",
        "category",
        "scenario",
        "concurrency",
        "rps",
        "success_rate",
        "latency_avg_ms",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "latency_max_ms",
    ])

    for r in suite.results:
        writer.writerow([
            r.server,
            r.category.value,
            r.scenario,
            r.concurrency,
            f"{r.rps:.2f}",
            f"{r.success_rate:.2f}",
            f"{r.latency_avg_ms:.4f}",
            f"{r.latency_p50_ms:.4f}",
            f"{r.latency_p95_ms:.4f}",
            f"{r.latency_p99_ms:.4f}",
            f"{r.latency_max_ms:.4f}",
        ])

    return output.getvalue()
