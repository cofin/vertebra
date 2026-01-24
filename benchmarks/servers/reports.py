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

    # Vertebra-specific analysis
    analysis = generate_vertebra_analysis(suite)
    if any(analysis.get(k) for k in ["native_vs_asgi", "overhead_breakdown"]):
        lines.extend(format_vertebra_report(analysis))

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


def generate_vertebra_analysis(suite: BenchmarkSuite) -> dict[str, object]:
    """Generate Vertebra-specific metrics breakdown.

    Analyzes Vertebra variants and compares:
    - Raw ASGI vs Litestar ASGI vs Litestar Native performance
    - Overhead estimates for each component layer
    - Performance relative to baseline servers

    Args:
        suite: BenchmarkSuite with results

    Returns:
        Dictionary with analysis metrics
    """
    vertebra_servers = ["vertebra", "vertebra_asgi_litestar", "vertebra_native_litestar", "vertebra_fastapi"]
    baseline_servers = ["uvicorn_httptools", "granian"]

    # Get results for vertebra servers
    vertebra_results = {r.server: r for r in suite.results if r.server in vertebra_servers}
    baseline_results = {r.server: r for r in suite.results if r.server in baseline_servers}

    analysis: dict[str, object] = {
        "vertebra_variants": {},
        "native_vs_asgi": {},
        "overhead_breakdown": {},
        "baseline_comparison": {},
    }

    # Skip if no vertebra results
    if not vertebra_results:
        return analysis

    # Group by scenario for analysis
    scenarios = {r.scenario for r in vertebra_results.values()}

    for scenario in scenarios:
        # Get results for this scenario
        scenario_vertebra = {
            server: r for server, r in vertebra_results.items() if r.scenario == scenario
        }
        scenario_baselines = {
            server: r for server, r in baseline_results.items() if r.scenario == scenario
        }

        # Vertebra variants comparison
        if len(scenario_vertebra) > 1:
            analysis["vertebra_variants"][scenario] = {
                server: {"rps": r.rps, "p99_ms": r.latency_p99_ms}
                for server, r in scenario_vertebra.items()
            }

        # Native vs ASGI comparison (Litestar)
        asgi_result = scenario_vertebra.get("vertebra_asgi_litestar")
        native_result = scenario_vertebra.get("vertebra_native_litestar")

        if asgi_result and native_result:
            speedup = (native_result.rps / asgi_result.rps - 1) * 100 if asgi_result.rps > 0 else 0
            analysis["native_vs_asgi"][scenario] = {
                "asgi_rps": asgi_result.rps,
                "native_rps": native_result.rps,
                "speedup_percent": speedup,
                "asgi_p99_ms": asgi_result.latency_p99_ms,
                "native_p99_ms": native_result.latency_p99_ms,
            }

        # Overhead breakdown estimation
        raw_vertebra = scenario_vertebra.get("vertebra")
        litestar_vertebra = scenario_vertebra.get("vertebra_asgi_litestar")
        uvicorn = scenario_baselines.get("uvicorn_httptools")

        if raw_vertebra and uvicorn:
            # Estimate: Vertebra raw vs Uvicorn raw = IPC + HTTP layer overhead
            ipc_overhead = (1 - raw_vertebra.rps / uvicorn.rps) * 100 if uvicorn.rps > 0 else 0

            breakdown = {
                "vertebra_raw_rps": raw_vertebra.rps,
                "uvicorn_raw_rps": uvicorn.rps,
                "ipc_transport_overhead_percent": ipc_overhead,
            }

            if litestar_vertebra:
                # Framework overhead = raw vs framework
                framework_overhead = (1 - litestar_vertebra.rps / raw_vertebra.rps) * 100 if raw_vertebra.rps > 0 else 0
                breakdown["framework_overhead_percent"] = framework_overhead
                breakdown["litestar_rps"] = litestar_vertebra.rps

            analysis["overhead_breakdown"][scenario] = breakdown

        # Baseline comparison
        if raw_vertebra and scenario_baselines:
            analysis["baseline_comparison"][scenario] = {
                "vertebra_rps": raw_vertebra.rps,
                **{
                    server: {
                        "rps": r.rps,
                        "ratio": raw_vertebra.rps / r.rps if r.rps > 0 else 0,
                    }
                    for server, r in scenario_baselines.items()
                },
            }

    return analysis


def format_vertebra_report(analysis: dict[str, object]) -> list[str]:
    """Format Vertebra analysis as markdown sections.

    Args:
        analysis: Output from generate_vertebra_analysis

    Returns:
        List of markdown lines
    """
    lines = ["## Vertebra Performance Analysis", ""]

    # Native vs ASGI comparison
    native_vs_asgi = analysis.get("native_vs_asgi", {})
    if native_vs_asgi:
        lines.extend([
            "### Native Router vs ASGI Bridge (Litestar)",
            "",
            "| Scenario | ASGI RPS | Native RPS | Speedup | ASGI p99 | Native p99 |",
            "|----------|--------:|----------:|--------:|---------:|----------:|",
        ])

        for scenario, data in native_vs_asgi.items():
            lines.append(
                f"| {scenario} | {data['asgi_rps']:,.0f} | {data['native_rps']:,.0f} | "
                f"{data['speedup_percent']:+.1f}% | {data['asgi_p99_ms']:.2f}ms | "
                f"{data['native_p99_ms']:.2f}ms |"
            )

        lines.append("")

    # Overhead breakdown
    overhead = analysis.get("overhead_breakdown", {})
    if overhead:
        lines.extend([
            "### Component Overhead Estimates",
            "",
            "Comparing Vertebra layers to isolate overhead sources:",
            "",
        ])

        for scenario, data in overhead.items():
            lines.append(f"**{scenario}**")
            lines.append("")
            lines.append(f"- Uvicorn baseline: {data['uvicorn_raw_rps']:,.0f} RPS")
            lines.append(f"- Vertebra raw ASGI: {data['vertebra_raw_rps']:,.0f} RPS")
            lines.append(f"- IPC transport overhead: {data['ipc_transport_overhead_percent']:.1f}%")

            if "framework_overhead_percent" in data:
                lines.append(f"- Vertebra + Litestar: {data['litestar_rps']:,.0f} RPS")
                lines.append(f"- Framework overhead: {data['framework_overhead_percent']:.1f}%")

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
